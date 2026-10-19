//! Owns the application-wide Tokio runtime and its native threads.
//!
//! Requests run on Tokio workers; codec workers handle large data conversions.
//! The owner thread coordinates shutdown, drains writers and joins codec workers.
//! The Erlang runtime owner's resource monitor signals shutdown when that process exits.

use std::collections::HashMap;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};

use crate::codec::CodecPool;
use greptimedb_ingester::BulkStreamWriter;
use rustler::{Env, LocalPid, Monitor, Resource};
use tokio::runtime::{Builder, Handle};
use tokio::sync::{watch, Notify};
use tokio::time::Instant;
use tokio_util::task::TaskTracker;

static RUNTIMES: AtomicUsize = AtomicUsize::new(0);
static SHARED: Mutex<SharedState> = Mutex::new(SharedState::Stopped);
static NEXT_WRITER: AtomicUsize = AtomicUsize::new(0);

enum SharedState {
    Stopped,
    Starting,
    Running(Arc<RuntimeOwner>),
    Stopping(Arc<RuntimeOwner>),
}

pub struct Context {
    writers: Mutex<HashMap<usize, BulkStreamWriter>>,
    writers_changed: Notify,
}

pub(crate) struct WriterReservation<'a> {
    context: &'a Context,
    id: usize,
    writer: Option<BulkStreamWriter>,
}

impl WriterReservation<'_> {
    pub(crate) fn into_writer(mut self) -> Option<BulkStreamWriter> {
        self.writer.take()
    }
}

impl Drop for WriterReservation<'_> {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            self.context.insert_writer(self.id, writer);
        }
    }
}

#[derive(Clone, Copy)]
pub enum Shutdown {
    Running,
    Graceful(Instant),
    Abort,
}

impl Shutdown {
    pub fn is_stopping(self) -> bool {
        !matches!(self, Self::Running)
    }
}

pub async fn wait_for_shutdown(stopping: &mut watch::Receiver<Shutdown>, finish_stream: bool) {
    loop {
        let state = *stopping.borrow_and_update();
        match state {
            Shutdown::Running => {
                if stopping.changed().await.is_err() {
                    return;
                }
            }
            Shutdown::Graceful(deadline) if finish_stream => {
                tokio::select! {
                    biased;
                    changed = stopping.changed() => { if changed.is_err() { return; } },
                    _ = tokio::time::sleep_until(deadline) => return,
                }
            }
            _ => return,
        }
    }
}

impl Context {
    pub(crate) fn writers(&self) -> MutexGuard<'_, HashMap<usize, BulkStreamWriter>> {
        match self.writers.lock() {
            Ok(writers) => writers,
            Err(poisoned) => {
                self.writers.clear_poison();
                poisoned.into_inner()
            }
        }
    }

    pub(crate) fn insert_writer(&self, id: usize, writer: BulkStreamWriter) {
        self.writers().insert(id, writer);
        self.writers_changed.notify_one();
    }

    pub(crate) fn reserve_writer(&self, id: usize) -> Option<WriterReservation<'_>> {
        self.writers().remove(&id).map(|writer| WriterReservation {
            context: self,
            id,
            writer: Some(writer),
        })
    }

    async fn finish_writers(&self, stopping: &mut watch::Receiver<Shutdown>, tasks: &TaskTracker) {
        let mut finishing = FuturesUnordered::new();
        let mut tasks_done = false;
        loop {
            let Shutdown::Graceful(deadline) = *stopping.borrow() else {
                return;
            };
            if Instant::now() >= deadline {
                return;
            }
            let writers: Vec<_> = self.writers().drain().map(|(_, writer)| writer).collect();
            for writer in writers {
                finishing.push(AssertUnwindSafe(writer.finish()).catch_unwind());
            }
            if tasks_done && finishing.is_empty() {
                return;
            }
            // Returned writers must drain without waiting for unrelated stalled requests.
            tokio::select! {
                biased;
                _ = wait_for_shutdown(stopping, true) => return,
                _ = tasks.wait(), if !tasks_done => tasks_done = true,
                _ = finishing.next(), if !finishing.is_empty() => {},
                _ = self.writers_changed.notified() => {},
            }
        }
    }
}

pub struct RuntimeOwner {
    handle: Handle,
    send_tasks: TaskTracker,
    pub(crate) context: Arc<Context>,
    pub(crate) codec_pool: Arc<CodecPool>,
    shutdown: watch::Sender<Shutdown>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl RuntimeOwner {
    fn new(workers: usize, codec_workers: usize) -> std::io::Result<Self> {
        let codec_pool = CodecPool::new(codec_workers)?;
        let owner_codec_pool = codec_pool.clone();
        let runtime = Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("greptime-worker")
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        let send_tasks = TaskTracker::new();
        let owner_send_tasks = send_tasks.clone();
        let (shutdown, mut stopping) = watch::channel(Shutdown::Running);
        let context = Arc::new(Context {
            writers: Mutex::new(HashMap::new()),
            writers_changed: Notify::new(),
        });
        let owner_context = context.clone();
        RUNTIMES.fetch_add(1, Ordering::AcqRel);
        let thread = std::thread::Builder::new()
            .name("greptime-runtime-owner".into())
            .spawn(move || {
                let context = owner_context;
                runtime.block_on(async {
                    let _ = stopping.changed().await;
                    owner_send_tasks.close();
                    context
                        .finish_writers(&mut stopping, &owner_send_tasks)
                        .await;
                    owner_send_tasks.wait().await;
                });
                // Resources can outlive shutdown; release aborted or expired writers too.
                let writers = std::mem::take(&mut *context.writers());
                drop(writers);
                drop(context);
                owner_codec_pool.stop();
                drop(runtime);
                RUNTIMES.fetch_sub(1, Ordering::AcqRel);
            });
        match thread {
            Ok(thread) => Ok(Self {
                handle,
                send_tasks,
                context,
                codec_pool,
                shutdown,
                thread: Mutex::new(Some(thread)),
            }),
            Err(error) => {
                RUNTIMES.fetch_sub(1, Ordering::AcqRel);
                Err(error)
            }
        }
    }

    pub fn spawn<F>(&self, future: F) -> Result<(), ()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Registration stays under the shutdown read guard so its writer cannot
        // close the tracker and finish waiting before this task is registered.
        let shutdown = self.shutdown.borrow();
        if shutdown.is_stopping() {
            return Err(());
        }
        self.send_tasks.spawn_on(future, &self.handle);
        drop(shutdown);
        Ok(())
    }

    pub fn is_stopping(&self) -> bool {
        self.shutdown.borrow().is_stopping()
    }

    pub fn is_draining(&self) -> bool {
        matches!(*self.shutdown.borrow(), Shutdown::Graceful(deadline) if Instant::now() < deadline)
    }

    pub fn stopping(&self) -> watch::Receiver<Shutdown> {
        self.shutdown.subscribe()
    }

    fn signal_shutdown(&self, drain: Option<Duration>) {
        let requested = drain.map_or(Shutdown::Abort, |timeout| {
            Shutdown::Graceful(Instant::now() + timeout)
        });
        // Once aborted, a later join or graceful stop must not re-enable draining.
        self.shutdown.send_if_modified(|state| {
            if matches!(state, Shutdown::Running)
                || matches!(
                    (*state, requested),
                    (Shutdown::Graceful(_), Shutdown::Abort)
                )
            {
                *state = requested;
                true
            } else {
                false
            }
        });
        self.codec_pool.close();
    }

    fn stop(&self, drain: Option<Duration>) {
        self.signal_shutdown(drain);
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }

    pub fn discard(&self, id: usize) {
        let context = self.context.clone();
        let _ = self.spawn(async move {
            context.writers().remove(&id);
        });
    }
}

pub struct RuntimeResource(pub Arc<RuntimeOwner>);

impl Resource for RuntimeResource {
    const IMPLEMENTS_DOWN: bool = true;

    fn down(&self, _env: Env, _pid: LocalPid, _monitor: Monitor) {
        self.0.signal_shutdown(None);
    }
}

impl Drop for RuntimeResource {
    fn drop(&mut self) {
        self.0.signal_shutdown(None);
    }
}

struct StartGuard {
    previous: Option<Arc<RuntimeOwner>>,
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        let mut shared = shared_state();
        if matches!(*shared, SharedState::Starting) {
            *shared = self
                .previous
                .take()
                .map_or(SharedState::Stopped, SharedState::Running);
        }
    }
}

pub fn start(workers: usize, codec_workers: usize) -> Result<Arc<RuntimeOwner>, String> {
    if !(1..=64).contains(&workers) {
        return Err("runtime_workers must be between 1 and 64".into());
    }
    if !(1..=64).contains(&codec_workers) {
        return Err("codec_workers must be between 1 and 64".into());
    }
    let mut starting = {
        let mut shared = shared_state();
        let previous = match &*shared {
            SharedState::Stopped => None,
            SharedState::Running(previous) if previous.is_stopping() => Some(previous.clone()),
            SharedState::Running(_) => return Err("runtime already started".into()),
            SharedState::Starting => return Err("runtime is starting".into()),
            SharedState::Stopping(_) => return Err("runtime is stopping".into()),
        };
        *shared = SharedState::Starting;
        StartGuard { previous }
    };
    // Reserve startup before unlocking; connections must not wait for joins or creation.
    if let Some(previous) = starting.previous.as_ref() {
        previous.stop(None);
    }
    starting.previous = None;
    let runtime = catch_unwind(AssertUnwindSafe(|| {
        RuntimeOwner::new(workers, codec_workers)
    }))
    .map_err(|panic| {
        let reason = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("unknown panic");
        format!("runtime initialization panicked: {reason}")
    })?
    .map_err(|e| e.to_string())?;
    let runtime = Arc::new(runtime);
    *shared_state() = SharedState::Running(runtime.clone());
    drop(starting);
    Ok(runtime)
}

pub fn shared() -> Result<Arc<RuntimeOwner>, String> {
    match &*shared_state() {
        SharedState::Running(runtime) if !runtime.is_stopping() => Ok(runtime.clone()),
        _ => Err("greptimedb_rs runtime is not running".into()),
    }
}

pub fn stop(resource: &RuntimeResource) {
    stop_with_timeout(resource, Duration::from_secs(5));
}

pub fn stop_with_timeout(resource: &RuntimeResource, timeout: Duration) {
    {
        let mut shared = shared_state();
        if matches!(&*shared, SharedState::Running(runtime) if Arc::ptr_eq(runtime, &resource.0)) {
            *shared = SharedState::Stopping(resource.0.clone());
        }
    }
    resource.0.stop(Some(timeout));
    let mut shared = shared_state();
    if matches!(&*shared, SharedState::Stopping(runtime) if Arc::ptr_eq(runtime, &resource.0)) {
        *shared = SharedState::Stopped;
    }
}

fn shared_state() -> MutexGuard<'static, SharedState> {
    match SHARED.lock() {
        Ok(shared) => shared,
        Err(poisoned) => {
            let shared = poisoned.into_inner();
            if let SharedState::Running(runtime) = &*shared {
                // Fail closed without joining under the state lock. Startup reclaims it.
                runtime.signal_shutdown(None);
            }
            SHARED.clear_poison();
            shared
        }
    }
}

pub fn writer_id() -> usize {
    NEXT_WRITER.fetch_add(1, Ordering::Relaxed)
}

pub fn count() -> usize {
    RUNTIMES.load(Ordering::Acquire)
}

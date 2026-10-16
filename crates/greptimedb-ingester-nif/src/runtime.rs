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

use futures::FutureExt;

use crate::codec::CodecPool;
use greptimedb_ingester::BulkStreamWriter;
use rustler::{Env, LocalPid, Monitor, Resource};
use tokio::runtime::{Builder, Handle};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::task::TaskTracker;

static RUNTIMES: AtomicUsize = AtomicUsize::new(0);
static SHARED: Mutex<Option<Arc<RuntimeOwner>>> = Mutex::new(None);
static NEXT_WRITER: AtomicUsize = AtomicUsize::new(0);

pub struct Context {
    writers: Mutex<HashMap<usize, BulkStreamWriter>>,
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

    async fn finish_writers(&self, stopping: &mut watch::Receiver<Shutdown>) {
        let Shutdown::Graceful(deadline) = *stopping.borrow() else {
            return;
        };
        if Instant::now() >= deadline {
            return;
        }
        let writers: Vec<_> = self.writers().drain().map(|(_, writer)| writer).collect();
        let finish = futures::future::join_all(
            writers
                .into_iter()
                .map(|writer| AssertUnwindSafe(writer.finish()).catch_unwind()),
        );
        tokio::select! {
            biased;
            _ = wait_for_shutdown(stopping, true) => {},
            _ = finish => {},
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
                    owner_send_tasks.wait().await;
                    context.finish_writers(&mut stopping).await;
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

pub fn start(workers: usize, codec_workers: usize) -> Result<Arc<RuntimeOwner>, String> {
    if !(1..=64).contains(&workers) {
        return Err("runtime_workers must be between 1 and 64".into());
    }
    if !(1..=64).contains(&codec_workers) {
        return Err("codec_workers must be between 1 and 64".into());
    }
    let mut shared = shared_state();
    if let Some(previous) = shared.as_ref() {
        if !previous.is_stopping() {
            return Err("runtime already started".into());
        }
        previous.stop(None);
    }
    // Catch initialization panics before they unwind through the shared-state lock.
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
    *shared = Some(runtime.clone());
    Ok(runtime)
}

pub fn shared() -> Result<Arc<RuntimeOwner>, String> {
    shared_state()
        .as_ref()
        .filter(|runtime| !runtime.is_stopping())
        .cloned()
        .ok_or_else(|| "greptimedb_rs runtime is not running".into())
}

pub fn stop(resource: &RuntimeResource) {
    stop_with_timeout(resource, Duration::from_secs(5));
}

pub fn stop_with_timeout(resource: &RuntimeResource, timeout: Duration) {
    let mut shared = shared_state();
    resource.0.stop(Some(timeout));
    if shared
        .as_ref()
        .is_some_and(|runtime| Arc::ptr_eq(runtime, &resource.0))
    {
        *shared = None;
    }
}

fn shared_state() -> MutexGuard<'static, Option<Arc<RuntimeOwner>>> {
    match SHARED.lock() {
        Ok(shared) => shared,
        Err(poisoned) => {
            let mut shared = poisoned.into_inner();
            if let Some(previous) = shared.take() {
                previous.stop(None);
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

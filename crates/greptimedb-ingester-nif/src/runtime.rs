//! Owns the application-wide Tokio runtime and its native threads.
//!
//! Non-Send SDK futures and streaming writers stay on the owner thread's LocalSet.
//! Its event loop drives requests, then drains tasks and joins codec workers on shutdown.
//! The Erlang runtime owner's resource monitor signals shutdown when that process exits.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;

use crate::codec::CodecPool;
use greptimedb_ingester::BulkStreamWriter;
use rustler::{Env, LocalPid, Monitor, Resource};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch};

static RUNTIMES: AtomicUsize = AtomicUsize::new(0);
static SHARED: Mutex<Option<Arc<RuntimeOwner>>> = Mutex::new(None);
static NEXT_WRITER: AtomicUsize = AtomicUsize::new(0);

pub struct Context {
    pub writers: RefCell<HashMap<usize, BulkStreamWriter>>,
}

type Job = Box<dyn FnOnce(Rc<Context>) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

pub struct RuntimeOwner {
    jobs: mpsc::UnboundedSender<Job>,
    pub(crate) codec_pool: Arc<CodecPool>,
    shutdown: watch::Sender<bool>,
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
        let (shutdown, mut stopping) = watch::channel(false);
        let (jobs, mut receiver) = mpsc::unbounded_channel::<Job>();
        RUNTIMES.fetch_add(1, Ordering::AcqRel);
        let thread = std::thread::Builder::new()
            .name("greptime-runtime-owner".into())
            .spawn(move || {
                let context = Rc::new(Context {
                    writers: RefCell::new(HashMap::new()),
                });
                let tasks = tokio::task::LocalSet::new();
                runtime.block_on(tasks.run_until(async {
                    loop {
                        tokio::select! {
                            biased;
                            _ = stopping.changed() => break,
                            job = receiver.recv() => match job {
                                Some(job) => { tokio::task::spawn_local(job(context.clone())); }
                                None => break,
                            }
                        }
                    }
                }));
                receiver.close();
                runtime.block_on(tasks.run_until(async {
                    while let Some(job) = receiver.recv().await {
                        tokio::task::spawn_local(job(context.clone()));
                    }
                }));
                runtime.block_on(tasks);
                drop(context);
                owner_codec_pool.stop();
                drop(runtime);
                RUNTIMES.fetch_sub(1, Ordering::AcqRel);
            });
        match thread {
            Ok(thread) => Ok(Self {
                jobs,
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

    pub fn spawn<F, Fut>(&self, make: F) -> Result<(), ()>
    where
        F: FnOnce(Rc<Context>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        if *self.shutdown.borrow() {
            return Err(());
        }
        self.jobs
            .send(Box::new(move |context| Box::pin(make(context))))
            .map_err(|_| ())
    }

    pub fn is_stopping(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub fn stopping(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    fn signal_shutdown(&self) {
        self.shutdown.send_replace(true);
        self.codec_pool.close();
    }

    fn stop(&self) {
        self.signal_shutdown();
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }

    pub fn discard(&self, id: usize) {
        let _ = self.spawn(move |context| async move {
            context.writers.borrow_mut().remove(&id);
        });
    }
}

pub struct RuntimeResource(pub Arc<RuntimeOwner>);

impl Resource for RuntimeResource {
    const IMPLEMENTS_DOWN: bool = true;

    fn down(&self, _env: Env, _pid: LocalPid, _monitor: Monitor) {
        self.0.signal_shutdown();
    }
}

impl Drop for RuntimeResource {
    fn drop(&mut self) {
        self.0.signal_shutdown();
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
        if !*previous.shutdown.borrow() {
            return Err("runtime already started".into());
        }
        previous.stop();
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
        .filter(|runtime| !*runtime.shutdown.borrow())
        .cloned()
        .ok_or_else(|| "greptimedb_rs runtime is not running".into())
}

pub fn stop(resource: &RuntimeResource) {
    let mut shared = shared_state();
    resource.0.stop();
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
                previous.stop();
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

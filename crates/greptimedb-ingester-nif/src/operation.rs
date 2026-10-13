use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::data_conversion_pool::DataConversionPool;
use greptimedb_ingester::BulkStreamWriter;
use rustler::{Env, LocalPid, Monitor, Resource};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, watch, Notify};

static RUNTIMES: AtomicUsize = AtomicUsize::new(0);
static OPERATIONS: AtomicUsize = AtomicUsize::new(0);
static SHARED: Mutex<Option<Arc<RuntimeOwner>>> = Mutex::new(None);
static NEXT_WRITER: AtomicUsize = AtomicUsize::new(0);

pub struct Context {
    pub writers: RefCell<HashMap<usize, BulkStreamWriter>>,
}

type Job = Box<dyn FnOnce(Rc<Context>) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

pub struct RuntimeOwner {
    jobs: mpsc::UnboundedSender<Job>,
    pub(crate) data_conversion_pool: Arc<DataConversionPool>,
    shutdown: watch::Sender<bool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl RuntimeOwner {
    fn new(workers: usize, cpu_workers: usize) -> std::io::Result<Self> {
        let data_conversion_pool = DataConversionPool::new(cpu_workers)?;
        let owner_data_conversion_pool = data_conversion_pool.clone();
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
                // SDK streaming futures are not Send. Construct and poll them
                // here; all clients share this LocalSet and the worker pool.
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
                owner_data_conversion_pool.stop();
                drop(runtime);
                RUNTIMES.fetch_sub(1, Ordering::AcqRel);
            });
        match thread {
            Ok(thread) => Ok(Self {
                jobs,
                data_conversion_pool,
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
        self.data_conversion_pool.close();
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

pub fn start(workers: usize, cpu_workers: usize) -> Result<Arc<RuntimeOwner>, String> {
    if !(1..=64).contains(&workers) {
        return Err("runtime_workers must be between 1 and 64".into());
    }
    if !(1..=64).contains(&cpu_workers) {
        return Err("cpu_workers must be between 1 and 64".into());
    }
    let mut shared = SHARED.lock().unwrap();
    if let Some(previous) = shared.as_ref() {
        if !*previous.shutdown.borrow() {
            return Err("runtime already started".into());
        }
        previous.stop();
    }
    let runtime = Arc::new(RuntimeOwner::new(workers, cpu_workers).map_err(|e| e.to_string())?);
    *shared = Some(runtime.clone());
    Ok(runtime)
}

pub fn shared() -> Result<Arc<RuntimeOwner>, String> {
    SHARED
        .lock()
        .unwrap()
        .as_ref()
        .filter(|runtime| !*runtime.shutdown.borrow())
        .cloned()
        .ok_or_else(|| "greptimedb_rs runtime is not running".into())
}

pub fn stop(resource: &RuntimeResource) {
    let mut shared = SHARED.lock().unwrap();
    resource.0.stop();
    if shared
        .as_ref()
        .is_some_and(|runtime| Arc::ptr_eq(runtime, &resource.0))
    {
        *shared = None;
    }
}

pub fn writer_id() -> usize {
    NEXT_WRITER.fetch_add(1, Ordering::Relaxed)
}

pub struct Operation {
    pub cancel: watch::Sender<bool>,
    pub active: AtomicBool,
    cpu_jobs: AtomicUsize,
    cpu_done: Notify,
}

impl Operation {
    pub fn new() -> (Arc<Self>, watch::Receiver<bool>) {
        let (cancel, receiver) = watch::channel(false);
        OPERATIONS.fetch_add(1, Ordering::AcqRel);
        (
            Arc::new(Self {
                cancel,
                active: AtomicBool::new(true),
                cpu_jobs: AtomicUsize::new(0),
                cpu_done: Notify::new(),
            }),
            receiver,
        )
    }

    pub(crate) fn begin_cpu(self: &Arc<Self>) -> CpuGuard {
        self.cpu_jobs.fetch_add(1, Ordering::AcqRel);
        CpuGuard(self.clone())
    }

    pub(crate) async fn wait_cpu(&self) {
        while self.cpu_jobs.load(Ordering::Acquire) != 0 {
            self.cpu_done.notified().await;
        }
    }

    pub fn cancel(&self) {
        let _ = self.cancel.send(true);
    }

    pub fn finish(&self) {
        if self.active.swap(false, Ordering::AcqRel) {
            OPERATIONS.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

pub(crate) struct CpuGuard(Arc<Operation>);

impl Drop for CpuGuard {
    fn drop(&mut self) {
        if self.0.cpu_jobs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.cpu_done.notify_one();
        }
    }
}

pub struct OperationResource(pub Arc<Operation>);

impl Resource for OperationResource {
    const IMPLEMENTS_DOWN: bool = true;

    fn down(&self, _env: Env, _pid: LocalPid, _monitor: Monitor) {
        self.0.cancel();
    }
}

pub fn counts() -> (usize, usize) {
    (
        RUNTIMES.load(Ordering::Acquire),
        OPERATIONS.load(Ordering::Acquire),
    )
}

//! Runs Erlang row decoding, SDK data conversion and Erlang result encoding.
//!
//! A fixed thread pool and shared permits bound queued/running codec jobs.
//! Transport protocol encoding and decoding remain the SDK's responsibility.

use crossbeam_channel::{bounded, Sender};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use tokio::sync::{oneshot, Semaphore};
use tokio::time::Instant;

use crate::operation::{CodecJobGuard, Operation};
use crate::Failure;

static WORKERS: AtomicUsize = AtomicUsize::new(0);
static CAPACITY: AtomicUsize = AtomicUsize::new(0);
static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static QUEUED: AtomicUsize = AtomicUsize::new(0);
static WAITING: AtomicUsize = AtomicUsize::new(0);

type Job = Box<dyn FnOnce() + Send>;

pub(crate) struct CodecPool {
    jobs: Mutex<Option<Sender<Job>>>,
    permits: Arc<Semaphore>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl CodecPool {
    pub(crate) fn new(workers: usize) -> std::io::Result<Arc<Self>> {
        let (sender, receiver) = bounded::<Job>(workers * 2);
        let pool = Arc::new(Self {
            jobs: Mutex::new(Some(sender)),
            permits: Arc::new(Semaphore::new(workers * 2)),
            threads: Mutex::new(Vec::new()),
            stopped: AtomicBool::new(false),
        });
        WORKERS.store(workers, Ordering::Release);
        CAPACITY.store(workers * 2, Ordering::Release);
        for i in 0..workers {
            let receiver = receiver.clone();
            let thread = std::thread::Builder::new()
                .name(format!("greptime-codec-{i}"))
                .spawn(move || loop {
                    let job = receiver.recv();
                    match job {
                        Ok(job) => job(),
                        Err(_) => break,
                    }
                })?;
            pool.threads.lock().unwrap().push(thread);
        }
        Ok(pool)
    }

    pub(crate) async fn run<F, T>(
        &self,
        operation: Arc<Operation>,
        deadline: Instant,
        work: F,
    ) -> Result<T, Failure>
    where
        F: FnOnce() -> Result<T, Failure> + Send + 'static,
        T: Send + 'static,
    {
        WAITING.fetch_add(1, Ordering::AcqRel);
        let waiting = Waiting;
        let mut cancelled = operation.cancel.subscribe();
        let permit = tokio::select! {
            biased;
            _ = async {
                if !*cancelled.borrow() { let _ = cancelled.changed().await; }
            } => return Err(Failure::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(Failure::Timeout),
            permit = self.permits.clone().acquire_owned() => permit.map_err(|_| Failure::Closed)?,
        };
        drop(waiting);
        crate::check_operation(deadline, &operation)?;
        let permits = self.permits.clone();
        let mut guard = JobGuard::new(operation.clone());
        let (sender, receiver) = oneshot::channel();
        let job = Box::new(move || {
            guard.start();
            let result = catch_unwind(AssertUnwindSafe(|| {
                if permits.is_closed() {
                    return Err(Failure::Closed);
                }
                crate::check_operation(deadline, &operation)?;
                let value = work()?;
                crate::check_operation(deadline, &operation)?;
                if permits.is_closed() {
                    return Err(Failure::Closed);
                }
                Ok(value)
            }))
            .unwrap_or(Err(Failure::Panic));
            let _ = sender.send(result);
            drop(guard);
            drop(permit);
        });
        {
            let jobs = self.jobs.lock().unwrap();
            let sender = jobs.as_ref().ok_or(Failure::Closed)?;
            // This job owns a permit, so fewer than capacity jobs can be queued.
            // Keep submission nonblocking even when shutdown races with admission.
            sender.try_send(job).map_err(|_| Failure::Closed)?;
        }
        tokio::select! {
            biased;
            _ = async {
                if !*cancelled.borrow() { let _ = cancelled.changed().await; }
            } => Err(Failure::Cancelled),
            _ = tokio::time::sleep_until(deadline) => Err(Failure::Timeout),
            result = receiver => result.map_err(|_| Failure::Panic)?,
        }
    }

    pub(crate) fn close(&self) {
        self.permits.close();
    }

    pub(crate) fn stop(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        self.close();
        self.jobs.lock().unwrap().take();
        for thread in self.threads.lock().unwrap().drain(..) {
            let _ = thread.join();
        }
        WORKERS.store(0, Ordering::Release);
        CAPACITY.store(0, Ordering::Release);
    }
}

impl Drop for CodecPool {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Waiting;

impl Drop for Waiting {
    fn drop(&mut self) {
        WAITING.fetch_sub(1, Ordering::AcqRel);
    }
}

struct JobGuard {
    _operation: CodecJobGuard,
    started: bool,
}

impl JobGuard {
    fn new(operation: Arc<Operation>) -> Self {
        let operation = operation.begin_codec_job();
        QUEUED.fetch_add(1, Ordering::AcqRel);
        Self {
            _operation: operation,
            started: false,
        }
    }

    fn start(&mut self) {
        self.started = true;
        QUEUED.fetch_sub(1, Ordering::AcqRel);
        ACTIVE.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        if self.started {
            ACTIVE.fetch_sub(1, Ordering::AcqRel);
        } else {
            QUEUED.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

pub(crate) fn counts() -> (usize, usize, usize, usize, usize) {
    (
        WORKERS.load(Ordering::Acquire),
        CAPACITY.load(Ordering::Acquire),
        ACTIVE.load(Ordering::Acquire),
        QUEUED.load(Ordering::Acquire),
        WAITING.load(Ordering::Acquire),
    )
}

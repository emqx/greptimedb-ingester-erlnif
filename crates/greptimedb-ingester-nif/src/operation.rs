//! Tracks cancellation and reclamation of individual native requests.
//!
//! State is shared by request tasks and codec workers; this module creates no threads.
//! The resource monitor cancels requests when their Erlang connection process exits.
//! Codec job guards keep cancelled requests active until their conversion/encoding work ends.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use rustler::{Env, LocalPid, Monitor, Resource};
use tokio::sync::{watch, Notify};

static OPERATIONS: AtomicUsize = AtomicUsize::new(0);

pub struct Operation {
    pub finish_stream: bool,
    // Request tasks and conversion jobs need both the latest cancellation state
    // and an async wakeup. Receivers added after cancellation can read true.
    pub cancel: watch::Sender<bool>,
    pub active: AtomicBool,
    codec_jobs: AtomicUsize,
    codec_done: Notify,
}

impl Operation {
    pub fn new(finish_stream: bool) -> (Arc<Self>, watch::Receiver<bool>) {
        let (cancel, receiver) = watch::channel(false);
        OPERATIONS.fetch_add(1, Ordering::AcqRel);
        (
            Arc::new(Self {
                finish_stream,
                cancel,
                active: AtomicBool::new(true),
                codec_jobs: AtomicUsize::new(0),
                codec_done: Notify::new(),
            }),
            receiver,
        )
    }

    pub(crate) fn begin_codec_job(self: &Arc<Self>) -> CodecJobGuard {
        self.codec_jobs.fetch_add(1, Ordering::AcqRel);
        CodecJobGuard(self.clone())
    }

    pub(crate) async fn wait_codec_jobs(&self) {
        while self.codec_jobs.load(Ordering::Acquire) != 0 {
            self.codec_done.notified().await;
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

pub(crate) struct CodecJobGuard(Arc<Operation>);

impl Drop for CodecJobGuard {
    fn drop(&mut self) {
        if self.0.codec_jobs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.codec_done.notify_one();
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

pub fn count() -> usize {
    OPERATIONS.load(Ordering::Acquire)
}

use std::borrow::BorrowMut;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::metrics;
use subsquid_messages::query_result;
use subsquid_network_transport::PeerId;

use crate::query::QueryResult;

#[derive(Debug)]
pub struct RunningTask {
    pub worker_id: PeerId,
    result_sender: oneshot::Sender<QueryResult>,
    timeout_handle: JoinHandle<()>,
    start_time: Instant,
}

impl RunningTask {
    fn timeout(self) -> FinishedTask {
        self.finish(QueryResult::Timeout)
    }

    fn result_received(self, result: query_result::Result) -> FinishedTask {
        self.cancel_timeout();
        self.finish(result.into())
    }

    fn cancel_timeout(&self) {
        self.timeout_handle.abort();
    }

    fn finish(self, result: QueryResult) -> FinishedTask {
        let exec_time = self.start_time.elapsed();
        self.result_sender
            .send(result.clone())
            .unwrap_or_else(|_| log::warn!("Query result receiver dropped"));
        let finished_task = FinishedTask {
            worker_id: self.worker_id,
            exec_time,
            result,
        };
        metrics::query_finished(&finished_task);
        finished_task
    }
}

#[derive(Debug)]
pub struct FinishedTask {
    pub worker_id: PeerId,
    pub exec_time: Duration,
    pub result: QueryResult,
}

impl FinishedTask {
    pub fn exec_time_ms(&self) -> u32 {
        self.exec_time
            .as_millis()
            .try_into()
            .expect("Tasks do not take that long")
    }
}

/// This wrapper is a drop guard for task
pub struct Task(Option<RunningTask>);

impl Task {
    pub fn new(
        worker_id: PeerId,
        result_sender: oneshot::Sender<QueryResult>,
        timeout_handle: JoinHandle<()>,
    ) -> Self {
        Self(Some(RunningTask {
            worker_id,
            result_sender,
            timeout_handle,
            start_time: Instant::now(),
        }))
    }

    /// Panics if task is already finished
    pub fn worker_id(&self) -> PeerId {
        self.0.as_ref().expect("Task already finished").worker_id
    }

    /// Panics if task is already finished
    pub fn timeout(&mut self) -> FinishedTask {
        self.0
            .borrow_mut()
            .take()
            .expect("Task already finished")
            .timeout()
    }

    /// Panics if task is already finished
    pub fn result_received(&mut self, result: query_result::Result) -> FinishedTask {
        self.0
            .borrow_mut()
            .take()
            .expect("Task already finished")
            .result_received(result)
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        self.0.as_ref().map(|t| t.cancel_timeout());
    }
}

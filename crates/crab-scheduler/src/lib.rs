//! Lane scheduler components for Crab.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crab_core::{CrabError, CrabResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedRun {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedRun {
    pub logical_session_id: String,
    pub run: QueuedRun,
}

#[derive(Debug, Clone)]
pub struct SessionLaneQueues {
    queue_limit: usize,
    queues: BTreeMap<String, VecDeque<QueuedRun>>,
}

impl SessionLaneQueues {
    pub fn new(queue_limit: usize) -> CrabResult<Self> {
        if queue_limit == 0 {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_LANE_QUEUE_LIMIT",
                value: queue_limit.to_string(),
                reason: "must be greater than 0",
            });
        }

        Ok(Self {
            queue_limit,
            queues: BTreeMap::new(),
        })
    }

    pub fn enqueue(&mut self, logical_session_id: &str, run: QueuedRun) -> CrabResult<()> {
        validate_session_id(logical_session_id)?;
        validate_run(&run)?;
        let queue = self
            .queues
            .entry(logical_session_id.to_string())
            .or_default();
        if queue.len() >= self.queue_limit {
            return Err(CrabError::InvariantViolation {
                context: "lane_queue_overflow",
                message: format!(
                    "queue limit {} reached for {logical_session_id}",
                    self.queue_limit
                ),
            });
        }

        queue.push_back(run);
        Ok(())
    }

    pub fn dequeue(&mut self, logical_session_id: &str) -> CrabResult<Option<QueuedRun>> {
        validate_session_id(logical_session_id)?;
        let maybe_item = match self.queues.get_mut(logical_session_id) {
            Some(queue) => queue.pop_front(),
            None => None,
        };

        let should_remove_lane = self
            .queues
            .get(logical_session_id)
            .is_some_and(VecDeque::is_empty);
        if should_remove_lane {
            let _ = self.queues.remove(logical_session_id);
        }
        Ok(maybe_item)
    }

    #[must_use]
    pub fn lane_count(&self) -> usize {
        self.queues.len()
    }

    pub fn queued_count(&self, logical_session_id: &str) -> CrabResult<usize> {
        validate_session_id(logical_session_id)?;
        Ok(self
            .queues
            .get(logical_session_id)
            .map_or(0, std::collections::VecDeque::len))
    }

    #[must_use]
    pub fn total_queued_count(&self) -> usize {
        self.queues.values().map(VecDeque::len).sum()
    }

    fn first_dispatchable_lane_id_excluding(
        &self,
        excluded_lanes: &BTreeSet<String>,
    ) -> Option<String> {
        for (lane_id, queue) in &self.queues {
            if excluded_lanes.contains(lane_id) || queue.is_empty() {
                continue;
            }
            return Some(lane_id.clone());
        }
        None
    }

    fn pop_front_from_known_non_empty(&mut self, logical_session_id: &str) -> QueuedRun {
        let queue = self
            .queues
            .get_mut(logical_session_id)
            .expect("dispatchable lane should exist");
        let next = queue
            .pop_front()
            .expect("dispatchable lane should contain at least one run");
        if queue.is_empty() {
            let _ = self.queues.remove(logical_session_id);
        }
        next
    }
}

#[derive(Debug, Clone)]
pub struct LaneScheduler {
    queues: SessionLaneQueues,
    max_concurrent_lanes: usize,
    active_lanes: BTreeSet<String>,
}

impl LaneScheduler {
    pub fn new(max_concurrent_lanes: usize, queue_limit: usize) -> CrabResult<Self> {
        if max_concurrent_lanes == 0 {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_MAX_CONCURRENT_LANES",
                value: max_concurrent_lanes.to_string(),
                reason: "must be greater than 0",
            });
        }

        Ok(Self {
            queues: SessionLaneQueues::new(queue_limit)?,
            max_concurrent_lanes,
            active_lanes: BTreeSet::new(),
        })
    }

    pub fn enqueue(&mut self, logical_session_id: &str, run: QueuedRun) -> CrabResult<()> {
        self.queues.enqueue(logical_session_id, run)
    }

    pub fn try_dispatch_next(&mut self) -> Option<DispatchedRun> {
        if self.active_lanes.len() >= self.max_concurrent_lanes {
            return None;
        }

        let lane_id = self
            .queues
            .first_dispatchable_lane_id_excluding(&self.active_lanes)?;
        let run = self.queues.pop_front_from_known_non_empty(&lane_id);
        let _ = self.active_lanes.insert(lane_id.clone());
        Some(DispatchedRun {
            logical_session_id: lane_id,
            run,
        })
    }

    pub fn complete_lane(&mut self, logical_session_id: &str) -> CrabResult<()> {
        validate_session_id(logical_session_id)?;
        if !self.active_lanes.remove(logical_session_id) {
            return Err(CrabError::InvariantViolation {
                context: "lane_scheduler_complete",
                message: format!("lane {logical_session_id} is not active"),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn active_lane_count(&self) -> usize {
        self.active_lanes.len()
    }

    pub fn queued_count(&self, logical_session_id: &str) -> CrabResult<usize> {
        self.queues.queued_count(logical_session_id)
    }

    #[must_use]
    pub fn total_queued_count(&self) -> usize {
        self.queues.total_queued_count()
    }
}

fn validate_session_id(logical_session_id: &str) -> CrabResult<()> {
    if logical_session_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "lane_queue_validate_session",
            message: "logical_session_id must not be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_run(run: &QueuedRun) -> CrabResult<()> {
    if run.run_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "lane_queue_validate_run",
            message: "run_id must not be empty".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crab_core::CrabError;

    use super::{LaneScheduler, QueuedRun, SessionLaneQueues};

    #[test]
    fn rejects_zero_queue_limit() {
        let error = SessionLaneQueues::new(0).expect_err("zero queue limit should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_LANE_QUEUE_LIMIT",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn enqueue_and_dequeue_are_fifo_for_one_lane() {
        let mut lanes = SessionLaneQueues::new(4).expect("queue init should succeed");

        lanes
            .enqueue("discord:channel:a", run("run-1"))
            .expect("first enqueue should succeed");
        lanes
            .enqueue("discord:channel:a", run("run-2"))
            .expect("second enqueue should succeed");
        lanes
            .enqueue("discord:channel:a", run("run-3"))
            .expect("third enqueue should succeed");

        assert_eq!(
            lanes
                .dequeue("discord:channel:a")
                .expect("first dequeue should succeed"),
            Some(run("run-1"))
        );
        assert_eq!(
            lanes
                .dequeue("discord:channel:a")
                .expect("second dequeue should succeed"),
            Some(run("run-2"))
        );
        assert_eq!(
            lanes
                .dequeue("discord:channel:a")
                .expect("third dequeue should succeed"),
            Some(run("run-3"))
        );
        assert_eq!(
            lanes
                .dequeue("discord:channel:a")
                .expect("dequeue on empty lane should succeed"),
            None
        );
    }

    #[test]
    fn interleaved_lanes_keep_independent_fifo_order() {
        let mut lanes = SessionLaneQueues::new(4).expect("queue init should succeed");
        lanes
            .enqueue("discord:channel:a", run("a-1"))
            .expect("enqueue a-1 should succeed");
        lanes
            .enqueue("discord:channel:b", run("b-1"))
            .expect("enqueue b-1 should succeed");
        lanes
            .enqueue("discord:channel:a", run("a-2"))
            .expect("enqueue a-2 should succeed");
        lanes
            .enqueue("discord:channel:b", run("b-2"))
            .expect("enqueue b-2 should succeed");

        assert_eq!(
            lanes
                .dequeue("discord:channel:a")
                .expect("dequeue a should succeed"),
            Some(run("a-1"))
        );
        assert_eq!(
            lanes
                .dequeue("discord:channel:b")
                .expect("dequeue b should succeed"),
            Some(run("b-1"))
        );
        assert_eq!(
            lanes
                .dequeue("discord:channel:a")
                .expect("dequeue a should succeed"),
            Some(run("a-2"))
        );
        assert_eq!(
            lanes
                .dequeue("discord:channel:b")
                .expect("dequeue b should succeed"),
            Some(run("b-2"))
        );
    }

    #[test]
    fn overflow_is_rejected() {
        let mut lanes = SessionLaneQueues::new(2).expect("queue init should succeed");
        lanes
            .enqueue("discord:channel:overflow", run("first"))
            .expect("first enqueue should succeed");
        lanes
            .enqueue("discord:channel:overflow", run("second"))
            .expect("second enqueue should succeed");

        let error = lanes
            .enqueue("discord:channel:overflow", run("third"))
            .expect_err("overflow enqueue should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "lane_queue_overflow",
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_session_id_for_enqueue_and_dequeue() {
        let mut lanes = SessionLaneQueues::new(2).expect("queue init should succeed");

        let enqueue_error = lanes
            .enqueue("  ", run("run-1"))
            .expect_err("empty session id enqueue should fail");
        assert!(matches!(
            enqueue_error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_session",
                ..
            }
        ));

        let dequeue_error = lanes
            .dequeue("")
            .expect_err("empty session id dequeue should fail");
        assert!(matches!(
            dequeue_error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_session",
                ..
            }
        ));

        let count_error = lanes
            .queued_count("\n")
            .expect_err("empty session id count should fail");
        assert!(matches!(
            count_error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_session",
                ..
            }
        ));
    }

    #[test]
    fn rejects_invalid_run_id() {
        let mut lanes = SessionLaneQueues::new(2).expect("queue init should succeed");

        let error = lanes
            .enqueue("discord:channel:x", run("  "))
            .expect_err("empty run id should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_run",
                ..
            }
        ));
    }

    #[test]
    fn dequeue_from_missing_lane_returns_none() {
        let mut lanes = SessionLaneQueues::new(2).expect("queue init should succeed");
        let popped = lanes
            .dequeue("discord:channel:missing")
            .expect("dequeue from missing lane should succeed");
        assert!(popped.is_none());
    }

    #[test]
    fn lane_and_total_counts_track_queue_contents() {
        let mut lanes = SessionLaneQueues::new(4).expect("queue init should succeed");
        assert_eq!(lanes.lane_count(), 0);
        assert_eq!(lanes.total_queued_count(), 0);
        assert_eq!(
            lanes
                .queued_count("discord:channel:a")
                .expect("missing lane count should succeed"),
            0
        );

        lanes
            .enqueue("discord:channel:a", run("a-1"))
            .expect("enqueue a-1 should succeed");
        lanes
            .enqueue("discord:channel:b", run("b-1"))
            .expect("enqueue b-1 should succeed");
        assert_eq!(lanes.lane_count(), 2);
        assert_eq!(lanes.total_queued_count(), 2);
        assert_eq!(
            lanes
                .queued_count("discord:channel:a")
                .expect("lane a count should succeed"),
            1
        );

        let _ = lanes
            .dequeue("discord:channel:a")
            .expect("dequeue a should succeed");
        assert_eq!(lanes.lane_count(), 1);
        assert_eq!(lanes.total_queued_count(), 1);
    }

    #[test]
    fn lane_scheduler_rejects_zero_max_concurrency() {
        let error = LaneScheduler::new(0, 4).expect_err("zero max_concurrent_lanes should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_MAX_CONCURRENT_LANES",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn lane_scheduler_propagates_queue_limit_validation() {
        let error =
            LaneScheduler::new(2, 0).expect_err("zero queue limit should fail scheduler init");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_LANE_QUEUE_LIMIT",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn lane_scheduler_dispatch_respects_global_cap() {
        let mut scheduler = LaneScheduler::new(1, 4).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("a-1"))
            .expect("enqueue a should succeed");
        scheduler
            .enqueue("discord:channel:b", run("b-1"))
            .expect("enqueue b should succeed");

        let first = scheduler
            .try_dispatch_next()
            .expect("first dispatch should return a run");
        assert_eq!(first.logical_session_id, "discord:channel:a");
        assert_eq!(first.run, run("a-1"));
        assert_eq!(scheduler.active_lane_count(), 1);
        assert_eq!(scheduler.total_queued_count(), 1);
        assert!(scheduler.try_dispatch_next().is_none());

        scheduler
            .complete_lane("discord:channel:a")
            .expect("complete active lane should succeed");
        let second = scheduler
            .try_dispatch_next()
            .expect("second dispatch should return a run");
        assert_eq!(second.logical_session_id, "discord:channel:b");
        assert_eq!(second.run, run("b-1"));
        assert!(scheduler.try_dispatch_next().is_none());
    }

    #[test]
    fn lane_scheduler_dispatch_skips_active_lanes() {
        let mut scheduler = LaneScheduler::new(2, 4).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("a-1"))
            .expect("enqueue a-1 should succeed");
        scheduler
            .enqueue("discord:channel:a", run("a-2"))
            .expect("enqueue a-2 should succeed");
        scheduler
            .enqueue("discord:channel:b", run("b-1"))
            .expect("enqueue b-1 should succeed");

        let first = scheduler
            .try_dispatch_next()
            .expect("first dispatch should return run");
        let second = scheduler
            .try_dispatch_next()
            .expect("second dispatch should return run");

        assert_eq!(first.logical_session_id, "discord:channel:a");
        assert_eq!(second.logical_session_id, "discord:channel:b");
        assert_eq!(scheduler.active_lane_count(), 2);
        assert_eq!(scheduler.total_queued_count(), 1);
    }

    #[test]
    fn lane_scheduler_complete_validates_and_requires_active_lane() {
        let mut scheduler = LaneScheduler::new(1, 1).expect("scheduler init should succeed");

        let invalid_session_error = scheduler
            .complete_lane(" ")
            .expect_err("invalid session id should fail");
        assert!(matches!(
            invalid_session_error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_session",
                ..
            }
        ));

        let inactive_error = scheduler
            .complete_lane("discord:channel:inactive")
            .expect_err("inactive lane completion should fail");
        assert!(matches!(
            inactive_error,
            CrabError::InvariantViolation {
                context: "lane_scheduler_complete",
                ..
            }
        ));
    }

    #[test]
    fn lane_scheduler_counts_track_queued_work() {
        let mut scheduler = LaneScheduler::new(2, 3).expect("scheduler init should succeed");
        assert_eq!(
            scheduler
                .queued_count("discord:channel:a")
                .expect("missing lane count should succeed"),
            0
        );
        assert_eq!(scheduler.total_queued_count(), 0);

        scheduler
            .enqueue("discord:channel:a", run("a-1"))
            .expect("enqueue should succeed");
        scheduler
            .enqueue("discord:channel:b", run("b-1"))
            .expect("enqueue should succeed");
        assert_eq!(
            scheduler
                .queued_count("discord:channel:a")
                .expect("lane a count should succeed"),
            1
        );
        assert_eq!(scheduler.total_queued_count(), 2);

        let _ = scheduler
            .try_dispatch_next()
            .expect("dispatch should return first run");
        assert_eq!(scheduler.total_queued_count(), 1);
    }

    #[test]
    fn lane_scheduler_dispatch_with_no_pending_work_returns_none() {
        let mut scheduler = LaneScheduler::new(2, 2).expect("scheduler init should succeed");
        assert!(scheduler.try_dispatch_next().is_none());

        scheduler
            .enqueue("discord:channel:a", run("a-1"))
            .expect("enqueue should succeed");
        let _ = scheduler
            .try_dispatch_next()
            .expect("dispatch should return queued run");
        scheduler
            .complete_lane("discord:channel:a")
            .expect("active lane completion should succeed");
        assert!(scheduler.try_dispatch_next().is_none());
    }

    fn run(id: &str) -> QueuedRun {
        QueuedRun {
            run_id: id.to_string(),
        }
    }
}

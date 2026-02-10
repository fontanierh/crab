//! Lane scheduler components for Crab.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crab_core::{CrabError, CrabResult, LaneState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedRun {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedRun {
    pub logical_session_id: String,
    pub run: QueuedRun,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRun {
    pub run_id: String,
    pub cancellation_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRunCancellation {
    pub run_id: String,
    pub already_cancelling: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledQueuedRun {
    pub logical_session_id: String,
    pub run_id: String,
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
                message: queue_overflow_reason(self.queue_limit),
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

    fn cancel_queued_run_by_id(&mut self, run_id: &str) -> CrabResult<Option<String>> {
        validate_run_id("lane_queue_cancel_queued", run_id)?;
        let mut matched_lane_id: Option<String> = None;

        for (lane_id, queue) in &self.queues {
            let match_count = queue
                .iter()
                .filter(|queued| queued.run_id == run_id)
                .count();
            if match_count == 0 {
                continue;
            }
            if match_count > 1 || matched_lane_id.is_some() {
                return Err(CrabError::InvariantViolation {
                    context: "lane_queue_cancel_queued",
                    message: format!("run_id {run_id} is queued more than once"),
                });
            }
            matched_lane_id = Some(lane_id.clone());
        }

        let Some(lane_id) = matched_lane_id else {
            return Ok(None);
        };

        let queue = self
            .queues
            .get_mut(&lane_id)
            .expect("matched lane should still exist");
        queue.retain(|queued| queued.run_id != run_id);

        if queue.is_empty() {
            let _ = self.queues.remove(&lane_id);
        }

        Ok(Some(lane_id))
    }
}

#[derive(Debug, Clone)]
pub struct LaneScheduler {
    queues: SessionLaneQueues,
    max_concurrent_lanes: usize,
    active_lanes: BTreeMap<String, ActiveRun>,
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
            active_lanes: BTreeMap::new(),
        })
    }

    pub fn enqueue(&mut self, logical_session_id: &str, run: QueuedRun) -> CrabResult<()> {
        self.queues.enqueue(logical_session_id, run)
    }

    pub fn try_dispatch_next(&mut self) -> Option<DispatchedRun> {
        if self.active_lanes.len() >= self.max_concurrent_lanes {
            return None;
        }

        let active_lane_ids: BTreeSet<String> = self.active_lanes.keys().cloned().collect();
        let lane_id = self
            .queues
            .first_dispatchable_lane_id_excluding(&active_lane_ids)?;
        let run = self.queues.pop_front_from_known_non_empty(&lane_id);
        self.active_lanes.insert(
            lane_id.clone(),
            ActiveRun {
                run_id: run.run_id.clone(),
                cancellation_requested: false,
            },
        );
        Some(DispatchedRun {
            logical_session_id: lane_id,
            run,
        })
    }

    pub fn complete_lane(&mut self, logical_session_id: &str) -> CrabResult<()> {
        validate_session_id(logical_session_id)?;
        if self.active_lanes.remove(logical_session_id).is_none() {
            return Err(CrabError::InvariantViolation {
                context: "lane_scheduler_complete",
                message: format!("lane {logical_session_id} is not active"),
            });
        }
        Ok(())
    }

    pub fn active_run(&self, logical_session_id: &str) -> CrabResult<Option<ActiveRun>> {
        validate_session_id(logical_session_id)?;
        Ok(self.active_lanes.get(logical_session_id).cloned())
    }

    pub fn request_cancel_active_run(
        &mut self,
        logical_session_id: &str,
    ) -> CrabResult<ActiveRunCancellation> {
        validate_session_id(logical_session_id)?;
        let Some(active_run) = self.active_lanes.get_mut(logical_session_id) else {
            return Err(CrabError::InvariantViolation {
                context: "lane_scheduler_cancel_active",
                message: format!("lane {logical_session_id} is not active"),
            });
        };

        let already_cancelling = active_run.cancellation_requested;
        active_run.cancellation_requested = true;
        Ok(ActiveRunCancellation {
            run_id: active_run.run_id.clone(),
            already_cancelling,
        })
    }

    pub fn cancel_queued_run_by_id(
        &mut self,
        run_id: &str,
    ) -> CrabResult<Option<CancelledQueuedRun>> {
        validate_run_id("lane_scheduler_cancel_queued", run_id)?;

        if self
            .active_lanes
            .values()
            .any(|active_run| active_run.run_id == run_id)
        {
            return Err(CrabError::InvariantViolation {
                context: "lane_scheduler_cancel_queued",
                message: format!(
                    "run_id {run_id} is currently active and cannot be cancelled as queued"
                ),
            });
        }

        let Some(logical_session_id) = self.queues.cancel_queued_run_by_id(run_id)? else {
            return Ok(None);
        };
        Ok(Some(CancelledQueuedRun {
            logical_session_id,
            run_id: run_id.to_string(),
        }))
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

#[derive(Debug, Clone)]
pub struct LaneStateMachine {
    state: LaneState,
}

impl Default for LaneStateMachine {
    fn default() -> Self {
        Self {
            state: LaneState::Idle,
        }
    }
}

impl LaneStateMachine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_state(state: LaneState) -> Self {
        Self { state }
    }

    #[must_use]
    pub fn state(&self) -> LaneState {
        self.state
    }

    pub fn transition_to(&mut self, next: LaneState) -> CrabResult<()> {
        if self.state == next {
            return Ok(());
        }

        if !is_valid_lane_transition(self.state, next) {
            return Err(CrabError::InvariantViolation {
                context: "lane_state_transition",
                message: format!("invalid transition: {:?} -> {:?}", self.state, next),
            });
        }

        self.state = next;
        Ok(())
    }
}

fn is_valid_lane_transition(current: LaneState, next: LaneState) -> bool {
    matches!(
        (current, next),
        (LaneState::Idle, LaneState::Running | LaneState::Rotating)
            | (
                LaneState::Running,
                LaneState::Idle | LaneState::Cancelling | LaneState::Rotating
            )
            | (LaneState::Cancelling, LaneState::Idle | LaneState::Rotating)
            | (LaneState::Rotating, LaneState::Idle)
    )
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
    validate_run_id("lane_queue_validate_run", &run.run_id)
}

fn validate_run_id(context: &'static str, run_id: &str) -> CrabResult<()> {
    if run_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: "run_id must not be empty".to_string(),
        });
    }
    Ok(())
}

fn queue_overflow_reason(queue_limit: usize) -> String {
    format!(
        "Queue is full for this session (limit={queue_limit}). Please wait for in-flight runs to complete before retrying."
    )
}

#[cfg(test)]
mod tests {
    use crab_core::{CrabError, LaneState};

    use super::{ActiveRun, LaneScheduler, LaneStateMachine, QueuedRun, SessionLaneQueues};

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
                message,
            } if message == super::queue_overflow_reason(2)
        ));
    }

    #[test]
    fn lane_scheduler_overflow_rejection_has_user_facing_reason() {
        let mut scheduler = LaneScheduler::new(2, 1).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:overflow", run("first"))
            .expect("first enqueue should succeed");
        let error = scheduler
            .enqueue("discord:channel:overflow", run("second"))
            .expect_err("overflow enqueue should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "lane_queue_overflow",
                message,
            } if message == super::queue_overflow_reason(1)
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

    #[test]
    fn request_cancel_active_run_marks_run_as_cancelling() {
        let mut scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("run-a"))
            .expect("enqueue should succeed");
        let _ = scheduler
            .try_dispatch_next()
            .expect("dispatch should return queued run");

        let first = scheduler
            .request_cancel_active_run("discord:channel:a")
            .expect("active run cancellation request should succeed");
        assert_eq!(first.run_id, "run-a");
        assert!(!first.already_cancelling);
        assert_eq!(
            scheduler
                .active_run("discord:channel:a")
                .expect("active run lookup should succeed"),
            Some(ActiveRun {
                run_id: "run-a".to_string(),
                cancellation_requested: true,
            })
        );

        let second = scheduler
            .request_cancel_active_run("discord:channel:a")
            .expect("duplicate cancellation request should be idempotent");
        assert_eq!(second.run_id, "run-a");
        assert!(second.already_cancelling);
    }

    #[test]
    fn request_cancel_active_run_validates_and_requires_active_lane() {
        let mut scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");

        let invalid_error = scheduler
            .request_cancel_active_run(" ")
            .expect_err("blank session id should fail");
        assert!(matches!(
            invalid_error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_session",
                ..
            }
        ));

        let missing_error = scheduler
            .request_cancel_active_run("discord:channel:missing")
            .expect_err("cancelling missing lane should fail");
        assert!(matches!(
            missing_error,
            CrabError::InvariantViolation {
                context: "lane_scheduler_cancel_active",
                ..
            }
        ));
    }

    #[test]
    fn active_run_lookup_reports_none_for_inactive_lane() {
        let scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");
        assert_eq!(
            scheduler
                .active_run("discord:channel:a")
                .expect("active run lookup should validate"),
            None
        );
    }

    #[test]
    fn active_run_lookup_validates_session_id() {
        let scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");
        let error = scheduler
            .active_run(" ")
            .expect_err("blank session id should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "lane_queue_validate_session",
                ..
            }
        ));
    }

    #[test]
    fn cancel_queued_run_by_id_removes_pending_run_and_preserves_fifo() {
        let mut scheduler = LaneScheduler::new(2, 4).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("run-a1"))
            .expect("enqueue should succeed");
        scheduler
            .enqueue("discord:channel:a", run("run-a2"))
            .expect("enqueue should succeed");
        scheduler
            .enqueue("discord:channel:b", run("run-b1"))
            .expect("enqueue should succeed");

        let cancelled = scheduler
            .cancel_queued_run_by_id("run-a2")
            .expect("queued cancellation should succeed");
        assert_eq!(
            cancelled,
            Some(super::CancelledQueuedRun {
                logical_session_id: "discord:channel:a".to_string(),
                run_id: "run-a2".to_string(),
            })
        );
        assert_eq!(
            scheduler
                .queued_count("discord:channel:a")
                .expect("lane count should succeed"),
            1
        );
        assert_eq!(scheduler.total_queued_count(), 2);

        let first = scheduler
            .try_dispatch_next()
            .expect("dispatch should return remaining lane a run");
        let second = scheduler
            .try_dispatch_next()
            .expect("dispatch should return lane b run");
        assert_eq!(first.logical_session_id, "discord:channel:a");
        assert_eq!(first.run, run("run-a1"));
        assert_eq!(second.logical_session_id, "discord:channel:b");
        assert_eq!(second.run, run("run-b1"));
    }

    #[test]
    fn cancel_queued_run_by_id_returns_none_when_missing() {
        let mut scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("run-a1"))
            .expect("enqueue should succeed");

        let cancelled = scheduler
            .cancel_queued_run_by_id("run-missing")
            .expect("missing queued cancellation should succeed");
        assert_eq!(cancelled, None);
        assert_eq!(scheduler.total_queued_count(), 1);
    }

    #[test]
    fn cancel_queued_run_by_id_removes_empty_lane_entry() {
        let mut scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:solo", run("run-solo"))
            .expect("enqueue should succeed");

        let cancelled = scheduler
            .cancel_queued_run_by_id("run-solo")
            .expect("queued cancellation should succeed");
        assert_eq!(
            cancelled,
            Some(super::CancelledQueuedRun {
                logical_session_id: "discord:channel:solo".to_string(),
                run_id: "run-solo".to_string(),
            })
        );
        assert_eq!(scheduler.total_queued_count(), 0);
        assert_eq!(
            scheduler
                .queued_count("discord:channel:solo")
                .expect("missing lane count should be 0"),
            0
        );
    }

    #[test]
    fn cancel_queued_run_by_id_rejects_active_run_and_blank_input() {
        let mut scheduler = LaneScheduler::new(1, 2).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("run-a1"))
            .expect("enqueue should succeed");
        let _ = scheduler
            .try_dispatch_next()
            .expect("dispatch should return queued run");

        let active_error = scheduler
            .cancel_queued_run_by_id("run-a1")
            .expect_err("active run cannot be cancelled as queued");
        assert!(matches!(
            active_error,
            CrabError::InvariantViolation {
                context: "lane_scheduler_cancel_queued",
                ..
            }
        ));

        let blank_error = scheduler
            .cancel_queued_run_by_id(" ")
            .expect_err("blank run_id should fail validation");
        assert!(matches!(
            blank_error,
            CrabError::InvariantViolation {
                context: "lane_scheduler_cancel_queued",
                ..
            }
        ));
    }

    #[test]
    fn cancel_queued_run_by_id_rejects_ambiguous_duplicate_queue_ids() {
        let mut scheduler = LaneScheduler::new(2, 3).expect("scheduler init should succeed");
        scheduler
            .enqueue("discord:channel:a", run("run-dup"))
            .expect("enqueue should succeed");
        scheduler
            .enqueue("discord:channel:b", run("run-dup"))
            .expect("enqueue should succeed");

        let error = scheduler
            .cancel_queued_run_by_id("run-dup")
            .expect_err("duplicate queued run ids should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "lane_queue_cancel_queued",
                ..
            }
        ));
    }

    #[test]
    fn session_lane_cancel_queued_validates_run_id() {
        let mut lanes = SessionLaneQueues::new(2).expect("queue init should succeed");
        let error = lanes
            .cancel_queued_run_by_id(" ")
            .expect_err("blank run_id should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "lane_queue_cancel_queued",
                ..
            }
        ));
    }

    #[test]
    fn lane_state_machine_defaults_to_idle() {
        let machine = LaneStateMachine::new();
        assert_eq!(machine.state(), LaneState::Idle);
    }

    #[test]
    fn lane_state_machine_accepts_valid_transitions() {
        let mut machine = LaneStateMachine::new();
        machine
            .transition_to(LaneState::Running)
            .expect("idle -> running should be valid");
        machine
            .transition_to(LaneState::Cancelling)
            .expect("running -> cancelling should be valid");
        machine
            .transition_to(LaneState::Rotating)
            .expect("cancelling -> rotating should be valid");
        machine
            .transition_to(LaneState::Idle)
            .expect("rotating -> idle should be valid");
        machine
            .transition_to(LaneState::Rotating)
            .expect("idle -> rotating should be valid");
        machine
            .transition_to(LaneState::Idle)
            .expect("rotating -> idle should be valid");

        let mut from_running = LaneStateMachine::with_state(LaneState::Running);
        from_running
            .transition_to(LaneState::Idle)
            .expect("running -> idle should be valid");
        from_running
            .transition_to(LaneState::Running)
            .expect("idle -> running should be valid");
        from_running
            .transition_to(LaneState::Rotating)
            .expect("running -> rotating should be valid");
        from_running
            .transition_to(LaneState::Idle)
            .expect("rotating -> idle should be valid");

        let mut cancelling = LaneStateMachine::with_state(LaneState::Cancelling);
        cancelling
            .transition_to(LaneState::Idle)
            .expect("cancelling -> idle should be valid");
    }

    #[test]
    fn lane_state_machine_rejects_invalid_transitions() {
        let mut idle = LaneStateMachine::new();
        let idle_error = idle
            .transition_to(LaneState::Cancelling)
            .expect_err("idle -> cancelling should be invalid");
        assert!(matches!(
            idle_error,
            CrabError::InvariantViolation {
                context: "lane_state_transition",
                ..
            }
        ));

        let mut rotating = LaneStateMachine::with_state(LaneState::Rotating);
        let rotating_error = rotating
            .transition_to(LaneState::Running)
            .expect_err("rotating -> running should be invalid");
        assert!(matches!(
            rotating_error,
            CrabError::InvariantViolation {
                context: "lane_state_transition",
                ..
            }
        ));
    }

    #[test]
    fn lane_state_machine_allows_idempotent_transitions() {
        let mut machine = LaneStateMachine::with_state(LaneState::Running);
        machine
            .transition_to(LaneState::Running)
            .expect("same-state transition should be a no-op");
        assert_eq!(machine.state(), LaneState::Running);
    }

    fn run(id: &str) -> QueuedRun {
        QueuedRun {
            run_id: id.to_string(),
        }
    }
}

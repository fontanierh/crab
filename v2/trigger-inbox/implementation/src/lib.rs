mod contract;
mod store;

pub use contract::*;

use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use store::TriggerStore;

type Clock = Arc<dyn Fn() -> Result<u64, TriggerInboxError> + Send + Sync>;

/// Durable, transactional inbox shared by bridges, schedules, self-work and operators.
pub struct TriggerInbox {
    store: TriggerStore,
    clock: Clock,
}

impl TriggerInbox {
    /// Open or create a file-backed inbox.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TriggerInboxError> {
        Self::with_store(TriggerStore::open(path)?)
    }

    /// Open an ephemeral inbox. Production callers should use [`Self::open`].
    pub fn open_in_memory() -> Result<Self, TriggerInboxError> {
        Self::with_store(TriggerStore::open_in_memory()?)
    }

    fn with_store(store: TriggerStore) -> Result<Self, TriggerInboxError> {
        Ok(Self {
            store,
            clock: Arc::new(system_time_ms),
        })
    }

    #[cfg(test)]
    fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }
}

#[boxology::implementation]
impl TriggerInbox {
    pub async fn enqueue(
        &self,
        context: boxology::CallContext,
        request: EnqueueTrigger,
    ) -> Result<TriggerReceipt, TriggerInboxError> {
        let _ = context;
        self.store.enqueue(request, (self.clock)()?)
    }

    pub async fn claim(
        &self,
        context: boxology::CallContext,
        request: ClaimTriggers,
    ) -> Result<TriggerBatch, TriggerInboxError> {
        let _ = context;
        self.store.claim(request)
    }

    pub async fn extend_lease(
        &self,
        context: boxology::CallContext,
        request: ExtendLease,
    ) -> Result<TriggerLease, TriggerInboxError> {
        let _ = context;
        self.store.extend_lease(request)
    }

    pub async fn settle(
        &self,
        context: boxology::CallContext,
        request: SettleTrigger,
    ) -> Result<TriggerReceipt, TriggerInboxError> {
        let _ = context;
        self.store.settle(request)
    }

    pub async fn inspect(
        &self,
        context: boxology::CallContext,
        request: TriggerReference,
    ) -> Result<TriggerRecord, TriggerInboxError> {
        let _ = context;
        self.store.inspect(request)
    }
}

fn system_time_ms() -> Result<u64, TriggerInboxError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TriggerInboxError::StorageUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| TriggerInboxError::StorageUnavailable)
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};
    use tempfile::TempDir;

    use super::*;
    use crate::store::{
        MAX_SETTLEMENT_DETAIL_BYTES, MAX_TRIGGER_ATTACHMENTS, MAX_TRIGGER_MESSAGE_BYTES,
    };

    struct Fixture {
        directory: TempDir,
        now: Arc<AtomicU64>,
    }

    impl Fixture {
        fn new(now_ms: u64) -> Self {
            Self {
                directory: tempfile::tempdir().expect("temporary directory is created"),
                now: Arc::new(AtomicU64::new(now_ms)),
            }
        }

        fn open(&self) -> TriggerInbox {
            let now = self.now.clone();
            TriggerInbox::open(self.directory.path().join("triggers.sqlite"))
                .expect("inbox opens")
                .with_clock(Arc::new(move || Ok(now.load(Ordering::SeqCst))))
        }

        fn set_now(&self, now_ms: u64) {
            self.now.store(now_ms, Ordering::SeqCst);
        }
    }

    fn context() -> CallContext {
        CallContext::new(
            Caller::Anonymous,
            None,
            CancelToken::new(),
            TraceContext::empty(),
            None,
        )
    }

    fn trigger(key: &str, not_before_ms: u64) -> EnqueueTrigger {
        EnqueueTrigger {
            source: TriggerSource::Bridge,
            source_id: "whatsapp".into(),
            deduplication_key: key.into(),
            target_channel_id: "jim".into(),
            lane: "primary".into(),
            mode: TriggerMode::Queue,
            not_before_ms,
            message_json: format!(r#"{{"text":"{key}"}}"#),
            attachments: vec![TriggerAttachment {
                media_type: "text/plain".into(),
                name: Some(format!("{key}.txt")),
                content_handle: format!("content:{key}"),
            }],
        }
    }

    async fn enqueue(inbox: &TriggerInbox, key: &str, not_before_ms: u64) -> TriggerReceipt {
        inbox
            .enqueue(context(), trigger(key, not_before_ms))
            .await
            .expect("trigger enqueues")
    }

    async fn claim(inbox: &TriggerInbox, now_ms: u64, lease_duration_ms: u64) -> TriggerBatch {
        inbox
            .claim(
                context(),
                ClaimTriggers {
                    worker_id: "worker-1".into(),
                    lane: "primary".into(),
                    limit: 16,
                    lease_duration_ms,
                    now_ms,
                },
            )
            .await
            .expect("claim succeeds")
    }

    #[test]
    fn contract_declares_the_small_durable_queue_protocol() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            ["enqueue", "claim", "extend_lease", "settle", "inspect"]
        );
    }

    #[tokio::test]
    async fn enqueue_survives_restart_and_distinguishes_retries_from_conflicts() {
        let fixture = Fixture::new(100);
        let first = {
            let inbox = fixture.open();
            enqueue(&inbox, "event-1", 100).await
        };
        assert_eq!(first.state, TriggerState::Pending);
        assert!(!first.deduplicated);
        assert_eq!(first.recorded_at_ms, 100);

        fixture.set_now(999);
        let inbox = fixture.open();
        let record = inbox
            .inspect(
                context(),
                TriggerReference {
                    trigger_id: first.trigger_id.clone(),
                },
            )
            .await
            .expect("persisted trigger is readable");
        assert_eq!(record.message_json, r#"{"text":"event-1"}"#);
        assert_eq!(record.enqueued_at_ms, 100);

        let duplicate = enqueue(&inbox, "event-1", 100).await;
        assert_eq!(duplicate.trigger_id, first.trigger_id);
        assert_eq!(duplicate.recorded_at_ms, 100);
        assert!(duplicate.deduplicated);

        let mut conflict = trigger("event-1", 100);
        conflict.message_json = r#"{"text":"different"}"#.into();
        assert_eq!(
            inbox.enqueue(context(), conflict).await,
            Err(TriggerInboxError::DuplicateKeyConflict)
        );
    }

    #[tokio::test]
    async fn claim_preserves_lane_fifo_and_recovers_expired_leases() {
        let fixture = Fixture::new(100);
        let inbox = fixture.open();
        enqueue(&inbox, "delayed-head", 200).await;
        enqueue(&inbox, "ready-tail", 100).await;

        assert!(claim(&inbox, 150, 50).await.leases.is_empty());
        let first_batch = claim(&inbox, 200, 50).await;
        assert_eq!(first_batch.leases.len(), 2);
        assert_eq!(
            first_batch
                .leases
                .iter()
                .map(|lease| lease.trigger.deduplication_key.as_str())
                .collect::<Vec<_>>(),
            ["delayed-head", "ready-tail"]
        );
        assert!(
            first_batch
                .leases
                .iter()
                .all(|lease| lease.trigger.attempt == 1)
        );

        drop(inbox);
        let reopened = fixture.open();
        assert!(claim(&reopened, 249, 50).await.leases.is_empty());
        let recovered = claim(&reopened, 250, 50).await;
        assert_eq!(recovered.leases.len(), 2);
        assert_eq!(recovered.leases[0].trigger.attempt, 2);
        assert_ne!(
            recovered.leases[0].lease_token,
            first_batch.leases[0].lease_token
        );
    }

    #[tokio::test]
    async fn extend_retry_and_complete_require_the_current_unexpired_lease() {
        let fixture = Fixture::new(100);
        let inbox = fixture.open();
        let receipt = enqueue(&inbox, "retry-me", 100).await;
        let lease = claim(&inbox, 100, 50).await.leases.remove(0);

        assert_eq!(
            inbox
                .extend_lease(
                    context(),
                    ExtendLease {
                        trigger_id: receipt.trigger_id.clone(),
                        lease_token: "wrong".into(),
                        extend_by_ms: 30,
                        now_ms: 120,
                    },
                )
                .await,
            Err(TriggerInboxError::LeaseMismatch)
        );

        let extended = inbox
            .extend_lease(
                context(),
                ExtendLease {
                    trigger_id: receipt.trigger_id.clone(),
                    lease_token: lease.lease_token.clone(),
                    extend_by_ms: 30,
                    now_ms: 120,
                },
            )
            .await
            .expect("lease extends");
        assert_eq!(extended.expires_at_ms, 180);

        let retry = inbox
            .settle(
                context(),
                SettleTrigger {
                    trigger_id: receipt.trigger_id.clone(),
                    lease_token: lease.lease_token,
                    outcome: SettlementOutcome::Retry,
                    detail: Some("transient".into()),
                    retry_not_before_ms: Some(220),
                    settled_at_ms: 130,
                },
            )
            .await
            .expect("retry settles");
        assert_eq!(retry.state, TriggerState::RetryScheduled);
        assert!(claim(&inbox, 219, 50).await.leases.is_empty());

        let second_lease = claim(&inbox, 220, 50).await.leases.remove(0);
        assert_eq!(second_lease.trigger.attempt, 2);
        let completed = inbox
            .settle(
                context(),
                SettleTrigger {
                    trigger_id: receipt.trigger_id.clone(),
                    lease_token: second_lease.lease_token,
                    outcome: SettlementOutcome::Completed,
                    detail: None,
                    retry_not_before_ms: None,
                    settled_at_ms: 221,
                },
            )
            .await
            .expect("completion settles");
        assert_eq!(completed.state, TriggerState::Completed);
        assert_eq!(
            inbox
                .inspect(
                    context(),
                    TriggerReference {
                        trigger_id: receipt.trigger_id,
                    },
                )
                .await
                .expect("record remains inspectable")
                .state,
            TriggerState::Completed
        );
    }

    #[tokio::test]
    async fn invalid_inputs_and_expired_settlement_fail_explicitly() {
        let inbox = TriggerInbox::open_in_memory().expect("in-memory inbox opens");
        let mut invalid = trigger("invalid", 0);
        invalid.message_json = "not-json".into();
        assert_eq!(
            inbox.enqueue(context(), invalid).await,
            Err(TriggerInboxError::InvalidPayload)
        );

        let receipt = inbox
            .enqueue(context(), trigger("expires", 0))
            .await
            .expect("valid trigger enqueues");
        let lease = claim(&inbox, 10, 10).await.leases.remove(0);
        assert_eq!(
            inbox
                .settle(
                    context(),
                    SettleTrigger {
                        trigger_id: receipt.trigger_id,
                        lease_token: lease.lease_token,
                        outcome: SettlementOutcome::Completed,
                        detail: None,
                        retry_not_before_ms: None,
                        settled_at_ms: 20,
                    },
                )
                .await,
            Err(TriggerInboxError::LeaseExpired)
        );
    }

    #[tokio::test]
    async fn durable_envelope_limits_reject_oversized_inputs_before_persistence() {
        let exact = TriggerInbox::open_in_memory().expect("exact-boundary inbox opens");
        let mut exact_message = trigger("exact-message", 0);
        let prefix = r#"{"text":""#;
        let suffix = r#""}"#;
        exact_message.message_json = format!(
            "{prefix}{}{suffix}",
            "x".repeat(MAX_TRIGGER_MESSAGE_BYTES - prefix.len() - suffix.len())
        );
        assert_eq!(exact_message.message_json.len(), MAX_TRIGGER_MESSAGE_BYTES);
        exact
            .enqueue(context(), exact_message)
            .await
            .expect("exact message boundary persists");

        let mut exact_attachments = trigger("exact-attachments", 0);
        exact_attachments.attachments = (0..MAX_TRIGGER_ATTACHMENTS)
            .map(|index| TriggerAttachment {
                media_type: "text/plain".into(),
                name: Some(format!("{index}.txt")),
                content_handle: format!("content:{index}"),
            })
            .collect();
        exact
            .enqueue(context(), exact_attachments)
            .await
            .expect("exact attachment boundary persists");

        let inbox = TriggerInbox::open_in_memory().expect("in-memory inbox opens");

        let mut message = trigger("large-message", 0);
        message.message_json = format!(r#"{{"text":"{}"}}"#, "x".repeat(MAX_TRIGGER_MESSAGE_BYTES));
        assert_eq!(
            inbox.enqueue(context(), message).await,
            Err(TriggerInboxError::InvalidPayload)
        );

        let mut attachments = trigger("many-attachments", 0);
        attachments.attachments = (0..=MAX_TRIGGER_ATTACHMENTS)
            .map(|index| TriggerAttachment {
                media_type: "text/plain".into(),
                name: Some(format!("{index}.txt")),
                content_handle: format!("content:{index}"),
            })
            .collect();
        assert_eq!(
            inbox.enqueue(context(), attachments).await,
            Err(TriggerInboxError::InvalidPayload)
        );

        let mut source = trigger("large-source", 0);
        source.source_id = "x".repeat(513);
        assert_eq!(
            inbox.enqueue(context(), source).await,
            Err(TriggerInboxError::InvalidSource)
        );
        assert_eq!(
            inbox
                .claim(
                    context(),
                    ClaimTriggers {
                        worker_id: "worker-1".into(),
                        lane: "primary".into(),
                        limit: 0,
                        lease_duration_ms: 10,
                        now_ms: 0,
                    },
                )
                .await,
            Err(TriggerInboxError::InvalidClaim)
        );

        let receipt = enqueue(&inbox, "settlement", 0).await;
        let lease = claim(&inbox, 0, 10).await.leases.remove(0);
        assert_eq!(
            inbox
                .settle(
                    context(),
                    SettleTrigger {
                        trigger_id: receipt.trigger_id.clone(),
                        lease_token: lease.lease_token,
                        outcome: SettlementOutcome::DeadLetter,
                        detail: Some("x".repeat(MAX_SETTLEMENT_DETAIL_BYTES + 1)),
                        retry_not_before_ms: None,
                        settled_at_ms: 1,
                    },
                )
                .await,
            Err(TriggerInboxError::InvalidSettlement)
        );
        assert_eq!(
            inbox
                .inspect(
                    context(),
                    TriggerReference {
                        trigger_id: receipt.trigger_id,
                    },
                )
                .await
                .expect("rejected settlement does not mutate the trigger")
                .state,
            TriggerState::Leased
        );
    }
}

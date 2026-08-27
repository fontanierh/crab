mod contract;
mod store;

pub use contract::*;

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::{SystemTime, UNIX_EPOCH},
};

use boxology_contract::{CallContext, ErasedCallError};
use boxology_import_native_channel::{ChannelInputMode, ChannelTurn, InterruptingTurnRequest};
use boxology_import_trigger_inbox::{
    ClaimTriggers, SettleTrigger, SettlementOutcome, TriggerAttachment, TriggerLease, TriggerMode,
    TriggerSource,
};
use generated::{NativeChannelImport, TriggerInboxImport};
use serde_json::{Map, Value, json};
use store::RouteStore;
use tokio::sync::{Mutex, OwnedMutexGuard};

const MAX_DRAIN_LIMIT: u64 = 1_000;

type Clock = Arc<dyn Fn() -> Result<u64, TurnRouterError> + Send + Sync>;

#[derive(Default)]
struct LaneLocks {
    locks: StdMutex<HashMap<String, Weak<Mutex<()>>>>,
}

impl LaneLocks {
    async fn lock(&self, lane: &str) -> Result<OwnedMutexGuard<()>, TurnRouterError> {
        let lock = {
            let mut locks = self
                .locks
                .lock()
                .map_err(|_| TurnRouterError::StorageUnavailable)?;
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(lane).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(lane.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        Ok(lock.lock_owned().await)
    }

    #[cfg(test)]
    fn registry_len(&self) -> Result<usize, TurnRouterError> {
        self.locks
            .lock()
            .map(|locks| locks.len())
            .map_err(|_| TurnRouterError::StorageUnavailable)
    }
}

/// Opened durable route state waiting for composition-owned imports.
pub struct TurnRouterState {
    store: RouteStore,
}

impl TurnRouterState {
    /// Open file-backed route state before assembling the Boxology graph.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, TurnRouterError> {
        Ok(Self {
            store: RouteStore::open(path)?,
        })
    }

    /// Open ephemeral route state before assembling the Boxology graph.
    pub fn open_in_memory() -> Result<Self, TurnRouterError> {
        Ok(Self {
            store: RouteStore::open_in_memory()?,
        })
    }

    /// Attach the composition-selected trigger inbox and native channel.
    #[must_use]
    pub fn connect(
        self,
        trigger_inbox: TriggerInboxImport,
        native_channel: NativeChannelImport,
    ) -> TurnRouter {
        TurnRouter {
            trigger_inbox: Arc::new(trigger_inbox),
            native_channel: Arc::new(native_channel),
            store: Arc::new(self.store),
            lane_locks: Arc::new(LaneLocks::default()),
            clock: Arc::new(system_time_ms),
        }
    }
}

/// Durable bridge/scheduler/self-work ingress router over generated Boxology imports.
pub struct TurnRouter {
    trigger_inbox: Arc<TriggerInboxImport>,
    native_channel: Arc<NativeChannelImport>,
    store: Arc<RouteStore>,
    lane_locks: Arc<LaneLocks>,
    clock: Clock,
}

impl TurnRouter {
    /// Open file-backed route state and attach generated imports.
    pub fn open(
        path: impl AsRef<Path>,
        trigger_inbox: TriggerInboxImport,
        native_channel: NativeChannelImport,
    ) -> Result<Self, TurnRouterError> {
        Ok(TurnRouterState::open(path)?.connect(trigger_inbox, native_channel))
    }

    /// Open in-memory route state while retaining real imported dispatch.
    pub fn open_in_memory(
        trigger_inbox: TriggerInboxImport,
        native_channel: NativeChannelImport,
    ) -> Result<Self, TurnRouterError> {
        Ok(TurnRouterState::open_in_memory()?.connect(trigger_inbox, native_channel))
    }

    async fn route_lease(
        &self,
        context: CallContext,
        lease: TriggerLease,
        request: &DrainLaneRequest,
    ) -> Result<RoutedTrigger, TurnRouterError> {
        let route = match self.store.resolve(&lease.trigger.target_channel_id) {
            Ok(route) if route.lane == lease.trigger.lane => route,
            Ok(_) | Err(TurnRouterError::UnknownRoute) => {
                return self
                    .settle(
                        context,
                        &lease,
                        None,
                        SettlementOutcome::DeadLetter,
                        RoutedTriggerOutcome::DeadLettered,
                        Some("unknown_route"),
                        None,
                    )
                    .await;
            }
            Err(error) => return Err(error),
        };
        let prompt = match trigger_prompt_json(&lease) {
            Ok(prompt) => prompt,
            Err(TurnRouterError::InvalidTriggerPayload) => {
                return self
                    .settle(
                        context,
                        &lease,
                        Some(&route.binding_id),
                        SettlementOutcome::DeadLetter,
                        RoutedTriggerOutcome::DeadLettered,
                        Some("invalid_trigger_payload"),
                        None,
                    )
                    .await;
            }
            Err(error) => return Err(error),
        };

        let delivery = self.deliver(context.clone(), &route, &lease, prompt).await;
        match delivery {
            Ok(()) => {
                self.settle(
                    context,
                    &lease,
                    Some(&route.binding_id),
                    SettlementOutcome::Completed,
                    RoutedTriggerOutcome::Completed,
                    None,
                    None,
                )
                .await
            }
            Err(failure) => {
                let exhausted = lease.trigger.attempt >= request.max_attempts;
                if failure.permanent || exhausted {
                    self.settle(
                        context,
                        &lease,
                        Some(&route.binding_id),
                        SettlementOutcome::DeadLetter,
                        RoutedTriggerOutcome::DeadLettered,
                        Some(failure.tag),
                        None,
                    )
                    .await
                } else {
                    let retry_at = (self.clock)()?.saturating_add(request.retry_delay_ms);
                    self.settle(
                        context,
                        &lease,
                        Some(&route.binding_id),
                        SettlementOutcome::Retry,
                        RoutedTriggerOutcome::RetryScheduled,
                        Some(failure.tag),
                        Some(retry_at),
                    )
                    .await
                }
            }
        }
    }

    async fn deliver(
        &self,
        context: CallContext,
        route: &ChannelRoute,
        lease: &TriggerLease,
        native_prompt_json: String,
    ) -> Result<(), ChannelFailure> {
        if matches!(lease.trigger.mode, TriggerMode::InterruptAndSteer) {
            self.native_channel
                .accept_interrupting_turn(
                    context,
                    InterruptingTurnRequest {
                        turn: ChannelTurn {
                            binding_id: route.binding_id.clone(),
                            client_turn_id: lease.trigger.trigger_id.clone(),
                            received_at_ms: lease.trigger.enqueued_at_ms,
                            mode: ChannelInputMode::Queue,
                            native_prompt_json,
                        },
                        reason: format!("trigger:{}", lease.trigger.trigger_id),
                    },
                )
                .await
                .map_err(classify_channel_error)?;
            return Ok(());
        }
        let mode = match lease.trigger.mode {
            TriggerMode::Queue => ChannelInputMode::Queue,
            TriggerMode::Steer => ChannelInputMode::Steer,
            TriggerMode::InterruptAndSteer => unreachable!("handled before mode mapping"),
            TriggerMode::Unknown { .. } => {
                return Err(ChannelFailure::permanent("invalid_trigger_mode"));
            }
        };
        self.native_channel
            .accept_turn(
                context,
                ChannelTurn {
                    binding_id: route.binding_id.clone(),
                    client_turn_id: lease.trigger.trigger_id.clone(),
                    received_at_ms: lease.trigger.enqueued_at_ms,
                    mode,
                    native_prompt_json,
                },
            )
            .await
            .map_err(classify_channel_error)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn settle(
        &self,
        context: CallContext,
        lease: &TriggerLease,
        binding_id: Option<&str>,
        settlement: SettlementOutcome,
        outcome: RoutedTriggerOutcome,
        detail: Option<&str>,
        retry_not_before_ms: Option<u64>,
    ) -> Result<RoutedTrigger, TurnRouterError> {
        self.trigger_inbox
            .settle(
                context,
                SettleTrigger {
                    trigger_id: lease.trigger.trigger_id.clone(),
                    lease_token: lease.lease_token.clone(),
                    outcome: settlement,
                    detail: detail.map(str::to_owned),
                    retry_not_before_ms,
                    settled_at_ms: (self.clock)()?,
                },
            )
            .await
            .map_err(|_| TurnRouterError::TriggerInboxUnavailable)?;
        Ok(RoutedTrigger {
            trigger_id: lease.trigger.trigger_id.clone(),
            binding_id: binding_id.map(str::to_owned),
            outcome,
            detail: detail.map(str::to_owned),
        })
    }
}

#[boxology::implementation]
impl TurnRouter {
    pub async fn put_route(
        &self,
        context: CallContext,
        request: PutRouteRequest,
    ) -> Result<ChannelRoute, TurnRouterError> {
        let _ = context;
        validate_route(&request)?;
        self.store.put(&request, (self.clock)()?)
    }

    pub async fn resolve_route(
        &self,
        context: CallContext,
        request: RouteReference,
    ) -> Result<ChannelRoute, TurnRouterError> {
        let _ = context;
        if request.target_channel_id.trim().is_empty() {
            return Err(TurnRouterError::InvalidRoute);
        }
        self.store.resolve(&request.target_channel_id)
    }

    pub async fn drain_lane(
        &self,
        context: CallContext,
        request: DrainLaneRequest,
    ) -> Result<DrainLaneReport, TurnRouterError> {
        validate_drain(&request)?;
        let _lane = self.lane_locks.lock(&request.lane).await?;
        let batch = self
            .trigger_inbox
            .claim(
                context.clone(),
                ClaimTriggers {
                    worker_id: request.worker_id.clone(),
                    lane: request.lane.clone(),
                    limit: request.limit,
                    lease_duration_ms: request.lease_duration_ms,
                    now_ms: (self.clock)()?,
                },
            )
            .await
            .map_err(|_| TurnRouterError::TriggerInboxUnavailable)?;
        let claimed =
            u64::try_from(batch.leases.len()).map_err(|_| TurnRouterError::StorageUnavailable)?;
        let mut results = Vec::with_capacity(batch.leases.len());
        for lease in batch.leases {
            results.push(self.route_lease(context.clone(), lease, &request).await?);
        }
        let completed = count_outcome(&results, &RoutedTriggerOutcome::Completed)?;
        let retry_scheduled = count_outcome(&results, &RoutedTriggerOutcome::RetryScheduled)?;
        let dead_lettered = count_outcome(&results, &RoutedTriggerOutcome::DeadLettered)?;
        Ok(DrainLaneReport {
            lane: request.lane,
            claimed,
            completed,
            retry_scheduled,
            dead_lettered,
            results,
        })
    }
}

fn validate_route(request: &PutRouteRequest) -> Result<(), TurnRouterError> {
    if request.target_channel_id.trim().is_empty()
        || request.lane.trim().is_empty()
        || request.binding_id.trim().is_empty()
    {
        Err(TurnRouterError::InvalidRoute)
    } else {
        Ok(())
    }
}

fn validate_drain(request: &DrainLaneRequest) -> Result<(), TurnRouterError> {
    if request.worker_id.trim().is_empty()
        || request.lane.trim().is_empty()
        || request.limit == 0
        || request.limit > MAX_DRAIN_LIMIT
        || request.lease_duration_ms == 0
        || request.retry_delay_ms == 0
        || request.max_attempts == 0
    {
        Err(TurnRouterError::InvalidDrain)
    } else {
        Ok(())
    }
}

fn trigger_prompt_json(lease: &TriggerLease) -> Result<String, TurnRouterError> {
    let message: Value = serde_json::from_str(&lease.trigger.message_json)
        .map_err(|_| TurnRouterError::InvalidTriggerPayload)?;
    let mut prompt = match &message {
        Value::Object(object) if object.contains_key("nativePrompt") => {
            let native = object
                .get("nativePrompt")
                .ok_or(TurnRouterError::InvalidTriggerPayload)?;
            let Value::Array(prompt) = native else {
                return Err(TurnRouterError::InvalidTriggerPayload);
            };
            if prompt.is_empty() {
                return Err(TurnRouterError::InvalidTriggerPayload);
            }
            prompt.clone()
        }
        Value::Object(object) if object.get("text").is_some_and(Value::is_string) => {
            let text = object
                .get("text")
                .and_then(Value::as_str)
                .ok_or(TurnRouterError::InvalidTriggerPayload)?;
            if text.trim().is_empty() {
                return Err(TurnRouterError::InvalidTriggerPayload);
            }
            vec![text_block(text, lease)?]
        }
        _ => vec![text_block(
            &serde_json::to_string(&message).map_err(|_| TurnRouterError::InvalidTriggerPayload)?,
            lease,
        )?],
    };
    prompt.extend(
        lease
            .trigger
            .attachments
            .iter()
            .map(resource_link)
            .collect::<Result<Vec<_>, _>>()?,
    );
    serde_json::to_string(&prompt).map_err(|_| TurnRouterError::InvalidTriggerPayload)
}

fn text_block(text: &str, lease: &TriggerLease) -> Result<Value, TurnRouterError> {
    let source = match lease.trigger.source {
        TriggerSource::Bridge => "bridge",
        TriggerSource::Scheduler => "scheduler",
        TriggerSource::SelfWork => "self_work",
        TriggerSource::Operator => "operator",
        TriggerSource::Unknown { .. } => return Err(TurnRouterError::InvalidTriggerPayload),
    };
    Ok(json!({
        "type": "text",
        "text": text,
        "_meta": {
            "crab": {
                "triggerId": lease.trigger.trigger_id,
                "source": source,
                "sourceId": lease.trigger.source_id,
            }
        }
    }))
}

fn resource_link(attachment: &TriggerAttachment) -> Result<Value, TurnRouterError> {
    if attachment.content_handle.trim().is_empty() || attachment.media_type.trim().is_empty() {
        return Err(TurnRouterError::InvalidTriggerPayload);
    }
    let mut link = Map::from_iter([
        ("type".into(), Value::String("resource_link".into())),
        (
            "name".into(),
            Value::String(
                attachment
                    .name
                    .clone()
                    .unwrap_or_else(|| "attachment".into()),
            ),
        ),
        (
            "uri".into(),
            Value::String(attachment.content_handle.clone()),
        ),
        (
            "mimeType".into(),
            Value::String(attachment.media_type.clone()),
        ),
    ]);
    link.insert(
        "_meta".into(),
        json!({ "crab": { "contentHandle": attachment.content_handle } }),
    );
    Ok(Value::Object(link))
}

fn count_outcome(
    results: &[RoutedTrigger],
    expected: &RoutedTriggerOutcome,
) -> Result<u64, TurnRouterError> {
    u64::try_from(
        results
            .iter()
            .filter(|result| &result.outcome == expected)
            .count(),
    )
    .map_err(|_| TurnRouterError::StorageUnavailable)
}

struct ChannelFailure {
    tag: &'static str,
    permanent: bool,
}

impl ChannelFailure {
    const fn permanent(tag: &'static str) -> Self {
        Self {
            tag,
            permanent: true,
        }
    }

    const fn transient(tag: &'static str) -> Self {
        Self {
            tag,
            permanent: false,
        }
    }
}

fn classify_channel_error(error: ErasedCallError) -> ChannelFailure {
    match error {
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "DuplicateTurnConflict" | "InvalidNativePayload"
            ) =>
        {
            ChannelFailure::permanent("invalid_channel_input")
        }
        ErasedCallError::Domain { error_tag, .. } if error_tag == "SteeringUnavailable" => {
            ChannelFailure::permanent("steering_unavailable")
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(error_tag.as_str(), "UnknownBinding" | "SessionMismatch") =>
        {
            ChannelFailure::permanent("stale_route")
        }
        _ => ChannelFailure::transient("native_channel_unavailable"),
    }
}

fn system_time_ms() -> Result<u64, TurnRouterError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TurnRouterError::StorageUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| TurnRouterError::StorageUnavailable)
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{LaneLocks, generated};

    #[tokio::test]
    async fn lane_locks_serialize_one_lane_without_retaining_idle_lanes() {
        let lanes = Arc::new(LaneLocks::default());
        let lane_a = lanes.lock("lane-a").await.expect("lane A locks");

        let lane_b = tokio::time::timeout(Duration::from_millis(100), lanes.lock("lane-b"))
            .await
            .expect("lane B is not blocked by lane A")
            .expect("lane B locks");
        drop(lane_b);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), lanes.lock("lane-a"))
                .await
                .is_err(),
            "drains for one lane must remain serialized"
        );
        drop(lane_a);

        let lane_a = tokio::time::timeout(Duration::from_millis(100), lanes.lock("lane-a"))
            .await
            .expect("lane A resumes after its drain completes")
            .expect("lane A relocks");
        drop(lane_a);

        let lane_c = lanes.lock("lane-c").await.expect("lane C locks");
        assert_eq!(
            lanes.registry_len().expect("registry reads"),
            1,
            "idle weak lane entries are pruned"
        );
        drop(lane_c);
    }

    #[test]
    fn contract_imports_both_durable_boundaries() {
        let descriptor = generated::implementation_descriptor();
        assert_eq!(descriptor.imports().len(), 2);
        assert_eq!(
            descriptor
                .contract()
                .capabilities()
                .iter()
                .map(|capability| capability.id().name().as_str())
                .collect::<Vec<_>>(),
            ["put_route", "resolve_route", "drain_lane"]
        );
    }
}

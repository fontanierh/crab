mod contract;
mod store;

pub use contract::*;

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};

use boxology_contract::{CallContext, ErasedCallError};
use boxology_import_native_channel::{
    BindingReference, ChannelInputMode, ChannelTurn, InterruptRequest,
};
use boxology_import_trigger_inbox::{
    ClaimTriggers, SettleTrigger, SettlementOutcome, TriggerAttachment, TriggerLease, TriggerMode,
    TriggerSource,
};
use generated::{NativeChannelImport, TriggerInboxImport};
use serde_json::{Map, Value, json};
use store::RouteStore;
use tokio::sync::Mutex;

const MAX_DRAIN_LIMIT: u64 = 1_000;

type Clock = Arc<dyn Fn() -> Result<u64, TurnRouterError> + Send + Sync>;

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
            lane_locks: Arc::new(StdMutex::new(HashMap::new())),
            clock: Arc::new(system_time_ms),
        }
    }
}

/// Durable bridge/scheduler/self-work ingress router over generated Boxology imports.
pub struct TurnRouter {
    trigger_inbox: Arc<TriggerInboxImport>,
    native_channel: Arc<NativeChannelImport>,
    store: Arc<RouteStore>,
    lane_locks: Arc<StdMutex<HashMap<String, Arc<Mutex<()>>>>>,
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

    fn lane_lock(&self, lane: &str) -> Result<Arc<Mutex<()>>, TurnRouterError> {
        let mut locks = self
            .lane_locks
            .lock()
            .map_err(|_| TurnRouterError::StorageUnavailable)?;
        Ok(locks
            .entry(lane.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone())
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

        let delivery = self
            .deliver(context.clone(), &route, &lease, prompt, (self.clock)()?)
            .await;
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
        now_ms: u64,
    ) -> Result<(), ChannelFailure> {
        let mode = match lease.trigger.mode {
            TriggerMode::Queue => ChannelInputMode::Queue,
            TriggerMode::Steer => ChannelInputMode::Steer,
            TriggerMode::InterruptAndSteer => {
                let status = self
                    .native_channel
                    .channel_status(
                        context.clone(),
                        BindingReference {
                            binding_id: route.binding_id.clone(),
                        },
                    )
                    .await
                    .map_err(classify_channel_error)?;
                let interrupted = self
                    .native_channel
                    .interrupt_and_drain(
                        context.clone(),
                        InterruptRequest {
                            binding_id: route.binding_id.clone(),
                            expected_session_id: status.binding.session_id,
                            requested_at_ms: now_ms,
                            reason: format!("trigger:{}", lease.trigger.trigger_id),
                        },
                    )
                    .await;
                if let Err(error) = interrupted
                    && !is_domain_tag(&error, "NothingToInterrupt")
                {
                    return Err(classify_channel_error(error));
                }
                ChannelInputMode::Queue
            }
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
        let lane_lock = self.lane_lock(&request.lane)?;
        let _lane = lane_lock.lock().await;
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

fn is_domain_tag(error: &ErasedCallError, expected: &str) -> bool {
    matches!(
        error,
        ErasedCallError::Domain { error_tag, .. } if error_tag == expected
    )
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
    use super::generated;

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

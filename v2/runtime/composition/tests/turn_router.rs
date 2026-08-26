use std::sync::{Arc, Mutex};

extern crate native_channel_contract as boxology_generated_contract;

use boxology_contract::{CallContext, Caller, CancelToken, ImplementationDescriptor, TraceContext};
use boxology_runtime::CompositionBuilder;
use native_channel_implementation::{
    AcceptedTurn, BindChannelRequest, BindingReference, ChannelBinding, ChannelInputMode,
    ChannelLifecycle, ChannelReceipt, ChannelStatus, ChannelTurn, ChannelTurnDisposition,
    InterruptReceipt, InterruptRequest, LocateBindingRequest, NativeChannelError,
    NativeChannelEvent, PublishReceipt, PublishedEventPage, ReplaceSessionRequest, ReplayRequest,
    generated as native_channel,
};
use trigger_inbox_contract::{
    EnqueueTrigger, TriggerAttachment, TriggerMode, TriggerReference, TriggerSource, TriggerState,
};
use trigger_inbox_implementation::{TriggerInbox, generated as trigger_inbox};
use turn_router_contract::{
    DrainLaneRequest, PutRouteRequest, RoutedTriggerOutcome, TurnRouterError,
};
use turn_router_implementation::{TurnRouterState, generated as turn_router};

#[derive(Default)]
struct FakeState {
    turns: Vec<ChannelTurn>,
    interrupts: Vec<InterruptRequest>,
}

struct FakeNativeChannel {
    state: Arc<Mutex<FakeState>>,
}

#[boxology::implementation]
impl FakeNativeChannel {
    async fn bind_channel(
        &self,
        context: CallContext,
        request: BindChannelRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::AlreadyBound)
    }

    async fn accept_turn(
        &self,
        context: CallContext,
        request: ChannelTurn,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        let _ = context;
        if request.binding_id == "binding-transient" {
            return Err(NativeChannelError::AdapterUnavailable);
        }
        if request.binding_id != "binding-good" {
            return Err(NativeChannelError::UnknownBinding);
        }
        let disposition = match request.mode {
            ChannelInputMode::Queue => ChannelTurnDisposition::StartedForegroundWork,
            ChannelInputMode::Steer => ChannelTurnDisposition::ContributedToActiveWork,
            ChannelInputMode::Unknown { .. } => {
                return Err(NativeChannelError::InvalidNativePayload);
            }
        };
        self.state
            .lock()
            .expect("fake state lock")
            .turns
            .push(request.clone());
        Ok(AcceptedTurn {
            binding_id: request.binding_id,
            session_id: "session-1".into(),
            client_turn_id: request.client_turn_id,
            accepted_at_ms: 100,
            mode: request.mode,
            run_id: "run-1".into(),
            disposition,
        })
    }

    async fn interrupt_and_drain(
        &self,
        context: CallContext,
        request: InterruptRequest,
    ) -> Result<InterruptReceipt, NativeChannelError> {
        let _ = context;
        self.state
            .lock()
            .expect("fake state lock")
            .interrupts
            .push(request.clone());
        Ok(InterruptReceipt {
            binding_id: request.binding_id,
            session_id: request.expected_session_id,
            cancel_requested_at_ms: request.requested_at_ms,
            pending_input_count: 0,
        })
    }

    async fn publish_native_event(
        &self,
        context: CallContext,
        request: NativeChannelEvent,
    ) -> Result<PublishReceipt, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::SequenceGap)
    }

    async fn replay_native_events(
        &self,
        context: CallContext,
        request: ReplayRequest,
    ) -> Result<PublishedEventPage, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::UnknownBinding)
    }

    async fn replace_session(
        &self,
        context: CallContext,
        request: ReplaceSessionRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::UnknownBinding)
    }

    async fn channel_status(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelStatus, NativeChannelError> {
        let _ = context;
        if request.binding_id != "binding-good" {
            return Err(NativeChannelError::UnknownBinding);
        }
        Ok(ChannelStatus {
            binding: ChannelBinding {
                binding_id: request.binding_id,
                channel_id: "target-good".into(),
                adapter_id: "test".into(),
                session_id: "session-1".into(),
                lifecycle: ChannelLifecycle::Attached,
                native_channel_json: "{}".into(),
                published_sequence: 0,
            },
            available_sequence: 0,
            pending_input_count: 0,
            last_error: None,
            updated_at_ms: 1,
        })
    }

    async fn inspect_binding(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = context;
        if request.binding_id != "binding-good" {
            return Err(NativeChannelError::UnknownBinding);
        }
        Ok(ChannelBinding {
            binding_id: request.binding_id,
            channel_id: "target-good".into(),
            adapter_id: "test".into(),
            session_id: "session-1".into(),
            lifecycle: ChannelLifecycle::Attached,
            native_channel_json: "{}".into(),
            published_sequence: 0,
        })
    }

    async fn find_binding(
        &self,
        context: CallContext,
        request: LocateBindingRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = context;
        if request.channel_id != "target-good" || request.adapter_id != "test" {
            return Err(NativeChannelError::UnknownBinding);
        }
        Ok(ChannelBinding {
            binding_id: "binding-good".into(),
            channel_id: request.channel_id,
            adapter_id: request.adapter_id,
            session_id: "session-1".into(),
            lifecycle: ChannelLifecycle::Attached,
            native_channel_json: "{}".into(),
            published_sequence: 0,
        })
    }

    async fn unbind_channel(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelReceipt, NativeChannelError> {
        let _ = (context, request);
        Ok(ChannelReceipt {
            accepted: true,
            recorded_at_ms: 1,
        })
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

fn enqueue(
    source_id: &str,
    target: &str,
    mode: TriggerMode,
    message_json: &str,
    attachments: Vec<TriggerAttachment>,
) -> EnqueueTrigger {
    EnqueueTrigger {
        source: TriggerSource::Bridge,
        source_id: source_id.into(),
        deduplication_key: format!("dedupe-{source_id}"),
        target_channel_id: target.into(),
        lane: "primary".into(),
        mode,
        not_before_ms: 0,
        message_json: message_json.into(),
        attachments,
    }
}

#[tokio::test]
async fn router_serially_delivers_all_modes_and_truthfully_settles_each_lease() {
    let fake_state = Arc::new(Mutex::new(FakeState::default()));
    let route_state = TurnRouterState::open_in_memory().expect("route state opens");
    let trigger_store = TriggerInbox::open_in_memory().expect("trigger store opens");
    let mut builder = CompositionBuilder::new();
    let trigger = trigger_inbox::register(&mut builder, trigger_store);
    let fake_channel_state = fake_state.clone();
    let channel = builder.register(
        ImplementationDescriptor::new(native_channel_contract::contract_descriptor(), [])
            .expect("fake native channel descriptor is valid"),
        move |imports| {
            native_channel::factory(
                FakeNativeChannel {
                    state: fake_channel_state,
                },
                imports,
            )
        },
    );
    let router = turn_router::register(&mut builder, move |imports| {
        route_state.connect(imports.trigger_inbox, imports.native_channel)
    });
    builder.connect(&router, &trigger);
    builder.connect(&router, &channel);
    let trigger_handle = builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&trigger);
    let router_handle = builder.handle::<turn_router_contract::TurnRouterHandle>(&router);
    let _composition = builder.start().expect("graph starts");

    for (target, binding) in [
        ("target-good", "binding-good"),
        ("target-retry", "binding-transient"),
    ] {
        router_handle
            .put_route(
                context(),
                PutRouteRequest {
                    target_channel_id: target.into(),
                    lane: "primary".into(),
                    binding_id: binding.into(),
                    expected_generation: None,
                },
            )
            .await
            .expect("route registers");
    }

    let attachment = TriggerAttachment {
        media_type: "image/png".into(),
        name: Some("diagram.png".into()),
        content_handle: "file:///tmp/diagram.png".into(),
    };
    let requests = [
        enqueue(
            "queue",
            "target-good",
            TriggerMode::Queue,
            r#"{"text":"hello"}"#,
            vec![attachment],
        ),
        enqueue(
            "steer",
            "target-good",
            TriggerMode::Steer,
            r#"{"nativePrompt":[{"type":"text","text":"now"}]}"#,
            Vec::new(),
        ),
        enqueue(
            "interrupt",
            "target-good",
            TriggerMode::InterruptAndSteer,
            r#"{"text":"urgent"}"#,
            Vec::new(),
        ),
        enqueue(
            "missing",
            "target-missing",
            TriggerMode::Queue,
            r#"{"text":"nowhere"}"#,
            Vec::new(),
        ),
        enqueue(
            "retry",
            "target-retry",
            TriggerMode::Queue,
            r#"{"text":"later"}"#,
            Vec::new(),
        ),
        enqueue(
            "invalid",
            "target-good",
            TriggerMode::Queue,
            r#"{"nativePrompt":"not-an-array"}"#,
            Vec::new(),
        ),
    ];
    let mut trigger_ids = Vec::new();
    for request in requests {
        trigger_ids.push(
            trigger_handle
                .enqueue(context(), request)
                .await
                .expect("trigger enqueues")
                .trigger_id,
        );
    }

    let report = router_handle
        .drain_lane(
            context(),
            DrainLaneRequest {
                worker_id: "router-1".into(),
                lane: "primary".into(),
                limit: 10,
                lease_duration_ms: 10_000,
                retry_delay_ms: 1_000,
                max_attempts: 3,
            },
        )
        .await
        .expect("lane drains");
    assert_eq!(report.claimed, 6);
    assert_eq!(report.completed, 3);
    assert_eq!(report.retry_scheduled, 1);
    assert_eq!(report.dead_lettered, 2);
    assert_eq!(
        report
            .results
            .iter()
            .map(|result| result.outcome.clone())
            .collect::<Vec<_>>(),
        [
            RoutedTriggerOutcome::Completed,
            RoutedTriggerOutcome::Completed,
            RoutedTriggerOutcome::Completed,
            RoutedTriggerOutcome::DeadLettered,
            RoutedTriggerOutcome::RetryScheduled,
            RoutedTriggerOutcome::DeadLettered,
        ]
    );

    {
        let state = fake_state.lock().expect("fake state lock");
        assert_eq!(state.turns.len(), 3);
        assert_eq!(state.turns[0].mode, ChannelInputMode::Queue);
        assert_eq!(state.turns[1].mode, ChannelInputMode::Steer);
        assert_eq!(state.turns[2].mode, ChannelInputMode::Queue);
        assert_eq!(state.interrupts.len(), 1);
        let first_prompt: serde_json::Value =
            serde_json::from_str(&state.turns[0].native_prompt_json).expect("prompt is JSON");
        assert_eq!(first_prompt[0]["text"], "hello");
        assert_eq!(first_prompt[1]["type"], "resource_link");
    }

    for (index, trigger_id) in trigger_ids.into_iter().enumerate() {
        let record = trigger_handle
            .inspect(context(), TriggerReference { trigger_id })
            .await
            .expect("settled trigger reads");
        let expected = match index {
            0..=2 => TriggerState::Completed,
            4 => TriggerState::RetryScheduled,
            _ => TriggerState::DeadLettered,
        };
        assert_eq!(record.state, expected);
    }
}

#[tokio::test]
async fn invalid_drain_fails_before_claiming() {
    let route_state = TurnRouterState::open_in_memory().expect("route state opens");
    let trigger_store = TriggerInbox::open_in_memory().expect("trigger store opens");
    let mut builder = CompositionBuilder::new();
    let trigger = trigger_inbox::register(&mut builder, trigger_store);
    let channel = builder.register(
        ImplementationDescriptor::new(native_channel_contract::contract_descriptor(), [])
            .expect("fake native channel descriptor is valid"),
        move |imports| {
            native_channel::factory(
                FakeNativeChannel {
                    state: Arc::new(Mutex::new(FakeState::default())),
                },
                imports,
            )
        },
    );
    let router = turn_router::register(&mut builder, move |imports| {
        route_state.connect(imports.trigger_inbox, imports.native_channel)
    });
    builder.connect(&router, &trigger);
    builder.connect(&router, &channel);
    let handle = builder.handle::<turn_router_contract::TurnRouterHandle>(&router);
    let _composition = builder.start().expect("graph starts");

    assert_eq!(
        handle
            .drain_lane(
                context(),
                DrainLaneRequest {
                    worker_id: "worker".into(),
                    lane: "primary".into(),
                    limit: 0,
                    lease_duration_ms: 1,
                    retry_delay_ms: 1,
                    max_attempts: 1,
                },
            )
            .await,
        Err(boxology_contract::CallError::Domain(
            TurnRouterError::InvalidDrain
        ))
    );
}

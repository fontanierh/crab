use std::collections::BTreeMap;
use std::path::PathBuf;

use crab_backends::{
    BackendEvent, BackendEventKind, BackendEventStream, CodexAppServerProcess,
    CodexLifecycleManager, OpenCodeServerProcess,
};
use crab_core::{
    apply_operator_command, build_checkpoint_prompt, build_fallback_checkpoint_document,
    build_memory_flush_prompt, build_onboarding_extraction_prompt,
    detect_workspace_bootstrap_state, enqueue_workspace_git_push_request,
    evaluate_rotation_triggers, execute_onboarding_completion_protocol, execute_rotation_sequence,
    finalize_hidden_memory_flush, maybe_commit_workspace_snapshot,
    parse_onboarding_capture_document, parse_operator_command, persist_onboarding_profile_files,
    resolve_checkpoint_turn_output, Checkpoint, CheckpointTurnDocument, CheckpointTurnResolution,
    CrabError, CrabResult, EventEnvelope, EventKind, EventSource, InferenceProfile, LaneState,
    LogicalSession, ManualRotationRequest, OnboardingCompletionEventRuntime,
    OnboardingCompletionInput, OperatorActorContext, OperatorCommand, OperatorSessionState,
    PhysicalSession, RotationSequenceRuntime, RotationTrigger, RotationTriggerInput, Run,
    RunProfileTelemetry, RunStatus, TokenAccounting, TranscriptEntry, TranscriptEntryRole,
    WorkspaceBootstrapState, WorkspaceGitCommitRequest, WorkspaceGitCommitTrigger,
    WorkspaceGitPushRequest, DEFAULT_CHECKPOINT_MAX_ATTEMPTS,
    DEFAULT_FALLBACK_TRANSCRIPT_TAIL_LIMIT, ONBOARDING_CAPTURE_INCOMPLETE_TOKEN,
};
use crab_discord::{DeliveryAttempt, GatewayMessage, RoutingKey, ShouldSendDecision};
use crab_scheduler::QueuedRun;
use crab_store::{CheckpointStore, EventStore};
use futures::executor::block_on;
use futures::future::poll_fn;

use crate::AppComposition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTurn {
    pub logical_session_id: String,
    pub run_id: String,
    pub message_id: String,
    pub author_id: String,
    pub routing_key: RoutingKey,
    pub queued_run_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedTurn {
    pub logical_session_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub status: RunStatus,
    pub emitted_event_count: usize,
}

pub trait TurnExecutorRuntime {
    fn now_epoch_ms(&mut self) -> CrabResult<u64>;

    fn resolve_run_profile(
        &mut self,
        logical_session_id: &str,
        author_id: &str,
        user_input: &str,
    ) -> CrabResult<RunProfileTelemetry>;

    fn ensure_physical_session(
        &mut self,
        logical_session_id: &str,
        profile: &InferenceProfile,
        active_physical_session_id: Option<&str>,
    ) -> CrabResult<PhysicalSession>;

    fn build_turn_context(
        &mut self,
        run: &Run,
        logical_session: &LogicalSession,
        physical_session: &PhysicalSession,
        inject_bootstrap_context: bool,
    ) -> CrabResult<String>;

    fn execute_backend_turn(
        &mut self,
        codex_lifecycle: &mut dyn CodexLifecycleManager,
        physical_session: &mut PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<BackendEventStream>;

    fn deliver_assistant_output(
        &mut self,
        run: &Run,
        channel_id: &str,
        message_id: &str,
        edit_generation: u32,
        content: &str,
    ) -> CrabResult<()>;
}

pub struct TurnExecutor<CP, OP, R>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    R: TurnExecutorRuntime,
{
    composition: AppComposition<CP, OP>,
    runtime: R,
}

impl<CP, OP, R> TurnExecutor<CP, OP, R>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    R: TurnExecutorRuntime,
{
    #[must_use]
    pub fn new(composition: AppComposition<CP, OP>, runtime: R) -> Self {
        Self {
            composition,
            runtime,
        }
    }

    #[must_use]
    pub fn composition(&self) -> &AppComposition<CP, OP> {
        &self.composition
    }

    #[must_use]
    pub fn composition_mut(&mut self) -> &mut AppComposition<CP, OP> {
        &mut self.composition
    }

    #[must_use]
    pub fn runtime_mut(&mut self) -> &mut R {
        &mut self.runtime
    }

    #[cfg(test)]
    fn process_gateway_message(
        &mut self,
        message: GatewayMessage,
    ) -> CrabResult<Option<DispatchedTurn>> {
        let maybe_queued = self.enqueue_gateway_message(message)?;
        if maybe_queued.is_none() {
            return Ok(None);
        }
        self.dispatch_next_run()
    }

    pub fn enqueue_gateway_message(
        &mut self,
        message: GatewayMessage,
    ) -> CrabResult<Option<QueuedTurn>> {
        let Some(ingress) = self.composition.gateway_ingress.ingest(message)? else {
            return Ok(None);
        };
        self.enqueue_ingress_message(ingress).map(Some)
    }

    fn enqueue_ingress_message(
        &mut self,
        ingress: crab_discord::IngressMessage,
    ) -> CrabResult<QueuedTurn> {
        let logical_session_id = ingress
            .routing_key
            .logical_session_id()
            .expect("gateway ingress should yield validated routing keys");
        let run_id = build_run_id(&logical_session_id, &ingress.message_id);
        self.composition.scheduler.enqueue(
            &logical_session_id,
            QueuedRun {
                run_id: run_id.clone(),
            },
        )?;

        let enqueue_result = self.persist_enqueued_run(&logical_session_id, &run_id, &ingress);
        if let Err(error) = enqueue_result {
            let _ = self.composition.scheduler.cancel_queued_run_by_id(&run_id);
            return Err(error);
        }
        let queued_run_count = self
            .composition
            .scheduler
            .queued_count(&logical_session_id)
            .expect("validated logical session id should always be queue-count addressable");

        tracing::debug!(
            logical_session_id = %logical_session_id,
            run_id = %run_id,
            message_id = %ingress.message_id,
            author_id = %ingress.author_id,
            queued_run_count,
            "enqueued ingress message"
        );

        Ok(QueuedTurn {
            logical_session_id,
            run_id,
            message_id: ingress.message_id,
            author_id: ingress.author_id,
            routing_key: ingress.routing_key,
            queued_run_count,
        })
    }

    pub fn dispatch_next_run(&mut self) -> CrabResult<Option<DispatchedTurn>> {
        let Some(dispatched) = self.composition.scheduler.try_dispatch_next() else {
            return Ok(None);
        };
        let logical_session_id = dispatched.logical_session_id;
        let run_id = dispatched.run.run_id;

        let execution = self.execute_dispatched_run(&logical_session_id, &run_id);
        if let Err(error) = execution {
            tracing::error!(
                logical_session_id = %logical_session_id,
                run_id = %run_id,
                error = %error,
                "dispatch failed"
            );
            let failure_result = self.mark_run_failed(&logical_session_id, &run_id, &error);
            self.composition
                .scheduler
                .complete_lane(&logical_session_id)
                .expect("dispatched lane must be active until completion");
            failure_result?;
            return Err(error);
        }

        self.composition
            .scheduler
            .complete_lane(&logical_session_id)
            .expect("dispatched lane must be active until completion");
        Ok(Some(
            execution.expect("execution should be available on success"),
        ))
    }

    #[cfg(test)]
    fn replay_delivery_for_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<usize> {
        let run = self.load_required_run(logical_session_id, run_id)?;
        let events = self
            .composition
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)?;
        let mut delivery = DiscordAssistantDelivery::new();
        let mut delivered_count = 0_usize;

        for event in events {
            if let Some(delta_text) = extract_event_text_delta(&event) {
                let planned = delivery.push_delta(delta_text);
                for op in planned {
                    let delivered = self.deliver_rendered_assistant_output(
                        &run,
                        &delivery_message_id(&run.id, op.chunk_index),
                        &op.content,
                        op.edit_generation,
                        event.emitted_at_epoch_ms,
                    )?;
                    if delivered {
                        delivered_count += 1;
                    }
                }
            }
        }

        Ok(delivered_count)
    }

    fn persist_enqueued_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
        ingress: &crab_discord::IngressMessage,
    ) -> CrabResult<()> {
        let now_epoch_ms = self.runtime.now_epoch_ms()?;
        let profile = self.runtime.resolve_run_profile(
            logical_session_id,
            &ingress.author_id,
            &ingress.content,
        )?;
        let run = Run {
            id: run_id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            physical_session_id: None,
            status: RunStatus::Queued,
            user_input: ingress.content.clone(),
            delivery_channel_id: Some(ingress.channel_id.clone()),
            profile: profile.clone(),
            queued_at_epoch_ms: now_epoch_ms,
            started_at_epoch_ms: None,
            completed_at_epoch_ms: None,
        };
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut session = self.load_or_initialize_session(
            logical_session_id,
            &profile.resolved_profile,
            now_epoch_ms,
        )?;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = now_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(&run, "queued", now_epoch_ms, None)
    }

    fn execute_dispatched_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<DispatchedTurn> {
        let mut run = self.load_required_run(logical_session_id, run_id)?;
        let started_at_epoch_ms = self.runtime.now_epoch_ms()?;

        run.status = RunStatus::Running;
        run.started_at_epoch_ms = Some(started_at_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut session = self.load_or_initialize_session(
            logical_session_id,
            &run.profile.resolved_profile,
            started_at_epoch_ms,
        )?;
        session.lane_state = LaneState::Running;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = started_at_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(&run, "running", started_at_epoch_ms, None)?;

        let turn_id = build_turn_id(&run.id);
        let mut backend_events = Vec::new();
        let mut delivery = DiscordAssistantDelivery::new();
        let mut onboarding_gate_resolution =
            self.resolve_pending_onboarding_gate(&run, started_at_epoch_ms)?;
        let mut manual_command_resolution = if onboarding_gate_resolution.is_some() {
            None
        } else {
            self.resolve_manual_rotation_command(&run, &mut session)?
        };
        let mut onboarding_completion_resolution = None;
        let mut supplemental_emitted_event_count = 0_usize;
        let mut rotation_onboarding_completion_resolution = None;
        if let Some(ref resolution) = onboarding_gate_resolution {
            supplemental_emitted_event_count =
                supplemental_emitted_event_count.saturating_add(resolution.emitted_event_count);
        }

        // Note: `tracing` macros can create stubborn per-line coverage gaps under `cargo llvm-cov`
        // (cfg(coverage)). Keep runtime logs, but exclude them from coverage builds.
        #[cfg(not(coverage))]
        tracing::info!(
            logical_session_id = %run.logical_session_id,
            run_id = %run.id,
            turn_id = %turn_id,
            backend = ?run.profile.resolved_profile.backend,
            model = %run.profile.resolved_profile.model,
            reasoning_level = %run.profile.resolved_profile.reasoning_level.as_token(),
            manual_rotation_command = manual_command_resolution.is_some(),
            "run started"
        );

        if manual_command_resolution.is_none() && onboarding_gate_resolution.is_none() {
            onboarding_completion_resolution =
                self.maybe_complete_pending_onboarding_capture(&run, started_at_epoch_ms)?;
            if let Some(ref resolution) = onboarding_completion_resolution {
                supplemental_emitted_event_count =
                    supplemental_emitted_event_count.saturating_add(resolution.emitted_event_count);
            }
        }

        if manual_command_resolution.is_none()
            && onboarding_completion_resolution.is_none()
            && onboarding_gate_resolution.is_none()
        {
            let mut physical_session = self.runtime.ensure_physical_session(
                logical_session_id,
                &run.profile.resolved_profile,
                session.active_physical_session_id.as_deref(),
            )?;
            run.physical_session_id = Some(physical_session.id.clone());
            self.composition.state_stores.run_store.upsert_run(&run)?;

            session.active_backend = run.profile.resolved_profile.backend;
            session.active_profile = run.profile.resolved_profile.clone();
            session.active_physical_session_id = Some(physical_session.id.clone());
            self.composition
                .state_stores
                .session_store
                .upsert_session(&session)?;

            let inject_bootstrap_context = physical_session.last_turn_id.is_none();
            let turn_context = self.runtime.build_turn_context(
                &run,
                &session,
                &physical_session,
                inject_bootstrap_context,
            )?;
            let mut backend_event_stream = self.runtime.execute_backend_turn(
                &mut self.composition.backends.codex,
                &mut physical_session,
                &run,
                &turn_id,
                &turn_context,
            )?;

            loop {
                let next = block_on(poll_fn(|cx| backend_event_stream.as_mut().poll_next(cx)));
                let Some(backend_event) = next else {
                    break;
                };
                let emitted_at_epoch_ms = self.runtime.now_epoch_ms()?;
                self.append_backend_event(&run, &backend_event, emitted_at_epoch_ms)?;
                if let Some(delta_text) = extract_backend_text_delta(&backend_event) {
                    let planned = delivery.push_delta(delta_text);
                    for op in planned {
                        let _ = self.deliver_rendered_assistant_output(
                            &run,
                            &delivery_message_id(&run.id, op.chunk_index),
                            &op.content,
                            op.edit_generation,
                            emitted_at_epoch_ms,
                        )?;
                    }
                }
                backend_events.push(backend_event);
            }
        } else {
            run.physical_session_id = session.active_physical_session_id.clone();
        }

        let final_status = if manual_command_resolution.is_some()
            || onboarding_completion_resolution.is_some()
            || onboarding_gate_resolution.is_some()
        {
            RunStatus::Succeeded
        } else {
            derive_final_status(&backend_events)
        };
        let completed_at_epoch_ms = self.runtime.now_epoch_ms()?;
        run.status = final_status;
        run.completed_at_epoch_ms = Some(completed_at_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        if final_status == RunStatus::Failed {
            // Backend sessions are managed out-of-process. If a turn fails, the session may be
            // corrupt/locked/unrecoverable; clear it so the next message forces a fresh physical
            // session.
            session.active_physical_session_id = None;
        }

        if let Some(run_usage) = resolve_backend_usage_accounting(&backend_events)? {
            session.token_accounting =
                merge_token_accounting(session.token_accounting.clone(), run_usage)?;
        }
        let token_total_before_rotation = session.token_accounting.total_tokens;
        #[cfg(coverage)]
        let _ = token_total_before_rotation;

        session.lane_state = LaneState::Idle;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = completed_at_epoch_ms;

        self.append_run_state_event(
            &run,
            run_status_token(final_status),
            completed_at_epoch_ms,
            None,
        )?;

        let manual_rotation_request = manual_command_resolution
            .as_ref()
            .and_then(|resolution| resolution.request);
        let rotation_outcome = if onboarding_gate_resolution.is_some() {
            None
        } else {
            self.maybe_execute_rotation(
                &run,
                &mut session,
                completed_at_epoch_ms,
                manual_rotation_request,
            )?
        };
        if let Some(rotation_outcome) = rotation_outcome.as_ref() {
            if let Some(ref resolution) = rotation_outcome.onboarding_completion_resolution {
                supplemental_emitted_event_count =
                    supplemental_emitted_event_count.saturating_add(resolution.emitted_event_count);
                rotation_onboarding_completion_resolution = Some(resolution.clone());
            }
            supplemental_emitted_event_count = supplemental_emitted_event_count
                .saturating_add(rotation_outcome.supplemental_emitted_event_count);
        }

        #[cfg(not(coverage))]
        tracing::info!(
            logical_session_id = %run.logical_session_id,
            run_id = %run.id,
            turn_id = %turn_id,
            status = %run_status_token(final_status),
            backend_events = backend_events.len(),
            rendered_len = delivery.total_rendered_len,
            token_total_before_rotation,
            rotated = rotation_outcome.is_some(),
            token_total_after_rotation = session.token_accounting.total_tokens,
            "run completed"
        );

        if let Some(manual_command_resolution) = manual_command_resolution.take() {
            let mut response = manual_command_resolution.user_message;
            let rotation_outcome = rotation_outcome
                .as_ref()
                .expect("manual rotation command must produce rotation outcome");
            response = format!(
                "{} (checkpoint: {})",
                response, rotation_outcome.checkpoint_id
            );
            let _ = self.deliver_rendered_assistant_output(
                &run,
                &delivery_message_id(&run.id, 0),
                &response,
                0,
                completed_at_epoch_ms,
            )?;
        } else if let Some(onboarding_completion_resolution) = onboarding_completion_resolution {
            let delivery_result = self.deliver_rendered_assistant_output(
                &run,
                &delivery_message_id(&run.id, 0),
                &onboarding_completion_resolution.user_message,
                0,
                completed_at_epoch_ms,
            );
            let _ = delivery_result?;
        } else if let Some(onboarding_gate_resolution) = onboarding_gate_resolution.take() {
            let delivery_result = self.deliver_rendered_assistant_output(
                &run,
                &delivery_message_id(&run.id, 0),
                &onboarding_gate_resolution.user_message,
                0,
                completed_at_epoch_ms,
            );
            let _ = delivery_result?;
        } else if let Some(rotation_onboarding_completion_resolution) =
            rotation_onboarding_completion_resolution
        {
            let delivery_result = self.deliver_rendered_assistant_output(
                &run,
                &delivery_message_id(&run.id, 0),
                &rotation_onboarding_completion_resolution.user_message,
                0,
                completed_at_epoch_ms,
            );
            let _ = delivery_result?;
        }

        if final_status == RunStatus::Failed && delivery.total_rendered_len == 0 {
            #[allow(clippy::single_match)]
            match Self::render_backend_failure_message(&backend_events, &run) {
                Some(message) => {
                    // Keep on one line: multi-line call sites can produce llvm-cov line-mapping gaps.
                    #[rustfmt::skip]
                    let _ = self.deliver_rendered_assistant_output(&run, &delivery_message_id(&run.id, 0), &message, 0, completed_at_epoch_ms)?;
                }
                None => {}
            }
        }

        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;
        self.maybe_persist_workspace_git_commits(
            &run,
            final_status,
            completed_at_epoch_ms,
            rotation_outcome.as_ref(),
        );

        Ok(DispatchedTurn {
            logical_session_id: logical_session_id.to_string(),
            run_id: run.id,
            turn_id,
            status: final_status,
            emitted_event_count: backend_events.len() + 2 + supplemental_emitted_event_count,
        })
    }

    fn render_backend_failure_message(
        backend_events: &[BackendEvent],
        run: &Run,
    ) -> Option<String> {
        let message = backend_events.iter().rev().find_map(|event| {
            if event.kind != BackendEventKind::Error {
                return None;
            }
            event
                .payload
                .get("message")
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
        })?;

        // Keep message short enough to be Discord-safe and user-readable.
        let truncated = if message.chars().count() > 1800 {
            let prefix: String = message.chars().take(1800).collect();
            format!("{prefix}...")
        } else {
            message
        };

        Some(format!(
            "Crab: backend failed for this message (run_id: {}).\n{}\nPlease resend your last message.",
            run.id, truncated
        ))
    }

    fn maybe_complete_pending_onboarding_capture(
        &mut self,
        run: &Run,
        completed_at_epoch_ms: u64,
    ) -> CrabResult<Option<OnboardingCompletionResolution>> {
        let bootstrap_state =
            detect_workspace_bootstrap_state(&self.composition.startup.workspace_root)?;
        if bootstrap_state != WorkspaceBootstrapState::PendingBootstrap {
            return Ok(None);
        }

        if !looks_like_onboarding_capture_payload(&run.user_input) {
            return Ok(None);
        }

        let capture = parse_onboarding_capture_document(&run.user_input)?;
        self.apply_onboarding_capture_document(
            run,
            capture,
            format!("onboarding:{}", run.id),
            completed_at_epoch_ms,
            "Onboarding capture applied.",
        )
        .map(Some)
    }

    fn resolve_pending_onboarding_gate(
        &mut self,
        run: &Run,
        emitted_at_epoch_ms: u64,
    ) -> CrabResult<Option<PendingOnboardingGateResolution>> {
        let bootstrap_state =
            detect_workspace_bootstrap_state(&self.composition.startup.workspace_root)?;
        if bootstrap_state != WorkspaceBootstrapState::PendingBootstrap {
            return Ok(None);
        }

        if run.profile.sender_is_owner && is_dm_logical_session_id(&run.logical_session_id) {
            return Ok(None);
        }

        let (reason, user_message) = if !run.profile.sender_is_owner {
            (
                "non_owner",
                "Onboarding is pending. Only the owner can continue onboarding in owner DM."
                    .to_string(),
            )
        } else {
            (
                "owner_non_dm",
                "Onboarding is pending. Continue in owner DM; server channels/threads are blocked until onboarding completes."
                    .to_string(),
            )
        };

        let mut payload = BTreeMap::new();
        payload.insert("event".to_string(), "onboarding_gate_blocked".to_string());
        payload.insert("reason".to_string(), reason.to_string());
        payload.insert("sender_id".to_string(), run.profile.sender_id.clone());
        payload.insert(
            "sender_is_owner".to_string(),
            run.profile.sender_is_owner.to_string(),
        );
        payload.insert(
            "logical_session_id".to_string(),
            run.logical_session_id.clone(),
        );
        self.append_system_run_note(run, payload, emitted_at_epoch_ms)?;

        Ok(Some(PendingOnboardingGateResolution {
            user_message,
            emitted_event_count: 1,
        }))
    }

    fn maybe_complete_pending_onboarding_from_rotation(
        &mut self,
        run: &Run,
        session: &mut LogicalSession,
        completed_at_epoch_ms: u64,
    ) -> CrabResult<(Option<OnboardingCompletionResolution>, usize)> {
        let bootstrap_state =
            detect_workspace_bootstrap_state(&self.composition.startup.workspace_root)?;
        if bootstrap_state != WorkspaceBootstrapState::PendingBootstrap {
            return Ok((None, 0));
        }
        if !run.profile.sender_is_owner || !is_dm_logical_session_id(&run.logical_session_id) {
            return Ok((None, 0));
        }

        let ensure_physical_session = self.runtime.ensure_physical_session(
            &run.logical_session_id,
            &run.profile.resolved_profile,
            session.active_physical_session_id.as_deref(),
        );
        let mut physical_session = ensure_physical_session?;
        session.active_backend = run.profile.resolved_profile.backend;
        session.active_profile = run.profile.resolved_profile.clone();
        session.active_physical_session_id = Some(physical_session.id.clone());
        self.composition
            .state_stores
            .session_store
            .upsert_session(session)?;

        let extraction_prompt =
            build_onboarding_extraction_prompt(&format!("onboarding-rotation:{}", run.id))?;
        let mut hidden_onboarding_run = run.clone();
        hidden_onboarding_run.user_input = extraction_prompt.clone();
        hidden_onboarding_run.physical_session_id = Some(physical_session.id.clone());
        let hidden_turn_id = format!("turn:{}:hidden-onboarding-capture", run.id);
        let mut backend_event_stream = self.runtime.execute_backend_turn(
            &mut self.composition.backends.codex,
            &mut physical_session,
            &hidden_onboarding_run,
            &hidden_turn_id,
            &extraction_prompt,
        )?;
        let mut raw_output = String::new();
        loop {
            let next = block_on(poll_fn(|cx| backend_event_stream.as_mut().poll_next(cx)));
            let Some(backend_event) = next else {
                break;
            };
            if let Some(delta) = extract_backend_text_delta(&backend_event) {
                raw_output.push_str(delta);
            }
        }

        if raw_output.trim() == ONBOARDING_CAPTURE_INCOMPLETE_TOKEN {
            let mut payload = BTreeMap::new();
            payload.insert(
                "event".to_string(),
                "onboarding_rotation_incomplete".to_string(),
            );
            self.append_system_run_note(run, payload, completed_at_epoch_ms)?;
            return Ok((None, 1));
        }

        let capture = match parse_onboarding_capture_document(&raw_output) {
            Ok(capture) => capture,
            Err(error) => {
                let mut payload = BTreeMap::new();
                payload.insert(
                    "event".to_string(),
                    "onboarding_rotation_parse_error".to_string(),
                );
                payload.insert("error".to_string(), error.to_string());
                self.append_system_run_note(run, payload, completed_at_epoch_ms)?;
                return Ok((None, 1));
            }
        };

        let completion = match self.apply_onboarding_capture_document(
            run,
            capture,
            format!("onboarding-rotation:{}", run.id),
            completed_at_epoch_ms,
            "Onboarding completed during checkpoint rotation.",
        ) {
            Ok(resolution) => resolution,
            Err(error) => {
                let mut payload = BTreeMap::new();
                payload.insert(
                    "event".to_string(),
                    "onboarding_rotation_apply_error".to_string(),
                );
                payload.insert("error".to_string(), error.to_string());
                self.append_system_run_note(run, payload, completed_at_epoch_ms)?;
                return Ok((None, 1));
            }
        };
        let mut payload = BTreeMap::new();
        payload.insert(
            "event".to_string(),
            "onboarding_rotation_applied".to_string(),
        );
        self.append_system_run_note(run, payload, completed_at_epoch_ms)?;
        Ok((Some(completion), 1))
    }

    fn apply_onboarding_capture_document(
        &mut self,
        run: &Run,
        capture: crab_core::OnboardingCaptureDocument,
        onboarding_session_id: String,
        completed_at_epoch_ms: u64,
        response_prefix: &str,
    ) -> CrabResult<OnboardingCompletionResolution> {
        let workspace_root = self.composition.startup.workspace_root.clone();
        let profile_write_outcome = persist_onboarding_profile_files(&workspace_root, &capture)?;
        let completion_outcome_result = execute_onboarding_completion_protocol(
            self,
            &workspace_root,
            &OnboardingCompletionInput {
                logical_session_id: run.logical_session_id.clone(),
                run_id: run.id.clone(),
                onboarding_session_id,
                completed_at_epoch_ms,
                capture,
                profile: Some(run.profile.clone()),
            },
        );
        let completion_outcome = completion_outcome_result?;

        let mut emitted_event_count = 1;
        if !profile_write_outcome.conflict_paths.is_empty() {
            let mut payload = BTreeMap::new();
            payload.insert(
                "event".to_string(),
                "onboarding_profile_conflicts".to_string(),
            );
            payload.insert(
                "conflict_paths".to_string(),
                profile_write_outcome.conflict_paths.join(","),
            );
            self.append_system_run_note(run, payload, completed_at_epoch_ms)?;
            emitted_event_count += 1;
        }

        let mut user_message = format!(
            "{response_prefix} BOOTSTRAP.md retired: {}.",
            completion_outcome.bootstrap_retired
        );
        if !profile_write_outcome.conflict_paths.is_empty() {
            user_message
                .push_str(" Profile conflicts detected; review the conflicted managed files.");
        }

        Ok(OnboardingCompletionResolution {
            user_message,
            emitted_event_count,
        })
    }

    fn resolve_manual_rotation_command(
        &mut self,
        run: &Run,
        session: &mut LogicalSession,
    ) -> CrabResult<Option<ManualRotationCommandResolution>> {
        let Some((command, manual_request)) = parse_manual_rotation_command(&run.user_input)?
        else {
            return Ok(None);
        };

        let mut operator_state = OperatorSessionState {
            active_backend: session.active_backend,
            active_profile: session.active_profile.clone(),
            active_physical_session_id: session.active_physical_session_id.clone(),
        };
        let actor = OperatorActorContext {
            sender_id: run.profile.sender_id.clone(),
            sender_is_owner: run.profile.sender_is_owner,
        };
        let outcome = apply_operator_command(&mut operator_state, &command, &actor)?;

        session.active_backend = operator_state.active_backend;
        session.active_profile = operator_state.active_profile;
        session.active_physical_session_id = operator_state.active_physical_session_id;

        let mut payload = BTreeMap::new();
        payload.insert(
            "operator_command".to_string(),
            operator_command_token(command).to_string(),
        );
        payload.insert(
            "requires_rotation".to_string(),
            outcome.requires_rotation.to_string(),
        );
        payload.insert(
            "manual_rotation_request".to_string(),
            manual_rotation_request_token(manual_request).to_string(),
        );
        payload.insert("sender_id".to_string(), run.profile.sender_id.clone());
        payload.insert(
            "sender_is_owner".to_string(),
            run.profile.sender_is_owner.to_string(),
        );
        payload.insert("message".to_string(), outcome.user_message.clone());
        let emitted_at_epoch_ms = self.runtime.now_epoch_ms()?;
        self.append_run_event(
            run,
            EventKind::RunNote,
            EventSource::System,
            payload,
            emitted_at_epoch_ms,
        )?;

        Ok(Some(ManualRotationCommandResolution {
            request: Some(manual_request),
            user_message: outcome.user_message,
        }))
    }

    fn maybe_execute_rotation(
        &mut self,
        run: &Run,
        session: &mut LogicalSession,
        now_epoch_ms: u64,
        manual_request: Option<ManualRotationRequest>,
    ) -> CrabResult<Option<RotationExecutionOutcome>> {
        self.maybe_execute_rotation_with_sabotage(run, session, now_epoch_ms, manual_request, None)
    }

    fn maybe_execute_rotation_with_sabotage(
        &mut self,
        run: &Run,
        session: &mut LogicalSession,
        now_epoch_ms: u64,
        manual_request: Option<ManualRotationRequest>,
        completed_event_log_sabotage_path: Option<PathBuf>,
    ) -> CrabResult<Option<RotationExecutionOutcome>> {
        let decision = evaluate_rotation_triggers(&RotationTriggerInput {
            now_epoch_ms,
            last_activity_epoch_ms: session.last_activity_epoch_ms,
            lane_is_idle: session.lane_state == LaneState::Idle,
            token_usage_total: Some(session.token_accounting.total_tokens),
            compaction_token_threshold: self.composition.rotation_policy.compaction_token_threshold,
            inactivity_timeout_secs: self.composition.rotation_policy.inactivity_timeout_secs,
            manual_request,
        })?;

        if !decision.should_rotate {
            return Ok(None);
        }

        let mut started_payload = BTreeMap::new();
        started_payload.insert("rotation_event".to_string(), "started".to_string());
        started_payload.insert(
            "triggers".to_string(),
            render_rotation_triggers(&decision.triggers),
        );
        self.append_run_event(
            run,
            EventKind::RunNote,
            EventSource::System,
            started_payload,
            now_epoch_ms,
        )?;

        #[cfg(not(coverage))]
        tracing::info!(
            logical_session_id = %run.logical_session_id,
            run_id = %run.id,
            triggers = %render_rotation_triggers(&decision.triggers),
            token_usage_total = session.token_accounting.total_tokens,
            "rotation started"
        );

        session.lane_state = LaneState::Rotating;
        self.composition
            .state_stores
            .session_store
            .upsert_session(session)?;

        let (onboarding_completion_resolution, onboarding_rotation_note_count) =
            self.maybe_complete_pending_onboarding_from_rotation(run, session, now_epoch_ms)?;

        let event_store = self.composition.state_stores.event_store.clone();
        let checkpoint_store = self.composition.state_stores.checkpoint_store.clone();
        let mut rotation_runtime = TurnExecutorRotationRuntime {
            run,
            logical_session: session,
            runtime: &mut self.runtime,
            codex_lifecycle: &mut self.composition.backends.codex,
            event_store,
            checkpoint_store,
            checkpoint_created_at_epoch_ms: now_epoch_ms,
            trigger_tokens: decision
                .triggers
                .iter()
                .map(|trigger| rotation_trigger_token(*trigger).to_string())
                .collect(),
            completed_event_log_sabotage_path,
        };
        let outcome = execute_rotation_sequence(&mut rotation_runtime)?;

        session.last_successful_checkpoint_id = Some(outcome.checkpoint_id.clone());
        session.lane_state = LaneState::Idle;

        let had_rotation_warnings = outcome.used_fallback_checkpoint
            || outcome.memory_flush_error.is_some()
            || outcome.checkpoint_turn_error.is_some();
        if had_rotation_warnings {
            #[cfg(not(coverage))]
            tracing::warn!(
                logical_session_id = %run.logical_session_id,
                run_id = %run.id,
                triggers = %render_rotation_triggers(&decision.triggers),
                checkpoint_id = %outcome.checkpoint_id,
                used_fallback_checkpoint = outcome.used_fallback_checkpoint,
                memory_flush_error = ?outcome.memory_flush_error,
                checkpoint_turn_error = ?outcome.checkpoint_turn_error,
                "rotation completed with warnings"
            );
        } else {
            #[cfg(not(coverage))]
            tracing::info!(
                logical_session_id = %run.logical_session_id,
                run_id = %run.id,
                triggers = %render_rotation_triggers(&decision.triggers),
                checkpoint_id = %outcome.checkpoint_id,
                "rotation completed"
            );
        }

        let mut completed_payload = BTreeMap::new();
        completed_payload.insert("rotation_event".to_string(), "completed".to_string());
        completed_payload.insert("checkpoint_id".to_string(), outcome.checkpoint_id.clone());
        completed_payload.insert(
            "used_fallback_checkpoint".to_string(),
            outcome.used_fallback_checkpoint.to_string(),
        );
        completed_payload.insert(
            "memory_flush_error".to_string(),
            outcome
                .memory_flush_error
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        );
        completed_payload.insert(
            "checkpoint_turn_error".to_string(),
            outcome
                .checkpoint_turn_error
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        );
        completed_payload.insert(
            "triggers".to_string(),
            render_rotation_triggers(&decision.triggers),
        );
        self.append_run_event(
            run,
            EventKind::RunNote,
            EventSource::System,
            completed_payload,
            now_epoch_ms,
        )?;

        Ok(Some(RotationExecutionOutcome {
            checkpoint_id: outcome.checkpoint_id,
            onboarding_completion_resolution,
            supplemental_emitted_event_count: onboarding_rotation_note_count,
        }))
    }

    fn mark_run_failed(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
        cause: &CrabError,
    ) -> CrabResult<()> {
        let now_epoch_ms = self.runtime.now_epoch_ms()?;
        let mut run = self.load_required_run(logical_session_id, run_id)?;
        run.status = RunStatus::Failed;
        run.completed_at_epoch_ms = Some(now_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut session = self.load_or_initialize_session(
            logical_session_id,
            &run.profile.resolved_profile,
            now_epoch_ms,
        )?;
        session.lane_state = LaneState::Idle;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = now_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(&run, "failed", now_epoch_ms, Some(cause.to_string()))
    }

    fn maybe_persist_workspace_git_commits(
        &self,
        run: &Run,
        final_status: RunStatus,
        completed_at_epoch_ms: u64,
        rotation_outcome: Option<&RotationExecutionOutcome>,
    ) {
        if final_status != RunStatus::Succeeded {
            return;
        }

        if let Some(rotation_outcome) = rotation_outcome {
            self.try_persist_workspace_git_commit(
                run,
                WorkspaceGitCommitTrigger::RotationCheckpoint,
                Some(rotation_outcome.checkpoint_id.as_str()),
                Some(run_status_token(final_status)),
                completed_at_epoch_ms,
            );
        }

        self.try_persist_workspace_git_commit(
            run,
            WorkspaceGitCommitTrigger::RunFinalized,
            None,
            Some(run_status_token(final_status)),
            completed_at_epoch_ms,
        );
    }

    fn try_persist_workspace_git_commit(
        &self,
        run: &Run,
        trigger: WorkspaceGitCommitTrigger,
        checkpoint_id: Option<&str>,
        run_status: Option<&str>,
        emitted_at_epoch_ms: u64,
    ) {
        let request = WorkspaceGitCommitRequest {
            logical_session_id: run.logical_session_id.clone(),
            run_id: run.id.clone(),
            trigger,
            checkpoint_id: checkpoint_id.map(ToOwned::to_owned),
            run_status: run_status.map(ToOwned::to_owned),
            emitted_at_epoch_ms,
        };

        let result = maybe_commit_workspace_snapshot(
            &self.composition.startup.workspace_root,
            &self.composition.workspace_git,
            &request,
        );
        let _ = result.as_ref().map(|outcome| {
            self.maybe_enqueue_workspace_git_push(run, trigger, emitted_at_epoch_ms, outcome);
            #[cfg(not(coverage))]
            if !outcome.staging_skipped_paths.is_empty() {
                tracing::info!(
                    logical_session_id = %run.logical_session_id,
                    run_id = %run.id,
                    trigger = %trigger.as_token(),
                    skipped_paths = ?outcome.staging_skipped_paths,
                    "workspace git staging policy skipped paths"
                );
            }
        });
        #[cfg(not(coverage))]
        if let Err(_error) = &result {
            tracing::warn!(
                logical_session_id = %run.logical_session_id,
                run_id = %run.id,
                trigger = %trigger.as_token(),
                error = %_error,
                "workspace git commit persistence failed"
            );
        }
    }

    fn maybe_enqueue_workspace_git_push(
        &self,
        run: &Run,
        trigger: WorkspaceGitCommitTrigger,
        emitted_at_epoch_ms: u64,
        commit_outcome: &crab_core::WorkspaceGitCommitOutcome,
    ) {
        #[cfg(coverage)]
        let _ = (run, trigger);

        if !commit_outcome.enabled {
            return;
        }
        if self.composition.workspace_git.push_policy != crab_core::WorkspaceGitPushPolicy::OnCommit
        {
            return;
        }

        let Some(commit_key) = commit_outcome.commit_key.as_deref() else {
            return;
        };
        let Some(commit_id) = commit_outcome.commit_id.as_deref() else {
            return;
        };

        let push_request = WorkspaceGitPushRequest {
            commit_key: commit_key.to_string(),
            commit_id: commit_id.to_string(),
            enqueued_at_epoch_ms: emitted_at_epoch_ms,
        };
        let enqueue_result = enqueue_workspace_git_push_request(
            &self.composition.state_stores.root,
            &self.composition.workspace_git,
            &push_request,
        );
        if let Err(_error) = enqueue_result {
            #[cfg(not(coverage))]
            tracing::warn!(
                logical_session_id = %run.logical_session_id,
                run_id = %run.id,
                trigger = %trigger.as_token(),
                commit_key = %commit_key,
                error = %_error,
                "workspace git push queue enqueue failed"
            );
        }
    }

    fn load_required_run(&self, logical_session_id: &str, run_id: &str) -> CrabResult<Run> {
        self.composition
            .state_stores
            .run_store
            .get_run(logical_session_id, run_id)?
            .ok_or_else(|| CrabError::InvariantViolation {
                context: "turn_executor_run_lookup",
                message: format!("run {logical_session_id}/{run_id} not found"),
            })
    }

    fn load_or_initialize_session(
        &self,
        logical_session_id: &str,
        profile: &InferenceProfile,
        now_epoch_ms: u64,
    ) -> CrabResult<LogicalSession> {
        if let Some(existing) = self
            .composition
            .state_stores
            .session_store
            .get_session(logical_session_id)?
        {
            return Ok(existing);
        }

        Ok(LogicalSession {
            id: logical_session_id.to_string(),
            active_backend: profile.backend,
            active_profile: profile.clone(),
            active_physical_session_id: None,
            last_successful_checkpoint_id: None,
            lane_state: LaneState::Idle,
            queued_run_count: 0,
            last_activity_epoch_ms: now_epoch_ms,
            token_accounting: TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
            },
        })
    }

    fn append_backend_event(
        &mut self,
        run: &Run,
        backend_event: &BackendEvent,
        emitted_at_epoch_ms: u64,
    ) -> CrabResult<()> {
        let kind = map_backend_event_kind(backend_event.kind);
        let mut payload = backend_event.payload.clone();
        payload.insert(
            "backend_sequence".to_string(),
            backend_event.sequence.to_string(),
        );
        if let Some(state_token) = backend_state_token(backend_event.kind) {
            payload.insert("state".to_string(), state_token.to_string());
        }
        self.append_run_event(
            run,
            kind,
            EventSource::Backend,
            payload,
            emitted_at_epoch_ms,
        )
    }

    fn deliver_rendered_assistant_output(
        &mut self,
        run: &Run,
        message_id: &str,
        rendered_output: &str,
        edit_generation: u32,
        delivered_at_epoch_ms: u64,
    ) -> CrabResult<bool> {
        if rendered_output.trim().is_empty() {
            return Ok(false);
        }

        let channel_id = match run.delivery_channel_id.as_deref() {
            Some(channel_id) => channel_id.to_string(),
            None => delivery_channel_id(&run.logical_session_id)?,
        };
        let message_id = message_id.to_string();

        let attempt = DeliveryAttempt {
            logical_session_id: run.logical_session_id.clone(),
            run_id: run.id.clone(),
            channel_id: channel_id.clone(),
            message_id: message_id.clone(),
            edit_generation,
            content: rendered_output.to_string(),
            delivered_at_epoch_ms,
        };

        match self.composition.delivery_ledger.should_send(&attempt)? {
            ShouldSendDecision::SkipDuplicate => return Ok(false),
            ShouldSendDecision::Send => {}
        }

        self.runtime.deliver_assistant_output(
            run,
            &channel_id,
            &message_id,
            edit_generation,
            rendered_output,
        )?;
        let _ = self.composition.delivery_ledger.mark_sent(&attempt)?;
        Ok(true)
    }

    fn append_run_state_event(
        &mut self,
        run: &Run,
        state: &str,
        emitted_at_epoch_ms: u64,
        reason: Option<String>,
    ) -> CrabResult<()> {
        let mut payload = BTreeMap::new();
        payload.insert("state".to_string(), state.to_string());
        if let Some(reason) = reason {
            payload.insert("reason".to_string(), reason);
        }
        self.append_run_event(
            run,
            EventKind::RunState,
            EventSource::System,
            payload,
            emitted_at_epoch_ms,
        )
    }

    fn append_run_event(
        &self,
        run: &Run,
        kind: EventKind,
        source: EventSource,
        payload: BTreeMap<String, String>,
        emitted_at_epoch_ms: u64,
    ) -> CrabResult<()> {
        let sequence = self.next_event_sequence(&run.logical_session_id, &run.id)?;
        let event = EventEnvelope {
            event_id: format!(
                "evt:turn-executor:{}:{}:{}",
                run.logical_session_id, run.id, sequence
            ),
            run_id: run.id.clone(),
            turn_id: Some(build_turn_id(&run.id)),
            lane_id: Some(run.logical_session_id.clone()),
            logical_session_id: run.logical_session_id.clone(),
            physical_session_id: run.physical_session_id.clone(),
            backend: Some(run.profile.resolved_profile.backend),
            resolved_model: Some(run.profile.resolved_profile.model.clone()),
            resolved_reasoning_level: Some(
                run.profile
                    .resolved_profile
                    .reasoning_level
                    .as_token()
                    .to_string(),
            ),
            profile_source: Some(run.profile.profile_source_token().to_string()),
            sequence,
            emitted_at_epoch_ms,
            source,
            kind,
            payload,
            profile: Some(run.profile.clone()),
            idempotency_key: Some(format!(
                "turn-executor:{}:{}:{}",
                run.logical_session_id, run.id, sequence
            )),
        };
        self.composition
            .state_stores
            .event_store
            .append_event(&event)
    }

    fn append_system_run_note(
        &self,
        run: &Run,
        payload: BTreeMap<String, String>,
        emitted_at_epoch_ms: u64,
    ) -> CrabResult<()> {
        self.append_run_event(
            run,
            EventKind::RunNote,
            EventSource::System,
            payload,
            emitted_at_epoch_ms,
        )
    }

    fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64> {
        let events = self
            .composition
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)?;
        Ok(events.last().map_or(1, |event| event.sequence + 1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManualRotationCommandResolution {
    request: Option<ManualRotationRequest>,
    user_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OnboardingCompletionResolution {
    user_message: String,
    emitted_event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingOnboardingGateResolution {
    user_message: String,
    emitted_event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RotationExecutionOutcome {
    checkpoint_id: String,
    onboarding_completion_resolution: Option<OnboardingCompletionResolution>,
    supplemental_emitted_event_count: usize,
}

struct TurnExecutorRotationRuntime<'a, R>
where
    R: TurnExecutorRuntime,
{
    run: &'a Run,
    logical_session: &'a mut LogicalSession,
    runtime: &'a mut R,
    codex_lifecycle: &'a mut dyn CodexLifecycleManager,
    event_store: EventStore,
    checkpoint_store: CheckpointStore,
    checkpoint_created_at_epoch_ms: u64,
    trigger_tokens: Vec<String>,
    completed_event_log_sabotage_path: Option<PathBuf>,
}

impl<R> RotationSequenceRuntime for TurnExecutorRotationRuntime<'_, R>
where
    R: TurnExecutorRuntime,
{
    fn run_hidden_memory_flush(&mut self) -> CrabResult<()> {
        let cycle_id = format!("{}:{}", self.run.id, self.checkpoint_created_at_epoch_ms);
        let _prompt = build_memory_flush_prompt(&cycle_id)
            .expect("turn rotation runtime always uses a non-empty cycle id");
        let _ = finalize_hidden_memory_flush("NO_REPLY")
            .expect("turn rotation runtime uses a known-good hidden flush ack token");
        Ok(())
    }

    fn run_hidden_checkpoint_turn(&mut self) -> CrabResult<CheckpointTurnDocument> {
        let mut physical_session = self.runtime.ensure_physical_session(
            &self.run.logical_session_id,
            &self.run.profile.resolved_profile,
            self.logical_session.active_physical_session_id.as_deref(),
        )?;
        self.logical_session.active_backend = self.run.profile.resolved_profile.backend;
        self.logical_session.active_profile = self.run.profile.resolved_profile.clone();
        self.logical_session.active_physical_session_id = Some(physical_session.id.clone());

        let max_attempts = DEFAULT_CHECKPOINT_MAX_ATTEMPTS;
        let mut prompt = build_checkpoint_prompt();
        let mut attempt = 1_u8;
        loop {
            let mut hidden_checkpoint_run = self.run.clone();
            hidden_checkpoint_run.user_input = prompt.clone();
            hidden_checkpoint_run.physical_session_id = Some(physical_session.id.clone());

            let hidden_turn_id = format!("turn:{}:hidden-checkpoint:{attempt}", self.run.id);
            let mut backend_event_stream = self.runtime.execute_backend_turn(
                self.codex_lifecycle,
                &mut physical_session,
                &hidden_checkpoint_run,
                &hidden_turn_id,
                &prompt,
            )?;
            let mut raw_output = String::new();
            loop {
                let next = block_on(poll_fn(|cx| backend_event_stream.as_mut().poll_next(cx)));
                let Some(backend_event) = next else {
                    break;
                };
                if let Some(delta) = extract_backend_text_delta(&backend_event) {
                    raw_output.push_str(delta);
                }
            }

            let resolution = resolve_checkpoint_turn_output(&raw_output, attempt, max_attempts)?;
            match resolution {
                CheckpointTurnResolution::Parsed(document) => return Ok(document),
                CheckpointTurnResolution::Retry {
                    corrective_prompt, ..
                } => {
                    prompt = corrective_prompt;
                    attempt = attempt.saturating_add(1);
                }
                CheckpointTurnResolution::Exhausted { error, .. } => {
                    return Err(CrabError::InvariantViolation {
                        context: "turn_executor_rotation_checkpoint_turn",
                        message: format!(
                            "hidden checkpoint backend output failed strict checkpoint schema validation: {error}"
                        ),
                    });
                }
            }
        }
    }

    fn build_fallback_checkpoint(&mut self) -> CrabResult<CheckpointTurnDocument> {
        let events = self
            .event_store
            .replay_run(&self.run.logical_session_id, &self.run.id)
            .unwrap_or_default();
        let transcript = build_fallback_transcript_entries(self.run, &events);

        let mut metadata = BTreeMap::new();
        metadata.insert(
            "backend".to_string(),
            backend_kind_token(self.run.profile.resolved_profile.backend).to_string(),
        );
        metadata.insert(
            "model".to_string(),
            self.run.profile.resolved_profile.model.clone(),
        );
        metadata.insert(
            "reasoning_level".to_string(),
            self.run
                .profile
                .resolved_profile
                .reasoning_level
                .as_token()
                .to_string(),
        );
        metadata.insert(
            "rotation_triggers".to_string(),
            self.trigger_tokens.join(","),
        );
        metadata.insert(
            "last_successful_checkpoint_id".to_string(),
            self.logical_session
                .last_successful_checkpoint_id
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        );

        build_fallback_checkpoint_document(
            &transcript,
            &metadata,
            DEFAULT_FALLBACK_TRANSCRIPT_TAIL_LIMIT,
        )
    }

    fn persist_checkpoint(&mut self, checkpoint: &CheckpointTurnDocument) -> CrabResult<String> {
        let checkpoint_id = format!(
            "ckpt:{}:{}",
            self.run.id, self.checkpoint_created_at_epoch_ms
        );
        let mut state = BTreeMap::new();
        state.insert(
            "decisions_count".to_string(),
            checkpoint.decisions.len().to_string(),
        );
        state.insert(
            "open_questions_count".to_string(),
            checkpoint.open_questions.len().to_string(),
        );
        state.insert(
            "next_actions_count".to_string(),
            checkpoint.next_actions.len().to_string(),
        );
        state.insert(
            "artifacts_count".to_string(),
            checkpoint.artifacts.len().to_string(),
        );
        state.insert(
            "rotation_triggers".to_string(),
            self.trigger_tokens.join(","),
        );

        self.checkpoint_store.put_checkpoint(&Checkpoint {
            id: checkpoint_id.clone(),
            logical_session_id: self.run.logical_session_id.clone(),
            run_id: self.run.id.clone(),
            created_at_epoch_ms: self.checkpoint_created_at_epoch_ms,
            summary: checkpoint.summary.clone(),
            memory_digest: format!(
                "fallback:{}:{}:{}",
                checkpoint.summary.len(),
                checkpoint.decisions.len(),
                checkpoint.artifacts.len()
            ),
            state,
        })?;

        Ok(checkpoint_id)
    }

    fn end_physical_session(&mut self) -> CrabResult<()> {
        Ok(())
    }

    fn clear_active_physical_session(&mut self) -> CrabResult<()> {
        if let Some(path) = self.completed_event_log_sabotage_path.take() {
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).map_err(|error| CrabError::Io {
                context: "turn_executor_rotation_sabotage",
                path: Some(path.to_string_lossy().into_owned()),
                message: error.to_string(),
            })?;
        }
        self.logical_session.active_physical_session_id = None;
        // Token-triggered compaction should be based on tokens since last successful rotation.
        self.logical_session.token_accounting = TokenAccounting {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
        };
        Ok(())
    }
}

fn build_run_id(logical_session_id: &str, message_id: &str) -> String {
    format!("run:{logical_session_id}:{}", message_id.trim())
}

fn build_turn_id(run_id: &str) -> String {
    format!("turn:{run_id}")
}

fn is_dm_logical_session_id(logical_session_id: &str) -> bool {
    logical_session_id.starts_with("discord:dm:")
}

fn looks_like_onboarding_capture_payload(input: &str) -> bool {
    let trimmed = input.trim();
    trimmed.starts_with('{')
        && trimmed.contains("\"schema_version\"")
        && trimmed.contains("\"agent_identity\"")
}

fn parse_manual_rotation_command(
    input: &str,
) -> CrabResult<Option<(OperatorCommand, ManualRotationRequest)>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let lowered = trimmed.to_ascii_lowercase();
    if !lowered.starts_with("/compact") && !lowered.starts_with("/reset") {
        return Ok(None);
    }

    let parsed = parse_operator_command(trimmed)?;
    match parsed {
        Some(OperatorCommand::ManualCompact) => Ok(Some((
            OperatorCommand::ManualCompact,
            ManualRotationRequest::Compact,
        ))),
        Some(OperatorCommand::ManualReset) => Ok(Some((
            OperatorCommand::ManualReset,
            ManualRotationRequest::Reset,
        ))),
        Some(_) | None => Ok(None),
    }
}

fn operator_command_token(command: OperatorCommand) -> &'static str {
    match command {
        OperatorCommand::ManualCompact => "/compact",
        OperatorCommand::ManualReset => "/reset",
        OperatorCommand::SetBackend { .. }
        | OperatorCommand::SetModel { .. }
        | OperatorCommand::SetReasoning { .. }
        | OperatorCommand::ShowProfile
        | OperatorCommand::OnboardingRerun
        | OperatorCommand::OnboardingResetBootstrap => "other",
    }
}

fn manual_rotation_request_token(request: ManualRotationRequest) -> &'static str {
    match request {
        ManualRotationRequest::Compact => "compact",
        ManualRotationRequest::Reset => "reset",
    }
}

fn rotation_trigger_token(trigger: RotationTrigger) -> &'static str {
    match trigger {
        RotationTrigger::ManualCompact => "manual_compact",
        RotationTrigger::ManualReset => "manual_reset",
        RotationTrigger::TokenCompaction => "token_compaction",
        RotationTrigger::InactivityTimeout => "inactivity_timeout",
    }
}

fn render_rotation_triggers(triggers: &[RotationTrigger]) -> String {
    triggers
        .iter()
        .map(|trigger| rotation_trigger_token(*trigger))
        .collect::<Vec<_>>()
        .join(",")
}

fn backend_kind_token(backend: crab_core::BackendKind) -> &'static str {
    match backend {
        crab_core::BackendKind::Claude => "claude",
        crab_core::BackendKind::Codex => "codex",
        crab_core::BackendKind::OpenCode => "opencode",
    }
}

fn build_fallback_transcript_entries(run: &Run, events: &[EventEnvelope]) -> Vec<TranscriptEntry> {
    let mut transcript = vec![TranscriptEntry {
        role: TranscriptEntryRole::User,
        text: run.user_input.clone(),
    }];

    for event in events {
        match event.kind {
            EventKind::TextDelta => {
                if let Some(delta) = extract_event_text_delta(event) {
                    transcript.push(TranscriptEntry {
                        role: TranscriptEntryRole::Assistant,
                        text: delta.to_string(),
                    });
                }
            }
            EventKind::ToolCall | EventKind::ToolResult => {
                let rendered = event
                    .payload
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                if !rendered.trim().is_empty() {
                    transcript.push(TranscriptEntry {
                        role: TranscriptEntryRole::Tool,
                        text: rendered,
                    });
                }
            }
            EventKind::RunNote => {
                let rendered = event
                    .payload
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                if !rendered.trim().is_empty() {
                    transcript.push(TranscriptEntry {
                        role: TranscriptEntryRole::System,
                        text: rendered,
                    });
                }
            }
            EventKind::RunState
            | EventKind::ApprovalRequest
            | EventKind::ApprovalDecision
            | EventKind::Heartbeat
            | EventKind::Error => {}
        }
    }

    transcript
}

fn run_status_token(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn derive_final_status(events: &[BackendEvent]) -> RunStatus {
    if events
        .iter()
        .any(|event| event.kind == BackendEventKind::Error)
    {
        return RunStatus::Failed;
    }
    if events
        .iter()
        .any(|event| event.kind == BackendEventKind::TurnInterrupted)
    {
        return RunStatus::Cancelled;
    }
    RunStatus::Succeeded
}

fn map_backend_event_kind(kind: BackendEventKind) -> EventKind {
    match kind {
        BackendEventKind::TextDelta => EventKind::TextDelta,
        BackendEventKind::ToolCall => EventKind::ToolCall,
        BackendEventKind::ToolResult => EventKind::ToolResult,
        BackendEventKind::RunNote => EventKind::RunNote,
        BackendEventKind::TurnCompleted | BackendEventKind::TurnInterrupted => EventKind::RunState,
        BackendEventKind::Error => EventKind::Error,
    }
}

fn backend_state_token(kind: BackendEventKind) -> Option<&'static str> {
    match kind {
        BackendEventKind::TurnCompleted => Some("succeeded"),
        BackendEventKind::TurnInterrupted => Some("cancelled"),
        _ => None,
    }
}

fn extract_backend_text_delta(event: &BackendEvent) -> Option<&str> {
    if event.kind != BackendEventKind::TextDelta {
        return None;
    }
    event
        .payload
        .get("text")
        .or_else(|| event.payload.get("delta"))
        .map(String::as_str)
}

fn extract_event_text_delta(event: &EventEnvelope) -> Option<&str> {
    if event.kind != EventKind::TextDelta {
        return None;
    }
    event
        .payload
        .get("text")
        .or_else(|| event.payload.get("delta"))
        .map(String::as_str)
}

fn resolve_backend_usage_accounting(
    events: &[BackendEvent],
) -> CrabResult<Option<TokenAccounting>> {
    let mut latest_usage = None;
    for event in events {
        if let Some(usage) = parse_usage_from_backend_payload(event.kind, &event.payload)? {
            latest_usage = Some(usage);
        }
    }
    Ok(latest_usage)
}

fn parse_usage_from_backend_payload(
    kind: BackendEventKind,
    payload: &BTreeMap<String, String>,
) -> CrabResult<Option<TokenAccounting>> {
    if !matches!(
        kind,
        BackendEventKind::RunNote
            | BackendEventKind::TurnCompleted
            | BackendEventKind::TurnInterrupted
            | BackendEventKind::Error
    ) {
        return Ok(None);
    }

    if let Some(usage) = parse_usage_triplet(
        payload,
        "run_usage_input_tokens",
        "run_usage_output_tokens",
        "run_usage_total_tokens",
    )? {
        return Ok(Some(usage));
    }
    if let Some(usage) = parse_usage_triplet(
        payload,
        "usage_input_tokens",
        "usage_output_tokens",
        "usage_total_tokens",
    )? {
        return Ok(Some(usage));
    }
    parse_usage_triplet(payload, "input_tokens", "output_tokens", "total_tokens")
}

fn parse_usage_triplet(
    payload: &BTreeMap<String, String>,
    input_key: &'static str,
    output_key: &'static str,
    total_key: &'static str,
) -> CrabResult<Option<TokenAccounting>> {
    let has_any = payload.contains_key(input_key)
        || payload.contains_key(output_key)
        || payload.contains_key(total_key);
    if !has_any {
        return Ok(None);
    }

    let parse_required = |key: &'static str| -> CrabResult<u64> {
        let raw_value = payload
            .get(key)
            .ok_or_else(|| CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: format!(
                    "usage payload is missing required key {key} while parsing {input_key}/{output_key}/{total_key}"
                ),
            })?;
        raw_value
            .parse::<u64>()
            .map_err(|_| CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: format!("usage payload key {key} must be an unsigned integer"),
            })
    };

    let input_tokens = parse_required(input_key)?;
    let output_tokens = parse_required(output_key)?;
    let total_tokens = parse_required(total_key)?;

    let minimum_total =
        input_tokens
            .checked_add(output_tokens)
            .ok_or(CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: format!(
                    "usage payload overflow while adding {input_key} and {output_key}"
                ),
            })?;
    if total_tokens < minimum_total {
        return Err(CrabError::InvariantViolation {
            context: "turn_executor_usage_accounting",
            message: format!(
                "{total_key} {total_tokens} must be greater than or equal to {input_key} + {output_key} {minimum_total}"
            ),
        });
    }

    Ok(Some(TokenAccounting {
        input_tokens,
        output_tokens,
        total_tokens,
    }))
}

fn merge_token_accounting(
    existing: TokenAccounting,
    increment: TokenAccounting,
) -> CrabResult<TokenAccounting> {
    let input_tokens = existing
        .input_tokens
        .checked_add(increment.input_tokens)
        .ok_or(CrabError::InvariantViolation {
            context: "turn_executor_usage_accounting",
            message: "input token accounting overflow".to_string(),
        })?;
    let output_tokens = existing
        .output_tokens
        .checked_add(increment.output_tokens)
        .ok_or(CrabError::InvariantViolation {
            context: "turn_executor_usage_accounting",
            message: "output token accounting overflow".to_string(),
        })?;
    let total_tokens = existing
        .total_tokens
        .checked_add(increment.total_tokens)
        .ok_or(CrabError::InvariantViolation {
            context: "turn_executor_usage_accounting",
            message: "total token accounting overflow".to_string(),
        })?;
    Ok(TokenAccounting {
        input_tokens,
        output_tokens,
        total_tokens,
    })
}

fn delivery_channel_id(logical_session_id: &str) -> CrabResult<String> {
    let mut parts = logical_session_id.split(':');
    let scheme = parts.next();
    let conversation_kind = parts.next();
    let provider_scoped_id = parts.next();
    let extra = parts.next();

    if scheme != Some("discord")
        || !matches!(conversation_kind, Some("channel" | "thread" | "dm"))
        || extra.is_some()
    {
        return Err(CrabError::InvariantViolation {
            context: "turn_executor_delivery_target",
            message: format!("unsupported logical_session_id shape: {logical_session_id}"),
        });
    }

    if conversation_kind == Some("dm") {
        return Err(CrabError::InvariantViolation {
            context: "turn_executor_delivery_target",
            message: format!(
                "direct messages require a delivery_channel_id (DM channel id) and cannot be derived from logical_session_id: {logical_session_id}"
            ),
        });
    }

    let provider_scoped_id = provider_scoped_id.ok_or_else(|| CrabError::InvariantViolation {
        context: "turn_executor_delivery_target",
        message: format!("unsupported logical_session_id shape: {logical_session_id}"),
    })?;
    let trimmed = provider_scoped_id.trim();
    if trimmed.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "turn_executor_delivery_target",
            message: format!("unsupported logical_session_id shape: {logical_session_id}"),
        });
    }
    Ok(trimmed.to_string())
}

fn delivery_message_id(run_id: &str, chunk_index: u32) -> String {
    format!("delivery:{run_id}:chunk:{chunk_index}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedAssistantDelivery {
    chunk_index: u32,
    edit_generation: u32,
    content: String,
}

/// Splits assistant output into consecutive Discord messages.
///
/// Policy:
/// - A blank line (`\n\n`) finalizes the current message chunk and starts a new one.
/// - Any chunk is additionally split at Discord's 2000-char limit.
/// - Only the active chunk is edited as more text arrives; prior chunks are posted once.
#[derive(Debug, Default)]
struct DiscordAssistantDelivery {
    next_chunk_index: u32,
    active_content: String,
    active_edit_generation: u32,
    active_last_delivered: String,
    total_rendered_len: usize,
}

impl DiscordAssistantDelivery {
    fn new() -> Self {
        Self::default()
    }

    fn push_delta(&mut self, delta: &str) -> Vec<PlannedAssistantDelivery> {
        if delta.is_empty() {
            return Vec::new();
        }

        self.active_content.push_str(delta);
        self.total_rendered_len = self.total_rendered_len.saturating_add(delta.len());

        let mut planned = Vec::new();
        self.flush_completed_chunks(&mut planned);

        if self.active_content.trim().is_empty() {
            return planned;
        }

        planned.push(PlannedAssistantDelivery {
            chunk_index: self.next_chunk_index,
            edit_generation: self.active_edit_generation,
            content: self.active_content.clone(),
        });
        self.active_last_delivered = self.active_content.clone();
        self.active_edit_generation = self.active_edit_generation.saturating_add(1);
        planned
    }

    fn flush_completed_chunks(&mut self, planned: &mut Vec<PlannedAssistantDelivery>) {
        const SECTION_DELIMITER: &str = "\n\n";

        loop {
            if let Some(pos) = self.active_content.find(SECTION_DELIMITER) {
                let prefix = self.active_content[..pos].to_string();
                let remainder = self.active_content[pos + SECTION_DELIMITER.len()..].to_string();
                let flushed = self.flush_current_prefix(&prefix, planned);
                // Leading/repeated delimiters should not create empty chunks.
                if flushed {
                    self.advance_chunk();
                }
                self.active_content = remainder;
                continue;
            }

            if self.active_content.chars().count() > crab_discord::DISCORD_MESSAGE_CHAR_LIMIT {
                let (prefix, remainder) = split_at_char_limit(
                    &self.active_content,
                    crab_discord::DISCORD_MESSAGE_CHAR_LIMIT,
                );
                let _ = self.flush_current_prefix(&prefix, planned);
                self.advance_chunk();
                self.active_content = remainder;
                continue;
            }

            break;
        }
    }

    fn flush_current_prefix(
        &mut self,
        prefix: &str,
        planned: &mut Vec<PlannedAssistantDelivery>,
    ) -> bool {
        let prefix = prefix.trim_end_matches('\n');
        if prefix.trim().is_empty() {
            return false;
        }

        if prefix == self.active_last_delivered {
            return true;
        }

        planned.push(PlannedAssistantDelivery {
            chunk_index: self.next_chunk_index,
            edit_generation: self.active_edit_generation,
            content: prefix.to_string(),
        });
        self.active_last_delivered = prefix.to_string();
        self.active_edit_generation = self.active_edit_generation.saturating_add(1);
        true
    }

    fn advance_chunk(&mut self) {
        self.next_chunk_index = self.next_chunk_index.saturating_add(1);
        self.active_content.clear();
        self.active_edit_generation = 0;
        self.active_last_delivered.clear();
    }
}

fn split_at_char_limit(value: &str, limit: usize) -> (String, String) {
    if limit == 0 {
        return ("".to_string(), value.to_string());
    }

    let mut split_at = value.len();
    let mut seen = 0usize;
    for (byte_index, _) in value.char_indices() {
        if seen == limit {
            split_at = byte_index;
            break;
        }
        seen = seen.saturating_add(1);
    }
    if seen < limit {
        split_at = value.len();
    }

    let (prefix, remainder) = value.split_at(split_at);
    (prefix.to_string(), remainder.to_string())
}

impl<CP, OP, R> OnboardingCompletionEventRuntime for TurnExecutor<CP, OP, R>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    R: TurnExecutorRuntime,
{
    fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64> {
        TurnExecutor::next_event_sequence(self, logical_session_id, run_id)
    }

    fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()> {
        self.composition
            .state_stores
            .event_store
            .append_event(event)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::fs;
    use std::path::{Path, PathBuf};

    use crab_backends::BackendEventKind;
    use crab_core::{
        build_checkpoint_prompt, BackendKind, CrabError, CrabResult, EventKind, InferenceProfile,
        LaneState, OwnerProfileMetadata, ProfileValueSource, ReasoningLevel, RunProfileTelemetry,
        RunStatus, WorkspaceGitPushPolicy, ONBOARDING_CAPTURE_INCOMPLETE_TOKEN,
    };
    use crab_discord::{GatewayConversationKind, GatewayMessage};
    use futures::stream;

    use super::{DispatchedTurn, QueuedTurn, TurnExecutor, TurnExecutorRuntime};
    use crate::composition::compose_runtime_with_processes_and_queue_limit;
    use crate::test_support::{
        runtime_config_for_workspace_with_lanes, FakeCodexProcess, FakeOpenCodeProcess,
        TempWorkspace,
    };

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        now_epochs: VecDeque<u64>,
        resolve_profile_results: VecDeque<CrabResult<RunProfileTelemetry>>,
        ensure_session_results: VecDeque<CrabResult<crab_core::PhysicalSession>>,
        build_context_results: VecDeque<CrabResult<String>>,
        execute_turn_results: VecDeque<CrabResult<Vec<crab_backends::BackendEvent>>>,
        deliver_results: VecDeque<CrabResult<()>>,
        ensure_session_sabotage_path: Option<PathBuf>,
        deliver_sabotage_path: Option<PathBuf>,
        now_epoch_sabotage: Option<(usize, PathBuf)>,
        now_epoch_call_count: usize,
        delivered_outputs: Vec<(String, String, String, u32, String)>,
        executed_turn_contexts: Vec<(String, String)>,
        build_context_bootstrap_flags: Vec<bool>,
        steps: Vec<String>,
    }

    impl FakeRuntime {
        fn with_backend_events(
            backend_events: Vec<crab_backends::BackendEvent>,
            now_epochs: &[u64],
        ) -> Self {
            let session = physical_session_fixture();
            Self {
                now_epochs: VecDeque::from(now_epochs.to_vec()),
                resolve_profile_results: VecDeque::from(vec![Ok(sample_profile_telemetry())]),
                ensure_session_results: VecDeque::from(vec![Ok(session)]),
                build_context_results: VecDeque::from(vec![Ok("context".to_string())]),
                execute_turn_results: VecDeque::from(vec![Ok(backend_events)]),
                deliver_results: VecDeque::new(),
                ensure_session_sabotage_path: None,
                deliver_sabotage_path: None,
                now_epoch_sabotage: None,
                now_epoch_call_count: 0,
                delivered_outputs: Vec::new(),
                executed_turn_contexts: Vec::new(),
                build_context_bootstrap_flags: Vec::new(),
                steps: Vec::new(),
            }
        }

        fn with_ensure_session_sabotage_path(mut self, path: PathBuf) -> Self {
            self.ensure_session_sabotage_path = Some(path);
            self
        }

        fn with_now_epoch_sabotage(mut self, call_index: usize, path: PathBuf) -> Self {
            self.now_epoch_sabotage = Some((call_index, path));
            self
        }

        fn with_delivery_results(mut self, results: Vec<CrabResult<()>>) -> Self {
            self.deliver_results = VecDeque::from(results);
            self
        }

        fn with_delivery_sabotage_path(mut self, path: PathBuf) -> Self {
            self.deliver_sabotage_path = Some(path);
            self
        }

        fn pop_result<T: Clone>(
            queue: &mut VecDeque<CrabResult<T>>,
            context: &'static str,
        ) -> CrabResult<T> {
            match queue.pop_front() {
                Some(result) => result,
                None => Err(CrabError::InvariantViolation {
                    context,
                    message: "missing scripted runtime result".to_string(),
                }),
            }
        }
    }

    impl TurnExecutorRuntime for FakeRuntime {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            self.now_epoch_call_count += 1;
            if let Some((trigger_call, path)) = self.now_epoch_sabotage.as_ref() {
                if *trigger_call == self.now_epoch_call_count {
                    replace_path_with_directory(path);
                }
            }
            match self.now_epochs.pop_front() {
                Some(value) => Ok(value),
                None => Err(CrabError::InvariantViolation {
                    context: "turn_executor_test_clock",
                    message: "missing scripted timestamp".to_string(),
                }),
            }
        }

        fn resolve_run_profile(
            &mut self,
            _logical_session_id: &str,
            _author_id: &str,
            _user_input: &str,
        ) -> CrabResult<RunProfileTelemetry> {
            self.steps.push("resolve_run_profile".to_string());
            Self::pop_result(
                &mut self.resolve_profile_results,
                "turn_executor_test_resolve_profile",
            )
        }

        fn ensure_physical_session(
            &mut self,
            _logical_session_id: &str,
            _profile: &InferenceProfile,
            _active_physical_session_id: Option<&str>,
        ) -> CrabResult<crab_core::PhysicalSession> {
            self.steps.push("ensure_physical_session".to_string());
            if let Some(path) = self.ensure_session_sabotage_path.take() {
                replace_path_with_directory(&path);
            }
            Self::pop_result(
                &mut self.ensure_session_results,
                "turn_executor_test_ensure_session",
            )
        }

        fn build_turn_context(
            &mut self,
            run: &crab_core::Run,
            _logical_session: &crab_core::LogicalSession,
            _physical_session: &crab_core::PhysicalSession,
            inject_bootstrap_context: bool,
        ) -> CrabResult<String> {
            self.steps.push("build_turn_context".to_string());
            self.build_context_bootstrap_flags
                .push(inject_bootstrap_context);
            if !inject_bootstrap_context {
                return Ok(run.user_input.clone());
            }
            Self::pop_result(
                &mut self.build_context_results,
                "turn_executor_test_build_context",
            )
        }

        fn execute_backend_turn(
            &mut self,
            _codex_lifecycle: &mut dyn crab_backends::CodexLifecycleManager,
            _physical_session: &mut crab_core::PhysicalSession,
            _run: &crab_core::Run,
            turn_id: &str,
            turn_context: &str,
        ) -> CrabResult<crab_backends::BackendEventStream> {
            self.steps.push("execute_backend_turn".to_string());
            self.executed_turn_contexts
                .push((turn_id.to_string(), turn_context.to_string()));
            let events = Self::pop_result(
                &mut self.execute_turn_results,
                "turn_executor_test_execute_turn",
            )?;
            Ok(Box::pin(stream::iter(events)))
        }

        fn deliver_assistant_output(
            &mut self,
            run: &crab_core::Run,
            channel_id: &str,
            message_id: &str,
            edit_generation: u32,
            content: &str,
        ) -> CrabResult<()> {
            self.steps.push("deliver_assistant_output".to_string());
            self.delivered_outputs.push((
                run.logical_session_id.clone(),
                channel_id.to_string(),
                message_id.to_string(),
                edit_generation,
                content.to_string(),
            ));
            if let Some(path) = self.deliver_sabotage_path.take() {
                replace_path_with_directory(&path);
            }
            match self.deliver_results.pop_front() {
                Some(result) => result,
                None => Ok(()),
            }
        }
    }

    fn sample_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: InferenceProfile {
                backend: BackendKind::Codex,
                model: "gpt-5-codex".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
            backend_source: ProfileValueSource::GlobalDefault,
            model_source: ProfileValueSource::GlobalDefault,
            reasoning_level_source: ProfileValueSource::GlobalDefault,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: "111111111111111111".to_string(),
            sender_is_owner: false,
            resolved_owner_profile: None,
        }
    }

    fn owner_profile_telemetry() -> RunProfileTelemetry {
        let mut telemetry = sample_profile_telemetry();
        telemetry.sender_id = "999999999999999999".to_string();
        telemetry.sender_is_owner = true;
        telemetry.resolved_owner_profile = Some(OwnerProfileMetadata {
            machine_location: Some("Paris, France".to_string()),
            machine_timezone: Some("Europe/Paris".to_string()),
            default_backend: Some(BackendKind::Codex),
            default_model: Some("gpt-5-codex".to_string()),
            default_reasoning_level: Some(ReasoningLevel::High),
        });
        telemetry
    }

    fn gateway_message(message_id: &str) -> GatewayMessage {
        GatewayMessage {
            message_id: message_id.to_string(),
            author_id: "111111111111111111".to_string(),
            author_is_bot: false,
            channel_id: "777".to_string(),
            guild_id: Some("555".to_string()),
            thread_id: None,
            content: "ship ws15-t2".to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
        }
    }

    fn gateway_message_with_content(message_id: &str, content: &str) -> GatewayMessage {
        let mut message = gateway_message(message_id);
        message.content = content.to_string();
        message
    }

    fn gateway_dm_message_with_content(
        message_id: &str,
        author_id: &str,
        content: &str,
    ) -> GatewayMessage {
        GatewayMessage {
            message_id: message_id.to_string(),
            author_id: author_id.to_string(),
            author_is_bot: false,
            channel_id: format!("dm-{author_id}"),
            guild_id: None,
            thread_id: None,
            content: content.to_string(),
            conversation_kind: GatewayConversationKind::DirectMessage,
        }
    }

    fn gateway_owner_dm_message_with_content(message_id: &str, content: &str) -> GatewayMessage {
        gateway_dm_message_with_content(message_id, "424242424242424242", content)
    }

    fn onboarding_capture_payload_json() -> &'static str {
        r#"{
  "schema_version": "v1",
  "agent_identity": "Crab",
  "owner_identity": "Henry",
  "primary_goals": ["Ship reliable automation", "Keep strict quality gates"],
  "machine_location": "Paris, France",
  "machine_timezone": "Europe/Paris"
}"#
    }

    fn delivery_run(logical_session_id: &str, run_id: &str) -> crab_core::Run {
        crab_core::Run {
            id: run_id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            physical_session_id: Some("physical-1".to_string()),
            status: RunStatus::Running,
            user_input: "ship ws15-t4".to_string(),
            delivery_channel_id: None,
            profile: sample_profile_telemetry(),
            queued_at_epoch_ms: 1,
            started_at_epoch_ms: Some(2),
            completed_at_epoch_ms: None,
        }
    }

    fn rotation_test_run(
        logical_session_id: &str,
        run_id: &str,
        user_input: &str,
    ) -> crab_core::Run {
        crab_core::Run {
            id: run_id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            physical_session_id: None,
            status: RunStatus::Running,
            user_input: user_input.to_string(),
            delivery_channel_id: None,
            profile: owner_profile_telemetry(),
            queued_at_epoch_ms: 1,
            started_at_epoch_ms: Some(2),
            completed_at_epoch_ms: None,
        }
    }

    fn rotation_test_session(
        logical_session_id: &str,
        profile: &InferenceProfile,
    ) -> crab_core::LogicalSession {
        crab_core::LogicalSession {
            id: logical_session_id.to_string(),
            active_backend: BackendKind::Codex,
            active_profile: profile.clone(),
            active_physical_session_id: Some("physical-1".to_string()),
            last_successful_checkpoint_id: None,
            lane_state: LaneState::Idle,
            queued_run_count: 0,
            last_activity_epoch_ms: 3,
            token_accounting: crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
            },
        }
    }

    fn physical_session_fixture() -> crab_core::PhysicalSession {
        crab_core::PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            backend: BackendKind::Codex,
            backend_session_id: "thread-abc".to_string(),
            created_at_epoch_ms: 1_739_173_200_000,
            last_turn_id: None,
        }
    }

    fn physical_session_fixture_for(logical_session_id: &str) -> crab_core::PhysicalSession {
        let mut session = physical_session_fixture();
        session.logical_session_id = logical_session_id.to_string();
        session
    }

    fn backend_event(
        sequence: u64,
        kind: BackendEventKind,
        payload: &[(&str, &str)],
    ) -> crab_backends::BackendEvent {
        crab_backends::BackendEvent {
            sequence,
            kind,
            payload: payload
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }

    fn checkpoint_document_json(summary: &str) -> String {
        format!(
            r#"{{"summary":"{summary}","decisions":["keep runtime policy"],"open_questions":["none"],"next_actions":["continue"],"artifacts":[{{"path":"state/rotation","note":"checkpoint"}}]}}"#
        )
    }

    fn hidden_checkpoint_backend_events(raw_output: &str) -> Vec<crab_backends::BackendEvent> {
        vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", raw_output)]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ]
    }

    fn build_executor_with_config(
        runtime: FakeRuntime,
        lane_queue_limit: usize,
        config: crab_core::RuntimeConfig,
    ) -> TurnExecutor<FakeCodexProcess, FakeOpenCodeProcess, FakeRuntime> {
        let bootstrap_path = Path::new(config.workspace_root.as_str()).join("BOOTSTRAP.md");
        let composition = compose_runtime_with_processes_and_queue_limit(
            &config,
            "999999999999999999",
            FakeCodexProcess,
            FakeOpenCodeProcess,
            lane_queue_limit,
        )
        .expect("composition should build");
        let _ = std::fs::remove_file(&bootstrap_path);
        TurnExecutor::new(composition, runtime)
    }

    fn build_executor(
        workspace: &TempWorkspace,
        runtime: FakeRuntime,
        lane_queue_limit: usize,
    ) -> TurnExecutor<FakeCodexProcess, FakeOpenCodeProcess, FakeRuntime> {
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        build_executor_with_config(runtime, lane_queue_limit, config)
    }

    fn build_executor_scenario(
        label: &str,
        runtime: FakeRuntime,
        lane_queue_limit: usize,
    ) -> (
        TempWorkspace,
        TurnExecutor<FakeCodexProcess, FakeOpenCodeProcess, FakeRuntime>,
    ) {
        let workspace = TempWorkspace::new("turn-executor", label);
        let executor = build_executor(&workspace, runtime, lane_queue_limit);
        (workspace, executor)
    }

    fn run_git_output(workspace_root: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(workspace_root)
            .args(args)
            .output()
            .expect("git command should be executable");
        assert!(output.status.success());
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn invalid_sender_profile_telemetry() -> RunProfileTelemetry {
        let mut profile = sample_profile_telemetry();
        profile.sender_id = " ".to_string();
        profile
    }

    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: [char; 16] = [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
        ];
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            let upper = usize::from(byte >> 4);
            let lower = usize::from(byte & 0x0f);
            output.push(HEX[upper]);
            output.push(HEX[lower]);
        }
        output
    }

    fn state_root(workspace: &TempWorkspace) -> PathBuf {
        workspace.path.join("state")
    }

    fn session_file_path(state_root: &Path, logical_session_id: &str) -> PathBuf {
        state_root.join("sessions").join(format!(
            "{}.json",
            hex_encode(logical_session_id.as_bytes())
        ))
    }

    fn run_file_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        state_root
            .join("runs")
            .join(hex_encode(logical_session_id.as_bytes()))
            .join(format!("{}.json", hex_encode(run_id.as_bytes())))
    }

    fn event_log_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        state_root
            .join("events")
            .join(hex_encode(logical_session_id.as_bytes()))
            .join(format!("{}.jsonl", hex_encode(run_id.as_bytes())))
    }

    fn outbound_log_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        state_root
            .join("outbound")
            .join(hex_encode(logical_session_id.as_bytes()))
            .join(format!("{}.jsonl", hex_encode(run_id.as_bytes())))
    }

    fn replace_path_with_directory(path: &Path) {
        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(path).expect("existing directory should be removable");
            } else {
                fs::remove_file(path).expect("existing file should be removable");
            }
        }
        fs::create_dir_all(path).expect("directory fixture should be creatable");
    }

    #[test]
    fn process_gateway_message_runs_end_to_end_pipeline_and_finalizes_success() {
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
                backend_event(2, BackendEventKind::ToolCall, &[("tool", "shell")]),
                backend_event(3, BackendEventKind::ToolResult, &[("status", "ok")]),
                backend_event(4, BackendEventKind::RunNote, &[("note", "thinking")]),
                backend_event(5, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let (_workspace, mut executor) = build_executor_scenario("success", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(
            dispatch,
            DispatchedTurn {
                logical_session_id: "discord:channel:777".to_string(),
                run_id: "run:discord:channel:777:m-1".to_string(),
                turn_id: "turn:run:discord:channel:777:m-1".to_string(),
                status: RunStatus::Succeeded,
                emitted_event_count: 7,
            }
        );

        let stored_run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(stored_run.status, RunStatus::Succeeded);
        assert_eq!(stored_run.started_at_epoch_ms, Some(2));
        assert_eq!(stored_run.completed_at_epoch_ms, Some(8));

        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.lane_state, LaneState::Idle);
        assert_eq!(session.queued_run_count, 0);
        assert_eq!(
            session.active_physical_session_id,
            Some("physical-1".to_string())
        );

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("event replay should succeed");
        assert_eq!(events.len(), 8);
        assert_eq!(events[0].kind, EventKind::RunState);
        assert_eq!(events[0].payload.get("state"), Some(&"queued".to_string()));
        assert_eq!(events[1].kind, EventKind::RunState);
        assert_eq!(events[1].payload.get("state"), Some(&"running".to_string()));
        assert_eq!(events[2].kind, EventKind::TextDelta);
        assert_eq!(events[3].kind, EventKind::ToolCall);
        assert_eq!(events[4].kind, EventKind::ToolResult);
        assert_eq!(events[5].kind, EventKind::RunNote);
        assert_eq!(events[6].kind, EventKind::RunState);
        assert_eq!(
            events[6].payload.get("state"),
            Some(&"succeeded".to_string())
        );
        assert_eq!(events[7].kind, EventKind::RunState);
        assert_eq!(
            events[7].payload.get("state"),
            Some(&"succeeded".to_string())
        );
        for event in &events {
            assert_eq!(
                event.turn_id,
                Some("turn:run:discord:channel:777:m-1".to_string())
            );
            assert_eq!(event.lane_id, Some("discord:channel:777".to_string()));
            assert_eq!(event.backend, Some(BackendKind::Codex));
            assert_eq!(event.resolved_model, Some("gpt-5-codex".to_string()));
            assert_eq!(event.resolved_reasoning_level, Some("medium".to_string()));
            assert_eq!(event.profile_source, Some("global_default".to_string()));
        }
        assert_eq!(events[0].physical_session_id, None);
        assert_eq!(events[1].physical_session_id, None);
        assert_eq!(
            events[2].physical_session_id,
            Some("physical-1".to_string())
        );
        assert_eq!(
            events[7].physical_session_id,
            Some("physical-1".to_string())
        );

        let outbound_records = executor
            .composition()
            .state_stores
            .outbound_record_store
            .list_run_records("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("outbound record list should succeed");
        assert_eq!(outbound_records.len(), 1);
        assert_eq!(outbound_records[0].channel_id, "777");
        assert_eq!(
            outbound_records[0].message_id,
            "delivery:run:discord:channel:777:m-1:chunk:0"
        );
        assert_eq!(outbound_records[0].edit_generation, 0);

        let runtime = executor.runtime_mut();
        assert_eq!(
            runtime.steps,
            vec![
                "resolve_run_profile".to_string(),
                "ensure_physical_session".to_string(),
                "build_turn_context".to_string(),
                "execute_backend_turn".to_string(),
                "deliver_assistant_output".to_string(),
            ]
        );
        assert_eq!(
            runtime.delivered_outputs,
            vec![(
                "discord:channel:777".to_string(),
                "777".to_string(),
                "delivery:run:discord:channel:777:m-1:chunk:0".to_string(),
                0,
                "hello".to_string(),
            )]
        );
    }

    #[test]
    fn failed_backend_run_delivers_user_facing_failure_when_no_output_is_emitted() {
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::Error,
                &[("message", "backend exploded")],
            )],
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let (_workspace, mut executor) = build_executor_scenario("failure-note", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Failed);

        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.active_physical_session_id, None);

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0]
            .4
            .contains("Crab: backend failed"));
        assert!(runtime.delivered_outputs[0].4.contains("backend exploded"));
        assert!(runtime.delivered_outputs[0].4.contains("Please resend"));
    }

    #[test]
    fn failed_backend_run_reports_error_even_if_non_error_event_trails() {
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::Error, &[("message", "boom")]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let (_workspace, mut executor) =
            build_executor_scenario("failure-note-trailing", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Failed);

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0].4.contains("boom"));
    }

    #[test]
    fn failed_backend_run_truncates_very_long_error_messages() {
        let long_message = "x".repeat(2500);
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::Error, &[("message", &long_message)]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let (_workspace, mut executor) =
            build_executor_scenario("failure-note-truncate", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Failed);

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.delivered_outputs.len(), 1);
        let delivered = &runtime.delivered_outputs[0].4;
        assert!(delivered.contains("..."));
        assert!(!delivered.contains(&long_message));
    }

    #[test]
    fn failed_backend_run_skips_failure_note_when_backend_provides_no_message() {
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::Error,
                &[("detail", "missing message key")],
            )],
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let (_workspace, mut executor) =
            build_executor_scenario("failure-note-missing", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Failed);

        let runtime = executor.runtime_mut();
        assert!(runtime.delivered_outputs.is_empty());
    }

    #[test]
    fn successful_run_persists_workspace_git_commit_with_run_metadata() {
        let workspace = TempWorkspace::new("turn-executor", "workspace-git-run-commit");
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = WorkspaceGitPushPolicy::Manual;
        let mut executor = build_executor_with_config(runtime, 8, config);

        executor
            .process_gateway_message(gateway_message("m-git-run"))
            .expect("run should succeed")
            .expect("run should dispatch");

        let commit_count = run_git_output(&workspace.path, &["rev-list", "--count", "HEAD"]);
        assert_eq!(commit_count.trim(), "1");
        let head_message = run_git_output(&workspace.path, &["log", "-1", "--pretty=%B"]);
        assert!(head_message.contains("Crab-Trigger: run_finalized"));
        assert!(head_message.contains("Crab-Logical-Session-Id: discord:channel:777"));
        assert!(head_message.contains("Crab-Run-Id: run:discord:channel:777:m-git-run"));
        assert!(head_message.contains("Crab-Run-Status: succeeded"));
    }

    #[test]
    fn rotation_run_persists_workspace_git_commit_with_checkpoint_metadata() {
        let workspace = TempWorkspace::new("turn-executor", "workspace-git-rotation-commit");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "ship it")]),
                backend_event(
                    2,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "50000"),
                        ("run_usage_output_tokens", "30000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    3,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5, 6, 7],
        );
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "ship it")]),
                backend_event(
                    2,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "50000"),
                        ("run_usage_output_tokens", "30000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    3,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "rotation checkpoint",
            ))),
        ]);
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = WorkspaceGitPushPolicy::Manual;
        let mut executor = build_executor_with_config(runtime, 8, config);

        executor
            .process_gateway_message(gateway_message("m-git-rotation"))
            .expect("rotation run should succeed")
            .expect("run should dispatch");

        let commit_count = run_git_output(&workspace.path, &["rev-list", "--count", "HEAD"]);
        assert_eq!(commit_count.trim(), "1");
        let head_message = run_git_output(&workspace.path, &["log", "-1", "--pretty=%B"]);
        assert!(head_message.contains("Crab-Trigger: rotation_checkpoint"));
        assert!(head_message.contains("Crab-Run-Id: run:discord:channel:777:m-git-rotation"));
        assert!(head_message
            .contains("Crab-Checkpoint-Id: ckpt:run:discord:channel:777:m-git-rotation:6"));
    }

    #[test]
    fn successful_run_enqueues_workspace_git_push_request_when_on_commit_policy_is_enabled() {
        let workspace = TempWorkspace::new("turn-executor", "workspace-git-push-queue");
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = WorkspaceGitPushPolicy::OnCommit;
        config.workspace_git.remote = Some(
            workspace
                .path
                .join("missing-remote.git")
                .to_string_lossy()
                .to_string(),
        );
        let mut executor = build_executor_with_config(runtime, 8, config);

        let dispatched = executor
            .process_gateway_message(gateway_message("m-git-push"))
            .expect("run should succeed")
            .expect("run should dispatch");
        assert_eq!(dispatched.status, RunStatus::Succeeded);

        let queue_path = workspace.path.join("state/workspace_git_push_queue.json");
        let queue_body = fs::read_to_string(queue_path).expect("push queue file should exist");
        assert!(queue_body.contains("run:discord:channel:777:m-git-push"));
        assert!(queue_body.contains("run_finalized"));
    }

    #[test]
    fn maybe_enqueue_workspace_git_push_request_skips_when_commit_key_is_missing() {
        let workspace = TempWorkspace::new("turn-executor", "workspace-git-push-missing-key");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = WorkspaceGitPushPolicy::OnCommit;
        let executor = build_executor_with_config(runtime, 8, config);
        let run = delivery_run("discord:channel:777", "run:discord:channel:777:missing-key");
        let commit_outcome = crab_core::WorkspaceGitCommitOutcome {
            enabled: true,
            trigger: Some(crab_core::WorkspaceGitCommitTrigger::RunFinalized),
            committed: false,
            commit_key: None,
            commit_id: Some("deadbeef".to_string()),
            skipped_reason: Some("missing_key".to_string()),
            staging_skipped_paths: Vec::new(),
        };

        executor.maybe_enqueue_workspace_git_push(
            &run,
            crab_core::WorkspaceGitCommitTrigger::RunFinalized,
            99,
            &commit_outcome,
        );

        assert!(!workspace
            .path
            .join("state/workspace_git_push_queue.json")
            .exists());
    }

    #[test]
    fn maybe_enqueue_workspace_git_push_request_skips_when_commit_id_is_missing() {
        let workspace = TempWorkspace::new("turn-executor", "workspace-git-push-missing-id");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = WorkspaceGitPushPolicy::OnCommit;
        let executor = build_executor_with_config(runtime, 8, config);
        let run = delivery_run("discord:channel:777", "run:discord:channel:777:missing-id");
        let commit_outcome = crab_core::WorkspaceGitCommitOutcome {
            enabled: true,
            trigger: Some(crab_core::WorkspaceGitCommitTrigger::RunFinalized),
            committed: false,
            commit_key: Some("run:discord:channel:777:missing-id:run_finalized".to_string()),
            commit_id: None,
            skipped_reason: Some("missing_id".to_string()),
            staging_skipped_paths: Vec::new(),
        };

        executor.maybe_enqueue_workspace_git_push(
            &run,
            crab_core::WorkspaceGitCommitTrigger::RunFinalized,
            99,
            &commit_outcome,
        );

        assert!(!workspace
            .path
            .join("state/workspace_git_push_queue.json")
            .exists());
    }

    #[test]
    fn maybe_enqueue_workspace_git_push_request_tolerates_enqueue_errors() {
        let workspace = TempWorkspace::new("turn-executor", "workspace-git-push-enqueue-error");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = WorkspaceGitPushPolicy::OnCommit;
        let executor = build_executor_with_config(runtime, 8, config);
        let run = delivery_run(
            "discord:channel:777",
            "run:discord:channel:777:enqueue-error",
        );
        let commit_outcome = crab_core::WorkspaceGitCommitOutcome {
            enabled: true,
            trigger: Some(crab_core::WorkspaceGitCommitTrigger::RunFinalized),
            committed: true,
            commit_key: Some("run:discord:channel:777:enqueue-error:run_finalized".to_string()),
            commit_id: Some("abcdef123456".to_string()),
            skipped_reason: None,
            staging_skipped_paths: Vec::new(),
        };
        let state_root = executor.composition.state_stores.root.clone();
        fs::remove_dir_all(&state_root).expect("state root should be removable");
        fs::write(&state_root, "blocked").expect("state root file sabotage should succeed");

        executor.maybe_enqueue_workspace_git_push(
            &run,
            crab_core::WorkspaceGitCommitTrigger::RunFinalized,
            99,
            &commit_outcome,
        );

        assert!(state_root.is_file());
    }

    #[test]
    fn process_gateway_message_accumulates_session_token_accounting_from_usage_events() {
        let workspace = TempWorkspace::new("turn-executor", "usage-accounting");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("usage_input_tokens", "7"),
                        ("usage_output_tokens", "5"),
                        ("usage_total_tokens", "12"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        );
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("usage_input_tokens", "7"),
                        ("usage_output_tokens", "5"),
                        ("usage_total_tokens", "12"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Ok(vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "3"),
                        ("run_usage_output_tokens", "4"),
                        ("run_usage_total_tokens", "9"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
        ]);
        runtime.resolve_profile_results = VecDeque::from(vec![
            Ok(sample_profile_telemetry()),
            Ok(sample_profile_telemetry()),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(crab_core::PhysicalSession {
                id: "physical-1".to_string(),
                logical_session_id: "discord:channel:777".to_string(),
                backend: BackendKind::Codex,
                backend_session_id: "thread-abc".to_string(),
                created_at_epoch_ms: 1_739_173_200_000,
                last_turn_id: None,
            }),
            Ok(crab_core::PhysicalSession {
                id: "physical-1".to_string(),
                logical_session_id: "discord:channel:777".to_string(),
                backend: BackendKind::Codex,
                backend_session_id: "thread-abc".to_string(),
                created_at_epoch_ms: 1_739_173_200_000,
                last_turn_id: Some("turn-previous".to_string()),
            }),
        ]);
        runtime.build_context_results = VecDeque::from(vec![Ok("context".to_string())]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-usage-1"))
            .expect("first run should succeed")
            .expect("first run should dispatch");
        executor
            .process_gateway_message(gateway_message("m-usage-2"))
            .expect("second run should succeed")
            .expect("second run should dispatch");

        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.token_accounting.input_tokens, 10);
        assert_eq!(session.token_accounting.output_tokens, 9);
        assert_eq!(session.token_accounting.total_tokens, 21);
        assert_eq!(
            executor.runtime_mut().build_context_bootstrap_flags,
            vec![true, false]
        );
        assert_eq!(
            executor.runtime_mut().executed_turn_contexts,
            vec![
                (
                    "turn:run:discord:channel:777:m-usage-1".to_string(),
                    "context".to_string()
                ),
                (
                    "turn:run:discord:channel:777:m-usage-2".to_string(),
                    "ship ws15-t2".to_string()
                ),
            ]
        );
    }

    #[test]
    fn process_gateway_message_rotates_when_token_threshold_is_reached() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-token-trigger");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "shipping now")]),
                backend_event(
                    2,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "50000"),
                        ("run_usage_output_tokens", "30000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    3,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "shipping now")]),
                backend_event(
                    2,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "50000"),
                        ("run_usage_output_tokens", "30000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    3,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "token trigger checkpoint",
            ))),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-rotation-token"))
            .expect("token-trigger run should succeed")
            .expect("run should dispatch");

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-rotation-token";
        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.token_accounting.total_tokens, 0);
        assert_eq!(session.active_physical_session_id, None);
        assert!(session.last_successful_checkpoint_id.is_some());

        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(logical_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("rotation should persist checkpoint");
        assert_eq!(
            Some(checkpoint.id.clone()),
            session.last_successful_checkpoint_id
        );
        assert_eq!(checkpoint.run_id, run_id);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("rotation_event")
                    .is_some_and(|value| value == "completed")
        }));
    }

    #[test]
    fn process_gateway_message_manual_compact_owner_rotates_with_hidden_checkpoint_backend_turn() {
        let workspace = TempWorkspace::new("turn-executor", "manual-compact-owner");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("manual compact checkpoint"),
        ))]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let dispatched = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-compact-owner",
                "/compact confirm",
            ))
            .expect("manual compact should succeed")
            .expect("manual compact should dispatch");
        assert_eq!(dispatched.status, RunStatus::Succeeded);

        let runtime = executor.runtime_mut();
        assert!(runtime.steps.contains(&"resolve_run_profile".to_string()));
        assert!(runtime
            .steps
            .contains(&"deliver_assistant_output".to_string()));
        assert!(runtime
            .steps
            .contains(&"ensure_physical_session".to_string()));
        assert!(!runtime.steps.contains(&"build_turn_context".to_string()));
        assert!(runtime.steps.contains(&"execute_backend_turn".to_string()));
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0]
            .4
            .contains("manual compact accepted"));

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-manual-compact-owner";
        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.active_physical_session_id, None);
        assert!(session.last_successful_checkpoint_id.is_some());

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("manual_rotation_request")
                    .is_some_and(|value| value == "compact")
        }));
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("rotation_event")
                    .is_some_and(|value| value == "completed")
        }));
    }

    #[test]
    fn manual_rotation_response_delivery_errors_are_propagated() {
        let workspace = TempWorkspace::new("turn-executor", "manual-compact-delivery-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("manual compact checkpoint"),
        ))]);
        runtime.deliver_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "deliver_manual",
            message: "manual response delivery failed".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-compact-delivery-error",
                "/compact confirm",
            ))
            .expect_err("manual response delivery failures should bubble up");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "deliver_manual",
                message: "manual response delivery failed".to_string(),
            }
        );
    }

    #[test]
    fn owner_can_complete_onboarding_capture_through_normal_turn_flow() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-complete-owner");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");

        let dispatched = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-onboarding-complete-owner",
                onboarding_capture_payload_json(),
            ))
            .expect("owner onboarding capture should succeed")
            .expect("owner onboarding capture should dispatch");
        assert_eq!(dispatched.status, RunStatus::Succeeded);

        let runtime = executor.runtime_mut();
        assert!(runtime.steps.contains(&"resolve_run_profile".to_string()));
        assert!(runtime
            .steps
            .contains(&"deliver_assistant_output".to_string()));
        assert!(!runtime
            .steps
            .contains(&"ensure_physical_session".to_string()));
        assert!(!runtime.steps.contains(&"build_turn_context".to_string()));
        assert!(!runtime.steps.contains(&"execute_backend_turn".to_string()));
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0]
            .4
            .contains("Onboarding capture applied."));

        assert!(!workspace.path.join("BOOTSTRAP.md").exists());
        let soul = fs::read_to_string(workspace.path.join("SOUL.md"))
            .expect("SOUL profile should be written");
        let identity = fs::read_to_string(workspace.path.join("IDENTITY.md"))
            .expect("IDENTITY profile should be written");
        let user = fs::read_to_string(workspace.path.join("USER.md"))
            .expect("USER profile should be written");
        let memory =
            fs::read_to_string(workspace.path.join("MEMORY.md")).expect("MEMORY should be written");
        assert!(soul.contains("Managed Mission Profile"));
        assert!(identity.contains("Managed Onboarding Identity"));
        assert!(user.contains("Managed Owner Profile"));
        assert!(memory.contains("Managed Onboarding Baseline"));

        let run_id = "run:discord:dm:424242424242424242:m-onboarding-complete-owner";
        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:dm:424242424242424242", run_id)
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.status, RunStatus::Succeeded);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:dm:424242424242424242", run_id)
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("event")
                    .is_some_and(|value| value == "bootstrap_completed")
        }));
    }

    #[test]
    fn onboarding_completion_delivery_errors_are_propagated() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-complete-delivery-error");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.deliver_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "deliver_onboarding",
            message: "onboarding response delivery failed".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");

        let error = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-onboarding-complete-delivery-error",
                onboarding_capture_payload_json(),
            ))
            .expect_err("onboarding response delivery failures should bubble up");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "deliver_onboarding",
                message: "onboarding response delivery failed".to_string(),
            }
        );
    }

    #[test]
    fn onboarding_capture_is_owner_only_while_bootstrap_is_pending() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-capture-non-owner");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(sample_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");

        let dispatched = executor
            .process_gateway_message(gateway_dm_message_with_content(
                "m-onboarding-capture-non-owner",
                "111111111111111111",
                onboarding_capture_payload_json(),
            ))
            .expect("non-owner onboarding gate should succeed")
            .expect("non-owner onboarding gate should dispatch");
        assert_eq!(dispatched.status, RunStatus::Succeeded);

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run(
                "discord:dm:111111111111111111",
                "run:discord:dm:111111111111111111:m-onboarding-capture-non-owner",
            )
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.status, RunStatus::Succeeded);
        assert!(workspace.path.join("BOOTSTRAP.md").exists());
        let runtime = executor.runtime_mut();
        assert!(runtime.delivered_outputs[0]
            .4
            .contains("Only the owner can continue onboarding"));
    }

    #[test]
    fn onboarding_pending_owner_non_dm_is_blocked_with_gate_message_and_note() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-gate-owner-non-dm");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");

        let dispatched = executor
            .process_gateway_message(gateway_message_with_content(
                "m-onboarding-owner-non-dm",
                "hello from owner in guild channel",
            ))
            .expect("owner non-dm onboarding gate should succeed")
            .expect("owner non-dm onboarding gate should dispatch");
        assert_eq!(dispatched.status, RunStatus::Succeeded);

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0]
            .4
            .contains("server channels/threads are blocked"));
        assert!(!runtime
            .steps
            .contains(&"ensure_physical_session".to_string()));
        assert!(!runtime.steps.contains(&"execute_backend_turn".to_string()));

        let run_id = "run:discord:channel:777:m-onboarding-owner-non-dm";
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", run_id)
            .expect("event replay should succeed");
        let gate_event = events
            .iter()
            .find(|event| {
                event.kind == EventKind::RunNote
                    && event
                        .payload
                        .get("event")
                        .is_some_and(|value| value == "onboarding_gate_blocked")
            })
            .expect("onboarding gate run note should be emitted");
        assert_eq!(
            gate_event.payload.get("reason"),
            Some(&"owner_non_dm".to_string())
        );
    }

    #[test]
    fn owner_dm_pending_bootstrap_without_capture_continues_normal_backend_flow() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-owner-dm-non-capture");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "normal reply")]),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5],
        );
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.ensure_session_results = VecDeque::from(vec![Ok(physical_session_fixture_for(
            "discord:dm:424242424242424242",
        ))]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");

        let dispatched = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-owner-dm-non-capture",
                "I have follow-up context",
            ))
            .expect("owner dm non-capture run should succeed")
            .expect("owner dm non-capture run should dispatch");
        assert_eq!(dispatched.status, RunStatus::Succeeded);

        let runtime = executor.runtime_mut();
        assert!(runtime
            .steps
            .contains(&"ensure_physical_session".to_string()));
        assert!(runtime.steps.contains(&"execute_backend_turn".to_string()));
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0].4.contains("normal reply"));
        assert!(workspace.path.join("BOOTSTRAP.md").exists());
    }

    #[test]
    fn malformed_owner_onboarding_capture_is_rejected_with_explicit_parse_error() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-capture-malformed");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");

        let error = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-onboarding-capture-malformed",
                r#"{"schema_version":"v1","agent_identity":"Crab"}"#,
            ))
            .expect_err("malformed onboarding payload should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "onboarding_capture_parse",
                ..
            }
        ));

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run(
                "discord:dm:424242424242424242",
                "run:discord:dm:424242424242424242:m-onboarding-capture-malformed",
            )
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.status, RunStatus::Failed);
        assert!(workspace.path.join("BOOTSTRAP.md").exists());

        let runtime = executor.runtime_mut();
        assert!(runtime.steps.contains(&"resolve_run_profile".to_string()));
        assert!(!runtime
            .steps
            .contains(&"ensure_physical_session".to_string()));
        assert!(!runtime.steps.contains(&"execute_backend_turn".to_string()));
    }

    #[test]
    fn owner_onboarding_capture_with_profile_conflicts_appends_run_note_and_warning_message() {
        let workspace = TempWorkspace::new("turn-executor", "onboarding-capture-conflicts");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for onboarding capture");
        fs::write(
            workspace.path.join("IDENTITY.md"),
            "# IDENTITY.md\n\n<!-- CRAB:ONBOARDING_MANAGED:START -->\nlegacy",
        )
        .expect("malformed identity markers should be writable for conflict setup");

        executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-onboarding-capture-conflicts",
                onboarding_capture_payload_json(),
            ))
            .expect("owner onboarding capture should succeed with conflict warning")
            .expect("owner onboarding capture should dispatch");

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(
            runtime.delivered_outputs[0]
                .4
                .contains("Profile conflicts detected"),
            "user delivery should surface conflict warning"
        );

        let run_id = "run:discord:dm:424242424242424242:m-onboarding-capture-conflicts";
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:dm:424242424242424242", run_id)
            .expect("event replay should succeed");
        let conflict_event = events
            .iter()
            .find(|event| {
                event.kind == EventKind::RunNote
                    && event
                        .payload
                        .get("event")
                        .is_some_and(|value| value == "onboarding_profile_conflicts")
            })
            .expect("conflict run note should be emitted");
        assert!(
            conflict_event
                .payload
                .get("conflict_paths")
                .is_some_and(|value| value.contains("IDENTITY.md")),
            "conflict payload should contain IDENTITY path"
        );
    }

    #[test]
    fn process_gateway_message_rejects_non_owner_manual_rotation_commands() {
        let workspace = TempWorkspace::new("turn-executor", "manual-reset-non-owner");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(sample_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-reset-non-owner",
                "/reset confirm",
            ))
            .expect_err("non-owner manual reset should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "operator_command_authorize",
                message: "sender 111111111111111111 is not authorized to run operator commands"
                    .to_string(),
            }
        );

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run(
                "discord:channel:777",
                "run:discord:channel:777:m-manual-reset-non-owner",
            )
            .expect("run lookup should succeed")
            .expect("run should be persisted");
        assert_eq!(run.status, RunStatus::Failed);

        let runtime = executor.runtime_mut();
        assert!(runtime.steps.contains(&"resolve_run_profile".to_string()));
        assert!(!runtime
            .steps
            .contains(&"ensure_physical_session".to_string()));
        assert!(runtime.delivered_outputs.is_empty());
    }

    #[test]
    fn process_gateway_message_rejects_manual_rotation_commands_without_confirm_token() {
        let workspace = TempWorkspace::new("turn-executor", "manual-command-invalid-shape");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-invalid-shape",
                "/compact now",
            ))
            .expect_err("invalid manual command shape should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "operator_command_parse",
                message: "onboarding command requires confirmation token \"confirm\"".to_string(),
            }
        );

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run(
                "discord:channel:777",
                "run:discord:channel:777:m-manual-invalid-shape",
            )
            .expect("run lookup should succeed")
            .expect("run should be persisted");
        assert_eq!(run.status, RunStatus::Failed);
    }

    #[test]
    fn manual_rotation_command_surfaces_clock_errors_during_operator_audit() {
        let workspace = TempWorkspace::new("turn-executor", "manual-clock-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-clock-error",
                "/compact confirm",
            ))
            .expect_err("missing scripted timestamp should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn manual_rotation_command_surfaces_operator_audit_event_store_errors() {
        let workspace = TempWorkspace::new("turn-executor", "manual-audit-event-store-error");
        let state_root = state_root(&workspace);
        let run_id = "run:discord:channel:777:m-manual-audit-event-store-error";
        let blocked_log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime = runtime.with_now_epoch_sabotage(3, blocked_log_path);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-audit-event-store-error",
                "/compact confirm",
            ))
            .expect_err("audit event append should fail when run log path is blocked");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn manual_rotation_command_surfaces_rotation_started_event_errors() {
        let workspace = TempWorkspace::new("turn-executor", "manual-rotation-start-event-error");
        let state_root = state_root(&workspace);
        let run_id = "run:discord:channel:777:m-manual-rotation-start-event-error";
        let blocked_log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime = runtime.with_now_epoch_sabotage(4, blocked_log_path);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-rotation-start-event-error",
                "/compact confirm",
            ))
            .expect_err("rotation started event append should fail when run log path is blocked");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn maybe_execute_rotation_surfaces_rotation_started_event_errors() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-start-event-direct-error");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:rotation-start-event-direct-error";
        let blocked_log_path = event_log_path(&state_root(&workspace), logical_session_id, run_id);
        replace_path_with_directory(&blocked_log_path);

        let run = rotation_test_run(logical_session_id, run_id, "/compact confirm");
        let mut session = rotation_test_session(logical_session_id, &run.profile.resolved_profile);

        let error = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                None,
            )
            .expect_err("rotation started event append should fail when event log path is blocked");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn maybe_execute_rotation_surfaces_rotation_completed_event_errors() {
        let workspace =
            TempWorkspace::new("turn-executor", "rotation-completed-event-direct-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("rotation completed event"),
        ))]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:rotation-completed-event-direct-error";
        let sabotage_path = event_log_path(&state_root(&workspace), logical_session_id, run_id);

        let run = rotation_test_run(logical_session_id, run_id, "/compact confirm");
        let mut session = rotation_test_session(logical_session_id, &run.profile.resolved_profile);

        let error = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                Some(sabotage_path),
            )
            .expect_err(
                "rotation completed event append should fail when event log path is sabotaged",
            );
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn maybe_execute_rotation_surfaces_rotation_sabotage_path_errors() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-sabotage-path-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("rotation sabotage"),
        ))]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:rotation-sabotage-path-error";
        let blocked_parent = state_root(&workspace).join("blocked-parent");
        fs::write(&blocked_parent, "blocked parent").expect("fixture file should be writable");
        let sabotage_path = blocked_parent.join("child");

        let run = rotation_test_run(logical_session_id, run_id, "/compact confirm");
        let mut session = rotation_test_session(logical_session_id, &run.profile.resolved_profile);

        let error = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                Some(sabotage_path),
            )
            .expect_err("invalid sabotage path parent should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "turn_executor_rotation_sabotage",
                ..
            }
        ));
    }

    #[test]
    fn manual_rotation_command_surfaces_rotation_session_persist_errors() {
        let workspace = TempWorkspace::new("turn-executor", "manual-rotation-session-error");
        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("session persist failure checkpoint"),
        ))]);
        runtime = runtime.with_now_epoch_sabotage(4, session_path);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-rotation-session-error",
                "/compact confirm",
            ))
            .expect_err("rotation session persist should fail when session path is blocked");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn manual_rotation_command_surfaces_checkpoint_persist_errors() {
        let workspace = TempWorkspace::new("turn-executor", "manual-checkpoint-store-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("checkpoint persist failure"),
        ))]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let checkpoints_root = state_root(&workspace).join("checkpoints");
        fs::write(&checkpoints_root, "blocked checkpoints root")
            .expect("fixture file should be writable");

        let error = executor
            .process_gateway_message(gateway_message_with_content(
                "m-manual-checkpoint-store-error",
                "/compact confirm",
            ))
            .expect_err("rotation checkpoint persistence should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "checkpoint_store_layout",
                ..
            }
        ));
    }

    #[test]
    fn rotation_trigger_evaluation_surfaces_invalid_runtime_policy_values() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-invalid-policy");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnCompleted,
                &[("stop_reason", "done")],
            )],
            &[1, 2, 3, 4, 5],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .composition_mut()
            .rotation_policy
            .compaction_token_threshold = 0;

        let error = executor
            .process_gateway_message(gateway_message("m-rotation-invalid-policy"))
            .expect_err("invalid runtime policy should fail rotation trigger evaluation");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_COMPACTION_TOKEN_THRESHOLD",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn process_gateway_message_rejects_invalid_usage_totals() {
        let workspace = TempWorkspace::new("turn-executor", "usage-invalid-total");
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("usage_input_tokens", "7"),
                        ("usage_output_tokens", "5"),
                        ("usage_total_tokens", "11"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message("m-usage-invalid-total"))
            .expect_err("invalid usage totals should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "usage_total_tokens 11 must be greater than or equal to usage_input_tokens + usage_output_tokens 12".to_string(),
            }
        );
    }

    #[test]
    fn token_trigger_rotation_uses_backend_hidden_checkpoint_turn_without_fallback() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-backend-checkpoint");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5],
        );
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "Primary hidden checkpoint from backend",
            ))),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture()),
            Ok(physical_session_fixture()),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-rotation-backend-checkpoint"))
            .expect("backend checkpoint run should succeed")
            .expect("backend checkpoint run should dispatch");
        let runtime = executor.runtime_mut();
        assert_eq!(runtime.executed_turn_contexts.len(), 2);
        assert_eq!(
            runtime.executed_turn_contexts[1].0,
            "turn:run:discord:channel:777:m-rotation-backend-checkpoint:hidden-checkpoint:1"
        );
        assert_eq!(
            runtime.executed_turn_contexts[1].1,
            build_checkpoint_prompt()
        );

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-rotation-backend-checkpoint";
        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(logical_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("rotation should persist checkpoint");
        assert_eq!(checkpoint.summary, "Primary hidden checkpoint from backend");

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        let rotation_completed = events
            .iter()
            .find(|event| {
                event.kind == EventKind::RunNote
                    && event
                        .payload
                        .get("rotation_event")
                        .is_some_and(|value| value == "completed")
            })
            .expect("rotation completed run note should exist");
        assert_eq!(
            rotation_completed.payload.get("used_fallback_checkpoint"),
            Some(&"false".to_string())
        );
        assert_eq!(
            rotation_completed.payload.get("checkpoint_turn_error"),
            Some(&"none".to_string())
        );
    }

    #[test]
    fn token_trigger_rotation_owner_dm_can_complete_onboarding_via_hidden_extraction() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-onboarding-success");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5]);
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Ok(hidden_checkpoint_backend_events(
                onboarding_capture_payload_json(),
            )),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "rotation onboarding checkpoint",
            ))),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .composition_mut()
            .rotation_policy
            .compaction_token_threshold = 1;
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let dispatched = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-rotation-onboarding-success",
                "let's keep going",
            ))
            .expect("rotation onboarding success should dispatch")
            .expect("rotation onboarding success should produce a run");
        assert_eq!(dispatched.status, RunStatus::Succeeded);
        assert!(!workspace.path.join("BOOTSTRAP.md").exists());

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.executed_turn_contexts.len(), 3);
        assert!(runtime.executed_turn_contexts[1]
            .1
            .contains("Onboarding extraction session id: onboarding-rotation:run:discord:dm:424242424242424242:m-rotation-onboarding-success"));
        assert_eq!(runtime.delivered_outputs.len(), 1);
        assert!(runtime.delivered_outputs[0]
            .4
            .contains("Onboarding completed during checkpoint rotation."));

        let logical_session_id = "discord:dm:424242424242424242";
        let run_id = "run:discord:dm:424242424242424242:m-rotation-onboarding-success";
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("event")
                    .is_some_and(|value| value == "onboarding_rotation_applied")
        }));
    }

    #[test]
    fn maybe_execute_rotation_pending_onboarding_skips_extraction_for_owner_non_dm() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-onboarding-owner-non-dm");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        runtime.execute_turn_results = VecDeque::from(vec![Ok(hidden_checkpoint_backend_events(
            &checkpoint_document_json("rotation without onboarding extraction"),
        ))]);
        runtime.ensure_session_results = VecDeque::from(vec![Ok(physical_session_fixture_for(
            "discord:channel:777",
        ))]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let run = rotation_test_run(
            "discord:channel:777",
            "run:discord:channel:777:rotation-onboarding-owner-non-dm",
            "/compact confirm",
        );
        let mut session =
            rotation_test_session("discord:channel:777", &run.profile.resolved_profile);
        let outcome = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                None,
            )
            .expect("rotation should succeed")
            .expect("rotation outcome should be present");
        assert!(outcome.onboarding_completion_resolution.is_none());
        assert_eq!(outcome.supplemental_emitted_event_count, 0);
        let runtime = executor.runtime_mut();
        assert_eq!(runtime.executed_turn_contexts.len(), 1);
        assert_eq!(
            runtime.executed_turn_contexts[0].1,
            build_checkpoint_prompt()
        );
    }

    #[test]
    fn onboarding_rotation_extraction_propagates_backend_execution_errors() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-onboarding-exec-error");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let logical_session_id = "discord:dm:424242424242424242";
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let expected = CrabError::InvariantViolation {
            context: "turn_executor_test_execute_turn",
            message: "forced backend execution error".to_string(),
        };
        runtime.execute_turn_results = VecDeque::from(vec![Err(expected.clone())]);
        runtime.ensure_session_results =
            VecDeque::from(vec![Ok(physical_session_fixture_for(logical_session_id))]);

        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let run = rotation_test_run(
            logical_session_id,
            "run:discord:dm:424242424242424242:rotation-onboarding-exec-error",
            "/compact confirm",
        );
        let mut session = rotation_test_session(logical_session_id, &run.profile.resolved_profile);
        let error = executor
            .maybe_complete_pending_onboarding_from_rotation(&run, &mut session, 4)
            .expect_err("backend execution error should propagate");
        assert_eq!(error, expected);
    }

    #[test]
    fn maybe_execute_rotation_onboarding_extraction_incomplete_appends_run_note() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-onboarding-incomplete");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(hidden_checkpoint_backend_events(
                ONBOARDING_CAPTURE_INCOMPLETE_TOKEN,
            )),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "rotation after incomplete onboarding extraction",
            ))),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let run = rotation_test_run(
            "discord:dm:424242424242424242",
            "run:discord:dm:424242424242424242:rotation-onboarding-incomplete",
            "/compact confirm",
        );
        let mut session = rotation_test_session(
            "discord:dm:424242424242424242",
            &run.profile.resolved_profile,
        );
        let outcome = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                None,
            )
            .expect("rotation should succeed")
            .expect("rotation outcome should be present");
        assert!(outcome.onboarding_completion_resolution.is_none());
        assert_eq!(outcome.supplemental_emitted_event_count, 1);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(
                "discord:dm:424242424242424242",
                "run:discord:dm:424242424242424242:rotation-onboarding-incomplete",
            )
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("event")
                    .is_some_and(|value| value == "onboarding_rotation_incomplete")
        }));
    }

    #[test]
    fn maybe_execute_rotation_onboarding_extraction_parse_error_appends_run_note() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-onboarding-parse-error");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(hidden_checkpoint_backend_events("{")),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "rotation after parse error",
            ))),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let run = rotation_test_run(
            "discord:dm:424242424242424242",
            "run:discord:dm:424242424242424242:rotation-onboarding-parse-error",
            "/compact confirm",
        );
        let mut session = rotation_test_session(
            "discord:dm:424242424242424242",
            &run.profile.resolved_profile,
        );
        let outcome = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                None,
            )
            .expect("rotation should succeed")
            .expect("rotation outcome should be present");
        assert!(outcome.onboarding_completion_resolution.is_none());
        assert_eq!(outcome.supplemental_emitted_event_count, 1);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(
                "discord:dm:424242424242424242",
                "run:discord:dm:424242424242424242:rotation-onboarding-parse-error",
            )
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("event")
                    .is_some_and(|value| value == "onboarding_rotation_parse_error")
        }));
    }

    #[test]
    fn maybe_execute_rotation_onboarding_extraction_apply_error_appends_run_note() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-onboarding-apply-error");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");

        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(hidden_checkpoint_backend_events(
                onboarding_capture_payload_json(),
            )),
            Ok(hidden_checkpoint_backend_events(&checkpoint_document_json(
                "rotation after apply error",
            ))),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
            Ok(physical_session_fixture_for(
                "discord:dm:424242424242424242",
            )),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let soul_path = workspace.path.join("SOUL.md");
        let _ = fs::remove_file(&soul_path);
        fs::create_dir_all(&soul_path).expect("SOUL path directory should be creatable");

        let run = rotation_test_run(
            "discord:dm:424242424242424242",
            "run:discord:dm:424242424242424242:rotation-onboarding-apply-error",
            "/compact confirm",
        );
        let mut session = rotation_test_session(
            "discord:dm:424242424242424242",
            &run.profile.resolved_profile,
        );
        let outcome = executor
            .maybe_execute_rotation_with_sabotage(
                &run,
                &mut session,
                4,
                Some(crab_core::ManualRotationRequest::Compact),
                None,
            )
            .expect("rotation should succeed")
            .expect("rotation outcome should be present");
        assert!(outcome.onboarding_completion_resolution.is_none());
        assert_eq!(outcome.supplemental_emitted_event_count, 1);
        assert!(workspace.path.join("BOOTSTRAP.md").exists());

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(
                "discord:dm:424242424242424242",
                "run:discord:dm:424242424242424242:rotation-onboarding-apply-error",
            )
            .expect("event replay should succeed");
        assert!(events.iter().any(|event| {
            event.kind == EventKind::RunNote
                && event
                    .payload
                    .get("event")
                    .is_some_and(|value| value == "onboarding_rotation_apply_error")
        }));
    }

    #[test]
    fn token_trigger_rotation_falls_back_when_backend_checkpoint_schema_validation_fails() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-checkpoint-schema-failure");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5],
        );
        let invalid_checkpoint_json = r#"{"summary":" ","decisions":["keep"],"open_questions":["none"],"next_actions":["continue"],"artifacts":[{"path":"state/rotation","note":"checkpoint"}]}"#;
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Ok(hidden_checkpoint_backend_events(invalid_checkpoint_json)),
            Ok(hidden_checkpoint_backend_events(invalid_checkpoint_json)),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture()),
            Ok(physical_session_fixture()),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-rotation-checkpoint-schema-failure"))
            .expect("rotation should succeed with fallback checkpoint")
            .expect("schema failure run should dispatch");
        let runtime = executor.runtime_mut();
        assert_eq!(runtime.executed_turn_contexts.len(), 3);
        assert_eq!(
            runtime.executed_turn_contexts[1].1,
            build_checkpoint_prompt()
        );
        assert_eq!(
            runtime.executed_turn_contexts[2].0,
            "turn:run:discord:channel:777:m-rotation-checkpoint-schema-failure:hidden-checkpoint:2"
        );
        assert!(runtime.executed_turn_contexts[2]
            .1
            .contains("Your previous checkpoint response was invalid"));
        assert_ne!(
            runtime.executed_turn_contexts[2].1,
            runtime.executed_turn_contexts[1].1
        );

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-rotation-checkpoint-schema-failure";
        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(logical_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("rotation should persist checkpoint");
        assert!(checkpoint
            .summary
            .starts_with("Fallback checkpoint generated"));

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        let rotation_completed = events
            .iter()
            .find(|event| {
                event.kind == EventKind::RunNote
                    && event
                        .payload
                        .get("rotation_event")
                        .is_some_and(|value| value == "completed")
            })
            .expect("rotation completed run note should exist");
        assert_eq!(
            rotation_completed.payload.get("used_fallback_checkpoint"),
            Some(&"true".to_string())
        );
        let checkpoint_turn_error = rotation_completed
            .payload
            .get("checkpoint_turn_error")
            .expect("checkpoint turn failure diagnostics should exist");
        assert!(checkpoint_turn_error.contains("checkpoint_turn_schema"));
    }

    #[test]
    fn token_trigger_rotation_falls_back_when_backend_checkpoint_turn_execution_fails() {
        let workspace = TempWorkspace::new("turn-executor", "rotation-checkpoint-backend-failure");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5],
        );
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "60000"),
                        ("run_usage_output_tokens", "20000"),
                        ("run_usage_total_tokens", "120000"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ]),
            Err(CrabError::InvariantViolation {
                context: "checkpoint_backend_turn",
                message: "backend unavailable".to_string(),
            }),
        ]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(physical_session_fixture()),
            Ok(physical_session_fixture()),
        ]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-rotation-checkpoint-backend-failure"))
            .expect("rotation should succeed with deterministic fallback")
            .expect("backend failure run should dispatch");
        let runtime = executor.runtime_mut();
        assert_eq!(runtime.executed_turn_contexts.len(), 2);
        assert_eq!(
            runtime.executed_turn_contexts[1].0,
            "turn:run:discord:channel:777:m-rotation-checkpoint-backend-failure:hidden-checkpoint:1"
        );
        assert_eq!(
            runtime.executed_turn_contexts[1].1,
            build_checkpoint_prompt()
        );

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-rotation-checkpoint-backend-failure";
        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(logical_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("rotation should persist checkpoint");
        assert!(checkpoint
            .summary
            .starts_with("Fallback checkpoint generated"));

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        let rotation_completed = events
            .iter()
            .find(|event| {
                event.kind == EventKind::RunNote
                    && event
                        .payload
                        .get("rotation_event")
                        .is_some_and(|value| value == "completed")
            })
            .expect("rotation completed run note should exist");
        assert_eq!(
            rotation_completed.payload.get("used_fallback_checkpoint"),
            Some(&"true".to_string())
        );
        let checkpoint_turn_error = rotation_completed
            .payload
            .get("checkpoint_turn_error")
            .expect("checkpoint turn failure diagnostics should exist");
        assert!(checkpoint_turn_error.contains("checkpoint_backend_turn"));
        assert!(checkpoint_turn_error.contains("backend unavailable"));
    }

    #[test]
    fn process_gateway_message_surfaces_token_accounting_merge_overflow() {
        let workspace = TempWorkspace::new("turn-executor", "usage-merge-overflow");
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(
                    1,
                    BackendEventKind::RunNote,
                    &[
                        ("run_usage_input_tokens", "1"),
                        ("run_usage_output_tokens", "0"),
                        ("run_usage_total_tokens", "1"),
                    ],
                ),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .composition()
            .state_stores
            .session_store
            .upsert_session(&crab_core::LogicalSession {
                id: "discord:channel:777".to_string(),
                active_backend: BackendKind::Codex,
                active_profile: InferenceProfile {
                    backend: BackendKind::Codex,
                    model: "gpt-5-codex".to_string(),
                    reasoning_level: ReasoningLevel::Medium,
                },
                active_physical_session_id: Some("physical-1".to_string()),
                last_successful_checkpoint_id: None,
                lane_state: LaneState::Idle,
                queued_run_count: 0,
                last_activity_epoch_ms: 1,
                token_accounting: crab_core::TokenAccounting {
                    input_tokens: u64::MAX,
                    output_tokens: 0,
                    total_tokens: 0,
                },
            })
            .expect("session seed should succeed");

        let error = executor
            .process_gateway_message(gateway_message("m-usage-merge-overflow"))
            .expect_err("overflow in token merge should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "input token accounting overflow".to_string(),
            }
        );
    }

    #[test]
    fn process_gateway_message_preserves_owner_and_non_owner_profile_context() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];

        let owner_workspace = TempWorkspace::new("turn-executor", "owner-run");
        let mut owner_runtime =
            FakeRuntime::with_backend_events(backend_events.clone(), &[1, 2, 3, 4, 5, 6, 7]);
        owner_runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        let mut owner_executor = build_executor(&owner_workspace, owner_runtime, 8);
        owner_executor
            .process_gateway_message(gateway_message("m-owner"))
            .expect("owner pipeline should succeed")
            .expect("owner run should dispatch");
        let owner_run = owner_executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-owner")
            .expect("owner run lookup should succeed")
            .expect("owner run should exist");
        assert!(owner_run.profile.sender_is_owner);
        let owner_profile = owner_run
            .profile
            .resolved_owner_profile
            .expect("owner profile metadata should persist");
        assert_eq!(
            owner_profile.machine_location,
            Some("Paris, France".to_string())
        );
        assert_eq!(
            owner_profile.machine_timezone,
            Some("Europe/Paris".to_string())
        );
        let owner_events = owner_executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-owner")
            .expect("owner event replay should succeed");
        assert!(owner_events.iter().all(|event| {
            event
                .profile
                .as_ref()
                .map(|profile| profile.sender_is_owner && profile.resolved_owner_profile.is_some())
                .unwrap_or(false)
        }));

        let non_owner_workspace = TempWorkspace::new("turn-executor", "non-owner-run");
        let non_owner_runtime =
            FakeRuntime::with_backend_events(backend_events, &[11, 12, 13, 14, 15, 16, 17]);
        let mut non_owner_executor = build_executor(&non_owner_workspace, non_owner_runtime, 8);
        non_owner_executor
            .process_gateway_message(gateway_message("m-non-owner"))
            .expect("non-owner pipeline should succeed")
            .expect("non-owner run should dispatch");
        let non_owner_run = non_owner_executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-non-owner")
            .expect("non-owner run lookup should succeed")
            .expect("non-owner run should exist");
        assert!(!non_owner_run.profile.sender_is_owner);
        assert!(non_owner_run.profile.resolved_owner_profile.is_none());
        let non_owner_events = non_owner_executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-non-owner")
            .expect("non-owner event replay should succeed");
        assert!(non_owner_events.iter().all(|event| {
            event
                .profile
                .as_ref()
                .map(|profile| !profile.sender_is_owner && profile.resolved_owner_profile.is_none())
                .unwrap_or(false)
        }));
    }

    #[test]
    fn restart_recovery_replays_missing_delivery_and_continues_next_run() {
        let workspace = TempWorkspace::new("turn-executor", "restart-recovery-continuity");
        let first_runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6, 7],
        )
        .with_delivery_results(vec![Err(CrabError::InvariantViolation {
            context: "deliver",
            message: "network down".to_string(),
        })]);
        let mut first_executor = build_executor(&workspace, first_runtime, 8);
        let first_error = first_executor
            .process_gateway_message(gateway_message("m-restart-failed"))
            .expect_err("first run should fail when delivery fails");
        assert_eq!(
            first_error,
            CrabError::InvariantViolation {
                context: "deliver",
                message: "network down".to_string(),
            }
        );

        let restart_runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "world")]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[11, 12, 13, 14, 15, 16, 17],
        );
        let mut restarted_executor = build_executor(&workspace, restart_runtime, 8);
        let replayed = restarted_executor
            .replay_delivery_for_run(
                "discord:channel:777",
                "run:discord:channel:777:m-restart-failed",
            )
            .expect("restart replay should redeliver missing output");
        assert_eq!(replayed, 1);

        let next_dispatch = restarted_executor
            .process_gateway_message(gateway_message("m-restart-next"))
            .expect("next run after restart should succeed")
            .expect("next run should dispatch");
        assert_eq!(next_dispatch.status, RunStatus::Succeeded);

        let recovered_outbound = restarted_executor
            .composition()
            .state_stores
            .outbound_record_store
            .list_run_records(
                "discord:channel:777",
                "run:discord:channel:777:m-restart-failed",
            )
            .expect("recovered run outbound records should list");
        assert_eq!(recovered_outbound.len(), 1);
        assert_eq!(recovered_outbound[0].edit_generation, 0);

        let restarted_runtime = restarted_executor.runtime_mut();
        assert_eq!(restarted_runtime.delivered_outputs.len(), 2);
        assert_eq!(
            restarted_runtime.delivered_outputs[0],
            (
                "discord:channel:777".to_string(),
                "777".to_string(),
                "delivery:run:discord:channel:777:m-restart-failed:chunk:0".to_string(),
                0,
                "hello".to_string(),
            )
        );
        assert_eq!(
            restarted_runtime.delivered_outputs[1],
            (
                "discord:channel:777".to_string(),
                "777".to_string(),
                "delivery:run:discord:channel:777:m-restart-next:chunk:0".to_string(),
                0,
                "world".to_string(),
            )
        );
    }

    #[test]
    fn process_gateway_message_propagates_enqueue_errors() {
        let workspace = TempWorkspace::new("turn-executor", "process-enqueue-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        runtime.resolve_profile_results =
            VecDeque::from(vec![Err(CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            })]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message("m-process-error"))
            .expect_err("enqueue errors should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_propagates_ingress_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "ingress-validation-error");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let mut invalid = gateway_message("m-invalid");
        invalid.channel_id = " ".to_string();

        let error = executor
            .enqueue_gateway_message(invalid)
            .expect_err("invalid gateway message should fail ingestion");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "channel_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_propagates_run_validation_errors_from_profile_resolution() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-run-validation");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        runtime.resolve_profile_results =
            VecDeque::from(vec![Ok(invalid_sender_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-invalid-profile"))
            .expect_err("invalid profile telemetry should fail run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.sender_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_propagates_session_lookup_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-session-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        replace_path_with_directory(&session_path);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-session-io"))
            .expect_err("session lookup read errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn enqueue_propagates_session_persist_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-session-persist-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        replace_path_with_directory(&index_path);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-session-persist-io"))
            .expect_err("session upsert index errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn enqueue_rolls_back_scheduler_entry_when_post_enqueue_persistence_fails() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-rollback");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[10]);
        runtime.resolve_profile_results =
            VecDeque::from(vec![Err(CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            })]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-rollback"))
            .expect_err("resolve profile failures should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.total_queued_count(), 0);
    }

    #[test]
    fn enqueue_rejects_queue_overflow_and_preserves_existing_fifo_item() {
        let workspace = TempWorkspace::new("turn-executor", "overflow");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 1);

        let first = executor
            .enqueue_gateway_message(gateway_message("m-1"))
            .expect("first enqueue should succeed")
            .expect("first message should be queued");
        assert_eq!(
            first,
            QueuedTurn {
                logical_session_id: "discord:channel:777".to_string(),
                run_id: "run:discord:channel:777:m-1".to_string(),
                message_id: "m-1".to_string(),
                author_id: "111111111111111111".to_string(),
                routing_key: crab_discord::RoutingKey::Channel {
                    channel_id: "777".to_string(),
                },
                queued_run_count: 1,
            }
        );

        let overflow = executor
            .enqueue_gateway_message(gateway_message("m-2"))
            .expect_err("queue overflow should fail");
        assert_eq!(
            overflow,
            CrabError::InvariantViolation {
                context: "lane_queue_overflow",
                message:
                    "Queue is full for this session (limit=1). Please wait for in-flight runs to complete before retrying.".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.total_queued_count(), 1);
    }

    #[test]
    fn dispatch_propagates_clock_errors_before_marking_run_running() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-start-clock");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-dispatch-clock"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing timestamp should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_running_timestamp_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-running-timestamp");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 9, 11]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-running-timestamp"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("backward start timestamp should fail run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "started_at_epoch_ms must be greater than or equal to queued_at_epoch_ms"
                    .to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_session_lookup_io_errors_while_marking_running() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-session-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-dispatch-session-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        replace_path_with_directory(&session_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("session lookup read errors should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_session_persist_io_errors_while_marking_running() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-running-session-persist-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-running-session-persist-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        replace_path_with_directory(&index_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("session upsert errors should fail dispatch while entering running state");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_run_persist_io_errors_after_physical_session_binding() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-physical-run-persist-io");
        let run_id = "run:discord:channel:777:m-physical-run-persist-io";
        let state_root = state_root(&workspace);
        let run_path = run_file_path(&state_root, "discord:channel:777", run_id);
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3])
            .with_ensure_session_sabotage_path(run_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-physical-run-persist-io"))
            .expect("enqueue should succeed");

        let _error = executor
            .dispatch_next_run()
            .expect_err("run upsert errors after session binding should fail dispatch");
    }

    #[test]
    fn dispatch_propagates_session_persist_io_errors_after_backend_binding() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-backend-session-persist-io");
        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3])
            .with_ensure_session_sabotage_path(index_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-backend-session-persist-io"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("session upsert errors after backend binding should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_ensure_physical_session_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-ensure-session");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        runtime.ensure_session_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "ensure_session",
            message: "cannot reuse backend session".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-ensure-session"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("session hydration errors should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "ensure_session",
                message: "cannot reuse backend session".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_backend_turn_execution_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-execute-turn");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        runtime.execute_turn_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "execute_turn",
            message: "backend refused turn".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-execute-turn"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("turn execution errors should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "execute_turn",
                message: "backend refused turn".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_backend_append_clock_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-append-clock");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TextDelta,
                &[("text", "delta")],
            )],
            &[1, 2],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-append-clock"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing backend event timestamp should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_completion_clock_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-completion-clock");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnCompleted,
                &[("done", "true")],
            )],
            &[1, 2, 3],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-completion-clock"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing completion timestamp should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_completion_timestamp_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-completion-timestamp");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnCompleted,
                &[("done", "true")],
            )],
            &[1, 5, 6, 4, 7],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-completion-timestamp"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("backward completion timestamp should fail run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message:
                    "completed_at_epoch_ms must be greater than or equal to started_at_epoch_ms"
                        .to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_completion_session_persist_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-completion-session-persist");
        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4])
            .with_now_epoch_sabotage(3, index_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-completion-session-persist"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("session upsert errors after completion should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_final_run_state_append_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-final-state-append-io");
        let run_id = "run:discord:channel:777:m-final-state-append-io";
        let state_root = state_root(&workspace);
        let log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4])
            .with_now_epoch_sabotage(3, log_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-final-state-append-io"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("final run-state event append errors should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_run_lookup_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-run-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-run-io";
        executor
            .enqueue_gateway_message(gateway_message("m-run-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let run_path = run_file_path(&state_root, "discord:channel:777", run_id);
        replace_path_with_directory(&run_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("run lookup read errors should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "run_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_running_state_append_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-running-append-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-running-append-io";
        executor
            .enqueue_gateway_message(gateway_message("m-running-append-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        replace_path_with_directory(&log_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("running-state event append should fail when log path is a directory");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn mark_run_failed_propagates_run_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "mark-failed-run-validate");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 9]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-mark-failed-validate";
        executor
            .enqueue_gateway_message(gateway_message("m-mark-failed-validate"))
            .expect("enqueue should succeed");

        let error = executor
            .mark_run_failed(
                "discord:channel:777",
                run_id,
                &CrabError::InvariantViolation {
                    context: "test",
                    message: "force failure".to_string(),
                },
            )
            .expect_err("backward completion timestamp should fail failed-run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message:
                    "completed_at_epoch_ms must be greater than or equal to queued_at_epoch_ms"
                        .to_string(),
            }
        );
    }

    #[test]
    fn mark_run_failed_propagates_session_lookup_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "mark-failed-session-read-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 11]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-mark-failed-session-io";
        executor
            .enqueue_gateway_message(gateway_message("m-mark-failed-session-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        replace_path_with_directory(&session_path);

        let error = executor
            .mark_run_failed(
                "discord:channel:777",
                run_id,
                &CrabError::InvariantViolation {
                    context: "test",
                    message: "force failure".to_string(),
                },
            )
            .expect_err("session lookup errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn mark_run_failed_propagates_session_persist_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "mark-failed-session-upsert-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 11]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-mark-failed-session-persist";
        executor
            .enqueue_gateway_message(gateway_message("m-mark-failed-session-persist"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        replace_path_with_directory(&index_path);

        let error = executor
            .mark_run_failed(
                "discord:channel:777",
                run_id,
                &CrabError::InvariantViolation {
                    context: "test",
                    message: "force failure".to_string(),
                },
            )
            .expect_err("session persist errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_marks_run_failed_and_releases_lane_when_runtime_step_errors() {
        let workspace = TempWorkspace::new("turn-executor", "runtime-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 11, 12]);
        runtime.build_context_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "build_context",
            message: "cannot build context".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-error"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("runtime build errors should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "build_context",
                message: "cannot build context".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.active_lane_count(), 0);

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-error")
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.status, RunStatus::Failed);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-error")
            .expect("event replay should succeed");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload.get("state"), Some(&"queued".to_string()));
        assert_eq!(events[1].payload.get("state"), Some(&"running".to_string()));
        assert_eq!(events[2].payload.get("state"), Some(&"failed".to_string()));
        assert!(events[2].payload.contains_key("reason"));
    }

    #[test]
    fn dispatch_derives_cancelled_and_failed_statuses_from_backend_event_stream() {
        let workspace_cancelled = TempWorkspace::new("turn-executor", "cancelled");
        let runtime_cancelled = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnInterrupted,
                &[("reason", "cancelled")],
            )],
            &[1, 2, 3, 4],
        );
        let mut cancelled_executor = build_executor(&workspace_cancelled, runtime_cancelled, 8);
        cancelled_executor
            .enqueue_gateway_message(gateway_message("m-cancel"))
            .expect("enqueue should succeed");
        let cancelled = cancelled_executor
            .dispatch_next_run()
            .expect("dispatch should succeed")
            .expect("run should dispatch");
        assert_eq!(cancelled.status, RunStatus::Cancelled);

        let workspace_failed = TempWorkspace::new("turn-executor", "failed-from-backend");
        let runtime_failed = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::Error,
                &[("message", "backend exploded")],
            )],
            &[10, 11, 12, 13],
        );
        let mut failed_executor = build_executor(&workspace_failed, runtime_failed, 8);
        failed_executor
            .enqueue_gateway_message(gateway_message("m-failed"))
            .expect("enqueue should succeed");
        let failed = failed_executor
            .dispatch_next_run()
            .expect("dispatch should succeed")
            .expect("run should dispatch");
        assert_eq!(failed.status, RunStatus::Failed);

        let failed_events = failed_executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-failed")
            .expect("event replay should succeed");
        assert_eq!(failed_events[2].kind, EventKind::Error);
    }

    #[test]
    fn dispatch_without_pending_work_returns_none_and_bot_messages_are_ignored() {
        let workspace = TempWorkspace::new("turn-executor", "idle");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);

        assert!(executor
            .dispatch_next_run()
            .expect("empty dispatch should succeed")
            .is_none());

        let mut bot_message = gateway_message("m-bot");
        bot_message.author_is_bot = true;
        assert!(executor
            .process_gateway_message(bot_message)
            .expect("ingress should succeed")
            .is_none());
        assert_eq!(executor.composition().scheduler.total_queued_count(), 0);
    }

    #[test]
    fn replay_delivery_for_run_reports_missing_run() {
        let workspace = TempWorkspace::new("turn-executor", "replay-missing-run");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .replay_delivery_for_run("discord:channel:777", "missing")
            .expect_err("replay should fail for missing runs");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_run_lookup",
                message: "run discord:channel:777/missing not found".to_string(),
            }
        );
    }

    #[test]
    fn replay_delivery_for_run_propagates_event_replay_errors() {
        let workspace = TempWorkspace::new("turn-executor", "replay-event-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-replay-event-io"))
            .expect("enqueue should succeed");
        let state_root = state_root(&workspace);
        let log_path = event_log_path(
            &state_root,
            "discord:channel:777",
            "run:discord:channel:777:m-replay-event-io",
        );
        replace_path_with_directory(&log_path);

        let error = executor
            .replay_delivery_for_run(
                "discord:channel:777",
                "run:discord:channel:777:m-replay-event-io",
            )
            .expect_err("replay should propagate event replay errors");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn replay_delivery_for_run_delivers_when_no_records_exist() {
        let workspace = TempWorkspace::new("turn-executor", "replay-deliver-no-records");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TextDelta,
                &[("text", "hello")],
            )],
            &[1, 2, 3, 4],
        )
        .with_delivery_results(vec![Err(CrabError::InvariantViolation {
            context: "deliver",
            message: "network down".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let error = executor
            .process_gateway_message(gateway_message("m-replay-no-records"))
            .expect_err("initial delivery failure should bubble up");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "deliver",
                message: "network down".to_string(),
            }
        );

        let replay_runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut replay_executor = build_executor(&workspace, replay_runtime, 8);
        let delivered = replay_executor
            .replay_delivery_for_run(
                "discord:channel:777",
                "run:discord:channel:777:m-replay-no-records",
            )
            .expect("replay should redeliver the missing output");
        assert_eq!(delivered, 1);
        assert_eq!(replay_executor.runtime_mut().delivered_outputs.len(), 1);
    }

    #[test]
    fn replay_delivery_for_run_propagates_delivery_errors() {
        let workspace = TempWorkspace::new("turn-executor", "replay-deliver-error");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TextDelta,
                &[("text", "hello")],
            )],
            &[1, 2, 3, 4],
        )
        .with_delivery_results(vec![Err(CrabError::InvariantViolation {
            context: "deliver",
            message: "initial send failed".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let _ = executor
            .process_gateway_message(gateway_message("m-replay-deliver-error"))
            .expect_err("initial delivery failure should bubble up");

        let replay_runtime = FakeRuntime::with_backend_events(Vec::new(), &[])
            .with_delivery_results(vec![Err(CrabError::InvariantViolation {
                context: "deliver",
                message: "still down".to_string(),
            })]);
        let mut replay_executor = build_executor(&workspace, replay_runtime, 8);
        let replay_error = replay_executor
            .replay_delivery_for_run(
                "discord:channel:777",
                "run:discord:channel:777:m-replay-deliver-error",
            )
            .expect_err("replay should propagate delivery failures");
        assert_eq!(
            replay_error,
            CrabError::InvariantViolation {
                context: "deliver",
                message: "still down".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_backend_event_append_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-backend-event-append-io");
        let state_root = state_root(&workspace);
        let run_id = "run:discord:channel:777:m-backend-append-io";
        let blocked_log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TextDelta,
                &[("text", "hello")],
            )],
            &[1, 2, 3, 4],
        )
        .with_ensure_session_sabotage_path(blocked_log_path);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message("m-backend-append-io"))
            .expect_err("backend event append failures should bubble up");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn assistant_delivery_splits_on_blank_lines_and_char_limit() {
        let mut delivery = super::DiscordAssistantDelivery::new();

        assert!(delivery.push_delta("").is_empty());

        let planned = delivery.push_delta("a\n\nb");
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].chunk_index, 0);
        assert_eq!(planned[0].edit_generation, 0);
        assert_eq!(planned[0].content, "a".to_string());
        assert_eq!(planned[1].chunk_index, 1);
        assert_eq!(planned[1].edit_generation, 0);
        assert_eq!(planned[1].content, "b".to_string());

        // Leading delimiters should not create an empty chunk.
        let mut delivery = super::DiscordAssistantDelivery::new();
        let planned = delivery.push_delta("\n\nhello");
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].chunk_index, 0);
        assert_eq!(planned[0].content, "hello".to_string());

        // If we already delivered the prefix, delimiters should not force a redundant edit.
        let mut delivery = super::DiscordAssistantDelivery::new();
        assert_eq!(delivery.push_delta("hello").len(), 1);
        assert!(delivery.push_delta("\n\n").is_empty());

        // Exceeding the Discord limit splits into multiple chunks.
        let mut delivery = super::DiscordAssistantDelivery::new();
        let oversized = "a".repeat(crab_discord::DISCORD_MESSAGE_CHAR_LIMIT + 1);
        let planned = delivery.push_delta(&oversized);
        assert_eq!(planned.len(), 2);
        assert_eq!(planned[0].chunk_index, 0);
        assert_eq!(planned[1].chunk_index, 1);
        assert_eq!(
            planned[0].content.chars().count(),
            crab_discord::DISCORD_MESSAGE_CHAR_LIMIT
        );
        assert_eq!(planned[1].content, "a".to_string());

        let (prefix, remainder) = super::split_at_char_limit("abc", 0);
        assert_eq!(prefix, "".to_string());
        assert_eq!(remainder, "abc".to_string());

        let (prefix, remainder) = super::split_at_char_limit("abc", 5);
        assert_eq!(prefix, "abc".to_string());
        assert_eq!(remainder, "".to_string());
    }

    #[test]
    fn deliver_rendered_assistant_output_skips_empty_or_duplicate_attempts() {
        let workspace = TempWorkspace::new("turn-executor", "delivery-skip-cases");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run = delivery_run("discord:channel:777", "run-delivery-skip");

        let skipped_empty = executor
            .deliver_rendered_assistant_output(
                &run,
                &super::delivery_message_id(&run.id, 0),
                "   ",
                0,
                1,
            )
            .expect("empty output should be skipped");
        assert!(!skipped_empty);

        let first = executor
            .deliver_rendered_assistant_output(
                &run,
                &super::delivery_message_id(&run.id, 0),
                "hello",
                0,
                2,
            )
            .expect("first delivery should send");
        let duplicate = executor
            .deliver_rendered_assistant_output(
                &run,
                &super::delivery_message_id(&run.id, 0),
                "hello",
                0,
                3,
            )
            .expect("duplicate should be skipped");
        assert!(first);
        assert!(!duplicate);
        assert_eq!(executor.runtime_mut().delivered_outputs.len(), 1);
    }

    #[test]
    fn deliver_rendered_assistant_output_propagates_should_send_errors() {
        let workspace = TempWorkspace::new("turn-executor", "delivery-should-send-error");
        let state_root = state_root(&workspace);
        let run_id = "run-delivery-should-send-error";
        let should_send_error_path = outbound_log_path(&state_root, "discord:channel:777", run_id);
        replace_path_with_directory(&should_send_error_path);

        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run = delivery_run("discord:channel:777", run_id);
        let should_send_error = executor
            .deliver_rendered_assistant_output(
                &run,
                &super::delivery_message_id(&run.id, 0),
                "hello",
                0,
                1,
            )
            .expect_err("should_send read failures should propagate");
        assert!(matches!(
            should_send_error,
            CrabError::Io {
                context: "outbound_record_read",
                ..
            }
        ));
        assert!(executor.runtime_mut().delivered_outputs.is_empty());
    }

    #[test]
    fn deliver_rendered_assistant_output_propagates_target_and_mark_sent_errors() {
        let workspace = TempWorkspace::new("turn-executor", "delivery-error-paths");
        let state_root = state_root(&workspace);
        let mark_sent_error_path = outbound_log_path(
            &state_root,
            "discord:channel:777",
            "run-delivery-mark-sent-error",
        );
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[])
            .with_delivery_sabotage_path(mark_sent_error_path);
        let mut executor = build_executor(&workspace, runtime, 8);

        let invalid_target_run = delivery_run("discord:unknown:777", "run-delivery-invalid-target");
        let target_error = executor
            .deliver_rendered_assistant_output(
                &invalid_target_run,
                &super::delivery_message_id(&invalid_target_run.id, 0),
                "hello",
                0,
                1,
            )
            .expect_err("invalid logical session shape should fail delivery");
        assert!(matches!(
            target_error,
            CrabError::InvariantViolation {
                context: "turn_executor_delivery_target",
                ..
            }
        ));

        let mark_sent_error_run =
            delivery_run("discord:channel:777", "run-delivery-mark-sent-error");
        let mark_sent_error = executor
            .deliver_rendered_assistant_output(
                &mark_sent_error_run,
                &super::delivery_message_id(&mark_sent_error_run.id, 0),
                "hello",
                0,
                2,
            )
            .expect_err("mark_sent write failures should propagate");
        assert!(matches!(
            mark_sent_error,
            CrabError::Io {
                context: "outbound_record_read",
                ..
            }
        ));
    }

    #[test]
    fn replay_delivery_for_run_skips_already_recorded_stream_generations() {
        let workspace = TempWorkspace::new("turn-executor", "replay-delivery-skip-duplicates");
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hel")]),
                backend_event(2, BackendEventKind::TextDelta, &[("text", "lo")]),
                backend_event(3, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .process_gateway_message(gateway_message("m-replay-skip"))
            .expect("initial dispatch should succeed")
            .expect("run should dispatch");

        let replay_runtime = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut replay_executor = build_executor(&workspace, replay_runtime, 8);
        let delivered = replay_executor
            .replay_delivery_for_run(
                "discord:channel:777",
                "run:discord:channel:777:m-replay-skip",
            )
            .expect("replay should succeed");
        assert_eq!(delivered, 0);
        assert!(replay_executor.runtime_mut().delivered_outputs.is_empty());
    }

    #[test]
    fn replay_delivery_for_run_redelivers_missing_generation_after_delivery_failure() {
        let workspace = TempWorkspace::new("turn-executor", "replay-delivery-redeliver-missing");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hel")]),
                backend_event(2, BackendEventKind::TextDelta, &[("text", "lo")]),
            ],
            &[1, 2, 3, 4, 5],
        )
        .with_delivery_results(vec![
            Ok(()),
            Err(CrabError::InvariantViolation {
                context: "deliver",
                message: "post-edit failed".to_string(),
            }),
        ]);
        let mut executor = build_executor(&workspace, runtime.clone(), 8);
        let error = executor
            .process_gateway_message(gateway_message("m-replay-redeliver"))
            .expect_err("second delivery failure should bubble up");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "deliver",
                message: "post-edit failed".to_string(),
            }
        );

        let first_attempt_records = executor
            .composition()
            .state_stores
            .outbound_record_store
            .list_run_records(
                "discord:channel:777",
                "run:discord:channel:777:m-replay-redeliver",
            )
            .expect("outbound records should be listable");
        assert_eq!(first_attempt_records.len(), 1);
        assert_eq!(first_attempt_records[0].edit_generation, 0);

        runtime.deliver_results = VecDeque::new();
        runtime.delivered_outputs.clear();
        let mut replay_executor = build_executor(&workspace, runtime, 8);
        let delivered = replay_executor
            .replay_delivery_for_run(
                "discord:channel:777",
                "run:discord:channel:777:m-replay-redeliver",
            )
            .expect("replay should redeliver missing generation");
        assert_eq!(delivered, 1);
        assert_eq!(
            replay_executor.runtime_mut().delivered_outputs,
            vec![(
                "discord:channel:777".to_string(),
                "777".to_string(),
                "delivery:run:discord:channel:777:m-replay-redeliver:chunk:0".to_string(),
                1,
                "hello".to_string(),
            )]
        );
    }

    #[test]
    fn usage_accounting_helpers_resolve_latest_supported_payload_variant() {
        let usage = super::resolve_backend_usage_accounting(&[
            backend_event(
                1,
                BackendEventKind::RunNote,
                &[
                    ("usage_input_tokens", "1"),
                    ("usage_output_tokens", "2"),
                    ("usage_total_tokens", "3"),
                ],
            ),
            backend_event(
                2,
                BackendEventKind::RunNote,
                &[
                    ("run_usage_input_tokens", "4"),
                    ("run_usage_output_tokens", "5"),
                    ("run_usage_total_tokens", "9"),
                ],
            ),
            backend_event(
                3,
                BackendEventKind::TurnCompleted,
                &[
                    ("input_tokens", "10"),
                    ("output_tokens", "6"),
                    ("total_tokens", "16"),
                ],
            ),
        ])
        .expect("usage parsing should succeed")
        .expect("latest usage payload should be selected");
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 6);
        assert_eq!(usage.total_tokens, 16);

        let none = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[("note", "no usage here")],
        )])
        .expect("usage parsing should succeed without usage payload");
        assert!(none.is_none());
    }

    #[test]
    fn usage_accounting_helpers_reject_invalid_payloads() {
        let run_usage_missing_field = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[("run_usage_total_tokens", "1")],
        )])
        .expect_err("missing run_usage fields should fail");
        assert_eq!(
            run_usage_missing_field,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "usage payload is missing required key run_usage_input_tokens while parsing run_usage_input_tokens/run_usage_output_tokens/run_usage_total_tokens".to_string(),
            }
        );

        let missing_field = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[("usage_total_tokens", "1")],
        )])
        .expect_err("missing usage fields should fail");
        assert_eq!(
            missing_field,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "usage payload is missing required key usage_input_tokens while parsing usage_input_tokens/usage_output_tokens/usage_total_tokens".to_string(),
            }
        );

        let non_numeric = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[
                ("usage_input_tokens", "a"),
                ("usage_output_tokens", "2"),
                ("usage_total_tokens", "3"),
            ],
        )])
        .expect_err("non-numeric usage values should fail");
        assert_eq!(
            non_numeric,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "usage payload key usage_input_tokens must be an unsigned integer"
                    .to_string(),
            }
        );

        let missing_output = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[("usage_input_tokens", "1"), ("usage_total_tokens", "1")],
        )])
        .expect_err("missing output usage value should fail");
        assert_eq!(
            missing_output,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "usage payload is missing required key usage_output_tokens while parsing usage_input_tokens/usage_output_tokens/usage_total_tokens".to_string(),
            }
        );

        let missing_total = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[("usage_input_tokens", "1"), ("usage_output_tokens", "1")],
        )])
        .expect_err("missing total usage value should fail");
        assert_eq!(
            missing_total,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "usage payload is missing required key usage_total_tokens while parsing usage_input_tokens/usage_output_tokens/usage_total_tokens".to_string(),
            }
        );

        let overflow = super::resolve_backend_usage_accounting(&[backend_event(
            1,
            BackendEventKind::RunNote,
            &[
                ("usage_input_tokens", "18446744073709551615"),
                ("usage_output_tokens", "1"),
                ("usage_total_tokens", "18446744073709551615"),
            ],
        )])
        .expect_err("usage overflow should fail");
        assert_eq!(
            overflow,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message:
                    "usage payload overflow while adding usage_input_tokens and usage_output_tokens"
                        .to_string(),
            }
        );
    }

    #[test]
    fn merge_token_accounting_handles_success_and_overflow() {
        let merged = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: 1,
                output_tokens: 2,
                total_tokens: 3,
            },
            crab_core::TokenAccounting {
                input_tokens: 4,
                output_tokens: 5,
                total_tokens: 9,
            },
        )
        .expect("merge should succeed");
        assert_eq!(merged.input_tokens, 5);
        assert_eq!(merged.output_tokens, 7);
        assert_eq!(merged.total_tokens, 12);

        let overflow = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: u64::MAX,
                output_tokens: 0,
                total_tokens: 0,
            },
            crab_core::TokenAccounting {
                input_tokens: 1,
                output_tokens: 0,
                total_tokens: 0,
            },
        )
        .expect_err("merge overflow should fail");
        assert_eq!(
            overflow,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "input token accounting overflow".to_string(),
            }
        );

        let output_overflow = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: u64::MAX,
                total_tokens: 0,
            },
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 1,
                total_tokens: 0,
            },
        )
        .expect_err("output merge overflow should fail");
        assert_eq!(
            output_overflow,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "output token accounting overflow".to_string(),
            }
        );

        let total_overflow = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: u64::MAX,
            },
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 1,
            },
        )
        .expect_err("total merge overflow should fail");
        assert_eq!(
            total_overflow,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "total token accounting overflow".to_string(),
            }
        );
    }

    #[test]
    fn helper_paths_cover_missing_scripts_and_status_tokens() {
        assert_eq!(super::run_status_token(RunStatus::Queued), "queued");
        assert_eq!(super::run_status_token(RunStatus::Running), "running");
        assert_eq!(super::run_status_token(RunStatus::Succeeded), "succeeded");
        assert_eq!(super::run_status_token(RunStatus::Failed), "failed");
        assert_eq!(super::run_status_token(RunStatus::Cancelled), "cancelled");
        assert_eq!(
            super::parse_manual_rotation_command("hello world").expect("non command should parse"),
            None
        );
        assert_eq!(
            super::parse_manual_rotation_command("   ").expect("blank input should parse"),
            None
        );
        assert_eq!(
            super::parse_manual_rotation_command("/compactx")
                .expect("unknown compact-like command should parse"),
            None
        );
        assert_eq!(
            super::parse_manual_rotation_command("/compact confirm")
                .expect("manual compact should parse"),
            Some((
                crab_core::OperatorCommand::ManualCompact,
                crab_core::ManualRotationRequest::Compact
            ))
        );
        assert_eq!(
            super::parse_manual_rotation_command("/reset confirm")
                .expect("manual reset should parse"),
            Some((
                crab_core::OperatorCommand::ManualReset,
                crab_core::ManualRotationRequest::Reset
            ))
        );
        assert_eq!(
            super::operator_command_token(crab_core::OperatorCommand::ManualCompact),
            "/compact"
        );
        assert_eq!(
            super::operator_command_token(crab_core::OperatorCommand::ManualReset),
            "/reset"
        );
        assert_eq!(
            super::operator_command_token(crab_core::OperatorCommand::ShowProfile),
            "other"
        );
        assert_eq!(
            super::manual_rotation_request_token(crab_core::ManualRotationRequest::Compact),
            "compact"
        );
        assert_eq!(
            super::manual_rotation_request_token(crab_core::ManualRotationRequest::Reset),
            "reset"
        );
        assert_eq!(
            super::rotation_trigger_token(crab_core::RotationTrigger::ManualCompact),
            "manual_compact"
        );
        assert_eq!(
            super::rotation_trigger_token(crab_core::RotationTrigger::ManualReset),
            "manual_reset"
        );
        assert_eq!(
            super::rotation_trigger_token(crab_core::RotationTrigger::TokenCompaction),
            "token_compaction"
        );
        assert_eq!(
            super::rotation_trigger_token(crab_core::RotationTrigger::InactivityTimeout),
            "inactivity_timeout"
        );
        assert_eq!(
            super::render_rotation_triggers(&[
                crab_core::RotationTrigger::ManualCompact,
                crab_core::RotationTrigger::TokenCompaction,
            ]),
            "manual_compact,token_compaction".to_string()
        );
        assert_eq!(super::backend_kind_token(BackendKind::Claude), "claude");
        assert_eq!(super::backend_kind_token(BackendKind::Codex), "codex");
        assert_eq!(super::backend_kind_token(BackendKind::OpenCode), "opencode");
        assert_eq!(
            super::delivery_message_id("run:discord:channel:777:msg", 0),
            "delivery:run:discord:channel:777:msg:chunk:0"
        );
        assert_eq!(
            super::delivery_channel_id("discord:channel:777"),
            Ok("777".to_string())
        );
        assert_eq!(
            super::delivery_channel_id("discord:thread:888"),
            Ok("888".to_string())
        );
        assert!(matches!(
            super::delivery_channel_id("discord:dm:999"),
            Err(CrabError::InvariantViolation {
                context: "turn_executor_delivery_target",
                ..
            })
        ));
        assert!(matches!(
            super::delivery_channel_id("discord:unknown:777"),
            Err(CrabError::InvariantViolation {
                context: "turn_executor_delivery_target",
                ..
            })
        ));
        assert!(matches!(
            super::delivery_channel_id("discord:channel:"),
            Err(CrabError::InvariantViolation {
                context: "turn_executor_delivery_target",
                ..
            })
        ));
        assert!(matches!(
            super::delivery_channel_id("discord:channel"),
            Err(CrabError::InvariantViolation {
                context: "turn_executor_delivery_target",
                ..
            })
        ));
        let delta_backend = backend_event(1, BackendEventKind::TextDelta, &[("delta", "x")]);
        assert_eq!(super::extract_backend_text_delta(&delta_backend), Some("x"));
        let non_delta_backend = backend_event(2, BackendEventKind::ToolCall, &[("tool", "sh")]);
        assert_eq!(super::extract_backend_text_delta(&non_delta_backend), None);
        let delta_event = crab_core::EventEnvelope {
            event_id: "evt-1".to_string(),
            run_id: "run-1".to_string(),
            turn_id: Some("turn-1".to_string()),
            lane_id: Some("discord:channel:1".to_string()),
            logical_session_id: "discord:channel:1".to_string(),
            physical_session_id: None,
            backend: Some(BackendKind::Codex),
            resolved_model: Some("gpt-5-codex".to_string()),
            resolved_reasoning_level: Some("medium".to_string()),
            profile_source: Some("global_default".to_string()),
            sequence: 1,
            emitted_at_epoch_ms: 1,
            source: crab_core::EventSource::Backend,
            kind: EventKind::TextDelta,
            payload: BTreeMap::from([("delta".to_string(), "y".to_string())]),
            profile: Some(sample_profile_telemetry()),
            idempotency_key: Some("event-1".to_string()),
        };
        assert_eq!(super::extract_event_text_delta(&delta_event), Some("y"));
        let mut non_delta_event = delta_event.clone();
        non_delta_event.kind = EventKind::ToolCall;
        assert_eq!(super::extract_event_text_delta(&non_delta_event), None);

        let run = delivery_run("discord:channel:1", "run-1");
        let transcript = super::build_fallback_transcript_entries(
            &run,
            &[
                delta_event.clone(),
                crab_core::EventEnvelope {
                    kind: EventKind::TextDelta,
                    payload: BTreeMap::new(),
                    ..delta_event.clone()
                },
                crab_core::EventEnvelope {
                    kind: EventKind::RunNote,
                    payload: BTreeMap::from([("note".to_string(), "remember".to_string())]),
                    ..delta_event.clone()
                },
                crab_core::EventEnvelope {
                    kind: EventKind::RunNote,
                    payload: BTreeMap::new(),
                    ..delta_event.clone()
                },
                crab_core::EventEnvelope {
                    kind: EventKind::ToolResult,
                    payload: BTreeMap::from([("status".to_string(), "ok".to_string())]),
                    ..delta_event.clone()
                },
                crab_core::EventEnvelope {
                    kind: EventKind::ToolCall,
                    payload: BTreeMap::new(),
                    ..delta_event.clone()
                },
                crab_core::EventEnvelope {
                    kind: EventKind::Error,
                    payload: BTreeMap::from([("error".to_string(), "boom".to_string())]),
                    ..delta_event.clone()
                },
            ],
        );
        assert_eq!(transcript[0].role, crab_core::TranscriptEntryRole::User);
        assert_eq!(
            transcript[1].role,
            crab_core::TranscriptEntryRole::Assistant
        );
        assert!(transcript
            .iter()
            .any(|entry| entry.role == crab_core::TranscriptEntryRole::System));
        assert!(transcript
            .iter()
            .any(|entry| entry.role == crab_core::TranscriptEntryRole::Tool));
        assert!(!transcript
            .iter()
            .any(|entry| entry.text.contains("error=boom")));
        assert!(!transcript.iter().any(|entry| entry.text.trim().is_empty()));

        let workspace_missing_profile =
            TempWorkspace::new("turn-executor", "missing-profile-script");
        let mut runtime_missing_profile = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        runtime_missing_profile.resolve_profile_results = VecDeque::new();
        let mut missing_profile_executor =
            build_executor(&workspace_missing_profile, runtime_missing_profile, 8);
        let profile_error = missing_profile_executor
            .enqueue_gateway_message(gateway_message("m-missing-profile"))
            .expect_err("missing profile script should fail");
        assert_eq!(
            profile_error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_resolve_profile",
                message: "missing scripted runtime result".to_string(),
            }
        );

        let workspace_missing_clock = TempWorkspace::new("turn-executor", "missing-clock-script");
        let runtime_missing_clock = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut missing_clock_executor =
            build_executor(&workspace_missing_clock, runtime_missing_clock, 8);
        let clock_error = missing_clock_executor
            .enqueue_gateway_message(gateway_message("m-missing-clock"))
            .expect_err("missing clock script should fail");
        assert_eq!(
            clock_error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_reports_missing_run_for_unknown_scheduler_entry() {
        let workspace = TempWorkspace::new("turn-executor", "missing-run");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .composition_mut()
            .scheduler
            .enqueue(
                "discord:channel:777",
                crab_scheduler::QueuedRun {
                    run_id: "missing".to_string(),
                },
            )
            .expect("queue injection should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing run should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_run_lookup",
                message: "run discord:channel:777/missing not found".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.active_lane_count(), 0);
    }

    #[test]
    fn helper_replace_path_with_directory_handles_existing_directory() {
        let workspace = TempWorkspace::new("turn-executor", "replace-path-dir");
        let target = workspace.path.join("already-directory");
        fs::create_dir_all(&target).expect("directory fixture should be creatable");

        replace_path_with_directory(&target);
        assert!(target.is_dir());
    }
}

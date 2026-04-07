use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use crab_backends::{BackendEvent, BackendEventKind, BackendEventStream};
use crab_core::{
    build_onboarding_extraction_prompt, consume_pending_rotation, detect_workspace_bootstrap_state,
    execute_onboarding_completion_protocol, execute_rotation_sequence,
    parse_onboarding_capture_document, persist_onboarding_profile_files, read_pending_rotations,
    Checkpoint, CheckpointTurnDocument, CrabError, CrabResult, EventEnvelope, EventKind,
    EventSource, InferenceProfile, LaneState, LogicalSession, OnboardingCompletionEventRuntime,
    OnboardingCompletionInput, PhysicalSession, RotationSequenceRuntime, Run, RunProfileTelemetry,
    RunStatus, TokenAccounting, WorkspaceBootstrapState, ONBOARDING_CAPTURE_INCOMPLETE_TOKEN,
};
use crab_discord::{
    DeliveryAttempt, GatewayAttachment, GatewayMessage, RoutingKey, ShouldSendDecision,
};
use crab_scheduler::QueuedRun;
use crab_store::CheckpointStore;
use futures::executor::block_on;
use futures::future::poll_fn;
use futures::StreamExt;

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

    fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>>;

    fn interrupt_backend_turn(
        &mut self,
        session: &PhysicalSession,
        turn_id: &str,
    ) -> CrabResult<()>;
}

pub struct TurnExecutor<R: TurnExecutorRuntime> {
    composition: AppComposition,
    runtime: R,
}

impl<R: TurnExecutorRuntime> TurnExecutor<R> {
    #[must_use]
    pub fn new(composition: AppComposition, runtime: R) -> Self {
        Self {
            composition,
            runtime,
        }
    }

    #[must_use]
    pub fn composition(&self) -> &AppComposition {
        &self.composition
    }

    #[must_use]
    pub fn composition_mut(&mut self) -> &mut AppComposition {
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

    pub fn enqueue_pending_trigger(
        &mut self,
        channel_id: &str,
        message: &str,
    ) -> CrabResult<QueuedTurn> {
        if channel_id.trim().is_empty() {
            return Err(CrabError::InvariantViolation {
                context: "pending_trigger_enqueue",
                message: "channel_id must not be empty".to_string(),
            });
        }
        let trigger_id = format!("trigger:{}", self.runtime.now_epoch_ms()?);
        let ingress = crab_discord::IngressMessage {
            message_id: trigger_id,
            // Use a synthetic numeric author ID because sender identity validation
            // requires digits-only Discord user IDs.
            author_id: "0".to_string(),
            channel_id: channel_id.to_string(),
            content: message.to_string(),
            routing_key: RoutingKey::Channel {
                channel_id: channel_id.to_string(),
            },
            attachments: vec![],
        };
        self.enqueue_ingress_message(ingress)
    }

    fn check_for_steering_message(&mut self, current_logical_session_id: &str) -> CrabResult<bool> {
        let Some(message) = self.runtime.next_gateway_message()? else {
            return self.check_for_steering_trigger(current_logical_session_id);
        };
        let Some(queued) = self.enqueue_gateway_message(message)? else {
            return self.check_for_steering_trigger(current_logical_session_id);
        };
        if queued.logical_session_id == current_logical_session_id {
            return Ok(true);
        }
        self.check_for_steering_trigger(current_logical_session_id)
    }

    fn check_for_steering_trigger(&mut self, current_logical_session_id: &str) -> CrabResult<bool> {
        let state_root = self.composition.state_stores.root.clone();
        let triggers = crab_core::read_steering_triggers(&state_root)?;
        let (matched, _) = self.consume_and_batch_triggers(
            triggers,
            current_logical_session_id,
            crab_core::consume_steering_trigger,
            "steering",
        )?;
        Ok(matched)
    }

    fn check_for_graceful_steering_trigger(
        &mut self,
        current_logical_session_id: &str,
    ) -> CrabResult<bool> {
        let state_root = self.composition.state_stores.root.clone();
        let triggers = crab_core::read_graceful_steering_triggers(&state_root)?;
        let (matched, _) = self.consume_and_batch_triggers(
            triggers,
            current_logical_session_id,
            crab_core::consume_graceful_steering_trigger,
            "graceful steering",
        )?;
        Ok(matched)
    }

    /// Consume all trigger files, batch messages by channel_id, and enqueue
    /// one combined run per channel. Returns true if any trigger matched the
    /// current lane.
    ///
    /// Design invariants (from Codex review):
    /// - Only delete trigger files whose messages were successfully enqueued.
    ///   Files for failed enqueues stay on disk for retry (no silent message loss).
    /// - Messages within a batch are ordered deterministically by filename
    ///   (timestamp + pid + monotonic counter). Exact chronological within a
    ///   single process; approximately chronological cross-process.
    /// - Message boundaries are preserved with `---` delimiters so the agent
    ///   can distinguish separate user messages from a single multi-line message.
    /// Returns `(matched_current_lane, consumed_trigger_count)`.
    pub fn consume_and_batch_triggers(
        &mut self,
        triggers: Vec<(PathBuf, crab_core::PendingTrigger)>,
        current_logical_session_id: &str,
        consume_fn: fn(&Path) -> CrabResult<()>,
        _label: &str,
    ) -> CrabResult<(bool, usize)> {
        if triggers.is_empty() {
            return Ok((false, 0));
        }

        // Group messages and their trigger paths by channel_id, preserving
        // chronological order (read_signal_files returns sorted results).
        let mut by_channel: BTreeMap<String, Vec<(PathBuf, String)>> = BTreeMap::new();
        for (trigger_path, trigger) in triggers {
            by_channel
                .entry(trigger.channel_id)
                .or_default()
                .push((trigger_path, trigger.message));
        }

        let mut matched_current_lane = false;
        let mut consumed_count = 0_usize;
        let mut deferred_delete_error: Option<CrabError> = None;

        for (channel_id, entries) in &by_channel {
            // Build combined message with explicit delimiters between messages.
            let combined = if entries.len() == 1 {
                entries[0].1.clone()
            } else {
                entries
                    .iter()
                    .map(|(_, msg)| msg.as_str())
                    .collect::<Vec<_>>()
                    .join("\n---\n")
            };

            match self.enqueue_pending_trigger(channel_id, &combined) {
                Ok(queued) => {
                    // Enqueue succeeded — delete all trigger files for this channel.
                    // Best-effort: attempt ALL deletions even if one fails, so we
                    // minimize leftover files that would cause duplicates on re-poll.
                    // Defer the first error until after all channels are processed,
                    // so a delete failure for one channel doesn't block steering
                    // detection for the current lane.
                    for (path, _) in entries {
                        if let Err(error) = consume_fn(path) {
                            if deferred_delete_error.is_none() {
                                deferred_delete_error = Some(error);
                            }
                        } else {
                            consumed_count += 1;
                        }
                    }
                    if queued.logical_session_id == current_logical_session_id {
                        matched_current_lane = true;
                    }
                }
                Err(_error) => {
                    // Enqueue failed — leave trigger files on disk for retry.
                    // This avoids the silent message loss that caused the
                    // reverted collapse attempt (commit eebf825).
                    #[cfg(not(coverage))]
                    tracing::warn!(
                        channel_id = %channel_id,
                        trigger_count = entries.len(),
                        error = %_error,
                        "failed to enqueue batched {} triggers, files retained for retry",
                        _label,
                    );
                }
            }
        }

        if let Some(error) = deferred_delete_error {
            return Err(error);
        }

        Ok((matched_current_lane, consumed_count))
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
            if event.kind == crab_core::EventKind::ToolCall {
                for op in delivery.notify_tool_boundary() {
                    let msg_id = delivery_message_id(&run.id, op.chunk_index);
                    let result = self.deliver_rendered_assistant_output(
                        &run,
                        &msg_id,
                        &op.content,
                        op.edit_generation,
                        event.emitted_at_epoch_ms,
                    );
                    if result? {
                        delivered_count += 1;
                    }
                }
            }
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
            user_input: build_user_input_with_attachments(
                &ingress.content,
                &ingress.attachments,
                &self.composition.state_stores.root,
                run_id,
            ),
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
        // Deliberately not updating last_activity_epoch_ms here: message enqueue is not
        // backend activity. Inactivity timeout must measure the gap from the previous run's
        // completion to the next run's start, so only execute_dispatched_run updates this field.
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
            "run started"
        );

        if onboarding_gate_resolution.is_none() {
            onboarding_completion_resolution =
                self.maybe_complete_pending_onboarding_capture(&run, started_at_epoch_ms)?;
            if let Some(ref resolution) = onboarding_completion_resolution {
                supplemental_emitted_event_count =
                    supplemental_emitted_event_count.saturating_add(resolution.emitted_event_count);
            }
        }

        let mut steered = false;
        if onboarding_completion_resolution.is_none() && onboarding_gate_resolution.is_none() {
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

            let inject_bootstrap_context = !session.has_injected_bootstrap;
            let turn_context = self.runtime.build_turn_context(
                &run,
                &session,
                &physical_session,
                inject_bootstrap_context,
            )?;
            if inject_bootstrap_context {
                session.has_injected_bootstrap = true;
                self.composition
                    .state_stores
                    .session_store
                    .upsert_session(&session)?;
            }
            let backend_event_stream = self.runtime.execute_backend_turn(
                &mut physical_session,
                &run,
                &turn_id,
                &turn_context,
            )?;

            // Bridge the async event stream to a sync channel so we can interleave
            // timeout-based polling with steering message checks.
            let (event_tx, event_rx) = std::sync::mpsc::channel();
            let bridge_handle = std::thread::spawn(move || {
                let mut stream = backend_event_stream;
                while let Some(event) = block_on(StreamExt::next(&mut stream)) {
                    if event_tx.send(event).is_err() {
                        break;
                    }
                }
            });

            let steering_poll_interval = Duration::from_millis(200);
            let mut last_event_was_tool_result = false;
            let mut graceful_steer_pending = false;
            loop {
                let maybe_event = match event_rx.recv_timeout(steering_poll_interval) {
                    Ok(event) => Some(event),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                if let Some(backend_event) = maybe_event {
                    // Graceful steering: detect agentic loop boundaries BEFORE
                    // appending/rendering the event, so the first event of the
                    // next iteration is not leaked to the user.
                    match &backend_event.kind {
                        BackendEventKind::ToolResult => {
                            last_event_was_tool_result = true;
                        }
                        BackendEventKind::TextDelta | BackendEventKind::ToolCall => {
                            if last_event_was_tool_result {
                                // Potential loop boundary: tools finished, new iteration starting.
                                // Fresh-poll for graceful triggers that arrived since last check.
                                if !graceful_steer_pending {
                                    let lsid = &run.logical_session_id;
                                    graceful_steer_pending =
                                        self.check_for_graceful_steering_trigger(lsid)?;
                                }
                                if graceful_steer_pending {
                                    // Drain any immediate steer so it doesn't fire later.
                                    let _ =
                                        self.check_for_steering_message(&run.logical_session_id)?;
                                    // Kill before appending/rendering the boundary event.
                                    let _ = self
                                        .runtime
                                        .interrupt_backend_turn(&physical_session, &turn_id);
                                    steered = true;
                                    break;
                                }
                            }
                            last_event_was_tool_result = false;
                        }
                        BackendEventKind::TurnCompleted => {
                            // Turn completed naturally while graceful steer was pending.
                            // The turn's output is valid (not interrupted). The steering
                            // message is already enqueued and will be dispatched next.
                            // Do NOT set steered here: the run succeeded normally.
                        }
                        _ => {}
                    }

                    let emitted_at_epoch_ms = self.runtime.now_epoch_ms()?;
                    self.append_backend_event(&run, &backend_event, emitted_at_epoch_ms)?;
                    if backend_event.kind == BackendEventKind::ToolCall {
                        for op in delivery.notify_tool_boundary() {
                            let _ = self.deliver_rendered_assistant_output(
                                &run,
                                &delivery_message_id(&run.id, op.chunk_index),
                                &op.content,
                                op.edit_generation,
                                emitted_at_epoch_ms,
                            )?;
                        }
                    }
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

                // Immediate steering check (always takes priority)
                if self.check_for_steering_message(&run.logical_session_id)? {
                    let _ = self
                        .runtime
                        .interrupt_backend_turn(&physical_session, &turn_id);
                    steered = true;
                    break;
                }

                // Graceful steering check (does NOT break immediately)
                if !graceful_steer_pending
                    && self.check_for_graceful_steering_trigger(&run.logical_session_id)?
                {
                    graceful_steer_pending = true;
                }
            }
            drop(event_rx);
            let _ = bridge_handle.join();

            if steered {
                let steered_at_epoch_ms = self.runtime.now_epoch_ms()?;
                let mut payload = BTreeMap::new();
                payload.insert("event".to_string(), "run_steered".to_string());
                self.append_system_run_note(&run, payload, steered_at_epoch_ms)?;
                supplemental_emitted_event_count =
                    supplemental_emitted_event_count.saturating_add(1);
            }
        } else {
            run.physical_session_id = session.active_physical_session_id.clone();
        }

        let final_status = if steered {
            RunStatus::Cancelled
        } else if onboarding_completion_resolution.is_some() || onboarding_gate_resolution.is_some()
        {
            RunStatus::Succeeded
        } else {
            derive_final_status(&backend_events)
        };
        let completed_at_epoch_ms = self.runtime.now_epoch_ms()?;
        run.status = final_status;
        run.completed_at_epoch_ms = Some(completed_at_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        // Do not clear the physical session on ordinary failures: physical sessions are valuable
        // continuity handles and should be preserved unless we explicitly rotate/reset.

        if let Some(run_usage) = resolve_backend_usage_accounting(&backend_events)? {
            session.token_accounting =
                merge_token_accounting(session.token_accounting.clone(), run_usage)?;
        } else if !backend_events.is_empty() && final_status == RunStatus::Succeeded {
            let mut payload = BTreeMap::new();
            payload.insert("event".to_string(), "missing_usage_data".to_string());
            payload.insert(
                "backend_event_count".to_string(),
                backend_events.len().to_string(),
            );
            self.append_system_run_note(&run, payload, completed_at_epoch_ms)?;
            supplemental_emitted_event_count = supplemental_emitted_event_count.saturating_add(1);
        }
        let token_total_before_rotation = session.token_accounting.context_window_tokens();
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

        let rotation_outcome = if onboarding_gate_resolution.is_some() {
            None
        } else {
            self.maybe_execute_cli_rotation(&run, &mut session, completed_at_epoch_ms)?
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
            token_total_after_rotation = session.token_accounting.context_window_tokens(),
            "run completed"
        );

        if let Some(onboarding_completion_resolution) = onboarding_completion_resolution {
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
        } else if let Some(ref outcome) = rotation_outcome {
            let notification = format!(
                "Crab: session rotated (cli_rotation). Checkpoint: {}",
                outcome.checkpoint_id
            );
            let notification_id = format!("delivery:{}:rotation-notification", run.id);
            // Keep on one line: multi-line call sites can produce llvm-cov line-mapping gaps.
            #[rustfmt::skip]
            let _ = self.deliver_rendered_assistant_output(&run, &notification_id, &notification, 0, completed_at_epoch_ms)?;
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

        cleanup_attachment_directory(&self.composition.state_stores.root, &run.id);

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
        // Combine guards on a single arm: the second condition (sender/DM) is a defensive
        // invariant that is currently unreachable in normal flow because the upstream
        // onboarding gate already enforces owner+DM, but it protects against future gate changes.
        if bootstrap_state != WorkspaceBootstrapState::PendingBootstrap
            || !run.profile.sender_is_owner
            || !is_dm_logical_session_id(&run.logical_session_id)
        {
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

    fn maybe_execute_cli_rotation(
        &mut self,
        run: &Run,
        session: &mut LogicalSession,
        now_epoch_ms: u64,
    ) -> CrabResult<Option<RotationExecutionOutcome>> {
        let pending = read_pending_rotations(&self.composition.state_stores.root)?;
        if pending.is_empty() {
            return Ok(None);
        }

        let (first_path, first_rotation) = pending.into_iter().next().expect("checked non-empty");

        let mut started_payload = BTreeMap::new();
        started_payload.insert("rotation_event".to_string(), "started".to_string());
        started_payload.insert("trigger".to_string(), "cli_rotation".to_string());
        // Keep on one line: multi-line call sites can produce llvm-cov line-mapping gaps.
        #[rustfmt::skip]
        self.append_run_event(run, EventKind::RunNote, EventSource::System, started_payload, now_epoch_ms)?;

        #[cfg(not(coverage))]
        tracing::info!(
            logical_session_id = %run.logical_session_id,
            run_id = %run.id,
            trigger = "cli_rotation",
            token_usage_total = session.token_accounting.context_window_tokens(),
            "rotation started"
        );

        session.lane_state = LaneState::Rotating;
        self.composition
            .state_stores
            .session_store
            .upsert_session(session)?;

        let (onboarding_completion_resolution, onboarding_rotation_note_count) =
            self.maybe_complete_pending_onboarding_from_rotation(run, session, now_epoch_ms)?;

        let checkpoint_store = self.composition.state_stores.checkpoint_store.clone();
        let mut rotation_runtime = TurnExecutorRotationRuntime {
            run,
            logical_session: session,
            checkpoint_store,
            checkpoint_created_at_epoch_ms: now_epoch_ms,
        };
        let outcome = execute_rotation_sequence(&mut rotation_runtime, &first_rotation.checkpoint)?;

        // Consume the pending rotation file.
        consume_pending_rotation(&first_path)?;

        session.last_successful_checkpoint_id = Some(outcome.checkpoint_id.clone());
        session.lane_state = LaneState::Idle;

        #[cfg(not(coverage))]
        tracing::info!(
            logical_session_id = %run.logical_session_id,
            run_id = %run.id,
            trigger = "cli_rotation",
            checkpoint_id = %outcome.checkpoint_id,
            "rotation completed"
        );

        let mut completed_payload = BTreeMap::new();
        completed_payload.insert("rotation_event".to_string(), "completed".to_string());
        completed_payload.insert("checkpoint_id".to_string(), outcome.checkpoint_id.clone());
        completed_payload.insert("trigger".to_string(), "cli_rotation".to_string());
        // Keep on one line: multi-line call sites can produce llvm-cov line-mapping gaps.
        #[rustfmt::skip]
        self.append_run_event(run, EventKind::RunNote, EventSource::System, completed_payload, now_epoch_ms)?;

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
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            has_injected_bootstrap: false,
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

struct TurnExecutorRotationRuntime<'a> {
    run: &'a Run,
    logical_session: &'a mut LogicalSession,
    checkpoint_store: CheckpointStore,
    checkpoint_created_at_epoch_ms: u64,
}

impl RotationSequenceRuntime for TurnExecutorRotationRuntime<'_> {
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
        state.insert("trigger".to_string(), "cli_rotation".to_string());

        self.checkpoint_store.put_checkpoint(&Checkpoint {
            id: checkpoint_id.clone(),
            logical_session_id: self.run.logical_session_id.clone(),
            run_id: self.run.id.clone(),
            created_at_epoch_ms: self.checkpoint_created_at_epoch_ms,
            summary: checkpoint.summary.clone(),
            memory_digest: format!(
                "cli:{}:{}:{}",
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
        self.logical_session.active_physical_session_id = None;
        self.logical_session.has_injected_bootstrap = false;
        self.logical_session.token_accounting = TokenAccounting {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
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

#[cfg(test)]
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

    if let Some(usage) = parse_usage_set(
        payload,
        "run_usage_input_tokens",
        "run_usage_output_tokens",
        "run_usage_total_tokens",
        "run_usage_cache_read_input_tokens",
        "run_usage_cache_creation_input_tokens",
    )? {
        return Ok(Some(usage));
    }
    if let Some(usage) = parse_usage_set(
        payload,
        "usage_input_tokens",
        "usage_output_tokens",
        "usage_total_tokens",
        "usage_cache_read_input_tokens",
        "usage_cache_creation_input_tokens",
    )? {
        return Ok(Some(usage));
    }
    parse_usage_set(
        payload,
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
    )
}

fn parse_usage_set(
    payload: &BTreeMap<String, String>,
    input_key: &'static str,
    output_key: &'static str,
    total_key: &'static str,
    cache_read_key: &'static str,
    cache_creation_key: &'static str,
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

    let parse_optional = |key: &'static str| -> u64 {
        payload
            .get(key)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };

    let input_tokens = parse_required(input_key)?;
    let output_tokens = parse_required(output_key)?;
    let total_tokens = parse_required(total_key)?;
    let cache_read_input_tokens = parse_optional(cache_read_key);
    let cache_creation_input_tokens = parse_optional(cache_creation_key);

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
        cache_read_input_tokens,
        cache_creation_input_tokens,
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
    let cache_read_input_tokens = existing
        .cache_read_input_tokens
        .checked_add(increment.cache_read_input_tokens)
        .ok_or(CrabError::InvariantViolation {
            context: "turn_executor_usage_accounting",
            message: "cache_read_input token accounting overflow".to_string(),
        })?;
    let cache_creation_input_tokens = existing
        .cache_creation_input_tokens
        .checked_add(increment.cache_creation_input_tokens)
        .ok_or(CrabError::InvariantViolation {
            context: "turn_executor_usage_accounting",
            message: "cache_creation_input token accounting overflow".to_string(),
        })?;
    Ok(TokenAccounting {
        input_tokens,
        output_tokens,
        total_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
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
                self.flush_oversized_prefix(&prefix, planned);
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

    /// Flush a prefix that may exceed the Discord character limit.
    /// Splits at the last newline before the limit for cleaner breaks,
    /// falling back to a hard character split if no newline is found.
    fn flush_oversized_prefix(
        &mut self,
        prefix: &str,
        planned: &mut Vec<PlannedAssistantDelivery>,
    ) {
        let limit = crab_discord::DISCORD_MESSAGE_CHAR_LIMIT;
        let mut remaining = prefix.to_string();

        loop {
            if remaining.chars().count() <= limit {
                let flushed = self.flush_current_prefix(&remaining, planned);
                if flushed {
                    self.advance_chunk();
                }
                break;
            }

            // Find a newline to split at, preferring the last one before the limit.
            let byte_limit = remaining
                .char_indices()
                .nth(limit)
                .map(|(i, _)| i)
                .unwrap_or(remaining.len());
            let split_pos = remaining[..byte_limit].rfind('\n').unwrap_or(byte_limit);

            let (head, tail) = remaining.split_at(split_pos);
            let head = head.to_string();
            // Skip the newline character we split at (if we split at a newline).
            let tail = if let Some(stripped) = tail.strip_prefix('\n') {
                stripped.to_string()
            } else {
                tail.to_string()
            };

            let flushed = self.flush_current_prefix(&head, planned);
            if flushed {
                self.advance_chunk();
            }
            remaining = tail;
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

    fn notify_tool_boundary(&mut self) -> Vec<PlannedAssistantDelivery> {
        if self.active_content.trim().is_empty() {
            return Vec::new();
        }

        let trimmed = self.active_content.trim_end_matches('\n').to_string();
        if trimmed == self.active_last_delivered {
            self.advance_chunk();
            return Vec::new();
        }

        let planned = vec![PlannedAssistantDelivery {
            chunk_index: self.next_chunk_index,
            edit_generation: self.active_edit_generation,
            content: trimmed.clone(),
        }];
        self.active_last_delivered = trimmed;
        self.active_edit_generation = self.active_edit_generation.saturating_add(1);
        self.advance_chunk();
        planned
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

impl<R: TurnExecutorRuntime> OnboardingCompletionEventRuntime for TurnExecutor<R> {
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

fn attachment_directory(state_root: &Path, run_id: &str) -> PathBuf {
    state_root
        .join("attachments")
        .join(run_id.replace(['/', '\\', ':'], "_"))
}

fn sanitize_attachment_filename(filename: &str, index: usize) -> String {
    let trimmed = filename.trim();
    if trimmed.is_empty() {
        return format!("attachment_{index}");
    }
    trimmed.replace(['/', '\\', '\0'], "_")
}

fn cleanup_attachment_directory(state_root: &Path, run_id: &str) {
    let _ = std::fs::remove_dir_all(attachment_directory(state_root, run_id));
}

#[cfg(not(any(test, coverage)))]
fn download_attachment(url: &str, dest_path: &Path) -> Result<(), String> {
    let response = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .map_err(|e| e.to_string())?
        .get(url)
        .send()
        .map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    std::fs::write(dest_path, &response.bytes().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())
}

#[cfg(any(test, coverage))]
fn download_attachment(url: &str, _dest_path: &Path) -> Result<(), String> {
    if url.contains("FAIL_DOWNLOAD") {
        return Err("simulated download failure".to_string());
    }
    Ok(())
}

fn build_user_input_with_attachments(
    content: &str,
    attachments: &[GatewayAttachment],
    state_root: &Path,
    run_id: &str,
) -> String {
    if attachments.is_empty() {
        return content.to_string();
    }
    let dir = attachment_directory(state_root, run_id);
    let _ = std::fs::create_dir_all(&dir);

    let mut lines = Vec::new();
    for (i, att) in attachments.iter().enumerate() {
        let safe_name = sanitize_attachment_filename(&att.filename, i);
        let dest = dir.join(&safe_name);
        match download_attachment(&att.url, &dest) {
            Ok(()) => lines.push(format!(
                "[attached file: {} (saved to {})]",
                att.filename,
                dest.to_string_lossy()
            )),
            Err(e) => lines.push(format!(
                "[attached file: {} (download failed: {})]",
                att.filename, e
            )),
        }
    }
    let annotation = lines.join("\n");
    if content.trim().is_empty() {
        format!("{annotation}\n(no text message provided)")
    } else {
        format!("{annotation}\n{content}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use crab_backends::BackendEventKind;
    use crab_core::{
        BackendKind, CrabError, CrabResult, EventKind, InferenceProfile, LaneState,
        OwnerProfileMetadata, ProfileValueSource, ReasoningLevel, RunProfileTelemetry, RunStatus,
    };
    use crab_discord::{GatewayAttachment, GatewayConversationKind, GatewayMessage};
    use futures::stream;

    use super::{
        attachment_directory, build_user_input_with_attachments, cleanup_attachment_directory,
        sanitize_attachment_filename, DispatchedTurn, QueuedTurn, TurnExecutor,
        TurnExecutorRuntime,
    };
    use crate::composition::compose_runtime_with_queue_limit;
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};

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
        gateway_messages: VecDeque<Option<GatewayMessage>>,
        interrupt_calls: Vec<String>,
        event_stream_delay: Option<Duration>,
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
                gateway_messages: VecDeque::new(),
                interrupt_calls: Vec::new(),
                event_stream_delay: None,
            }
        }

        fn with_gateway_messages(mut self, messages: Vec<Option<GatewayMessage>>) -> Self {
            self.gateway_messages = VecDeque::from(messages);
            self
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
            if let Some(delay) = self.event_stream_delay.take() {
                let (tx, rx) = futures::channel::mpsc::unbounded();
                std::thread::spawn(move || {
                    std::thread::sleep(delay);
                    for event in events {
                        let _ = tx.unbounded_send(event);
                    }
                });
                return Ok(Box::pin(rx));
            }
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

        fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
            match self.gateway_messages.pop_front() {
                Some(value) => Ok(value),
                None => Ok(None),
            }
        }

        fn interrupt_backend_turn(
            &mut self,
            _session: &crab_core::PhysicalSession,
            turn_id: &str,
        ) -> CrabResult<()> {
            self.interrupt_calls.push(turn_id.to_string());
            Ok(())
        }
    }

    fn sample_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: InferenceProfile {
                backend: BackendKind::Claude,
                model: "claude-sonnet-4-5".to_string(),
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
            machine_location: Some("Berlin, Germany".to_string()),
            machine_timezone: Some("Europe/Paris".to_string()),
            default_backend: Some(BackendKind::Claude),
            default_model: Some("claude-sonnet-4-5".to_string()),
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
            attachments: vec![],
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
            attachments: vec![],
        }
    }

    fn gateway_owner_dm_message_with_content(message_id: &str, content: &str) -> GatewayMessage {
        gateway_dm_message_with_content(message_id, "424242424242424242", content)
    }

    fn onboarding_capture_payload_json() -> &'static str {
        r#"{
  "schema_version": "v1",
  "agent_identity": "Crab",
  "owner_identity": "Alice",
  "primary_goals": ["Ship reliable automation", "Keep strict quality gates"],
  "machine_location": "Berlin, Germany",
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
            active_backend: BackendKind::Claude,
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
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            has_injected_bootstrap: false,
        }
    }

    fn physical_session_fixture() -> crab_core::PhysicalSession {
        crab_core::PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            backend: BackendKind::Claude,
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

    fn build_executor_with_config(
        runtime: FakeRuntime,
        lane_queue_limit: usize,
        config: crab_core::RuntimeConfig,
    ) -> TurnExecutor<FakeRuntime> {
        let bootstrap_path = Path::new(config.workspace_root.as_str()).join("BOOTSTRAP.md");
        let composition =
            compose_runtime_with_queue_limit(&config, "999999999999999999", lane_queue_limit)
                .expect("composition should build");
        let _ = std::fs::remove_file(&bootstrap_path);
        TurnExecutor::new(composition, runtime)
    }

    fn build_executor(
        workspace: &TempWorkspace,
        runtime: FakeRuntime,
        lane_queue_limit: usize,
    ) -> TurnExecutor<FakeRuntime> {
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        build_executor_with_config(runtime, lane_queue_limit, config)
    }

    fn build_executor_scenario(
        label: &str,
        runtime: FakeRuntime,
        lane_queue_limit: usize,
    ) -> (TempWorkspace, TurnExecutor<FakeRuntime>) {
        let workspace = TempWorkspace::new("turn-executor", label);
        let executor = build_executor(&workspace, runtime, lane_queue_limit);
        (workspace, executor)
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
                emitted_event_count: 8,
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
        assert_eq!(events.len(), 9);
        assert_eq!(events[0].kind, EventKind::RunState);
        assert_eq!(events[0].payload.get("state"), Some(&"queued".to_string()));
        assert_eq!(events[1].kind, EventKind::RunState);
        assert_eq!(events[1].payload.get("state"), Some(&"running".to_string()));
        assert_eq!(events[2].kind, EventKind::TextDelta);
        assert_eq!(events[3].kind, EventKind::ToolCall);
        assert_eq!(events[4].kind, EventKind::ToolResult);
        assert_eq!(events[5].kind, EventKind::RunNote);
        // Event 6: TurnCompleted maps to RunState(succeeded)
        assert_eq!(events[6].kind, EventKind::RunState);
        assert_eq!(
            events[6].payload.get("state"),
            Some(&"succeeded".to_string())
        );
        // Event 7: missing_usage_data diagnostic (no usage triplet in backend events)
        assert_eq!(events[7].kind, EventKind::RunNote);
        assert_eq!(
            events[7].payload.get("event"),
            Some(&"missing_usage_data".to_string())
        );
        assert_eq!(events[8].kind, EventKind::RunState);
        assert_eq!(
            events[8].payload.get("state"),
            Some(&"succeeded".to_string())
        );
        for event in &events {
            assert_eq!(
                event.turn_id,
                Some("turn:run:discord:channel:777:m-1".to_string())
            );
            assert_eq!(event.lane_id, Some("discord:channel:777".to_string()));
            assert_eq!(event.backend, Some(BackendKind::Claude));
            assert_eq!(event.resolved_model, Some("claude-sonnet-4-5".to_string()));
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
    fn tool_boundary_splits_delivery_in_streaming_loop() {
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "before\n")]),
                backend_event(2, BackendEventKind::ToolCall, &[("tool", "shell")]),
                backend_event(3, BackendEventKind::ToolResult, &[("status", "ok")]),
                backend_event(4, BackendEventKind::TextDelta, &[("text", "after")]),
                backend_event(5, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
        );
        let (_workspace, mut executor) = build_executor_scenario("tool-boundary-split", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-tb"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Succeeded);

        let delivered = &executor.runtime_mut().delivered_outputs;
        // First delivery: "before\n" via push_delta (chunk:0)
        // Second delivery: tool boundary trims trailing \n → delivers "before" on chunk:0
        // Third delivery: "after" via push_delta (chunk:1)
        assert!(delivered.len() >= 2);
        let last = delivered.last().unwrap();
        assert_eq!(last.4, "after");
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
        assert_eq!(
            session.active_physical_session_id,
            Some("physical-1".to_string())
        );

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
                backend: BackendKind::Claude,
                backend_session_id: "thread-abc".to_string(),
                created_at_epoch_ms: 1_739_173_200_000,
                last_turn_id: None,
            }),
            Ok(crab_core::PhysicalSession {
                id: "physical-1".to_string(),
                logical_session_id: "discord:channel:777".to_string(),
                backend: BackendKind::Claude,
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
        assert_eq!(session.token_accounting.cache_read_input_tokens, 0);
        assert_eq!(session.token_accounting.cache_creation_input_tokens, 0);
        assert_eq!(session.token_accounting.context_window_tokens(), 10);
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
                active_backend: BackendKind::Claude,
                active_profile: InferenceProfile {
                    backend: BackendKind::Claude,
                    model: "claude-sonnet-4-5".to_string(),
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
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                },
                has_injected_bootstrap: false,
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
            Some("Berlin, Germany".to_string())
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

        // Backward timestamps are now clamped instead of rejected, so
        // dispatch succeeds. The store silently fixes clock jitter.
        let result = executor.dispatch_next_run();
        assert!(
            result.is_ok(),
            "backward start timestamp should be clamped, not rejected"
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

        // Backward timestamps are now clamped instead of rejected, so
        // dispatch succeeds. The store silently fixes clock jitter.
        let result = executor.dispatch_next_run();
        assert!(
            result.is_ok(),
            "backward completion timestamp should be clamped, not rejected"
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

        // Backward timestamps are now clamped instead of rejected, so
        // mark_run_failed succeeds. The store silently fixes clock jitter.
        let result = executor.mark_run_failed(
            "discord:channel:777",
            run_id,
            &CrabError::InvariantViolation {
                context: "test",
                message: "force failure".to_string(),
            },
        );
        assert!(
            result.is_ok(),
            "backward completion timestamp should be clamped, not rejected"
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
    fn assistant_delivery_splits_oversized_section_between_blank_lines() {
        // A blank-line-delimited section that exceeds the Discord limit should
        // be split into multiple chunks.
        let limit = crab_discord::DISCORD_MESSAGE_CHAR_LIMIT;
        let mut delivery = super::DiscordAssistantDelivery::new();

        // Build: "short\n\n<oversized>\n\ntrailing"
        let oversized = "x".repeat(limit + 500);
        let input = format!("short\n\n{}\n\ntrailing", oversized);
        let planned = delivery.push_delta(&input);

        // Should produce at least 4 chunks: "short", split oversized (2 parts), "trailing"
        assert!(planned.len() >= 4);

        // First chunk is the short section.
        assert_eq!(planned[0].content, "short");
        assert_eq!(planned[0].chunk_index, 0);

        // The oversized section should be split -- no chunk should exceed the limit.
        for p in &planned {
            assert!(p.content.chars().count() <= limit);
        }

        // Last chunk is "trailing" (still streaming, so it's the active content).
        assert_eq!(planned.last().unwrap().content, "trailing");
    }

    #[test]
    fn assistant_delivery_oversized_section_splits_at_newline() {
        // When splitting an oversized section, prefer splitting at a newline.
        let limit = crab_discord::DISCORD_MESSAGE_CHAR_LIMIT;
        let mut delivery = super::DiscordAssistantDelivery::new();

        // Build a section with a newline near the limit boundary.
        let before_newline = "y".repeat(limit - 100);
        let after_newline = "z".repeat(200);
        let section = format!("{}\n{}", before_newline, after_newline);
        let input = format!("{}\n\ndone", section);

        let planned = delivery.push_delta(&input);

        // First chunk should be the part before the newline (split at \n).
        assert_eq!(planned[0].content, before_newline);
        assert_eq!(planned[1].content, after_newline);
        assert_eq!(planned.last().unwrap().content, "done");
    }

    #[test]
    fn assistant_delivery_splits_on_tool_boundary() {
        let mut delivery = super::DiscordAssistantDelivery::new();

        // Push pre-tool text.
        let planned = delivery.push_delta("before tool");
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].chunk_index, 0);

        // Tool boundary finalizes the chunk.
        let boundary_ops = delivery.notify_tool_boundary();
        assert_eq!(boundary_ops.len(), 0); // already delivered identical content
        assert_eq!(delivery.next_chunk_index, 1);

        // Post-tool text starts a new chunk.
        let post_ops = delivery.push_delta("after tool");
        assert_eq!(post_ops.len(), 1);
        assert_eq!(post_ops[0].chunk_index, 1);
    }

    #[test]
    fn assistant_delivery_tool_boundary_with_empty_buffer_is_noop() {
        let mut delivery = super::DiscordAssistantDelivery::new();

        let ops = delivery.notify_tool_boundary();
        assert!(ops.is_empty());
        assert_eq!(delivery.next_chunk_index, 0);

        // Whitespace-only buffer is also a no-op.
        delivery.push_delta("   ");
        let ops = delivery.notify_tool_boundary();
        assert!(ops.is_empty());
        assert_eq!(delivery.next_chunk_index, 0);
    }

    #[test]
    fn assistant_delivery_tool_boundary_emits_trimmed_content_when_trailing_newlines() {
        let mut delivery = super::DiscordAssistantDelivery::new();

        // Push text ending with a single newline (not \n\n which would trigger section split).
        // push_delta delivers "hello\n" but notify_tool_boundary trims trailing newlines.
        let _ = delivery.push_delta("hello\n");

        let boundary_ops = delivery.notify_tool_boundary();
        assert_eq!(boundary_ops.len(), 1);
        assert_eq!(boundary_ops[0].chunk_index, 0);
        assert_eq!(boundary_ops[0].content, "hello");

        // After boundary, new content goes to chunk 1.
        let post_ops = delivery.push_delta("new chunk");
        assert_eq!(post_ops.len(), 1);
        assert_eq!(post_ops[0].chunk_index, 1);
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
    fn replay_delivery_for_run_splits_on_tool_boundary() {
        // Initial run: TextDelta("pre\n") + ToolCall. The first push_delta delivery succeeds
        // but the tool boundary delivery (trimmed "pre") fails. Events are still stored.
        let workspace = TempWorkspace::new("turn-executor", "replay-delivery-tool-boundary");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "pre\n")]),
                backend_event(2, BackendEventKind::ToolCall, &[("tool", "shell")]),
            ],
            &[1, 2, 3, 4, 5],
        )
        .with_delivery_results(vec![
            Ok(()),
            Err(CrabError::InvariantViolation {
                context: "deliver",
                message: "tool-boundary delivery failed".to_string(),
            }),
        ]);
        let mut executor = build_executor(&workspace, runtime.clone(), 8);
        let error = executor
            .process_gateway_message(gateway_message("m-replay-tb"))
            .expect_err("tool boundary delivery failure should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "deliver",
                message: "tool-boundary delivery failed".to_string(),
            }
        );

        // Replay: outbound store has the push_delta delivery (chunk:0, gen:0, "pre\n") but
        // NOT the tool boundary delivery (chunk:0, gen:1, "pre"). Replay delivers the missing one.
        runtime.deliver_results = VecDeque::new();
        runtime.delivered_outputs.clear();
        let mut replay_executor = build_executor(&workspace, runtime, 8);
        let delivered = replay_executor
            .replay_delivery_for_run("discord:channel:777", "run:discord:channel:777:m-replay-tb")
            .expect("replay should succeed");
        // The tool boundary delivery (chunk:0, gen:1) was missing → replay delivers it.
        assert!(delivered >= 1);
        let replay_delivered = &replay_executor.runtime_mut().delivered_outputs;
        assert!(!replay_delivered.is_empty());
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
        assert_eq!(usage.cache_read_input_tokens, 0);
        assert_eq!(usage.cache_creation_input_tokens, 0);

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
                cache_read_input_tokens: 100,
                cache_creation_input_tokens: 50,
            },
            crab_core::TokenAccounting {
                input_tokens: 4,
                output_tokens: 5,
                total_tokens: 9,
                cache_read_input_tokens: 200,
                cache_creation_input_tokens: 75,
            },
        )
        .expect("merge should succeed");
        assert_eq!(merged.input_tokens, 5);
        assert_eq!(merged.output_tokens, 7);
        assert_eq!(merged.total_tokens, 12);
        assert_eq!(merged.cache_read_input_tokens, 300);
        assert_eq!(merged.cache_creation_input_tokens, 125);

        let overflow = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: u64::MAX,
                output_tokens: 0,
                total_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            crab_core::TokenAccounting {
                input_tokens: 1,
                output_tokens: 0,
                total_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
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
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 1,
                total_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
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
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 1,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
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

        let cache_read_overflow = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                cache_read_input_tokens: u64::MAX,
                cache_creation_input_tokens: 0,
            },
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                cache_read_input_tokens: 1,
                cache_creation_input_tokens: 0,
            },
        )
        .expect_err("cache_read merge overflow should fail");
        assert_eq!(
            cache_read_overflow,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "cache_read_input token accounting overflow".to_string(),
            }
        );

        let cache_creation_overflow = super::merge_token_accounting(
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: u64::MAX,
            },
            crab_core::TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 1,
            },
        )
        .expect_err("cache_creation merge overflow should fail");
        assert_eq!(
            cache_creation_overflow,
            CrabError::InvariantViolation {
                context: "turn_executor_usage_accounting",
                message: "cache_creation_input token accounting overflow".to_string(),
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
            backend: Some(BackendKind::Claude),
            resolved_model: Some("claude-sonnet-4-5".to_string()),
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

    #[test]
    fn steering_message_for_same_lane_interrupts_run_and_marks_cancelled() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "partial")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let steering_msg = gateway_message("m-steering");
        // now_epoch_ms calls: enqueue(1), started(2), event1(3), enqueue_steering(4), steered(5),
        // completed(6)
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8])
                .with_gateway_messages(vec![Some(steering_msg)]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (_workspace, mut executor) = build_executor_scenario("steering-same-lane", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(dispatch.status, RunStatus::Cancelled);
        assert_eq!(
            dispatch.logical_session_id,
            "discord:channel:777".to_string()
        );

        let stored_run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(stored_run.status, RunStatus::Cancelled);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("event replay should succeed");
        let steered_notes: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("event") == Some(&"run_steered".to_string())
            })
            .collect();
        assert_eq!(steered_notes.len(), 1, "should emit a run_steered note");

        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(
            session.active_physical_session_id,
            Some("physical-1".to_string()),
            "physical session should be preserved after steering"
        );

        let runtime = executor.runtime_mut();
        assert_eq!(
            runtime.interrupt_calls,
            vec!["turn:run:discord:channel:777:m-1".to_string()],
            "interrupt_backend_turn should be called once"
        );
    }

    #[test]
    fn steering_message_for_different_lane_does_not_interrupt_current_run() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let mut cross_lane_msg = gateway_message("m-cross");
        cross_lane_msg.channel_id = "888".to_string();
        // now_epoch_ms calls: enqueue(1), started(2), event1(3), enqueue_cross(4), event2(5),
        // completed(6)
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8])
                .with_gateway_messages(vec![Some(cross_lane_msg)]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (_workspace, mut executor) = build_executor_scenario("steering-cross-lane", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "cross-lane message should not interrupt the run"
        );

        let runtime = executor.runtime_mut();
        assert!(
            runtime.interrupt_calls.is_empty(),
            "interrupt_backend_turn should not be called for cross-lane messages"
        );
    }

    #[test]
    fn steering_during_timeout_interrupts_run_via_delayed_stream() {
        let backend_events = vec![backend_event(
            1,
            BackendEventKind::TextDelta,
            &[("text", "delayed")],
        )];
        let steering_msg = gateway_message("m-steer-timeout");
        // Events are delayed 300ms; steering poll is 200ms, so the first loop iteration
        // hits the Timeout arm. The steering message is found and the run is interrupted.
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8])
                .with_gateway_messages(vec![Some(steering_msg)]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        runtime.event_stream_delay = Some(Duration::from_millis(300));
        let (_workspace, mut executor) = build_executor_scenario("steering-timeout", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(dispatch.status, RunStatus::Cancelled);

        let runtime = executor.runtime_mut();
        assert_eq!(runtime.interrupt_calls.len(), 1);
    }

    #[test]
    fn steering_bot_message_is_ignored_and_does_not_interrupt() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "output")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let mut bot_msg = gateway_message("m-bot");
        bot_msg.author_is_bot = true;
        let runtime = FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8])
            .with_gateway_messages(vec![Some(bot_msg)]);
        let (_workspace, mut executor) = build_executor_scenario("steering-bot-msg", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "bot messages should not trigger steering"
        );
    }

    #[test]
    fn check_for_steering_trigger_steers_run_when_file_present_for_same_lane() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "partial")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        // now_epoch_ms calls: enqueue(1), started(2), event1(3), enqueue_trigger(4), steered(5),
        // completed(6)
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (workspace, mut executor) =
            build_executor_scenario("steering-trigger-same-lane", runtime, 8);

        // Write a steering trigger file to disk for channel 777 (same as the gateway message)
        let state_root = state_root(&workspace);
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        let trigger_path = crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "steer me".to_string(),
            },
        )
        .expect("steering trigger write should succeed");
        assert!(trigger_path.exists());

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(dispatch.status, RunStatus::Cancelled);

        // The steering trigger file should have been consumed
        assert!(
            !trigger_path.exists(),
            "steering trigger file should be consumed after processing"
        );

        let runtime = executor.runtime_mut();
        assert_eq!(
            runtime.interrupt_calls.len(),
            1,
            "interrupt_backend_turn should be called once for steering trigger"
        );
    }

    #[test]
    fn check_for_steering_trigger_enqueue_error_does_not_abort_run() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "output")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let runtime = FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8]);
        let (workspace, mut executor) =
            build_executor_scenario("steering-trigger-bad-channel", runtime, 8);

        // Write a steering trigger with a blank channel_id directly to disk
        // (bypassing validation) to exercise the enqueue error branch.
        let state_root = state_root(&workspace);
        let triggers_dir = state_root.join("steering_triggers");
        fs::create_dir_all(&triggers_dir).expect("triggers dir should be creatable");
        fs::write(
            triggers_dir.join("bad_trigger.json"),
            r#"{"channel_id":"  ","message":"bad trigger"}"#,
        )
        .expect("bad trigger file should be writable");

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "run should succeed even when steering trigger enqueue fails"
        );
    }

    #[test]
    fn check_for_steering_trigger_different_lane_does_not_steer_current_run() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "output")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        // now_epoch_ms calls: enqueue(1), started(2), event1(3), enqueue_trigger(4),
        // completed(5)
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (workspace, mut executor) =
            build_executor_scenario("steering-trigger-different-lane", runtime, 8);

        // Write a steering trigger for channel 999 -- different from channel 777
        // used by gateway_message, so logical_session_id will NOT match.
        let state_root = state_root(&workspace);
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        let trigger_path = crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "999".to_string(),
                message: "steer different lane".to_string(),
            },
        )
        .expect("steering trigger write should succeed");
        assert!(trigger_path.exists());

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        // The trigger is for a different lane, so the current run should NOT be steered.
        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "run should succeed when steering trigger targets a different lane"
        );

        // The steering trigger file should still be consumed (removed from disk).
        assert!(
            !trigger_path.exists(),
            "steering trigger file should be consumed even for a different lane"
        );

        // No interrupt should have been issued since the trigger is for another lane.
        let runtime = executor.runtime_mut();
        assert_eq!(
            runtime.interrupt_calls.len(),
            0,
            "interrupt_backend_turn should not be called for different-lane trigger"
        );
    }

    #[test]
    fn check_for_steering_message_returns_false_when_no_message_available() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "output")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let runtime = FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8]);
        let (_workspace, mut executor) = build_executor_scenario("steering-no-msg", runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(dispatch.status, RunStatus::Succeeded);
    }

    // ── consume_and_batch_triggers: multi-trigger batching tests ──────

    #[test]
    fn batch_triggers_combines_same_channel_messages_with_delimiter() {
        let workspace = TempWorkspace::new("turn-executor", "batch-same-channel");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let state_root = state_root(&workspace);
        fs::create_dir_all(&state_root).expect("state root");

        let p1 = crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "first message".to_string(),
            },
        )
        .expect("write trigger 1");
        let p2 = crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "second message".to_string(),
            },
        )
        .expect("write trigger 2");

        let triggers = crab_core::read_steering_triggers(&state_root)
            .expect("read triggers");
        assert_eq!(triggers.len(), 2, "should have 2 trigger files");

        let (matched, consumed) = executor
            .consume_and_batch_triggers(
                triggers,
                "discord:channel:777",
                crab_core::consume_steering_trigger,
                "steering",
            )
            .expect("batch should succeed");

        assert!(matched, "should match current lane");
        assert_eq!(consumed, 2, "both triggers should be consumed");
        assert!(!p1.exists(), "trigger 1 should be deleted");
        assert!(!p2.exists(), "trigger 2 should be deleted");

        // Verify the queued run contains combined messages with delimiter.
        let run_ids = executor
            .composition()
            .state_stores
            .run_store
            .list_run_ids("discord:channel:777")
            .expect("list run ids should succeed");
        assert_eq!(run_ids.len(), 1, "should have exactly 1 batched run");
        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", &run_ids[0])
            .expect("run read should succeed")
            .expect("run should exist");
        assert_eq!(
            run.user_input, "first message\n---\nsecond message",
            "messages should be joined with delimiter in chronological order"
        );
    }

    #[test]
    fn batch_triggers_creates_separate_runs_for_different_channels() {
        let workspace = TempWorkspace::new("turn-executor", "batch-cross-channel");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4, 5, 6, 7, 8]);
        // Two enqueue calls need two profile resolutions (default has one).
        runtime.resolve_profile_results.push_back(Ok(sample_profile_telemetry()));
        let mut executor = build_executor(&workspace, runtime, 8);

        let state_root = state_root(&workspace);
        fs::create_dir_all(&state_root).expect("state root");

        crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "msg for 777".to_string(),
            },
        )
        .expect("write trigger for 777");
        crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "999".to_string(),
                message: "msg for 999".to_string(),
            },
        )
        .expect("write trigger for 999");

        let triggers = crab_core::read_steering_triggers(&state_root)
            .expect("read triggers");
        assert_eq!(triggers.len(), 2);

        let (matched, consumed) = executor
            .consume_and_batch_triggers(
                triggers,
                "discord:channel:777",
                crab_core::consume_steering_trigger,
                "steering",
            )
            .expect("batch should succeed");

        assert!(matched, "should match current lane (777)");
        assert_eq!(consumed, 2, "both triggers consumed");

        // Two separate runs should exist, one per channel.
        assert_eq!(
            executor.composition().scheduler.queued_count("discord:channel:777").unwrap(),
            1,
            "channel 777 should have 1 queued run"
        );
        assert_eq!(
            executor.composition().scheduler.queued_count("discord:channel:999").unwrap(),
            1,
            "channel 999 should have 1 queued run"
        );
    }

    #[test]
    fn batch_triggers_single_trigger_not_delimited() {
        let workspace = TempWorkspace::new("turn-executor", "batch-single");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let state_root = state_root(&workspace);
        fs::create_dir_all(&state_root).expect("state root");

        crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "solo message".to_string(),
            },
        )
        .expect("write trigger");

        let triggers = crab_core::read_steering_triggers(&state_root)
            .expect("read triggers");

        let (_, consumed) = executor
            .consume_and_batch_triggers(
                triggers,
                "discord:channel:777",
                crab_core::consume_steering_trigger,
                "steering",
            )
            .expect("batch should succeed");

        assert_eq!(consumed, 1);

        let run_ids = executor
            .composition()
            .state_stores
            .run_store
            .list_run_ids("discord:channel:777")
            .expect("list run ids");
        assert_eq!(run_ids.len(), 1);
        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", &run_ids[0])
            .expect("run read")
            .expect("run should exist");
        assert_eq!(
            run.user_input, "solo message",
            "single trigger should not have delimiter"
        );
    }

    #[test]
    fn batch_triggers_empty_list_returns_false() {
        let workspace = TempWorkspace::new("turn-executor", "batch-empty");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let (matched, consumed) = executor
            .consume_and_batch_triggers(
                Vec::new(),
                "discord:channel:777",
                crab_core::consume_steering_trigger,
                "steering",
            )
            .expect("empty batch should succeed");

        assert!(!matched);
        assert_eq!(consumed, 0);
    }

    #[test]
    fn batch_triggers_enqueue_failure_retains_files() {
        let workspace = TempWorkspace::new("turn-executor", "batch-enqueue-fail");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let state_root = state_root(&workspace);
        fs::create_dir_all(&state_root).expect("state root");

        // Write a trigger with blank channel_id directly to bypass validation
        let triggers_dir = state_root.join("steering_triggers");
        fs::create_dir_all(&triggers_dir).expect("dir");
        let bad_path = triggers_dir.join("bad.json");
        fs::write(&bad_path, r#"{"channel_id":"  ","message":"bad"}"#)
            .expect("write bad trigger");

        let triggers = crab_core::read_steering_triggers(&state_root)
            .expect("read triggers");

        let (matched, consumed) = executor
            .consume_and_batch_triggers(
                triggers,
                "",
                crab_core::consume_steering_trigger,
                "steering",
            )
            .expect("batch should succeed even with enqueue failure");

        assert!(!matched);
        assert_eq!(consumed, 0, "failed enqueue should not count as consumed");
        assert!(bad_path.exists(), "file should be retained on enqueue failure");
    }

    // ── Bug 3: Bootstrap re-injection flag survives daemon restart ──────

    #[test]
    fn bootstrap_injection_flag_persists_across_simulated_daemon_restart() {
        // First turn: bootstrap should be injected and flag set.
        // Simulate daemon restart: create a new executor from the same workspace
        // (session is persisted on disk). Second turn should NOT re-inject bootstrap.
        let workspace = TempWorkspace::new("turn-executor", "bootstrap-flag-persist");
        let mut runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[1, 2, 3, 4, 5],
        );
        runtime.build_context_results = VecDeque::from(vec![Ok("bootstrap context".to_string())]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-bootstrap-1"))
            .expect("first run should succeed")
            .expect("first run should dispatch");

        assert_eq!(
            executor.runtime_mut().build_context_bootstrap_flags,
            vec![true],
            "first turn should inject bootstrap"
        );

        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert!(
            session.has_injected_bootstrap,
            "has_injected_bootstrap should be true after first turn"
        );

        // Simulate daemon restart: create a new executor using the same workspace state.
        let mut runtime2 = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "world")]),
                backend_event(
                    2,
                    BackendEventKind::TurnCompleted,
                    &[("stop_reason", "done")],
                ),
            ],
            &[10, 11, 12, 13, 14],
        );
        runtime2.ensure_session_results = VecDeque::from(vec![Ok(crab_core::PhysicalSession {
            last_turn_id: Some("turn-previous".to_string()),
            ..physical_session_fixture()
        })]);
        let mut executor2 = build_executor(&workspace, runtime2, 8);

        executor2
            .process_gateway_message(gateway_message("m-bootstrap-2"))
            .expect("second run should succeed")
            .expect("second run should dispatch");

        assert_eq!(
            executor2.runtime_mut().build_context_bootstrap_flags,
            vec![false],
            "second turn after restart should NOT re-inject bootstrap"
        );
    }

    // ── Bug 1: Missing usage data diagnostic ───────────────────────────

    #[test]
    fn missing_usage_data_emits_diagnostic_run_note() {
        // A successful turn with no usage event in backend_events should emit
        // a missing_usage_data RunNote.
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "output")]),
            backend_event(
                2,
                BackendEventKind::TurnCompleted,
                &[("stop_reason", "done")],
            ),
        ];
        let runtime = FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5]);
        let (_workspace, mut executor) = build_executor_scenario("missing-usage-diag", runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-no-usage"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-no-usage";
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        assert!(
            events.iter().any(|event| {
                event.kind == EventKind::RunNote
                    && event
                        .payload
                        .get("event")
                        .is_some_and(|v| v == "missing_usage_data")
            }),
            "missing usage data diagnostic should be emitted"
        );
    }

    #[test]
    fn no_missing_usage_diagnostic_when_usage_is_present() {
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "output")]),
            backend_event(
                2,
                BackendEventKind::RunNote,
                &[
                    ("usage_input_tokens", "100"),
                    ("usage_output_tokens", "50"),
                    ("usage_total_tokens", "150"),
                ],
            ),
            backend_event(
                3,
                BackendEventKind::TurnCompleted,
                &[("stop_reason", "done")],
            ),
        ];
        let runtime = FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6]);
        let (_workspace, mut executor) = build_executor_scenario("usage-present", runtime, 8);

        executor
            .process_gateway_message(gateway_message("m-with-usage"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-with-usage";
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        let missing_usage_count = events
            .iter()
            .filter(|e| e.kind == EventKind::RunNote)
            .filter(|e| e.payload.get("event") == Some(&"missing_usage_data".to_string()))
            .count();
        assert_eq!(
            missing_usage_count, 0,
            "no missing usage diagnostic should be emitted when usage is present"
        );
    }

    #[test]
    fn no_missing_usage_diagnostic_when_run_fails() {
        // Failed runs should not emit the diagnostic.
        let backend_events = vec![backend_event(
            1,
            BackendEventKind::Error,
            &[("message", "backend error")],
        )];
        let runtime = FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4]);
        let (_workspace, mut executor) =
            build_executor_scenario("no-usage-diag-on-failure", runtime, 8);

        let result = executor
            .process_gateway_message(gateway_message("m-fail-usage"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(result.status, RunStatus::Failed);
        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-fail-usage";
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");
        let missing_usage_count = events
            .iter()
            .filter(|e| {
                let is_run_note = e.kind == EventKind::RunNote;
                let has_missing_key =
                    e.payload.get("event") == Some(&"missing_usage_data".to_string());
                is_run_note && has_missing_key
            })
            .count();
        assert_eq!(
            missing_usage_count, 0,
            "no missing usage diagnostic should be emitted on failed runs"
        );
    }

    #[test]
    fn enqueue_pending_trigger_creates_queued_run_with_correct_session() {
        let workspace = TempWorkspace::new("turn-executor", "pending-trigger");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[42_000, 42_001]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let queued = executor
            .enqueue_pending_trigger("777", "Check deployment status")
            .expect("enqueue pending trigger should succeed");

        assert_eq!(queued.logical_session_id, "discord:channel:777");
        assert!(queued.run_id.contains("trigger:42000"));
        assert_eq!(queued.author_id, "0");
        assert_eq!(
            queued.routing_key,
            crab_discord::RoutingKey::Channel {
                channel_id: "777".to_string(),
            }
        );
        assert_eq!(queued.queued_run_count, 1);

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", &queued.run_id)
            .expect("run read should succeed")
            .expect("run should exist");
        assert_eq!(run.user_input, "Check deployment status");
        assert_eq!(run.status, RunStatus::Queued);
    }

    #[test]
    fn enqueue_pending_trigger_rejects_blank_channel_id() {
        let workspace = TempWorkspace::new("turn-executor", "trigger-blank-channel");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[42_000]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .enqueue_pending_trigger("  ", "hello")
            .expect_err("blank channel_id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "pending_trigger_enqueue",
                message: "channel_id must not be empty".to_string(),
            }
        );
    }

    fn temp_state_root(label: &str) -> PathBuf {
        let workspace = TempWorkspace::new("turn-executor", label);
        let root = workspace.path.join("state");
        fs::create_dir_all(&root).expect("state root should be creatable");
        std::mem::forget(workspace); // keep on disk for test lifetime
        root
    }

    #[test]
    fn build_user_input_with_attachments_returns_content_unchanged_without_attachments() {
        let root = temp_state_root("attach-empty");
        let result = build_user_input_with_attachments("hello world", &[], &root, "run-1");
        assert_eq!(result, "hello world");
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn build_user_input_with_attachments_prepends_annotation() {
        let root = temp_state_root("attach-single");
        let attachments = vec![GatewayAttachment {
            url: "https://cdn.example.com/photo.png".to_string(),
            filename: "photo.png".to_string(),
            size: 1024,
            content_type: Some("image/png".to_string()),
        }];
        let result =
            build_user_input_with_attachments("describe this", &attachments, &root, "run-2");
        assert!(result.starts_with("[attached file: photo.png (saved to "));
        assert!(result.ends_with("describe this"));
        assert!(result.contains("photo.png"));
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn build_user_input_with_attachments_handles_empty_content() {
        let root = temp_state_root("attach-no-text");
        let attachments = vec![GatewayAttachment {
            url: "https://cdn.example.com/data.bin".to_string(),
            filename: "data.bin".to_string(),
            size: 512,
            content_type: None,
        }];
        let result = build_user_input_with_attachments("", &attachments, &root, "run-3");
        assert!(result.contains("[attached file: data.bin (saved to "));
        assert!(result.ends_with("(no text message provided)"));
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn build_user_input_with_attachments_handles_multiple_attachments() {
        let root = temp_state_root("attach-multi");
        let attachments = vec![
            GatewayAttachment {
                url: "https://cdn.example.com/a.png".to_string(),
                filename: "a.png".to_string(),
                size: 100,
                content_type: None,
            },
            GatewayAttachment {
                url: "https://cdn.example.com/b.txt".to_string(),
                filename: "b.txt".to_string(),
                size: 200,
                content_type: None,
            },
        ];
        let result = build_user_input_with_attachments("two files", &attachments, &root, "run-4");
        assert!(result.contains("[attached file: a.png"));
        assert!(result.contains("[attached file: b.txt"));
        assert!(result.ends_with("two files"));
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn sanitize_attachment_filename_replaces_traversal_chars() {
        assert_eq!(
            sanitize_attachment_filename("../../../etc/passwd", 0),
            ".._.._.._etc_passwd"
        );
        assert_eq!(
            sanitize_attachment_filename("file\\name\0bad", 1),
            "file_name_bad"
        );
    }

    #[test]
    fn sanitize_attachment_filename_handles_empty() {
        assert_eq!(sanitize_attachment_filename("", 0), "attachment_0");
        assert_eq!(sanitize_attachment_filename("   ", 5), "attachment_5");
    }

    #[test]
    fn cleanup_attachment_directory_silent_on_missing() {
        let root = temp_state_root("attach-cleanup-miss");
        cleanup_attachment_directory(&root, "nonexistent-run");
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn cleanup_attachment_directory_removes_existing() {
        let root = temp_state_root("attach-cleanup-hit");
        let dir = attachment_directory(&root, "run-cleanup");
        fs::create_dir_all(&dir).expect("dir create should succeed");
        fs::write(dir.join("test.txt"), "data").expect("file write should succeed");
        assert!(dir.exists());
        cleanup_attachment_directory(&root, "run-cleanup");
        assert!(!dir.exists());
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn process_gateway_message_with_attachments_annotates_user_input() {
        let (workspace, mut executor) = build_executor_scenario(
            "attach-integration",
            FakeRuntime::with_backend_events(vec![], &[100, 200, 300, 400, 500]),
            128,
        );
        let mut message = gateway_message("msg-attach-1");
        message.attachments = vec![GatewayAttachment {
            url: "https://cdn.example.com/screenshot.png".to_string(),
            filename: "screenshot.png".to_string(),
            size: 4096,
            content_type: Some("image/png".to_string()),
        }];

        let queued = executor
            .enqueue_gateway_message(message)
            .expect("enqueue should succeed")
            .expect("message should be queued");

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", &queued.run_id)
            .expect("run read should succeed")
            .expect("run should exist");
        assert!(
            run.user_input
                .contains("[attached file: screenshot.png (saved to "),
            "user_input should contain attachment annotation, got: {}",
            run.user_input
        );
        drop(workspace);
    }

    #[test]
    fn process_gateway_message_with_attachments_cleans_up_after_run() {
        let (workspace, mut executor) = build_executor_scenario(
            "attach-cleanup-integ",
            FakeRuntime::with_backend_events(
                vec![],
                &[
                    100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200,
                ],
            ),
            128,
        );
        let mut message = gateway_message("msg-attach-cleanup");
        message.attachments = vec![GatewayAttachment {
            url: "https://cdn.example.com/test.txt".to_string(),
            filename: "test.txt".to_string(),
            size: 64,
            content_type: Some("text/plain".to_string()),
        }];

        let queued = executor
            .enqueue_gateway_message(message)
            .expect("enqueue should succeed")
            .expect("message should be queued");

        let state_root = &executor.composition().state_stores.root;
        let att_dir = attachment_directory(state_root, &queued.run_id);
        assert!(
            att_dir.exists(),
            "attachment dir should exist after enqueue"
        );

        let _ = executor.dispatch_next_run();
        assert!(
            !att_dir.exists(),
            "attachment dir should be removed after dispatch"
        );
        drop(workspace);
    }

    #[test]
    fn build_user_input_with_attachments_includes_download_failure_annotation() {
        let root = temp_state_root("attach-fail-dl");
        let attachments = vec![GatewayAttachment {
            url: "https://cdn.example.com/FAIL_DOWNLOAD/broken.png".to_string(),
            filename: "broken.png".to_string(),
            size: 999,
            content_type: None,
        }];
        let result =
            build_user_input_with_attachments("check this", &attachments, &root, "run-fail");
        assert!(result.contains("[attached file: broken.png (download failed:"));
        assert!(result.ends_with("check this"));
        let _ = fs::remove_dir_all(root.parent().unwrap());
    }

    #[test]
    fn process_gateway_message_executes_cli_rotation_and_delivers_notification() {
        use crab_core::{
            write_pending_rotation, CheckpointTurnArtifact, CheckpointTurnDocument,
            PendingRotation, PENDING_ROTATIONS_DIR_NAME,
        };

        // Backend events: text + completion, simple success.
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "reply")]),
                backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            // 8 now_epoch_ms calls:
            // 1=queued_at, 2=started_at, 3=emitted(event1), 4=emitted(event2),
            // 5=completed_at (also used for rotation and notification delivery)
            // NOTE: additional timestamps for the missing_usage_data note which
            // does NOT consume a now_epoch_ms call (it reuses completed_at_epoch_ms),
            // and for the final upsert_session. We pad extra just in case.
            &[100, 200, 300, 400, 500, 600, 700, 800],
        );
        let (workspace, mut executor) = build_executor_scenario("cli-rotation-integ", runtime, 128);

        // Write a pending rotation file BEFORE dispatching the run.
        let sr = state_root(&workspace);
        let rotation = PendingRotation {
            checkpoint: CheckpointTurnDocument {
                summary: "CLI rotation checkpoint summary".to_string(),
                decisions: vec!["decision-A".to_string()],
                open_questions: vec!["question-B".to_string()],
                next_actions: vec!["action-C".to_string()],
                artifacts: vec![CheckpointTurnArtifact {
                    path: "src/lib.rs".to_string(),
                    note: "updated library".to_string(),
                }],
            },
        };
        let pending_path =
            write_pending_rotation(&sr, &rotation).expect("write pending rotation should succeed");
        assert!(
            pending_path.exists(),
            "pending rotation file should exist before run"
        );

        // Dispatch the message through the full pipeline.
        let dispatch = executor
            .process_gateway_message(gateway_message("m-rot"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Succeeded);

        let logical_session_id = "discord:channel:777";
        let run_id = "run:discord:channel:777:m-rot";

        // 1) Verify the pending rotation file was consumed.
        assert!(
            !pending_path.exists(),
            "pending rotation file should be consumed after run"
        );
        let remaining = sr.join(PENDING_ROTATIONS_DIR_NAME);
        let dir_entries: Vec<_> = fs::read_dir(&remaining)
            .expect("pending_rotations dir should be readable")
            .flatten()
            .collect();
        assert!(
            dir_entries.is_empty(),
            "no pending rotation files should remain"
        );

        // 2) Verify checkpoint was persisted.
        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(logical_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("checkpoint should exist after rotation");
        assert_eq!(checkpoint.logical_session_id, logical_session_id);
        assert_eq!(checkpoint.run_id, run_id);
        assert_eq!(checkpoint.summary, "CLI rotation checkpoint summary");
        assert!(
            checkpoint.state.get("trigger") == Some(&"cli_rotation".to_string()),
            "checkpoint state should record cli_rotation trigger"
        );

        // 3) Verify the session handle was cleared (physical session removed, tokens zeroed).
        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(
            session.active_physical_session_id, None,
            "active_physical_session_id should be cleared after rotation"
        );
        assert_eq!(session.lane_state, LaneState::Idle);
        assert_eq!(
            session.token_accounting.total_tokens, 0,
            "token accounting should be zeroed after rotation"
        );
        assert!(
            !session.has_injected_bootstrap,
            "has_injected_bootstrap should be false after rotation"
        );
        assert_eq!(
            session.last_successful_checkpoint_id,
            Some(checkpoint.id.clone()),
            "session should reference the new checkpoint"
        );

        // 4) Verify rotation notification was delivered to Discord.
        let runtime = executor.runtime_mut();
        let rotation_delivery = runtime
            .delivered_outputs
            .iter()
            .find(|(_, _, msg_id, _, _)| msg_id.contains("rotation-notification"));
        assert!(
            rotation_delivery.is_some(),
            "rotation notification should be delivered to Discord"
        );
        let (_, _, notification_msg_id, _, notification_content) = rotation_delivery.unwrap();
        assert!(
            notification_content.contains("session rotated"),
            "notification should mention session rotated, got: {notification_content}"
        );
        assert!(
            notification_content.contains("cli_rotation"),
            "notification should mention cli_rotation trigger, got: {notification_content}"
        );
        assert!(
            notification_content.contains(&checkpoint.id),
            "notification should include the checkpoint ID, got: {notification_content}"
        );
        assert!(
            notification_msg_id.contains(run_id),
            "notification message ID should include the run ID"
        );

        // 5) Verify rotation events were emitted in the event log.
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("event replay should succeed");

        let rotation_started_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("rotation_event") == Some(&"started".to_string())
            })
            .collect();
        assert_eq!(
            rotation_started_events.len(),
            1,
            "exactly one rotation started event should be emitted"
        );
        assert_eq!(
            rotation_started_events[0].payload.get("trigger"),
            Some(&"cli_rotation".to_string())
        );

        let rotation_completed_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("rotation_event") == Some(&"completed".to_string())
            })
            .collect();
        assert_eq!(
            rotation_completed_events.len(),
            1,
            "exactly one rotation completed event should be emitted"
        );
        assert_eq!(
            rotation_completed_events[0].payload.get("checkpoint_id"),
            Some(&checkpoint.id)
        );
        assert_eq!(
            rotation_completed_events[0].payload.get("trigger"),
            Some(&"cli_rotation".to_string())
        );

        drop(workspace);
    }

    #[test]
    fn cli_rotation_during_pending_onboarding_completes_onboarding_and_delivers_message() {
        use crab_core::{
            write_pending_rotation, CheckpointTurnArtifact, CheckpointTurnDocument, PendingRotation,
        };

        let onboarding_json = onboarding_capture_payload_json();

        // Normal turn: completion only (no text delta to avoid delivery slot collision with
        // the rotation onboarding completion message at delivery_message_id(run_id, 0)).
        let normal_turn_events = vec![backend_event(
            1,
            BackendEventKind::TurnCompleted,
            &[("finish", "done")],
        )];
        // Hidden onboarding turn: return valid onboarding capture JSON as text deltas.
        let hidden_turn_events = vec![backend_event(
            1,
            BackendEventKind::TextDelta,
            &[("text", onboarding_json)],
        )];

        let dm_session_id = "discord:dm:424242424242424242";
        let dm_physical_session = physical_session_fixture_for(dm_session_id);

        // now_epoch_ms calls: queued_at, started_at, emitted(TurnCompleted), completed_at, plus padding.
        let mut runtime = FakeRuntime::with_backend_events(
            Vec::new(),
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        );
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(dm_physical_session.clone()), // normal turn
            Ok(dm_physical_session),         // hidden onboarding turn inside rotation
        ]);
        runtime.execute_turn_results = VecDeque::from(vec![
            Ok(normal_turn_events), // normal turn
            Ok(hidden_turn_events), // hidden onboarding extraction turn
        ]);

        let workspace = TempWorkspace::new("turn-executor", "cli-rotation-onboarding-complete");
        // Create BOOTSTRAP.md before build_executor (which removes it), then recreate it.
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut executor = build_executor(&workspace, runtime, 128);
        // Recreate BOOTSTRAP.md after build_executor removes it.
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable after executor build");

        // Write a pending rotation file before dispatching.
        let sr = state_root(&workspace);
        let rotation = PendingRotation {
            checkpoint: CheckpointTurnDocument {
                summary: "Rotation checkpoint during onboarding".to_string(),
                decisions: vec!["decision-1".to_string()],
                open_questions: vec![],
                next_actions: vec!["action-1".to_string()],
                artifacts: vec![CheckpointTurnArtifact {
                    path: "src/main.rs".to_string(),
                    note: "updated main".to_string(),
                }],
            },
        };
        let pending_path =
            write_pending_rotation(&sr, &rotation).expect("write pending rotation should succeed");
        assert!(pending_path.exists(), "pending rotation file should exist");

        // Dispatch via owner DM with normal (non-onboarding-capture) content.
        let dispatch = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-rot-onboard",
                "hello from owner",
            ))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Succeeded);

        let run_id = "run:discord:dm:424242424242424242:m-rot-onboard";

        // 1) Pending rotation consumed.
        assert!(
            !pending_path.exists(),
            "pending rotation file should be consumed"
        );

        // 2) Checkpoint persisted.
        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(dm_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("checkpoint should exist after rotation");
        assert_eq!(checkpoint.logical_session_id, dm_session_id);
        assert_eq!(checkpoint.run_id, run_id);

        // 3) BOOTSTRAP.md should be removed (onboarding completed).
        assert!(
            !workspace.path.join("BOOTSTRAP.md").exists(),
            "BOOTSTRAP.md should be removed after onboarding completion during rotation"
        );

        // 4) Onboarding profile files should be written.
        let soul = fs::read_to_string(workspace.path.join("SOUL.md"))
            .expect("SOUL profile should be written");
        assert!(
            soul.contains("Managed Mission Profile"),
            "SOUL.md should contain mission profile"
        );

        // 5) Verify onboarding completion delivery (not rotation notification).
        let runtime = executor.runtime_mut();
        let onboarding_delivery = runtime
            .delivered_outputs
            .iter()
            .find(|(_, _, _, _, content)| {
                content.contains("Onboarding completed during checkpoint rotation.")
            });
        assert!(
            onboarding_delivery.is_some(),
            "onboarding completion message should be delivered, got: {:?}",
            runtime.delivered_outputs
        );

        // 6) Verify rotation events were emitted in the event log.
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(dm_session_id, run_id)
            .expect("event replay should succeed");

        let rotation_started: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("rotation_event") == Some(&"started".to_string())
            })
            .collect();
        assert_eq!(rotation_started.len(), 1);

        let rotation_completed: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("rotation_event") == Some(&"completed".to_string())
            })
            .collect();
        assert_eq!(rotation_completed.len(), 1);

        let onboarding_applied: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("event") == Some(&"onboarding_rotation_applied".to_string())
            })
            .collect();
        assert_eq!(
            onboarding_applied.len(),
            1,
            "onboarding_rotation_applied event should be emitted"
        );

        drop(workspace);
    }

    #[test]
    fn cli_rotation_during_pending_onboarding_handles_incomplete_token() {
        use crab_core::{
            write_pending_rotation, CheckpointTurnArtifact, CheckpointTurnDocument,
            PendingRotation, ONBOARDING_CAPTURE_INCOMPLETE_TOKEN,
        };

        // Normal turn: text + completion.
        let normal_turn_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "reply")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        // Hidden onboarding turn: return INCOMPLETE token.
        let hidden_turn_events = vec![backend_event(
            1,
            BackendEventKind::TextDelta,
            &[("text", ONBOARDING_CAPTURE_INCOMPLETE_TOKEN)],
        )];

        let dm_session_id = "discord:dm:424242424242424242";
        let dm_physical_session = physical_session_fixture_for(dm_session_id);

        let mut runtime = FakeRuntime::with_backend_events(
            Vec::new(),
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        );
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(dm_physical_session.clone()),
            Ok(dm_physical_session),
        ]);
        runtime.execute_turn_results =
            VecDeque::from(vec![Ok(normal_turn_events), Ok(hidden_turn_events)]);

        let workspace = TempWorkspace::new("turn-executor", "cli-rotation-onboarding-incomplete");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut executor = build_executor(&workspace, runtime, 128);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable after executor build");

        let sr = state_root(&workspace);
        let rotation = PendingRotation {
            checkpoint: CheckpointTurnDocument {
                summary: "Rotation checkpoint during incomplete onboarding".to_string(),
                decisions: vec![],
                open_questions: vec![],
                next_actions: vec![],
                artifacts: vec![CheckpointTurnArtifact {
                    path: "src/lib.rs".to_string(),
                    note: "partial".to_string(),
                }],
            },
        };
        let pending_path =
            write_pending_rotation(&sr, &rotation).expect("write pending rotation should succeed");
        assert!(pending_path.exists());

        let dispatch = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-rot-incomplete",
                "hello from owner",
            ))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Succeeded);

        let run_id = "run:discord:dm:424242424242424242:m-rot-incomplete";
        let dm_session_id = "discord:dm:424242424242424242";

        // Pending rotation consumed.
        assert!(!pending_path.exists());

        // Checkpoint still persisted (rotation completed).
        let checkpoint = executor
            .composition()
            .state_stores
            .checkpoint_store
            .latest_checkpoint(dm_session_id)
            .expect("checkpoint lookup should succeed")
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.run_id, run_id);

        // BOOTSTRAP.md should still exist (onboarding NOT completed).
        assert!(
            workspace.path.join("BOOTSTRAP.md").exists(),
            "BOOTSTRAP.md should remain when onboarding is incomplete"
        );

        // Verify rotation notification (not onboarding message) was delivered.
        let runtime = executor.runtime_mut();
        let rotation_delivery = runtime
            .delivered_outputs
            .iter()
            .find(|(_, _, msg_id, _, _)| msg_id.contains("rotation-notification"));
        assert!(
            rotation_delivery.is_some(),
            "rotation notification should be delivered when onboarding is incomplete"
        );

        // Verify incomplete onboarding event was recorded.
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(dm_session_id, run_id)
            .expect("event replay should succeed");
        let incomplete_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("event") == Some(&"onboarding_rotation_incomplete".to_string())
            })
            .collect();
        assert_eq!(
            incomplete_events.len(),
            1,
            "onboarding_rotation_incomplete event should be emitted"
        );

        drop(workspace);
    }

    #[test]
    fn cli_rotation_during_pending_onboarding_handles_parse_error() {
        use crab_core::{
            write_pending_rotation, CheckpointTurnArtifact, CheckpointTurnDocument, PendingRotation,
        };

        // Normal turn: completion only.
        let normal_turn_events = vec![backend_event(
            1,
            BackendEventKind::TurnCompleted,
            &[("finish", "done")],
        )];
        // Hidden onboarding turn: return garbage (not INCOMPLETE token, not valid JSON).
        let hidden_turn_events = vec![backend_event(
            1,
            BackendEventKind::TextDelta,
            &[("text", "this is not valid json at all")],
        )];

        let dm_session_id = "discord:dm:424242424242424242";
        let dm_physical_session = physical_session_fixture_for(dm_session_id);

        let mut runtime = FakeRuntime::with_backend_events(
            Vec::new(),
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        );
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(dm_physical_session.clone()),
            Ok(dm_physical_session),
        ]);
        runtime.execute_turn_results =
            VecDeque::from(vec![Ok(normal_turn_events), Ok(hidden_turn_events)]);

        let workspace = TempWorkspace::new("turn-executor", "cli-rotation-onboarding-parse-error");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut executor = build_executor(&workspace, runtime, 128);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable after executor build");

        let sr = state_root(&workspace);
        let rotation = PendingRotation {
            checkpoint: CheckpointTurnDocument {
                summary: "Rotation with parse error".to_string(),
                decisions: vec![],
                open_questions: vec![],
                next_actions: vec![],
                artifacts: vec![CheckpointTurnArtifact {
                    path: "src/lib.rs".to_string(),
                    note: "updated".to_string(),
                }],
            },
        };
        write_pending_rotation(&sr, &rotation).expect("write pending rotation should succeed");

        let dispatch = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-rot-parse-err",
                "hello from owner",
            ))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Succeeded);

        let run_id = "run:discord:dm:424242424242424242:m-rot-parse-err";

        // BOOTSTRAP.md should still exist (onboarding NOT completed due to parse error).
        assert!(
            workspace.path.join("BOOTSTRAP.md").exists(),
            "BOOTSTRAP.md should remain when onboarding extraction produces invalid JSON"
        );

        // Verify parse error event was recorded.
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(dm_session_id, run_id)
            .expect("event replay should succeed");
        let parse_error_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("event")
                        == Some(&"onboarding_rotation_parse_error".to_string())
            })
            .collect();
        assert_eq!(
            parse_error_events.len(),
            1,
            "onboarding_rotation_parse_error event should be emitted"
        );
        assert!(
            parse_error_events[0].payload.contains_key("error"),
            "parse error event should include the error details"
        );

        drop(workspace);
    }

    #[test]
    fn cli_rotation_during_pending_onboarding_handles_apply_error() {
        use crab_core::{
            write_pending_rotation, CheckpointTurnArtifact, CheckpointTurnDocument, PendingRotation,
        };

        let onboarding_json = onboarding_capture_payload_json();

        // Normal turn: completion only.
        let normal_turn_events = vec![backend_event(
            1,
            BackendEventKind::TurnCompleted,
            &[("finish", "done")],
        )];
        // Hidden onboarding turn: return valid onboarding JSON (schema v1, all fields present).
        let hidden_turn_events = vec![backend_event(
            1,
            BackendEventKind::TextDelta,
            &[("text", onboarding_json)],
        )];

        let dm_session_id = "discord:dm:424242424242424242";
        let dm_physical_session = physical_session_fixture_for(dm_session_id);

        let mut runtime = FakeRuntime::with_backend_events(
            Vec::new(),
            &[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        );
        runtime.resolve_profile_results = VecDeque::from(vec![Ok(owner_profile_telemetry())]);
        runtime.ensure_session_results = VecDeque::from(vec![
            Ok(dm_physical_session.clone()),
            Ok(dm_physical_session),
        ]);
        runtime.execute_turn_results =
            VecDeque::from(vec![Ok(normal_turn_events), Ok(hidden_turn_events)]);

        let workspace = TempWorkspace::new("turn-executor", "cli-rotation-onboarding-apply-error");
        fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for onboarding setup");
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable");
        let mut executor = build_executor(&workspace, runtime, 128);
        fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable after executor build");

        // Sabotage: replace IDENTITY.md with a directory so persist_onboarding_profile_files
        // fails during apply_onboarding_capture_document.
        let identity_path = workspace.path.join("IDENTITY.md");
        let _ = fs::remove_file(&identity_path);
        fs::create_dir_all(&identity_path).expect("IDENTITY.md directory sabotage should succeed");

        let sr = state_root(&workspace);
        let rotation = PendingRotation {
            checkpoint: CheckpointTurnDocument {
                summary: "Rotation with apply error".to_string(),
                decisions: vec![],
                open_questions: vec![],
                next_actions: vec![],
                artifacts: vec![CheckpointTurnArtifact {
                    path: "src/lib.rs".to_string(),
                    note: "updated".to_string(),
                }],
            },
        };
        write_pending_rotation(&sr, &rotation).expect("write pending rotation should succeed");

        let dispatch = executor
            .process_gateway_message(gateway_owner_dm_message_with_content(
                "m-rot-apply-err",
                "hello from owner",
            ))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(dispatch.status, RunStatus::Succeeded);

        let run_id = "run:discord:dm:424242424242424242:m-rot-apply-err";

        // BOOTSTRAP.md should still exist.
        assert!(
            workspace.path.join("BOOTSTRAP.md").exists(),
            "BOOTSTRAP.md should remain when onboarding apply fails"
        );

        // Verify apply error event was recorded.
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run(dm_session_id, run_id)
            .expect("event replay should succeed");
        let apply_error_events: Vec<_> = events
            .iter()
            .filter(|e| {
                e.kind == EventKind::RunNote
                    && e.payload.get("event")
                        == Some(&"onboarding_rotation_apply_error".to_string())
            })
            .collect();
        assert_eq!(
            apply_error_events.len(),
            1,
            "onboarding_rotation_apply_error event should be emitted"
        );
        assert!(
            apply_error_events[0].payload.contains_key("error"),
            "apply error event should include the error details"
        );

        drop(workspace);
    }

    // ── Graceful steering tests ─────────────────────────────────────────

    #[test]
    fn graceful_steering_waits_for_tool_result_before_interrupting() {
        // Scenario: graceful trigger is present, events flow:
        //   TextDelta(1) -> ToolCall(2) -> ToolResult(3) -> TextDelta(4)
        // The interrupt should fire at TextDelta(4) -- the boundary.
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "thinking")]),
            backend_event(2, BackendEventKind::ToolCall, &[("tool", "read_file")]),
            backend_event(3, BackendEventKind::ToolResult, &[("output", "contents")]),
            backend_event(4, BackendEventKind::TextDelta, &[("text", "next")]),
            backend_event(5, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        // Generous timestamps: enqueue(1), started(2), event1(3), event2(4), event3(5),
        // steered_note(6), completed(7)
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (workspace, mut executor) =
            build_executor_scenario("graceful-steer-boundary", runtime, 8);

        // Pre-write a graceful steering trigger to the state dir
        let state_root = workspace.path.join("state");
        crab_core::write_graceful_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "graceful steer".to_string(),
            },
        )
        .expect("trigger write should succeed");

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Cancelled,
            "graceful steering should mark run as cancelled"
        );

        // Verify interrupt was called
        let runtime = executor.runtime_mut();
        assert_eq!(
            runtime.interrupt_calls.len(),
            1,
            "interrupt_backend_turn should be called once at the boundary"
        );

        // Verify the boundary event (TextDelta 4) was NOT appended (boundary detection is
        // before append, so events 1-3 are stored but 4 is not).
        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("event replay should succeed");
        let backend_event_count = events
            .iter()
            .filter(|e| {
                matches!(
                    e.kind,
                    EventKind::TextDelta | EventKind::ToolCall | EventKind::ToolResult
                )
            })
            .count();
        assert_eq!(
            backend_event_count, 3,
            "only 3 backend events should be stored (boundary event excluded)"
        );
    }

    #[test]
    fn graceful_steering_turn_completed_stays_succeeded() {
        // Scenario: graceful trigger present, but turn has no tools -- it completes
        // naturally. The run should be Succeeded, not Cancelled.
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "response")]),
            backend_event(2, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (workspace, mut executor) =
            build_executor_scenario("graceful-steer-completed", runtime, 8);

        let state_root = workspace.path.join("state");
        crab_core::write_graceful_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "777".to_string(),
                message: "graceful steer".to_string(),
            },
        )
        .expect("trigger write should succeed");

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "naturally completed turn should stay Succeeded even with graceful steer pending"
        );

        let runtime = executor.runtime_mut();
        assert!(
            runtime.interrupt_calls.is_empty(),
            "interrupt should not be called when turn completes naturally"
        );
    }

    #[test]
    fn graceful_steering_for_different_lane_does_not_steer() {
        // Scenario: graceful trigger targets a different channel than the active run.
        let backend_events = vec![
            backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
            backend_event(2, BackendEventKind::ToolCall, &[("tool", "bash")]),
            backend_event(3, BackendEventKind::ToolResult, &[("output", "ok")]),
            backend_event(4, BackendEventKind::TextDelta, &[("text", "done")]),
            backend_event(5, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (workspace, mut executor) =
            build_executor_scenario("graceful-steer-cross-lane", runtime, 8);

        let state_root = workspace.path.join("state");
        crab_core::write_graceful_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "888".to_string(), // different channel
                message: "graceful steer other lane".to_string(),
            },
        )
        .expect("trigger write should succeed");

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "cross-lane graceful trigger should not interrupt the run"
        );

        let runtime = executor.runtime_mut();
        assert!(
            runtime.interrupt_calls.is_empty(),
            "interrupt should not be called for cross-lane graceful triggers"
        );
    }

    #[test]
    fn graceful_steering_boundary_fresh_poll_finds_no_trigger() {
        // Scenario: no graceful trigger exists. Events include a ToolResult -> TextDelta
        // boundary. The fresh-poll at the boundary should fire (graceful_steer_pending
        // is false) and return false, so the turn completes normally.
        let backend_events = vec![
            backend_event(1, BackendEventKind::ToolResult, &[("output", "done")]),
            backend_event(2, BackendEventKind::TextDelta, &[("text", "summary")]),
            backend_event(3, BackendEventKind::TurnCompleted, &[("finish", "done")]),
        ];
        let mut runtime =
            FakeRuntime::with_backend_events(backend_events, &[1, 2, 3, 4, 5, 6, 7, 8]);
        runtime
            .resolve_profile_results
            .push_back(Ok(sample_profile_telemetry()));
        let (_workspace, mut executor) =
            build_executor_scenario("graceful-boundary-no-trigger", runtime, 8);

        // No trigger written -- the fresh-poll at the boundary should find nothing.

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");

        assert_eq!(
            dispatch.status,
            RunStatus::Succeeded,
            "turn should complete normally when no graceful trigger exists"
        );

        let runtime = executor.runtime_mut();
        assert!(
            runtime.interrupt_calls.is_empty(),
            "interrupt should not be called when no graceful trigger exists"
        );
    }
}

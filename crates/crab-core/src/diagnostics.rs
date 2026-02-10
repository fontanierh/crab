use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    validation::validate_non_empty_text, BackendKind, CrabError, CrabResult, ReasoningLevel,
    RunStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticCategory {
    Scheduler,
    RunLifecycle,
    BackendLifecycle,
    Recovery,
    Rotation,
    Operator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiagnosticEvent {
    RunQueued {
        logical_session_id: String,
        run_id: String,
        queued_depth: u32,
    },
    RunStarted {
        logical_session_id: String,
        run_id: String,
        backend: BackendKind,
        model: String,
        reasoning_level: ReasoningLevel,
    },
    RunCompleted {
        logical_session_id: String,
        run_id: String,
        status: RunStatus,
        total_tokens: u64,
    },
    RunCancelRequested {
        logical_session_id: String,
        run_id: String,
        reason: String,
    },
    BackendRestarted {
        backend: BackendKind,
        reason: String,
        logical_session_id: Option<String>,
        run_id: Option<String>,
    },
    RotationStarted {
        logical_session_id: String,
        trigger: String,
    },
    RotationCompleted {
        logical_session_id: String,
        checkpoint_id: String,
    },
    StartupRunRecovered {
        logical_session_id: String,
        run_id: String,
        previous_status: RunStatus,
    },
    OperatorCommandApplied {
        logical_session_id: String,
        command_name: String,
        requires_rotation: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticRecord {
    pub emitted_at_epoch_ms: u64,
    pub severity: DiagnosticSeverity,
    pub category: DiagnosticCategory,
    pub event_name: String,
    pub message: String,
    pub logical_session_id: Option<String>,
    pub run_id: Option<String>,
    pub physical_session_id: Option<String>,
    pub fields: BTreeMap<String, String>,
}

pub fn build_diagnostic_record(
    emitted_at_epoch_ms: u64,
    event: DiagnosticEvent,
) -> CrabResult<DiagnosticRecord> {
    if emitted_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "diagnostics_build",
            message: "emitted_at_epoch_ms must be greater than zero".to_string(),
        });
    }

    let mut record = match event {
        DiagnosticEvent::RunQueued {
            logical_session_id,
            run_id,
            queued_depth,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Info,
            DiagnosticCategory::Scheduler,
            "run_queued",
            "run queued for execution",
            CorrelationInput::required_run(logical_session_id, run_id),
            [
                ("queued_depth", queued_depth.to_string()),
                ("signal", "queue_pressure".to_string()),
            ],
        ),
        DiagnosticEvent::RunStarted {
            logical_session_id,
            run_id,
            backend,
            model,
            reasoning_level,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Info,
            DiagnosticCategory::RunLifecycle,
            "run_started",
            "run execution started",
            CorrelationInput::required_run(logical_session_id, run_id),
            [
                ("backend", backend_token(backend)),
                ("model", model),
                ("reasoning_level", reasoning_level.as_token().to_string()),
            ],
        ),
        DiagnosticEvent::RunCompleted {
            logical_session_id,
            run_id,
            status,
            total_tokens,
        } => build_record(
            emitted_at_epoch_ms,
            run_completion_severity(status),
            DiagnosticCategory::RunLifecycle,
            "run_completed",
            "run execution completed",
            CorrelationInput::required_run(logical_session_id, run_id),
            [
                ("status", status_token(status)),
                ("total_tokens", total_tokens.to_string()),
            ],
        ),
        DiagnosticEvent::RunCancelRequested {
            logical_session_id,
            run_id,
            reason,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Warn,
            DiagnosticCategory::RunLifecycle,
            "run_cancel_requested",
            "run cancellation requested",
            CorrelationInput::required_run(logical_session_id, run_id),
            [("reason", reason)],
        ),
        DiagnosticEvent::BackendRestarted {
            backend,
            reason,
            logical_session_id,
            run_id,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Warn,
            DiagnosticCategory::BackendLifecycle,
            "backend_restarted",
            "backend process restarted",
            CorrelationInput {
                logical_session_id,
                run_id,
                physical_session_id: None,
            },
            [("backend", backend_token(backend)), ("reason", reason)],
        ),
        DiagnosticEvent::RotationStarted {
            logical_session_id,
            trigger,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Info,
            DiagnosticCategory::Rotation,
            "rotation_started",
            "physical session rotation started",
            CorrelationInput::required_session(logical_session_id),
            [("trigger", trigger)],
        ),
        DiagnosticEvent::RotationCompleted {
            logical_session_id,
            checkpoint_id,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Info,
            DiagnosticCategory::Rotation,
            "rotation_completed",
            "physical session rotation completed",
            CorrelationInput::required_session(logical_session_id),
            [("checkpoint_id", checkpoint_id)],
        ),
        DiagnosticEvent::StartupRunRecovered {
            logical_session_id,
            run_id,
            previous_status,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Warn,
            DiagnosticCategory::Recovery,
            "startup_run_recovered",
            "stale run recovered during startup",
            CorrelationInput::required_run(logical_session_id, run_id),
            [("previous_status", status_token(previous_status))],
        ),
        DiagnosticEvent::OperatorCommandApplied {
            logical_session_id,
            command_name,
            requires_rotation,
        } => build_record(
            emitted_at_epoch_ms,
            DiagnosticSeverity::Info,
            DiagnosticCategory::Operator,
            "operator_command_applied",
            "operator command applied",
            CorrelationInput::required_session(logical_session_id),
            [
                ("command_name", command_name),
                ("requires_rotation", requires_rotation.to_string()),
            ],
        ),
    };

    let normalized_correlation = normalize_correlation_ids(CorrelationInput {
        logical_session_id: record.logical_session_id.clone(),
        run_id: record.run_id.clone(),
        physical_session_id: record.physical_session_id.clone(),
    })?;
    record.logical_session_id = normalized_correlation.logical_session_id;
    record.run_id = normalized_correlation.run_id;
    record.physical_session_id = normalized_correlation.physical_session_id;

    validate_diagnostic_record(&record)?;
    Ok(record)
}

pub fn render_diagnostic_record_json(record: &DiagnosticRecord) -> CrabResult<String> {
    validate_diagnostic_record(record)?;
    let encoded = serde_json::to_string(record)
        .expect("DiagnosticRecord serialization should be infallible for scalar fields");
    Ok(encoded)
}

pub fn parse_diagnostic_record_json(line: &str) -> CrabResult<DiagnosticRecord> {
    validate_non_empty_text("diagnostics_record_parse", "line", line)?;
    let parsed = serde_json::from_str::<DiagnosticRecord>(line).map_err(|error| {
        CrabError::Serialization {
            context: "diagnostics_record_parse",
            path: None,
            message: error.to_string(),
        }
    })?;
    validate_diagnostic_record(&parsed)?;
    Ok(parsed)
}

pub fn render_diagnostics_fixture(records: &[DiagnosticRecord]) -> CrabResult<String> {
    if records.is_empty() {
        return Ok(String::new());
    }

    let mut lines = Vec::with_capacity(records.len());
    for record in records {
        lines.push(render_diagnostic_record_json(record)?);
    }
    Ok(format!("{}\n", lines.join("\n")))
}

pub fn parse_diagnostics_fixture(fixture: &str) -> CrabResult<Vec<DiagnosticRecord>> {
    let mut records = Vec::new();
    for (index, line) in fixture.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed =
            parse_diagnostic_record_json(line).map_err(|error| CrabError::InvariantViolation {
                context: "diagnostics_fixture_parse",
                message: format!("line {} is invalid: {error}", index + 1),
            })?;
        records.push(parsed);
    }
    Ok(records)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CorrelationInput {
    logical_session_id: Option<String>,
    run_id: Option<String>,
    physical_session_id: Option<String>,
}

impl CorrelationInput {
    fn required_session(logical_session_id: String) -> Self {
        Self {
            logical_session_id: Some(logical_session_id),
            run_id: None,
            physical_session_id: None,
        }
    }

    fn required_run(logical_session_id: String, run_id: String) -> Self {
        Self {
            logical_session_id: Some(logical_session_id),
            run_id: Some(run_id),
            physical_session_id: None,
        }
    }
}

fn build_record<const N: usize>(
    emitted_at_epoch_ms: u64,
    severity: DiagnosticSeverity,
    category: DiagnosticCategory,
    event_name: &'static str,
    message: &'static str,
    correlation: CorrelationInput,
    fields: [(&'static str, String); N],
) -> DiagnosticRecord {
    let mut details = BTreeMap::new();
    for (key, value) in fields {
        details.insert(key.to_string(), value);
    }

    DiagnosticRecord {
        emitted_at_epoch_ms,
        severity,
        category,
        event_name: event_name.to_string(),
        message: message.to_string(),
        logical_session_id: correlation.logical_session_id,
        run_id: correlation.run_id,
        physical_session_id: correlation.physical_session_id,
        fields: details,
    }
}

fn validate_diagnostic_record(record: &DiagnosticRecord) -> CrabResult<()> {
    if record.emitted_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "diagnostics_record_validate",
            message: "emitted_at_epoch_ms must be greater than zero".to_string(),
        });
    }

    validate_non_empty_text(
        "diagnostics_record_validate",
        "event_name",
        &record.event_name,
    )?;
    validate_non_empty_text("diagnostics_record_validate", "message", &record.message)?;

    let normalized = normalize_correlation_ids(CorrelationInput {
        logical_session_id: record.logical_session_id.clone(),
        run_id: record.run_id.clone(),
        physical_session_id: record.physical_session_id.clone(),
    })?;

    if normalized.logical_session_id != record.logical_session_id
        || normalized.run_id != record.run_id
        || normalized.physical_session_id != record.physical_session_id
    {
        return Err(CrabError::InvariantViolation {
            context: "diagnostics_record_validate",
            message: "correlation identifiers must already be normalized".to_string(),
        });
    }

    for (key, value) in &record.fields {
        validate_non_empty_text("diagnostics_record_validate", "field_key", key)?;
        validate_non_empty_text("diagnostics_record_validate", "field_value", value)?;
    }

    Ok(())
}

fn normalize_correlation_ids(input: CorrelationInput) -> CrabResult<CorrelationInput> {
    let logical_session_id = normalize_optional_field(
        "diagnostics_correlation_validate",
        "logical_session_id",
        input.logical_session_id,
    )?;
    let run_id =
        normalize_optional_field("diagnostics_correlation_validate", "run_id", input.run_id)?;
    let physical_session_id = normalize_optional_field(
        "diagnostics_correlation_validate",
        "physical_session_id",
        input.physical_session_id,
    )?;

    if run_id.is_some() && logical_session_id.is_none() {
        return Err(CrabError::InvariantViolation {
            context: "diagnostics_correlation_validate",
            message: "run_id requires logical_session_id".to_string(),
        });
    }

    Ok(CorrelationInput {
        logical_session_id,
        run_id,
        physical_session_id,
    })
}

fn normalize_optional_field(
    context: &'static str,
    field: &'static str,
    value: Option<String>,
) -> CrabResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };

    let normalized = value.trim().to_string();
    validate_non_empty_text(context, field, &normalized)?;
    Ok(Some(normalized))
}

fn run_completion_severity(status: RunStatus) -> DiagnosticSeverity {
    match status {
        RunStatus::Succeeded => DiagnosticSeverity::Info,
        RunStatus::Queued | RunStatus::Running | RunStatus::Failed | RunStatus::Cancelled => {
            DiagnosticSeverity::Warn
        }
    }
}

fn backend_token(backend: BackendKind) -> String {
    format!("{backend:?}").to_ascii_lowercase()
}

fn status_token(status: RunStatus) -> String {
    format!("{status:?}").to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{BackendKind, CrabError, ReasoningLevel, RunStatus};

    use super::{
        build_diagnostic_record, parse_diagnostic_record_json, parse_diagnostics_fixture,
        render_diagnostic_record_json, render_diagnostics_fixture, DiagnosticCategory,
        DiagnosticEvent, DiagnosticRecord, DiagnosticSeverity,
    };

    fn assert_record(
        event: DiagnosticEvent,
        severity: DiagnosticSeverity,
        category: DiagnosticCategory,
        event_name: &str,
    ) -> DiagnosticRecord {
        let record = build_diagnostic_record(1_739_281_600_000, event)
            .expect("diagnostic build should succeed");
        assert_eq!(record.severity, severity);
        assert_eq!(record.category, category);
        assert_eq!(record.event_name, event_name);
        assert_eq!(
            record.logical_session_id,
            Some("discord:channel:123".to_string())
        );
        record
    }

    #[test]
    fn builds_high_signal_records_for_all_event_variants() {
        let queued = assert_record(
            DiagnosticEvent::RunQueued {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-1".to_string(),
                queued_depth: 2,
            },
            DiagnosticSeverity::Info,
            DiagnosticCategory::Scheduler,
            "run_queued",
        );
        assert_eq!(queued.run_id, Some("run-1".to_string()));
        assert_eq!(
            queued.fields,
            BTreeMap::from([
                ("queued_depth".to_string(), "2".to_string()),
                ("signal".to_string(), "queue_pressure".to_string()),
            ])
        );

        let started = assert_record(
            DiagnosticEvent::RunStarted {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-2".to_string(),
                backend: BackendKind::OpenCode,
                model: "o4-mini".to_string(),
                reasoning_level: ReasoningLevel::High,
            },
            DiagnosticSeverity::Info,
            DiagnosticCategory::RunLifecycle,
            "run_started",
        );
        assert_eq!(started.fields["backend"], "opencode");
        assert_eq!(started.fields["model"], "o4-mini");
        assert_eq!(started.fields["reasoning_level"], "high");

        let completed_ok = assert_record(
            DiagnosticEvent::RunCompleted {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-3".to_string(),
                status: RunStatus::Succeeded,
                total_tokens: 1234,
            },
            DiagnosticSeverity::Info,
            DiagnosticCategory::RunLifecycle,
            "run_completed",
        );
        assert_eq!(completed_ok.fields["status"], "succeeded");
        assert_eq!(completed_ok.fields["total_tokens"], "1234");

        let completed_warn = assert_record(
            DiagnosticEvent::RunCompleted {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-4".to_string(),
                status: RunStatus::Cancelled,
                total_tokens: 5,
            },
            DiagnosticSeverity::Warn,
            DiagnosticCategory::RunLifecycle,
            "run_completed",
        );
        assert_eq!(completed_warn.fields["status"], "cancelled");

        let cancelled = assert_record(
            DiagnosticEvent::RunCancelRequested {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-5".to_string(),
                reason: "manual_cancel".to_string(),
            },
            DiagnosticSeverity::Warn,
            DiagnosticCategory::RunLifecycle,
            "run_cancel_requested",
        );
        assert_eq!(cancelled.fields["reason"], "manual_cancel");

        let backend_restarted = build_diagnostic_record(
            1_739_281_600_000,
            DiagnosticEvent::BackendRestarted {
                backend: BackendKind::Codex,
                reason: "heartbeat_unhealthy".to_string(),
                logical_session_id: None,
                run_id: None,
            },
        )
        .expect("backend restart should support global scope");
        assert_eq!(backend_restarted.severity, DiagnosticSeverity::Warn);
        assert_eq!(
            backend_restarted.category,
            DiagnosticCategory::BackendLifecycle
        );
        assert_eq!(backend_restarted.logical_session_id, None);
        assert_eq!(backend_restarted.run_id, None);
        assert_eq!(backend_restarted.fields["backend"], "codex");

        let rotation_started = assert_record(
            DiagnosticEvent::RotationStarted {
                logical_session_id: "discord:channel:123".to_string(),
                trigger: "inactivity".to_string(),
            },
            DiagnosticSeverity::Info,
            DiagnosticCategory::Rotation,
            "rotation_started",
        );
        assert_eq!(rotation_started.fields["trigger"], "inactivity");

        let rotation_completed = assert_record(
            DiagnosticEvent::RotationCompleted {
                logical_session_id: "discord:channel:123".to_string(),
                checkpoint_id: "ckpt-9".to_string(),
            },
            DiagnosticSeverity::Info,
            DiagnosticCategory::Rotation,
            "rotation_completed",
        );
        assert_eq!(rotation_completed.fields["checkpoint_id"], "ckpt-9");

        let startup_recovered = assert_record(
            DiagnosticEvent::StartupRunRecovered {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-6".to_string(),
                previous_status: RunStatus::Running,
            },
            DiagnosticSeverity::Warn,
            DiagnosticCategory::Recovery,
            "startup_run_recovered",
        );
        assert_eq!(startup_recovered.fields["previous_status"], "running");

        let operator_applied = assert_record(
            DiagnosticEvent::OperatorCommandApplied {
                logical_session_id: "discord:channel:123".to_string(),
                command_name: "/backend codex".to_string(),
                requires_rotation: true,
            },
            DiagnosticSeverity::Info,
            DiagnosticCategory::Operator,
            "operator_command_applied",
        );
        assert_eq!(operator_applied.fields["command_name"], "/backend codex");
        assert_eq!(operator_applied.fields["requires_rotation"], "true");
    }

    #[test]
    fn rejects_invalid_inputs_and_correlation_shape() {
        let error = build_diagnostic_record(
            0,
            DiagnosticEvent::RunQueued {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-1".to_string(),
                queued_depth: 1,
            },
        )
        .expect_err("zero timestamps should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "diagnostics_build",
                message: "emitted_at_epoch_ms must be greater than zero".to_string(),
            }
        );

        let correlation_error = build_diagnostic_record(
            1_739_281_600_000,
            DiagnosticEvent::BackendRestarted {
                backend: BackendKind::Claude,
                reason: "restart".to_string(),
                logical_session_id: None,
                run_id: Some("run-1".to_string()),
            },
        )
        .expect_err("run id without session id should fail");
        assert_eq!(
            correlation_error,
            CrabError::InvariantViolation {
                context: "diagnostics_correlation_validate",
                message: "run_id requires logical_session_id".to_string(),
            }
        );

        let blank_reason = build_diagnostic_record(
            1_739_281_600_000,
            DiagnosticEvent::RunCancelRequested {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-2".to_string(),
                reason: " ".to_string(),
            },
        )
        .expect_err("blank field values should be rejected");
        assert_eq!(
            blank_reason,
            CrabError::InvariantViolation {
                context: "diagnostics_record_validate",
                message: "field_value must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn record_json_round_trip_and_validation() {
        let built = build_diagnostic_record(
            1_739_281_600_100,
            DiagnosticEvent::RunStarted {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-7".to_string(),
                backend: BackendKind::Codex,
                model: "gpt-5-codex".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
        )
        .expect("build should succeed");
        let encoded = render_diagnostic_record_json(&built).expect("render should succeed");
        let parsed = parse_diagnostic_record_json(&encoded).expect("parse should succeed");
        assert_eq!(parsed, built);

        let invalid = "{\"emitted_at_epoch_ms\":1,\"severity\":\"info\",\"category\":\"scheduler\",\"event_name\":\"x\",\"message\":\"m\",\"logical_session_id\":null,\"run_id\":\"run-1\",\"physical_session_id\":null,\"fields\":{}}";
        let error = parse_diagnostic_record_json(invalid)
            .expect_err("shape validation should reject run id without session id");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "diagnostics_correlation_validate",
                message: "run_id requires logical_session_id".to_string(),
            }
        );
    }

    #[test]
    fn diagnostics_fixture_round_trip_uses_deterministic_json_lines() {
        let first = build_diagnostic_record(
            1_739_281_600_000,
            DiagnosticEvent::RunQueued {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-10".to_string(),
                queued_depth: 3,
            },
        )
        .expect("first record should build");

        let second = build_diagnostic_record(
            1_739_281_600_010,
            DiagnosticEvent::OperatorCommandApplied {
                logical_session_id: "discord:channel:123".to_string(),
                command_name: "/reasoning high".to_string(),
                requires_rotation: false,
            },
        )
        .expect("second record should build");

        let fixture = render_diagnostics_fixture(&[first.clone(), second.clone()])
            .expect("fixture rendering should succeed");
        let expected = concat!(
            "{\"emitted_at_epoch_ms\":1739281600000,\"severity\":\"info\",\"category\":\"scheduler\",\"event_name\":\"run_queued\",\"message\":\"run queued for execution\",\"logical_session_id\":\"discord:channel:123\",\"run_id\":\"run-10\",\"physical_session_id\":null,\"fields\":{\"queued_depth\":\"3\",\"signal\":\"queue_pressure\"}}\n",
            "{\"emitted_at_epoch_ms\":1739281600010,\"severity\":\"info\",\"category\":\"operator\",\"event_name\":\"operator_command_applied\",\"message\":\"operator command applied\",\"logical_session_id\":\"discord:channel:123\",\"run_id\":null,\"physical_session_id\":null,\"fields\":{\"command_name\":\"/reasoning high\",\"requires_rotation\":\"false\"}}\n"
        );
        assert_eq!(fixture, expected);

        let parsed = parse_diagnostics_fixture(&fixture).expect("fixture parsing should succeed");
        assert_eq!(parsed, vec![first, second]);
    }

    #[test]
    fn diagnostics_fixture_parser_ignores_blank_lines_and_reports_invalid_lines() {
        let fixture = "\n  \n";
        let parsed = parse_diagnostics_fixture(fixture).expect("blank fixtures should parse");
        assert!(parsed.is_empty());

        let error = parse_diagnostics_fixture("{\"bad\": true}\nnot-json\n")
            .expect_err("invalid line should fail");
        let inner_error = parse_diagnostic_record_json("{\"bad\": true}")
            .expect_err("single invalid line should fail parse");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "diagnostics_fixture_parse",
                message: format!("line 1 is invalid: {inner_error}"),
            }
        );
    }

    #[test]
    fn fixture_and_record_validation_cover_empty_and_non_normalized_paths() {
        let empty = render_diagnostics_fixture(&[]).expect("empty fixture should render");
        assert!(empty.is_empty());

        let zero_ts = "{\"emitted_at_epoch_ms\":0,\"severity\":\"info\",\"category\":\"scheduler\",\"event_name\":\"run_queued\",\"message\":\"run queued for execution\",\"logical_session_id\":\"discord:channel:123\",\"run_id\":\"run-1\",\"physical_session_id\":null,\"fields\":{\"queued_depth\":\"1\",\"signal\":\"queue_pressure\"}}";
        let zero_error =
            parse_diagnostic_record_json(zero_ts).expect_err("zero timestamp should fail");
        assert_eq!(
            zero_error,
            CrabError::InvariantViolation {
                context: "diagnostics_record_validate",
                message: "emitted_at_epoch_ms must be greater than zero".to_string(),
            }
        );

        let non_normalized = DiagnosticRecord {
            emitted_at_epoch_ms: 1_739_281_600_200,
            severity: DiagnosticSeverity::Info,
            category: DiagnosticCategory::Scheduler,
            event_name: "run_queued".to_string(),
            message: "run queued for execution".to_string(),
            logical_session_id: Some(" discord:channel:123 ".to_string()),
            run_id: Some("run-12".to_string()),
            physical_session_id: None,
            fields: BTreeMap::from([
                ("queued_depth".to_string(), "2".to_string()),
                ("signal".to_string(), "queue_pressure".to_string()),
            ]),
        };
        let normalize_error = render_diagnostic_record_json(&non_normalized)
            .expect_err("non-normalized correlation should fail");
        assert_eq!(
            normalize_error,
            CrabError::InvariantViolation {
                context: "diagnostics_record_validate",
                message: "correlation identifiers must already be normalized".to_string(),
            }
        );
    }

    #[test]
    fn validation_error_paths_are_exercised() {
        let parse_blank = parse_diagnostic_record_json("  ")
            .expect_err("blank parse input should fail validation");
        assert_eq!(
            parse_blank,
            CrabError::InvariantViolation {
                context: "diagnostics_record_parse",
                message: "line must not be empty".to_string(),
            }
        );

        let mut blank_event_name = build_diagnostic_record(
            1_739_281_600_300,
            DiagnosticEvent::RunQueued {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-13".to_string(),
                queued_depth: 1,
            },
        )
        .expect("seed record should build");
        blank_event_name.event_name = " ".to_string();
        let event_name_error =
            render_diagnostic_record_json(&blank_event_name).expect_err("blank event_name fails");
        assert_eq!(
            event_name_error,
            CrabError::InvariantViolation {
                context: "diagnostics_record_validate",
                message: "event_name must not be empty".to_string(),
            }
        );
        let fixture_error = render_diagnostics_fixture(&[blank_event_name])
            .expect_err("fixture render should propagate record errors");
        assert_eq!(fixture_error, event_name_error);

        let mut blank_message = build_diagnostic_record(
            1_739_281_600_301,
            DiagnosticEvent::RunQueued {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-14".to_string(),
                queued_depth: 1,
            },
        )
        .expect("seed record should build");
        blank_message.message = " ".to_string();
        let message_error =
            render_diagnostic_record_json(&blank_message).expect_err("blank message fails");
        assert_eq!(
            message_error,
            CrabError::InvariantViolation {
                context: "diagnostics_record_validate",
                message: "message must not be empty".to_string(),
            }
        );

        let mut blank_field_key = build_diagnostic_record(
            1_739_281_600_302,
            DiagnosticEvent::RunQueued {
                logical_session_id: "discord:channel:123".to_string(),
                run_id: "run-15".to_string(),
                queued_depth: 1,
            },
        )
        .expect("seed record should build");
        blank_field_key.fields = BTreeMap::from([(" ".to_string(), "value".to_string())]);
        let field_key_error =
            render_diagnostic_record_json(&blank_field_key).expect_err("blank field key fails");
        assert_eq!(
            field_key_error,
            CrabError::InvariantViolation {
                context: "diagnostics_record_validate",
                message: "field_key must not be empty".to_string(),
            }
        );

        let mut blank_session = build_diagnostic_record(
            1_739_281_600_303,
            DiagnosticEvent::BackendRestarted {
                backend: BackendKind::Codex,
                reason: "restart".to_string(),
                logical_session_id: Some("discord:channel:123".to_string()),
                run_id: None,
            },
        )
        .expect("seed record should build");
        blank_session.logical_session_id = Some(" ".to_string());
        let session_error =
            render_diagnostic_record_json(&blank_session).expect_err("blank session id fails");
        assert_eq!(
            session_error,
            CrabError::InvariantViolation {
                context: "diagnostics_correlation_validate",
                message: "logical_session_id must not be empty".to_string(),
            }
        );

        let mut blank_run = build_diagnostic_record(
            1_739_281_600_304,
            DiagnosticEvent::BackendRestarted {
                backend: BackendKind::Codex,
                reason: "restart".to_string(),
                logical_session_id: Some("discord:channel:123".to_string()),
                run_id: Some("run-16".to_string()),
            },
        )
        .expect("seed record should build");
        blank_run.run_id = Some(" ".to_string());
        let run_error = render_diagnostic_record_json(&blank_run).expect_err("blank run id fails");
        assert_eq!(
            run_error,
            CrabError::InvariantViolation {
                context: "diagnostics_correlation_validate",
                message: "run_id must not be empty".to_string(),
            }
        );

        let mut blank_physical = build_diagnostic_record(
            1_739_281_600_305,
            DiagnosticEvent::BackendRestarted {
                backend: BackendKind::Codex,
                reason: "restart".to_string(),
                logical_session_id: Some("discord:channel:123".to_string()),
                run_id: None,
            },
        )
        .expect("seed record should build");
        blank_physical.physical_session_id = Some(" ".to_string());
        let physical_error =
            render_diagnostic_record_json(&blank_physical).expect_err("blank physical id fails");
        assert_eq!(
            physical_error,
            CrabError::InvariantViolation {
                context: "diagnostics_correlation_validate",
                message: "physical_session_id must not be empty".to_string(),
            }
        );
    }
}

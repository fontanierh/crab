use std::{path::Path, sync::Arc, time::Duration};

use agent_host_implementation::{
    AcpEventDirection, AcpEventKind, AgentHost, AgentHostError, AgentInputMode, AgentLifecycle,
    AgentProtocol, AgentSteeringExtension, AuthorityAttestation, AuthorityProbeConfig,
    AuthorityVerifier, CRAB_AGENT_ID_ENV, CRAB_PARENT_SESSION_ID_ENV, CRAB_SESSION_ID_ENV,
    CRAB_STATE_DIRECTORY_ENV, CRAB_SUB_AGENT_ID_ENV, CRAB_WORKING_DIRECTORY_ENV, ConfiguredAgent,
    ConfiguredMcpServer, DetachSessionsRequest, DiscoverAgentsRequest, FilesystemAuthority,
    NetworkAuthority, OpenSessionRequest, PermissionAuthority, PermissionRequest, PreflightRequest,
    PromptDisposition, PromptRequest, ReadEventsRequest, ResumeSessionRequest, RootAuthority,
    RunReference, SandboxAuthority, SessionReference, SteeringSupport, generated,
};
use async_trait::async_trait;
use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};
use boxology_runtime::CompositionBuilder;
use serde_json::Value;

struct FixtureAuthority;

#[async_trait]
impl AuthorityVerifier for FixtureAuthority {
    async fn verify(
        &self,
        _agent: &ConfiguredAgent,
        working_directory: &Path,
        now_ms: u64,
    ) -> Result<AuthorityAttestation, AgentHostError> {
        if !working_directory.is_absolute() {
            return Err(AgentHostError::PreflightFailed);
        }
        Ok(AuthorityAttestation {
            sandbox: SandboxAuthority::DisabledAndVerified,
            permissions: PermissionAuthority::YoloAndVerified,
            filesystem: FilesystemAuthority::UnrestrictedAndVerified,
            network: NetworkAuthority::UnrestrictedAndVerified,
            root: RootAuthority::PasswordlessSudoAndVerified,
            verified_at_ms: now_ms,
            evidence_json: r#"{"fixture":true}"#.into(),
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

fn configured_agent(protocol: AgentProtocol) -> ConfiguredAgent {
    let executable = env!("CARGO_BIN_EXE_acp_fixture");
    let protocol_argument = match protocol {
        AgentProtocol::V1 => "v1",
        AgentProtocol::V2 => "v2",
    };
    ConfiguredAgent::new(
        format!("fixture-{protocol_argument}"),
        format!("Fixture {protocol_argument}"),
        executable,
        protocol,
        AuthorityProbeConfig::new(executable),
    )
    .arguments([protocol_argument])
    .environment([("FIXTURE_SECRET", "not-exposed")])
    .session_options([("mode", "bypassPermissions"), ("model", "opus")])
}

fn configured_agent_with_mcp(protocol: AgentProtocol) -> ConfiguredAgent {
    configured_agent(protocol).session_mcp_servers([ConfiguredMcpServer::new(
        "crab-sub-agents",
        env!("CARGO_BIN_EXE_acp_fixture"),
    )
    .arguments(["mcp"])
    .environment([("MCP_MARKER", "visible")])])
}

fn configured_steering_agent() -> ConfiguredAgent {
    configured_agent(AgentProtocol::V1)
        .environment([
            ("FIXTURE_SECRET", "not-exposed"),
            ("ACP_FIXTURE_STEERING", "1"),
        ])
        .steering_extension(AgentSteeringExtension::SessionSteeringV1)
}

fn prompt(session_id: &str, turn: &str, mode: AgentInputMode, text: &str) -> PromptRequest {
    PromptRequest {
        session_id: session_id.into(),
        client_turn_id: turn.into(),
        mode,
        native_prompt_json: serde_json::to_string(&serde_json::json!([
            { "type": "text", "text": text }
        ]))
        .expect("prompt JSON serializes"),
    }
}

async fn open_fixture(
    host: &AgentHost,
    protocol: AgentProtocol,
    working_directory: &Path,
) -> agent_host_implementation::AgentSession {
    let agent_id = match protocol {
        AgentProtocol::V1 => "fixture-v1",
        AgentProtocol::V2 => "fixture-v2",
    };
    host.open_session(
        context(),
        OpenSessionRequest {
            agent_id: agent_id.into(),
            working_directory: working_directory.to_string_lossy().into_owned(),
            bootstrap_prompt: None,
            metadata_json: "{}".into(),
        },
    )
    .await
    .expect("real ACP subprocess opens")
}

#[tokio::test]
async fn prompt_errors_keep_the_native_error_and_emit_one_terminal_event() {
    for protocol in [AgentProtocol::V1, AgentProtocol::V2] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let host = AgentHost::open_with_authority_verifier(
            directory.path().join("agent-host.sqlite"),
            vec![configured_agent(protocol)],
            Arc::new(FixtureAuthority),
        )
        .expect("host opens");
        let session = open_fixture(&host, protocol, directory.path()).await;
        let failed = host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "prompt-error",
                    AgentInputMode::Queue,
                    "error",
                ),
            )
            .await
            .expect("the run is accepted before the ACP response arrives");
        wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;

        let page = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("failed run events remain readable");
        let run_events = page
            .events
            .iter()
            .filter(|event| event.run_id.as_deref() == Some(failed.run_id.as_str()))
            .collect::<Vec<_>>();
        let native_error = run_events
            .iter()
            .find(|event| {
                serde_json::from_str::<Value>(&event.native_event_json)
                    .ok()
                    .and_then(|message| message.pointer("/error/code").cloned())
                    == Some(serde_json::json!(-32001))
            })
            .expect("the native JSON-RPC error is preserved verbatim");
        assert!(
            native_error
                .native_event_json
                .contains("fixture prompt failed")
        );
        let terminal = run_events
            .iter()
            .filter(|event| event.kind == AcpEventKind::RunFinished)
            .collect::<Vec<_>>();
        assert_eq!(terminal.len(), 1, "every failed run has one terminal event");
        assert!(terminal[0].sequence > native_error.sequence);
        assert_eq!(terminal[0].direction, AcpEventDirection::AgentToClient);
        let terminal_message: Value = serde_json::from_str(&terminal[0].native_event_json)
            .expect("Crab terminal event is valid JSON-RPC");
        assert_eq!(terminal_message["method"], "crab/run_finished");
        assert_eq!(terminal_message["params"]["sessionId"], session.session_id);
        assert_eq!(terminal_message["params"]["runId"], failed.run_id);
        assert_eq!(terminal_message["params"]["outcome"], "failed");
        assert_eq!(
            host.session_status(
                context(),
                SessionReference {
                    session_id: session.session_id.clone(),
                },
            )
            .await
            .expect("session status remains queryable")
            .active_run_id,
            None
        );

        let recovered = host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "after-error",
                    AgentInputMode::Queue,
                    "recovered",
                ),
            )
            .await
            .expect("the same session accepts later work");
        wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;
        let recovered_page = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: page.next_sequence,
                    limit: 1_000,
                },
            )
            .await
            .expect("later native events are readable");
        let recovered_terminals = recovered_page
            .events
            .iter()
            .filter(|event| {
                event.run_id.as_deref() == Some(recovered.run_id.as_str())
                    && event.kind == AcpEventKind::RunFinished
            })
            .collect::<Vec<_>>();
        assert_eq!(
            recovered_terminals.len(),
            1,
            "native completion is not duplicated by the fallback"
        );
        assert!(
            !recovered_terminals[0]
                .native_event_json
                .contains("crab/run_finished")
        );
        host.shutdown().await;
    }
}

#[tokio::test]
async fn required_session_options_fail_closed_for_both_acp_profiles() {
    for protocol in [AgentProtocol::V1, AgentProtocol::V2] {
        for failure in ["rewrite", "missing", "unsupported"] {
            let directory = tempfile::tempdir().expect("temporary directory");
            let mut agent = configured_agent(protocol);
            match failure {
                "rewrite" => {
                    agent
                        .environment
                        .insert("ACP_FIXTURE_REWRITE_OPTION".into(), "mode".into());
                }
                "missing" => {
                    agent
                        .environment
                        .insert("ACP_FIXTURE_DROP_OPTION".into(), "mode".into());
                }
                "unsupported" => {
                    agent.session_options = [("unknown".into(), "value".into())].into();
                }
                _ => unreachable!(),
            }
            let host = AgentHost::open_with_authority_verifier(
                directory.path().join("agent-host.sqlite"),
                vec![agent],
                Arc::new(FixtureAuthority),
            )
            .expect("host opens");

            assert_eq!(
                host.open_session(
                    context(),
                    OpenSessionRequest {
                        agent_id: match protocol {
                            AgentProtocol::V1 => "fixture-v1",
                            AgentProtocol::V2 => "fixture-v2",
                        }
                        .into(),
                        working_directory: directory.path().to_string_lossy().into_owned(),
                        bootstrap_prompt: None,
                        metadata_json: "{}".into(),
                    },
                )
                .await,
                Err(AgentHostError::ProtocolNegotiationFailed),
                "{protocol:?} accepted {failure} session policy"
            );
        }
    }
}

#[tokio::test]
async fn every_acp_session_receives_the_configured_stdio_mcp_and_crab_context() {
    for protocol in [AgentProtocol::V1, AgentProtocol::V2] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let agent = configured_agent_with_mcp(protocol);
        let expected_agent_id = agent.agent_id.clone();
        let host = AgentHost::open_with_authority_verifier(
            directory.path().join("agent-host.sqlite"),
            vec![agent],
            Arc::new(FixtureAuthority),
        )
        .expect("host opens");
        let catalog = host
            .discover_agents(context(), DiscoverAgentsRequest {})
            .await
            .expect("catalog is available");
        assert_eq!(catalog.agents[0].mcp_server_names, ["crab-sub-agents"]);

        let session = host
            .open_session(
                context(),
                OpenSessionRequest {
                    agent_id: expected_agent_id.clone(),
                    working_directory: directory.path().to_string_lossy().into_owned(),
                    bootstrap_prompt: None,
                    metadata_json: serde_json::json!({
                        "crabSubAgent": {
                            "subAgentId": "sub_fixture",
                            "parentSessionId": "session_parent",
                            "contextMode": "fresh"
                        }
                    })
                    .to_string(),
                },
            )
            .await
            .expect("MCP-enabled session opens");
        let canonical_directory =
            std::fs::canonicalize(directory.path()).expect("temporary directory canonicalizes");
        let events = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: 0,
                    limit: 100,
                },
            )
            .await
            .expect("session setup events are readable");
        let request = events
            .events
            .iter()
            .filter(|event| event.direction == AcpEventDirection::ClientToAgent)
            .filter_map(|event| serde_json::from_str::<Value>(&event.native_event_json).ok())
            .find(|event| event["method"] == "session/new")
            .expect("session/new is preserved");
        let server = &request["params"]["mcpServers"][0];
        assert_eq!(server["name"], "crab-sub-agents");
        assert_eq!(server["command"], env!("CARGO_BIN_EXE_acp_fixture"));
        assert_eq!(server["args"], serde_json::json!(["mcp"]));
        let environment = server["env"]
            .as_array()
            .expect("stdio MCP environment")
            .iter()
            .map(|entry| {
                (
                    entry["name"].as_str().expect("environment name"),
                    entry["value"].as_str().expect("environment value"),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(environment["MCP_MARKER"], "visible");
        assert_eq!(
            environment[CRAB_STATE_DIRECTORY_ENV],
            canonical_directory.to_string_lossy()
        );
        assert_eq!(environment[CRAB_SESSION_ID_ENV], session.session_id);
        assert_eq!(environment[CRAB_AGENT_ID_ENV], expected_agent_id);
        assert_eq!(
            environment[CRAB_WORKING_DIRECTORY_ENV],
            canonical_directory.to_string_lossy()
        );
        assert_eq!(environment[CRAB_SUB_AGENT_ID_ENV], "sub_fixture");
        assert_eq!(environment[CRAB_PARENT_SESSION_ID_ENV], "session_parent");
        host.shutdown().await;
    }
}

#[tokio::test]
async fn v2_fails_closed_when_the_agent_does_not_advertise_stdio_mcp() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut agent = configured_agent_with_mcp(AgentProtocol::V2);
    agent
        .environment
        .insert("ACP_FIXTURE_HIDE_STDIO_MCP".into(), "1".into());
    let host = AgentHost::open_with_authority_verifier(
        directory.path().join("agent-host.sqlite"),
        vec![agent],
        Arc::new(FixtureAuthority),
    )
    .expect("host opens");

    assert_eq!(
        host.open_session(
            context(),
            OpenSessionRequest {
                agent_id: "fixture-v2".into(),
                working_directory: directory.path().to_string_lossy().into_owned(),
                bootstrap_prompt: None,
                metadata_json: "{}".into(),
            },
        )
        .await,
        Err(AgentHostError::ProtocolNegotiationFailed)
    );
}

async fn wait_for_lifecycle(host: &AgentHost, session_id: &str, expected: AgentLifecycle) {
    for _ in 0..200 {
        let status = host
            .session_status(
                context(),
                SessionReference {
                    session_id: session_id.into(),
                },
            )
            .await
            .expect("session status is readable");
        if status.lifecycle == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("session never reached {expected:?}");
}

async fn wait_for_event(host: &AgentHost, session_id: &str, needle: &str) {
    for _ in 0..200 {
        let events = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session_id.into(),
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("session events are readable");
        if events
            .events
            .iter()
            .any(|event| event.native_event_json.contains(needle))
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("session never emitted {needle}");
}

#[tokio::test]
async fn interrupting_input_is_durable_before_cancellation_for_both_acp_profiles() {
    for protocol in [AgentProtocol::V1, AgentProtocol::V2] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let host = AgentHost::open_with_authority_verifier(
            directory.path().join("agent-host.sqlite"),
            vec![configured_agent(protocol)],
            Arc::new(FixtureAuthority),
        )
        .expect("host opens");
        let session = open_fixture(&host, protocol, directory.path()).await;
        let held = host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "interrupt-held",
                    AgentInputMode::Queue,
                    "hold",
                ),
            )
            .await
            .expect("active work starts");
        let interrupting_request = prompt(
            &session.session_id,
            "interrupt-urgent",
            AgentInputMode::InterruptAndQueue,
            "urgent",
        );
        let accepted = host
            .prompt(context(), interrupting_request.clone())
            .await
            .expect("interrupting input is accepted");
        assert_eq!(
            accepted.disposition,
            PromptDisposition::CancelRequestedThenQueued
        );
        assert_eq!(accepted.interrupted_run_id.as_deref(), Some(&*held.run_id));
        assert!(accepted.cancel_requested_at_ms.is_some());
        assert_eq!(
            host.prompt(context(), interrupting_request.clone())
                .await
                .expect("an exact retry is stable"),
            accepted
        );
        let mut conflicting_retry = interrupting_request;
        conflicting_retry.mode = AgentInputMode::Queue;
        assert_eq!(
            host.prompt(context(), conflicting_retry).await,
            Err(AgentHostError::DuplicateTurnConflict)
        );

        wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;
        let page = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("the interruption journal is readable");
        let client_messages = page
            .events
            .iter()
            .filter(|event| event.direction == AcpEventDirection::ClientToAgent)
            .filter_map(|event| {
                serde_json::from_str::<Value>(&event.native_event_json)
                    .ok()
                    .map(|message| (event.sequence, message))
            })
            .collect::<Vec<_>>();
        let cancel_sequences = client_messages
            .iter()
            .filter(|(_, message)| message["method"] == "session/cancel")
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            cancel_sequences.len(),
            1,
            "an exact retry does not recancel"
        );
        let urgent_sequence = client_messages
            .iter()
            .find(|(_, message)| {
                message["method"] == "session/prompt"
                    && message.pointer("/params/prompt/0/text") == Some(&Value::from("urgent"))
            })
            .map(|(sequence, _)| *sequence)
            .expect("the urgent prompt is dispatched");
        assert!(cancel_sequences[0] < urgent_sequence);
        assert!(page.events.iter().any(|event| {
            event.kind == AcpEventKind::Message && event.native_event_json.contains("echo:urgent")
        }));
        for run_id in [&held.run_id, &accepted.run_id] {
            assert_eq!(
                page.events
                    .iter()
                    .filter(|event| {
                        event.run_id.as_deref() == Some(run_id.as_str())
                            && event.kind == AcpEventKind::RunFinished
                    })
                    .count(),
                1,
                "interrupted and urgent runs each finish exactly once"
            );
        }
        host.shutdown().await;
    }
}

#[tokio::test]
async fn failed_v1_and_v2_sessions_resume_native_identity_without_bootstrap_replay() {
    for protocol in [AgentProtocol::V1, AgentProtocol::V2] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("agent-host.sqlite");
        let agent = configured_agent_with_mcp(protocol);
        let agent_id = agent.agent_id.clone();
        let host = AgentHost::open_with_authority_verifier(
            &database,
            vec![agent.clone()],
            Arc::new(FixtureAuthority),
        )
        .expect("host opens");
        let session = host
            .open_session(
                context(),
                OpenSessionRequest {
                    agent_id,
                    working_directory: directory.path().to_string_lossy().into_owned(),
                    bootstrap_prompt: Some("bootstrap-once".into()),
                    metadata_json: r#"{"marker":"durable"}"#.into(),
                },
            )
            .await
            .expect("session opens");
        wait_for_event(&host, &session.session_id, "bootstrap-once").await;
        wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;

        let crashed = host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "crash-turn",
                    AgentInputMode::Queue,
                    "crash",
                ),
            )
            .await
            .expect("crashing prompt is accepted first");
        wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Failed).await;
        let failed_events = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("failed session events remain readable");
        let failed_cursor = failed_events.next_sequence;
        let crash_terminals = failed_events
            .events
            .iter()
            .filter(|event| {
                event.run_id.as_deref() == Some(crashed.run_id.as_str())
                    && event.kind == AcpEventKind::RunFinished
            })
            .collect::<Vec<_>>();
        assert_eq!(crash_terminals.len(), 1);
        assert!(
            crash_terminals[0]
                .native_event_json
                .contains("crab/run_finished")
        );
        drop(host);

        let resumed_host = AgentHost::open_with_authority_verifier(
            &database,
            vec![agent],
            Arc::new(FixtureAuthority),
        )
        .expect("host reopens");
        let resumed = resumed_host
            .resume_session(
                context(),
                ResumeSessionRequest {
                    session_id: session.session_id.clone(),
                },
            )
            .await
            .expect("native ACP session resumes");
        assert_eq!(resumed.session_id, session.session_id);
        assert_eq!(resumed.native_session_id, session.native_session_id);

        let recovery_events = resumed_host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: failed_cursor,
                    limit: 1_000,
                },
            )
            .await
            .expect("recovery events are readable");
        assert!(recovery_events.caught_up);
        assert!(
            recovery_events
                .events
                .windows(2)
                .all(|pair| pair[1].sequence == pair[0].sequence + 1)
        );
        let resume_request = recovery_events
            .events
            .iter()
            .filter(|event| event.direction == AcpEventDirection::ClientToAgent)
            .filter_map(|event| serde_json::from_str::<Value>(&event.native_event_json).ok())
            .find(|event| event["method"] == "session/resume")
            .expect("native resume request is preserved");
        assert_eq!(
            resume_request["params"]["sessionId"],
            session.native_session_id
        );
        assert_eq!(resume_request["params"]["_meta"]["marker"], "durable");
        assert_eq!(
            resume_request["params"]["mcpServers"][0]["name"],
            "crab-sub-agents"
        );
        assert!(!recovery_events.events.iter().any(|event| {
            event.native_event_json.contains("session/new")
                || event.native_event_json.contains("bootstrap-once")
        }));

        assert_eq!(
            resumed_host
                .resume_session(
                    context(),
                    ResumeSessionRequest {
                        session_id: session.session_id.clone(),
                    },
                )
                .await,
            Err(AgentHostError::SessionResumeUnavailable)
        );
        resumed_host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "after-resume",
                    AgentInputMode::Queue,
                    "after-resume",
                ),
            )
            .await
            .expect("resumed session accepts new work");
        wait_for_event(&resumed_host, &session.session_id, "echo:after-resume").await;
        resumed_host
            .close_session(
                context(),
                SessionReference {
                    session_id: session.session_id.clone(),
                },
            )
            .await
            .expect("resumed session closes");
        assert_eq!(
            resumed_host
                .resume_session(
                    context(),
                    ResumeSessionRequest {
                        session_id: session.session_id,
                    },
                )
                .await,
            Err(AgentHostError::SessionResumeUnavailable)
        );
    }
}

#[tokio::test]
async fn graceful_detach_preserves_v1_and_v2_native_sessions_for_exact_resume() {
    for protocol in [AgentProtocol::V1, AgentProtocol::V2] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = directory.path().join("agent-host.sqlite");
        let agent = configured_agent(protocol);
        let host = AgentHost::open_with_authority_verifier(
            &database,
            vec![agent.clone()],
            Arc::new(FixtureAuthority),
        )
        .expect("host opens");
        let session = open_fixture(&host, protocol, directory.path()).await;
        let held = host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "detach-held",
                    AgentInputMode::Queue,
                    "hold",
                ),
            )
            .await
            .expect("active work starts");
        let queued = host
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "detach-queued",
                    AgentInputMode::Queue,
                    "must-not-replay",
                ),
            )
            .await
            .expect("later work queues");

        let report = host
            .detach_sessions(context(), DetachSessionsRequest {})
            .await
            .expect("host-wide detach completes");
        assert_eq!(
            report.detached_session_ids.as_slice(),
            std::slice::from_ref(&session.session_id)
        );
        assert!(report.failed_session_ids.is_empty());
        wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Detached).await;
        let before_resume = host
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("detached journal remains readable");
        assert!(
            before_resume
                .events
                .iter()
                .any(|event| event.native_event_json.contains("session/cancel"))
        );
        assert!(
            !before_resume
                .events
                .iter()
                .any(|event| event.native_event_json.contains("session/close"))
        );
        for run_id in [&held.run_id, &queued.run_id] {
            assert_eq!(
                before_resume
                    .events
                    .iter()
                    .filter(|event| {
                        event.run_id.as_deref() == Some(run_id.as_str())
                            && event.kind == AcpEventKind::RunFinished
                    })
                    .count(),
                1,
                "active and queued runs each terminate exactly once"
            );
        }
        assert!(before_resume.events.iter().any(|event| {
            event.run_id.as_deref() == Some(queued.run_id.as_str())
                && event.native_event_json.contains("crab/run_finished")
        }));
        assert_eq!(
            host.prompt(
                context(),
                prompt(
                    &session.session_id,
                    "while-detached",
                    AgentInputMode::Queue,
                    "no transport",
                ),
            )
            .await,
            Err(AgentHostError::SessionClosed)
        );
        drop(host);

        let restarted = AgentHost::open_with_authority_verifier(
            &database,
            vec![agent],
            Arc::new(FixtureAuthority),
        )
        .expect("host reopens");
        let resumed = restarted
            .resume_session(
                context(),
                ResumeSessionRequest {
                    session_id: session.session_id.clone(),
                },
            )
            .await
            .expect("detached native session resumes");
        assert_eq!(resumed.session_id, session.session_id);
        assert_eq!(resumed.native_session_id, session.native_session_id);
        let after_resume = restarted
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id.clone(),
                    after_sequence: before_resume.next_sequence,
                    limit: 1_000,
                },
            )
            .await
            .expect("resume journal is readable");
        assert!(after_resume.events.iter().any(|event| {
            event.direction == AcpEventDirection::ClientToAgent
                && event.native_event_json.contains("session/resume")
        }));
        assert!(
            !after_resume
                .events
                .iter()
                .any(|event| event.native_event_json.contains("must-not-replay"))
        );
        assert_eq!(held.session_id, resumed.session_id);

        restarted
            .close_session(
                context(),
                SessionReference {
                    session_id: session.session_id.clone(),
                },
            )
            .await
            .expect("explicit close remains available");
        let closed = restarted
            .read_events(
                context(),
                ReadEventsRequest {
                    session_id: session.session_id,
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("closed journal remains readable");
        assert!(
            closed
                .events
                .iter()
                .any(|event| event.native_event_json.contains("session/close"))
        );
    }
}

#[tokio::test]
async fn v1_resume_fails_closed_when_the_restarted_agent_withholds_support() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("agent-host.sqlite");
    let agent = configured_agent(AgentProtocol::V1);
    let host =
        AgentHost::open_with_authority_verifier(&database, vec![agent], Arc::new(FixtureAuthority))
            .expect("host opens");
    let session = open_fixture(&host, AgentProtocol::V1, directory.path()).await;
    host.prompt(
        context(),
        prompt(
            &session.session_id,
            "crash-unsupported",
            AgentInputMode::Queue,
            "crash",
        ),
    )
    .await
    .expect("crashing prompt is accepted");
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Failed).await;
    drop(host);

    let mut unsupported = configured_agent(AgentProtocol::V1);
    unsupported
        .environment
        .insert("ACP_FIXTURE_HIDE_RESUME".into(), "1".into());
    let restarted = AgentHost::open_with_authority_verifier(
        &database,
        vec![unsupported],
        Arc::new(FixtureAuthority),
    )
    .expect("host reopens");
    assert_eq!(
        restarted
            .resume_session(
                context(),
                ResumeSessionRequest {
                    session_id: session.session_id.clone(),
                },
            )
            .await,
        Err(AgentHostError::SessionResumeUnavailable)
    );
    wait_for_lifecycle(&restarted, &session.session_id, AgentLifecycle::Failed).await;
    assert_eq!(
        restarted
            .resume_session(
                context(),
                ResumeSessionRequest {
                    session_id: "missing".into(),
                },
            )
            .await,
        Err(AgentHostError::UnknownSession)
    );
    drop(restarted);

    let supported = AgentHost::open_with_authority_verifier(
        &database,
        vec![configured_agent(AgentProtocol::V1)],
        Arc::new(FixtureAuthority),
    )
    .expect("host reopens with resume support restored");
    supported
        .resume_session(
            context(),
            ResumeSessionRequest {
                session_id: session.session_id,
            },
        )
        .await
        .expect("failed capability negotiation remains recoverable");
    supported.shutdown().await;
}

#[tokio::test]
async fn v1_queue_cancel_permission_and_restart_are_end_to_end() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("agent-host.sqlite");
    let agent = configured_agent(AgentProtocol::V1);
    let host = AgentHost::open_with_authority_verifier(
        &database,
        vec![agent.clone()],
        Arc::new(FixtureAuthority),
    )
    .expect("host opens");

    let catalog = host
        .discover_agents(context(), DiscoverAgentsRequest {})
        .await
        .expect("catalog is available");
    assert_eq!(catalog.agents.len(), 1);
    assert_eq!(catalog.agents[0].environment_names, ["FIXTURE_SECRET"]);
    assert!(!format!("{:?}", catalog.agents[0]).contains("not-exposed"));
    host.preflight(
        context(),
        PreflightRequest {
            agent_id: "fixture-v1".into(),
            working_directory: directory.path().to_string_lossy().into_owned(),
        },
    )
    .await
    .expect("mandatory preflight runs");

    let session = open_fixture(&host, AgentProtocol::V1, directory.path()).await;
    assert_eq!(session.native_session_id, "fixture-native-session");
    let idle_steer = host
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "v1-idle-steer",
                AgentInputMode::Steer,
                "ordinary idle turn",
            ),
        )
        .await
        .expect("steer on an idle v1 session is an ordinary prompt");
    assert_eq!(
        idle_steer.disposition,
        PromptDisposition::StartedForegroundWork
    );
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;
    let held = host
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "turn-hold",
                AgentInputMode::Queue,
                "hold",
            ),
        )
        .await
        .expect("first prompt starts");
    assert_eq!(held.disposition, PromptDisposition::StartedForegroundWork);
    assert_eq!(
        host.prompt(
            context(),
            prompt(
                &session.session_id,
                "v1-steer",
                AgentInputMode::Steer,
                "unsupported",
            ),
        )
        .await,
        Err(AgentHostError::SteeringUnavailable)
    );
    let queued_request = prompt(
        &session.session_id,
        "turn-queued",
        AgentInputMode::Queue,
        "queued",
    );
    let queued = host
        .prompt(context(), queued_request.clone())
        .await
        .expect("second prompt queues without blocking");
    assert_eq!(queued.disposition, PromptDisposition::QueuedForTurnBoundary);
    assert_ne!(queued.run_id, held.run_id);
    assert_eq!(
        host.prompt(context(), queued_request.clone())
            .await
            .expect("identical retry deduplicates"),
        queued
    );
    let mut conflict = queued_request;
    conflict.native_prompt_json = prompt(
        &session.session_id,
        "unused",
        AgentInputMode::Queue,
        "different",
    )
    .native_prompt_json;
    assert_eq!(
        host.prompt(context(), conflict).await,
        Err(AgentHostError::DuplicateTurnConflict)
    );

    host.cancel_run(
        context(),
        RunReference {
            session_id: session.session_id.clone(),
            run_id: held.run_id,
        },
    )
    .await
    .expect("active ACP work cancels cooperatively");
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;

    host.prompt(
        context(),
        prompt(
            &session.session_id,
            "turn-permission",
            AgentInputMode::Queue,
            "permission",
        ),
    )
    .await
    .expect("permission turn starts");
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;

    let page = host
        .read_events(
            context(),
            ReadEventsRequest {
                session_id: session.session_id.clone(),
                after_sequence: 0,
                limit: 1_000,
            },
        )
        .await
        .expect("full native event stream is readable");
    assert!(page.caught_up);
    assert!(
        page.events
            .windows(2)
            .all(|pair| pair[1].sequence == pair[0].sequence + 1)
    );
    assert!(page.events.iter().any(|event| {
        event.direction == AcpEventDirection::ClientToAgent
            && event.native_event_json.contains("session/prompt")
    }));
    assert!(page.events.iter().any(|event| {
        event.direction == AcpEventDirection::AgentToClient
            && event.kind == AcpEventKind::Message
            && event.native_event_json.contains("echo:queued")
    }));
    assert!(
        page.events
            .iter()
            .any(|event| event.kind == AcpEventKind::RunFinished)
    );
    let permission_event = page
        .events
        .iter()
        .find(|event| event.kind == AcpEventKind::PermissionRequest)
        .expect("permission request is preserved");
    let permission_message: Value =
        serde_json::from_str(&permission_event.native_event_json).expect("valid JSON-RPC");
    let request_id = permission_message["id"]
        .as_str()
        .expect("string request id")
        .to_owned();
    let resolution = host
        .resolve_permission(
            context(),
            PermissionRequest {
                session_id: session.session_id.clone(),
                request_id,
                native_request_json: permission_event.native_event_json.clone(),
            },
        )
        .await
        .expect("automatic unrestricted resolution is queryable");
    assert!(resolution.native_response_json.contains("allow-always"));

    host.close_session(
        context(),
        SessionReference {
            session_id: session.session_id.clone(),
        },
    )
    .await
    .expect("session closes");
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Stopped).await;
    drop(host);

    let restarted =
        AgentHost::open_with_authority_verifier(&database, vec![agent], Arc::new(FixtureAuthority))
            .expect("host reopens same state");
    let persisted = restarted
        .read_events(
            context(),
            ReadEventsRequest {
                session_id: session.session_id.clone(),
                after_sequence: 0,
                limit: 1_000,
            },
        )
        .await
        .expect("events survive restart");
    assert!(persisted.events.len() >= page.events.len());
    assert_eq!(
        &persisted.events[..page.events.len()],
        page.events.as_slice()
    );
    assert_eq!(
        restarted
            .prompt(
                context(),
                prompt(
                    &session.session_id,
                    "after-close",
                    AgentInputMode::Queue,
                    "nope",
                ),
            )
            .await,
        Err(AgentHostError::SessionClosed)
    );
}

#[tokio::test]
async fn negotiated_v1_extension_steers_the_active_run() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let host = AgentHost::open_with_authority_verifier(
        directory.path().join("agent-host.sqlite"),
        vec![configured_steering_agent()],
        Arc::new(FixtureAuthority),
    )
    .expect("host opens");
    let session = open_fixture(&host, AgentProtocol::V1, directory.path()).await;
    assert_eq!(
        session.negotiation.steering,
        SteeringSupport::AgentExtension
    );

    let held = host
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "extension-hold",
                AgentInputMode::Queue,
                "hold",
            ),
        )
        .await
        .expect("foreground work starts");
    let steer_request = prompt(
        &session.session_id,
        "extension-steer",
        AgentInputMode::Steer,
        "follow up now",
    );
    let steered = host
        .prompt(context(), steer_request.clone())
        .await
        .expect("extension contributes immediately");
    assert_eq!(steered.run_id, held.run_id);
    assert_eq!(
        steered.disposition,
        PromptDisposition::ContributedToActiveWork
    );
    assert_eq!(
        host.prompt(context(), steer_request)
            .await
            .expect("identical retry deduplicates"),
        steered
    );

    let page = host
        .read_events(
            context(),
            ReadEventsRequest {
                session_id: session.session_id.clone(),
                after_sequence: 0,
                limit: 1_000,
            },
        )
        .await
        .expect("native extension events are readable");
    assert!(page.events.iter().any(|event| {
        event.direction == AcpEventDirection::ClientToAgent
            && event.native_event_json.contains("_session/steering")
            && event.native_event_json.contains("promptRequired")
    }));
    assert_eq!(
        page.events
            .iter()
            .filter(|event| {
                event.direction == AcpEventDirection::ClientToAgent
                    && event.native_event_json.contains("_session/steering")
            })
            .count(),
        1,
        "the durable retry does not reinject"
    );
    assert!(page.events.iter().any(|event| {
        event.direction == AcpEventDirection::AgentToClient
            && event.native_event_json.contains("steered:follow up now")
    }));

    host.cancel_run(
        context(),
        RunReference {
            session_id: session.session_id.clone(),
            run_id: held.run_id,
        },
    )
    .await
    .expect("active turn cancels");
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;
    host.shutdown().await;
}

#[tokio::test]
async fn v1_steering_negotiation_and_idle_fallback_fail_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let unadvertised = configured_agent(AgentProtocol::V1)
        .steering_extension(AgentSteeringExtension::SessionSteeringV1);
    let host = AgentHost::open_with_authority_verifier(
        directory.path().join("unadvertised.sqlite"),
        vec![unadvertised],
        Arc::new(FixtureAuthority),
    )
    .expect("host opens");
    assert_eq!(
        host.open_session(
            context(),
            OpenSessionRequest {
                agent_id: "fixture-v1".into(),
                working_directory: directory.path().to_string_lossy().into_owned(),
                bootstrap_prompt: None,
                metadata_json: "{}".into(),
            },
        )
        .await,
        Err(AgentHostError::ProtocolNegotiationFailed)
    );

    let fallback_agent = configured_steering_agent().environment([
        ("FIXTURE_SECRET", "not-exposed"),
        ("ACP_FIXTURE_STEERING", "1"),
        ("ACP_FIXTURE_STEERING_OUTCOME", "promptRequired"),
    ]);
    let fallback = AgentHost::open_with_authority_verifier(
        directory.path().join("fallback.sqlite"),
        vec![fallback_agent],
        Arc::new(FixtureAuthority),
    )
    .expect("fallback host opens");
    let session = open_fixture(&fallback, AgentProtocol::V1, directory.path()).await;
    let held = fallback
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "fallback-hold",
                AgentInputMode::Queue,
                "hold",
            ),
        )
        .await
        .expect("foreground work starts");
    let continued = fallback
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "fallback-steer",
                AgentInputMode::Steer,
                "continue through prompt",
            ),
        )
        .await
        .expect("idle extension result falls back through session/prompt");
    assert_ne!(continued.run_id, held.run_id);
    assert_eq!(
        continued.disposition,
        PromptDisposition::StartedForegroundWork
    );
    wait_for_lifecycle(&fallback, &session.session_id, AgentLifecycle::Ready).await;
    let page = fallback
        .read_events(
            context(),
            ReadEventsRequest {
                session_id: session.session_id.clone(),
                after_sequence: 0,
                limit: 1_000,
            },
        )
        .await
        .expect("fallback lifecycle is readable");
    assert!(page.events.iter().any(|event| {
        event.run_id.as_deref() == Some(continued.run_id.as_str())
            && event
                .native_event_json
                .contains("echo:continue through prompt")
    }));
    for run_id in [&held.run_id, &continued.run_id] {
        assert_eq!(
            page.events
                .iter()
                .filter(|event| {
                    event.run_id.as_deref() == Some(run_id.as_str())
                        && event.kind == AcpEventKind::RunFinished
                })
                .count(),
            1
        );
    }
    fallback.shutdown().await;
}

#[tokio::test]
async fn v2_steers_active_work_while_queue_waits_for_idle() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let host = AgentHost::open_with_authority_verifier(
        directory.path().join("agent-host.sqlite"),
        vec![configured_agent(AgentProtocol::V2)],
        Arc::new(FixtureAuthority),
    )
    .expect("host opens");
    let session = open_fixture(&host, AgentProtocol::V2, directory.path()).await;
    let held = host
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "v2-hold",
                AgentInputMode::Queue,
                "hold",
            ),
        )
        .await
        .expect("v2 work starts");
    let steered = host
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "v2-steer",
                AgentInputMode::Steer,
                "steer",
            ),
        )
        .await
        .expect("v2 prompt contributes immediately");
    assert_eq!(
        steered.disposition,
        PromptDisposition::ContributedToActiveWork
    );
    assert_eq!(steered.run_id, held.run_id);
    let queued = host
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "v2-queue",
                AgentInputMode::Queue,
                "queued",
            ),
        )
        .await
        .expect("queue remains a distinct later run");
    assert_eq!(queued.disposition, PromptDisposition::QueuedForTurnBoundary);
    assert_ne!(queued.run_id, held.run_id);

    host.cancel_run(
        context(),
        RunReference {
            session_id: session.session_id.clone(),
            run_id: held.run_id,
        },
    )
    .await
    .expect("v2 cancel is sent");
    wait_for_lifecycle(&host, &session.session_id, AgentLifecycle::Ready).await;
    let page = host
        .read_events(
            context(),
            ReadEventsRequest {
                session_id: session.session_id.clone(),
                after_sequence: 0,
                limit: 1_000,
            },
        )
        .await
        .expect("events are readable");
    assert!(page.events.iter().any(|event| {
        event.native_event_json.contains("echo:steer")
            && event.direction == AcpEventDirection::AgentToClient
    }));
    assert!(page.events.iter().any(|event| {
        event.native_event_json.contains("state_update") && event.kind == AcpEventKind::RunFinished
    }));
    host.shutdown().await;
}

#[tokio::test]
async fn boxology_typed_handle_drives_the_real_acp_process() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let host = AgentHost::open_with_authority_verifier(
        directory.path().join("agent-host.sqlite"),
        vec![configured_agent(AgentProtocol::V1)],
        Arc::new(FixtureAuthority),
    )
    .expect("host opens");
    let mut builder = CompositionBuilder::new();
    let registered = generated::register(&mut builder, host);
    let handle = builder.handle::<boxology_generated_contract::AgentHostHandle>(&registered);
    let _composition = builder.start().expect("Boxology composition starts");

    let catalog = handle
        .discover_agents(context(), DiscoverAgentsRequest {})
        .await
        .expect("typed capability dispatches");
    assert_eq!(catalog.agents[0].agent_id, "fixture-v1");
    let session = handle
        .open_session(
            context(),
            OpenSessionRequest {
                agent_id: "fixture-v1".into(),
                working_directory: directory.path().to_string_lossy().into_owned(),
                bootstrap_prompt: None,
                metadata_json: "{}".into(),
            },
        )
        .await
        .expect("typed handle opens real ACP subprocess");
    handle
        .prompt(
            context(),
            prompt(
                &session.session_id,
                "typed-turn",
                AgentInputMode::Queue,
                "typed",
            ),
        )
        .await
        .expect("typed handle submits prompt");
    let mut ready = false;
    for _ in 0..200 {
        let status = handle
            .session_status(
                context(),
                SessionReference {
                    session_id: session.session_id.clone(),
                },
            )
            .await
            .expect("typed status dispatches");
        if status.lifecycle == AgentLifecycle::Ready {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(ready, "typed session never became ready");
    let events = handle
        .read_events(
            context(),
            ReadEventsRequest {
                session_id: session.session_id.clone(),
                after_sequence: 0,
                limit: 1_000,
            },
        )
        .await
        .expect("typed event read dispatches");
    assert!(
        events
            .events
            .iter()
            .any(|event| event.native_event_json.contains("echo:typed"))
    );
    handle
        .close_session(
            context(),
            SessionReference {
                session_id: session.session_id,
            },
        )
        .await
        .expect("typed close dispatches");
}

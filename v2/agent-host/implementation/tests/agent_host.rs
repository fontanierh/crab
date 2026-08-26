use std::{path::Path, sync::Arc, time::Duration};

use agent_host_implementation::{
    AcpEventDirection, AcpEventKind, AgentHost, AgentHostError, AgentInputMode, AgentLifecycle,
    AgentProtocol, AuthorityAttestation, AuthorityProbeConfig, AuthorityVerifier, ConfiguredAgent,
    DiscoverAgentsRequest, FilesystemAuthority, NetworkAuthority, OpenSessionRequest,
    PermissionAuthority, PermissionRequest, PreflightRequest, PromptDisposition, PromptRequest,
    ReadEventsRequest, RootAuthority, RunReference, SandboxAuthority, SessionReference, generated,
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

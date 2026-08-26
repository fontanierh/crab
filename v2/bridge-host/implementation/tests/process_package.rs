use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bridge_host_implementation::{
    AuthenticationMethod, BridgeAttachment, BridgeHostError, BridgeInbound, BridgeInboundSink,
    BridgeIngressMode, BridgeOutbound, BridgePackageFactory, BridgeSpec,
    ProcessBridgePackageFactory, TriggerIntent,
};

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<String>>,
}

#[async_trait]
impl BridgeInboundSink for RecordingSink {
    async fn accept(&self, request: BridgeInbound) -> Result<TriggerIntent, BridgeHostError> {
        self.events
            .lock()
            .expect("recording sink lock")
            .push(request.external_event_id.clone());
        Ok(TriggerIntent {
            source_id: request.bridge_id,
            deduplication_key: request.external_event_id,
            target_channel_id: request.target_channel_id,
            ingress_mode: BridgeIngressMode::Queue,
            message_json: request.message_json,
            attachment_handles: Vec::new(),
            trigger_id: "trigger-fixture-1".into(),
            deduplicated: false,
            recorded_at_ms: 2,
        })
    }
}

fn spec(directory: &std::path::Path) -> BridgeSpec {
    BridgeSpec {
        bridge_id: "fixture-bridge".into(),
        package_id: "fixture".into(),
        display_name: "Fixture".into(),
        launch_json: serde_json::json!({
            "executable": env!("CARGO_BIN_EXE_bridge_fixture"),
            "arguments": [],
            "workingDirectory": directory,
            "environmentNames": [],
        })
        .to_string(),
        configuration_json: "{}".into(),
        authentication_methods: vec![AuthenticationMethod::PhoneCode],
        ingress_mode: BridgeIngressMode::Queue,
        desired_running: true,
        health_interval_ms: 1_000,
        credential_validation_interval_ms: 1_000,
        restart_limit: 3,
        restart_window_ms: 60_000,
    }
}

#[tokio::test]
async fn real_process_protocol_handles_health_auth_delivery_and_shutdown() {
    let directory = tempfile::tempdir().expect("temporary working directory");
    let sink = Arc::new(RecordingSink::default());
    let package = ProcessBridgePackageFactory
        .launch(&spec(directory.path()), sink.clone())
        .await
        .expect("fixture process launches");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if sink.events.lock().expect("recording sink lock").len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fixture package can invoke durable host ingress");
    let initial = package
        .health(None)
        .await
        .expect("active health probe works");
    assert!(initial.process_alive && initial.service_connected && initial.can_receive);
    assert!(!initial.credential_valid);

    let challenge = package
        .begin_authentication(Some(&AuthenticationMethod::PhoneCode), "{}")
        .await
        .expect("phone challenge begins");
    assert_eq!(challenge.method, AuthenticationMethod::PhoneCode);
    let credential = package
        .submit_authentication("challenge-1", r#"{"code":"123456"}"#)
        .await
        .expect("credential is returned ephemerally");
    let validation = package
        .validate_credentials(&credential.secret_json)
        .await
        .expect("credential is actively validated");
    assert!(validation.valid);

    let delivery = package
        .deliver(
            &BridgeOutbound {
                bridge_id: "fixture-bridge".into(),
                message_id: "message-1".into(),
                destination_json: r#"{"chat":"one"}"#.into(),
                message_json: r#"{"text":"hello"}"#.into(),
                attachments: vec![BridgeAttachment {
                    media_type: "text/plain".into(),
                    name: Some("note.txt".into()),
                    content_handle: "content-1".into(),
                }],
                idempotency_key: "delivery-1".into(),
            },
            Some(&credential.secret_json),
        )
        .await
        .expect("selected message delivers");
    assert_eq!(delivery.external_delivery_id, "external-message-1");
    package.stop().await.expect("process group stops");
}

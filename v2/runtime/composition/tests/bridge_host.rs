use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};
use boxology_runtime::CompositionBuilder;
use bridge_host_contract::{
    AuthenticationMethod, BeginAuthenticationRequest, BridgeAlertTarget, BridgeInbound,
    BridgeIngressMode, BridgeLifecycle, BridgeManagement, BridgeOutbound, BridgeReference,
    BridgeSpec, DeliveryLifecycle, ImportBridgeContentRequest, ReconcileBridgeRequest,
    ReplaceBridgeRequest, SubmitAuthenticationRequest,
};
use bridge_host_implementation::{
    BridgeCredentialReceipt, BridgeCredentialSink, BridgeCredentialUpdate, BridgeHostState,
    BridgeInboundSink, BridgePackage, BridgePackageError, BridgePackageFactory, ContentUpload,
    CredentialStore, InMemoryCredentialStore, PackageChallenge, PackageCredential,
    PackageCredentialValidation, PackageDelivery, PackageHealth, generated as bridge_host,
};
use sha2::{Digest, Sha256};
use trigger_inbox_contract::{ClaimTriggers, TriggerMode, TriggerReference};
use trigger_inbox_implementation::{TriggerInbox, generated as trigger_inbox};

#[derive(Default)]
struct FakePackage {
    deliveries: AtomicUsize,
    stops: AtomicUsize,
    validations: AtomicUsize,
    credential_commits: AtomicUsize,
}

#[async_trait]
impl BridgePackage for FakePackage {
    async fn health(
        &self,
        credential_json: Option<&str>,
    ) -> Result<PackageHealth, BridgePackageError> {
        Ok(PackageHealth {
            process_alive: true,
            service_connected: true,
            can_receive: true,
            can_send: true,
            credential_valid: credential_json.is_some_and(|value| value.contains("secret-token")),
            detail_json: r#"{"fake":true}"#.into(),
        })
    }

    async fn begin_authentication(
        &self,
        method: Option<&AuthenticationMethod>,
        context_json: &str,
    ) -> Result<PackageChallenge, BridgePackageError> {
        assert_eq!(context_json, "{}");
        Ok(PackageChallenge {
            method: method.cloned().unwrap_or(AuthenticationMethod::PhoneCode),
            expires_at_ms: Some(9_999_999_999_999),
            presentation_json: r#"{"prompt":"enter code"}"#.into(),
        })
    }

    async fn submit_authentication(
        &self,
        _challenge_id: &str,
        response_json: &str,
    ) -> Result<PackageCredential, BridgePackageError> {
        assert!(response_json.contains("123456"));
        Ok(PackageCredential {
            secret_json: r#"{"token":"secret-token"}"#.into(),
            expires_at_ms: None,
            account_hint: Some("fixture-account".into()),
            detail_json: r#"{"paired":true}"#.into(),
        })
    }

    async fn validate_credentials(
        &self,
        credential_json: &str,
    ) -> Result<PackageCredentialValidation, BridgePackageError> {
        self.validations.fetch_add(1, Ordering::SeqCst);
        Ok(PackageCredentialValidation {
            valid: credential_json.contains("secret-token"),
            expires_at_ms: None,
            account_hint: Some("fixture-account".into()),
            detail_json: r#"{"validated":true}"#.into(),
        })
    }

    async fn credential_committed(&self, credential_json: &str) -> Result<(), BridgePackageError> {
        assert!(credential_json.contains("secret-token"));
        self.credential_commits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn invalidate_credentials(
        &self,
        _credential_json: &str,
    ) -> Result<(), BridgePackageError> {
        Ok(())
    }

    async fn deliver(
        &self,
        request: &BridgeOutbound,
        credential_json: Option<&str>,
    ) -> Result<PackageDelivery, BridgePackageError> {
        assert!(credential_json.is_some_and(|value| value.contains("secret-token")));
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(PackageDelivery {
            external_delivery_id: format!("external-{}", request.message_id),
            detail_json: r#"{"sent":true}"#.into(),
        })
    }

    async fn stop(&self) -> Result<(), BridgePackageError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct FakeFactory {
    package: Arc<FakePackage>,
    launches: Arc<AtomicUsize>,
    inbound_sink: Arc<Mutex<Option<Arc<dyn BridgeInboundSink>>>>,
    credential_sink: Arc<Mutex<Option<Arc<dyn BridgeCredentialSink>>>>,
}

struct FailOnceFactory {
    package: Arc<FakePackage>,
    launches: Arc<AtomicUsize>,
}

#[async_trait]
impl BridgePackageFactory for FailOnceFactory {
    async fn launch(
        &self,
        _spec: &BridgeSpec,
        _inbound: Arc<dyn BridgeInboundSink>,
        _credentials: Arc<dyn BridgeCredentialSink>,
    ) -> Result<Arc<dyn BridgePackage>, BridgePackageError> {
        if self.launches.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(BridgePackageError::LaunchFailed)
        } else {
            Ok(self.package.clone())
        }
    }
}

#[async_trait]
impl BridgePackageFactory for FakeFactory {
    async fn launch(
        &self,
        _spec: &BridgeSpec,
        inbound: Arc<dyn BridgeInboundSink>,
        credentials: Arc<dyn BridgeCredentialSink>,
    ) -> Result<Arc<dyn BridgePackage>, BridgePackageError> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        *self.inbound_sink.lock().expect("inbound sink lock") = Some(inbound);
        *self.credential_sink.lock().expect("credential sink lock") = Some(credentials);
        Ok(self.package.clone())
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

fn spec(mode: BridgeIngressMode) -> BridgeSpec {
    BridgeSpec {
        bridge_id: "whatsapp".into(),
        package_id: "whatsapp".into(),
        display_name: "WhatsApp".into(),
        launch_json: r#"{"fixture":true}"#.into(),
        configuration_json: "{}".into(),
        authentication_methods: vec![AuthenticationMethod::PhoneCode],
        ingress_mode: mode,
        management: BridgeManagement::AgentManaged,
        alert_target: None,
        desired_running: true,
        health_interval_ms: 10,
        credential_validation_interval_ms: 10,
        restart_limit: 3,
        restart_window_ms: 60_000,
    }
}

#[tokio::test]
async fn bridge_host_owns_auth_ingress_delivery_and_generations() {
    let package = Arc::new(FakePackage::default());
    let observed_package = package.clone();
    let launches = Arc::new(AtomicUsize::new(0));
    let inbound_sink = Arc::new(Mutex::new(None));
    let credential_sink = Arc::new(Mutex::new(None));
    let credential_store = Arc::new(InMemoryCredentialStore::default());
    let bridge_state = BridgeHostState::open_in_memory().expect("bridge state opens");
    let trigger_store = TriggerInbox::open_in_memory().expect("trigger inbox opens");
    let mut builder = CompositionBuilder::new();
    let trigger = trigger_inbox::register(&mut builder, trigger_store);
    let trigger_handle = builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&trigger);
    let factory_credential_sink = credential_sink.clone();
    let factory_inbound_sink = inbound_sink.clone();
    let host_credential_store = credential_store.clone();
    let bridge = bridge_host::register(&mut builder, move |imports| {
        bridge_state.connect(
            imports.trigger_inbox,
            Arc::new(FakeFactory {
                package: package.clone(),
                launches: launches.clone(),
                inbound_sink: factory_inbound_sink.clone(),
                credential_sink: factory_credential_sink.clone(),
            }),
            host_credential_store.clone(),
        )
    });
    builder.connect(&bridge, &trigger);
    let bridge_handle = builder.handle::<bridge_host_contract::BridgeHostHandle>(&bridge);
    let _composition = builder.start().expect("resolved graph starts");

    let registered = bridge_handle
        .register_bridge(context(), spec(BridgeIngressMode::InterruptAndSteer))
        .await
        .expect("dynamic bridge registers and starts");
    assert_eq!(
        registered.lifecycle,
        BridgeLifecycle::AwaitingAuthentication
    );
    assert_eq!(registered.generation, 1);
    assert_eq!(
        registered.ingress_mode,
        BridgeIngressMode::InterruptAndSteer
    );

    let challenge = bridge_handle
        .begin_authentication(
            context(),
            BeginAuthenticationRequest {
                bridge_id: "whatsapp".into(),
                preferred_method: Some(AuthenticationMethod::PhoneCode),
                context_json: "{}".into(),
            },
        )
        .await
        .expect("authentication challenge begins");
    let credential = bridge_handle
        .submit_authentication(
            context(),
            SubmitAuthenticationRequest {
                bridge_id: "whatsapp".into(),
                challenge_id: challenge.challenge_id,
                response_json: r#"{"code":"123456"}"#.into(),
            },
        )
        .await
        .expect("credential is stored by opaque handle");
    assert!(credential.credential_handle.is_some());
    assert!(!format!("{credential:?}").contains("secret-token"));
    assert_eq!(
        observed_package.credential_commits.load(Ordering::SeqCst),
        1
    );
    let handle = credential
        .credential_handle
        .as_deref()
        .expect("opaque credential handle");
    let original = credential_store
        .get(handle)
        .await
        .expect("initial credential loads");
    let previous_fingerprint = format!("{:x}", Sha256::digest(original.as_bytes()));
    let sink = credential_sink
        .lock()
        .expect("credential sink lock")
        .clone()
        .expect("factory captured credential sink");
    let refreshed = r#"{"revision":2,"token":"secret-token"}"#;
    let receipt = sink
        .persist(BridgeCredentialUpdate {
            bridge_id: "whatsapp".into(),
            previous_fingerprint: previous_fingerprint.clone(),
            credential_json: refreshed.into(),
        })
        .await
        .expect("live credential update persists");
    assert_eq!(
        receipt,
        BridgeCredentialReceipt {
            credential_fingerprint: format!("{:x}", Sha256::digest(refreshed.as_bytes())),
        }
    );
    let accepted_fingerprint = receipt.credential_fingerprint.clone();
    assert_eq!(
        credential_store
            .get(handle)
            .await
            .expect("refreshed credential loads"),
        refreshed
    );
    assert!(
        sink.persist(BridgeCredentialUpdate {
            bridge_id: "whatsapp".into(),
            previous_fingerprint,
            credential_json: r#"{"revision":3,"token":"secret-token"}"#.into(),
        })
        .await
        .is_err(),
        "a stale package snapshot cannot overwrite the accepted update"
    );

    let healthy = bridge_handle
        .reconcile_bridge(
            context(),
            ReconcileBridgeRequest {
                bridge_id: "whatsapp".into(),
                expected_generation: 1,
                desired_running: true,
            },
        )
        .await
        .expect("active health probe observes valid credential");
    assert_eq!(healthy.lifecycle, BridgeLifecycle::Healthy);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if observed_package.validations.load(Ordering::SeqCst) >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("supervisor actively revalidates credentials");

    let captured_inbound = inbound_sink
        .lock()
        .expect("inbound sink lock")
        .clone()
        .expect("inbound sink captured");
    let stored = captured_inbound
        .store_content(ContentUpload {
            bridge_id: "whatsapp".into(),
            external_event_id: "event-1".into(),
            media_type: "image/jpeg".into(),
            name: Some("diagram.jpg".into()),
            bytes: b"private image".to_vec(),
        })
        .await
        .expect("package content persists");
    let inbound = BridgeInbound {
        bridge_id: "whatsapp".into(),
        external_event_id: "event-1".into(),
        received_at_ms: 10,
        target_channel_id: "jim".into(),
        sender_json: r#"{"phone":"redacted"}"#.into(),
        message_json: r#"{"text":"hello"}"#.into(),
        attachments: vec![stored.attachment.clone()],
    };
    let intent = bridge_handle
        .accept_inbound(context(), inbound.clone())
        .await
        .expect("inbound is durably enqueued before ack");
    assert_eq!(intent.ingress_mode, BridgeIngressMode::InterruptAndSteer);
    let trigger_record = trigger_handle
        .inspect(
            context(),
            TriggerReference {
                trigger_id: intent.trigger_id.clone(),
            },
        )
        .await
        .expect("trigger exists");
    assert_eq!(trigger_record.mode, TriggerMode::InterruptAndSteer);
    assert!(trigger_record.message_json.contains("externalEventId"));
    assert_eq!(
        trigger_record.attachments[0].content_handle,
        stored.attachment.content_handle
    );
    assert_eq!(
        bridge_handle
            .accept_inbound(context(), inbound)
            .await
            .expect("inbound retry returns original intent"),
        intent
    );
    let forged = BridgeInbound {
        bridge_id: "whatsapp".into(),
        external_event_id: "event-forged".into(),
        received_at_ms: 11,
        target_channel_id: "jim".into(),
        sender_json: r#"{"phone":"redacted"}"#.into(),
        message_json: r#"{"text":"forged"}"#.into(),
        attachments: vec![bridge_host_contract::BridgeAttachment {
            media_type: "image/jpeg".into(),
            name: Some("diagram.jpg".into()),
            content_handle: "file:///tmp/untrusted.jpg".into(),
        }],
    };
    assert!(
        bridge_handle
            .accept_inbound(context(), forged)
            .await
            .is_err(),
        "ingress rejects handles the host did not issue"
    );

    let source_directory = tempfile::tempdir().expect("content source directory");
    let source = source_directory.path().join("agent diagram.jpg");
    std::fs::write(&source, b"agent-created image").expect("content source writes");
    let imported = bridge_handle
        .import_content(
            context(),
            ImportBridgeContentRequest {
                bridge_id: "whatsapp".into(),
                import_id: "agent-diagram-v1".into(),
                source_path: source.to_string_lossy().into_owned(),
                media_type: "image/jpeg".into(),
                name: Some("diagram.jpg".into()),
            },
        )
        .await
        .expect("agent content imports into the bridge store");
    assert_eq!(imported.size_bytes, 19);
    assert_eq!(
        imported.sha256,
        format!("{:x}", Sha256::digest(b"agent-created image"))
    );
    assert_eq!(
        bridge_handle
            .import_content(
                context(),
                ImportBridgeContentRequest {
                    bridge_id: "whatsapp".into(),
                    import_id: "agent-diagram-v1".into(),
                    source_path: source.to_string_lossy().into_owned(),
                    media_type: "image/jpeg".into(),
                    name: Some("diagram.jpg".into()),
                },
            )
            .await
            .expect("exact import retry is idempotent"),
        imported
    );

    let outbound = BridgeOutbound {
        bridge_id: "whatsapp".into(),
        message_id: "message-1".into(),
        destination_json: r#"{"chat":"one"}"#.into(),
        message_json: r#"{"text":"reply"}"#.into(),
        attachments: vec![imported.attachment],
        idempotency_key: "outbound-1".into(),
    };
    let delivered = bridge_handle
        .deliver_message(context(), outbound.clone())
        .await
        .expect("selected message delivers");
    assert_eq!(delivered.lifecycle, DeliveryLifecycle::Delivered);
    assert_eq!(
        bridge_handle
            .deliver_message(context(), outbound)
            .await
            .expect("delivery retry is idempotent"),
        delivered
    );

    let replacement = bridge_handle
        .replace_bridge(
            context(),
            ReplaceBridgeRequest {
                expected_generation: 1,
                spec: spec(BridgeIngressMode::Queue),
            },
        )
        .await
        .expect("policy changes require a new generation");
    assert_eq!(replacement.generation, 2);
    assert_eq!(replacement.ingress_mode, BridgeIngressMode::Queue);
    assert!(
        sink.persist(BridgeCredentialUpdate {
            bridge_id: "whatsapp".into(),
            previous_fingerprint: accepted_fingerprint,
            credential_json: r#"{"revision":3,"token":"secret-token"}"#.into(),
        })
        .await
        .is_err(),
        "a superseded package instance cannot mutate current credentials"
    );

    bridge_handle
        .stop_bridge(
            context(),
            BridgeReference {
                bridge_id: "whatsapp".into(),
            },
        )
        .await
        .expect("bridge stops through host");
}

#[tokio::test]
async fn supervisor_recovers_a_failed_package_within_the_restart_budget() {
    let package = Arc::new(FakePackage::default());
    let launches = Arc::new(AtomicUsize::new(0));
    let bridge_state = BridgeHostState::open_in_memory().expect("bridge state opens");
    let trigger_store = TriggerInbox::open_in_memory().expect("trigger inbox opens");
    let mut builder = CompositionBuilder::new();
    let trigger = trigger_inbox::register(&mut builder, trigger_store);
    let trigger_handle = builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&trigger);
    let bridge = bridge_host::register(&mut builder, move |imports| {
        bridge_state.connect(
            imports.trigger_inbox,
            Arc::new(FailOnceFactory {
                package: package.clone(),
                launches: launches.clone(),
            }),
            Arc::new(InMemoryCredentialStore::default()),
        )
    });
    builder.connect(&bridge, &trigger);
    let bridge_handle = builder.handle::<bridge_host_contract::BridgeHostHandle>(&bridge);
    let _composition = builder.start().expect("resolved graph starts");

    let mut recovering = spec(BridgeIngressMode::Queue);
    recovering.authentication_methods.clear();
    recovering.alert_target = Some(BridgeAlertTarget {
        channel_id: "primary".into(),
        lane: "primary".into(),
    });
    assert!(
        bridge_handle
            .register_bridge(context(), recovering)
            .await
            .is_err(),
        "the synchronous first launch reports its failure"
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let status = bridge_handle
                .bridge_status(
                    context(),
                    BridgeReference {
                        bridge_id: "whatsapp".into(),
                    },
                )
                .await
                .expect("registered bridge remains inspectable");
            if status.lifecycle == BridgeLifecycle::Healthy {
                assert_eq!(status.restart_count_in_window, 2);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("supervisor retries and restores health");

    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is valid")
            .as_millis(),
    )
    .expect("clock fits");
    let alerts = trigger_handle
        .claim(
            context(),
            ClaimTriggers {
                worker_id: "incident-test".into(),
                lane: "primary".into(),
                limit: 10,
                lease_duration_ms: 10_000,
                now_ms,
            },
        )
        .await
        .expect("supervisor alerts are durable");
    assert_eq!(alerts.leases.len(), 2);
    assert!(
        alerts.leases[0]
            .trigger
            .message_json
            .contains("crab.bridge.incident")
    );
    assert!(
        alerts.leases[1]
            .trigger
            .message_json
            .contains("crab.bridge.recovered")
    );
    assert!(
        alerts
            .leases
            .iter()
            .all(|lease| lease.trigger.mode == TriggerMode::Queue
                && lease.trigger.target_channel_id == "primary")
    );

    bridge_handle
        .stop_bridge(
            context(),
            BridgeReference {
                bridge_id: "whatsapp".into(),
            },
        )
        .await
        .expect("recovered bridge stops cleanly");
}

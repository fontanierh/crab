mod contract;
pub use contract::*;

/// Fail-closed placeholder for bridge supervision, authentication and delivery.
pub struct BridgeHostDraft;

#[boxology::implementation]
impl BridgeHostDraft {
    pub async fn register_bridge(
        &self,
        context: boxology::CallContext,
        request: BridgeSpec,
    ) -> Result<BridgeRecord, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn reconcile_bridge(
        &self,
        context: boxology::CallContext,
        request: ReconcileBridgeRequest,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn report_health(
        &self,
        context: boxology::CallContext,
        request: HealthObservation,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn begin_authentication(
        &self,
        context: boxology::CallContext,
        request: BeginAuthenticationRequest,
    ) -> Result<AuthenticationChallenge, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn submit_authentication(
        &self,
        context: boxology::CallContext,
        request: SubmitAuthenticationRequest,
    ) -> Result<CredentialStatus, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn validate_credentials(
        &self,
        context: boxology::CallContext,
        request: BridgeReference,
    ) -> Result<CredentialStatus, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn invalidate_credentials(
        &self,
        context: boxology::CallContext,
        request: BridgeReference,
    ) -> Result<BridgeReceipt, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn accept_inbound(
        &self,
        context: boxology::CallContext,
        request: BridgeInbound,
    ) -> Result<TriggerIntent, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn deliver_message(
        &self,
        context: boxology::CallContext,
        request: BridgeOutbound,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn delivery_status(
        &self,
        context: boxology::CallContext,
        request: DeliveryReference,
    ) -> Result<DeliveryReceipt, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn bridge_status(
        &self,
        context: boxology::CallContext,
        request: BridgeReference,
    ) -> Result<BridgeStatus, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }

    pub async fn stop_bridge(
        &self,
        context: boxology::CallContext,
        request: BridgeReference,
    ) -> Result<BridgeReceipt, BridgeHostError> {
        let _ = (context, request);
        Err(BridgeHostError::DraftOnly)
    }
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use super::generated;

    #[test]
    fn contract_covers_supervision_auth_ingress_and_selected_delivery() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        for required in [
            "reconcile_bridge",
            "begin_authentication",
            "validate_credentials",
            "accept_inbound",
            "deliver_message",
        ] {
            assert!(
                names.contains(&required),
                "missing bridge concern: {required}"
            );
        }
        assert!(names.iter().all(|name| !name.contains("native_event")));
    }
}

mod contract;
pub use contract::*;

/// Fail-closed placeholder for the supervised ACP sub-agent boundary.
pub struct SubAgentHostDraft;

#[boxology::implementation]
impl SubAgentHostDraft {
    pub async fn spawn(
        &self,
        context: boxology::CallContext,
        request: SpawnSubAgentRequest,
    ) -> Result<SubAgentRecord, SubAgentHostError> {
        let _ = (context, request);
        Err(SubAgentHostError::DraftOnly)
    }

    pub async fn send_to_child(
        &self,
        context: boxology::CallContext,
        request: SendToChildRequest,
    ) -> Result<InteractionReceipt, SubAgentHostError> {
        let _ = (context, request);
        Err(SubAgentHostError::DraftOnly)
    }

    pub async fn send_to_parent(
        &self,
        context: boxology::CallContext,
        request: SendToParentRequest,
    ) -> Result<InteractionReceipt, SubAgentHostError> {
        let _ = (context, request);
        Err(SubAgentHostError::DraftOnly)
    }

    pub async fn read_events(
        &self,
        context: boxology::CallContext,
        request: ReadSubAgentEventsRequest,
    ) -> Result<SubAgentEventPage, SubAgentHostError> {
        let _ = (context, request);
        Err(SubAgentHostError::DraftOnly)
    }

    pub async fn status(
        &self,
        context: boxology::CallContext,
        request: SubAgentReference,
    ) -> Result<SubAgentStatus, SubAgentHostError> {
        let _ = (context, request);
        Err(SubAgentHostError::DraftOnly)
    }

    pub async fn stop(
        &self,
        context: boxology::CallContext,
        request: StopSubAgentRequest,
    ) -> Result<SubAgentReceipt, SubAgentHostError> {
        let _ = (context, request);
        Err(SubAgentHostError::DraftOnly)
    }
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use super::{SubAgentContextMode, SubAgentInputMode, generated};

    #[test]
    fn contract_is_non_blocking_bidirectional_and_supervised() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "spawn",
                "send_to_child",
                "send_to_parent",
                "read_events",
                "status",
                "stop",
            ]
        );
        assert_ne!(
            SubAgentContextMode::Fresh,
            SubAgentContextMode::InheritParent
        );
        assert_ne!(SubAgentInputMode::Queue, SubAgentInputMode::Steer);
        assert_ne!(
            SubAgentInputMode::Steer,
            SubAgentInputMode::InterruptAndSteer
        );
    }
}

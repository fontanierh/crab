mod contract;
pub use contract::*;

/// Placeholder boundary implementation. It is deliberately unusable until an ACP runtime is
/// supplied; returning fabricated sessions or authority attestations would make the draft unsafe.
pub struct AgentHostDraft;

#[boxology::implementation]
impl AgentHostDraft {
    pub async fn discover_agents(
        &self,
        context: boxology::CallContext,
        request: DiscoverAgentsRequest,
    ) -> Result<AgentCatalog, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn preflight(
        &self,
        context: boxology::CallContext,
        request: PreflightRequest,
    ) -> Result<PreflightReport, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn open_session(
        &self,
        context: boxology::CallContext,
        request: OpenSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn prompt(
        &self,
        context: boxology::CallContext,
        request: PromptRequest,
    ) -> Result<PromptAccepted, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn read_events(
        &self,
        context: boxology::CallContext,
        request: ReadEventsRequest,
    ) -> Result<EventPage, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn resolve_permission(
        &self,
        context: boxology::CallContext,
        request: PermissionRequest,
    ) -> Result<PermissionResolution, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn session_status(
        &self,
        context: boxology::CallContext,
        request: SessionReference,
    ) -> Result<SessionStatus, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn cancel_run(
        &self,
        context: boxology::CallContext,
        request: RunReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }

    pub async fn close_session(
        &self,
        context: boxology::CallContext,
        request: SessionReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::DraftOnly)
    }
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use boxology_contract::CapabilityId;

    use super::{AgentHostDraft, AgentHostError, generated};

    #[test]
    fn draft_fails_closed_and_declares_the_complete_host_surface() {
        let capabilities = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().clone())
            .collect::<Vec<CapabilityId>>();
        let names = capabilities
            .iter()
            .map(|capability| capability.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "discover_agents",
                "preflight",
                "open_session",
                "prompt",
                "read_events",
                "resolve_permission",
                "session_status",
                "cancel_run",
                "close_session",
            ]
        );

        let _ = AgentHostDraft;
        assert_eq!(AgentHostError::DraftOnly, AgentHostError::DraftOnly);
    }
}

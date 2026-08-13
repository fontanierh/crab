mod contract;
pub use contract::*;

/// Fail-closed placeholder for the native channel boundary.
pub struct NativeChannelDraft;

#[boxology::implementation]
impl NativeChannelDraft {
    pub async fn bind_channel(
        &self,
        context: boxology::CallContext,
        request: BindChannelRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn accept_turn(
        &self,
        context: boxology::CallContext,
        request: ChannelTurn,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn interrupt_and_drain(
        &self,
        context: boxology::CallContext,
        request: InterruptRequest,
    ) -> Result<InterruptReceipt, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn publish_native_event(
        &self,
        context: boxology::CallContext,
        request: NativeChannelEvent,
    ) -> Result<PublishReceipt, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn replay_native_events(
        &self,
        context: boxology::CallContext,
        request: ReplayRequest,
    ) -> Result<PublishedEventPage, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn replace_session(
        &self,
        context: boxology::CallContext,
        request: ReplaceSessionRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn channel_status(
        &self,
        context: boxology::CallContext,
        request: BindingReference,
    ) -> Result<ChannelStatus, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }

    pub async fn unbind_channel(
        &self,
        context: boxology::CallContext,
        request: BindingReference,
    ) -> Result<ChannelReceipt, NativeChannelError> {
        let _ = (context, request);
        Err(NativeChannelError::DraftOnly)
    }
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use super::{ChannelInputMode, generated};

    #[test]
    fn contract_keeps_native_publication_separate_from_bridge_delivery() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "bind_channel",
                "accept_turn",
                "interrupt_and_drain",
                "publish_native_event",
                "replay_native_events",
                "replace_session",
                "channel_status",
                "unbind_channel",
            ]
        );
        assert!(names.iter().all(|name| !name.contains("bridge")));
        assert_ne!(ChannelInputMode::Queue, ChannelInputMode::Steer);
    }
}

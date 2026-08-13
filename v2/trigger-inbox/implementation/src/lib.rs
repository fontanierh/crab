mod contract;
pub use contract::*;

/// Fail-closed placeholder for the durable trigger store.
pub struct TriggerInboxDraft;

#[boxology::implementation]
impl TriggerInboxDraft {
    pub async fn enqueue(
        &self,
        context: boxology::CallContext,
        request: EnqueueTrigger,
    ) -> Result<TriggerReceipt, TriggerInboxError> {
        let _ = (context, request);
        Err(TriggerInboxError::DraftOnly)
    }

    pub async fn claim(
        &self,
        context: boxology::CallContext,
        request: ClaimTriggers,
    ) -> Result<TriggerBatch, TriggerInboxError> {
        let _ = (context, request);
        Err(TriggerInboxError::DraftOnly)
    }

    pub async fn extend_lease(
        &self,
        context: boxology::CallContext,
        request: ExtendLease,
    ) -> Result<TriggerLease, TriggerInboxError> {
        let _ = (context, request);
        Err(TriggerInboxError::DraftOnly)
    }

    pub async fn settle(
        &self,
        context: boxology::CallContext,
        request: SettleTrigger,
    ) -> Result<TriggerReceipt, TriggerInboxError> {
        let _ = (context, request);
        Err(TriggerInboxError::DraftOnly)
    }

    pub async fn inspect(
        &self,
        context: boxology::CallContext,
        request: TriggerReference,
    ) -> Result<TriggerRecord, TriggerInboxError> {
        let _ = (context, request);
        Err(TriggerInboxError::DraftOnly)
    }
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use super::generated;

    #[test]
    fn trigger_contract_is_a_small_durable_queue_protocol() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            ["enqueue", "claim", "extend_lease", "settle", "inspect"]
        );
    }
}

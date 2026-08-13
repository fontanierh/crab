//! Thin assembly proof for the Crab v2 contract draft.
//!
//! This crate deliberately contains no routing, auth or agent policy. Those decisions belong to
//! the four boxes. The composition only proves that generated adapters form one coherent graph.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

use std::sync::Arc;

use agent_host_implementation::{AgentHostDraft, generated as agent_host};
use boxology_contract::{ExposureLevel, ImplementationDescriptor};
use boxology_runtime::{
    AssemblyErrors, Composition, CompositionBuilder, test_support::StubTransport,
};
use bridge_host_implementation::{BridgeHostDraft, generated as bridge_host};
use native_channel_implementation::{NativeChannelDraft, generated as native_channel};
use trigger_inbox_implementation::{TriggerInboxDraft, generated as trigger_inbox};

/// A started draft graph and its in-process binding.
pub struct DraftRuntime {
    /// Keeps the Boxology graph alive.
    pub composition: Composition,
    /// Lets contract tests inspect and dispatch the exposed generated capabilities.
    pub in_process: Arc<StubTransport>,
}

/// Assemble all v2 boxes through generated Boxology adapters.
pub fn start_draft() -> Result<DraftRuntime, AssemblyErrors> {
    let in_process = Arc::new(StubTransport::new());
    let mut builder = CompositionBuilder::new();

    add_agent_host(&mut builder, &in_process);
    add_native_channel(&mut builder, &in_process);
    add_bridge_host(&mut builder, &in_process);
    add_trigger_inbox(&mut builder, &in_process);

    let composition = builder.start()?;
    Ok(DraftRuntime {
        composition,
        in_process,
    })
}

fn expose_all(
    builder: &mut CompositionBuilder,
    transport: &Arc<StubTransport>,
    descriptor: &ImplementationDescriptor,
) {
    let box_id = descriptor.contract().box_id().clone();
    for capability in descriptor.contract().capabilities() {
        builder.expose(
            box_id.clone(),
            capability.id().clone(),
            transport.clone(),
            ExposureLevel::CodeOnly,
        );
    }
}

fn add_agent_host(builder: &mut CompositionBuilder, transport: &Arc<StubTransport>) {
    let descriptor = agent_host::implementation_descriptor();
    expose_all(builder, transport, &descriptor);
    builder.add_box(descriptor, |imports| {
        agent_host::factory(AgentHostDraft, imports)
    });
}

fn add_native_channel(builder: &mut CompositionBuilder, transport: &Arc<StubTransport>) {
    let descriptor = native_channel::implementation_descriptor();
    expose_all(builder, transport, &descriptor);
    builder.add_box(descriptor, |imports| {
        native_channel::factory(NativeChannelDraft, imports)
    });
}

fn add_bridge_host(builder: &mut CompositionBuilder, transport: &Arc<StubTransport>) {
    let descriptor = bridge_host::implementation_descriptor();
    expose_all(builder, transport, &descriptor);
    builder.add_box(descriptor, |imports| {
        bridge_host::factory(BridgeHostDraft, imports)
    });
}

fn add_trigger_inbox(builder: &mut CompositionBuilder, transport: &Arc<StubTransport>) {
    let descriptor = trigger_inbox::implementation_descriptor();
    expose_all(builder, transport, &descriptor);
    builder.add_box(descriptor, |imports| {
        trigger_inbox::factory(TriggerInboxDraft, imports)
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use boxology_manifest::{Kind, Manifest, RelativePath};

    use super::start_draft;

    const MANIFEST: &str = include_str!("../../boxology.toml");

    #[test]
    fn manifest_and_runtime_expose_the_same_four_box_graph() {
        let manifest = Manifest::parse(
            RelativePath::new("boxology.toml").expect("manifest path is valid"),
            MANIFEST.as_bytes(),
        )
        .expect("runtime manifest is valid");
        assert_eq!(manifest.kind(), Kind::Composition);

        let runtime = start_draft().expect("draft graph assembles");
        let binding = runtime
            .in_process
            .runtime()
            .expect("in-process binding starts");
        let counts = binding.exposures().iter().fold(
            BTreeMap::<String, usize>::new(),
            |mut counts, exposure| {
                *counts
                    .entry(exposure.descriptor().id().box_id().as_str().to_owned())
                    .or_default() += 1;
                counts
            },
        );

        assert_eq!(
            counts.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "agent-host",
                "bridge-host",
                "native-channel",
                "trigger-inbox"
            ]
        );
        assert!(counts.values().all(|count| *count > 0));
        assert_eq!(
            manifest
                .composition()
                .expect("composition exists")
                .boxes()
                .len(),
            4
        );
    }
}

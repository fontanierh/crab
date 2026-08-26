# ACP native UI landscape

## Recommendation

Keep `native-channel` UI-agnostic and keep Crab—not the UI—as session/process owner. For a plug-in
client, expose a thin ACP agent facade: the UI speaks ordinary ACP while Crab maps session
list/resume, prompt, cancel and updates onto its already-owned persistent session.

1. Spike [ACP UI](https://github.com/formulahendry/acp-ui) as the drop-in client. It already renders
   sessions, permissions, thoughts, tools and protocol traffic across desktop, mobile and web.
2. If direct attachment is too constraining, build Crab's channel with
   [acp-components](https://github.com/zvzuola/acp-components): its typed core and React workbench
   cover the same rich ACP lifecycle without forcing us to adopt another orchestrator.
3. Track the official remote-transport and proxy-chain RFDs. Until stable, version the facade
   transport at the channel edge and retain Crab's lossless ordered event log as source of truth.

## Shortlist

| Project | Best use | Fit |
|---|---|---|
| [ACP UI](https://github.com/formulahendry/acp-ui) | Plug-in client | Best first experiment; MIT, Tauri/web/mobile, remote WebSocket support |
| [acp-components](https://github.com/zvzuola/acp-components) | Build our UI | Best foundation; MIT, framework-neutral core plus full React workbench |
| [Codeg](https://github.com/xintaofei/codeg) | Adopt a whole workspace | Mature and Apache-2.0, but overlaps Crab's session/sub-agent ownership |
| [Anycode](https://github.com/anycode-ade/anycode) | Full browser IDE | Rich Apache-2.0 React/Rust IDE; much larger surface than a channel |
| [AgentX](https://github.com/sxhxliang/agent-studio) | Native desktop IDE | Strong ACP/tool/session UI; heavier GPUI application to embed |
| [Harnss](https://github.com/OpenSource03/harnss) | Desktop reference | Rich rendering, but its maintainers explicitly flag an active rewrite |
| [ACP to AG-UI](https://github.com/namanrajpal/acp-to-agui) | Custom web product | Useful thin bridge; translation risks losing new ACP-native detail |

Official inventories and evolving protocol work:
[ACP clients](https://agentclientprotocol.com/get-started/clients),
[ACP v2 prompt lifecycle](https://agentclientprotocol.com/protocol/v2/prompt-lifecycle),
[remote transport RFD](https://agentclientprotocol.com/rfds/streamable-http-websocket-transport),
[proxy-chain RFD](https://agentclientprotocol.com/rfds/proxy-chains).

# Crab

A minimal Rust harness for running autonomous AI coding agents. Discord-native, agent-driven, built for long-running unattended operation.

Crab gives an AI agent six primitives -- ingress, a backend contract, sessions, checkpoints, memory, and self-trigger -- then gets out of the way. The agent builds everything else: its own skills, schedules, integrations, and infrastructure. See [PHILOSOPHY.md](PHILOSOPHY.md) for the full thesis.

## What Crab Does

- **Runs coding agents behind Discord.** Claude Code, Codex CLI, and OpenCode are supported as backends behind a unified contract.
- **Manages sessions and context.** Per-channel logical sessions, physical session rotation, checkpoint persistence, and crash recovery.
- **Handles delivery.** Streaming edits, idempotent message delivery, chunked output, and automatic retry/rate-limit handling.
- **Persists everything.** Every run produces a durable event log. Sessions, checkpoints, and outbound records are all recoverable after restart.
- **Enables autonomy.** `crab-trigger` lets the agent schedule future invocations of itself -- the primitive that makes cron jobs, delayed follow-ups, session chaining, and multi-hour unattended work sessions possible.
- **Stays out of the way.** No plugin marketplace, no tool registry, no admin dashboard. The agent is the operator.

## Architecture

```
Discord Gateway
  -> Session Router
  -> Lane Scheduler (per-session FIFO + global concurrency cap)
  -> Backend Adapter (Claude Code | Codex CLI | OpenCode)
  -> Event Log + Checkpoint Store
  -> Memory Files + Memory CLI
  -> Self-Trigger (crab-trigger)
```

Eight Rust crates: `crab-core`, `crab-store`, `crab-scheduler`, `crab-backends`, `crab-discord`, `crab-discord-connector`, `crab-app`, `crab-telemetry`.

## Quick Start

### Prerequisites

- Rust stable toolchain (pinned in `rust-toolchain.toml`)
- A Discord bot token ([provisioning guide](crab/docs/09-discord-provisioning-and-secrets.md))
- At least one backend installed: `claude` (Claude Code), `codex` (Codex CLI), or `opencode`

### Build

```bash
cargo build -p crab-app -p crab-discord-connector
```

### Run

```bash
# Set required env vars
export CRAB_DISCORD_TOKEN="your-bot-token"
export CRAB_BOT_USER_ID="your-bot-user-id"

# Launch connector + daemon
cargo run -p crab-discord-connector -- --crabd ./target/debug/crabd
```

The connector bridges Discord Gateway/REST to the `crabd` daemon over stdio JSONL. On first message, Crab bootstraps a workspace at `~/.crab/workspace/` and runs an onboarding conversation to learn about its owner and purpose.

### Install (production)

```bash
cargo build -p crab-app --bin crabctl
./target/debug/crabctl install --target macos
./target/debug/crabctl doctor --target macos
```

`crabctl` handles prerequisite bootstrap, runtime layout, service provisioning, upgrades, and rollbacks for macOS and Linux.

## Self-Trigger

`crab-trigger` lets the agent schedule future invocations of itself. This is the primitive that enables autonomous operation.

```bash
# Immediate self-trigger
crab-trigger --state-dir "$CRAB_STATE_DIR" --channel <channel_id> --message "Check deployment"

# Delayed self-trigger (wake up in 30 minutes)
sleep 1800 && crab-trigger --state-dir "$CRAB_STATE_DIR" --channel <channel_id> --message "Follow up" &
```

See [PHILOSOPHY.md](PHILOSOPHY.md) for why this matters.

## Memory

Crab uses file-based memory -- markdown files in `~/.crab/workspace/memory/`. No vector database, no embeddings. The agent reads and writes its own memory files. Two CLI commands expose recall to the agent runtime:

```bash
# Ranked keyword search across memory files
crab-memory-search --workspace-root ~/.crab/workspace --user-scope <user_id> --query "owner timezone"

# Exact line-range retrieval
crab-memory-get --workspace-root ~/.crab/workspace --user-scope <user_id> --path memory/global/2026-02-10.md --start-line 1 --end-line 40
```

## Quality Gates

100% test coverage enforced. No exceptions.

```bash
make quality     # Run all gates (fmt, clippy, dead code, coverage, duplication)
make quick       # Fast local preflight (non-gating)
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full quality bar and PR process.

## Docs

- [Philosophy](PHILOSOPHY.md) -- why Crab is a minimal bootstrap
- [Design](crab/DESIGN.md) -- v1 architecture and operational semantics
- [Architecture handbook](crab/docs/README.md) -- deep-dive guides (16 docs)
- [Contributing](CONTRIBUTING.md) -- how to contribute
- [Project rules](AGENTS.md) -- operating rules for all contributors

### Architecture Handbook

| Doc | Topic |
|-----|-------|
| [01](crab/docs/01-initial-turn-and-onboarding.md) | Initial turn and onboarding |
| [02](crab/docs/02-sessions-lanes-and-turn-lifecycle.md) | Sessions, lanes, and turn lifecycle |
| [03](crab/docs/03-rotation-checkpoint-and-compaction.md) | Rotation, checkpoint, and compaction |
| [04](crab/docs/04-memory-model-and-context.md) | Memory model and context |
| [05](crab/docs/05-reliability-delivery-and-recovery.md) | Reliability, delivery, and recovery |
| [06](crab/docs/06-backend-contract-and-inference-profiles.md) | Backend contract and inference profiles |
| [07](crab/docs/07-storage-and-state-model.md) | Storage and state model |
| [08](crab/docs/08-deployment-readiness-gaps.md) | Deployment readiness gaps |
| [09](crab/docs/09-discord-provisioning-and-secrets.md) | Discord provisioning and secrets |
| [10](crab/docs/10-target-machine-operations.md) | Target machine operations |
| [11](crab/docs/11-feature-integration-matrix.md) | Feature integration matrix |
| [14](crab/docs/14-overall-chat-flow-2026-02-12.md) | End-to-end runtime chat flow |

## License

[MIT](LICENSE)

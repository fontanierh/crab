# Crab v2 runtime bundle

This directory is a verified, platform-specific Crab v2 release. It includes the production Rust
binaries, locked Claude and Codex ACP adapters, and the first-party WhatsApp bridge with their
production dependencies. Building requires Rust, npm, and the network; running does not install or
fetch anything.

## Verify

Run this before first use and after copying the bundle:

```sh
python3 libexec/v2_bundle.py verify .
```

The verifier rejects missing, changed, extra, special, absolute-symlink, and escaping-symlink
entries. `bundle-manifest.json` records the source commit and SHA-256 of every runtime entry.

## Install or update

The same command handles first installation and every update. Only the first run needs the
workspace argument:

```sh
python3 libexec/v2_bundle.py deploy . \
  --workspace /absolute/path/to/agent-workspace \
  --environment-file ~/.crab-secrets/crab.env

# Or select Codex on first deployment (Claude is the default):
python3 libexec/v2_bundle.py deploy . \
  --workspace /absolute/path/to/agent-workspace \
  --agent codex

# Later, from a newly verified bundle:
python3 libexec/v2_bundle.py deploy .
```

`--agent` is valid only on first deployment; updates preserve the durable config. The optional
environment file must be an owner-only regular dotenv file. Only names declared by the selected
runtime config are captured. Claude requires `CLAUDE_CODE_OAUTH_TOKEN`; Codex reuses its user-owned
ChatGPT authentication. Later updates preserve already captured environment values when the file
is omitted.

Deployment copies the bundle into an immutable release under `~/.crab-v2`, keeps config, state,
logs and credentials outside releases, and owns the single `com.crab.v2.runtime` user LaunchAgent.
It stops the old runtime gracefully, switches one `current` symlink, then proves one launchd-owned
process and a ready configured topology over authenticated local IPC. A bridge awaiting
authentication or reporting degradation is ready enough to preserve a successful update, while
remaining unhealthy and actionable. The health evidence must attest the exact resolved config and
the launchd-owned PID. Any structural failure restores and verifies the previous release.

The bundled WhatsApp preset intentionally has an empty `inboundPolicy`, so it cannot trigger the
agent until exact authorized DM IDs or group-and-sender pairs are added to the durable runtime
config. Updates preserve that config. See `bridges/whatsapp/README.md` for the strict policy shape.

Check all layers without exposing the captured environment:

```sh
python3 ~/.crab-v2/libexec/v2_bundle.py status
```

Status fails unless the complete configured topology is healthy and returns explicit
`needsAction` entries for authentication or degradation. Inspect the same owner-only evidence
directly with:

```sh
~/.crab-v2/bin/crab-v2-health \
  --config ~/.crab-v2/config/runtime.json \
  --state-dir ~/.crab-v2/state
```

Inspect one ACP session without opening runtime state directly:

```sh
~/.crab-v2/bin/crab-v2-agent --state-dir ~/.crab-v2/state status <session-id>
~/.crab-v2/bin/crab-v2-agent --state-dir ~/.crab-v2/state list 100
~/.crab-v2/bin/crab-v2-agent --state-dir ~/.crab-v2/state \
  diagnostics <session-id> 0 100
```

Diagnostic output may contain raw adapter stderr and is shown only on this explicit owner operation.

Discover native channels and inspect their complete ACP event cursors without opening SQLite:

```sh
~/.crab-v2/bin/crab-v2-channel --state-dir ~/.crab-v2/state list 100
~/.crab-v2/bin/crab-v2-channel --state-dir ~/.crab-v2/state status <binding-id>
~/.crab-v2/bin/crab-v2-channel --state-dir ~/.crab-v2/state events <binding-id> 0 100
```

`interrupt <binding-id> <expected-session-id> <reason>` is an explicit cooperative cancel action.
It never discards already accepted Queue or Steer input.

The Claude and Codex presets both negotiate active steering. If the turn finishes before injection,
the adapter returns the content untouched and Crab starts a normal lifecycle-owned prompt.

The runtime requires macOS, Python 3, Node.js 22 or newer, and Crab's documented unrestricted-host
preflight. Authenticate each ACP agent and bridge with its native flow. Credentials, runtime state,
and logs are never part of a release bundle.

# Target-Machine Operations

## Scope

This runbook defines how to operate Crab on the target machine once deployment begins:

- runtime layout and environment policy
- service lifecycle (start/stop/restart/reboot persistence)
- upgrade and rollback flow
- incident response and disaster recovery basics

This document complements:

- `crab/docs/08-deployment-readiness-gaps.md` (what is still missing)
- `crab/docs/09-discord-provisioning-and-secrets.md` (Discord OAuth/token policy)

## Current Runtime Shape

`crabd` is the production daemon entrypoint (`crates/crab-app/src/bin/crabd.rs`), but
its transport boundary is currently `stdio` JSONL.

Implications:

- `crabd` expects inbound `CrabdInboundFrame` JSON lines on stdin:
  - `kind=gateway_message` frames for Discord ingress
  - `kind=outbound_receipt` frames for connector delivery receipts
- `crabd` emits outbound `CrabdOutboundOp` JSON lines (`op=post|edit`) on stdout.
- `crab-discord-connector` is the in-repo process that bridges Discord Gateway/REST
  with this `crabd` JSONL contract.
- Connector receipts are required for `crabd` to consider outbound Discord delivery complete.

## Installer-Driven Provisioning (`crabctl`)

Crab now includes an installer CLI in `crab-app`:

- `crabctl install`
- `crabctl upgrade`
- `crabctl rollback`
- `crabctl doctor`

Build:

```bash
cargo build -p crab-app --bin crabctl
```

Preview (no host mutations):

```bash
./target/debug/crabctl install --target macos --dry-run
./target/debug/crabctl install --target linux --dry-run
```

Apply:

```bash
sudo ./target/debug/crabctl install --target macos
# or
sudo ./target/debug/crabctl install --target linux
```

Upgrade / rollback / health checks:

```bash
sudo ./target/debug/crabctl upgrade --target macos --release-id <release-id>
sudo ./target/debug/crabctl rollback --target macos
./target/debug/crabctl doctor --target macos
```

Notes:

- Use `--root-prefix <path>` to stage deterministic integration tests in temporary roots.
- `doctor` exits `0` when healthy and non-zero when unhealthy.
- `upgrade` exits `3` when blocked by state-schema incompatibility preflight (no install mutations
  are performed in this case).
- Installer operations are idempotent; rerunning converges rather than requiring destructive cleanup.

## Recommended Host Layout

Use explicit directories with ownership isolation:

```text
/opt/crab/
  bin/
    crabd
  releases/
    <release-id>/
  current -> /opt/crab/releases/<release-id>

/etc/crab/
  crab.env

/var/lib/crab/
  workspace/   # CRAB_WORKSPACE_ROOT

/var/log/crab/
  runtime.log
```

Guidelines:

- Run under a dedicated system user (for example `crab`).
- Grant write access only where needed (`/var/lib/crab`, `/var/log/crab`).
- Keep runtime config in `/etc/crab/crab.env` with restricted permissions (`0600`).

## Environment Configuration

Required minimum keys:

- `CRAB_DISCORD_TOKEN`
- `CRAB_BOT_USER_ID`

Strongly recommended keys:

- `CRAB_WORKSPACE_ROOT=/var/lib/crab/workspace`
- `CRAB_MAX_CONCURRENT_LANES`
- `CRAB_COMPACTION_TOKEN_THRESHOLD`
- `CRAB_INACTIVITY_TIMEOUT_SECS`
- `CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS`
- `CRAB_HEARTBEAT_INTERVAL_SECS`
- `CRAB_RUN_STALL_TIMEOUT_SECS`
- `CRAB_BACKEND_STALL_TIMEOUT_SECS`
- `CRAB_DISPATCHER_STALL_TIMEOUT_SECS`
- `CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED`
- `CRAB_WORKSPACE_GIT_REMOTE` (required when `CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED=true` and `CRAB_WORKSPACE_GIT_PUSH_POLICY=on-commit`)
- `CRAB_WORKSPACE_GIT_BRANCH`
- `CRAB_WORKSPACE_GIT_COMMIT_NAME`
- `CRAB_WORKSPACE_GIT_COMMIT_EMAIL`
- `CRAB_WORKSPACE_GIT_PUSH_POLICY` (`on-commit` or `manual`)
- `CRAB_OWNER_DISCORD_USER_IDS`
- `CRAB_OWNER_ALIASES`
- `CRAB_OWNER_DEFAULT_BACKEND`
- `CRAB_OWNER_DEFAULT_MODEL`
- `CRAB_OWNER_DEFAULT_REASONING_LEVEL`
- `CRAB_OWNER_MACHINE_LOCATION`
- `CRAB_OWNER_MACHINE_TIMEZONE`
- `CRAB_CONNECTOR_IDLE_SLEEP_MS`
- `CRAB_CONNECTOR_RETRY_MAX_ATTEMPTS`
- `CRAB_CONNECTOR_RETRY_BACKOFF_MS`
- `CRAB_CONNECTOR_RETRY_MAX_BACKOFF_MS`
- `CRAB_CONNECTOR_MESSAGE_ID_MAP_PATH`

Example env file:

```bash
CRAB_DISCORD_TOKEN=***redacted***
CRAB_BOT_USER_ID=123456789012345678
CRAB_WORKSPACE_ROOT=/var/lib/crab/workspace
CRAB_MAX_CONCURRENT_LANES=4
CRAB_COMPACTION_TOKEN_THRESHOLD=120000
CRAB_INACTIVITY_TIMEOUT_SECS=1800
CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS=90
CRAB_HEARTBEAT_INTERVAL_SECS=10
CRAB_RUN_STALL_TIMEOUT_SECS=90
CRAB_BACKEND_STALL_TIMEOUT_SECS=30
CRAB_DISPATCHER_STALL_TIMEOUT_SECS=20
CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED=false
CRAB_WORKSPACE_GIT_BRANCH=main
CRAB_WORKSPACE_GIT_COMMIT_NAME=Crab Workspace Bot
CRAB_WORKSPACE_GIT_COMMIT_EMAIL=crab@localhost
CRAB_WORKSPACE_GIT_PUSH_POLICY=on-commit
CRAB_OWNER_DISCORD_USER_IDS=123456789012345678
CRAB_OWNER_MACHINE_TIMEZONE=America/New_York
```

Timeout and heartbeat semantics:

- `CRAB_INACTIVITY_TIMEOUT_SECS`:
  per-logical-session idle duration before Crab triggers an inactivity rotation
  (`flush -> checkpoint -> end physical session -> clear handle`). Default `1800` (30 minutes).
- `CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS`:
  startup grace window used to decide whether in-flight runs from before restart are stale and
  should be reconciled/cancelled. Default `90`.
- `CRAB_HEARTBEAT_INTERVAL_SECS`:
  cadence for periodic health loop execution (run/backend/dispatcher heartbeat checks). Default `10`.
- `CRAB_RUN_STALL_TIMEOUT_SECS`:
  age threshold to classify an active run as stalled during heartbeat. Default `90`.
- `CRAB_BACKEND_STALL_TIMEOUT_SECS`:
  backend-manager heartbeat stall threshold before restart/escalation. Default `30`.
- `CRAB_DISPATCHER_STALL_TIMEOUT_SECS`:
  dispatcher heartbeat threshold for queued-work stall detection/nudge. Default `20`.

Workspace git bootstrap behavior when `CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED=true`:

- Startup validates that git persistence is scoped to `CRAB_WORKSPACE_ROOT` only (no parent/external repo mutation).
- If no local repo exists, Crab bootstraps one and sets HEAD to `CRAB_WORKSPACE_GIT_BRANCH` when needed.
- If `CRAB_WORKSPACE_GIT_REMOTE` is set, Crab validates or binds `origin` deterministically.
- Existing non-empty repositories with branch mismatch are rejected (operator action required).

Workspace git commit behavior when persistence is enabled:

- Crab attempts commits on successful run finalization.
- If a run triggers rotation, Crab attempts a `rotation_checkpoint` commit and then
  a `run_finalized` commit (the second may be a no-op when no additional changes exist).
- Commit messages include structured trailers for correlation:
  - `Crab-Trigger`
  - `Crab-Logical-Session-Id`
  - `Crab-Run-Id`
  - `Crab-Checkpoint-Id`
  - `Crab-Run-Status`
  - `Crab-Commit-Key`
  - `Crab-Staging-Policy-Version`
  - `Crab-Staging-Skipped-Count`
  - `Crab-Staging-Skipped-Rules`
- Secret/transient guardrails:
  - automated staging excludes sensitive/transient paths (dotenv/secret directories/credential dirs/private keys/transient build artifacts)
  - safe dotenv templates (`.env.example`, `.env.sample`, `.env.template`) remain eligible
- Replay safety:
  - if HEAD already carries the same `Crab-Commit-Key` and no further changes exist,
    Crab skips writing a duplicate commit.

Workspace git push divergence handling:

- Push failures are classified by `failure_kind` in daemon diagnostics.
- `non_fast_forward` and `diverged_history` are marked `manual_recovery_required`
  and exhausted immediately (no endless retry loop).
- Deterministic operator recovery commands:
  - `git fetch --prune origin`
  - `git log --oneline --graph --decorate --left-right HEAD...origin/<branch>`
  - `git rebase origin/<branch>`
  - `git push --porcelain origin HEAD:refs/heads/<branch>`

State-schema compatibility preflight (`crabctl doctor` + `crabctl upgrade`):

- Checks `workspace/state/schema_version.json` against binary-supported range.
- Missing marker is treated as legacy `version=0`.
- Incompatible state blocks `upgrade` and prints actionable remediation commands.
- `doctor` reports compatibility as explicit pass/fail with remediation hints.

## Logging (Structured, `tracing`)

- `crabd` uses stdout for JSONL IPC frames and logs to stderr.
- `crab-discord-connector` logs to stderr.
- Configure verbosity with `RUST_LOG` (for example `RUST_LOG=info` or `RUST_LOG=debug`).
- Practical debugging presets:
  - `RUST_LOG=info,crab_app=debug,crab_discord_connector=debug` (runtime flow + connector ops)
  - `RUST_LOG=info,crab_backends=debug` (backend protocol/recovery)

## Remote Debugging (Tailscale)

Typical workflow once you can SSH to the host over Tailscale:

1. Tail logs:
   - `tail -F /var/log/crab/runtime.log`
2. Grep by identifiers shown in logs:
   - `rg \"run:discord:\" /var/log/crab/runtime.log | tail -n 50`
   - `rg \"rotation (started|completed)\" /var/log/crab/runtime.log | tail -n 50`
3. Inspect service state:
   - Linux: `systemctl status crab` and `journalctl -u crab -f`
   - macOS: `launchctl print system/com.crab.runtime`

## Service Management

### Linux (systemd)

Use a dedicated service unit. The command shown below assumes a connector executable
that bridges Discord <-> `crabd` JSONL.

`/etc/systemd/system/crab.service`:

```ini
[Unit]
Description=Crab Discord Runtime
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=crab
Group=crab
EnvironmentFile=/etc/crab/crab.env
WorkingDirectory=/opt/crab/current
ExecStart=/opt/crab/bin/crab-discord-connector --crabd /opt/crab/bin/crabd
Restart=always
RestartSec=2
TimeoutStopSec=30
StandardOutput=append:/var/log/crab/runtime.log
StandardError=append:/var/log/crab/runtime.log

[Install]
WantedBy=multi-user.target
```

Commands:

```bash
sudo systemctl daemon-reload
sudo systemctl enable crab
sudo systemctl start crab
sudo systemctl status crab
sudo journalctl -u crab -f
```

### macOS (launchd)

`/Library/LaunchDaemons/com.crab.runtime.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.crab.runtime</string>
    <key>ProgramArguments</key>
    <array>
      <string>/opt/crab/bin/crab-discord-connector</string>
      <string>--crabd</string>
      <string>/opt/crab/bin/crabd</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
      <key>CRAB_WORKSPACE_ROOT</key>
      <string>/var/lib/crab/workspace</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/var/log/crab/runtime.log</string>
    <key>StandardErrorPath</key>
    <string>/var/log/crab/runtime.log</string>
  </dict>
</plist>
```

Commands:

```bash
sudo launchctl bootstrap system /Library/LaunchDaemons/com.crab.runtime.plist
sudo launchctl enable system/com.crab.runtime
sudo launchctl kickstart -k system/com.crab.runtime
sudo launchctl print system/com.crab.runtime
```

## Operations Checklist

For every deploy/restart:

1. Confirm service is running and restart policy is active.
2. Confirm log stream shows successful startup reconciliation and heartbeat ticks.
3. Confirm workspace bootstrap assets are healthy:
   - `.agents/skills` exists under `CRAB_WORKSPACE_ROOT`
   - `.claude/skills` is a symlink to `../.agents/skills`
   - `.agents/skills/skill-authoring-policy/SKILL.md` exists
4. Send a controlled Discord message in a test channel and verify one reply.
5. Restart service once and confirm replay behavior does not duplicate reply/edit.
6. Record result in deployment notes linked to `crab/docs/08-deployment-readiness-gaps.md` checklist.

## Upgrade Procedure

1. Build and validate release candidate from repository:
   - `make quality`
2. Stage release files under `/opt/crab/releases/<release-id>/`.
3. Install updated binaries in `/opt/crab/bin/` (or update symlinked release path).
4. Keep previous release directory intact for rollback.
5. Restart service.
6. Run operations checklist above.

## Rollback Procedure

1. Stop service.
2. Repoint binaries/symlink to previous known-good release.
3. Restart service.
4. Re-run smoke validation (single ingress/reply + restart replay check).
5. Record rollback cause and follow-up actions.

## Backup and Recovery

Back up at minimum:

- `CRAB_WORKSPACE_ROOT` (`/var/lib/crab/workspace`) including:
  - identity/prompt files
  - memory files
  - session/run/event/checkpoint/outbound stores
- `/etc/crab/crab.env` (secret-safe handling)

Restore order:

1. Restore workspace data.
2. Restore environment file/secrets.
3. Restart service.
4. Validate startup reconciliation and heartbeat behavior.
5. Run controlled message test to confirm runtime integrity.

## Security Notes

- Never log or commit `CRAB_DISCORD_TOKEN`.
- Restrict file permissions on `/etc/crab/crab.env`.
- Keep bot install scope limited to trusted servers.
- Rotate Discord token immediately on suspected exposure (see `crab/docs/09-discord-provisioning-and-secrets.md`).

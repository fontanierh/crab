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

- `crabd` expects inbound `GatewayMessage` JSON lines on stdin.
- `crabd` emits outbound JSON lines (`op=post|edit`) on stdout.
- `crab-discord-connector` is the in-repo process that bridges Discord Gateway/REST
  with this `crabd` JSONL contract.
- The remaining protocol hardening gap is delivery receipts/acks (tracked in
  `crab/docs/08-deployment-readiness-gaps.md`, Gap 1).

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
CRAB_COMPACTION_TOKEN_THRESHOLD=80000
CRAB_INACTIVITY_TIMEOUT_SECS=1800
CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS=90
CRAB_HEARTBEAT_INTERVAL_SECS=10
CRAB_RUN_STALL_TIMEOUT_SECS=90
CRAB_BACKEND_STALL_TIMEOUT_SECS=30
CRAB_DISPATCHER_STALL_TIMEOUT_SECS=20
CRAB_OWNER_DISCORD_USER_IDS=123456789012345678
CRAB_OWNER_MACHINE_TIMEZONE=America/New_York
```

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
3. Send a controlled Discord message in a test channel and verify one reply.
4. Restart service once and confirm replay behavior does not duplicate reply/edit.
5. Record result in deployment notes linked to `crab/docs/08-deployment-readiness-gaps.md` checklist.

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

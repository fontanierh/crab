# Self-Trigger: Scheduled Channel Messages

## Overview

The `crab-trigger` CLI lets the Crab agent schedule itself to revisit a Discord channel at a future time. The agent handles timing (via `sleep`, `cron`, `at`, etc.) and invokes `crab-trigger` to fire immediately. The command writes a trigger file to the state directory; the daemon's main loop picks it up on the next tick and enqueues a synthetic run.

## CLI Usage

```
crab-trigger --state-dir <path> --channel <channel_id> --message <text>
```

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `--state-dir` | Yes | Path to the Crab state directory (e.g. `/path/to/workspace/state`) |
| `--channel` | Yes | Discord channel ID to trigger |
| `--message` | Yes | Message content for the trigger |
| `--help` | No | Show usage help |

### Exit Codes

- `0` — trigger file written successfully (path printed to stdout)
- `1` — error (message printed to stderr)

## How It Works

1. The agent calls `crab-trigger`, which writes a JSON file to `state/pending_triggers/<timestamp>-<hex>.json`.
2. On the next daemon tick (up to 250ms later), `crabd` reads all files in `state/pending_triggers/`.
3. For each valid trigger file, the daemon builds a synthetic `IngressMessage` with `author_id: "crab:self-trigger"` and enqueues it through the normal run pipeline.
4. On successful enqueue the trigger file is deleted.
5. The run proceeds through the normal dispatch path — session initialization, context assembly, backend turn, and Discord delivery.

## Agent Usage Examples

### Delay with sleep

```bash
sleep 1800 && crab-trigger --state-dir "$CRAB_STATE_DIR" --channel 123456789 --message "Check on deployment" &
```

### Fire immediately

```bash
crab-trigger --state-dir "$CRAB_STATE_DIR" --channel 123456789 --message "Reminder: review PR"
```

## Environment

The `CRAB_STATE_DIR` environment variable is automatically set when the Claude backend is spawned, pointing to the workspace's `state/` directory.

## File Format

Each trigger file is a JSON object:

```json
{
  "channel_id": "123456789",
  "message": "Check on deployment"
}
```

The schema uses `deny_unknown_fields` to reject malformed trigger files. Malformed files are silently skipped during polling.

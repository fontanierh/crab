# Verified runtime bundle

`make v2-bundle` is the single release recipe for a clean machine. It creates one
platform-specific directory and refuses a dirty source tree. Development builds require both
`--allow-dirty` and an explicit output path.

![Crab v2 runtime bundle flow](runtime-bundle-flow.png)

```text
crab-v2-<commit>-<platform>/
├── bin/                  ten production binaries; no test fixtures
├── agents/
│   ├── claude/           Claude ACP adapter 0.70.0 + locked production closure
│   └── codex/            Codex ACP adapter 1.6.2 + locked production closure
├── bridges/whatsapp/     first-party package + locked production closure
├── config/               generic and bundle-relative launch presets
├── libexec/v2_bundle.py  copied, stdlib-only verifier
├── bundle-manifest.json  source/platform provenance + complete entry inventory
└── README.md             verify and launch recipe
```

## Build

```sh
make v2-bundle
```

The builder uses `cargo build --release --locked` and `npm ci --omit=dev`; it stages privately,
verifies every entry, smoke-tests all public CLIs plus the agent and bridge, then publishes with one
rename. An existing output is never overwritten.

## Verify anywhere

```sh
python3 /path/to/bundle/libexec/v2_bundle.py verify /path/to/bundle
```

Verification needs only Python 3. Runtime needs Node 22+, but never Rust, npm, `npx`, an install
step, or a package fetch. The manifest rejects missing, altered, extra, special, absolute-symlink,
and escaping-symlink entries. It does not contain credentials or runtime state.

The supplied launch presets pin either Claude Opus 5 with `bypassPermissions` or Codex GPT-5.6-Sol
with `agent-full-access` and high reasoning. Both verify their vendored adapter without `npx` and
register the bundled WhatsApp bridge in queue mode with QR and phone authentication. Claude
negotiates its `_session/steering` extension; Codex remains queue-only until its idle-race work can
remain host-owned. WhatsApp bridge incidents and recovery target the primary queue lane. The
WhatsApp preset starts with an empty, default-deny inbound policy; exact authorized DM IDs or
group-and-sender pairs belong in the durable runtime config and survive updates. The first-party
authority probes currently make both presets macOS-specific.

## Deploy and update

![Crab v2 atomic deployment flow](runtime-deploy-flow.png)

The bundle's stdlib-only tool is also the only deployment recipe. The first deployment needs an
agent workspace; updates reuse the durable config:

```sh
python3 /path/to/bundle/libexec/v2_bundle.py deploy /path/to/bundle \
  --workspace /absolute/path/to/agent-workspace \
  --environment-file ~/.crab-secrets/crab.env

# Or select Codex on first deployment (Claude is the default):
python3 /path/to/bundle/libexec/v2_bundle.py deploy /path/to/bundle \
  --workspace /absolute/path/to/agent-workspace \
  --agent codex

python3 /path/to/new-bundle/libexec/v2_bundle.py deploy /path/to/new-bundle
```

```text
~/.crab-v2/
├── releases/<commit>-<platform>/  immutable, verified release closures
├── current -> releases/<release>  the only release switch
├── bin -> current/bin             stable launch and operator paths
├── agents -> current/agents
├── bridges -> current/bridges
├── libexec -> current/libexec
├── config/runtime.json            durable owner-only topology
├── state/                         durable owner-only runtime state
├── logs/                          launchd stdout and stderr
├── deployment.json                active source/release provenance
└── deploy.lock                    rejects concurrent updates
```

`--agent` is accepted only while creating the durable config; updates always preserve that config.
Codex reuses its user-owned ChatGPT authentication. Claude's first deployment requires
`CLAUDE_CODE_OAUTH_TOKEN`; later updates reuse the captured value when it is unavailable.

The command verifies both source and copied bundles, requires a clean host-matching release,
captures only config-declared environment names from the ambient process and an optional
owner-private environment file, and preserves unavailable values from the prior owner-only
LaunchAgent. It then gracefully stops
`com.crab.v2.runtime`, atomically flips `current`,
and proves all three readiness facts: the manifest still verifies, launchd owns the only `crab-v2`
PID, and the owner-authenticated bridge IPC responds. A failure at any point after cutover restores
the prior symlink, plist, provenance and process, then verifies that rollback.

```sh
python3 ~/.crab-v2/libexec/v2_bundle.py status
```

Status checks the release, stable links, exact managed plist, provenance, singleton PID and local
IPC. Its JSON never includes environment values. This v2 label and service root are separate from
the live v1 `com.crab.runtime` service.

`~/.crab-v2/bin/crab-v2-agent` reads bounded session status and private adapter diagnostics through
the same owner-authenticated IPC. It never opens SQLite directly; diagnostic output is emitted only
for an explicit operator request and may contain sensitive stderr.

# Verified runtime bundle

`make v2-bundle` is the single release recipe for a clean machine. It creates one
platform-specific directory and refuses a dirty source tree. Development builds require both
`--allow-dirty` and an explicit output path.

![Crab v2 runtime bundle flow](runtime-bundle-flow.png)

```text
crab-v2-<commit>-<platform>/
├── bin/                  seven production binaries; no test fixtures
├── agents/claude/        Claude ACP adapter 0.70.0 + locked production closure
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

The supplied launch preset pins Claude Opus 5 with `bypassPermissions`, verifies the vendored ACP
adapter without `npx`, and registers the bundled WhatsApp bridge in queue mode with QR and phone
authentication. The first-party authority probe currently makes this preset macOS-specific.

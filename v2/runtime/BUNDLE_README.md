# Crab v2 runtime bundle

This directory is a verified, platform-specific Crab v2 release. It includes the production Rust
binaries, the locked Claude ACP adapter, and the first-party WhatsApp bridge with its production
dependencies. Building requires Rust, npm, and the network; running does not install or fetch
anything.

## Verify

Run this before first use and after copying the bundle:

```sh
python3 libexec/v2_bundle.py verify .
```

The verifier rejects missing, changed, extra, special, absolute-symlink, and escaping-symlink
entries. `bundle-manifest.json` records the source commit and SHA-256 of every runtime entry.

## Configure and start

1. Copy `config/runtime.bundle.example.json` to a durable location.
2. Replace `/absolute/path/to/agent-workspace` with the real workspace.
3. Keep the config beside this bundle's `config/` directory, or preserve its relative paths.
4. Authenticate Claude using its native login, then start:

```sh
bin/crab-v2 \
  --config config/runtime.bundle.example.json \
  --state-dir /private/path/to/crab-v2-state
```

The runtime requires macOS, Python 3 for verification, Node.js 22 or newer for the bundled agent
and bridge, and Crab's documented unrestricted-host preflight. Credentials and runtime state are
not part of the bundle.

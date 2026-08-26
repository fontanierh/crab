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

## Install or update

The same command handles first installation and every update. Only the first run needs the
workspace argument:

```sh
python3 libexec/v2_bundle.py deploy . --workspace /absolute/path/to/agent-workspace

# Later, from a newly verified bundle:
python3 libexec/v2_bundle.py deploy .
```

Deployment copies the bundle into an immutable release under `~/.crab-v2`, keeps config, state,
logs and credentials outside releases, and owns the single `com.crab.v2.runtime` user LaunchAgent.
It stops the old runtime gracefully, switches one `current` symlink, then proves one launchd-owned
process and authenticated local IPC. Any failure restores and verifies the previous release.

Check all layers without exposing the captured environment:

```sh
python3 ~/.crab-v2/libexec/v2_bundle.py status
```

The runtime requires macOS, Python 3, Node.js 22 or newer, and Crab's documented unrestricted-host
preflight. Authenticate each ACP agent and bridge with its native flow. Credentials, runtime state,
and logs are never part of a release bundle.

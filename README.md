# 2026-02-06-autofun

Bun starter configured to test Google Nano Banana Pro (`gemini-3-pro-image-preview`) via the official Google Gen AI SDK.

## Crab Quality Gates (Rust)

The Crab harness quality bar is enforced locally and in CI.

Run all required checks with:

```bash
make quality
```

Individual commands:

- `make fmt-check`
- `make clippy`
- `make deadcode-check`
- `make test`
- `make coverage-gate`
- `make duplication-check`

## Install

```bash
bun install
```

## Run

```bash
export GOOGLE_API_KEY="your-key"
bun run index.ts "A tiny fox astronaut on the moon, cinematic lighting"
```

If successful, the script writes `output.png` in this directory.

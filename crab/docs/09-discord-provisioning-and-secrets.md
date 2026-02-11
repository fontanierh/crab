# Discord Provisioning And Secrets

## Scope

This runbook documents how to provision Discord access for Crab and how to operate
`CRAB_DISCORD_TOKEN` safely.

This is intentionally operational and step-by-step:

- app/bot creation
- OAuth install flow
- required intents and permissions
- token storage
- token rotation
- incident response

## 1) Create The Discord App And Bot

1. Open the Discord Developer Portal:
   `https://discord.com/developers/applications`
2. Create a new application.
3. Open the app's `Bot` settings page.
4. Create the bot user if not already present.
5. Generate/reset the bot token once, then store it immediately in your secret manager.

Notes:

- Discord no longer lets you re-copy old bot tokens after leaving the page; if lost, reset the token.
- Treat this token as a production credential with full bot authority.

## 2) Configure Gateway Intents

Crab needs message ingress from channels/threads/DMs and message content.

Enable the needed intents in `Bot` settings:

- `Server Members Intent` (privileged; needed if your runtime requires member events)
- `Message Content Intent` (privileged for verified large bots; required for content-driven ingestion)

Practical policy:

- For small/unverified bots, enable intents directly in the portal.
- For verified bots at scale, request privileged intents through the Developer Portal flow when eligible.

## 3) Configure OAuth Install URL

In `OAuth2 -> URL Generator`:

- required scope: `bot`
- optional scope: `applications.commands` (only if slash commands are used)

Permissions:

- Crab's design currently assumes full server administration autonomy for the primary bot.
- For that mode, grant `Administrator` during install.

Security caution:

- `Administrator` grants full server power; only install on servers you control/trust.
- If you want least privilege later, replace Administrator with an explicit permission set.

Install:

1. Copy generated URL.
2. Open it in browser.
3. Select target server.
4. Authorize.
5. Confirm bot appears in server member list.

## 4) Map Credentials Into Runtime Config

Crab runtime requires:

- `CRAB_DISCORD_TOKEN` (required)

Recommended additional config for ownership and behavior:

- `CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED`
- `CRAB_WORKSPACE_GIT_REMOTE`
- `CRAB_WORKSPACE_GIT_BRANCH`
- `CRAB_WORKSPACE_GIT_COMMIT_NAME`
- `CRAB_WORKSPACE_GIT_COMMIT_EMAIL`
- `CRAB_WORKSPACE_GIT_PUSH_POLICY`
- `CRAB_OWNER_DISCORD_USER_IDS`
- `CRAB_OWNER_ALIASES`
- `CRAB_OWNER_DEFAULT_BACKEND`
- `CRAB_OWNER_DEFAULT_MODEL`
- `CRAB_OWNER_DEFAULT_REASONING_LEVEL`
- `CRAB_OWNER_MACHINE_LOCATION`
- `CRAB_OWNER_MACHINE_TIMEZONE`

## 5) Secret Storage Policy For `CRAB_DISCORD_TOKEN`

Required:

- Store only in a secret manager or encrypted runtime environment.
- Never commit tokens to git, docs, screenshots, or chat logs.
- Never place tokens in plaintext `.env` files tracked by source control.
- Restrict read access to operators who deploy/rotate Crab.

Recommended:

- Use one token per environment (dev/staging/prod).
- Maintain token metadata: owner, created date, last rotated date, incident history.
- Rotate on schedule even without incidents.

## 6) Rotation Procedure

When rotating token:

1. Generate/reset bot token in Developer Portal (`Bot -> Reset Token`).
2. Update secret manager entry for `CRAB_DISCORD_TOKEN`.
3. Restart Crab runtime so process picks up new env value.
4. Verify bot reconnects and receives messages.
5. Verify outbound send/edit works.
6. Confirm old token is no longer present in runtime/process environment snapshots.

Post-rotation checks:

- no auth failures in logs
- ingress events still flow
- outbound replies still post/edit

## 7) Incident Response (Suspected Token Leak)

If token exposure is suspected:

1. Immediately reset token in Developer Portal.
2. Update secret manager and restart runtime.
3. Audit recent bot actions in Discord server logs.
4. Review deployment logs and shell history for accidental token output.
5. Invalidate any cached secrets or copied config bundles.
6. Record incident details and corrective actions.

If unauthorized actions occurred:

- temporarily remove bot from critical servers/channels
- restore server configuration from known-good state
- re-invite bot with reviewed permissions

## 8) Verification Checklist

Before declaring Discord provisioning complete:

- app created and bot user present
- required intents enabled
- OAuth URL generated and bot installed in target server
- `CRAB_DISCORD_TOKEN` stored in secret manager
- runtime process can authenticate successfully
- inbound message path validated
- outbound send/edit path validated
- rotation drill executed at least once

## Sources

- Discord Developer Portal (App/Bot/OAuth flows): `https://discord.com/developers/applications`
- Discord tutorials/docs index (app creation + OAuth URL generator flow):
  `https://discord.com/developers/docs/tutorials`
- OAuth2 scopes reference:
  `https://discord.com/developers/docs/topics/oauth2`
- Privileged intents overview:
  `https://support-dev.discord.com/hc/en-us/articles/6207308062871-What-are-Privileged-Intents`
- Privileged intent request flow:
  `https://support-dev.discord.com/hc/en-us/articles/6205754771351-How-do-I-get-Privileged-Intents-for-my-bot`
- Message content privileged intent FAQ:
  `https://support-dev.discord.com/hc/en-us/articles/4404772028055-Message-Content-Privileged-Intent-FAQ`
- Bot token reset/copy behavior:
  `https://support-dev.discord.com/hc/en-us/articles/6470840524311-Why-can-t-I-copy-my-bot-s-token`

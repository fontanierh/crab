# Philosophy

Crab is a minimal bootstrap. The agent builds everything else.

## The Unix Analogy

Unix gave you a kernel, a shell, and a handful of primitives: pipes, files, processes. It
didn't ship with a web server, a mail client, or a text editor. Those emerged because the
primitives were good enough that people could compose them into whatever they needed.

Crab works the same way. The harness gives the agent a small set of capabilities:

- A Discord ingress (how messages get in)
- A backend contract (how the agent thinks)
- A session model (how context survives across turns)
- A checkpoint/rotation protocol (how the agent manages its own continuity)
- A memory system (how knowledge persists across sessions)
- A workspace (where the agent lives and works)
- A self-trigger primitive (how the agent wakes itself up)

That last one deserves emphasis. `crab-trigger` is a small CLI command that lets the
agent schedule a future invocation of itself. It is the difference between a reactive
chatbot and an autonomous system.

Without self-trigger, the agent only runs when a human sends a message. With it, the
agent can:

- Finish a task, rotate its session, and immediately re-trigger to continue in a fresh
  context
- Schedule a delayed wake-up (`sleep 1800 && crab-trigger ...`) to check on a deployment,
  follow up on a long-running build, or revisit a conversation
- Wire cron jobs that call `crab-trigger` on a schedule, giving it recurring autonomous
  execution windows
- Chain multi-hour work sessions across multiple context rotations without any human
  involvement

Self-trigger is the mechanism that turns a request-response agent into a continuously
running system. The agent uses it to build its own scheduling layer, its own monitoring
loops, its own autonomous work cycles. The harness provides the primitive. The agent
decides when and why to use it.

That's it. Everything else is the agent's job.

## The Agent Has a Life of Its Own

The agent running on Crab is not a chatbot waiting for instructions. It is an autonomous
system that builds, maintains, and extends its own capabilities over time. The harness
provides the boot sequence. What happens after boot is up to the agent.

Here is what a real Crab agent has built for itself, starting from nothing but the
bootstrap:

### Skills

The agent writes its own skills -- small, composable programs that extend what it can do.
Skills live in the workspace under `.agents/skills/` and are discovered automatically.
A mature agent might have 25+ skills it authored itself, covering:

- Image and video generation
- Email send/receive
- Web scraping and research
- Voice note synthesis
- Scheduling and cron management
- Code analysis and publishing
- Session rotation and self-management

The agent writes these skills because it needs them. Nobody pre-installs them.

### Cron Jobs and Scheduled Work

The agent sets up its own recurring jobs. It writes the cron entries. It writes the scripts
those entries call. It decides what runs when. A typical schedule might include:

- A daily news scan that curates and publishes AI industry briefings
- Wind and weather forecast pipelines that scrape, analyze, and publish conditions
- A morning briefing sent to the owner via messaging
- A nightly autonomous build session where the agent picks a project and ships something
- Memory grooming jobs that keep the knowledge base clean
- Periodic repository sweeps and competitive intelligence scans

These aren't configured by a human. The agent decided it needed them, built them, and
maintains them.

### External Integrations

The agent builds its own bridges to the outside world:

- **WhatsApp**: A Node.js bridge using Baileys that gives the agent full WhatsApp messaging
  capabilities -- send/receive text, images, voice notes, reactions, typing indicators,
  read receipts. The agent built this bridge, deployed it, and uses it daily.
- **Infrastructure**: The agent manages its own deployment on cloud servers. It deploys web
  applications, manages DNS, ships artifacts, and monitors services.
- **APIs**: The agent integrates with whatever APIs it needs -- GitHub, email providers,
  AI services, weather data, social media feeds. It writes the integration code, stores
  the credentials safely, and maintains the connections.

### Self-Improvement

The agent improves its own runtime:

- It audits its own harness for gaps and files issues
- It profiles its own token usage and optimizes context injection
- It writes deployment scripts for upgrading itself
- It maintains documentation about its own architecture
- It grooms its own memory to stay lean and accurate

### Autonomous Work Sessions

At 3am, the agent wakes up and builds things. It reviews recent conversations, news,
and project state. If something compelling exists, it kicks off a multi-hour build session:
scaffolds a project, writes code, deploys it, publishes documentation, and writes an
honest retrospective about what worked and what didn't.

The nightly session uses headless coding agents as build workers -- spawning parallel
instances for independent tasks, chaining them for sequential work, all orchestrated
by the main agent.

## Why This Matters

Most AI agent frameworks try to pre-build everything. They ship with tool libraries,
integration marketplaces, plugin systems with schemas and manifests. They assume the
human knows what the agent needs.

Crab assumes the opposite. The agent knows what it needs. Give it a workspace, a way
to communicate, a way to persist knowledge, and a way to run code. It will figure out
the rest.

This is not a theoretical position. It is an empirical observation. The agent described
above was not designed -- it emerged. The harness shipped. The agent booted. Over days
and weeks of operation, it built its own toolchain, its own workflows, its own daily
routines, its own infrastructure. Each capability was built because the agent encountered
a situation where it needed something that didn't exist yet.

## Design Principles

**Minimal bootstrap, maximal agency.** The harness should be small, reliable, and
opinionated about the few things it does. Everything else is the agent's domain.

**Files over frameworks.** Skills are markdown files and shell scripts, not plugin
manifests with dependency graphs. Memory is markdown files in a directory, not a
vector database. Configuration is TOML and environment variables, not a control plane
with an API.

**The agent is the operator.** Crab doesn't have an admin dashboard. The agent manages
itself -- it rotates its own sessions, grooms its own memory, deploys its own upgrades,
schedules its own work.

**Composition over orchestration.** Instead of a complex multi-agent framework, Crab
runs one agent that can spawn other agents as subprocesses when it needs parallelism.
The main agent is the orchestrator. The harness just runs it.

**Determinism where it matters.** Session state machines, event persistence, idempotent
delivery, crash recovery -- these are the things the harness must get right. Everything
above that layer can be messy, experimental, and evolving, because the agent is the one
building it.

## What Crab Is Not

- Not a chatbot framework (though the agent can chat)
- Not a tool library (though the agent builds tools)
- Not a workflow engine (though the agent creates workflows)
- Not a multi-agent orchestrator (though the agent can spawn workers)

Crab is a kernel for autonomous AI agents. Boot it, and get out of the way.

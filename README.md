# lex

Lex is a repo-local coordination and supervision layer for coding agents.

It gives a repository its own shared task system, session presence, inboxes, role contracts, delegation flow, and supervised worker infrastructure so multiple agents can operate in the same codebase without colliding, drifting, or losing context.

Lex keeps coordination local to the repo instead of scattering it across terminal history, ad hoc notes, or external services.

## What Lex does

Lex provides:

- a durable `.lex/` scaffold for project memory and runtime state
- a SQLite-backed coordination store for agents, tasks, sessions, leases, messages, events, workers, and dispatch records
- a terminal UI for the common coordination workflow
- explicit session bootstrap and continuity for supervised work
- role contracts with guarded task actions
- task claiming, delegation, watches, and task-scoped messaging
- Git-aware session state and claimed-path conflict detection
- action and event provenance tracking
- roster reconciliation and retirement flow for agent identity hygiene
- supervised local worker runtimes with approval-gated dispatch packets

## Why it exists

Lex is for repositories where more than one coding agent may be active and coordination needs to be durable, inspectable, and local.

It is built to solve problems like:

- two agent instances accidentally reusing the same identity
- tasks being "owned" by stale or vanished sessions
- work coordination living only in terminal scrollback
- parent tasks needing to delegate child work without losing control
- local workers needing approval, delivery, acknowledgement, and completion tracking
- repo conflicts being discovered too late because nobody had explicit claimed work boundaries

Lex turns those fuzzy coordination states into explicit system state.

## Quick start

The default entrypoint is the terminal UI:

```bash
python3 -m lex.cli
```

That opens the main dashboard for the common workflow:

* install Lex into a repo
* register agents
* start sessions
* create or claim tasks
* inspect inboxes
* send task messages
* review coordination state

If a full terminal UI is not available, Lex falls back to a simpler interactive shell.

### TUI hotkeys

* `a` register agent
* `tab` switch focus between tasks and sessions
* `h` heartbeat a session
* `x` end a session
* `m` send a message on the selected task
* `t` change selected task status
* `s` start session
* `n` create task
* `c` claim selected task
* `j` / `k` move task selection
* `r` refresh
* `q` quit

## Minimal CLI walkthrough

If you want the direct command flow instead of the TUI:

```bash
python3 -m lex.cli init
python3 -m lex.cli agent identify codex --role dev --specialty frontend
python3 -m lex.cli session start codex-brisk-otter --label primary
python3 -m lex.cli task create "Define schema"
python3 -m lex.cli task claim 1 codex-brisk-otter
python3 -m lex.cli msg send --task 1 --from codex-brisk-otter --type note --body "Schema draft started."
python3 -m lex.cli task show 1
```

That gives you a local coordination loop with explicit identity, live session presence, task ownership, and durable task-scoped messaging.

## Core concepts

### Agents

Each agent gets a unique identity inside the workspace.

```bash
python3 -m lex.cli agent identify codex
```

Lex can allocate a unique name like `codex-brisk-otter` so two terminals do not accidentally reuse the same identity. You can also provide your own name with `--name`. Duplicate names are rejected.

Agents use a two-part organization model:

* canonical primary role: `dev`, `pm`, `auditor`, or `infra`
* optional specialty: built-in or workspace-defined

Built-in specialties:

* `frontend`
* `infra`
* `ux`
* `security`
* `release`

To add a custom specialty for a workspace:

```bash
python3 -m lex.cli specialty add tech_lead
python3 -m lex.cli agent role codex-brisk-otter dev --specialty tech_lead
```

### Sessions

Sessions make presence explicit so Lex can distinguish a live owner from a stale one.

```bash
python3 -m lex.cli session start codex-brisk-otter --label primary
python3 -m lex.cli session heartbeat 1
python3 -m lex.cli session list --active-only
python3 -m lex.cli session end 1
```

Each new session receives a bootstrap packet with:

* role contract and allowed or blocked verbs
* required first actions
* workflow template
* hydrated memory for active tasks, subscriptions, inbox state, and recent decisions

Session bootstrap is mandatory before supervised work.

### Tasks and messages

Tasks can be created, claimed, inspected, and discussed through task-scoped messages.

```bash
python3 -m lex.cli task create "Define schema"
python3 -m lex.cli task claim 1 codex-brisk-otter
python3 -m lex.cli msg send --task 1 --from codex-brisk-otter --type note --body "Schema draft started."
python3 -m lex.cli task show 1
python3 -m lex.cli msg task 1
```

### Delegation

Parent tasks can delegate child work while retaining ownership of the parent.

```bash
python3 -m lex.cli task create "Ship coordination UX" --created-by codex-brisk-otter --delegation-mode hypervisor
python3 -m lex.cli task claim 2 codex-brisk-otter
python3 -m lex.cli task delegate 2 codex-brisk-otter claude-steady-ibis "Review message model" --body "Inspect task messaging and suggest improvements."
python3 -m lex.cli task show 2
```

### Watches

Subscriptions track delivery and acknowledgement state.

```bash
python3 -m lex.cli watch add codex-brisk-otter 3
python3 -m lex.cli watch list --agent codex-brisk-otter
python3 -m lex.cli watch ack codex-brisk-otter 3
```

## Install into another project

The default install flow is interactive:

```bash
python3 -m lex.cli --root /path/to/project install
```

Lex detects whether the target already has `AGENTS.md`, `CLAUDE.md`, and a Git checkout, then walks through:

* whether to preserve, merge, assisted-merge, or overwrite root agent files
* whether to ignore only runtime state or keep Lex local-only
* whether ignore rules belong in `.gitignore` or `.git/info/exclude`

### Non-interactive install examples

For a new or shared project workflow, merge Lex into root agent files and keep only runtime state out of Git:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files merge --ignore-policy runtime --ignore-target gitignore
```

For an existing project where you do not want to touch `AGENTS.md` or `CLAUDE.md`, preserve those files and install only the scaffold:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files preserve --ignore-policy runtime --ignore-target gitignore
```

For a local-only workflow, keep Lex out of the shared repo index by writing ignore rules to `.git/info/exclude`:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files merge --ignore-policy all --ignore-target local-exclude
```

Agent-file integration modes:

* `preserve`: leave root agent files untouched
* `merge`: append or update a managed Lex block without overwriting user content
* `assisted`: preserve root files for now and generate a merge packet for an agent to propose semantic edits
* `overwrite`: replace root agent files with Lex bridge files

## Assisted merge

For repos with meaningful existing agent architecture:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files assisted --assisted-agent codex
python3 -m lex.cli --root /path/to/project merge diff
python3 -m lex.cli --root /path/to/project merge apply
```

This flow writes:

* `.lex/runtime/install-merge-plan.md`
* `.lex/runtime/install-merge-context/`
* `.lex/runtime/install-merge-proposal/`

An agent can prepare proposed `AGENTS.md` and `CLAUDE.md` files in the proposal directory. `merge diff` shows the proposed changes, and `merge apply` writes only the approved proposal files back to the project root.

## Read-side inspection

Use these commands to inspect current state without mutating it:

```bash
python3 -m lex.cli task show 1
python3 -m lex.cli msg task 1
python3 -m lex.cli event list --task 1
```

For live coordination, the same commands support follow mode:

```bash
python3 -m lex.cli msg inbox claude-steady-ibis --follow
python3 -m lex.cli msg task 3 --follow
python3 -m lex.cli event list --task 3 --follow
```

## Role guards

Role contracts apply to task verbs.

For example, a `pm` session is expected to review inbox, inspect child work, and delegate before acting freely, and `task claim` is blocked unless you explicitly override the role contract:

```bash
python3 -m lex.cli task claim 7 codex-pm-dalton
python3 -m lex.cli task claim 7 codex-pm-dalton --force-role-override
```

Recommended mapping:

* `dev`: implementation and technical execution
* `pm`: planning, design, scoping, release coordination
* `auditor`: review, verification, compliance, regression checking
* `infra`: integration, merge coordination, release plumbing

## Git awareness and conflict detection

Lex tracks session-level Git state so coordination reflects actual repo conditions more closely.

This includes:

* branch and base snapshot capture
* dirty and staged state refresh on heartbeat
* changed file tracking
* claimed-path conflict detection
* prefix overlap detection
* cross-branch downgrade behavior for softer conflict signaling

## Supervised workers

Lex can supervise approved local worker processes and deliver structured task packets into worker inboxes under `.lex/runtime/workers/`.

```bash
python3 -m lex.cli worker register codex-dev codex \
  --role dev \
  --command-json '["codex"]' \
  --approval-policy always \
  --created-by codex-pm-dalton

python3 -m lex.cli worker request-start codex-dev \
  --requested-by codex-pm-dalton \
  --task-id 1 \
  --reason "Need a supervised dev worker for child tasks"

python3 -m lex.cli worker approve 1 approved --approved-by human
python3 -m lex.cli worker start 1
python3 -m lex.cli worker runtime-list
python3 -m lex.cli worker cleanup
```

Worker runtimes expose:

* `LEX_WORKER_INBOX`
* `LEX_WORKER_RUNTIME_ID`
* `LEX_DB_PATH`
* `LEX_ROOT`

The dispatch layer writes structured task packets into the runtime inbox and tracks approval, delivery, acknowledgement, and completion state in the Lex database.

```bash
python3 -m lex.cli dispatch create \
  --task-id 1 \
  --from codex-pm-dalton \
  --to-worker codex-dev \
  --summary "Implement child task" \
  --body "Read the assigned task packet and report completion back into Lex." \
  --require-approval

python3 -m lex.cli dispatch approve 1 approved --approved-by human
python3 -m lex.cli dispatch send 1 --runtime-id 1
python3 -m lex.cli dispatch ack 1 --runtime-id 1 --note "accepted"
python3 -m lex.cli dispatch complete 1 completed --note "merged into feature branch"
```

## Observability and operator visibility

Lex tracks action history and derived state so an operator can understand what is happening without reconstructing the repo by hand.

This includes:

* action and event provenance
* dashboard summary counts
* stale and blocked work signals
* review-needed and attention-needed surfaced state
* dirty repo and risk indicators tied to live session context

## Status

Lex is already useful as a local multi-agent coordination layer, but some areas should still be understood as active v1 evolution rather than frozen surface area.

The current implementation is strongest in:

* agent identity and roster control
* session presence and bootstrap
* task coordination and delegation
* role contracts
* worker dispatch flow
* Git-aware repo coordination
* provenance and operator visibility

## Testing

The repository includes a local automated test suite covering core coordination, contracts, Git-aware behavior, workers, and UI flows.

Run the test suite with:

```bash
python3 -m pytest
```

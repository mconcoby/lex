# lex

`lex` is a repo-local operating layer for coding agents.

This project was independently implemented and was inspired at a high level by [mex](https://github.com/theDakshJaitly/mex). No code was copied.

The v1 implementation in this repository provides:

- A durable `.lex/` scaffold for project memory
- A SQLite-backed coordination store for agents, tasks, leases, and messages
- A Python CLI for bootstrapping, live agent presence, and concurrent task coordination
- An experimental dispatch control plane for supervised local worker runtimes and task packets

## Quick start

The default entrypoint is now a TUI:

```bash
python3 -m lex.cli
```

That opens a terminal dashboard for the common workflow: install, register agents, start sessions, create or claim tasks, inspect inboxes, and run merge review actions. If a full terminal UI is not available, lex falls back to the simpler interactive shell.

Current TUI hotkeys:
- `a` register agent
- `tab` switch focus between tasks and sessions
- `h` heartbeat a session
- `x` end a session
- `m` send a message on the selected task
- `t` change selected task status
- `s` start session
- `n` create task
- `c` claim selected task
- `j` / `k` move task selection
- `r` refresh
- `q` quit

Session bootstrap is mandatory before supervised work. Each new session gets a role-specific bootstrap packet with:
- role contract and allowed or blocked verbs
- required first actions
- workflow template
- hydrated memory for active tasks, subscriptions, inbox state, and recent decisions

The first step for a new agent instance should be identification. Lex can allocate a unique instance name so two Codex terminals do not accidentally reuse the same identity:

```bash
python3 -m lex.cli agent identify codex
```

That registers a unique name like `codex-brisk-otter`. If you want to choose a name yourself, pass `--name`, and Lex will reject duplicates.
Agents now use a two-part org model:
- canonical primary role: `dev`, `pm`, `auditor`, or `infra`
- built-in specialties: `frontend`, `infra`, `ux`, `security`, `release`
- user-defined specialties can be added per workspace

The subcommands still exist for scripting and direct control:

```bash
python3 -m lex.cli init
python3 -m lex.cli agent identify codex --role dev --specialty frontend
python3 -m lex.cli task create "Define schema"
python3 -m lex.cli task claim 1 codex-brisk-otter
python3 -m lex.cli msg send --task 1 --from codex-brisk-otter --type note --body "Schema draft started."
python3 -m lex.cli task list
```

## Install Into Another Project

The default experience is interactive:

```bash
python3 -m lex.cli --root /path/to/project install
```

`lex install` detects whether the target already has `AGENTS.md`, `CLAUDE.md`, and a Git checkout, then walks the user through:
- whether to preserve, merge, assisted-merge, or overwrite root agent files
- whether to ignore only runtime state or keep lex local-only
- whether ignore rules belong in `.gitignore` or `.git/info/exclude`

For scripted installs, a non-interactive path still exists.

For a new or shared project workflow, merge `lex` into root agent files and keep only runtime state out of Git:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files merge --ignore-policy runtime --ignore-target gitignore
```

For an existing project where you do not want to touch `AGENTS.md` or `CLAUDE.md`, preserve those files and install only the scaffold:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files preserve --ignore-policy runtime --ignore-target gitignore
```

For a local-only workflow, keep `lex` out of the shared repo index by writing ignore rules to `.git/info/exclude`:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files merge --ignore-policy all --ignore-target local-exclude
```

Agent-file integration modes:
- `preserve`: leave root agent files untouched
- `merge`: append or update a managed lex block without overwriting user content
- `assisted`: preserve root files for now and generate a merge packet for an agent to propose semantic edits
- `overwrite`: replace root agent files with lex bridge files

## Assisted Merge

For repos with meaningful existing agent architecture, choose `assisted` in the install wizard or run:

```bash
python3 -m lex.cli --root /path/to/project install --non-interactive --agent-files assisted --assisted-agent codex
python3 -m lex.cli --root /path/to/project merge diff
python3 -m lex.cli --root /path/to/project merge apply
```

The assisted flow writes:
- `.lex/runtime/install-merge-plan.md`
- `.lex/runtime/install-merge-context/`
- `.lex/runtime/install-merge-proposal/`

An agent can prepare proposed `AGENTS.md` and `CLAUDE.md` files in the proposal directory. `merge diff` shows the proposed changes, and `merge apply` writes only the approved proposal files back to the project root.

## Delegation

Hypervisor-style parent tasks can delegate child work while retaining ownership of the parent.

```bash
python3 -m lex.cli task create "Ship coordination UX" --created-by codex-brisk-otter --delegation-mode hypervisor
python3 -m lex.cli task claim 2 codex-brisk-otter
python3 -m lex.cli task delegate 2 codex-brisk-otter claude-steady-ibis "Review message model" --body "Inspect task messaging and suggest improvements."
python3 -m lex.cli task show 2
```

## Read-Side Coordination

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

## Sessions

Sessions make agent presence explicit so `lex` can distinguish a live owner from a stale one.

```bash
python3 -m lex.cli agent identify codex --role dev --specialty frontend
python3 -m lex.cli session start codex-brisk-otter --label primary
python3 -m lex.cli session bootstrap-show 1
python3 -m lex.cli session bootstrap-ack 1 --by human
python3 -m lex.cli session action 1 review_inbox
python3 -m lex.cli session heartbeat 1
python3 -m lex.cli session list --active-only
python3 -m lex.cli task show 2
python3 -m lex.cli session end 1
```

Role guards apply to task verbs. For example, a `pm` session is expected to review inbox, inspect child work, and delegate before acting freely, and `task claim` is blocked unless you explicitly override the role contract:

```bash
python3 -m lex.cli task claim 7 codex-pm-dalton
python3 -m lex.cli task claim 7 codex-pm-dalton --force-role-override
```

## Watches

Subscriptions now track delivery and acknowledgement state.

```bash
python3 -m lex.cli watch add codex-brisk-otter 3
python3 -m lex.cli watch list --agent codex-brisk-otter
python3 -m lex.cli watch ack codex-brisk-otter 3
```

## Supervised Workers

Lex can now supervise approved local worker processes and deliver structured task packets into worker inboxes under `.lex/runtime/workers/`.

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
- `LEX_WORKER_INBOX`
- `LEX_WORKER_RUNTIME_ID`
- `LEX_DB_PATH`
- `LEX_ROOT`

The current dispatch layer writes structured task packets into the runtime inbox and tracks approval, delivery, acknowledgement, and completion state in the Lex database.

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

## Agent Roles

Roles are split into a canonical primary role plus optional specialty so Lex can mirror software-organization responsibilities without copying a human org chart too literally.

```bash
python3 -m lex.cli agent identify codex --role pm --specialty ux
python3 -m lex.cli agent register claude-steady-ibis claude --role auditor --specialty security
python3 -m lex.cli agent list
```

Built-in specialties:
- `frontend`
- `infra`
- `ux`
- `security`
- `release`

To add a custom specialty for a workspace:

```bash
python3 -m lex.cli specialty add tech_lead
python3 -m lex.cli agent role codex-brisk-otter dev --specialty tech_lead
```

Recommended mapping:
- `dev`: implementation and technical execution
- `pm`: planning, design, scoping, release coordination
- `auditor`: review, verification, compliance, regression checking
- `infra`: integration, merge coordination, release plumbing

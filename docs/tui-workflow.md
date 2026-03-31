# lex TUI Workflow

The preferred entrypoint for day-to-day use is:

```bash
python3 -m lex.cli
```

When a real terminal is available, this launches the lex TUI. Otherwise lex falls back to the simpler menu shell.

## Core Navigation

- `tab`: switch focus between the task pane and the session pane
- `j` / `k`: move the current selection within the focused pane
- `r`: refresh the dashboard
- `q`: quit

## Task Actions

- `n`: create a task
- `c`: claim the selected task
- `m`: send a message on the selected task
- `t`: change the selected task status
- `d`: delegate a child task from the selected task
- `f`: hand off the selected task to another agent

The selected-task pane shows:

- task title and status
- owner and delegation mode
- recent child tasks
- recent task-thread messages

## Session Actions

- `a`: register an agent
- `s`: start a session
- `h`: send a heartbeat for the selected session
- `x`: end the selected session

## Visual Warnings

- `!` in the task list indicates a lease that is close to expiring
- `!` in the session list indicates a stale session heartbeat

## Usage Notes

- The TUI operates on the same `.lex/lex.db` state as the CLI commands.
- If another agent is active in the same repository, use lex task ownership and session state before making overlapping changes.
- For complex root-agent-file integration, use the assisted merge workflow outside the TUI:

```bash
python3 -m lex.cli merge diff
python3 -m lex.cli merge apply
```

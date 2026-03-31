from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import sqlite3
import socket
import sys
import time
from pathlib import Path
from typing import Callable, Sequence

from lex.db import BUILTIN_SPECIALTIES, connect, ensure_workspace, fetch_one, initialize_database, list_specialties, log_event, resolve_paths
from lex.installer import InstallContext, inspect_install_context, install_scaffold
from lex.merge_workflow import apply_proposal, create_merge_packet, resolve_merge_paths, unified_diff
from lex.rich_output import (
    console,
    print_ok,
    print_info,
    render_agent_list,
    render_event_list,
    render_inbox_rows,
    render_session_list,
    render_task_list,
    render_task_message_rows,
    render_task_show,
)
from lex.tui import run_tui


AGENT_NAME_RE = re.compile(r"^(codex|claude|cursor)-[a-z]+-[a-z]+$")
AGENT_ADJECTIVES = (
    "brisk",
    "calm",
    "clear",
    "eager",
    "keen",
    "nimble",
    "quiet",
    "sharp",
    "steady",
    "swift",
)
AGENT_NOUNS = (
    "badger",
    "falcon",
    "heron",
    "ibis",
    "lynx",
    "otter",
    "raven",
    "sparrow",
    "stoat",
    "wren",
)
CANONICAL_AGENT_ROLES = {"dev", "pm", "auditor"}
VALID_TASK_STATES = {
    "open",
    "claimed",
    "in_progress",
    "blocked",
    "review_requested",
    "handoff_pending",
    "done",
    "abandoned",
}
VALID_MESSAGE_TYPES = {
    "note",
    "question",
    "answer",
    "blocker",
    "handoff",
    "review_request",
    "review_result",
    "decision",
    "artifact_notice",
}
SESSION_STALE_MINUTES = 15


def emit_json(data: object) -> None:
    print(json.dumps(data, indent=2, sort_keys=True))


def require_agent_name(name: str) -> None:
    if not AGENT_NAME_RE.match(name):
        raise SystemExit("agent names must match <agent>-<adjective>-<noun>")


def require_message_type(message_type: str) -> None:
    if message_type not in VALID_MESSAGE_TYPES:
        raise SystemExit(f"invalid message type: {message_type}")


def normalize_agent_role(role: str | None) -> str:
    normalized = (role or "").strip()
    if not normalized:
        return ""
    if normalized not in CANONICAL_AGENT_ROLES:
        allowed = ", ".join(sorted(CANONICAL_AGENT_ROLES))
        raise SystemExit(f"invalid agent role: {normalized}. expected one of: {allowed}")
    return normalized


def normalize_agent_specialty(conn: sqlite3.Connection, specialty: str | None) -> str:
    normalized = (specialty or "").strip()
    if not normalized:
        return ""
    allowed = list_specialties(conn)
    if normalized not in allowed:
        allowed_str = ", ".join(allowed)
        raise SystemExit(f"invalid agent specialty: {normalized}. expected one of: {allowed_str}")
    return normalized


def allocate_agent_name(conn: sqlite3.Connection, kind: str) -> str:
    existing = {
        row["name"]
        for row in conn.execute("SELECT name FROM agents WHERE name LIKE ?", (f"{kind}-%",)).fetchall()
    }
    for adjective in AGENT_ADJECTIVES:
        for noun in AGENT_NOUNS:
            candidate = f"{kind}-{adjective}-{noun}"
            if candidate not in existing:
                return candidate
    raise SystemExit(f"no available generated agent names remain for kind: {kind}")


def get_agent(conn: sqlite3.Connection, name: str):
    row = fetch_one(conn, "SELECT * FROM agents WHERE name = ?", (name,))
    if row is None:
        raise SystemExit(f"unknown agent: {name}")
    return row


def get_task(conn: sqlite3.Connection, task_id: int):
    row = fetch_one(conn, "SELECT * FROM tasks WHERE id = ?", (task_id,))
    if row is None:
        raise SystemExit(f"unknown task: {task_id}")
    return row


def get_task_with_owner(conn: sqlite3.Connection, task_id: int):
    row = fetch_one(
        conn,
        """
        SELECT
            t.*,
            a.name AS owner_name,
            a.role AS owner_role,
            a.specialty AS owner_specialty,
            creator.name AS created_by_name
        FROM tasks t
        LEFT JOIN agents a ON a.id = t.owner_agent_id
        LEFT JOIN agents creator ON creator.id = t.created_by_agent_id
        WHERE t.id = ?
        """,
        (task_id,),
    )
    if row is None:
        raise SystemExit(f"unknown task: {task_id}")
    return row


def get_active_lease(conn: sqlite3.Connection, task_id: int):
    return fetch_one(
        conn,
        """
        SELECT * FROM task_leases
        WHERE task_id = ? AND state = 'active' AND released_at IS NULL
          AND expires_at > CURRENT_TIMESTAMP
        ORDER BY id DESC
        LIMIT 1
        """,
        (task_id,),
    )


def get_session(conn: sqlite3.Connection, session_id: int):
    row = fetch_one(
        conn,
        """
        SELECT
            s.*,
            a.name AS agent_name,
            a.kind AS agent_kind
        FROM sessions s
        JOIN agents a ON a.id = s.agent_id
        WHERE s.id = ?
        """,
        (session_id,),
    )
    if row is None:
        raise SystemExit(f"unknown session: {session_id}")
    return row


def get_active_session_for_agent(conn: sqlite3.Connection, agent_id: int):
    return fetch_one(
        conn,
        """
        SELECT
            s.*,
            a.name AS agent_name,
            a.kind AS agent_kind
        FROM sessions s
        JOIN agents a ON a.id = s.agent_id
        WHERE s.agent_id = ? AND s.status = 'active' AND s.ended_at IS NULL
        ORDER BY s.id DESC
        LIMIT 1
        """,
        (agent_id,),
    )


def release_stale_leases(conn: sqlite3.Connection) -> int:
    stale_leases = conn.execute(
        """
        SELECT tl.id, tl.task_id, tl.agent_id, tl.session_id
        FROM task_leases tl
        JOIN sessions s ON s.id = tl.session_id
        WHERE tl.state = 'active'
          AND tl.released_at IS NULL
          AND (
              s.status != 'active'
              OR s.ended_at IS NOT NULL
              OR s.heartbeat_at < datetime('now', ?)
          )
        """,
        (f"-{SESSION_STALE_MINUTES} minutes",),
    ).fetchall()
    if not stale_leases:
        return 0
    lease_ids = [row["id"] for row in stale_leases]
    placeholders = ", ".join("?" for _ in lease_ids)
    conn.execute(
        f"""
        UPDATE task_leases
        SET state = 'released', released_at = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        """,
        tuple(lease_ids),
    )
    for lease in stale_leases:
        log_event(
            conn,
            "lease.released",
            task_id=lease["task_id"],
            agent_id=lease["agent_id"],
            session_id=lease["session_id"],
            payload={"reason": "stale_session"},
        )
    return len(stale_leases)


def build_session_fingerprint(*, kind: str, cwd: str) -> tuple[str, str]:
    hostname = socket.gethostname()
    username = getpass.getuser()
    try:
        tty = os.ttyname(sys.stdin.fileno())
    except OSError:
        tty = "notty"
    pid = os.getpid()
    start_ns = time.time_ns()
    source = f"{kind}|{hostname}|{username}|{cwd}|{tty}|{pid}|{start_ns}"
    fingerprint = hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]
    tty_label = tty.rsplit("/", 1)[-1]
    return fingerprint, f"{hostname}:{tty_label}:{pid}"


def follow_rows(
    fetcher: Callable[[int | None], list[sqlite3.Row]],
    renderer: Callable[[list[sqlite3.Row]], None],
    *,
    initial_rows: list[sqlite3.Row] | None = None,
    initial_since_id: int | None,
    follow: bool,
    poll_interval: float,
    max_polls: int | None,
) -> None:
    last_seen_id = initial_since_id
    polls = 0
    if initial_rows:
        renderer(initial_rows)
        last_seen_id = initial_rows[-1]["id"]
        polls += 1
        if not follow:
            return
        if max_polls is not None and polls >= max_polls:
            return
    while True:
        rows = fetcher(last_seen_id)
        if rows:
            renderer(rows)
            last_seen_id = rows[-1]["id"]
        polls += 1
        if not follow:
            return
        if max_polls is not None and polls >= max_polls:
            return
        time.sleep(poll_interval)




def prompt_text(prompt: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    try:
        response = input(f"{prompt}{suffix}: ").strip()
    except EOFError:
        return default or ""
    if response:
        return response
    return default or ""


def prompt_int(prompt: str, default: int) -> int:
    while True:
        response = prompt_text(prompt, str(default))
        if response.isdigit():
            return int(response)
        print("Enter a whole number.")


def print_dashboard(root: Path) -> None:
    paths = ensure_workspace(root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    agent_count = conn.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
    active_session_count = conn.execute(
        "SELECT COUNT(*) FROM sessions WHERE status = 'active' AND ended_at IS NULL"
    ).fetchone()[0]
    open_task_count = conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE status NOT IN ('done', 'abandoned')"
    ).fetchone()[0]
    inbox_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    print("")
    print(f"lex dashboard  root={root}")
    print(f"agents={agent_count}  active_sessions={active_session_count}  open_tasks={open_task_count}  messages={inbox_count}")
    print("")


def run_interactive_shell(root: Path) -> None:
    while True:
        print_dashboard(root)
        action = prompt_choice(
            "What do you want to do?",
            [
                ("install", "Install or reconfigure lex in this workspace"),
                ("agent_register", "Register an agent"),
                ("session_start", "Start an agent session"),
                ("task_create", "Create a task"),
                ("task_claim", "Claim a task"),
                ("task_list", "List tasks"),
                ("task_show", "Show one task"),
                ("msg_inbox", "View an agent inbox"),
                ("merge_diff", "Show assisted-merge diffs"),
                ("merge_apply", "Apply assisted-merge proposals"),
                ("quit", "Exit"),
            ],
            "task_list",
        )
        print("")
        if action == "quit":
            return
        if action == "install":
            cmd_install(
                argparse.Namespace(
                    root=str(root),
                    agent_files="merge",
                    ignore_policy="runtime",
                    ignore_target="gitignore",
                    assisted_agent="codex",
                    non_interactive=False,
                )
            )
        elif action == "agent_register":
            cmd_agent_register(
                argparse.Namespace(
                    root=str(root),
                    name=prompt_text("Agent name"),
                    kind=prompt_choice(
                        "Agent kind",
                        [("codex", "Codex"), ("claude", "Claude"), ("cursor", "Cursor")],
                        "codex",
                    ),
                )
            )
        elif action == "session_start":
            cmd_session_start(
                argparse.Namespace(
                    root=str(root),
                    agent=prompt_text("Agent name"),
                    label=prompt_text("Session label", "primary"),
                    cwd=prompt_text("Working directory", str(root)),
                    capability=[],
                )
            )
        elif action == "task_create":
            created_by = prompt_text("Created by agent (optional)")
            cmd_task_create(
                argparse.Namespace(
                    root=str(root),
                    title=prompt_text("Task title"),
                    slug="",
                    description=prompt_text("Description (optional)"),
                    priority=prompt_int("Priority", 2),
                    created_by=created_by or None,
                    parent_task=None,
                    delegation_mode=prompt_choice(
                        "Delegation mode",
                        [("direct", "Direct"), ("hypervisor", "Hypervisor")],
                        "direct",
                    ),
                    path=[],
                )
            )
        elif action == "task_claim":
            cmd_task_claim(
                argparse.Namespace(
                    root=str(root),
                    task_id=prompt_int("Task id", 1),
                    agent=prompt_text("Agent name"),
                    ttl_minutes=prompt_int("Lease TTL minutes", 30),
                )
            )
        elif action == "task_list":
            cmd_task_list(argparse.Namespace(root=str(root), json=False))
        elif action == "task_show":
            cmd_task_show(
                argparse.Namespace(root=str(root), task_id=prompt_int("Task id", 1), json=False)
            )
        elif action == "msg_inbox":
            cmd_msg_inbox(
                argparse.Namespace(
                    root=str(root),
                    agent=prompt_text("Agent name"),
                    limit=20,
                    json=False,
                    since_id=None,
                    follow=False,
                    poll_interval=2.0,
                    max_polls=None,
                )
            )
        elif action == "merge_diff":
            cmd_merge_diff(argparse.Namespace(root=str(root)))
        elif action == "merge_apply":
            cmd_merge_apply(argparse.Namespace(root=str(root)))
        print("")


def cmd_init(args: argparse.Namespace) -> None:
    paths = ensure_workspace(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    log_event(conn, "workspace.initialized", payload={"root": str(paths.root)})
    conn.commit()
    print(f"initialized lex in {paths.lex_dir}")


def prompt_choice(prompt: str, options: list[tuple[str, str]], default: str) -> str:
    while True:
        print(prompt)
        for index, (value, label) in enumerate(options, start=1):
            suffix = " [default]" if value == default else ""
            print(f"  {index}. {label}{suffix}")
        try:
            response = input("> ").strip()
        except EOFError:
            return "quit" if any(value == "quit" for value, _ in options) else default
        if not response:
            return default
        if response.isdigit():
            selection = int(response)
            if 1 <= selection <= len(options):
                return options[selection - 1][0]
        for value, _label in options:
            if response == value:
                return value
        print("Invalid selection. Enter a number or one of the option values.")


def print_install_summary(context: InstallContext) -> None:
    print(f"Installing lex into {context.root}")
    print("Detected:")
    print(f"  git checkout: {'yes' if context.has_git_dir else 'no'}")
    print(f"  .gitignore: {'yes' if context.has_gitignore else 'no'}")
    print(f"  AGENTS.md: {'yes' if context.has_agents_file else 'no'}")
    print(f"  CLAUDE.md: {'yes' if context.has_claude_file else 'no'}")
    print("")


def choose_agent_file_mode(context: InstallContext) -> str:
    if context.has_agents_file or context.has_claude_file:
        prompt = (
            "How should lex handle existing root agent files?\n"
            "This controls whether AGENTS.md and CLAUDE.md get a small managed lex bridge block."
        )
        default = "merge"
        options = [
            ("preserve", "Preserve existing root agent files exactly as they are and install only the .lex scaffold"),
            ("merge", "Merge a managed lex block into existing root agent files without overwriting other content"),
            ("assisted", "Preserve root files now and prepare an assisted semantic merge packet for an agent"),
            ("overwrite", "Overwrite root agent files with lex bridge files"),
        ]
    else:
        prompt = "How should lex expose itself to root agent files?"
        default = "merge"
        options = [
            ("preserve", "Do not create root bridge files; install only the .lex scaffold"),
            ("merge", "Create root AGENTS.md and CLAUDE.md bridge files that point into .lex"),
            ("assisted", "Install the .lex scaffold and prepare an assisted merge packet before creating root bridge files"),
            ("overwrite", "Write fresh root bridge files even if they appear later in the install"),
        ]
    return prompt_choice(prompt, options, default)


def choose_ignore_policy(context: InstallContext) -> tuple[str, str]:
    if context.has_git_dir:
        prompt = (
            "How should lex handle Git ignore rules?\n"
            "Choose whether to ignore nothing, only runtime state, or the full lex workflow locally."
        )
        default = "runtime:gitignore"
        options = [
            ("runtime:gitignore", "Ignore only .lex/lex.db and .lex/runtime/ in the shared .gitignore"),
            ("all:local-exclude", "Keep lex local-only by writing ignore rules to .git/info/exclude"),
            ("none:gitignore", "Do not add any ignore rules"),
        ]
    else:
        prompt = "How should lex handle ignore rules in this folder?"
        default = "runtime:gitignore"
        options = [
            ("runtime:gitignore", "Create a .gitignore entry for lex runtime state"),
            ("none:gitignore", "Do not create ignore rules"),
        ]
    combined = prompt_choice(prompt, options, default)
    ignore_policy, ignore_target = combined.split(":", 1)
    return ignore_policy, ignore_target


def choose_assisted_agent() -> str:
    return prompt_choice(
        "Which agent should prepare the assisted merge proposal?",
        [
            ("codex", "Codex"),
            ("claude", "Claude"),
            ("manual", "Manual packet only"),
        ],
        "codex",
    )


def resolve_install_options(args: argparse.Namespace, context: InstallContext) -> tuple[str, str, str, str]:
    if args.non_interactive:
        return args.agent_files, args.ignore_policy, args.ignore_target, args.assisted_agent

    print_install_summary(context)
    agent_files = choose_agent_file_mode(context)
    print("")
    ignore_policy, ignore_target = choose_ignore_policy(context)
    assisted_agent = args.assisted_agent
    if agent_files == "assisted":
        print("")
        assisted_agent = choose_assisted_agent()
    print("")
    print("Installer choices:")
    print(f"  agent file mode: {agent_files}")
    print(f"  ignore policy: {ignore_policy}")
    print(f"  ignore target: {ignore_target}")
    if agent_files == "assisted":
        print(f"  assisted agent: {assisted_agent}")
    print("")
    return agent_files, ignore_policy, ignore_target, assisted_agent


def cmd_install(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    context = inspect_install_context(root)
    agent_files, ignore_policy, ignore_target, assisted_agent = resolve_install_options(args, context)
    result = install_scaffold(
        root,
        agent_files=agent_files,
        ignore_policy=ignore_policy,
        ignore_target=ignore_target,
    )
    paths = ensure_workspace(root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    log_event(
        conn,
        "workspace.installed",
        payload={
            "root": str(root),
            "agent_files": agent_files,
            "ignore_policy": ignore_policy,
            "ignore_target": ignore_target,
        },
    )
    if agent_files == "assisted":
        merge_paths = create_merge_packet(root, agent_kind=assisted_agent)
        log_event(
            conn,
            "merge.packet_created",
            payload={"agent_kind": assisted_agent, "plan_path": str(merge_paths.plan_path)},
        )
    conn.commit()
    print(f"installed lex in {root}")
    if result.created_files:
        print("created:")
        for path in result.created_files:
            print(f"  {path}")
    if result.updated_files:
        print("updated:")
        for path in result.updated_files:
            print(f"  {path}")
    if result.skipped_files:
        print("skipped existing scaffold:")
        for path in result.skipped_files:
            print(f"  {path}")
    if result.warnings:
        print("warnings:")
        for warning in result.warnings:
            print(f"  {warning}")
    if agent_files == "assisted":
        print("next steps:")
        print(f"  review {root / '.lex' / 'runtime' / 'install-merge-plan.md'}")
        print("  have the selected agent write proposals into .lex/runtime/install-merge-proposal/")
        print("  run `lex merge diff` and then `lex merge apply` once approved")


def cmd_merge_plan(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    paths = create_merge_packet(root, agent_kind=args.agent)
    print(f"created assisted merge packet in {paths.lex_runtime}")
    print(f"plan: {paths.plan_path}")
    print(f"context: {paths.context_dir}")
    print(f"proposal target: {paths.proposal_dir}")


def cmd_merge_diff(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    paths = resolve_merge_paths(root)
    diffs = []
    for filename in ("AGENTS.md", "CLAUDE.md"):
        diff_text = unified_diff(root / filename, paths.proposal_dir / filename, filename)
        if diff_text:
            diffs.append(diff_text)
    if not diffs:
        print("no proposal diffs found")
        return
    print("\n".join(diffs).rstrip())


def cmd_merge_apply(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    applied = apply_proposal(root)
    if not applied:
        print("no proposal files were applied")
        return
    paths = ensure_workspace(root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    log_event(conn, "merge.applied", payload={"files": applied})
    conn.commit()
    print("applied proposal files:")
    for filename in applied:
        print(f"  {filename}")


def cmd_agent_register(args: argparse.Namespace) -> None:
    require_agent_name(args.name)
    paths = ensure_workspace(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    role = normalize_agent_role(args.role)
    specialty = normalize_agent_specialty(conn, args.specialty)
    try:
        conn.execute(
            "INSERT INTO agents (name, kind, role, specialty, status) VALUES (?, ?, ?, ?, 'active')",
            (args.name, args.kind, role, specialty),
        )
    except sqlite3.IntegrityError as exc:
        raise SystemExit(f"failed to register agent: {exc}") from exc
    agent_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    log_event(
        conn,
        "agent.registered",
        agent_id=agent_id,
        payload={"name": args.name, "kind": args.kind, "role": role, "specialty": specialty},
    )
    conn.commit()
    print_ok(f"registered {args.name}")


def cmd_agent_identify(args: argparse.Namespace) -> None:
    paths = ensure_workspace(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    name = args.name or allocate_agent_name(conn, args.kind)
    require_agent_name(name)
    role = normalize_agent_role(args.role)
    specialty = normalize_agent_specialty(conn, args.specialty)
    try:
        conn.execute(
            "INSERT INTO agents (name, kind, role, specialty, status) VALUES (?, ?, ?, ?, 'active')",
            (name, args.kind, role, specialty),
        )
    except sqlite3.IntegrityError as exc:
        raise SystemExit(f"failed to identify agent: {exc}") from exc
    agent_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    log_event(
        conn,
        "agent.registered",
        agent_id=agent_id,
        payload={"name": name, "kind": args.kind, "role": role, "specialty": specialty, "identified": True},
    )
    conn.commit()
    if args.json:
        emit_json(
            {"id": agent_id, "name": name, "kind": args.kind, "role": role, "specialty": specialty, "status": "active"}
        )
        return
    print_ok(f"identified {name}")


def cmd_agent_update_role(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    agent = get_agent(conn, args.agent)
    role = normalize_agent_role(args.role)
    specialty = normalize_agent_specialty(conn, args.specialty)
    conn.execute(
        "UPDATE agents SET role = ?, specialty = ? WHERE id = ?",
        (role, specialty, agent["id"]),
    )
    log_event(
        conn,
        "agent.role_changed",
        agent_id=agent["id"],
        payload={
            "name": args.agent,
            "from": agent["role"],
            "from_specialty": agent["specialty"],
            "to": role,
            "to_specialty": specialty,
        },
    )
    conn.commit()
    label = role if not specialty else f"{role}/{specialty}"
    print_ok(f"{args.agent} role -> {label or '-'}")


def cmd_agent_list(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    rows = conn.execute("SELECT id, name, kind, role, specialty, status, created_at FROM agents ORDER BY id").fetchall()
    if args.json:
        emit_json([dict(row) for row in rows])
        return
    render_agent_list(rows)


def cmd_specialty_add(args: argparse.Namespace) -> None:
    paths = ensure_workspace(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    specialty = (args.name or "").strip()
    if not specialty:
        raise SystemExit("specialty name is required")
    if specialty in BUILTIN_SPECIALTIES:
        print_ok(f"specialty {specialty} already available")
        return
    try:
        conn.execute("INSERT INTO specialties (name) VALUES (?)", (specialty,))
    except sqlite3.IntegrityError:
        print_ok(f"specialty {specialty} already available")
        return
    log_event(conn, "specialty.added", payload={"name": specialty})
    conn.commit()
    print_ok(f"added specialty {specialty}")


def cmd_specialty_list(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    rows = [{"name": name, "source": "builtin" if name in BUILTIN_SPECIALTIES else "custom"} for name in list_specialties(conn)]
    if args.json:
        emit_json(rows)
        return
    for row in rows:
        print(f"{row['name']:<16} {row['source']}")


def cmd_session_start(args: argparse.Namespace) -> None:
    paths = ensure_workspace(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    agent = get_agent(conn, args.agent)
    fingerprint = args.fingerprint
    fingerprint_label = args.fingerprint_label
    if fingerprint is None or fingerprint_label is None:
        fingerprint, fingerprint_label = build_session_fingerprint(kind=agent["kind"], cwd=args.cwd)
    conflicting_sessions = conn.execute(
        """
        SELECT s.id, s.label, s.fingerprint_label
        FROM sessions s
        WHERE s.agent_id = ?
          AND s.status = 'active'
          AND s.ended_at IS NULL
          AND s.fingerprint IS NOT NULL
          AND s.fingerprint != ?
        ORDER BY s.id DESC
        """,
        (agent["id"], fingerprint),
    ).fetchall()
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, fingerprint, fingerprint_label, status, cwd, capabilities_json)
        VALUES (?, ?, ?, ?, 'active', ?, ?)
        """,
        (
            agent["id"],
            args.label,
            fingerprint,
            fingerprint_label,
            args.cwd,
            json.dumps({"capabilities": args.capability or []}),
        ),
    )
    session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    log_event(
        conn,
        "session.started",
        agent_id=agent["id"],
        session_id=session_id,
        payload={"label": args.label, "cwd": args.cwd, "fingerprint": fingerprint, "fingerprint_label": fingerprint_label},
    )
    conn.commit()
    print_ok(f"started session {session_id} for {args.agent}")
    print_info(f"instance {fingerprint_label} ({fingerprint})")
    if conflicting_sessions:
        other = conflicting_sessions[0]
        print_info(
            "warning: another active session exists for this agent "
            f"(session {other['id']} {other['label'] or '-'} @ {other['fingerprint_label'] or '-'})"
        )


def cmd_session_heartbeat(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    session = get_session(conn, args.session_id)
    conn.execute(
        """
        UPDATE sessions
        SET heartbeat_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'active' AND ended_at IS NULL
        """,
        (args.session_id,),
    )
    log_event(
        conn,
        "session.heartbeat",
        agent_id=session["agent_id"],
        session_id=args.session_id,
        payload={"label": session["label"]},
    )
    conn.commit()
    print_ok(f"heartbeat recorded for session {args.session_id}")


def cmd_session_end(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    session = get_session(conn, args.session_id)
    conn.execute(
        """
        UPDATE sessions
        SET status = 'ended',
            heartbeat_at = CURRENT_TIMESTAMP,
            ended_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (args.session_id,),
    )
    released_count = release_stale_leases(conn)
    log_event(
        conn,
        "session.ended",
        agent_id=session["agent_id"],
        session_id=args.session_id,
        payload={"label": session["label"], "released_leases": released_count},
    )
    conn.commit()
    print_ok(f"ended session {args.session_id}")


def cmd_session_list(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    params: list[object] = []
    query = """
        SELECT
            s.id,
            a.name AS agent_name,
            a.kind AS agent_kind,
            s.label,
            s.fingerprint,
            s.fingerprint_label,
            s.status,
            s.cwd,
            s.started_at,
            s.heartbeat_at,
            s.ended_at
        FROM sessions s
        JOIN agents a ON a.id = s.agent_id
    """
    if args.active_only:
        query += " WHERE s.status = 'active' AND s.ended_at IS NULL"
    query += " ORDER BY s.id DESC"
    rows = conn.execute(query, tuple(params)).fetchall()
    if args.json:
        emit_json([dict(row) for row in rows])
        return
    render_session_list(rows)


def cmd_task_create(args: argparse.Namespace) -> None:
    paths = ensure_workspace(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    creator_id = None
    if args.created_by:
        creator_id = get_agent(conn, args.created_by)["id"]
    claimed_paths = args.path or []
    conn.execute(
        """
        INSERT INTO tasks (
            slug, title, description, status, priority, parent_task_id,
            delegation_mode, claimed_paths_json, created_by_agent_id
        )
        VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?)
        """,
        (
            args.slug,
            args.title,
            args.description or "",
            args.priority,
            args.parent_task,
            args.delegation_mode,
            json.dumps(claimed_paths),
            creator_id,
        ),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    log_event(
        conn,
        "task.created",
        task_id=task_id,
        agent_id=creator_id,
        payload={"title": args.title, "parent_task_id": args.parent_task},
    )
    conn.commit()
    print_ok(f"created task {task_id}")


def cmd_task_list(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    release_stale_leases(conn)
    rows = conn.execute(
        """
        SELECT
            t.id,
            t.title,
            t.status,
            t.priority,
            t.parent_task_id,
            a.name AS owner_name
        FROM tasks t
        LEFT JOIN agents a ON a.id = t.owner_agent_id
        ORDER BY t.id
        """
    ).fetchall()
    if args.json:
        emit_json([dict(row) for row in rows])
        return
    render_task_list(rows)


def cmd_task_show(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    release_stale_leases(conn)
    task = get_task_with_owner(conn, args.task_id)
    lease = get_active_lease(conn, args.task_id)
    owner_session = None
    if task["owner_agent_id"] is not None:
        owner_session = get_active_session_for_agent(conn, task["owner_agent_id"])
    children = conn.execute(
        """
        SELECT
            t.id,
            t.title,
            t.status,
            a.name AS owner_name
        FROM tasks t
        LEFT JOIN agents a ON a.id = t.owner_agent_id
        WHERE t.parent_task_id = ?
        ORDER BY t.id
        """,
        (args.task_id,),
    ).fetchall()
    recent_messages = conn.execute(
        """
        SELECT
            m.id,
            sender.name AS from_name,
            recipient.name AS to_name,
            m.type,
            m.body,
            m.created_at
        FROM messages m
        JOIN agents sender ON sender.id = m.from_agent_id
        LEFT JOIN agents recipient ON recipient.id = m.to_agent_id
        WHERE m.task_id = ?
        ORDER BY m.id DESC
        LIMIT 10
        """,
        (args.task_id,),
    ).fetchall()
    data = {
        "id": task["id"],
        "slug": task["slug"],
        "title": task["title"],
        "description": task["description"],
        "status": task["status"],
        "priority": task["priority"],
        "owner": task["owner_name"],
        "owner_role": task["owner_role"],
        "owner_specialty": task["owner_specialty"],
        "created_by": task["created_by_name"],
        "parent_task_id": task["parent_task_id"],
        "delegation_mode": task["delegation_mode"],
        "claimed_paths": json.loads(task["claimed_paths_json"]),
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
        "completed_at": task["completed_at"],
        "active_lease": dict(lease) if lease else None,
        "active_owner_session": dict(owner_session) if owner_session else None,
        "children": [dict(row) for row in children],
        "recent_messages": [dict(row) for row in recent_messages],
    }
    if args.json:
        emit_json(data)
        return
    render_task_show(data, lease, owner_session, children, recent_messages)


def cmd_task_delegate(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    parent_task = get_task(conn, args.parent_task_id)
    owner = get_agent(conn, args.owner_agent)
    assignee = get_agent(conn, args.assignee_agent)
    assignee_session = get_active_session_for_agent(conn, assignee["id"])
    if parent_task["owner_agent_id"] not in (None, owner["id"]):
        raise SystemExit("only the parent owner may delegate child tasks")
    if parent_task["delegation_mode"] != "hypervisor":
        raise SystemExit("parent task must be in hypervisor delegation mode")
    conn.execute(
        """
        INSERT INTO tasks (
            slug, title, description, status, priority, owner_agent_id, parent_task_id,
            delegation_mode, claimed_paths_json, created_by_agent_id
        )
        VALUES (?, ?, ?, 'claimed', ?, ?, ?, 'direct', ?, ?)
        """,
        (
            args.slug,
            args.title,
            args.description or "",
            args.priority if args.priority is not None else parent_task["priority"],
            assignee["id"],
            args.parent_task_id,
            json.dumps(args.path or []),
            owner["id"],
        ),
    )
    child_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO task_leases (task_id, agent_id, session_id, expires_at)
        VALUES (?, ?, ?, datetime('now', ?))
        """,
        (child_task_id, assignee["id"], assignee_session["id"] if assignee_session else None, f"+{args.ttl_minutes} minutes"),
    )
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, ?, 'handoff', ?, ?)
        """,
        (
            child_task_id,
            owner["id"],
            assignee["id"],
            args.subject or "delegated child task",
            args.body,
        ),
    )
    log_event(
        conn,
        "task.delegated",
        task_id=child_task_id,
        agent_id=owner["id"],
        payload={"parent_task_id": args.parent_task_id, "assignee": args.assignee_agent},
    )
    conn.commit()
    print_ok(f"delegated child task {child_task_id} to {args.assignee_agent}")


def cmd_task_claim(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    release_stale_leases(conn)
    task = get_task(conn, args.task_id)
    agent = get_agent(conn, args.agent)
    session = get_active_session_for_agent(conn, agent["id"])
    active_lease = fetch_one(
        conn,
        """
        SELECT * FROM task_leases
        WHERE task_id = ? AND state = 'active' AND released_at IS NULL
          AND expires_at > CURRENT_TIMESTAMP
        ORDER BY id DESC
        LIMIT 1
        """,
        (args.task_id,),
    )
    if active_lease is not None and active_lease["agent_id"] != agent["id"]:
        raise SystemExit(f"task {args.task_id} is already leased by another agent")
    if active_lease is None:
        conn.execute(
            """
            INSERT INTO task_leases (task_id, agent_id, session_id, expires_at)
            VALUES (?, ?, ?, datetime('now', ?))
            """,
            (args.task_id, agent["id"], session["id"] if session else None, f"+{args.ttl_minutes} minutes"),
        )
    conn.execute(
        """
        UPDATE tasks
        SET owner_agent_id = ?, status = CASE WHEN status = 'open' THEN 'claimed' ELSE status END
        WHERE id = ?
        """,
        (agent["id"], args.task_id),
    )
    log_event(
        conn,
        "task.claimed",
        task_id=args.task_id,
        agent_id=agent["id"],
        payload={"ttl_minutes": args.ttl_minutes, "previous_status": task["status"]},
    )
    conn.commit()
    print_ok(f"claimed task {args.task_id} for {args.agent}")


def cmd_task_update_priority(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    task = get_task(conn, args.task_id)
    agent = get_agent(conn, args.agent)
    if task["owner_agent_id"] not in (None, agent["id"]):
        raise SystemExit("only the owner may update task priority")
    conn.execute(
        "UPDATE tasks SET priority = ?, owner_agent_id = ? WHERE id = ?",
        (args.priority, agent["id"], args.task_id),
    )
    log_event(
        conn,
        "task.priority_changed",
        task_id=args.task_id,
        agent_id=agent["id"],
        payload={"from": task["priority"], "to": args.priority},
    )
    conn.commit()
    print_ok(f"task {args.task_id} priority -> p{args.priority}")


def cmd_task_update_status(args: argparse.Namespace) -> None:
    if args.status not in VALID_TASK_STATES:
        raise SystemExit(f"invalid task status: {args.status}")
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    task = get_task(conn, args.task_id)
    agent = get_agent(conn, args.agent)
    if task["owner_agent_id"] not in (None, agent["id"]):
        raise SystemExit("only the owner may update task status")
    completed_at = "CURRENT_TIMESTAMP" if args.status == "done" else None
    if completed_at:
        conn.execute(
            "UPDATE tasks SET status = ?, owner_agent_id = ?, completed_at = CURRENT_TIMESTAMP WHERE id = ?",
            (args.status, agent["id"], args.task_id),
        )
    else:
        conn.execute(
            "UPDATE tasks SET status = ?, owner_agent_id = ?, completed_at = NULL WHERE id = ?",
            (args.status, agent["id"], args.task_id),
        )
    log_event(
        conn,
        "task.status_changed",
        task_id=args.task_id,
        agent_id=agent["id"],
        payload={"from": task["status"], "to": args.status},
    )
    conn.commit()
    print_ok(f"task {args.task_id} → {args.status}")


def cmd_task_handoff(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    task = get_task(conn, args.task_id)
    from_agent = get_agent(conn, args.from_agent)
    to_agent = get_agent(conn, args.to_agent)
    if task["owner_agent_id"] not in (None, from_agent["id"]):
        raise SystemExit("only the owner may hand off the task")
    conn.execute(
        "UPDATE tasks SET owner_agent_id = ?, status = 'handoff_pending' WHERE id = ?",
        (to_agent["id"], args.task_id),
    )
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, ?, 'handoff', ?, ?)
        """,
        (args.task_id, from_agent["id"], to_agent["id"], args.subject or "handoff", args.body),
    )
    conn.execute(
        """
        UPDATE task_leases
        SET state = 'released', released_at = CURRENT_TIMESTAMP
        WHERE task_id = ? AND agent_id = ? AND state = 'active' AND released_at IS NULL
        """,
        (args.task_id, from_agent["id"]),
    )
    log_event(
        conn,
        "task.handoff",
        task_id=args.task_id,
        agent_id=from_agent["id"],
        payload={"to_agent": args.to_agent},
    )
    conn.commit()
    print_ok(f"handed off task {args.task_id} to {args.to_agent}")


def cmd_lease_renew(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    agent = get_agent(conn, args.agent)
    updated = conn.execute(
        """
        UPDATE task_leases
        SET heartbeat_at = CURRENT_TIMESTAMP,
            expires_at = datetime('now', ?)
        WHERE task_id = ? AND agent_id = ? AND state = 'active' AND released_at IS NULL
        """,
        (f"+{args.ttl_minutes} minutes", args.task_id, agent["id"]),
    ).rowcount
    if updated == 0:
        raise SystemExit("no active lease found to renew")
    log_event(
        conn,
        "lease.renewed",
        task_id=args.task_id,
        agent_id=agent["id"],
        payload={"ttl_minutes": args.ttl_minutes},
    )
    conn.commit()
    print_ok(f"renewed lease for task {args.task_id}")


def cmd_msg_send(args: argparse.Namespace) -> None:
    require_message_type(args.type)
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    from_agent = get_agent(conn, args.from_agent)
    to_agent = get_agent(conn, args.to_agent) if args.to_agent else None
    if args.task_id is not None:
        get_task(conn, args.task_id)
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            args.task_id,
            from_agent["id"],
            to_agent["id"] if to_agent else None,
            args.type,
            args.subject or "",
            args.body,
        ),
    )
    log_event(
        conn,
        "message.sent",
        task_id=args.task_id,
        agent_id=from_agent["id"],
        payload={"type": args.type, "to": args.to_agent},
    )
    conn.commit()
    print_ok("message sent")


def cmd_msg_inbox(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    agent = get_agent(conn, args.agent)
    def fetch_rows(since_id: int | None) -> list[sqlite3.Row]:
        params: list[object] = [agent["id"], agent["id"], agent["id"], agent["id"]]
        query = """
            SELECT
                m.id,
                m.task_id,
                t.title AS task_title,
                sender.name AS from_name,
                recipient.name AS to_name,
                m.type,
                m.subject,
                m.body,
                m.created_at
            FROM messages m
            LEFT JOIN tasks t ON t.id = m.task_id
            JOIN agents sender ON sender.id = m.from_agent_id
            LEFT JOIN agents recipient ON recipient.id = m.to_agent_id
            WHERE (
                m.to_agent_id = ?
                OR (
                    m.to_agent_id IS NULL
                    AND m.task_id IS NOT NULL
                    AND (
                        EXISTS (
                            SELECT 1
                            FROM tasks task_scope
                            WHERE task_scope.id = m.task_id
                              AND task_scope.owner_agent_id = ?
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM messages participation
                            WHERE participation.task_id = m.task_id
                              AND (
                                  participation.from_agent_id = ?
                                  OR participation.to_agent_id = ?
                              )
                        )
                    )
                )
            )
        """
        if since_id is not None:
            query += " AND m.id > ?"
            params.append(since_id)
            query += " ORDER BY m.id ASC LIMIT ?"
        else:
            query += " ORDER BY m.id DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(query, tuple(params)).fetchall()
        if since_id is None:
            rows = list(reversed(rows))
        return list(rows)

    rows = fetch_rows(args.since_id)
    if args.json:
        emit_json([dict(row) for row in rows])
        return
    follow_rows(
        fetch_rows,
        render_inbox_rows,
        initial_rows=rows,
        initial_since_id=args.since_id,
        follow=args.follow,
        poll_interval=args.poll_interval,
        max_polls=args.max_polls,
    )


def cmd_msg_task(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    get_task(conn, args.task_id)
    def fetch_rows(since_id: int | None) -> list[sqlite3.Row]:
        params: list[object] = [args.task_id]
        query = """
            SELECT
                m.id,
                sender.name AS from_name,
                recipient.name AS to_name,
                m.type,
                m.subject,
                m.body,
                m.created_at
            FROM messages m
            JOIN agents sender ON sender.id = m.from_agent_id
            LEFT JOIN agents recipient ON recipient.id = m.to_agent_id
            WHERE m.task_id = ?
        """
        if since_id is not None:
            query += " AND m.id > ?"
            params.append(since_id)
            query += " ORDER BY m.id ASC LIMIT ?"
        else:
            query += " ORDER BY m.id DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(query, tuple(params)).fetchall()
        if since_id is None:
            rows = list(reversed(rows))
        return list(rows)

    rows = fetch_rows(args.since_id)
    if args.json:
        emit_json([dict(row) for row in rows])
        return
    follow_rows(
        fetch_rows,
        render_task_message_rows,
        initial_rows=rows,
        initial_since_id=args.since_id,
        follow=args.follow,
        poll_interval=args.poll_interval,
        max_polls=args.max_polls,
    )


def cmd_event_list(args: argparse.Namespace) -> None:
    paths = resolve_paths(args.root)
    conn = connect(paths.db_path)
    initialize_database(conn)
    agent = get_agent(conn, args.agent) if args.agent is not None else None

    def fetch_rows(since_id: int | None) -> list[sqlite3.Row]:
        params: list[object] = []
        query = """
            SELECT
                e.id,
                e.event_type,
                e.task_id,
                a.name AS agent_name,
                e.payload_json,
                e.created_at
            FROM events e
            LEFT JOIN agents a ON a.id = e.agent_id
        """
        where = []
        if args.task_id is not None:
            where.append("e.task_id = ?")
            params.append(args.task_id)
        if agent is not None:
            where.append("e.agent_id = ?")
            params.append(agent["id"])
        if since_id is not None:
            where.append("e.id > ?")
            params.append(since_id)
        if where:
            query += " WHERE " + " AND ".join(where)
        if since_id is not None:
            query += " ORDER BY e.id ASC LIMIT ?"
        else:
            query += " ORDER BY e.id DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(query, tuple(params)).fetchall()
        if since_id is None:
            rows = list(reversed(rows))
        return list(rows)

    rows = fetch_rows(args.since_id)
    data = [
        {
            **dict(row),
            "payload": json.loads(row["payload_json"]),
        }
        for row in rows
    ]
    if args.json:
        emit_json(data)
        return
    follow_rows(
        fetch_rows,
        render_event_list,
        initial_rows=rows,
        initial_since_id=args.since_id,
        follow=args.follow,
        poll_interval=args.poll_interval,
        max_polls=args.max_polls,
    )


def add_follow_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--follow", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--max-polls", type=int)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lex")
    parser.add_argument("--root", default=".", help="workspace root")
    subparsers = parser.add_subparsers(dest="command")

    init_parser = subparsers.add_parser("init")
    init_parser.set_defaults(func=cmd_init)

    install_parser = subparsers.add_parser("install")
    install_parser.add_argument("--agent-files", choices=["preserve", "merge", "assisted", "overwrite"], default="merge")
    install_parser.add_argument("--ignore-policy", choices=["none", "runtime", "all"], default="runtime")
    install_parser.add_argument("--ignore-target", choices=["gitignore", "local-exclude"], default="gitignore")
    install_parser.add_argument("--assisted-agent", choices=["codex", "claude", "manual"], default="codex")
    install_parser.add_argument("--non-interactive", action="store_true")
    install_parser.set_defaults(func=cmd_install)

    merge_parser = subparsers.add_parser("merge")
    merge_sub = merge_parser.add_subparsers(dest="merge_command", required=True)

    merge_plan = merge_sub.add_parser("plan")
    merge_plan.add_argument("--agent", choices=["codex", "claude", "manual"], default="codex")
    merge_plan.set_defaults(func=cmd_merge_plan)

    merge_diff = merge_sub.add_parser("diff")
    merge_diff.set_defaults(func=cmd_merge_diff)

    merge_apply = merge_sub.add_parser("apply")
    merge_apply.set_defaults(func=cmd_merge_apply)

    agent_parser = subparsers.add_parser("agent")
    agent_sub = agent_parser.add_subparsers(dest="agent_command", required=True)

    agent_identify = agent_sub.add_parser("identify")
    agent_identify.add_argument("kind", choices=["codex", "claude", "cursor"])
    agent_identify.add_argument("--name")
    agent_identify.add_argument("--role")
    agent_identify.add_argument("--specialty")
    agent_identify.add_argument("--json", action="store_true")
    agent_identify.set_defaults(func=cmd_agent_identify)

    agent_register = agent_sub.add_parser("register")
    agent_register.add_argument("name")
    agent_register.add_argument("kind", choices=["codex", "claude", "cursor"])
    agent_register.add_argument("--role")
    agent_register.add_argument("--specialty")
    agent_register.set_defaults(func=cmd_agent_register)

    agent_role = agent_sub.add_parser("role")
    agent_role.add_argument("agent")
    agent_role.add_argument("role")
    agent_role.add_argument("--specialty")
    agent_role.set_defaults(func=cmd_agent_update_role)

    specialty_parser = subparsers.add_parser("specialty")
    specialty_sub = specialty_parser.add_subparsers(dest="specialty_command", required=True)

    specialty_add = specialty_sub.add_parser("add")
    specialty_add.add_argument("name")
    specialty_add.set_defaults(func=cmd_specialty_add)

    specialty_list = specialty_sub.add_parser("list")
    specialty_list.add_argument("--json", action="store_true")
    specialty_list.set_defaults(func=cmd_specialty_list)

    agent_list = agent_sub.add_parser("list")
    agent_list.add_argument("--json", action="store_true")
    agent_list.set_defaults(func=cmd_agent_list)

    session_parser = subparsers.add_parser("session")
    session_sub = session_parser.add_subparsers(dest="session_command", required=True)

    session_start = session_sub.add_parser("start")
    session_start.add_argument("agent")
    session_start.add_argument("--label")
    session_start.add_argument("--cwd", default=".")
    session_start.add_argument("--capability", action="append")
    session_start.add_argument("--fingerprint")
    session_start.add_argument("--fingerprint-label")
    session_start.set_defaults(func=cmd_session_start)

    session_heartbeat = session_sub.add_parser("heartbeat")
    session_heartbeat.add_argument("session_id", type=int)
    session_heartbeat.set_defaults(func=cmd_session_heartbeat)

    session_end = session_sub.add_parser("end")
    session_end.add_argument("session_id", type=int)
    session_end.set_defaults(func=cmd_session_end)

    session_list = session_sub.add_parser("list")
    session_list.add_argument("--active-only", action="store_true")
    session_list.add_argument("--json", action="store_true")
    session_list.set_defaults(func=cmd_session_list)

    task_parser = subparsers.add_parser("task")
    task_sub = task_parser.add_subparsers(dest="task_command", required=True)

    task_create = task_sub.add_parser("create")
    task_create.add_argument("title")
    task_create.add_argument("--slug")
    task_create.add_argument("--description")
    task_create.add_argument("--priority", type=int, default=2)
    task_create.add_argument("--created-by")
    task_create.add_argument("--parent-task", type=int)
    task_create.add_argument("--delegation-mode", default="direct", choices=["direct", "hypervisor"])
    task_create.add_argument("--path", action="append")
    task_create.set_defaults(func=cmd_task_create)

    task_list = task_sub.add_parser("list")
    task_list.add_argument("--json", action="store_true")
    task_list.set_defaults(func=cmd_task_list)

    task_show = task_sub.add_parser("show")
    task_show.add_argument("task_id", type=int)
    task_show.add_argument("--json", action="store_true")
    task_show.set_defaults(func=cmd_task_show)

    task_claim = task_sub.add_parser("claim")
    task_claim.add_argument("task_id", type=int)
    task_claim.add_argument("agent")
    task_claim.add_argument("--ttl-minutes", type=int, default=30)
    task_claim.set_defaults(func=cmd_task_claim)

    task_priority = task_sub.add_parser("priority")
    task_priority.add_argument("task_id", type=int)
    task_priority.add_argument("agent")
    task_priority.add_argument("priority", type=int)
    task_priority.set_defaults(func=cmd_task_update_priority)

    task_delegate = task_sub.add_parser("delegate")
    task_delegate.add_argument("parent_task_id", type=int)
    task_delegate.add_argument("owner_agent")
    task_delegate.add_argument("assignee_agent")
    task_delegate.add_argument("title")
    task_delegate.add_argument("--slug")
    task_delegate.add_argument("--description")
    task_delegate.add_argument("--subject")
    task_delegate.add_argument("--body", required=True)
    task_delegate.add_argument("--priority", type=int)
    task_delegate.add_argument("--ttl-minutes", type=int, default=30)
    task_delegate.add_argument("--path", action="append")
    task_delegate.set_defaults(func=cmd_task_delegate)

    task_status = task_sub.add_parser("status")
    task_status.add_argument("task_id", type=int)
    task_status.add_argument("agent")
    task_status.add_argument("status")
    task_status.set_defaults(func=cmd_task_update_status)

    task_handoff = task_sub.add_parser("handoff")
    task_handoff.add_argument("task_id", type=int)
    task_handoff.add_argument("from_agent")
    task_handoff.add_argument("to_agent")
    task_handoff.add_argument("--subject")
    task_handoff.add_argument("--body", required=True)
    task_handoff.set_defaults(func=cmd_task_handoff)

    lease_parser = subparsers.add_parser("lease")
    lease_sub = lease_parser.add_subparsers(dest="lease_command", required=True)
    lease_renew = lease_sub.add_parser("renew")
    lease_renew.add_argument("task_id", type=int)
    lease_renew.add_argument("agent")
    lease_renew.add_argument("--ttl-minutes", type=int, default=30)
    lease_renew.set_defaults(func=cmd_lease_renew)

    msg_parser = subparsers.add_parser("msg")
    msg_sub = msg_parser.add_subparsers(dest="msg_command", required=True)

    msg_send = msg_sub.add_parser("send")
    msg_send.add_argument("--task", dest="task_id", type=int)
    msg_send.add_argument("--from", dest="from_agent", required=True)
    msg_send.add_argument("--to", dest="to_agent")
    msg_send.add_argument("--type", required=True)
    msg_send.add_argument("--subject")
    msg_send.add_argument("--body", required=True)
    msg_send.set_defaults(func=cmd_msg_send)

    msg_inbox = msg_sub.add_parser("inbox")
    msg_inbox.add_argument("agent")
    msg_inbox.add_argument("--limit", type=int, default=20)
    msg_inbox.add_argument("--json", action="store_true")
    add_follow_arguments(msg_inbox)
    msg_inbox.set_defaults(func=cmd_msg_inbox)

    msg_task = msg_sub.add_parser("task")
    msg_task.add_argument("task_id", type=int)
    msg_task.add_argument("--limit", type=int, default=20)
    msg_task.add_argument("--json", action="store_true")
    add_follow_arguments(msg_task)
    msg_task.set_defaults(func=cmd_msg_task)

    event_parser = subparsers.add_parser("event")
    event_sub = event_parser.add_subparsers(dest="event_command", required=True)

    event_list = event_sub.add_parser("list")
    event_list.add_argument("--task", dest="task_id", type=int)
    event_list.add_argument("--agent")
    event_list.add_argument("--limit", type=int, default=20)
    event_list.add_argument("--json", action="store_true")
    add_follow_arguments(event_list)
    event_list.set_defaults(func=cmd_event_list)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        root = Path(args.root).resolve()
        if sys.stdin.isatty() and sys.stdout.isatty():
            try:
                run_tui(root)
            except Exception:
                run_interactive_shell(root)
        else:
            run_interactive_shell(root)
        return
    args.func(args)


if __name__ == "__main__":
    main()

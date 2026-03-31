from __future__ import annotations

import json
import sqlite3

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


# ── style maps ────────────────────────────────────────────────────────────────

STATUS_ICON: dict[str, str] = {
    "open":             "◌",
    "claimed":          "⊙",
    "in_progress":      "●",
    "blocked":          "⊘",
    "review_requested": "⊛",
    "handoff_pending":  "⇢",
    "done":             "✔",
    "abandoned":        "✘",
}

STATUS_STYLE: dict[str, str] = {
    "open":             "bright_black",
    "claimed":          "yellow",
    "in_progress":      "cyan",
    "blocked":          "red",
    "review_requested": "magenta",
    "handoff_pending":  "blue",
    "done":             "green",
    "abandoned":        "bright_black",
}

AGENT_STYLE: dict[str, str] = {
    "codex":  "bright_cyan",
    "claude": "bright_magenta",
    "cursor": "bright_yellow",
}

MSG_TYPE_STYLE: dict[str, str] = {
    "handoff":         "blue",
    "review_request":  "magenta",
    "review_result":   "bright_magenta",
    "blocker":         "bold red",
    "note":            "white",
    "question":        "yellow",
    "answer":          "green",
    "decision":        "bright_cyan",
    "artifact_notice": "cyan",
}

PRIORITY_MARKUP: dict[int, str] = {
    1: "[bold red]p1[/bold red]",
    2: "p2",
    3: "[dim]p3[/dim]",
}


def _status_text(status: str) -> Text:
    icon = STATUS_ICON.get(status, "?")
    style = STATUS_STYLE.get(status, "white")
    t = Text()
    t.append(f"{icon} ", style=style)
    t.append(status, style=style)
    return t


def _agent_text(name: str | None, kind: str | None = None) -> Text:
    if not name:
        return Text("-", style="bright_black")
    style = AGENT_STYLE.get(kind or "", "white")
    return Text(name, style=style)


def _msg_type_text(msg_type: str) -> Text:
    return Text(msg_type, style=MSG_TYPE_STYLE.get(msg_type, "white"))


def _role_label(role: str | None, specialty: str | None = None) -> str:
    role_text = role or "-"
    if specialty:
        return f"{role_text}/{specialty}"
    return role_text


# ── list views ────────────────────────────────────────────────────────────────

def render_task_list(rows: list[sqlite3.Row]) -> None:
    table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold", expand=True)
    table.add_column("#", style="bright_black", width=4, justify="right", no_wrap=True)
    table.add_column("Status", width=16, no_wrap=True)
    table.add_column("P", width=2, justify="right", no_wrap=True)
    table.add_column("Owner", width=20, no_wrap=True)
    table.add_column("^", width=3, justify="right", no_wrap=True)
    table.add_column("Title", ratio=1)
    for row in rows:
        parent = str(row["parent_task_id"]) if row["parent_task_id"] is not None else ""
        table.add_row(
            str(row["id"]),
            _status_text(row["status"]),
            str(row["priority"]),
            _agent_text(row["owner_name"]),
            parent,
            row["title"],
        )
    console.print(table)


def render_agent_list(rows: list[sqlite3.Row]) -> None:
    table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold", expand=False)
    table.add_column("#", style="bright_black", width=4, justify="right")
    table.add_column("Name", min_width=24)
    table.add_column("Kind", width=8)
    table.add_column("Role", min_width=16)
    table.add_column("Status", width=10)
    for row in rows:
        style = AGENT_STYLE.get(row["kind"], "white")
        status_style = "green" if row["status"] == "active" else "bright_black"
        table.add_row(
            str(row["id"]),
            Text(row["name"], style=style),
            row["kind"],
            _role_label(row["role"], row["specialty"]),
            Text(row["status"], style=status_style),
        )
    console.print(table)


def render_session_list(rows: list[sqlite3.Row]) -> None:
    table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold", expand=False)
    table.add_column("#", style="bright_black", width=4, justify="right")
    table.add_column("Agent", min_width=24)
    table.add_column("Kind", width=8)
    table.add_column("Role", min_width=16)
    table.add_column("Status", width=8)
    table.add_column("Label", min_width=14)
    table.add_column("Instance", min_width=18)
    table.add_column("Heartbeat")
    for row in rows:
        style = AGENT_STYLE.get(row["agent_kind"], "white")
        status_style = "green" if row["status"] == "active" else "bright_black"
        table.add_row(
            str(row["id"]),
            Text(row["agent_name"], style=style),
            row["agent_kind"],
            _role_label(row["agent_role"], row["agent_specialty"]),
            Text(row["status"], style=status_style),
            row["label"] or "-",
            row["fingerprint_label"] or "-",
            Text(row["heartbeat_at"], style="bright_black"),
        )
    console.print(table)


def render_event_list(rows: list[sqlite3.Row]) -> None:
    table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold", expand=False)
    table.add_column("#", style="bright_black", width=4, justify="right")
    table.add_column("Time", style="bright_black")
    table.add_column("Task", width=5, justify="right", style="bright_black")
    table.add_column("Agent", min_width=22)
    table.add_column("Role", min_width=14)
    table.add_column("Event")
    table.add_column("Payload", style="bright_black")
    for row in rows:
        payload = json.loads(row["payload_json"])
        payload_str = json.dumps(payload, sort_keys=True) if payload else ""
        task_str = str(row["task_id"]) if row["task_id"] is not None else ""
        table.add_row(
            str(row["id"]),
            row["created_at"],
            task_str,
            row["agent_name"] or "-",
            _role_label(row["agent_role"], row["agent_specialty"]),
            Text(row["event_type"], style="cyan"),
            payload_str,
        )
    console.print(table)


def render_inbox_rows(rows: list[sqlite3.Row]) -> None:
    for row in rows:
        if row["task_id"] is not None:
            title = row["task_title"] or "(untitled)"
            task_label = Text.assemble(
                ("task ", "bright_black"),
                (str(row["task_id"]), "bright_black"),
                ("  ", ""),
                (title, "dim"),
            )
        else:
            task_label = Text("direct", style="bright_black")

        header = Text.assemble(
            ("[", "bright_black"),
            (str(row["id"]), "white"),
            ("]  ", "bright_black"),
            _msg_type_text(row["type"]),
            ("  from ", "bright_black"),
            (row["from_name"], "bold"),
            ("  ", ""),
            (row["created_at"], "bright_black"),
        )
        lines: list[Text] = [task_label]
        if row["subject"]:
            lines.append(Text.assemble(("subject  ", "bright_black"), (row["subject"], "italic")))
        lines.append(Text(row["body"]))
        body = Text("\n").join(lines)
        console.print(Panel(body, title=header, title_align="left", border_style="bright_black", padding=(0, 1)))


def render_task_message_rows(rows: list[sqlite3.Row]) -> None:
    for row in rows:
        target = Text.assemble((" → ", "bright_black"), (row["to_name"], "")) if row["to_name"] else Text("")
        header = Text.assemble(
            ("[", "bright_black"),
            (str(row["id"]), "white"),
            ("]  ", "bright_black"),
            _msg_type_text(row["type"]),
            ("  ", ""),
            (row["from_name"], "bold"),
            target,
            ("  ", ""),
            (row["created_at"], "bright_black"),
        )
        lines: list[Text] = []
        if row["subject"]:
            lines.append(Text.assemble(("subject  ", "bright_black"), (row["subject"], "italic")))
        lines.append(Text(row["body"]))
        body = Text("\n").join(lines)
        console.print(Panel(body, title=header, title_align="left", border_style="bright_black", padding=(0, 1)))


def render_task_show(data: dict, lease, owner_session, children, recent_messages) -> None:
    meta = Table.grid(padding=(0, 2))
    meta.add_column(style="bright_black", min_width=12)
    meta.add_column()
    meta.add_row("status", _status_text(data["status"]))
    meta.add_row("priority", Text.from_markup(PRIORITY_MARKUP.get(data["priority"], str(data["priority"]))))
    meta.add_row("owner", _agent_text(data["owner"]))
    if data.get("owner_role"):
        meta.add_row("owner role", _role_label(data["owner_role"], data.get("owner_specialty")))
    meta.add_row("parent", str(data["parent_task_id"]) if data["parent_task_id"] else "-")
    meta.add_row("delegation", data["delegation_mode"])
    meta.add_row("created by", _agent_text(data["created_by"]))
    if data["claimed_paths"]:
        meta.add_row("paths", ", ".join(data["claimed_paths"]))
    if owner_session:
        meta.add_row(
            "session",
            Text.assemble(
                (f"#{owner_session['id']} ", "bright_black"),
                (owner_session["label"] or "-", ""),
                ("  heartbeat ", "bright_black"),
                (owner_session["heartbeat_at"], "bright_black"),
            ),
        )
    if lease:
        meta.add_row("lease", Text(f"expires {lease['expires_at']}", style="bright_black"))

    title = Text.assemble(("  #" + str(data["id"]) + "  ", "bright_black"), (data["title"], "bold"))
    console.print(Panel(meta, title=title, title_align="left", border_style="cyan", padding=(0, 1)))

    if data.get("description"):
        console.print(Panel(Text(data["description"], style="dim"), border_style="bright_black", padding=(0, 1)))

    if children:
        child_table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold", expand=False)
        child_table.add_column("#", style="bright_black", width=4, justify="right")
        child_table.add_column("Status", min_width=16)
        child_table.add_column("Owner", min_width=22)
        child_table.add_column("Title")
        for child in children:
            child_table.add_row(
                str(child["id"]),
                _status_text(child["status"]),
                _agent_text(child["owner_name"]),
                child["title"],
            )
        console.print(Panel(child_table, title=" children ", title_align="left", border_style="bright_black"))

    if recent_messages:
        msgs = list(reversed(recent_messages))
        msg_table = Table(box=box.SIMPLE_HEAVY, show_header=False, padding=(0, 1), expand=False)
        msg_table.add_column("Time", style="bright_black", width=20)
        msg_table.add_column("From → To", min_width=28)
        msg_table.add_column("Type", min_width=16)
        msg_table.add_column("Body")
        for msg in msgs:
            target_part = Text.assemble((" → ", "bright_black"), (msg["to_name"], "")) if msg["to_name"] else Text("")
            from_to = Text.assemble((msg["from_name"], "bold"), target_part)
            body_preview = msg["body"][:80] + ("…" if len(msg["body"]) > 80 else "")
            msg_table.add_row(
                msg["created_at"],
                from_to,
                _msg_type_text(msg["type"]),
                body_preview,
            )
        console.print(Panel(msg_table, title=" recent messages ", title_align="left", border_style="bright_black"))


# ── one-line output ───────────────────────────────────────────────────────────

def print_ok(message: str) -> None:
    console.print(f"[green]✔[/green]  {message}")


def print_err(message: str) -> None:
    console.print(f"[red]✘[/red]  [red]{message}[/red]")


def print_info(message: str) -> None:
    console.print(f"[bright_black]·[/bright_black]  {message}")

"""Tests for action provenance derivation and new agent kinds."""
import pytest

from lex.cli import main
from lex.db import connect, derive_event_provenance, ensure_workspace, initialize_database


def init_workspace(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)
    return paths, conn


# --- derive_event_provenance ---

def test_provenance_automated_by_kind():
    assert derive_event_provenance(agent_kind="ci", session_id=None) == "automated"
    assert derive_event_provenance(agent_kind="automated", session_id=None) == "automated"
    # automated kind wins even with a session
    assert derive_event_provenance(agent_kind="ci", session_id=1) == "automated"


def test_provenance_interactive():
    assert derive_event_provenance(agent_kind="codex", session_id=42) == "interactive"
    assert derive_event_provenance(agent_kind="claude", session_id=1) == "interactive"


def test_provenance_delegated():
    # session-backed action on a child task
    assert derive_event_provenance(agent_kind="codex", session_id=1, task_parent_id=5) == "delegated"


def test_provenance_loose():
    assert derive_event_provenance(agent_kind="codex", session_id=None) == "loose"
    assert derive_event_provenance(agent_kind=None, session_id=None) == "loose"


def test_provenance_delegated_requires_session():
    # parent_task_id set but no session → still loose
    assert derive_event_provenance(agent_kind="codex", session_id=None, task_parent_id=3) == "loose"


# --- new agent kinds accepted by schema ---

def test_ci_agent_kind_accepted(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("ci-deploy-main", "ci"))
    agent = conn.execute("SELECT kind FROM agents WHERE name = ?", ("ci-deploy-main",)).fetchone()
    assert agent["kind"] == "ci"


def test_automated_agent_kind_accepted(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("automated-sync-bot", "automated"))
    agent = conn.execute("SELECT kind FROM agents WHERE name = ?", ("automated-sync-bot",)).fetchone()
    assert agent["kind"] == "automated"


def test_invalid_agent_kind_still_rejected(tmp_path):
    import sqlite3
    _, conn = init_workspace(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("bad-agent-x", "robot"))


# --- events emitted by loose vs interactive calls carry derivable provenance ---

def test_loose_claim_has_no_session_id(tmp_path):
    """A claim with no registered session emits an event with session_id=NULL."""
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("codex-brisk-otter", "codex"))
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES (?, 'open', ?)",
        ("Loose task", codex_id),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "claim", str(task_id), "codex-brisk-otter"])

    event = conn.execute(
        "SELECT session_id FROM events WHERE task_id = ? AND event_type = 'task.claimed'",
        (task_id,),
    ).fetchone()
    assert event is not None
    assert event["session_id"] is None
    assert derive_event_provenance(agent_kind="codex", session_id=event["session_id"]) == "loose"


def test_interactive_claim_has_session_id(tmp_path):
    """A claim from an agent with an active session emits an event with session_id set."""
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("codex-brisk-otter", "codex"))
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)"
        " VALUES (?, 'test', 'active', ?, '{}')",
        (codex_id, str(tmp_path)),
    )
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES (?, 'open', ?)",
        ("Interactive task", codex_id),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "claim", str(task_id), "codex-brisk-otter"])

    event = conn.execute(
        "SELECT session_id FROM events WHERE task_id = ? AND event_type = 'task.claimed'",
        (task_id,),
    ).fetchone()
    assert event is not None
    assert event["session_id"] is not None
    assert derive_event_provenance(agent_kind="codex", session_id=event["session_id"]) == "interactive"


def test_delegated_claim_derives_delegated_provenance(tmp_path):
    """A claim on a child task by a session-backed agent derives delegated provenance."""
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("codex-brisk-otter", "codex"))
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)"
        " VALUES (?, 'test', 'active', ?, '{}')",
        (codex_id, str(tmp_path)),
    )
    conn.execute("INSERT INTO tasks (title) VALUES ('Parent')")
    parent_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id, parent_task_id) VALUES (?, 'open', ?, ?)",
        ("Child task", codex_id, parent_id),
    )
    child_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "claim", str(child_id), "codex-brisk-otter"])

    event = conn.execute(
        "SELECT session_id FROM events WHERE task_id = ? AND event_type = 'task.claimed'",
        (child_id,),
    ).fetchone()
    assert event["session_id"] is not None
    assert derive_event_provenance(
        agent_kind="codex", session_id=event["session_id"], task_parent_id=parent_id
    ) == "delegated"

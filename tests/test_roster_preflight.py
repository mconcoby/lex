"""Tests for roster reconciliation preflight: detection, gating, and retire."""
import pytest

from lex.cli import main
from lex.db import connect, ensure_workspace, initialize_database, run_roster_preflight


def init_workspace(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)
    return paths, conn


def make_agent(conn, name="codex-brisk-otter", kind="codex", role="dev", status="active"):
    conn.execute(
        "INSERT INTO agents (name, kind, role, status) VALUES (?, ?, ?, ?)",
        (name, kind, role, status),
    )
    return conn.execute("SELECT id FROM agents WHERE name = ?", (name,)).fetchone()["id"]


def make_active_task(conn, owner_id, title="Task"):
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES (?, 'claimed', ?)",
        (title, owner_id),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def make_session(conn, agent_id):
    conn.execute(
        "INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)"
        " VALUES (?, 'test', 'active', '/', '{}')",
        (agent_id,),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


# --- run_roster_preflight detection ---

def test_preflight_clean_roster(tmp_path):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn)
    make_session(conn, agent_id)
    make_active_task(conn, agent_id)

    issues = run_roster_preflight(conn)
    assert issues == []


def test_preflight_detects_orphaned_task(tmp_path):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn)
    make_active_task(conn, agent_id)  # no session

    issues = run_roster_preflight(conn)
    orphans = [i for i in issues if i["kind"] == "orphaned_task"]
    assert len(orphans) == 1
    assert orphans[0]["agent_name"] == "codex-brisk-otter"


def test_preflight_excludes_reconnecting_agent_orphans(tmp_path):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn)
    make_active_task(conn, agent_id)  # no session — would be orphan

    # Should be excluded because this agent is reconnecting
    issues = run_roster_preflight(conn, exclude_reconnecting_agent_id=agent_id)
    orphans = [i for i in issues if i["kind"] == "orphaned_task"]
    assert orphans == []


def test_preflight_detects_retired_with_active_lease(tmp_path):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn, status="active")
    task_id = make_active_task(conn, agent_id)
    conn.execute(
        "INSERT INTO task_leases (task_id, agent_id, expires_at) VALUES (?, ?, datetime('now', '+30 minutes'))",
        (task_id, agent_id),
    )
    # Retire the agent (bypassing retire_agent helper to test detection directly)
    conn.execute("UPDATE agents SET status = 'retired' WHERE id = ?", (agent_id,))

    issues = run_roster_preflight(conn)
    retired = [i for i in issues if i["kind"] == "retired_leased"]
    assert len(retired) == 1


def test_preflight_detects_duplicate_role(tmp_path):
    _, conn = init_workspace(tmp_path)
    id1 = make_agent(conn, name="codex-alpha-dev", kind="codex", role="dev")
    id2 = make_agent(conn, name="codex-beta-dev", kind="codex", role="dev")
    make_session(conn, id1)
    make_session(conn, id2)

    issues = run_roster_preflight(conn)
    dups = [i for i in issues if i["kind"] == "duplicate_role"]
    assert len(dups) == 2  # one entry per agent in the duplicate pair


# --- session start gates on preflight ---

def test_session_start_blocked_by_orphaned_task(tmp_path):
    _, conn = init_workspace(tmp_path)
    make_agent(conn, name="codex-brisk-otter")
    make_agent(conn, name="codex-ghost-owner")
    ghost_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-ghost-owner",)).fetchone()["id"]
    make_active_task(conn, ghost_id)  # ghost agent has an active task, no session
    conn.commit()

    with pytest.raises(SystemExit, match="orphaned_task"):
        main(["--root", str(tmp_path), "session", "start", "codex-brisk-otter", "--cwd", str(tmp_path)])


def test_session_start_allowed_for_reconnecting_owner(tmp_path):
    _, conn = init_workspace(tmp_path)
    make_agent(conn, name="codex-brisk-otter")
    agent_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    make_active_task(conn, agent_id)  # agent owns an active task, no session yet
    conn.commit()

    # Should succeed — starting a session for the task owner resolves the orphan
    main(["--root", str(tmp_path), "session", "start", "codex-brisk-otter", "--cwd", str(tmp_path)])
    session = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()
    assert session is not None


# --- agent retire ---

def test_retire_agent_marks_retired_and_releases_lease(tmp_path):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn)
    task_id = make_active_task(conn, agent_id)
    conn.execute(
        "INSERT INTO task_leases (task_id, agent_id, expires_at) VALUES (?, ?, datetime('now', '+30 minutes'))",
        (task_id, agent_id),
    )
    conn.commit()

    main(["--root", str(tmp_path), "agent", "retire", "codex-brisk-otter", "--force"])

    agent = conn.execute("SELECT status FROM agents WHERE id = ?", (agent_id,)).fetchone()
    lease = conn.execute("SELECT state FROM task_leases WHERE task_id = ?", (task_id,)).fetchone()
    assert agent["status"] == "retired"
    assert lease["state"] == "released"


def test_retire_agent_blocked_without_force_when_has_active_tasks(tmp_path):
    _, conn = init_workspace(tmp_path)
    make_agent(conn)
    agent_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    make_active_task(conn, agent_id)
    conn.commit()

    with pytest.raises(SystemExit, match="active tasks"):
        main(["--root", str(tmp_path), "agent", "retire", "codex-brisk-otter"])


def test_retire_agent_emits_audit_event(tmp_path):
    _, conn = init_workspace(tmp_path)
    make_agent(conn)
    conn.commit()

    main(["--root", str(tmp_path), "agent", "retire", "codex-brisk-otter"])

    event = conn.execute(
        "SELECT event_type FROM events WHERE event_type = 'agent.retired'"
    ).fetchone()
    assert event is not None


def test_retired_agent_status_accepted_by_schema(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute(
        "INSERT INTO agents (name, kind, status) VALUES (?, 'codex', 'retired')",
        ("codex-old-ghost",),
    )
    row = conn.execute("SELECT status FROM agents WHERE name = ?", ("codex-old-ghost",)).fetchone()
    assert row["status"] == "retired"


# --- lex agent preflight command ---

def test_agent_preflight_command_passes_clean_roster(tmp_path, capsys):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn)
    make_session(conn, agent_id)
    conn.commit()

    main(["--root", str(tmp_path), "agent", "preflight"])
    out = capsys.readouterr().out
    assert "passed" in out


def test_agent_preflight_command_fails_with_orphan(tmp_path):
    _, conn = init_workspace(tmp_path)
    agent_id = make_agent(conn)
    make_active_task(conn, agent_id)  # no session
    conn.commit()

    with pytest.raises(SystemExit):
        main(["--root", str(tmp_path), "agent", "preflight"])

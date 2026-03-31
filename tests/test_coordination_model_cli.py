import sqlite3

import pytest

from lex.cli import main
from lex.db import connect, detect_path_conflicts, ensure_workspace, initialize_database


def init_workspace(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)
    return paths, conn


def register_agents(conn):
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("codex-brisk-otter", "codex"))
    conn.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("claude-steady-ibis", "claude"))


def test_stale_session_lease_is_released_during_claim(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]

    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json, heartbeat_at)
        VALUES (?, 'stale', 'active', ?, '{}', datetime('now', '-20 minutes'))
        """,
        (claude_id, str(tmp_path)),
    )
    stale_session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)
        VALUES (?, 'primary', 'active', ?, '{}')
        """,
        (codex_id, str(tmp_path)),
    )
    active_session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Recover task', 'in_progress', ?)",
        (claude_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO task_leases (task_id, agent_id, session_id, expires_at)
        VALUES (?, ?, ?, datetime('now', '+30 minutes'))
        """,
        (task_id, claude_id, stale_session_id),
    )
    conn.commit()

    main(["--root", str(tmp_path), "task", "claim", str(task_id), "codex-brisk-otter"])

    task = conn.execute("SELECT owner_agent_id FROM tasks WHERE id = ?", (task_id,)).fetchone()
    released_lease = conn.execute(
        "SELECT state, released_at FROM task_leases WHERE session_id = ?",
        (stale_session_id,),
    ).fetchone()
    new_lease = conn.execute(
        "SELECT agent_id, session_id, state FROM task_leases WHERE task_id = ? ORDER BY id DESC LIMIT 1",
        (task_id,),
    ).fetchone()

    assert task["owner_agent_id"] == codex_id
    assert released_lease["state"] == "released"
    assert released_lease["released_at"] is not None
    assert new_lease["agent_id"] == codex_id
    assert new_lease["session_id"] == active_session_id
    assert new_lease["state"] == "active"


def test_session_end_releases_bound_leases(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]

    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)
        VALUES (?, 'primary', 'active', ?, '{}')
        """,
        (codex_id, str(tmp_path)),
    )
    session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('End session task', 'claimed', ?)",
        (codex_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO task_leases (task_id, agent_id, session_id, expires_at)
        VALUES (?, ?, ?, datetime('now', '+30 minutes'))
        """,
        (task_id, codex_id, session_id),
    )
    conn.commit()

    main(["--root", str(tmp_path), "session", "end", str(session_id)])

    lease = conn.execute(
        "SELECT state, released_at FROM task_leases WHERE task_id = ? ORDER BY id DESC LIMIT 1",
        (task_id,),
    ).fetchone()
    assert lease["state"] == "released"
    assert lease["released_at"] is not None


def test_task_priority_command_updates_priority(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO tasks (title, status, priority, owner_agent_id) VALUES ('Escalate me', 'in_progress', 3, ?)",
        (codex_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "priority", str(task_id), "codex-brisk-otter", "1"])

    task = conn.execute("SELECT priority FROM tasks WHERE id = ?", (task_id,)).fetchone()
    event = conn.execute(
        "SELECT event_type, payload_json FROM events WHERE task_id = ? ORDER BY id DESC LIMIT 1",
        (task_id,),
    ).fetchone()
    assert task["priority"] == 1
    assert event["event_type"] == "task.priority_changed"


def test_database_rejects_duplicate_active_leases_for_task(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Lease me', 'claimed', ?)",
        (codex_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO task_leases (task_id, agent_id, expires_at) VALUES (?, ?, datetime('now', '+30 minutes'))",
        (task_id, codex_id),
    )

    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint failed: task_leases.task_id"):
        conn.execute(
            "INSERT INTO task_leases (task_id, agent_id, expires_at) VALUES (?, ?, datetime('now', '+30 minutes'))",
            (task_id, claude_id),
        )


def test_database_rejects_lease_bound_to_another_agents_session(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)
        VALUES (?, 'primary', 'active', ?, '{}')
        """,
        (codex_id, str(tmp_path)),
    )
    session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Session bound task', 'claimed', ?)",
        (claude_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError, match="lease session must belong to the same agent"):
        conn.execute(
            """
            INSERT INTO task_leases (task_id, agent_id, session_id, expires_at)
            VALUES (?, ?, ?, datetime('now', '+30 minutes'))
            """,
            (task_id, claude_id, session_id),
        )


def test_database_rejects_done_task_without_completed_at(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]

    with pytest.raises(sqlite3.IntegrityError, match="done task requires completed_at"):
        conn.execute(
            "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Incomplete done task', 'done', ?)",
            (codex_id,),
        )


def test_database_rejects_non_done_task_with_completed_at(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]

    with pytest.raises(sqlite3.IntegrityError, match="non-done task cannot have completed_at"):
        conn.execute(
            """
            INSERT INTO tasks (title, status, owner_agent_id, completed_at)
            VALUES ('Premature completion', 'claimed', ?, CURRENT_TIMESTAMP)
            """,
            (codex_id,),
        )


def test_database_rejects_self_referencing_dependency(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Self task', 'claimed', ?)",
        (codex_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError, match="task cannot depend on itself"):
        conn.execute(
            "INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?, ?)",
            (task_id, task_id),
        )


def test_database_rejects_invalid_agent_kind(tmp_path):
    _, conn = init_workspace(tmp_path)

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO agents (name, kind) VALUES (?, ?)",
            ("bad-agent", "invalid_kind"),
        )


def test_database_rejects_active_lease_with_ended_session(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json, ended_at)
        VALUES (?, 'ended', 'ended', ?, '{}', CURRENT_TIMESTAMP)
        """,
        (codex_id, str(tmp_path)),
    )
    session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Lease task', 'claimed', ?)",
        (codex_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    with pytest.raises(sqlite3.IntegrityError, match="active lease session must be active"):
        conn.execute(
            """
            INSERT INTO task_leases (task_id, agent_id, session_id, expires_at)
            VALUES (?, ?, ?, datetime('now', '+30 minutes'))
            """,
            (task_id, codex_id, session_id),
        )


def test_cli_reopen_done_task_clears_completed_at(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        """
        INSERT INTO tasks (title, status, owner_agent_id, completed_at)
        VALUES ('Reopen me', 'done', ?, CURRENT_TIMESTAMP)
        """,
        (codex_id,),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "status", str(task_id), "codex-brisk-otter", "in_progress"])

    task = conn.execute("SELECT status, completed_at FROM tasks WHERE id = ?", (task_id,)).fetchone()
    assert task["status"] == "in_progress"
    assert task["completed_at"] is None


# --- claimed-path conflict detection ---

def _setup_claimed_task(conn, agent_id, paths, tmp_path):
    """Insert a claimed task with an active lease and given paths. Returns task_id."""
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id, claimed_paths_json) VALUES (?, 'claimed', ?, ?)",
        ("Blocking task", agent_id, __import__("json").dumps(paths)),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO task_leases (task_id, agent_id, expires_at) VALUES (?, ?, datetime('now', '+30 minutes'))",
        (task_id, agent_id),
    )
    return task_id


def test_detect_path_conflicts_exact_match(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    _setup_claimed_task(conn, codex_id, ["src/lex/db.py"], tmp_path)

    conflicts = detect_path_conflicts(conn, ["src/lex/db.py"])
    assert len(conflicts) == 1
    assert conflicts[0]["conflicting_path"] == "src/lex/db.py"


def test_detect_path_conflicts_prefix_containment(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    # Existing task owns the directory; candidate claims a file inside it
    _setup_claimed_task(conn, codex_id, ["src/lex"], tmp_path)

    conflicts = detect_path_conflicts(conn, ["src/lex/db.py"])
    assert len(conflicts) == 1

    # Reverse: candidate owns the directory; existing task owns a file inside
    _, conn2 = init_workspace(tmp_path / "ws2")
    conn2.execute("INSERT INTO agents (name, kind) VALUES (?, ?)", ("codex-brisk-otter", "codex"))
    codex_id2 = conn2.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    _setup_claimed_task(conn2, codex_id2, ["src/lex/db.py"], tmp_path)
    conflicts2 = detect_path_conflicts(conn2, ["src/lex"])
    assert len(conflicts2) == 1


def test_detect_path_conflicts_siblings_do_not_conflict(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    _setup_claimed_task(conn, codex_id, ["src/foobar"], tmp_path)

    # src/foo and src/foobar are siblings — must not conflict
    conflicts = detect_path_conflicts(conn, ["src/foo"])
    assert conflicts == []


def test_detect_path_conflicts_excludes_self(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    task_id = _setup_claimed_task(conn, codex_id, ["src/lex"], tmp_path)

    # Should not report a conflict against itself
    conflicts = detect_path_conflicts(conn, ["src/lex"], exclude_task_id=task_id)
    assert conflicts == []


def test_detect_path_conflicts_expired_lease_not_counted(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id, claimed_paths_json) VALUES (?, 'claimed', ?, ?)",
        ("Expired task", codex_id, '["src/lex"]'),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO task_leases (task_id, agent_id, expires_at) VALUES (?, ?, datetime('now', '-1 minute'))",
        (task_id, codex_id),
    )

    conflicts = detect_path_conflicts(conn, ["src/lex"])
    assert conflicts == []


def test_claim_warns_on_path_conflict(tmp_path, capsys):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]
    _setup_claimed_task(conn, codex_id, ["src/lex"], tmp_path)
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id, claimed_paths_json) VALUES (?, 'open', ?, ?)",
        ("New task", claude_id, '["src/lex/cli.py"]'),
    )
    new_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "claim", str(new_task_id), "claude-steady-ibis"])
    out = capsys.readouterr().out
    assert "warning" in out
    assert "conflict" in out


def test_claim_strict_blocks_on_path_conflict(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]
    _setup_claimed_task(conn, codex_id, ["src/lex"], tmp_path)
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id, claimed_paths_json) VALUES (?, 'open', ?, ?)",
        ("Blocked task", claude_id, '["src/lex/cli.py"]'),
    )
    new_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    with pytest.raises(SystemExit):
        main(["--root", str(tmp_path), "task", "claim", str(new_task_id), "claude-steady-ibis", "--strict"])


def test_claim_emits_conflict_detected_event(tmp_path):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]
    _setup_claimed_task(conn, codex_id, ["src/lex"], tmp_path)
    conn.execute(
        "INSERT INTO tasks (title, status, owner_agent_id, claimed_paths_json) VALUES (?, 'open', ?, ?)",
        ("Event task", claude_id, '["src/lex/cli.py"]'),
    )
    new_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()

    main(["--root", str(tmp_path), "task", "claim", str(new_task_id), "claude-steady-ibis"])

    event = conn.execute(
        "SELECT event_type FROM events WHERE task_id = ? AND event_type = 'task.conflict_detected'",
        (new_task_id,),
    ).fetchone()
    assert event is not None

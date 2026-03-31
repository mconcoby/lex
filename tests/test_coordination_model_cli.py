from rex.cli import main
from rex.db import connect, ensure_workspace, initialize_database


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

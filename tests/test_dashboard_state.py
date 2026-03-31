from rex.dashboard import load_dashboard_state
from rex.db import connect, ensure_workspace, initialize_database


def test_dashboard_state_includes_active_sessions_tasks_and_messages(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)

    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-brisk-otter', 'codex', 'dev', 'tech_lead', 'active')")
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('claude-steady-ibis', 'claude', 'auditor', 'reviewer', 'active')")
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)
        VALUES (1, 'primary', 'active', ?, '{}')
        """,
        (str(tmp_path),),
    )
    conn.execute(
        """
        INSERT INTO tasks (title, description, status, priority, owner_agent_id, delegation_mode)
        VALUES ('Polish dashboard', 'Add detail pane', 'in_progress', 1, 1, 'direct')
        """
    )
    conn.execute(
        """
        INSERT INTO tasks (title, description, status, priority, owner_agent_id, parent_task_id, delegation_mode)
        VALUES ('Review layout', '', 'claimed', 2, 2, 1, 'direct')
        """
    )
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (1, 1, 2, 'review_request', 'layout', 'Please review the selected task pane.')
        """
    )
    conn.execute(
        """
        INSERT INTO events (event_type, task_id, agent_id, payload_json)
        VALUES ('task.created', 1, 1, '{}')
        """
    )
    conn.commit()

    state = load_dashboard_state(tmp_path)

    assert len(state.agents) == 2
    assert len(state.sessions) == 1
    assert len(state.tasks) == 2
    assert state.agents[0]["role"] == "dev"
    assert state.agents[0]["specialty"] == "tech_lead"
    assert state.sessions[0]["agent_role"] == "dev"
    assert state.sessions[0]["agent_specialty"] == "tech_lead"
    assert state.tasks[0]["title"] == "Polish dashboard"
    assert state.inbox[0]["body"] == "Please review the selected task pane."
    assert state.events[0]["event_type"] == "task.created"

    detail = state.task_details[1]
    assert detail["title"] == "Polish dashboard"
    assert detail["owner_name"] == "codex-brisk-otter"
    assert detail["owner_role"] == "dev"
    assert detail["owner_specialty"] == "tech_lead"
    assert len(detail["children"]) == 1
    assert detail["children"][0]["title"] == "Review layout"
    assert len(detail["messages"]) == 1
    assert detail["messages"][0]["type"] == "review_request"


def test_dashboard_state_orders_tasks_by_status_then_priority(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)

    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-brisk-otter', 'codex', '', '', 'active')")
    conn.execute(
        """
        INSERT INTO tasks (title, status, priority, owner_agent_id, delegation_mode)
        VALUES ('Open task', 'open', 1, 1, 'direct')
        """
    )
    conn.execute(
        """
        INSERT INTO tasks (title, status, priority, owner_agent_id, delegation_mode)
        VALUES ('Claimed task', 'claimed', 2, 1, 'direct')
        """
    )
    conn.execute(
        """
        INSERT INTO tasks (title, status, priority, owner_agent_id, delegation_mode)
        VALUES ('In progress task', 'in_progress', 3, 1, 'direct')
        """
    )
    conn.commit()

    state = load_dashboard_state(tmp_path)
    titles = [task["title"] for task in state.tasks[:3]]

    assert titles == ["In progress task", "Claimed task", "Open task"]


def test_dashboard_state_flags_stale_sessions_and_expiring_leases(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)

    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-brisk-otter', 'codex', '', '', 'active')")
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json, heartbeat_at)
        VALUES (1, 'primary', 'active', ?, '{}', datetime('now', '-20 minutes'))
        """,
        (str(tmp_path),),
    )
    conn.execute(
        """
        INSERT INTO tasks (title, status, priority, owner_agent_id, delegation_mode)
        VALUES ('Expiring lease task', 'in_progress', 1, 1, 'direct')
        """
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO task_leases (task_id, agent_id, expires_at)
        VALUES (?, 1, datetime('now', '+2 minutes'))
        """,
        (task_id,),
    )
    conn.commit()

    state = load_dashboard_state(tmp_path)

    assert state.sessions[0]["is_stale"] == 1
    assert state.tasks[0]["lease_expiring"] == 1
    assert state.task_details[task_id]["lease_expiring"] == 1

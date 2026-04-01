import json

from lex.cli import main
from lex.db import connect, ensure_workspace, initialize_database


def init_workspace(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)
    return paths, conn


def test_session_start_creates_bootstrap_with_hydrated_memory(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-pm-dalton', 'codex', 'pm', '', 'active')")
    conn.execute("INSERT INTO tasks (title, status, owner_agent_id) VALUES ('Parent task', 'claimed', 1)")
    conn.execute("INSERT INTO watches (agent_id, task_id) VALUES (1, 1)")
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (1, 1, 1, 'note', 'hello', 'review queue')
        """
    )
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])

    bootstrap = conn.execute("SELECT * FROM session_bootstraps ORDER BY id DESC LIMIT 1").fetchone()
    memory = json.loads(bootstrap["memory_json"])
    contract = json.loads(bootstrap["role_contract_json"])

    assert contract["role"] == "pm"
    assert "task_claim" in contract["blocked_verbs"]
    assert memory["active_tasks"][0]["title"] == "Parent task"
    assert memory["subscriptions"][0]["task_id"] == 1
    assert memory["inbox"][0]["subject"] == "hello"


def test_session_start_links_bootstrap_continuity_to_previous_session(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-dev-otter', 'codex', 'dev', '', 'active')")
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, status, cwd, capabilities_json)
        VALUES (1, 'first', 'active', ?, '{}')
        """,
        (str(tmp_path),),
    )
    first_session_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO session_bootstraps (
            session_id, agent_id, role_contract_json, memory_json, system_prompt, workflow_template_json, required_actions_json,
            acknowledged_at, acknowledged_by
        )
        VALUES (?, 1, '{}', '{}', '', '[]', '[]', CURRENT_TIMESTAMP, 'human')
        """,
        (first_session_id,),
    )
    conn.execute(
        "UPDATE sessions SET status = 'ended', ended_at = CURRENT_TIMESTAMP WHERE id = ?",
        (first_session_id,),
    )
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-dev-otter", "--label", "second"])

    bootstrap = conn.execute("SELECT * FROM session_bootstraps ORDER BY id DESC LIMIT 1").fetchone()
    assert bootstrap["continuity_from_session_id"] == first_session_id


def test_pm_claim_requires_override_after_bootstrap(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-pm-dalton', 'codex', 'pm', '', 'active')")
    conn.execute("INSERT INTO tasks (title) VALUES ('Dev task')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]

    try:
        main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton"])
    except SystemExit as exc:
        assert "bootstrap must be acknowledged" in str(exc)
    else:
        raise AssertionError("expected bootstrap acknowledgement guard")

    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])
    main(["--root", str(tmp_path), "msg", "inbox", "codex-pm-dalton"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "inspect_open_child_tasks"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "assign_or_delegate_work"])

    try:
        main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton"])
    except SystemExit as exc:
        assert "not allowed to perform task_claim" in str(exc)
    else:
        raise AssertionError("expected PM claim guard")

    main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton", "--force-role-override"])
    task = conn.execute("SELECT owner_agent_id FROM tasks WHERE id = 1").fetchone()
    override_event = conn.execute(
        "SELECT event_type FROM events WHERE event_type = 'role.override_used' ORDER BY id DESC LIMIT 1"
    ).fetchone()

    assert task["owner_agent_id"] == 1
    assert override_event["event_type"] == "role.override_used"


def test_watch_ack_tracks_delivery(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-dev-otter', 'codex', 'dev', '', 'active')")
    conn.execute("INSERT INTO tasks (title) VALUES ('Observed task')")
    conn.commit()

    main(["--root", str(tmp_path), "watch", "add", "codex-dev-otter", "1"])
    main(["--root", str(tmp_path), "msg", "send", "--task", "1", "--from", "codex-dev-otter", "--type", "note", "--body", "event source"])

    watch = conn.execute("SELECT last_sent_event_id, last_ack_event_id FROM watches WHERE agent_id = 1 AND task_id = 1").fetchone()
    assert watch["last_sent_event_id"] > watch["last_ack_event_id"]

    main(["--root", str(tmp_path), "watch", "ack", "codex-dev-otter", "1"])
    updated = conn.execute("SELECT last_sent_event_id, last_ack_event_id, last_acknowledged_at FROM watches WHERE agent_id = 1 AND task_id = 1").fetchone()

    assert updated["last_ack_event_id"] == updated["last_sent_event_id"]
    assert updated["last_acknowledged_at"] is not None

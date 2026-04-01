"""Comprehensive tests for dispatch lifecycle, worker crash paths, role-drift enforcement, and force-override audit recording."""

import json
import sys
import time

from lex.cli import main
from lex.db import connect, ensure_workspace, initialize_database


def init_workspace(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)
    return paths, conn


def register_agent(conn, name, kind="codex", role="dev"):
    conn.execute(
        "INSERT INTO agents (name, kind, role, specialty, status) VALUES (?, ?, ?, '', 'active')",
        (name, kind, role),
    )
    conn.commit()
    return conn.execute("SELECT id FROM agents WHERE name = ?", (name,)).fetchone()["id"]


def wait_for_runtime_status(conn, runtime_id, allowed, timeout=8.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = conn.execute("SELECT * FROM worker_runtimes WHERE id = ?", (runtime_id,)).fetchone()
        if row is not None and row["status"] in allowed:
            return dict(row)
        time.sleep(0.2)
    row = conn.execute("SELECT * FROM worker_runtimes WHERE id = ?", (runtime_id,)).fetchone()
    raise AssertionError(f"runtime {runtime_id} did not reach {allowed}; last status={row['status']}")


# ---------------------------------------------------------------------------
# Worker crash paths
# ---------------------------------------------------------------------------

def test_worker_nonzero_exit_marks_runtime_failed(tmp_path):
    """Worker that exits with code 1 should record status='failed' and exit_code=1."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    command_json = json.dumps([sys.executable, "-c", "raise SystemExit(1)"])

    main(["--root", str(tmp_path), "worker", "register", "codex-crash-dev", "codex",
          "--role", "dev", "--command-json", command_json,
          "--approval-policy", "never", "--created-by", "codex-pm-dalton"])
    main(["--root", str(tmp_path), "worker", "request-start", "codex-crash-dev",
          "--requested-by", "codex-pm-dalton", "--reason", "crash test"])

    runtime_id = conn.execute("SELECT id FROM worker_runtimes ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "worker", "start", str(runtime_id)])
    runtime = wait_for_runtime_status(conn, runtime_id, {"failed", "exited"})

    assert runtime["status"] == "failed"
    assert runtime["exit_code"] == 1


def test_worker_stop_command_emits_event_and_marks_stopped(tmp_path):
    """worker stop command marks runtime stopped and emits worker.runtime_stopped event."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    command_json = json.dumps([sys.executable, "-c", "import time; time.sleep(30)"])

    main(["--root", str(tmp_path), "worker", "register", "codex-long-dev", "codex",
          "--role", "dev", "--command-json", command_json,
          "--approval-policy", "never", "--created-by", "codex-pm-dalton"])
    main(["--root", str(tmp_path), "worker", "request-start", "codex-long-dev",
          "--requested-by", "codex-pm-dalton", "--reason", "stop signal test"])

    runtime_id = conn.execute("SELECT id FROM worker_runtimes ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "worker", "start", str(runtime_id)])
    wait_for_runtime_status(conn, runtime_id, {"running", "launching"})

    main(["--root", str(tmp_path), "worker", "stop", str(runtime_id), "--signal", "TERM"])

    runtime = conn.execute("SELECT status, ended_at FROM worker_runtimes WHERE id = ?", (runtime_id,)).fetchone()
    event = conn.execute(
        "SELECT event_type FROM events WHERE event_type = 'worker.runtime_stopped' ORDER BY id DESC LIMIT 1"
    ).fetchone()

    assert runtime["status"] == "stopped"
    assert runtime["ended_at"] is not None
    assert event is not None


def test_worker_crash_fails_in_flight_dispatch_packet(tmp_path):
    """Packets in 'delivered' state against a crashed runtime are failed by cleanup."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title, status, priority, delegation_mode) VALUES ('Crash task', 'open', 2, 'direct')")
    conn.commit()
    task_id = conn.execute("SELECT id FROM tasks ORDER BY id DESC LIMIT 1").fetchone()["id"]

    conn.execute(
        "INSERT INTO worker_definitions (name, kind, role, specialty, command_json, approval_policy, created_by_agent_id) VALUES ('codex-crash-dev', 'codex', 'dev', '', '[]', 'never', 1)"
    )
    conn.execute(
        """
        INSERT INTO worker_runtimes (
            worker_id, task_id, requested_by_agent_id, reason,
            approval_required, approval_status, status, started_at, heartbeat_at
        )
        VALUES (1, ?, 1, 'crash test', 0, 'not_required', 'running', CURRENT_TIMESTAMP, datetime('now', '-10 minutes'))
        """,
        (task_id,),
    )
    conn.execute(
        """
        INSERT INTO dispatch_packets (
            task_id, runtime_id, to_worker_id, from_agent_id,
            packet_json, approval_status, delivery_status, delivered_at
        )
        VALUES (?, 1, 1, 1, '{}', 'approved', 'delivered', CURRENT_TIMESTAMP)
        """,
        (task_id,),
    )
    conn.commit()

    main(["--root", str(tmp_path), "worker", "cleanup", "--stale-minutes", "1"])

    packet = conn.execute("SELECT delivery_status, completion_note FROM dispatch_packets WHERE id = 1").fetchone()
    assert packet["delivery_status"] == "failed"
    assert "stale heartbeat" in packet["completion_note"]


def test_worker_stop_no_pid_does_not_raise(tmp_path):
    """worker stop succeeds even when runtime has no recorded PID (never launched)."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute(
        """
        INSERT INTO worker_definitions (name, kind, role, specialty, command_json, approval_policy, created_by_agent_id)
        VALUES ('codex-nopid-dev', 'codex', 'dev', '', '[]', 'never', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO worker_runtimes (
            worker_id, task_id, requested_by_agent_id, reason,
            approval_required, approval_status, status, started_at, heartbeat_at
        )
        VALUES (1, NULL, 1, 'no pid test', 0, 'not_required', 'approved', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )
    conn.commit()
    runtime_id = conn.execute("SELECT id FROM worker_runtimes ORDER BY id DESC LIMIT 1").fetchone()["id"]

    # Should complete without raising
    main(["--root", str(tmp_path), "worker", "stop", str(runtime_id)])
    runtime = conn.execute("SELECT status FROM worker_runtimes WHERE id = ?", (runtime_id,)).fetchone()
    assert runtime["status"] == "stopped"


# ---------------------------------------------------------------------------
# Role drift / bootstrap enforcement edges
# ---------------------------------------------------------------------------

def test_role_drift_event_emitted_before_bootstrap_ack(tmp_path):
    """Acting before bootstrap ack logs role.drift_detected with reason=bootstrap_not_acknowledged."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title) VALUES ('Test task')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])

    try:
        main(["--root", str(tmp_path), "task", "priority", "1", "codex-pm-dalton", "3"])
    except SystemExit:
        pass

    event = conn.execute(
        "SELECT payload_json FROM events WHERE event_type = 'role.drift_detected' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(event["payload_json"])
    assert payload["reason"] == "bootstrap_not_acknowledged"
    assert payload["verb"] == "task_priority"


def test_role_drift_event_emitted_with_pending_required_actions(tmp_path):
    """Acting before required actions complete logs role.drift_detected with reason=required_actions_pending."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title) VALUES ('Test task')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])

    # Only complete two of three required actions (skip assign_or_delegate_work)
    main(["--root", str(tmp_path), "session", "action", str(session_id), "review_inbox"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "inspect_open_child_tasks"])

    try:
        main(["--root", str(tmp_path), "task", "priority", "1", "codex-pm-dalton", "3"])
    except SystemExit:
        pass

    event = conn.execute(
        "SELECT payload_json FROM events WHERE event_type = 'role.drift_detected' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(event["payload_json"])
    assert payload["reason"] == "required_actions_pending"
    assert "assign_or_delegate_work" in payload["pending_actions"]


def test_auditor_blocked_from_task_claim(tmp_path):
    """Auditor role cannot claim tasks — task_claim is in blocked_verbs."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "gemini-sharp-auditor", kind="gemini", role="auditor")
    conn.execute("INSERT INTO tasks (title) VALUES ('Audit target')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "gemini-sharp-auditor", "--label", "audit"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "review_inbox"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "inspect_review_queue"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "record_review_plan"])

    try:
        main(["--root", str(tmp_path), "task", "claim", "1", "gemini-sharp-auditor"])
    except SystemExit as exc:
        assert "not allowed to perform task_claim" in str(exc)
    else:
        raise AssertionError("expected auditor task_claim to be blocked")


def test_agent_with_no_role_bypasses_contract(tmp_path):
    """Agent with empty role string has no contract and all verbs are permitted."""
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-norole-dev', 'codex', '', '', 'active')")
    conn.execute("INSERT INTO tasks (title) VALUES ('Free task')")
    conn.commit()

    # Should not raise — no contract means no enforcement
    main(["--root", str(tmp_path), "task", "claim", "1", "codex-norole-dev"])
    task = conn.execute("SELECT owner_agent_id FROM tasks WHERE id = 1").fetchone()
    assert task["owner_agent_id"] is not None


def test_dev_required_actions_gate_early_verbs(tmp_path):
    """Dev agent cannot update task status before completing required actions after bootstrap ack."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-dev-otter", role="dev")
    conn.execute("INSERT INTO tasks (title, status, owner_agent_id, delegation_mode) VALUES ('Dev task', 'claimed', 1, 'direct')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-dev-otter", "--label", "dev"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])

    # Only complete one of three required actions
    main(["--root", str(tmp_path), "session", "action", str(session_id), "review_inbox"])

    try:
        main(["--root", str(tmp_path), "task", "status", "1", "codex-dev-otter", "in_progress"])
    except SystemExit as exc:
        assert "required_actions_pending" in str(exc) or "bootstrap actions still pending" in str(exc)
    else:
        raise AssertionError("expected pending required actions to block task status update")


# ---------------------------------------------------------------------------
# Force-override audit recording
# ---------------------------------------------------------------------------

def test_force_override_event_payload_contains_verb_and_role(tmp_path):
    """role.override_used event records the verb and role used to override."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title) VALUES ('PM override task')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "review_inbox"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "inspect_open_child_tasks"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "assign_or_delegate_work"])

    main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton", "--force-role-override"])

    event = conn.execute(
        "SELECT payload_json, agent_id FROM events WHERE event_type = 'role.override_used' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(event["payload_json"])
    assert payload["verb"] == "task_claim"
    assert payload["role"] == "pm"


def test_force_override_without_flag_still_blocked(tmp_path):
    """Without --force-role-override, a blocked verb raises SystemExit even if everything else is clear."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title) VALUES ('PM no-override task')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "review_inbox"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "inspect_open_child_tasks"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "assign_or_delegate_work"])

    try:
        main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton"])
    except SystemExit as exc:
        assert "not allowed to perform task_claim" in str(exc)
    else:
        raise AssertionError("expected blocked verb to raise without --force-role-override")


def test_force_override_no_active_session_still_records_event(tmp_path):
    """role.override_used is recorded even when the overriding agent has no active session."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title) VALUES ('Sessionless override')")
    conn.commit()

    # No session started — agent acts directly
    main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton", "--force-role-override"])

    event = conn.execute(
        "SELECT payload_json FROM events WHERE event_type = 'role.override_used' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(event["payload_json"])
    assert payload["verb"] == "task_claim"


def test_override_emits_drift_event_when_not_forced(tmp_path):
    """A blocked verb without override emits role.drift_detected before raising."""
    _, conn = init_workspace(tmp_path)
    register_agent(conn, "codex-pm-dalton", role="pm")
    conn.execute("INSERT INTO tasks (title) VALUES ('Drift detection task')")
    conn.commit()

    main(["--root", str(tmp_path), "session", "start", "codex-pm-dalton", "--label", "pm"])
    session_id = conn.execute("SELECT id FROM sessions ORDER BY id DESC LIMIT 1").fetchone()["id"]
    main(["--root", str(tmp_path), "session", "bootstrap-ack", str(session_id), "--by", "human"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "review_inbox"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "inspect_open_child_tasks"])
    main(["--root", str(tmp_path), "session", "action", str(session_id), "assign_or_delegate_work"])

    try:
        main(["--root", str(tmp_path), "task", "claim", "1", "codex-pm-dalton"])
    except SystemExit:
        pass

    event = conn.execute(
        "SELECT payload_json FROM events WHERE event_type = 'role.drift_detected' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert event is not None
    payload = json.loads(event["payload_json"])
    assert payload["reason"] == "blocked_by_role_contract"
    assert payload["verb"] == "task_claim"
    assert payload["role"] == "pm"

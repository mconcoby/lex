from lex.cli import main
from lex.dashboard import load_dashboard_state
from lex.db import connect, ensure_workspace, initialize_database


def init_workspace(tmp_path):
    paths = ensure_workspace(tmp_path)
    conn = connect(paths.db_path)
    initialize_database(conn)
    return paths, conn


def test_session_start_persists_fingerprint_and_warns_on_second_instance(tmp_path, capsys):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-brisk-otter', 'codex', 'dev', 'engineer', 'active')")
    conn.commit()

    main(
        [
            "--root", str(tmp_path),
            "session", "start", "codex-brisk-otter",
            "--label", "primary",
            "--fingerprint", "fp-one",
            "--fingerprint-label", "hostA:pts1:100",
        ]
    )
    first = capsys.readouterr().out
    assert "instance hostA:pts1:100 (fp-one)" in first

    main(
        [
            "--root", str(tmp_path),
            "session", "start", "codex-brisk-otter",
            "--label", "secondary",
            "--fingerprint", "fp-two",
            "--fingerprint-label", "hostA:pts2:101",
        ]
    )
    second = capsys.readouterr().out
    assert "warning: another active session exists for this agent" in second

    rows = conn.execute(
        "SELECT label, fingerprint, fingerprint_label FROM sessions ORDER BY id"
    ).fetchall()
    assert rows[0]["fingerprint"] == "fp-one"
    assert rows[0]["fingerprint_label"] == "hostA:pts1:100"
    assert rows[1]["fingerprint"] == "fp-two"
    assert rows[1]["fingerprint_label"] == "hostA:pts2:101"


def test_dashboard_state_exposes_session_fingerprint(tmp_path):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-brisk-otter', 'codex', 'dev', 'engineer', 'active')")
    conn.execute(
        """
        INSERT INTO sessions (agent_id, label, fingerprint, fingerprint_label, status, cwd, capabilities_json)
        VALUES (1, 'primary', 'fp-one', 'hostA:pts1:100', 'active', ?, '{}')
        """,
        (str(tmp_path),),
    )
    conn.commit()

    state = load_dashboard_state(tmp_path)

    assert state.sessions[0]["fingerprint"] == "fp-one"
    assert state.sessions[0]["fingerprint_label"] == "hostA:pts1:100"
    assert state.sessions[0]["agent_role"] == "dev"
    assert state.sessions[0]["agent_specialty"] == "engineer"


def test_session_start_same_fingerprint_does_not_warn(tmp_path, capsys):
    _, conn = init_workspace(tmp_path)
    conn.execute("INSERT INTO agents (name, kind, role, specialty, status) VALUES ('codex-brisk-otter', 'codex', 'dev', 'engineer', 'active')")
    conn.commit()

    main(
        [
            "--root", str(tmp_path),
            "session", "start", "codex-brisk-otter",
            "--label", "primary",
            "--fingerprint", "fp-one",
            "--fingerprint-label", "hostA:pts1:100",
        ]
    )
    capsys.readouterr()

    main(
        [
            "--root", str(tmp_path),
            "session", "start", "codex-brisk-otter",
            "--label", "resume",
            "--fingerprint", "fp-one",
            "--fingerprint-label", "hostA:pts1:100",
        ]
    )
    second = capsys.readouterr().out

    assert "warning: another active session exists for this agent" not in second

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


def test_msg_inbox_limits_broadcasts_to_owned_or_participated_tasks(tmp_path, capsys):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]

    conn.execute(
        "INSERT INTO tasks (title, owner_agent_id) VALUES (?, ?)",
        ("Owned task", codex_id),
    )
    owned_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, owner_agent_id) VALUES (?, ?)",
        ("Other task", claude_id),
    )
    other_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO tasks (title, owner_agent_id) VALUES (?, ?)",
        ("Participated task", claude_id),
    )
    participated_task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, NULL, 'note', '', ?)
        """,
        (owned_task_id, claude_id, "Owned broadcast"),
    )
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, NULL, 'note', '', ?)
        """,
        (other_task_id, claude_id, "Other broadcast"),
    )
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, NULL, 'note', '', ?)
        """,
        (participated_task_id, codex_id, "Joined thread"),
    )
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, NULL, 'note', '', ?)
        """,
        (participated_task_id, claude_id, "Participated broadcast"),
    )
    conn.commit()

    main(["--root", str(tmp_path), "msg", "inbox", "codex-brisk-otter"])
    out = capsys.readouterr().out

    assert "Owned task" in out
    assert "Owned broadcast" in out
    assert "Participated task" in out
    assert "Participated broadcast" in out
    assert "Other task" not in out
    assert "Other broadcast" not in out


def test_msg_renderers_show_subject_and_task_title(tmp_path, capsys):
    _, conn = init_workspace(tmp_path)
    register_agents(conn)
    codex_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("codex-brisk-otter",)).fetchone()["id"]
    claude_id = conn.execute("SELECT id FROM agents WHERE name = ?", ("claude-steady-ibis",)).fetchone()["id"]

    conn.execute(
        "INSERT INTO tasks (title, owner_agent_id) VALUES (?, ?)",
        ("Coordination UX", codex_id),
    )
    task_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO messages (task_id, from_agent_id, to_agent_id, type, subject, body)
        VALUES (?, ?, NULL, 'review_request', ?, ?)
        """,
        (task_id, claude_id, "Thread subject", "Thread body"),
    )
    conn.commit()

    main(["--root", str(tmp_path), "msg", "inbox", "codex-brisk-otter"])
    inbox_out = capsys.readouterr().out
    assert "task 1" in inbox_out and "Coordination UX" in inbox_out
    assert "Thread subject" in inbox_out

    main(["--root", str(tmp_path), "msg", "task", str(task_id)])
    task_out = capsys.readouterr().out
    assert "Thread subject" in task_out
    assert "Thread body" in task_out

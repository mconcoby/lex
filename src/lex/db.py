from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


DEFAULT_LEX_DIRNAME = ".lex"
DEFAULT_DB_NAME = "lex.db"
ROLE_MIGRATIONS: dict[str, tuple[str, str]] = {
    "engineer": ("dev", "engineer"),
    "devops": ("dev", "devops"),
    "tech_lead": ("dev", "tech_lead"),
    "product_manager": ("pm", "product_manager"),
    "designer": ("pm", "designer"),
    "release_manager": ("pm", "release_manager"),
    "qa": ("auditor", "qa"),
    "reviewer": ("auditor", "reviewer"),
}
CANONICAL_ROLES = {"dev", "pm", "auditor"}
BUILTIN_SPECIALTIES = ("frontend", "infra", "ux", "security", "release")


SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    PRAGMA journal_mode = WAL;
    """,
    """
    CREATE TABLE IF NOT EXISTS agents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        kind TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT '',
        specialty TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'active',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent_id INTEGER NOT NULL,
        label TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        cwd TEXT,
        capabilities_json TEXT NOT NULL DEFAULT '{}',
        started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        ended_at TEXT,
        FOREIGN KEY (agent_id) REFERENCES agents(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slug TEXT UNIQUE,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'open',
        priority INTEGER NOT NULL DEFAULT 2,
        owner_agent_id INTEGER,
        parent_task_id INTEGER,
        delegation_mode TEXT NOT NULL DEFAULT 'direct',
        claimed_paths_json TEXT NOT NULL DEFAULT '[]',
        created_by_agent_id INTEGER,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT,
        FOREIGN KEY (owner_agent_id) REFERENCES agents(id),
        FOREIGN KEY (parent_task_id) REFERENCES tasks(id),
        FOREIGN KEY (created_by_agent_id) REFERENCES agents(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS task_leases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        agent_id INTEGER NOT NULL,
        session_id INTEGER,
        state TEXT NOT NULL DEFAULT 'active',
        acquired_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        expires_at TEXT NOT NULL,
        released_at TEXT,
        FOREIGN KEY (task_id) REFERENCES tasks(id),
        FOREIGN KEY (agent_id) REFERENCES agents(id),
        FOREIGN KEY (session_id) REFERENCES sessions(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS task_dependencies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        depends_on_task_id INTEGER NOT NULL,
        kind TEXT NOT NULL DEFAULT 'blocks',
        FOREIGN KEY (task_id) REFERENCES tasks(id),
        FOREIGN KEY (depends_on_task_id) REFERENCES tasks(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER,
        from_agent_id INTEGER NOT NULL,
        to_agent_id INTEGER,
        type TEXT NOT NULL,
        subject TEXT NOT NULL DEFAULT '',
        body TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        read_at TEXT,
        FOREIGN KEY (task_id) REFERENCES tasks(id),
        FOREIGN KEY (from_agent_id) REFERENCES agents(id),
        FOREIGN KEY (to_agent_id) REFERENCES agents(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER,
        agent_id INTEGER NOT NULL,
        kind TEXT NOT NULL,
        path TEXT NOT NULL,
        summary TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id),
        FOREIGN KEY (agent_id) REFERENCES agents(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
        task_id INTEGER,
        agent_id INTEGER,
        session_id INTEGER,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (task_id) REFERENCES tasks(id),
        FOREIGN KEY (agent_id) REFERENCES agents(id),
        FOREIGN KEY (session_id) REFERENCES sessions(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS watches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agent_id INTEGER NOT NULL,
        task_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(agent_id, task_id),
        FOREIGN KEY (agent_id) REFERENCES agents(id),
        FOREIGN KEY (task_id) REFERENCES tasks(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS specialties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TRIGGER IF NOT EXISTS tasks_updated_at
    AFTER UPDATE ON tasks
    FOR EACH ROW
    BEGIN
        UPDATE tasks
        SET updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.id;
    END;
    """,
)


SESSION_REQUIRED_COLUMNS: dict[str, str] = {
    "label": "TEXT",
    "heartbeat_at": "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP",
    "fingerprint": "TEXT",
    "fingerprint_label": "TEXT",
}
AGENT_REQUIRED_COLUMNS: dict[str, str] = {
    "role": "TEXT NOT NULL DEFAULT ''",
    "specialty": "TEXT NOT NULL DEFAULT ''",
}


@dataclass(frozen=True)
class LexPaths:
    root: Path
    lex_dir: Path
    db_path: Path


def resolve_paths(root: str | Path | None = None) -> LexPaths:
    workspace_root = Path(root or Path.cwd()).resolve()
    lex_dir = workspace_root / DEFAULT_LEX_DIRNAME
    return LexPaths(root=workspace_root, lex_dir=lex_dir, db_path=lex_dir / DEFAULT_DB_NAME)


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def initialize_database(conn: sqlite3.Connection) -> None:
    for statement in SCHEMA_STATEMENTS:
        conn.execute(statement)
    migrate_database(conn)
    conn.commit()


def migrate_database(conn: sqlite3.Connection) -> None:
    agent_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(agents)").fetchall()
    }
    for column_name, definition in AGENT_REQUIRED_COLUMNS.items():
        if column_name not in agent_columns:
            try:
                conn.execute(f"ALTER TABLE agents ADD COLUMN {column_name} {definition}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc):
                    raise
    migrate_agent_roles(conn)
    session_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
    }
    for column_name, definition in SESSION_REQUIRED_COLUMNS.items():
        if column_name not in session_columns:
            try:
                conn.execute(f"ALTER TABLE sessions ADD COLUMN {column_name} {definition}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc):
                    raise


def migrate_agent_roles(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT id, role, specialty FROM agents").fetchall()
    for row in rows:
        role = (row["role"] or "").strip()
        specialty = (row["specialty"] or "").strip()
        if not role or role in CANONICAL_ROLES:
            continue
        mapped = ROLE_MIGRATIONS.get(role)
        if mapped is None:
            continue
        canonical_role, default_specialty = mapped
        conn.execute(
            "UPDATE agents SET role = ?, specialty = ? WHERE id = ?",
            (canonical_role, specialty or default_specialty, row["id"]),
        )


def log_event(
    conn: sqlite3.Connection,
    event_type: str,
    *,
    task_id: int | None = None,
    agent_id: int | None = None,
    session_id: int | None = None,
    payload: dict | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO events (event_type, task_id, agent_id, session_id, payload_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (event_type, task_id, agent_id, session_id, json.dumps(payload or {})),
    )


def ensure_workspace(root: str | Path | None = None) -> LexPaths:
    paths = resolve_paths(root)
    paths.lex_dir.mkdir(parents=True, exist_ok=True)
    return paths


def fetch_one(conn: sqlite3.Connection, query: str, params: Iterable | tuple = ()) -> sqlite3.Row | None:
    return conn.execute(query, tuple(params)).fetchone()


def list_specialties(conn: sqlite3.Connection) -> list[str]:
    custom = [row["name"] for row in conn.execute("SELECT name FROM specialties ORDER BY name").fetchall()]
    return sorted(set(BUILTIN_SPECIALTIES).union(custom))

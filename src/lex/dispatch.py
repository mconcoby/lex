from __future__ import annotations

import json
import os
import shlex
import signal
import subprocess
import sys
from pathlib import Path

from lex.db import LexPaths


VALID_WORKER_APPROVAL_POLICIES = ("always", "on_sensitive", "never")
VALID_WORKER_RUNTIME_STATUSES = (
    "pending_approval",
    "approved",
    "launching",
    "running",
    "exited",
    "failed",
    "stopped",
    "rejected",
)
VALID_PACKET_STATUSES = (
    "draft",
    "pending_approval",
    "ready",
    "delivered",
    "acknowledged",
    "completed",
    "failed",
    "cancelled",
)
SENSITIVE_ACTIONS_REQUIRING_APPROVAL = {"spawn_worker", "final_integration", "merge"}


def runtime_root(paths: LexPaths) -> Path:
    root = paths.lex_dir / "runtime"
    root.mkdir(parents=True, exist_ok=True)
    return root


def worker_runtime_dir(paths: LexPaths, runtime_id: int) -> Path:
    root = runtime_root(paths) / "workers" / f"runtime-{runtime_id}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def decode_json_list(raw: str, *, field_name: str) -> list[str]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{field_name} must be valid JSON") from exc
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise SystemExit(f"{field_name} must be a JSON array of strings")
    return value


def decode_json_object(raw: str, *, field_name: str) -> dict[str, str]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{field_name} must be valid JSON") from exc
    if not isinstance(value, dict) or not all(isinstance(key, str) and isinstance(val, str) for key, val in value.items()):
        raise SystemExit(f"{field_name} must be a JSON object of string keys and values")
    return value


def command_preview(command: list[str]) -> str:
    return shlex.join(command)


def should_require_runtime_approval(*, policy: str, sensitive_action: str | None) -> bool:
    if policy == "always":
        return True
    if policy == "never":
        return False
    return (sensitive_action or "") in SENSITIVE_ACTIONS_REQUIRING_APPROVAL


def should_require_packet_approval(*, requested: bool, sensitive_action: str | None) -> bool:
    return requested or (sensitive_action or "") in SENSITIVE_ACTIONS_REQUIRING_APPROVAL


def launch_worker_supervisor(paths: LexPaths, runtime_id: int) -> subprocess.Popen[bytes]:
    log_dir = worker_runtime_dir(paths, runtime_id)
    stdout_path = log_dir / "supervisor.log"
    stderr_path = log_dir / "supervisor.err.log"
    env = os.environ.copy()
    src_path = str(paths.root / "src")
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src_path if not existing_pythonpath else f"{src_path}{os.pathsep}{existing_pythonpath}"
    stdout_handle = stdout_path.open("ab")
    stderr_handle = stderr_path.open("ab")
    try:
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "lex.worker_runtime",
                "--root",
                str(paths.root),
                str(runtime_id),
            ],
            cwd=str(paths.root),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stdout_handle,
            stderr=stderr_handle,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        stdout_handle.close()
        stderr_handle.close()


def stop_runtime_process(pid: int, sig: int = signal.SIGTERM) -> None:
    os.kill(pid, sig)

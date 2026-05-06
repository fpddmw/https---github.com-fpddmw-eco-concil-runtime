from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from eco_council_runtime.kernel.core.paths import execution_lock_path, execution_lock_state_path

LOCK_STATE_SCHEMA = "runtime-lock-state-v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def runtime_lock_state_payload(run_dir: Path) -> dict[str, Any]:
    lock_path = execution_lock_path(run_dir)
    state_path = execution_lock_state_path(run_dir)
    state = load_json_if_exists(state_path)
    holder_pid = int(state.get("holder_pid") or 0)
    recorded_state = maybe_text(state.get("lock_state"))
    pid_alive = process_is_alive(holder_pid)
    probe_status = "missing-lock-file"
    lock_state = recorded_state or "not-created"

    if lock_path.exists():
        try:
            with lock_path.open("a+", encoding="utf-8") as handle:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    probe_status = "locked"
                else:
                    probe_status = "available"
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            probe_status = "probe-failed"

    if recorded_state == "held":
        lock_state = "held" if pid_alive or probe_status == "locked" else "stale"
    elif probe_status == "locked":
        lock_state = "held-untracked"
    elif recorded_state == "released":
        lock_state = "released"
    elif probe_status == "available":
        lock_state = "available"

    return {
        "schema_version": LOCK_STATE_SCHEMA,
        "generated_at_utc": utc_now_iso(),
        "run_dir": str(run_dir),
        "lock_path": str(lock_path),
        "lock_state_path": str(state_path),
        "lock_state": lock_state,
        "recorded_state": recorded_state,
        "probe_status": probe_status,
        "holder_pid": holder_pid,
        "holder_pid_alive": pid_alive,
        "acquired_at_utc": maybe_text(state.get("acquired_at_utc")),
        "released_at_utc": maybe_text(state.get("released_at_utc")),
        "metadata": state.get("metadata", {}) if isinstance(state.get("metadata"), dict) else {},
    }


@contextmanager
def exclusive_runtime_lock(
    run_dir: Path,
    *,
    metadata: dict[str, Any] | None = None,
) -> Iterator[Path]:
    path = execution_lock_path(run_dir)
    state_path = execution_lock_state_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        acquired_at = utc_now_iso()
        lock_metadata = metadata if isinstance(metadata, dict) else {}
        write_json(
            state_path,
            {
                "schema_version": LOCK_STATE_SCHEMA,
                "lock_state": "held",
                "lock_path": str(path),
                "lock_state_path": str(state_path),
                "holder_pid": os.getpid(),
                "acquired_at_utc": acquired_at,
                "released_at_utc": "",
                "metadata": lock_metadata,
            },
        )
        try:
            yield path
        finally:
            write_json(
                state_path,
                {
                    "schema_version": LOCK_STATE_SCHEMA,
                    "lock_state": "released",
                    "lock_path": str(path),
                    "lock_state_path": str(state_path),
                    "holder_pid": os.getpid(),
                    "acquired_at_utc": acquired_at,
                    "released_at_utc": utc_now_iso(),
                    "metadata": lock_metadata,
                },
            )
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

from __future__ import annotations

from pathlib import Path
from typing import Any


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def orphaned_artifact_wrapper(path: Path, *, source: str) -> dict[str, Any]:
    return {
        "payload": None,
        "source": source,
        "artifact_path": str(path),
        "artifact_present": True,
        "payload_present": False,
    }


__all__ = [
    "list_items",
    "maybe_text",
    "normalize_space",
    "orphaned_artifact_wrapper",
    "resolve_path",
]

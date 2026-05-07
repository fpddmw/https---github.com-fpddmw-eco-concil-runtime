from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

class SkillExecutionError(RuntimeError):
    def __init__(self, message: str, payload: dict[str, Any] | None = None):
        super().__init__(message)
        self.payload = payload or {}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_hash(payload: Any) -> str:
    return stable_hash(json.dumps(payload, ensure_ascii=True, sort_keys=True))


def new_runtime_event_id(prefix: str, *parts: Any) -> str:
    import uuid

    return prefix + "-" + stable_hash(uuid.uuid4().hex, *parts)[:20]


def backoff_delay_seconds(retry_backoff_ms: int, attempt_number: int) -> float:
    if retry_backoff_ms <= 0:
        return 0.0
    return max(0.0, (retry_backoff_ms * max(1, attempt_number)) / 1000.0)


def retryable_return_code(return_code: int) -> bool:
    return return_code != 0


__all__ = (
    "SkillExecutionError",
    "utc_now_iso",
    "maybe_text",
    "stable_hash",
    "json_hash",
    "new_runtime_event_id",
    "backoff_delay_seconds",
    "retryable_return_code",
)

from __future__ import annotations

from typing import Any

PERMISSION_PROFILES = ("standard", "restricted", "network-enabled")
DEFAULT_ADMISSION_POLICY_SCHEMA = "runtime-admission-policy-v1"
DEFAULT_DEAD_LETTER_SCHEMA = "runtime-dead-letter-v1"
DEFAULT_HEALTH_SCHEMA = "runtime-health-v1"
ALWAYS_ALLOWED_SIDE_EFFECTS = {"reads-artifacts", "writes-artifacts"}
RUNBOOK_SECTIONS = {
    "admission": "Admission Blocks",
    "timeout": "Timeout Recovery",
    "subprocess": "Subprocess Failures",
    "payload-contract": "Payload Contract Failures",
    "workflow": "Workflow Stage Failures",
    "unknown": "Unknown Failures",
}



def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def stable_hash(*parts: Any) -> str:
    import hashlib

    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


__all__ = (
    "PERMISSION_PROFILES",
    "DEFAULT_ADMISSION_POLICY_SCHEMA",
    "DEFAULT_DEAD_LETTER_SCHEMA",
    "DEFAULT_HEALTH_SCHEMA",
    "ALWAYS_ALLOWED_SIDE_EFFECTS",
    "RUNBOOK_SECTIONS",
    "maybe_text",
    "unique_texts",
    "stable_hash",
    "utc_now_iso",
)

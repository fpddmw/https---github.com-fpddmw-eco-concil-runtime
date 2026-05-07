from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor_common import maybe_text
from eco_council_runtime.kernel.operator.operations import refresh_runtime_surfaces

def structured_failure(
    *,
    error_code: str,
    message: str,
    retryable: bool,
    attempts: list[dict[str, Any]],
    execution_policy: dict[str, Any],
    recovery_hints: list[str],
) -> dict[str, Any]:
    return {
        "error_code": error_code,
        "message": message,
        "retryable": retryable,
        "attempt_count": len(attempts),
        "last_attempt": attempts[-1] if attempts else {},
        "execution_policy": execution_policy,
        "recovery_hints": recovery_hints,
    }


DEAD_LETTER_ID_PATTERN = re.compile(r"(deadletter-[0-9a-f]{20})")


def extract_dead_letter_id(*texts: str) -> str:
    for text in texts:
        match = DEAD_LETTER_ID_PATTERN.search(maybe_text(text))
        if match:
            return match.group(1)
    return ""


def refresh_runtime_surfaces_safely(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    try:
        return refresh_runtime_surfaces(run_dir, round_id=round_id)
    except Exception:  # noqa: BLE001
        return {}


__all__ = (
    "structured_failure",
    "DEAD_LETTER_ID_PATTERN",
    "extract_dead_letter_id",
    "refresh_runtime_surfaces_safely",
)

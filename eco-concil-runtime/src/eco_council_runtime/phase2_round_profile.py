from __future__ import annotations

from pathlib import Path
import re

from .kernel.executor import maybe_text
from .kernel.source_queue_history import discovered_round_ids

ROUND_SUFFIX_PATTERN = re.compile(r"^(?P<prefix>.*?)(?P<number>\d+)$")


def increment_round_id(round_id: str) -> str:
    normalized = maybe_text(round_id)
    match = ROUND_SUFFIX_PATTERN.match(normalized)
    if match is None:
        return f"{normalized}-next"
    prefix = maybe_text(match.group("prefix"))
    number_text = maybe_text(match.group("number"))
    return f"{prefix}{int(number_text) + 1:0{len(number_text)}d}"


def default_next_round_id_builder(
    *,
    run_dir: Path,
    current_round_id: str,
) -> str:
    observed = set(discovered_round_ids(run_dir))
    candidate = increment_round_id(current_round_id)
    while candidate in observed:
        candidate = increment_round_id(candidate)
    return candidate


__all__ = [
    "default_next_round_id_builder",
    "increment_round_id",
]

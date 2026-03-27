"""Shared round-id helpers for eco-council runtime flows."""

from __future__ import annotations

import re
import sys
from typing import Any

from eco_council_runtime.domain.text import maybe_text

ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_ID_INPUT_PATTERN = re.compile(r"^round[-_](\d{3})$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")


def normalize_round_id(round_id: Any) -> str:
    text = maybe_text(round_id)
    match = ROUND_ID_INPUT_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(f"Unsupported round_id format: {round_id!r}. Expected round-001 or round_001 style.")
    return f"round-{match.group(1)}"


def round_dir_name(round_id: Any) -> str:
    return normalize_round_id(round_id).replace("-", "_")


def round_id_from_dirname(dirname: Any) -> str | None:
    match = ROUND_DIR_PATTERN.fullmatch(maybe_text(dirname))
    if match is None:
        return None
    return f"round-{match.group(1)}"


def round_number(round_id: Any) -> int:
    return int(normalize_round_id(round_id).split("-")[-1])


def next_round_id(round_id: Any) -> str:
    return f"round-{round_number(round_id) + 1:03d}"


def strict_round_sort_key(round_id: Any) -> tuple[int, str]:
    text = maybe_text(round_id)
    try:
        return (round_number(text), text)
    except ValueError:
        return (sys.maxsize, text)


def parse_round_components(round_id: Any) -> tuple[str, int, int] | None:
    text = maybe_text(round_id)
    match = re.match(r"^(.*?)(\d+)$", text)
    if match is None:
        return None
    prefix, digits = match.groups()
    return prefix, int(digits), len(digits)


def current_round_number(round_id: Any) -> int | None:
    components = parse_round_components(round_id)
    if components is None:
        return None
    return components[1]


def next_round_id_for(round_id: Any) -> str:
    components = parse_round_components(round_id)
    if components is None:
        return f"{maybe_text(round_id)}-next"
    prefix, number, width = components
    return f"{prefix}{number + 1:0{width}d}"


def round_sort_key(round_id: Any) -> tuple[str, int, str]:
    text = maybe_text(round_id)
    components = parse_round_components(text)
    if components is None:
        return (text, 10**9, text)
    prefix, number, _width = components
    return (prefix, number, text)

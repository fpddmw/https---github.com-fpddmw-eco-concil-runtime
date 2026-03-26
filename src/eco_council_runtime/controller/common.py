"""Small generic helpers shared across controller modules."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.controller.io import maybe_text


def maybe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def unique_strings(values: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def first_nonempty(items: list[str]) -> str:
    for item in items:
        text = maybe_text(item)
        if text:
            return text
    return ""


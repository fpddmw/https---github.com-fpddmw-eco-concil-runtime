"""Shared text-normalization helpers for the eco-council runtime."""

from __future__ import annotations

from typing import Any, Iterable


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def text_truthy(value: Any) -> bool:
    return maybe_text(value).casefold() in {"1", "true", "yes", "on"}


def unique_strings(values: Iterable[Any], *, casefold: bool = False) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        key = text.casefold() if casefold else text
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output

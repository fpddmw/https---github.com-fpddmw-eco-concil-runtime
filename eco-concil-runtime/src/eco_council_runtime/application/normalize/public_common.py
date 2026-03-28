"""Shared helpers for public-source normalization."""

from __future__ import annotations

import re
import statistics
from collections import Counter
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import stable_hash, utc_now_iso
from eco_council_runtime.domain.text import maybe_text, normalize_space


def source_domain(value: str) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    text = text.casefold()
    if "://" in text:
        text = text.split("://", 1)[1]
    domain = text.split("/", 1)[0]
    return domain[4:] if domain.startswith("www.") else domain


def top_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
    return items


def top_counter_text(counter: Counter[str], limit: int = 3) -> str:
    parts = [f"{item['value']} ({item['count']})" for item in top_counter_items(counter, limit=limit)]
    return ", ".join(parts)


def maybe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.fmean(values), 3)


def collect_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("records", "items", "data", "results"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
    return []


def make_public_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    signal_kind: str,
    external_id: str,
    title: str,
    text: str,
    url: str,
    author_name: str,
    channel_name: str,
    language: str,
    query_text: str,
    published_at_utc: str | None,
    engagement: dict[str, Any],
    metadata: dict[str, Any],
    artifact_path: Path,
    record_locator: str,
    sha256_value: str,
    raw_obj: Any,
) -> dict[str, Any]:
    identity = external_id or url or f"{signal_kind}:{record_locator}"
    signal_hash = stable_hash(source_skill, identity, maybe_text(title), maybe_text(text))
    return {
        "signal_id": f"pubsig-{signal_hash[:12]}",
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "external_id": external_id,
        "title": title,
        "text": text,
        "url": url,
        "author_name": author_name,
        "channel_name": channel_name,
        "language": language,
        "query_text": query_text,
        "published_at_utc": published_at_utc,
        "captured_at_utc": utc_now_iso(),
        "engagement": engagement,
        "metadata": metadata,
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "sha256": sha256_value,
        "raw_json": raw_obj,
    }


def strip_simple_html(value: str) -> str:
    return normalize_space(re.sub(r"<[^>]+>", " ", value))


__all__ = [
    "collect_records",
    "make_public_signal",
    "maybe_mean",
    "source_domain",
    "strip_simple_html",
    "top_counter_items",
    "top_counter_text",
]

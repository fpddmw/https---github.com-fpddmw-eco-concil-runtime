"""Cache and payload helpers for source normalization pipelines."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import (
    load_json_if_exists,
    read_json,
    read_jsonl,
    stable_hash,
    write_json,
)

NORMALIZE_CACHE_VERSION = "v3"


def normalize_cache_dir(run_dir: Path) -> Path:
    return run_dir / "analytics" / "normalize_cache"


def normalize_cache_path(
    run_dir: Path,
    *,
    domain: str,
    source_skill: str,
    run_id: str,
    round_id: str,
    artifact_sha256: str,
) -> Path:
    key = stable_hash(NORMALIZE_CACHE_VERSION, domain, source_skill, run_id, round_id, artifact_sha256)
    safe_domain = re.sub(r"[^a-z0-9_-]+", "-", domain.lower())
    safe_source = re.sub(r"[^a-z0-9_-]+", "-", source_skill.lower())
    return normalize_cache_dir(run_dir) / safe_domain / f"{safe_source}_{key[:16]}.json"


def read_cache_payload(path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(path)
    if not isinstance(payload, dict):
        return None
    return payload


def write_cache_payload(path: Path, payload: dict[str, Any]) -> None:
    write_json(path, payload, pretty=False)


def parse_source_payload(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return read_json(path)
    if suffix == ".jsonl":
        return read_jsonl(path)
    raise ValueError(f"Unsupported JSON payload path: {path}")


__all__ = [
    "NORMALIZE_CACHE_VERSION",
    "normalize_cache_dir",
    "normalize_cache_path",
    "parse_source_payload",
    "read_cache_payload",
    "write_cache_payload",
]

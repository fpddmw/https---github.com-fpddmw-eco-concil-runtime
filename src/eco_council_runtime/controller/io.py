"""Low-level IO and subprocess helpers for the eco-council controller."""

from __future__ import annotations

from eco_council_runtime.adapters.filesystem import (
    atomic_write_text_file,
    cloned_json,
    exclusive_file_lock,
    extract_json_suffix,
    file_sha256,
    load_json_if_exists,
    load_text,
    pretty_json,
    read_json,
    read_jsonl,
    run_check_command,
    run_json_command,
    stable_hash,
    stable_json,
    utc_now_iso,
    write_json,
    write_jsonl,
    write_text,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text

__all__ = [
    "atomic_write_text_file",
    "cloned_json",
    "exclusive_file_lock",
    "extract_json_suffix",
    "file_sha256",
    "load_json_if_exists",
    "load_text",
    "maybe_text",
    "pretty_json",
    "read_json",
    "read_jsonl",
    "run_check_command",
    "run_json_command",
    "stable_hash",
    "stable_json",
    "truncate_text",
    "utc_now_iso",
    "write_json",
    "write_jsonl",
    "write_text",
]

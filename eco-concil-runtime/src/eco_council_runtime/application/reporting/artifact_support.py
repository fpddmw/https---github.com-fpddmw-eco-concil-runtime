"""Shared support for reporting artifact packet, prompt, and promotion flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import atomic_write_text_file, load_json_if_exists
from eco_council_runtime.controller.paths import report_draft_path, report_target_path

REPORT_ROLES = ("sociologist", "environmentalist")
READINESS_ROLES = ("sociologist", "environmentalist")
PROMOTABLE_REPORT_ROLES = ("sociologist", "environmentalist", "historian")


def write_text(path: Path, text: str) -> None:
    atomic_write_text_file(path, text)


def load_report_for_decision(run_dir: Path, round_id: str, role: str, *, prefer_drafts: bool) -> tuple[dict[str, Any] | None, str]:
    final_report = load_json_if_exists(report_target_path(run_dir, round_id, role))
    if not isinstance(final_report, dict):
        final_report = None
    draft_report = load_json_if_exists(report_draft_path(run_dir, round_id, role))
    if not isinstance(draft_report, dict):
        draft_report = None
    if prefer_drafts and draft_report is not None:
        return draft_report, "draft"
    if final_report is not None:
        return final_report, "final"
    if draft_report is not None:
        return draft_report, "draft"
    return None, "missing"


__all__ = [
    "PROMOTABLE_REPORT_ROLES",
    "READINESS_ROLES",
    "REPORT_ROLES",
    "load_report_for_decision",
    "write_text",
]

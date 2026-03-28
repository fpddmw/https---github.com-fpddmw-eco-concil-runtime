"""Archive import orchestration helpers for supervisor lifecycle flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.cli_invocation import runtime_module_argv
from eco_council_runtime.controller.io import maybe_text, run_json_command, utc_now_iso
from eco_council_runtime.controller.state_config import ensure_case_library_archive_config, ensure_signal_corpus_config
from eco_council_runtime.layout import PROJECT_DIR

REPO_DIR = PROJECT_DIR


def case_library_argv(*args: object) -> list[str]:
    return runtime_module_argv("case_library", *args)


def signal_corpus_argv(*args: object) -> list[str]:
    return runtime_module_argv("signal_corpus", *args)


def maybe_auto_import_signal_corpus(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any] | None:
    signal_corpus = ensure_signal_corpus_config(state)
    db_text = maybe_text(signal_corpus.get("db"))
    if not db_text or not bool(signal_corpus.get("auto_import")):
        return {
            "enabled": bool(db_text),
            "attempted": False,
        }
    attempted_at_utc = utc_now_iso()
    try:
        payload = run_json_command(
            signal_corpus_argv("import-run", "--db", db_text, "--run-dir", str(run_dir), "--overwrite", "--pretty"),
            cwd=REPO_DIR,
        )
        result = {
            "enabled": True,
            "attempted": True,
            "ok": True,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "import_result": payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload,
        }
        signal_corpus["last_imported_round_id"] = round_id
        signal_corpus["last_imported_at_utc"] = attempted_at_utc
    except Exception as exc:  # noqa: BLE001
        result = {
            "enabled": True,
            "attempted": True,
            "ok": False,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "error": str(exc),
        }
    signal_corpus["last_import"] = result
    state["signal_corpus"] = signal_corpus
    return result


def maybe_auto_import_case_library(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any] | None:
    archive = ensure_case_library_archive_config(state)
    db_text = maybe_text(archive.get("db"))
    if not db_text or not bool(archive.get("auto_import")):
        return {
            "enabled": bool(db_text),
            "attempted": False,
        }
    attempted_at_utc = utc_now_iso()
    try:
        payload = run_json_command(
            case_library_argv("import-run", "--db", db_text, "--run-dir", str(run_dir), "--overwrite", "--pretty"),
            cwd=REPO_DIR,
        )
        result = {
            "enabled": True,
            "attempted": True,
            "ok": True,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "import_result": payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload,
        }
        archive["last_imported_round_id"] = round_id
        archive["last_imported_at_utc"] = attempted_at_utc
    except Exception as exc:  # noqa: BLE001
        result = {
            "enabled": True,
            "attempted": True,
            "ok": False,
            "db": db_text,
            "round_id": round_id,
            "attempted_at_utc": attempted_at_utc,
            "error": str(exc),
        }
    archive["last_import"] = result
    state["case_library_archive"] = archive
    return result


__all__ = [
    "case_library_argv",
    "maybe_auto_import_case_library",
    "maybe_auto_import_signal_corpus",
    "signal_corpus_argv",
]
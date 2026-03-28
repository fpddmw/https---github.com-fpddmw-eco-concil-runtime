"""Supervisor state configuration helpers for archives and history context."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from eco_council_runtime.application.investigation.history_context import materialize_history_context_artifacts
from eco_council_runtime.controller.constants import DEFAULT_HISTORY_TOP_K, MAX_HISTORY_TOP_K
from eco_council_runtime.controller.io import maybe_text
from eco_council_runtime.layout import RUNS_ROOT

DEFAULT_ARCHIVE_DIR = RUNS_ROOT / "archives"
DEFAULT_CASE_LIBRARY_DB = DEFAULT_ARCHIVE_DIR / "eco_council_case_library.sqlite"
DEFAULT_SIGNAL_CORPUS_DB = DEFAULT_ARCHIVE_DIR / "eco_council_signal_corpus.sqlite"


def normalize_history_top_k(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = DEFAULT_HISTORY_TOP_K
    return max(1, min(MAX_HISTORY_TOP_K, count))


def ensure_history_context_config(state: dict[str, Any]) -> dict[str, Any]:
    history = state.get("history_context")
    if not isinstance(history, dict):
        history = {}
    history["db"] = maybe_text(history.get("db"))
    history["top_k"] = normalize_history_top_k(history.get("top_k"))
    state["history_context"] = history
    return history


def apply_history_cli_config(state: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    history = ensure_history_context_config(state)
    if bool(getattr(args, "disable_history_context", False)):
        history["db"] = ""
    elif maybe_text(getattr(args, "history_db", "")):
        history["db"] = str(Path(args.history_db).expanduser().resolve())
    top_k_value = int(getattr(args, "history_top_k", 0) or 0)
    if top_k_value > 0:
        history["top_k"] = normalize_history_top_k(top_k_value)
    state["history_context"] = history
    return history


def history_cli_updates_requested(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "disable_history_context", False)
        or maybe_text(getattr(args, "history_db", ""))
        or int(getattr(args, "history_top_k", 0) or 0) > 0
    )


def default_case_library_db_path() -> str:
    return str(DEFAULT_CASE_LIBRARY_DB.resolve())


def default_signal_corpus_db_path() -> str:
    return str(DEFAULT_SIGNAL_CORPUS_DB.resolve())


def ensure_case_library_archive_config(state: dict[str, Any]) -> dict[str, Any]:
    archive = state.get("case_library_archive")
    if not isinstance(archive, dict):
        archive = {
            "db": default_case_library_db_path(),
            "auto_import": True,
            "last_imported_round_id": "",
            "last_imported_at_utc": "",
            "last_import": {},
        }
    db_text = maybe_text(archive.get("db"))
    if not db_text and "db" not in archive:
        db_text = default_case_library_db_path()
    archive["db"] = db_text
    if not db_text:
        archive["auto_import"] = False
    elif "auto_import" not in archive:
        archive["auto_import"] = True
    else:
        archive["auto_import"] = bool(archive.get("auto_import"))
    archive["last_imported_round_id"] = maybe_text(archive.get("last_imported_round_id"))
    archive["last_imported_at_utc"] = maybe_text(archive.get("last_imported_at_utc"))
    last_import = archive.get("last_import")
    if not isinstance(last_import, dict):
        last_import = {}
    archive["last_import"] = last_import
    state["case_library_archive"] = archive
    return archive


def apply_case_library_archive_cli_config(state: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    archive = ensure_case_library_archive_config(state)
    if bool(getattr(args, "disable_auto_archive", False)):
        archive["db"] = ""
        archive["auto_import"] = False
    elif maybe_text(getattr(args, "case_library_db", "")):
        archive["db"] = str(Path(args.case_library_db).expanduser().resolve())
        archive["auto_import"] = True
    state["case_library_archive"] = archive
    return archive


def case_library_archive_cli_updates_requested(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "disable_auto_archive", False)
        or maybe_text(getattr(args, "case_library_db", ""))
    )


def ensure_signal_corpus_config(state: dict[str, Any]) -> dict[str, Any]:
    signal_corpus = state.get("signal_corpus")
    if not isinstance(signal_corpus, dict):
        signal_corpus = {
            "db": default_signal_corpus_db_path(),
            "auto_import": True,
            "last_imported_round_id": "",
            "last_imported_at_utc": "",
            "last_import": {},
        }
    db_text = maybe_text(signal_corpus.get("db"))
    if not db_text and "db" not in signal_corpus:
        db_text = default_signal_corpus_db_path()
    signal_corpus["db"] = db_text
    if not db_text:
        signal_corpus["auto_import"] = False
    elif "auto_import" not in signal_corpus:
        signal_corpus["auto_import"] = True
    else:
        signal_corpus["auto_import"] = bool(signal_corpus.get("auto_import"))
    signal_corpus["last_imported_round_id"] = maybe_text(signal_corpus.get("last_imported_round_id"))
    signal_corpus["last_imported_at_utc"] = maybe_text(signal_corpus.get("last_imported_at_utc"))
    last_import = signal_corpus.get("last_import")
    if not isinstance(last_import, dict):
        last_import = {}
    signal_corpus["last_import"] = last_import
    state["signal_corpus"] = signal_corpus
    return signal_corpus


def apply_signal_corpus_cli_config(state: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    signal_corpus = ensure_signal_corpus_config(state)
    if bool(getattr(args, "disable_auto_archive", False) or getattr(args, "disable_signal_corpus_import", False)):
        signal_corpus["db"] = ""
        signal_corpus["auto_import"] = False
    elif maybe_text(getattr(args, "signal_corpus_db", "")):
        signal_corpus["db"] = str(Path(args.signal_corpus_db).expanduser().resolve())
        signal_corpus["auto_import"] = True
    state["signal_corpus"] = signal_corpus
    return signal_corpus


def signal_corpus_cli_updates_requested(args: argparse.Namespace) -> bool:
    return bool(
        getattr(args, "disable_auto_archive", False)
        or getattr(args, "disable_signal_corpus_import", False)
        or maybe_text(getattr(args, "signal_corpus_db", ""))
    )


def write_history_context_file(run_dir: Path, state: dict[str, Any], round_id: str) -> Path | None:
    history = ensure_history_context_config(state)
    result = materialize_history_context_artifacts(
        run_dir,
        state,
        round_id,
        pretty=True,
    )
    if not isinstance(result, dict):
        return None
    context_path = maybe_text(result.get("history_context_path"))
    if not context_path:
        return None
    return Path(context_path)

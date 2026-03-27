"""Supervisor state configuration helpers for archives and history context."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from eco_council_runtime.cli_invocation import runtime_module_argv
from eco_council_runtime.controller.constants import DEFAULT_HISTORY_TOP_K, MAX_HISTORY_TOP_K
from eco_council_runtime.controller.io import load_json_if_exists, maybe_text, run_json_command, write_text
from eco_council_runtime.controller.paths import history_context_path, investigation_plan_path, mission_path
from eco_council_runtime.investigation import build_investigation_plan
from eco_council_runtime.layout import PROJECT_DIR, RUNS_ROOT

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


def render_history_context_text(
    *,
    mission: dict[str, Any],
    search_payload: dict[str, Any],
    investigation_plan: dict[str, Any] | None = None,
) -> str:
    cases = search_payload.get("cases") if isinstance(search_payload.get("cases"), list) else []
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    plan = investigation_plan if isinstance(investigation_plan, dict) else {}
    history_query = plan.get("history_query") if isinstance(plan.get("history_query"), dict) else {}
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    open_questions = plan.get("open_questions") if isinstance(plan.get("open_questions"), list) else []
    retrieval_focus_parts = []
    for field_name in ("claim_types", "metric_families", "gap_types", "source_skills"):
        values = (
            search_payload.get(field_name)
            if isinstance(search_payload.get(field_name), list)
            else history_query.get(field_name)
        )
        if isinstance(values, list):
            text = ", ".join(maybe_text(item) for item in values if maybe_text(item))
            if text:
                retrieval_focus_parts.append(f"{field_name}={text}")
    lines = [
        "Compact historical-case context from the local eco-council case library.",
        "Use it only as planning guidance. Current-round evidence remains primary.",
        "Do not repeat exhausted fetch paths unless the region, time window, or claim mix is materially different.",
        "",
        f"Current topic: {maybe_text(mission.get('topic'))}",
        f"Current objective: {maybe_text(mission.get('objective'))}",
        f"Current region: {maybe_text(region.get('label')) or 'n/a'}",
        f"Current investigation profile: {maybe_text(history_query.get('profile_id')) or maybe_text(plan.get('profile_id')) or 'n/a'}",
        "",
        f"Retrieved similar cases: {len(cases)}",
    ]
    if retrieval_focus_parts:
        lines.extend(["Retrieval focus: " + "; ".join(retrieval_focus_parts)])
    priority_legs = history_query.get("priority_leg_ids") if isinstance(history_query.get("priority_leg_ids"), list) else []
    if priority_legs:
        lines.append("Priority legs: " + ", ".join(maybe_text(item) for item in priority_legs if maybe_text(item)))
    if hypotheses:
        lines.extend(["", f"Primary hypotheses: {len(hypotheses)}"])
        for index, hypothesis in enumerate(hypotheses[:3], start=1):
            if not isinstance(hypothesis, dict):
                continue
            lines.append(f"{index}. {maybe_text(hypothesis.get('summary')) or maybe_text(hypothesis.get('statement'))}")
            alternatives = hypothesis.get("alternative_hypotheses")
            if isinstance(alternatives, list) and alternatives:
                alt_text = "; ".join(
                    maybe_text(item.get("summary")) or maybe_text(item.get("statement"))
                    for item in alternatives[:2]
                    if isinstance(item, dict) and (maybe_text(item.get("summary")) or maybe_text(item.get("statement")))
                )
                if alt_text:
                    lines.append(f"   alternatives={alt_text}")
    if open_questions:
        lines.extend(["", "Open planning questions:"])
        for question in open_questions[:4]:
            if maybe_text(question):
                lines.append(f"- {maybe_text(question)}")
    for index, case in enumerate(cases, start=1):
        if not isinstance(case, dict):
            continue
        missing = case.get("final_missing_evidence_types")
        missing_text = ", ".join(maybe_text(item) for item in missing if maybe_text(item)) if isinstance(missing, list) else ""
        reasons = case.get("match_reasons")
        reason_text = ", ".join(maybe_text(item) for item in reasons if maybe_text(item)) if isinstance(reasons, list) else ""
        planning_overlap_parts = []
        if maybe_text(case.get("case_profile_id")):
            planning_overlap_parts.append(f"profile={maybe_text(case.get('case_profile_id'))}")
        for field_name in ("matched_claim_types", "matched_metric_families", "matched_gap_types", "matched_source_skills"):
            values = case.get(field_name)
            if not isinstance(values, list):
                continue
            text = ", ".join(maybe_text(item) for item in values if maybe_text(item))
            if text:
                planning_overlap_parts.append(f"{field_name}={text}")
        lines.extend(
            [
                "",
                f"{index}. case_id={maybe_text(case.get('case_id'))}; score={case.get('score')}; region={maybe_text(case.get('region_label'))}; rounds={case.get('round_count')}; moderator_status={maybe_text(case.get('final_moderator_status')) or 'unknown'}; evidence={maybe_text(case.get('final_evidence_sufficiency')) or 'unknown'}",
                f"   topic={maybe_text(case.get('topic'))}",
                f"   decision_summary={maybe_text(case.get('final_decision_summary')) or maybe_text(case.get('final_brief')) or 'n/a'}",
            ]
        )
        if planning_overlap_parts:
            lines.append("   planning_overlap=" + "; ".join(planning_overlap_parts))
        if missing_text:
            lines.append(f"   missing_evidence_types={missing_text}")
        if reason_text:
            lines.append(f"   match_reasons={reason_text}")
    return "\n".join(lines)


def write_history_context_file(run_dir: Path, state: dict[str, Any], round_id: str) -> Path | None:
    target = history_context_path(run_dir, round_id)
    history = ensure_history_context_config(state)
    db_text = maybe_text(history.get("db"))
    if not db_text:
        if target.exists():
            target.unlink()
        return None

    db_path = Path(db_text).expanduser().resolve()
    if not db_path.exists():
        if target.exists():
            target.unlink()
        return None

    mission_payload = load_json_if_exists(mission_path(run_dir))
    if not isinstance(mission_payload, dict):
        if target.exists():
            target.unlink()
        return None

    plan_payload = load_json_if_exists(investigation_plan_path(run_dir, round_id))
    if not isinstance(plan_payload, dict) or not isinstance(plan_payload.get("history_query"), dict):
        plan_payload = build_investigation_plan(mission=mission_payload, round_id=round_id)
    history_query = plan_payload.get("history_query") if isinstance(plan_payload.get("history_query"), dict) else {}
    region = mission_payload.get("region") if isinstance(mission_payload.get("region"), dict) else {}
    query = maybe_text(history_query.get("query")) or maybe_text(mission_payload.get("topic"))
    region_label = maybe_text(history_query.get("region_label")) or maybe_text(region.get("label"))
    argv = runtime_module_argv(
        "case_library",
        "search-cases",
        "--db",
        db_path,
        "--exclude-case-id",
        maybe_text(mission_payload.get("run_id")),
        "--limit",
        str(normalize_history_top_k(history.get("top_k"))),
        "--pretty",
    )
    if query:
        argv.extend(["--query", query])
    if region_label:
        argv.extend(["--region-label", region_label])
    if maybe_text(history_query.get("profile_id")):
        argv.extend(["--profile-id", maybe_text(history_query.get("profile_id"))])
    for field_name, flag in (
        ("claim_types", "--claim-type"),
        ("metric_families", "--metric-family"),
        ("gap_types", "--gap-type"),
        ("source_skills", "--source-skill"),
    ):
        values = history_query.get(field_name)
        if not isinstance(values, list):
            continue
        for value in values:
            if maybe_text(value):
                argv.extend([flag, maybe_text(value)])

    try:
        payload = run_json_command(argv, cwd=PROJECT_DIR)
    except Exception:
        if target.exists():
            target.unlink()
        return None
    search_payload = payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), dict) else payload
    cases = search_payload.get("cases") if isinstance(search_payload, dict) else None
    if not isinstance(cases, list) or not cases:
        if target.exists():
            target.unlink()
        return None

    write_text(
        target,
        render_history_context_text(
            mission=mission_payload,
            search_payload=search_payload,
            investigation_plan=plan_payload,
        ),
    )
    return target

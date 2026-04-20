from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .deliberation_plane import load_phase2_control_state
from .executor import maybe_text, new_runtime_event_id, utc_now_iso
from .phase2_state_surfaces import (
    build_reporting_surface,
    load_council_decision_wrapper,
    load_falsification_probe_wrapper,
    load_final_publication_wrapper,
    load_next_actions_wrapper,
    load_reporting_handoff_wrapper,
    load_supervisor_state_wrapper,
)
from .ledger import append_ledger_event, load_ledger_tail
from .manifest import load_json_if_exists, write_json
from .paths import (
    benchmark_compare_path,
    benchmark_manifest_path,
    controller_state_path,
    ensure_runtime_dirs,
    history_bootstrap_state_path,
    orchestration_plan_path,
    promotion_gate_path,
    replay_report_path,
    round_close_state_path,
    scenario_baseline_manifest_path,
    scenario_fixture_path,
    supervisor_state_path,
)
from .source_queue_history import load_round_tasks_wrapper

BENCHMARK_EVENT_TYPES = {
    "benchmark-manifest",
    "benchmark-compare",
    "scenario-fixture",
    "scenario-replay",
}

INPUT_ARTIFACT_SPECS: tuple[tuple[str, str], ...] = (
    ("mission", "mission.json"),
    ("round_tasks", "investigation/round_tasks_{round_id}.json"),
    ("source_selection_sociologist", "runtime/source_selection_sociologist_{round_id}.json"),
    ("source_selection_environmentalist", "runtime/source_selection_environmentalist_{round_id}.json"),
    ("fetch_plan", "runtime/fetch_plan_{round_id}.json"),
)

OUTPUT_ARTIFACT_SPECS: tuple[tuple[str, str], ...] = (
    ("orchestration_plan", "runtime/orchestration_plan_{round_id}.json"),
    ("board_summary", "board/board_state_summary_{round_id}.json"),
    ("board_brief", "board/board_brief_{round_id}.md"),
    ("next_actions", "investigation/next_actions_{round_id}.json"),
    ("falsification_probes", "investigation/falsification_probes_{round_id}.json"),
    ("round_readiness", "reporting/round_readiness_{round_id}.json"),
    ("promotion_basis", "promotion/promoted_evidence_basis_{round_id}.json"),
    ("reporting_handoff", "reporting/reporting_handoff_{round_id}.json"),
    ("council_decision_draft", "reporting/council_decision_draft_{round_id}.json"),
    ("council_decision", "reporting/council_decision_{round_id}.json"),
    ("final_publication", "reporting/final_publication_{round_id}.json"),
    ("signal_archive_import", "archive/signal_corpus_import_{round_id}.json"),
    ("case_archive_import", "archive/case_library_import_{round_id}.json"),
    ("signal_corpus_query", "archive/signal_corpus_query_{round_id}.json"),
    ("case_library_query", "archive/case_library_query_{round_id}.json"),
    ("history_retrieval", "investigation/history_retrieval_{round_id}.json"),
    ("history_context", "investigation/history_context_{round_id}.md"),
)

VOLATILE_JSON_KEYS = {
    "generated_at_utc",
    "started_at_utc",
    "completed_at_utc",
    "updated_at_utc",
    "created_at_utc",
    "event_id",
    "receipt_id",
    "batch_id",
    "last_receipt_id",
    "last_event_id",
    "execution_input_hash",
    "payload_hash",
    "lock_path",
}

NON_SEMANTIC_JSON_KEYS = {
    "artifacts",
    "inspection_paths",
    "paths",
    "command_snapshot",
    "execution_policy",
}

RUN_ARTIFACT_ROOTS = (
    "analytics",
    "archive",
    "board",
    "investigation",
    "promotion",
    "reporting",
    "runtime",
    "receipts",
)

RECOVERABLE_INPUT_LOADERS = {
    "round_tasks": load_round_tasks_wrapper,
}

RECOVERABLE_OUTPUT_LOADERS = {
    "next_actions": load_next_actions_wrapper,
    "falsification_probes": load_falsification_probe_wrapper,
}


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def json_hash(payload: Any) -> str:
    return stable_hash(json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True))


def artifact_specs(round_id: str, specs: tuple[tuple[str, str], ...]) -> list[dict[str, str]]:
    return [
        {
            "artifact_key": artifact_key,
            "relative_path": template.format(round_id=round_id),
        }
        for artifact_key, template in specs
    ]


def artifact_preview(payload: Any) -> dict[str, Any]:
    return {
        "top_level_type": type(payload).__name__,
        "item_count": len(payload) if isinstance(payload, (dict, list)) else 1,
    }


def try_parse_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def parse_iso_datetime(value: Any) -> datetime | None:
    text = maybe_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def duration_seconds(started_at: Any, completed_at: Any) -> float | None:
    started = parse_iso_datetime(started_at)
    completed = parse_iso_datetime(completed_at)
    if started is None or completed is None:
        return None
    return round(max(0.0, (completed - started).total_seconds()), 6)


def rounded_number(value: Any) -> float:
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return 0.0


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def normalize_path_token(path_text: str, run_dir: Path) -> str:
    if not path_text or path_text.startswith("<"):
        return path_text
    candidate = Path(path_text)
    if not candidate.is_absolute():
        return path_text
    parts = list(candidate.parts)
    for root_name in RUN_ARTIFACT_ROOTS:
        if root_name in parts:
            index = parts.index(root_name)
            return f"<run_dir>/{Path(*parts[index:]).as_posix()}"
    if "archives" in parts:
        index = parts.index("archives")
        return f"<run_parent>/{Path(*parts[index:]).as_posix()}"
    resolved_run_dir = run_dir.resolve()
    resolved_parent = resolved_run_dir.parent
    try:
        return f"<run_dir>/{candidate.relative_to(resolved_run_dir).as_posix()}"
    except ValueError:
        pass
    try:
        return f"<run_parent>/{candidate.relative_to(resolved_parent).as_posix()}"
    except ValueError:
        return f"<external_path>/{candidate.name}"


def normalize_string_value(value: str, run_dir: Path) -> str:
    if not value:
        return ""
    text = str(value)
    path_token, separator, remainder = text.partition(":")
    if path_token.startswith("/"):
        normalized_path = normalize_path_token(path_token, run_dir)
        return normalized_path + (separator + remainder if separator else "")
    run_dir_text = run_dir.resolve().as_posix()
    run_parent_text = run_dir.resolve().parent.as_posix()
    text = text.replace(run_dir_text, "<run_dir>")
    text = text.replace(run_parent_text, "<run_parent>")
    return text


def normalize_json_value(value: Any, run_dir: Path) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key in sorted(value):
            if key in VOLATILE_JSON_KEYS:
                continue
            if key in NON_SEMANTIC_JSON_KEYS:
                continue
            if key.endswith("_command"):
                continue
            normalized[key] = normalize_json_value(value[key], run_dir)
        return normalized
    if isinstance(value, list):
        return [normalize_json_value(item, run_dir) for item in value]
    if isinstance(value, str):
        return normalize_string_value(value, run_dir)
    return value


def drop_json_keys(value: Any, *, keys: set[str]) -> Any:
    if isinstance(value, dict):
        return {
            key: drop_json_keys(item, keys=keys)
            for key, item in value.items()
            if key not in keys
        }
    if isinstance(value, list):
        return [drop_json_keys(item, keys=keys) for item in value]
    return value


def benchmark_payload_value(artifact_key: str, payload: Any) -> Any:
    if artifact_key in {"next_actions", "falsification_probes"}:
        return drop_json_keys(payload, keys={"action_source", "snapshot_id"})
    return payload


def payload_semantic_fingerprint(
    artifact_key: str,
    payload: Any,
    run_dir: Path,
) -> tuple[str, dict[str, Any]]:
    normalized_payload = normalize_json_value(
        benchmark_payload_value(artifact_key, payload),
        run_dir,
    )
    canonical = json.dumps(
        normalized_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return stable_hash(canonical), artifact_preview(payload)


def file_semantic_fingerprint(path: Path, run_dir: Path) -> tuple[str, str, dict[str, Any]]:
    payload = try_parse_json(path)
    if payload is not None:
        semantic_hash, preview = payload_semantic_fingerprint("", payload, run_dir)
        return "json", semantic_hash, preview
    normalized_text = normalize_string_value(path.read_text(encoding="utf-8"), run_dir)
    line_count = 0 if not normalized_text else normalized_text.count("\n") + 1
    preview = {"top_level_type": "text", "line_count": line_count}
    return "text", stable_hash(normalized_text), preview


def recoverable_loader(*, artifact_key: str, category: str):
    if category == "input":
        return RECOVERABLE_INPUT_LOADERS.get(artifact_key)
    return RECOVERABLE_OUTPUT_LOADERS.get(artifact_key)


def digest_artifact(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    artifact_key: str,
    relative_path: str,
    category: str,
) -> dict[str, Any]:
    path = (run_dir / relative_path).resolve()
    loader = recoverable_loader(artifact_key=artifact_key, category=category)
    if loader is not None:
        wrapper = loader(run_dir, run_id=run_id, round_id=round_id)
        payload_present = bool(wrapper.get("payload_present"))
        if payload_present:
            payload = wrapper.get("payload")
            semantic_hash, preview = payload_semantic_fingerprint(
                artifact_key,
                payload,
                run_dir,
            )
            return {
                "artifact_key": artifact_key,
                "category": category,
                "relative_path": relative_path,
                "exists": path.exists(),
                "artifact_present": path.exists(),
                "payload_present": True,
                "payload_source": maybe_text(wrapper.get("source")) or "artifact",
                "format": "json",
                "semantic_hash": semantic_hash,
                "byte_size": path.stat().st_size if path.exists() else 0,
                "preview": preview,
            }
        if path.exists():
            file_format, semantic_hash, preview = file_semantic_fingerprint(path, run_dir)
            return {
                "artifact_key": artifact_key,
                "category": category,
                "relative_path": relative_path,
                "exists": True,
                "artifact_present": True,
                "payload_present": False,
                "payload_source": "invalid-artifact",
                "format": file_format,
                "semantic_hash": semantic_hash,
                "byte_size": path.stat().st_size,
                "preview": preview,
            }
        return {
            "artifact_key": artifact_key,
            "category": category,
            "relative_path": relative_path,
            "exists": False,
            "artifact_present": False,
            "payload_present": False,
            "payload_source": maybe_text(wrapper.get("source")) or "missing",
            "format": "",
            "semantic_hash": "",
            "byte_size": 0,
            "preview": {},
        }
    if not path.exists():
        return {
            "artifact_key": artifact_key,
            "category": category,
            "relative_path": relative_path,
            "exists": False,
            "artifact_present": False,
            "payload_present": False,
            "payload_source": "missing",
            "format": "",
            "semantic_hash": "",
            "byte_size": 0,
            "preview": {},
        }
    file_format, semantic_hash, preview = file_semantic_fingerprint(path, run_dir)
    return {
        "artifact_key": artifact_key,
        "category": category,
        "relative_path": relative_path,
        "exists": True,
        "artifact_present": True,
        "payload_present": True,
        "payload_source": "artifact",
        "format": file_format,
        "semantic_hash": semantic_hash,
        "byte_size": path.stat().st_size,
        "preview": preview,
    }


def artifact_rows(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    specs: tuple[tuple[str, str], ...],
    category: str,
) -> list[dict[str, Any]]:
    return [
        digest_artifact(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            artifact_key=spec["artifact_key"],
            relative_path=spec["relative_path"],
            category=category,
        )
        for spec in artifact_specs(round_id, specs)
    ]


def comparison_artifact_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "artifact_key": maybe_text(row.get("artifact_key")),
            "present": bool(row.get("payload_present")),
            "semantic_hash": maybe_text(row.get("semantic_hash")),
        }
        for row in rows
        if maybe_text(row.get("artifact_key"))
    ]


def output_artifact_lookup(round_id: str) -> dict[str, str]:
    return {
        spec["relative_path"]: spec["artifact_key"]
        for spec in artifact_specs(round_id, OUTPUT_ARTIFACT_SPECS)
    }


def artifact_hash_lookup(rows: list[dict[str, Any]]) -> dict[str, str]:
    return {maybe_text(row.get("artifact_key")): maybe_text(row.get("semantic_hash")) for row in rows if maybe_text(row.get("artifact_key"))}


def resolve_output_artifact_key(run_dir: Path, round_id: str, artifact_path: Any) -> str:
    text = maybe_text(artifact_path)
    if not text:
        return ""
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = (run_dir / candidate).resolve()
    relative_path = ""
    try:
        relative_path = candidate.relative_to(run_dir.resolve()).as_posix()
    except ValueError:
        parts = list(candidate.parts)
        for root_name in RUN_ARTIFACT_ROOTS:
            if root_name in parts:
                index = parts.index(root_name)
                relative_path = Path(*parts[index:]).as_posix()
                break
    if not relative_path:
        return ""
    return output_artifact_lookup(round_id).get(relative_path, "")


def summarized_step_rows(
    run_dir: Path,
    round_id: str,
    steps: Any,
    *,
    artifact_hashes: dict[str, str],
) -> list[dict[str, Any]]:
    if not isinstance(steps, list):
        return []
    rows: list[dict[str, Any]] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        stage = maybe_text(step.get("stage"))
        if not stage:
            continue
        artifact_key = resolve_output_artifact_key(
            run_dir,
            round_id,
            maybe_text(step.get("artifact_path")) or maybe_text(step.get("expected_output_path")),
        )
        rows.append(
            {
                "stage": stage,
                "skill_name": maybe_text(step.get("skill_name")),
                "status": maybe_text(step.get("status")),
                "phase_group": maybe_text(step.get("phase_group")),
                "artifact_key": artifact_key,
                "artifact_hash": artifact_hashes.get(artifact_key, ""),
                "attempt_count": int(step.get("attempt_count") or 0),
                "recovered_after_retry": bool(step.get("recovered_after_retry")),
                "duration_seconds": duration_seconds(step.get("started_at_utc"), step.get("completed_at_utc")),
                "gate_status": maybe_text(step.get("gate_status")),
                "readiness_status": maybe_text(step.get("readiness_status")),
                "promote_allowed": bool(step.get("promote_allowed")),
            }
        )
    return rows


def comparison_step_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    comparison_rows: list[dict[str, Any]] = []
    for row in rows:
        comparison_rows.append(
            {
                "stage": maybe_text(row.get("stage")),
                "skill_name": maybe_text(row.get("skill_name")),
                "status": maybe_text(row.get("status")),
                "artifact_key": maybe_text(row.get("artifact_key")),
                "artifact_hash": maybe_text(row.get("artifact_hash")),
                "gate_status": maybe_text(row.get("gate_status")),
                "readiness_status": maybe_text(row.get("readiness_status")),
                "promote_allowed": bool(row.get("promote_allowed")),
            }
        )
    return comparison_rows


def phase2_state_snapshot(
    run_dir: Path,
    run_id: str,
    round_id: str,
    artifact_hashes: dict[str, str],
) -> dict[str, Any]:
    control_state = load_phase2_control_state(run_dir, run_id=run_id, round_id=round_id)
    plan = load_json_if_exists(orchestration_plan_path(run_dir, round_id)) or {}
    gate = (
        control_state.get("promotion_gate", {})
        if isinstance(control_state.get("promotion_gate"), dict)
        else {}
    ) or load_json_if_exists(promotion_gate_path(run_dir, round_id)) or {}
    controller = (
        control_state.get("controller", {})
        if isinstance(control_state.get("controller"), dict)
        else {}
    ) or load_json_if_exists(controller_state_path(run_dir, round_id)) or {}
    supervisor_context = load_supervisor_state_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        supervisor_state_path=str(supervisor_state_path(run_dir, round_id).resolve()),
    )
    supervisor = (
        supervisor_context.get("payload")
        if isinstance(supervisor_context.get("payload"), dict)
        else {}
    )
    handoff_context = load_reporting_handoff_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    decision_draft_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
    )
    decision_context = load_council_decision_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        decision_stage="canonical",
    )
    final_publication_context = load_final_publication_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    reporting_surface = build_reporting_surface(
        supervisor_payload=supervisor,
        handoff_payload=handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else {},
        decision_draft_payload=decision_draft_context.get("payload")
        if isinstance(decision_draft_context.get("payload"), dict)
        else {},
        decision_payload=decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else {},
        final_publication_payload=final_publication_context.get("payload")
        if isinstance(final_publication_context.get("payload"), dict)
        else {},
    )
    steps = summarized_step_rows(run_dir, round_id, controller.get("steps"), artifact_hashes=artifact_hashes)
    summary = {
        "planning_mode": maybe_text(controller.get("planning_mode")) or maybe_text(supervisor.get("planning_mode")) or "missing",
        "controller_status": maybe_text(controller.get("controller_status")) or "missing",
        "supervisor_status": maybe_text(supervisor.get("supervisor_status")) or "missing",
        "supervisor_substatus": maybe_text(supervisor.get("supervisor_substatus")),
        "phase2_posture": maybe_text(supervisor.get("phase2_posture")),
        "terminal_state": maybe_text(supervisor.get("terminal_state")),
        "readiness_status": maybe_text(supervisor.get("readiness_status")) or maybe_text(controller.get("readiness_status")) or maybe_text(gate.get("readiness_status")) or "unknown",
        "gate_status": maybe_text(supervisor.get("gate_status")) or maybe_text(controller.get("gate_status")) or maybe_text(gate.get("gate_status")) or "unknown",
        "promotion_status": maybe_text(supervisor.get("promotion_status")) or maybe_text(controller.get("promotion_status")) or "unknown",
        "reporting_ready": bool(reporting_surface.get("reporting_ready")),
        "reporting_blockers": (
            reporting_surface.get("reporting_blockers", [])
            if isinstance(reporting_surface.get("reporting_blockers"), list)
            else []
        ),
        "reporting_handoff_status": maybe_text(reporting_surface.get("handoff_status")),
        "reporting_surface_source": maybe_text(reporting_surface.get("surface_source")),
        "publication_status": maybe_text(reporting_surface.get("publication_status")),
        "publication_posture": maybe_text(reporting_surface.get("publication_posture")),
        "resume_status": maybe_text(controller.get("resume_status")),
        "failed_stage": maybe_text(controller.get("failed_stage")),
        "completed_stage_names": [maybe_text(item) for item in controller.get("completed_stage_names", []) if maybe_text(item)]
        if isinstance(controller.get("completed_stage_names"), list)
        else [],
        "pending_stage_names": [maybe_text(item) for item in controller.get("pending_stage_names", []) if maybe_text(item)]
        if isinstance(controller.get("pending_stage_names"), list)
        else [],
        "gate_reasons": [maybe_text(item) for item in controller.get("gate_reasons", []) if maybe_text(item)]
        if isinstance(controller.get("gate_reasons"), list)
        else [],
        "recommended_next_skills": [maybe_text(item) for item in controller.get("recommended_next_skills", []) if maybe_text(item)]
        if isinstance(controller.get("recommended_next_skills"), list)
        else [],
        "planned_stage_sequence": [maybe_text(item) for item in controller.get("planning", {}).get("stage_sequence", []) if maybe_text(item)]
        if isinstance(controller.get("planning"), dict) and isinstance(controller.get("planning", {}).get("stage_sequence"), list)
        else [],
        "planner_probe_stage_included": bool(plan.get("probe_stage_included")),
        "step_count": len(steps),
    }
    comparison = {
        "planning_mode": summary["planning_mode"],
        "controller_status": summary["controller_status"],
        "supervisor_status": summary["supervisor_status"],
        "supervisor_substatus": summary["supervisor_substatus"],
        "phase2_posture": summary["phase2_posture"],
        "terminal_state": summary["terminal_state"],
        "readiness_status": summary["readiness_status"],
        "gate_status": summary["gate_status"],
        "promotion_status": summary["promotion_status"],
        "reporting_ready": summary["reporting_ready"],
        "reporting_blockers": summary["reporting_blockers"],
        "reporting_handoff_status": summary["reporting_handoff_status"],
        "reporting_surface_source": summary["reporting_surface_source"],
        "publication_status": summary["publication_status"],
        "publication_posture": summary["publication_posture"],
        "failed_stage": summary["failed_stage"],
        "completed_stage_names": summary["completed_stage_names"],
        "pending_stage_names": summary["pending_stage_names"],
        "gate_reasons": summary["gate_reasons"],
        "recommended_next_skills": summary["recommended_next_skills"],
        "planned_stage_sequence": summary["planned_stage_sequence"],
        "planner_probe_stage_included": summary["planner_probe_stage_included"],
        "steps": comparison_step_rows(steps),
    }
    return {"summary": summary, "comparison": comparison, "steps": steps}


def post_round_state_snapshot(run_dir: Path, round_id: str, artifact_hashes: dict[str, str]) -> dict[str, Any]:
    round_close = load_json_if_exists(round_close_state_path(run_dir, round_id)) or {}
    history_bootstrap = load_json_if_exists(history_bootstrap_state_path(run_dir, round_id)) or {}
    round_close_steps = summarized_step_rows(run_dir, round_id, round_close.get("steps"), artifact_hashes=artifact_hashes)
    history_steps = summarized_step_rows(run_dir, round_id, history_bootstrap.get("steps"), artifact_hashes=artifact_hashes)
    steps = round_close_steps + history_steps
    round_close_next_skills = (
        [maybe_text(item) for item in round_close.get("recommended_next_skills", []) if maybe_text(item)]
        if isinstance(round_close.get("recommended_next_skills"), list)
        else []
    )
    history_next_skills = (
        [maybe_text(item) for item in history_bootstrap.get("recommended_next_skills", []) if maybe_text(item)]
        if isinstance(history_bootstrap.get("recommended_next_skills"), list)
        else []
    )
    summary = {
        "close_status": maybe_text(round_close.get("close_status")) or "missing",
        "archive_status": maybe_text(round_close.get("archive_status")) or "missing",
        "close_posture": maybe_text(round_close.get("close_posture")),
        "publication_status": maybe_text(round_close.get("publication_status")),
        "publication_posture": maybe_text(round_close.get("publication_posture")),
        "bootstrap_status": maybe_text(history_bootstrap.get("bootstrap_status")) or "missing",
        "selected_case_count": int(history_bootstrap.get("selected_case_count") or 0),
        "selected_signal_count": int(history_bootstrap.get("selected_signal_count") or 0),
        "failed_stage": maybe_text(round_close.get("failed_stage")) or maybe_text(history_bootstrap.get("failed_stage")),
        "recommended_next_skills": unique_texts(round_close_next_skills + history_next_skills),
        "warning_count": len(round_close.get("warnings", [])) if isinstance(round_close.get("warnings"), list) else 0,
        "step_count": len(steps),
    }
    comparison = {
        "close_status": summary["close_status"],
        "archive_status": summary["archive_status"],
        "close_posture": summary["close_posture"],
        "publication_status": summary["publication_status"],
        "publication_posture": summary["publication_posture"],
        "bootstrap_status": summary["bootstrap_status"],
        "selected_case_count": summary["selected_case_count"],
        "selected_signal_count": summary["selected_signal_count"],
        "failed_stage": summary["failed_stage"],
        "recommended_next_skills": summary["recommended_next_skills"],
        "warning_count": summary["warning_count"],
        "steps": comparison_step_rows(steps),
    }
    return {"summary": summary, "comparison": comparison, "steps": steps}


def core_ledger_events(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    events = load_ledger_tail(run_dir, 1_000_000)
    return [
        event
        for event in events
        if isinstance(event, dict)
        and maybe_text(event.get("round_id")) == round_id
        and maybe_text(event.get("event_type")) not in BENCHMARK_EVENT_TYPES
    ]


def skill_timing_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        skill_name = maybe_text(event.get("skill_name"))
        if not skill_name:
            continue
        bucket = buckets.setdefault(
            skill_name,
            {
                "skill_name": skill_name,
                "event_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "blocked_count": 0,
                "total_duration_seconds": 0.0,
                "max_duration_seconds": 0.0,
                "total_attempt_count": 0,
                "recovered_after_retry_count": 0,
            },
        )
        bucket["event_count"] += 1
        status = maybe_text(event.get("status"))
        if status == "completed":
            bucket["completed_count"] += 1
        elif status == "failed":
            bucket["failed_count"] += 1
        elif status == "blocked":
            bucket["blocked_count"] += 1
        duration = duration_seconds(event.get("started_at_utc"), event.get("completed_at_utc"))
        if duration is not None:
            bucket["total_duration_seconds"] += duration
            bucket["max_duration_seconds"] = max(bucket["max_duration_seconds"], duration)
        bucket["total_attempt_count"] += int(event.get("attempt_count") or 0)
        if bool(event.get("recovered_after_retry")):
            bucket["recovered_after_retry_count"] += 1
    rows: list[dict[str, Any]] = []
    for skill_name in sorted(buckets):
        bucket = buckets[skill_name]
        event_count = int(bucket["event_count"] or 0)
        rows.append(
            {
                "skill_name": skill_name,
                "event_count": event_count,
                "completed_count": int(bucket["completed_count"] or 0),
                "failed_count": int(bucket["failed_count"] or 0),
                "blocked_count": int(bucket["blocked_count"] or 0),
                "total_duration_seconds": rounded_number(bucket["total_duration_seconds"]),
                "average_duration_seconds": rounded_number(bucket["total_duration_seconds"] / event_count) if event_count else 0.0,
                "max_duration_seconds": rounded_number(bucket["max_duration_seconds"]),
                "total_attempt_count": int(bucket["total_attempt_count"] or 0),
                "recovered_after_retry_count": int(bucket["recovered_after_retry_count"] or 0),
            }
        )
    return rows


def round_event_summary(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for event in events:
        event_type = maybe_text(event.get("event_type"))
        if not event_type:
            continue
        bucket = buckets.setdefault(
            event_type,
            {
                "event_type": event_type,
                "event_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "blocked_count": 0,
                "degraded_count": 0,
                "total_duration_seconds": 0.0,
                "max_duration_seconds": 0.0,
            },
        )
        bucket["event_count"] += 1
        status = maybe_text(event.get("status"))
        if status == "completed":
            bucket["completed_count"] += 1
        elif status == "failed":
            bucket["failed_count"] += 1
        elif status == "blocked":
            bucket["blocked_count"] += 1
        elif status == "completed-with-warnings":
            bucket["degraded_count"] += 1
        duration = duration_seconds(event.get("started_at_utc"), event.get("completed_at_utc"))
        if duration is not None:
            bucket["total_duration_seconds"] += duration
            bucket["max_duration_seconds"] = max(bucket["max_duration_seconds"], duration)
    rows: list[dict[str, Any]] = []
    for event_type in sorted(buckets):
        bucket = buckets[event_type]
        event_count = int(bucket["event_count"] or 0)
        rows.append(
            {
                "event_type": event_type,
                "event_count": event_count,
                "completed_count": int(bucket["completed_count"] or 0),
                "failed_count": int(bucket["failed_count"] or 0),
                "blocked_count": int(bucket["blocked_count"] or 0),
                "degraded_count": int(bucket["degraded_count"] or 0),
                "total_duration_seconds": rounded_number(bucket["total_duration_seconds"]),
                "average_duration_seconds": rounded_number(bucket["total_duration_seconds"] / event_count) if event_count else 0.0,
                "max_duration_seconds": rounded_number(bucket["max_duration_seconds"]),
            }
        )
    return rows


def failure_summary(
    events: list[dict[str, Any]],
    *,
    phase2: dict[str, Any],
    post_round: dict[str, Any],
) -> dict[str, Any]:
    event_failures: list[dict[str, Any]] = []
    failed_event_types: list[str] = []
    failed_skills: list[str] = []
    blocked_event_count = 0
    failed_event_count = 0
    degraded_event_count = 0
    for event in events:
        status = maybe_text(event.get("status"))
        if status == "failed":
            failed_event_count += 1
            failed_event_types.append(maybe_text(event.get("event_type")))
            failed_skills.append(maybe_text(event.get("skill_name")))
        elif status == "blocked":
            blocked_event_count += 1
            failed_event_types.append(maybe_text(event.get("event_type")))
            failed_skills.append(maybe_text(event.get("skill_name")))
        elif status == "completed-with-warnings":
            degraded_event_count += 1
        if status not in {"failed", "blocked", "completed-with-warnings"}:
            continue
        failure = event.get("failure", {}) if isinstance(event.get("failure"), dict) else {}
        event_failures.append(
            {
                "event_type": maybe_text(event.get("event_type")),
                "skill_name": maybe_text(event.get("skill_name")),
                "status": status,
                "failed_stage": maybe_text(event.get("failed_stage")),
                "error_code": maybe_text(failure.get("error_code")),
                "message": maybe_text(failure.get("message")),
            }
        )
    failed_stage_names = unique_texts(
        [phase2.get("summary", {}).get("failed_stage"), post_round.get("summary", {}).get("failed_stage")]
        + [event.get("failed_stage") for event in event_failures]
    )
    return {
        "failed_event_count": failed_event_count,
        "blocked_event_count": blocked_event_count,
        "degraded_event_count": degraded_event_count,
        "failing_event_types": unique_texts(failed_event_types),
        "failing_skills": unique_texts(failed_skills),
        "failed_stage_names": failed_stage_names,
        "event_failures": event_failures,
    }


def benchmark_manifest_payload(run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    input_artifacts = artifact_rows(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        specs=INPUT_ARTIFACT_SPECS,
        category="input",
    )
    output_artifacts = artifact_rows(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        specs=OUTPUT_ARTIFACT_SPECS,
        category="output",
    )
    output_hashes = artifact_hash_lookup(output_artifacts)
    phase2 = phase2_state_snapshot(run_dir, run_id, round_id, output_hashes)
    post_round = post_round_state_snapshot(run_dir, round_id, output_hashes)
    events = core_ledger_events(run_dir, round_id)
    failure = failure_summary(events, phase2=phase2, post_round=post_round)
    comparison_inputs = comparison_artifact_rows(input_artifacts)
    comparison_outputs = comparison_artifact_rows(output_artifacts)
    comparison_basis = {
        "scenario_fingerprint": json_hash(comparison_inputs),
        "phase2": phase2["comparison"],
        "post_round": post_round["comparison"],
        "artifact_outputs": comparison_outputs,
    }
    output_fingerprint = json_hash(comparison_basis)
    summary = {
        "scenario_input_count": len(input_artifacts),
        "output_artifact_count": len(output_artifacts),
        "present_output_artifact_count": len([row for row in output_artifacts if bool(row.get("payload_present"))]),
        "artifact_file_output_count": len([row for row in output_artifacts if bool(row.get("artifact_present"))]),
        "failed_event_count": failure["failed_event_count"],
        "blocked_event_count": failure["blocked_event_count"],
        "controller_status": phase2["summary"]["controller_status"],
        "supervisor_status": phase2["summary"]["supervisor_status"],
        "reporting_ready": phase2["summary"]["reporting_ready"],
        "close_status": post_round["summary"]["close_status"],
        "bootstrap_status": post_round["summary"]["bootstrap_status"],
    }
    return {
        "schema_version": "runtime-benchmark-manifest-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "scenario_fingerprint": comparison_basis["scenario_fingerprint"],
        "output_fingerprint": output_fingerprint,
        "summary": summary,
        "scenario_inputs": input_artifacts,
        "artifact_outputs": output_artifacts,
        "phase2_summary": phase2["summary"],
        "post_round_summary": post_round["summary"],
        "round_step_summary": {
            "phase2": phase2["steps"],
            "post_round": post_round["steps"],
        },
        "skill_timing_summary": skill_timing_summary(events),
        "round_event_summary": round_event_summary(events),
        "failure_summary": failure,
        "comparison_basis": comparison_basis,
    }


def benchmark_manifest_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "benchmark-manifest", started_at, completed_at),
        "event_type": "benchmark-manifest",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "scenario_fingerprint": payload.get("scenario_fingerprint"),
        "output_fingerprint": payload.get("output_fingerprint"),
        "benchmark_manifest_path": str(output_path),
    }


def materialize_benchmark_manifest(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    payload = benchmark_manifest_payload(run_dir, run_id, round_id)
    output_path = benchmark_manifest_path(run_dir, round_id)
    write_json(output_path, payload)
    append_ledger_event(
        run_dir,
        benchmark_manifest_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=output_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "benchmark_manifest_path": str(output_path),
            "scenario_fingerprint": payload["scenario_fingerprint"],
            "output_fingerprint": payload["output_fingerprint"],
            "failed_event_count": payload["summary"]["failed_event_count"],
            "blocked_event_count": payload["summary"]["blocked_event_count"],
        },
        "benchmark_manifest": payload,
    }


def scenario_fixture_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "scenario-fixture", started_at, completed_at),
        "event_type": "scenario-fixture",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "scenario_id": payload.get("scenario_id"),
        "scenario_fingerprint": payload.get("scenario_fingerprint"),
        "fixture_path": str(output_path),
    }


def materialize_scenario_fixture(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    scenario_id: str = "",
    baseline_manifest_override: str = "",
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    if maybe_text(baseline_manifest_override):
        baseline_manifest_path_value = Path(baseline_manifest_override).expanduser().resolve()
        if not baseline_manifest_path_value.exists():
            raise ValueError(f"Missing benchmark manifest for scenario fixture: {baseline_manifest_path_value}")
        baseline_payload = load_json_if_exists(baseline_manifest_path_value) or {}
    else:
        baseline_manifest_path_value = benchmark_manifest_path(run_dir, round_id)
        if not baseline_manifest_path_value.exists():
            baseline_payload = materialize_benchmark_manifest(run_dir, run_id=run_id, round_id=round_id)["benchmark_manifest"]
        else:
            baseline_payload = load_json_if_exists(baseline_manifest_path_value) or {}
    if not baseline_payload:
        raise ValueError(f"Missing benchmark manifest for scenario fixture: {baseline_manifest_path_value}")
    fixture_path = scenario_fixture_path(run_dir, round_id)
    frozen_baseline_path = scenario_baseline_manifest_path(run_dir, round_id)
    write_json(frozen_baseline_path, baseline_payload)
    resolved_scenario_id = maybe_text(scenario_id) or f"scenario-{stable_hash(run_id, round_id, baseline_payload.get('scenario_fingerprint'))[:12]}"
    payload = {
        "schema_version": "runtime-scenario-fixture-v1",
        "generated_at_utc": utc_now_iso(),
        "scenario_id": resolved_scenario_id,
        "run_id": run_id,
        "round_id": round_id,
        "scenario_fingerprint": baseline_payload.get("scenario_fingerprint", ""),
        "scenario_identity": {
            "run_id": run_id,
            "round_id": round_id,
            "identity_policy": "benchmark-replay-must-preserve-run-and-round-ids",
        },
        "scenario_inputs": baseline_payload.get("scenario_inputs", []),
        "expected_terminal_posture": {
            "phase2": baseline_payload.get("phase2_summary", {}),
            "post_round": baseline_payload.get("post_round_summary", {}),
        },
        "expected_artifacts": baseline_payload.get("comparison_basis", {}).get("artifact_outputs", []),
        "baseline_manifest": {
            "path": str(frozen_baseline_path),
            "source_path": str(baseline_manifest_path_value),
            "output_fingerprint": maybe_text(baseline_payload.get("output_fingerprint")),
            "scenario_fingerprint": maybe_text(baseline_payload.get("scenario_fingerprint")),
        },
        "replay_contract": {
            "benchmark_command_template": f"python3 eco-concil-runtime/scripts/eco_runtime_kernel.py materialize-benchmark-manifest --run-dir <candidate-run-dir> --run-id {run_id} --round-id {round_id}",
            "compare_command_template": f"python3 eco-concil-runtime/scripts/eco_runtime_kernel.py compare-benchmark-manifests --run-dir <candidate-run-dir> --run-id {run_id} --round-id {round_id} --left-manifest-path {frozen_baseline_path} --right-manifest-path <candidate-run-dir>/runtime/benchmark_manifest_{round_id}.json",
            "replay_command_template": f"python3 eco-concil-runtime/scripts/eco_runtime_kernel.py replay-runtime-scenario --run-dir <candidate-run-dir> --run-id {run_id} --round-id {round_id} --fixture-path {fixture_path.resolve()}",
            "replay_steps": [
                "Re-run the fixed scenario with the same run_id and round_id.",
                "Materialize the candidate benchmark manifest if needed.",
                "Run replay-runtime-scenario against this fixture to compare outputs.",
            ],
        },
    }
    write_json(fixture_path, payload)
    append_ledger_event(
        run_dir,
        scenario_fixture_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=fixture_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "scenario_id": resolved_scenario_id,
            "scenario_fixture_path": str(fixture_path),
            "scenario_fingerprint": payload["scenario_fingerprint"],
        },
        "scenario_fixture": payload,
    }


def diff_values(left: Any, right: Any, *, path: str = "$", limit: int = 200) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    if type(left) is not type(right):
        return [{"path": path, "left": left, "right": right}]
    if isinstance(left, dict):
        changes: list[dict[str, Any]] = []
        for key in sorted(set(left) | set(right)):
            next_path = f"{path}.{key}"
            if key not in left:
                changes.append({"path": next_path, "left": None, "right": right[key]})
            elif key not in right:
                changes.append({"path": next_path, "left": left[key], "right": None})
            else:
                changes.extend(diff_values(left[key], right[key], path=next_path, limit=limit - len(changes)))
            if len(changes) >= limit:
                return changes[:limit]
        return changes
    if isinstance(left, list):
        if len(left) != len(right):
            return [{"path": path, "left": left, "right": right}]
        changes: list[dict[str, Any]] = []
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            changes.extend(diff_values(left_item, right_item, path=f"{path}[{index}]", limit=limit - len(changes)))
            if len(changes) >= limit:
                return changes[:limit]
        return changes
    if left != right:
        return [{"path": path, "left": left, "right": right}]
    return []


def compare_named_rows(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    *,
    key_field: str,
) -> list[dict[str, Any]]:
    left_map = {maybe_text(item.get(key_field)): item for item in left_rows if maybe_text(item.get(key_field))}
    right_map = {maybe_text(item.get(key_field)): item for item in right_rows if maybe_text(item.get(key_field))}
    drift: list[dict[str, Any]] = []
    for key in sorted(set(left_map) | set(right_map)):
        left_item = left_map.get(key)
        right_item = right_map.get(key)
        if left_item is None or right_item is None:
            drift.append({"key": key, "left": left_item, "right": right_item})
            continue
        if left_item != right_item:
            drift.append({"key": key, "left": left_item, "right": right_item})
    return drift


def compare_artifact_outputs(left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return compare_named_rows(left_rows, right_rows, key_field="artifact_key")


def compare_failure_summary(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_failed = int(left.get("failed_event_count") or 0)
    right_failed = int(right.get("failed_event_count") or 0)
    left_blocked = int(left.get("blocked_event_count") or 0)
    right_blocked = int(right.get("blocked_event_count") or 0)
    return {
        "left_failed_event_count": left_failed,
        "right_failed_event_count": right_failed,
        "failed_event_delta": right_failed - left_failed,
        "left_blocked_event_count": left_blocked,
        "right_blocked_event_count": right_blocked,
        "blocked_event_delta": right_blocked - left_blocked,
        "new_failing_skills": [
            skill_name
            for skill_name in right.get("failing_skills", [])
            if skill_name not in set(left.get("failing_skills", []))
        ]
        if isinstance(right.get("failing_skills"), list) and isinstance(left.get("failing_skills"), list)
        else [],
        "new_failed_stage_names": [
            stage_name
            for stage_name in right.get("failed_stage_names", [])
            if stage_name not in set(left.get("failed_stage_names", []))
        ]
        if isinstance(right.get("failed_stage_names"), list) and isinstance(left.get("failed_stage_names"), list)
        else [],
    }


def compare_skill_timing(left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    left_map = {maybe_text(item.get("skill_name")): item for item in left_rows if maybe_text(item.get("skill_name"))}
    right_map = {maybe_text(item.get("skill_name")): item for item in right_rows if maybe_text(item.get("skill_name"))}
    deltas: list[dict[str, Any]] = []
    for skill_name in sorted(set(left_map) | set(right_map)):
        left_item = left_map.get(skill_name, {})
        right_item = right_map.get(skill_name, {})
        left_duration = rounded_number(left_item.get("total_duration_seconds"))
        right_duration = rounded_number(right_item.get("total_duration_seconds"))
        left_attempts = int(left_item.get("total_attempt_count") or 0)
        right_attempts = int(right_item.get("total_attempt_count") or 0)
        if left_item == right_item:
            continue
        deltas.append(
            {
                "skill_name": skill_name,
                "left_total_duration_seconds": left_duration,
                "right_total_duration_seconds": right_duration,
                "duration_delta_seconds": rounded_number(right_duration - left_duration),
                "left_total_attempt_count": left_attempts,
                "right_total_attempt_count": right_attempts,
                "attempt_delta": right_attempts - left_attempts,
            }
        )
    return deltas


def benchmark_compare_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "benchmark-compare", started_at, completed_at, payload.get("verdict")),
        "event_type": "benchmark-compare",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "scenario_match": bool(payload.get("scenario_match")),
        "output_match": bool(payload.get("output_match")),
        "verdict": payload.get("verdict"),
        "benchmark_compare_path": str(output_path),
    }


def compare_benchmark_manifests(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    left_manifest_path: str,
    right_manifest_path: str,
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    left_path = Path(left_manifest_path).expanduser().resolve()
    right_path = Path(right_manifest_path).expanduser().resolve()
    left_payload = load_json_if_exists(left_path) or {}
    right_payload = load_json_if_exists(right_path) or {}
    if not left_payload:
        raise ValueError(f"Missing left benchmark manifest: {left_path}")
    if not right_payload:
        raise ValueError(f"Missing right benchmark manifest: {right_path}")
    left_basis = left_payload.get("comparison_basis", {}) if isinstance(left_payload.get("comparison_basis"), dict) else {}
    right_basis = right_payload.get("comparison_basis", {}) if isinstance(right_payload.get("comparison_basis"), dict) else {}
    changed_fields = diff_values(left_basis, right_basis)
    artifact_drift = compare_artifact_outputs(
        left_basis.get("artifact_outputs", []) if isinstance(left_basis.get("artifact_outputs"), list) else [],
        right_basis.get("artifact_outputs", []) if isinstance(right_basis.get("artifact_outputs"), list) else [],
    )
    phase2_step_drift = compare_named_rows(
        left_basis.get("phase2", {}).get("steps", []) if isinstance(left_basis.get("phase2"), dict) else [],
        right_basis.get("phase2", {}).get("steps", []) if isinstance(right_basis.get("phase2"), dict) else [],
        key_field="stage",
    )
    post_round_step_drift = compare_named_rows(
        left_basis.get("post_round", {}).get("steps", []) if isinstance(left_basis.get("post_round"), dict) else [],
        right_basis.get("post_round", {}).get("steps", []) if isinstance(right_basis.get("post_round"), dict) else [],
        key_field="stage",
    )
    failure_delta = compare_failure_summary(
        left_payload.get("failure_summary", {}) if isinstance(left_payload.get("failure_summary"), dict) else {},
        right_payload.get("failure_summary", {}) if isinstance(right_payload.get("failure_summary"), dict) else {},
    )
    timing_deltas = compare_skill_timing(
        left_payload.get("skill_timing_summary", []) if isinstance(left_payload.get("skill_timing_summary"), list) else [],
        right_payload.get("skill_timing_summary", []) if isinstance(right_payload.get("skill_timing_summary"), list) else [],
    )
    scenario_match = maybe_text(left_payload.get("scenario_fingerprint")) == maybe_text(right_payload.get("scenario_fingerprint"))
    output_match = maybe_text(left_payload.get("output_fingerprint")) == maybe_text(right_payload.get("output_fingerprint"))
    failure_regression = bool(failure_delta["failed_event_delta"] > 0 or failure_delta["blocked_event_delta"] > 0)
    verdict = "match"
    if not scenario_match:
        verdict = "scenario-mismatch"
    elif not output_match or failure_regression:
        verdict = "regression"
    elif timing_deltas:
        verdict = "match-with-timing-delta"
    payload = {
        "schema_version": "runtime-benchmark-compare-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "left_manifest": {
            "path": str(left_path),
            "run_id": maybe_text(left_payload.get("run_id")),
            "round_id": maybe_text(left_payload.get("round_id")),
            "scenario_fingerprint": maybe_text(left_payload.get("scenario_fingerprint")),
            "output_fingerprint": maybe_text(left_payload.get("output_fingerprint")),
        },
        "right_manifest": {
            "path": str(right_path),
            "run_id": maybe_text(right_payload.get("run_id")),
            "round_id": maybe_text(right_payload.get("round_id")),
            "scenario_fingerprint": maybe_text(right_payload.get("scenario_fingerprint")),
            "output_fingerprint": maybe_text(right_payload.get("output_fingerprint")),
        },
        "scenario_match": scenario_match,
        "output_match": output_match,
        "failure_regression": failure_regression,
        "verdict": verdict,
        "changed_field_count": len(changed_fields),
        "timing_delta_count": len(timing_deltas),
        "artifact_drift": artifact_drift,
        "phase2_step_drift": phase2_step_drift,
        "post_round_step_drift": post_round_step_drift,
        "failure_delta": failure_delta,
        "timing_deltas": timing_deltas,
        "changed_fields": changed_fields,
    }
    output_path = benchmark_compare_path(run_dir, round_id)
    write_json(output_path, payload)
    append_ledger_event(
        run_dir,
        benchmark_compare_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=output_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "benchmark_compare_path": str(output_path),
            "verdict": verdict,
            "scenario_match": scenario_match,
            "output_match": output_match,
            "changed_field_count": len(changed_fields),
        },
        "benchmark_compare": payload,
    }


def replay_report_event(
    *,
    run_id: str,
    round_id: str,
    started_at: str,
    completed_at: str,
    payload: dict[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    return {
        "schema_version": "runtime-event-v3",
        "event_id": new_runtime_event_id("runtimeevt", run_id, round_id, "scenario-replay", started_at, completed_at, payload.get("replay_verdict")),
        "event_type": "scenario-replay",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "status": "completed",
        "replay_verdict": payload.get("replay_verdict"),
        "scenario_id": payload.get("scenario_id"),
        "replay_report_path": str(output_path),
    }


def replay_verdict(compare_verdict: str) -> str:
    if compare_verdict == "scenario-mismatch":
        return "fixture-mismatch"
    if compare_verdict == "regression":
        return "regression-detected"
    if compare_verdict == "match-with-timing-delta":
        return "matched-with-timing-delta"
    return "matched"


def replay_runtime_scenario(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    fixture_path_override: str = "",
    baseline_manifest_override: str = "",
) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    started_at = utc_now_iso()
    fixture_path_value = Path(fixture_path_override).expanduser().resolve() if maybe_text(fixture_path_override) else scenario_fixture_path(run_dir, round_id)
    fixture_payload = load_json_if_exists(fixture_path_value) or {}
    if not fixture_payload:
        raise ValueError(f"Missing scenario fixture: {fixture_path_value}")
    baseline_manifest_path_value = (
        Path(baseline_manifest_override).expanduser().resolve()
        if maybe_text(baseline_manifest_override)
        else Path(maybe_text(fixture_payload.get("baseline_manifest", {}).get("path"))).expanduser().resolve()
    )
    if not baseline_manifest_path_value.exists():
        raise ValueError(f"Missing baseline benchmark manifest for replay: {baseline_manifest_path_value}")
    benchmark_result = materialize_benchmark_manifest(run_dir, run_id=run_id, round_id=round_id)
    current_manifest_path = benchmark_manifest_path(run_dir, round_id)
    compare_result = compare_benchmark_manifests(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        left_manifest_path=str(baseline_manifest_path_value),
        right_manifest_path=str(current_manifest_path),
    )
    compare_payload = compare_result["benchmark_compare"]
    payload = {
        "schema_version": "runtime-replay-report-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "scenario_id": maybe_text(fixture_payload.get("scenario_id")),
        "fixture_path": str(fixture_path_value),
        "baseline_manifest_path": str(baseline_manifest_path_value),
        "current_manifest_path": str(current_manifest_path),
        "benchmark_compare_path": str(benchmark_compare_path(run_dir, round_id)),
        "scenario_match": bool(compare_payload.get("scenario_match")),
        "output_match": bool(compare_payload.get("output_match")),
        "compare_verdict": maybe_text(compare_payload.get("verdict")),
        "replay_verdict": replay_verdict(maybe_text(compare_payload.get("verdict"))),
        "expected_output_fingerprint": maybe_text(fixture_payload.get("baseline_manifest", {}).get("output_fingerprint")),
        "current_output_fingerprint": maybe_text(benchmark_result.get("benchmark_manifest", {}).get("output_fingerprint")),
        "artifact_drift_count": len(compare_payload.get("artifact_drift", []))
        if isinstance(compare_payload.get("artifact_drift"), list)
        else 0,
        "changed_field_count": int(compare_payload.get("changed_field_count") or 0),
        "timing_delta_count": int(compare_payload.get("timing_delta_count") or 0),
        "failure_delta": compare_payload.get("failure_delta", {}) if isinstance(compare_payload.get("failure_delta"), dict) else {},
        "replay_contract": fixture_payload.get("replay_contract", {}) if isinstance(fixture_payload.get("replay_contract"), dict) else {},
    }
    output_path = replay_report_path(run_dir, round_id)
    write_json(output_path, payload)
    append_ledger_event(
        run_dir,
        replay_report_event(
            run_id=run_id,
            round_id=round_id,
            started_at=started_at,
            completed_at=utc_now_iso(),
            payload=payload,
            output_path=output_path,
        ),
    )
    return {
        "status": "completed",
        "summary": {
            "run_id": run_id,
            "round_id": round_id,
            "replay_report_path": str(output_path),
            "compare_verdict": payload["compare_verdict"],
            "replay_verdict": payload["replay_verdict"],
        },
        "replay_report": payload,
    }

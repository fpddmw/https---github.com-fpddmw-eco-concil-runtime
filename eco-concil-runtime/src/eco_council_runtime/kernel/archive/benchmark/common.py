from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.execution.executor import maybe_text
from eco_council_runtime.kernel.operator.surfaces import (
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
    load_orchestration_plan_wrapper,
)
from eco_council_runtime.kernel.source_queue.source_queue_history import load_round_tasks_wrapper

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
    ("report_basis_freeze", "report_basis/frozen_report_basis_{round_id}.json"),
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
    "report_basis",
    "reporting",
    "runtime",
    "receipts",
)

RECOVERABLE_INPUT_LOADERS = {
    "round_tasks": load_round_tasks_wrapper,
}

RECOVERABLE_OUTPUT_LOADERS = {
    "orchestration_plan": load_orchestration_plan_wrapper,
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
                "report_basis_freeze_allowed": bool(step.get("report_basis_freeze_allowed")),
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
                "report_basis_freeze_allowed": bool(row.get("report_basis_freeze_allowed")),
            }
        )
    return comparison_rows

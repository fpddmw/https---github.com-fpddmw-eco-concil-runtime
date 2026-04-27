#!/usr/bin/env python3
"""Scaffold a run from one mission contract and seed the first-round board state."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "scaffold-mission-run"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_round_task_snapshot,
)
from eco_council_runtime.kernel.source_queue_contract import source_role  # noqa: E402


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_input_path(run_dir: Path, raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Mission file must contain a JSON object: {path}")
    return payload


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def role_for_source_skill(source_skill: str) -> str:
    return source_role(source_skill)


def normalize_mission_payload(mission: dict[str, Any], mission_file: Path, run_id: str) -> dict[str, Any]:
    payload = json.loads(json.dumps(mission, ensure_ascii=True))
    if maybe_text(payload.get("run_id")) != run_id:
        raise ValueError(f"Mission run_id {maybe_text(payload.get('run_id'))!r} does not match --run-id {run_id!r}.")

    topic = maybe_text(payload.get("topic"))
    objective = maybe_text(payload.get("objective"))
    if not topic or not objective:
        raise ValueError("Mission must include non-empty topic and objective.")

    window = payload.get("window") if isinstance(payload.get("window"), dict) else {}
    if not maybe_text(window.get("start_utc")) or not maybe_text(window.get("end_utc")):
        raise ValueError("Mission window must include start_utc and end_utc.")

    region = payload.get("region") if isinstance(payload.get("region"), dict) else {}
    if not maybe_text(region.get("label")):
        raise ValueError("Mission region must include a non-empty label.")
    if not isinstance(region.get("geometry"), dict):
        raise ValueError("Mission region must include a geometry object.")

    imports = payload.get("artifact_imports")
    if imports is None:
        payload["artifact_imports"] = []
    elif not isinstance(imports, list):
        raise ValueError("Mission artifact_imports must be a JSON list when present.")

    normalized_imports: list[dict[str, Any]] = []
    for index, item in enumerate(payload["artifact_imports"], start=1):
        if not isinstance(item, dict):
            raise ValueError(f"artifact_imports[{index - 1}] must be a JSON object.")
        source_skill = maybe_text(item.get("source_skill"))
        artifact_path_text = maybe_text(item.get("artifact_path"))
        if not source_skill or not artifact_path_text:
            raise ValueError(f"artifact_imports[{index - 1}] must include source_skill and artifact_path.")
        role_for_source_skill(source_skill)
        source_path = Path(artifact_path_text).expanduser()
        if not source_path.is_absolute():
            source_path = (mission_file.parent / source_path).resolve()
        normalized_imports.append(
            {
                **item,
                "source_skill": source_skill,
                "artifact_path": str(source_path),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    payload["artifact_imports"] = normalized_imports

    requests = payload.get("source_requests")
    if requests is None:
        payload["source_requests"] = []
        return payload
    if not isinstance(requests, list):
        raise ValueError("Mission source_requests must be a JSON list when present.")

    normalized_requests: list[dict[str, Any]] = []
    for index, item in enumerate(requests, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"source_requests[{index - 1}] must be a JSON object.")
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            raise ValueError(f"source_requests[{index - 1}] must include source_skill.")
        role_for_source_skill(source_skill)
        fetch_argv = item.get("fetch_argv") if isinstance(item.get("fetch_argv"), list) else []
        normalized_requests.append(
            {
                **item,
                "source_skill": source_skill,
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "fetch_cwd": maybe_text(item.get("fetch_cwd")),
                "fetch_argv": [maybe_text(arg) for arg in fetch_argv if maybe_text(arg)],
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    payload["source_requests"] = normalized_requests
    return payload


def title_from_statement(statement: str, index: int) -> str:
    text = maybe_text(statement)
    if not text:
        return f"Mission hypothesis {index}"
    if len(text) <= 80:
        return text
    return text[:77].rstrip() + "..."


def artifact_imports(mission: dict[str, Any]) -> list[dict[str, Any]]:
    imports = mission.get("artifact_imports") if isinstance(mission.get("artifact_imports"), list) else []
    return [item for item in imports if isinstance(item, dict)]


def source_requests(mission: dict[str, Any]) -> list[dict[str, Any]]:
    requests = mission.get("source_requests") if isinstance(mission.get("source_requests"), list) else []
    return [item for item in requests if isinstance(item, dict)]


def build_round_tasks(*, mission: dict[str, Any], run_id: str, round_id: str) -> list[dict[str, Any]]:
    imports = artifact_imports(mission)
    requests = source_requests(mission)
    by_role = {"sociologist": [], "environmentalist": []}
    for item in [*imports, *requests]:
        source_skill = maybe_text(item.get("source_skill"))
        by_role[role_for_source_skill(source_skill)].append(source_skill)

    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    geometry = region.get("geometry") if isinstance(region.get("geometry"), dict) else {}
    tasks: list[dict[str, Any]] = []

    if by_role["sociologist"]:
        tasks.append(
            {
                "task_id": f"task-sociologist-{round_id}-01",
                "run_id": run_id,
                "round_id": round_id,
                "assigned_role": "sociologist",
                "status": "planned",
                "objective": "Import and normalize public-discussion artifacts that can surface claim narratives for the current mission.",
                "expected_output_kinds": ["normalized-public-signals", "claim-candidates"],
                "inputs": {
                    "mission_window": window,
                    "mission_geometry": geometry,
                    "source_skills": sorted(set(by_role["sociologist"])),
                    "evidence_requirements": [
                        {
                            "requirement_id": f"req-sociologist-{round_id}-public-import",
                            "requirement_type": "public-signal-import",
                            "summary": "Normalize public-discussion artifacts so downstream claim extraction can work from the canonical signal plane.",
                            "priority": "high",
                        }
                    ],
                },
            }
        )

    if by_role["environmentalist"]:
        tasks.append(
            {
                "task_id": f"task-environmentalist-{round_id}-01",
                "run_id": run_id,
                "round_id": round_id,
                "assigned_role": "environmentalist",
                "status": "planned",
                "objective": "Import and normalize physical-observation artifacts that can corroborate or contradict current mission hypotheses.",
                "expected_output_kinds": ["normalized-environment-signals", "observation-candidates"],
                "inputs": {
                    "mission_window": window,
                    "mission_geometry": geometry,
                    "source_skills": sorted(set(by_role["environmentalist"])),
                    "evidence_requirements": [
                        {
                            "requirement_id": f"req-environmentalist-{round_id}-environment-import",
                            "requirement_type": "environment-signal-import",
                            "summary": "Normalize physical-observation artifacts so downstream observation extraction and evidence coverage can operate from the signal plane.",
                            "priority": "high",
                        }
                    ],
                },
            }
        )

    return tasks


def build_board(*, mission: dict[str, Any], run_id: str, round_id: str, hypothesis_confidence: float) -> tuple[dict[str, Any], list[str]]:
    timestamp = utc_now_iso()
    raw_hypotheses = mission.get("hypotheses") if isinstance(mission.get("hypotheses"), list) else []
    hypotheses: list[dict[str, Any]] = []
    seeded_ids: list[str] = []
    for index, item in enumerate(raw_hypotheses, start=1):
        if isinstance(item, dict):
            statement = maybe_text(item.get("statement") or item.get("hypothesis") or item.get("title"))
            title = maybe_text(item.get("title")) or title_from_statement(statement, index)
            owner_role = maybe_text(item.get("owner_role")) or "moderator"
            confidence = maybe_number(item.get("confidence"))
            status = maybe_text(item.get("status")) or "active"
            linked_claim_ids = [maybe_text(value) for value in item.get("linked_claim_ids", []) if maybe_text(value)] if isinstance(item.get("linked_claim_ids"), list) else []
        else:
            statement = maybe_text(item)
            title = title_from_statement(statement, index)
            owner_role = "moderator"
            confidence = None
            status = "active"
            linked_claim_ids = []
        if not statement:
            continue
        resolved_confidence = confidence if confidence is not None else hypothesis_confidence
        hypothesis_id = "hypothesis-" + stable_hash(run_id, round_id, index, title, statement)[:12]
        seeded_ids.append(hypothesis_id)
        hypotheses.append(
            {
                "hypothesis_id": hypothesis_id,
                "run_id": run_id,
                "round_id": round_id,
                "title": title,
                "statement": statement,
                "status": status,
                "owner_role": owner_role,
                "linked_claim_ids": linked_claim_ids,
                "confidence": resolved_confidence,
                "created_at_utc": timestamp,
                "updated_at_utc": timestamp,
                "history": [{"status": status, "updated_at_utc": timestamp, "confidence": resolved_confidence}],
            }
        )

    board = {
        "schema_version": "board-v1",
        "run_id": run_id,
        "board_revision": 1,
        "updated_at_utc": timestamp,
        "events": [
            {
                "event_id": "boardevt-" + stable_hash(run_id, round_id, "mission-scaffold", timestamp)[:12],
                "run_id": run_id,
                "round_id": round_id,
                "event_type": "mission-scaffolded",
                "created_at_utc": timestamp,
                "payload": {
                    "topic": maybe_text(mission.get("topic")),
                    "artifact_import_count": len(artifact_imports(mission)),
                    "source_request_count": len(source_requests(mission)),
                    "seeded_hypothesis_count": len(hypotheses),
                },
            }
        ],
        "rounds": {
            round_id: {
                "notes": [],
                "challenge_tickets": [],
                "hypotheses": hypotheses,
                "tasks": [],
            }
        },
    }
    return board, seeded_ids


def scaffold_mission_run_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    mission_path: str,
    hypothesis_confidence: float | None,
    orchestration_mode: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    mission_file = resolve_input_path(run_dir_path, mission_path)
    mission = normalize_mission_payload(read_json_object(mission_file), mission_file, run_id)

    mission_output_path = (run_dir_path / "mission.json").resolve()
    task_output_path = (run_dir_path / "investigation" / f"round_tasks_{round_id}.json").resolve()
    board_output_path = (run_dir_path / "board" / "investigation_board.json").resolve()
    summary_output_path = (run_dir_path / "runtime" / f"mission_scaffold_{round_id}.json").resolve()

    task_payload = build_round_tasks(mission=mission, run_id=run_id, round_id=round_id)
    board_payload, seeded_hypothesis_ids = build_board(
        mission=mission,
        run_id=run_id,
        round_id=round_id,
        hypothesis_confidence=float(hypothesis_confidence if hypothesis_confidence is not None else 0.6),
    )

    write_json_file(mission_output_path, mission)
    write_json_file(task_output_path, task_payload)
    write_json_file(board_output_path, board_payload)
    store_round_task_snapshot(
        run_dir_path,
        task_snapshot={
            "schema_version": "round-task-snapshot-v1",
            "generated_at_utc": utc_now_iso(),
            "run_id": run_id,
            "round_id": round_id,
            "task_source": "round-tasks-artifact",
            "task_count": len(task_payload),
            "tasks": task_payload,
        },
        artifact_path=str(task_output_path),
    )

    scaffold_id = "mission-scaffold-" + stable_hash(run_id, round_id, mission_output_path, task_output_path)[:12]
    imports = artifact_imports(mission)
    requests = source_requests(mission)
    role_source_counts = {
        "sociologist": len([item for item in [*imports, *requests] if role_for_source_skill(maybe_text(item.get("source_skill"))) == "sociologist"]),
        "environmentalist": len([item for item in [*imports, *requests] if role_for_source_skill(maybe_text(item.get("source_skill"))) == "environmentalist"]),
    }
    summary_payload = {
        "schema_version": "ingress-scaffold-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "orchestration_mode": orchestration_mode,
        "scaffold_id": scaffold_id,
        "mission_path": str(mission_output_path),
        "task_path": str(task_output_path),
        "board_path": str(board_output_path),
        "import_source_count": len(imports),
        "request_source_count": len(requests),
        "task_count": len(task_payload),
        "seeded_hypothesis_ids": seeded_hypothesis_ids,
        "role_source_counts": role_source_counts,
    }
    write_json_file(summary_output_path, summary_payload)

    warnings: list[dict[str, str]] = []
    if not imports and not requests:
        warnings.append(
            {
                "code": "no-source-inputs",
                "message": "Mission scaffold completed without artifact_imports or source_requests; prepare-round will produce an empty plan.",
            }
        )
    if not seeded_hypothesis_ids:
        warnings.append({"code": "no-hypotheses", "message": "Mission scaffold completed without seeded hypotheses; readiness will remain blocked until hypotheses are added."})

    artifact_refs = [
        {"signal_id": "", "artifact_path": str(summary_output_path), "record_locator": "$", "artifact_ref": f"{summary_output_path}:$"},
        {"signal_id": "", "artifact_path": str(mission_output_path), "record_locator": "$", "artifact_ref": f"{mission_output_path}:$"},
        {"signal_id": "", "artifact_path": str(task_output_path), "record_locator": "$", "artifact_ref": f"{task_output_path}:$"},
        {"signal_id": "", "artifact_path": str(board_output_path), "record_locator": f"$.rounds.{round_id}", "artifact_ref": f"{board_output_path}:$.rounds.{round_id}"},
    ]
    suggested_next_skills = ["prepare-round", "summarize-board-state"]
    if orchestration_mode == "openclaw-agent":
        suggested_next_skills = [
            "query-board-delta",
            "materialize-history-context",
            "summarize-board-state",
            "prepare-round",
        ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "orchestration_mode": orchestration_mode,
            "output_path": str(summary_output_path),
            "scaffold_id": scaffold_id,
            "import_source_count": len(imports),
            "request_source_count": len(requests),
            "task_count": len(task_payload),
            "seeded_hypothesis_count": len(seeded_hypothesis_ids),
            "seeded_hypothesis_ids": seeded_hypothesis_ids,
        },
        "receipt_id": "ingress-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, scaffold_id)[:20],
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, summary_output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [scaffold_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [scaffold_id, *seeded_hypothesis_ids],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings if item.get("code") in {"no-source-inputs", "no-hypotheses"}],
            "challenge_hints": [],
            "suggested_next_skills": suggested_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scaffold a run from one mission contract and seed the first-round board state.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--mission-path", required=True)
    parser.add_argument("--hypothesis-confidence", type=float)
    parser.add_argument("--orchestration-mode", choices=["runtime-source-queue", "openclaw-agent"], default="runtime-source-queue")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = scaffold_mission_run_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        mission_path=args.mission_path,
        hypothesis_confidence=args.hypothesis_confidence,
        orchestration_mode=args.orchestration_mode,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

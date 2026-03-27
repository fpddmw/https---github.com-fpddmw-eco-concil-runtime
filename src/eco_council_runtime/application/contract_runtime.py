"""Runtime-facing contract services for scaffolding, bundle checks, and CLI handlers."""

from __future__ import annotations

import argparse
import copy
import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime import contract as contract_module
from eco_council_runtime.adapters.filesystem import atomic_write_text_file, read_json, utc_now_iso, write_json
from eco_council_runtime.investigation import build_investigation_plan


def validate_payload_or_raise(kind: str, payload: Any, *, label: str) -> dict[str, Any]:
    result = contract_module.validate_payload(kind, payload)
    if not result["validation"]["ok"]:
        raise ValueError(f"{label} failed validation: {result['validation']['issues']}")
    return result


def load_ddl() -> str:
    return contract_module.DDL_PATH.read_text(encoding="utf-8")


def parse_point(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 2:
        raise ValueError("--point must be in latitude,longitude format.")
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError as exc:
        raise ValueError("--point latitude and longitude must be numeric.") from exc
    if latitude < -90 or latitude > 90:
        raise ValueError("--point latitude must be between -90 and 90.")
    if longitude < -180 or longitude > 180:
        raise ValueError("--point longitude must be between -180 and 180.")
    return {"type": "Point", "latitude": latitude, "longitude": longitude}


def parse_bbox(raw: str) -> dict[str, Any]:
    parts = [part.strip() for part in raw.split(",")]
    if len(parts) != 4:
        raise ValueError("--bbox must be in west,south,east,north format.")
    try:
        west = float(parts[0])
        south = float(parts[1])
        east = float(parts[2])
        north = float(parts[3])
    except ValueError as exc:
        raise ValueError("--bbox coordinates must be numeric.") from exc
    if west < -180 or west > 180 or east < -180 or east > 180:
        raise ValueError("--bbox west/east must be between -180 and 180.")
    if south < -90 or south > 90 or north < -90 or north > 90:
        raise ValueError("--bbox south/north must be between -90 and 90.")
    if east <= west:
        raise ValueError("--bbox east must be greater than west.")
    if north <= south:
        raise ValueError("--bbox north must be greater than south.")
    return {"type": "BBox", "west": west, "south": south, "east": east, "north": north}


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    governance_families = contract_module.source_governance_for_role(mission, role)
    values = [
        contract_module.maybe_text(skill)
        for family in governance_families
        for skill in family.get("skills", [])
        if contract_module.maybe_text(skill)
    ]
    return contract_module.unique_strings(values)


def expected_output_kinds_for_role(role: str) -> list[str]:
    if role == "sociologist":
        return ["source-selection", "claim-curation", "claim-submission", "data-readiness-report", "expert-report"]
    if role == "environmentalist":
        return ["source-selection", "observation-curation", "observation-submission", "data-readiness-report", "expert-report"]
    if role == "historian":
        return ["expert-report"]
    return ["expert-report"]


def investigation_leg_ids(mission: dict[str, Any], round_id: str) -> set[str]:
    plan = build_investigation_plan(mission=mission, round_id=round_id)
    leg_ids: set[str] = set()
    for hypothesis in plan.get("hypotheses", []):
        if not isinstance(hypothesis, dict):
            continue
        for leg in hypothesis.get("chain_legs", []):
            if not isinstance(leg, dict):
                continue
            leg_id = contract_module.maybe_text(leg.get("leg_id"))
            if leg_id:
                leg_ids.add(leg_id)
    return leg_ids


def default_public_claim_summary(leg_ids: set[str]) -> str:
    if "public_interpretation" in leg_ids and leg_ids & {"source", "mechanism", "impact"}:
        return (
            "Collect attributable public claims that describe local impacts and any attribution or causal narratives "
            "about likely sources or mechanisms in the mission window."
        )
    if "public_interpretation" in leg_ids:
        return "Collect attributable public claims that explain who is saying what about mission-region impacts in the mission window."
    return "Collect attributable public claims that explain who is saying what in the mission window."


def default_environment_summary(leg_ids: set[str]) -> str:
    segments: list[str] = []
    if "source" in leg_ids:
        segments.append("source-region activity")
    if "mechanism" in leg_ids:
        segments.append("transport or propagation context")
    if "impact" in leg_ids:
        segments.append("mission-region physical impacts")
    if not segments:
        return "Collect mission-window physical observations that can support or contradict candidate claims."
    return (
        "Collect mission-window physical observations covering "
        + ", ".join(segments)
        + " so later matching can test the investigation chain."
    )


def default_round_tasks(*, mission: dict[str, Any], round_id: str) -> list[dict[str, Any]]:
    run_id = mission["run_id"]
    geometry = mission.get("region", {}).get("geometry") if isinstance(mission.get("region"), dict) else None
    mission_window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    leg_ids = investigation_leg_ids(mission, round_id)

    sociologist_task = copy.deepcopy(contract_module.EXAMPLES["round-task"])
    sociologist_task["task_id"] = f"task-sociologist-{round_id}-01"
    sociologist_task["run_id"] = run_id
    sociologist_task["round_id"] = round_id
    sociologist_task["assigned_role"] = "sociologist"
    sociologist_task["objective"] = (
        "Identify mission-window public claims, attribution narratives, and judge whether the current public-evidence preparation is sufficient for later matching."
    )
    sociologist_task["expected_output_kinds"] = expected_output_kinds_for_role("sociologist")
    sociologist_task["inputs"] = {
        "mission_geometry": geometry,
        "mission_window": mission_window,
        "evidence_requirements": [
            {
                "requirement_id": f"req-sociologist-{round_id}-public-claims",
                "requirement_type": "public-claim-discovery",
                "summary": default_public_claim_summary(leg_ids),
                "priority": "high",
                "focus_claim_ids": [],
                "anchor_refs": [],
            }
        ],
    }

    environmental_task = copy.deepcopy(contract_module.EXAMPLES["round-task"])
    environmental_task["task_id"] = f"task-environmentalist-{round_id}-01"
    environmental_task["run_id"] = run_id
    environmental_task["round_id"] = round_id
    environmental_task["assigned_role"] = "environmentalist"
    environmental_task["objective"] = (
        "Collect mission-relevant physical observations across the investigation chain and judge whether the current physical-evidence preparation is sufficient for later matching."
    )
    environmental_task["expected_output_kinds"] = expected_output_kinds_for_role("environmentalist")
    environmental_task["inputs"] = {
        "mission_geometry": geometry,
        "mission_window": mission_window,
        "evidence_requirements": [
            {
                "requirement_id": f"req-environmentalist-{round_id}-physical-corroboration",
                "requirement_type": "physical-corroboration",
                "summary": default_environment_summary(leg_ids),
                "priority": "high",
                "focus_claim_ids": [],
                "anchor_refs": [],
            }
        ],
    }

    return [sociologist_task, environmental_task]


def placeholder_source_selection(
    *,
    run_id: str,
    round_id: str,
    role: str,
    task_ids: list[str],
    allowed_sources: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": contract_module.SCHEMA_VERSION,
        "selection_id": f"source-selection-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "pending",
        "summary": f"Pending {role} source selection.",
        "task_ids": task_ids,
        "allowed_sources": allowed_sources,
        "selected_sources": [],
        "source_decisions": [],
        "family_plans": [],
        "override_requests": [],
    }


def placeholder_claim_curation(*, run_id: str, round_id: str) -> dict[str, Any]:
    return {
        "schema_version": contract_module.SCHEMA_VERSION,
        "curation_id": f"claim-curation-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": "sociologist",
        "status": "pending",
        "summary": "Pending sociologist claim curation.",
        "curated_claims": [],
        "rejected_candidate_ids": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }


def placeholder_observation_curation(*, run_id: str, round_id: str) -> dict[str, Any]:
    return {
        "schema_version": contract_module.SCHEMA_VERSION,
        "curation_id": f"observation-curation-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": "environmentalist",
        "status": "pending",
        "summary": "Pending environmentalist observation curation.",
        "curated_observations": [],
        "rejected_candidate_ids": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }


def placeholder_report(*, run_id: str, round_id: str, role: str) -> dict[str, Any]:
    return {
        "schema_version": contract_module.SCHEMA_VERSION,
        "report_id": f"report-{role}-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "agent_role": role,
        "status": "needs-more-evidence",
        "summary": f"Pending {role} execution.",
        "findings": [],
        "open_questions": [],
        "recommended_next_actions": [],
        "override_requests": [],
    }


def placeholder_investigation_plan(*, run_id: str, round_id: str) -> dict[str, Any]:
    return {
        "schema_version": contract_module.SCHEMA_VERSION,
        "plan_id": f"investigation-plan-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "profile_id": "local-event",
        "profile_summary": "Pending investigation planning context.",
        "generated_at_utc": utc_now_iso(),
        "mission_region": None,
        "mission_window": None,
        "hypotheses": [],
        "fetch_intents": [],
        "history_query": {
            "query": "",
            "region_label": "",
            "profile_id": "local-event",
            "claim_types": [],
            "metric_families": [],
            "gap_types": [],
            "source_skills": [],
            "priority_leg_ids": [],
            "alternative_hypotheses": [],
        },
        "open_questions": [],
        "notes": [
            "This placeholder plan was scaffolded without a mission payload.",
            "Populate or regenerate the round with a mission to enable causal-chain planning.",
        ],
    }


def normalize_round_tasks(
    *,
    tasks: list[dict[str, Any]],
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            raise ValueError(f"Task index {index} is not an object.")
        candidate = copy.deepcopy(task)
        candidate["run_id"] = run_id
        candidate["round_id"] = round_id
        validate_payload_or_raise("round-task", candidate, label=f"Task index {index}")
        normalized.append(candidate)
    if not normalized:
        raise ValueError("At least one round-task is required to scaffold a round.")
    return normalized


def scaffold_round(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    tasks: list[dict[str, Any]],
    mission: dict[str, Any] | None,
    pretty: bool,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    normalized_round_id = contract_module.normalize_round_id(round_id)
    round_path = run_path / contract_module.round_dir_name(normalized_round_id)
    normalized_tasks = normalize_round_tasks(tasks=tasks, run_id=run_id, round_id=normalized_round_id)
    investigation_plan = (
        build_investigation_plan(mission=mission, round_id=normalized_round_id)
        if isinstance(mission, dict)
        else placeholder_investigation_plan(run_id=run_id, round_id=normalized_round_id)
    )
    source_selection_roles = ("sociologist", "environmentalist")
    source_selection_files = {
        role: placeholder_source_selection(
            run_id=run_id,
            round_id=normalized_round_id,
            role=role,
            task_ids=[
                item["task_id"]
                for item in normalized_tasks
                if item.get("assigned_role") == role and isinstance(item.get("task_id"), str)
            ],
            allowed_sources=allowed_sources_for_role(mission or {}, role),
        )
        for role in source_selection_roles
    }

    files_to_write = {
        round_path / "moderator" / "tasks.json": normalized_tasks,
        round_path / "shared" / "claims.json": [],
        round_path / "shared" / "observations.json": [],
        round_path / "shared" / "evidence_cards.json": [],
        round_path / "shared" / "investigation_plan.json": investigation_plan,
        round_path / "moderator" / "override_requests.json": [],
        round_path / "sociologist" / "claim_curation.json": placeholder_claim_curation(
            run_id=run_id,
            round_id=normalized_round_id,
        ),
        round_path / "sociologist" / "claim_submissions.json": [],
        round_path / "sociologist" / "override_requests.json": [],
        round_path / "environmentalist" / "observation_curation.json": placeholder_observation_curation(
            run_id=run_id,
            round_id=normalized_round_id,
        ),
        round_path / "environmentalist" / "observation_submissions.json": [],
        round_path / "environmentalist" / "override_requests.json": [],
        round_path / "historian" / "override_requests.json": [],
        round_path / "sociologist" / "source_selection.json": source_selection_files["sociologist"],
        round_path / "environmentalist" / "source_selection.json": source_selection_files["environmentalist"],
        round_path / "sociologist" / "sociologist_report.json": placeholder_report(
            run_id=run_id,
            round_id=normalized_round_id,
            role="sociologist",
        ),
        round_path / "environmentalist" / "environmentalist_report.json": placeholder_report(
            run_id=run_id,
            round_id=normalized_round_id,
            role="environmentalist",
        ),
    }

    directories = (
        round_path / "sociologist" / "raw",
        round_path / "sociologist" / "normalized",
        round_path / "sociologist" / "derived",
        round_path / "environmentalist" / "raw",
        round_path / "environmentalist" / "normalized",
        round_path / "environmentalist" / "derived",
        round_path / "historian" / "raw",
        round_path / "historian" / "normalized",
        round_path / "historian" / "derived",
        round_path / "moderator" / "derived",
        round_path / "shared" / "contexts",
        round_path / "shared" / "evidence-library",
        round_path / "shared" / "evidence-library" / "audit-chain",
        round_path / "shared" / "evidence-library" / "audit-chain" / "objects",
    )
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

    for path, payload in files_to_write.items():
        write_json(path, payload, pretty=pretty)

    library_views = {
        round_path / "shared" / "evidence-library" / "claims_active.json": [],
        round_path / "shared" / "evidence-library" / "observations_active.json": [],
        round_path / "shared" / "evidence-library" / "cards_active.json": [],
        round_path / "shared" / "evidence-library" / "isolated_active.json": [],
        round_path / "shared" / "evidence-library" / "remands_open.json": [],
        round_path / "shared" / "evidence-library" / "context_sociologist.json": {},
        round_path / "shared" / "evidence-library" / "context_environmentalist.json": {},
        round_path / "shared" / "evidence-library" / "context_moderator.json": {},
    }
    for path, payload in library_views.items():
        write_json(path, payload, pretty=pretty)
    atomic_write_text_file(round_path / "shared" / "evidence-library" / "ledger.jsonl", "")
    atomic_write_text_file(round_path / "shared" / "evidence-library" / "audit-chain" / "receipts.jsonl", "")

    return {
        "round_id": normalized_round_id,
        "round_dir": str(round_path),
        "files_written": [
            str(path)
            for path in sorted(
                {
                    *files_to_write,
                    *library_views,
                    round_path / "shared" / "evidence-library" / "ledger.jsonl",
                    round_path / "shared" / "evidence-library" / "audit-chain" / "receipts.jsonl",
                }
            )
        ],
        "directories_ready": [str(path) for path in sorted(directories)],
    }


def scaffold_run_from_mission(
    *,
    run_dir: Path,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]] | None,
    pretty: bool,
) -> dict[str, Any]:
    validate_payload_or_raise("mission", mission, label="Mission payload")

    run_path = run_dir.expanduser().resolve()
    run_path.mkdir(parents=True, exist_ok=True)
    run_id = mission["run_id"]
    round_id = "round-001"
    task_list = tasks if tasks is not None else default_round_tasks(mission=mission, round_id=round_id)

    mission_path = run_path / "mission.json"
    write_json(mission_path, mission, pretty=pretty)
    round_result = scaffold_round(
        run_dir=run_path,
        run_id=run_id,
        round_id=round_id,
        tasks=task_list,
        mission=mission,
        pretty=pretty,
    )
    return {
        "run_dir": str(run_path),
        "run_id": run_id,
        "round_id": round_id,
        "mission_path": str(mission_path),
        "round": round_result,
        "schema_path": str(contract_module.SCHEMA_PATH),
    }


def scaffold_run(
    *,
    run_dir: Path,
    run_id: str,
    topic: str,
    objective: str,
    start_utc: str,
    end_utc: str,
    region_label: str,
    geometry: dict[str, Any],
    pretty: bool,
) -> dict[str, Any]:
    start_dt = contract_module.parse_utc_datetime(start_utc)
    end_dt = contract_module.parse_utc_datetime(end_utc)
    if start_dt is None:
        raise ValueError("--start-utc must be RFC3339 UTC with trailing Z.")
    if end_dt is None:
        raise ValueError("--end-utc must be RFC3339 UTC with trailing Z.")
    if end_dt < start_dt:
        raise ValueError("--end-utc must be >= --start-utc.")

    mission = copy.deepcopy(contract_module.EXAMPLES["mission"])
    mission["run_id"] = run_id
    mission["topic"] = topic
    mission["objective"] = objective
    mission["window"]["start_utc"] = start_utc
    mission["window"]["end_utc"] = end_utc
    mission["region"]["label"] = region_label
    mission["region"]["geometry"] = geometry
    return scaffold_run_from_mission(
        run_dir=run_dir,
        mission=mission,
        tasks=None,
        pretty=pretty,
    )


def validate_bundle(run_dir: Path) -> dict[str, Any]:
    bundle_path = run_dir.expanduser().resolve()
    results: list[dict[str, Any]] = []
    missing_required: list[str] = []
    missing_optional: list[str] = []
    round_summaries: list[dict[str, Any]] = []

    mission_path = bundle_path / "mission.json"
    if not mission_path.exists():
        missing_required.append(str(mission_path))
    else:
        mission_payload = read_json(mission_path)
        mission_result = contract_module.validate_payload("mission", mission_payload)
        mission_result["path"] = str(mission_path)
        results.append(mission_result)

    round_ids: list[str] = []
    for child in sorted(bundle_path.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        round_id = contract_module.round_id_from_dirname(child.name)
        if round_id:
            round_ids.append(round_id)
    round_ids.sort(key=contract_module.round_sort_key)
    if not round_ids:
        missing_required.append(str(bundle_path / "round_001"))

    for round_id in round_ids:
        round_path = bundle_path / contract_module.round_dir_name(round_id)
        round_required = {
            round_path / "moderator" / "tasks.json": "round-task",
            round_path / "shared" / "claims.json": "claim",
            round_path / "shared" / "observations.json": "observation",
            round_path / "shared" / "evidence_cards.json": "evidence-card",
            round_path / "sociologist" / "claim_curation.json": "claim-curation",
            round_path / "sociologist" / "claim_submissions.json": "claim-submission",
            round_path / "environmentalist" / "observation_curation.json": "observation-curation",
            round_path / "environmentalist" / "observation_submissions.json": "observation-submission",
            round_path / "sociologist" / "source_selection.json": "source-selection",
            round_path / "environmentalist" / "source_selection.json": "source-selection",
            round_path / "sociologist" / "sociologist_report.json": "expert-report",
            round_path / "environmentalist" / "environmentalist_report.json": "expert-report",
        }
        round_optional = {
            round_path / "historian" / "historian_report.json": "expert-report",
            round_path / "moderator" / "council_decision.json": "council-decision",
            round_path / "moderator" / "override_requests.json": "override-request",
            round_path / "sociologist" / "override_requests.json": "override-request",
            round_path / "environmentalist" / "override_requests.json": "override-request",
            round_path / "historian" / "override_requests.json": "override-request",
            round_path / "sociologist" / "data_readiness_report.json": "data-readiness-report",
            round_path / "environmentalist" / "data_readiness_report.json": "data-readiness-report",
            round_path / "moderator" / "matching_authorization.json": "matching-authorization",
            round_path / "moderator" / "matching_adjudication.json": "matching-adjudication",
            round_path / "moderator" / "investigation_review.json": "investigation-review",
            round_path / "shared" / "matching_result.json": "matching-result",
            round_path / "shared" / "evidence_adjudication.json": "evidence-adjudication",
            round_path / "shared" / "evidence-library" / "claims_active.json": "claim-submission",
            round_path / "shared" / "evidence-library" / "observations_active.json": "observation-submission",
            round_path / "shared" / "evidence-library" / "cards_active.json": "evidence-card",
            round_path / "shared" / "evidence-library" / "isolated_active.json": "isolated-entry",
            round_path / "shared" / "evidence-library" / "remands_open.json": "remand-entry",
        }

        round_results: list[dict[str, Any]] = []
        round_missing_required: list[str] = []
        round_missing_optional: list[str] = []

        for path, kind in round_required.items():
            if not path.exists():
                round_missing_required.append(str(path))
                continue
            payload = read_json(path)
            result = contract_module.validate_payload(kind, payload)
            result["path"] = str(path)
            round_results.append(result)
            results.append(result)

        for path, kind in round_optional.items():
            if not path.exists():
                round_missing_optional.append(str(path))
                continue
            payload = read_json(path)
            result = contract_module.validate_payload(kind, payload)
            result["path"] = str(path)
            round_results.append(result)
            results.append(result)

        try:
            from eco_council_runtime.controller.audit_chain import validate_round_audit_chain
        except Exception:  # noqa: BLE001
            validate_round_audit_chain = None
        if validate_round_audit_chain is not None:
            audit_result = validate_round_audit_chain(bundle_path, round_id, require_exists=False)
            if audit_result.get("receipt_count") or Path(contract_module.maybe_text(audit_result.get("path"))).exists():
                round_results.append(audit_result)
                results.append(audit_result)

        missing_required.extend(round_missing_required)
        missing_optional.extend(round_missing_optional)
        round_summaries.append(
            {
                "round_id": round_id,
                "round_dir": str(round_path),
                "missing_required_files": round_missing_required,
                "missing_optional_files": round_missing_optional,
                "results": round_results,
            }
        )

    ok = not missing_required and all(item["validation"]["ok"] for item in results)
    return {
        "run_dir": str(bundle_path),
        "ok": ok,
        "round_ids": round_ids,
        "missing_required_files": missing_required,
        "missing_optional_files": missing_optional,
        "results": results,
        "rounds": round_summaries,
    }


def command_list_kinds(_: argparse.Namespace) -> dict[str, Any]:
    return {
        "kinds": list(contract_module.OBJECT_KINDS),
        "schema_path": str(contract_module.SCHEMA_PATH),
        "ddl_path": str(contract_module.DDL_PATH),
    }


def command_write_example(args: argparse.Namespace) -> dict[str, Any]:
    payload = copy.deepcopy(contract_module.EXAMPLES[args.kind])
    output_path = Path(args.output).expanduser().resolve()
    write_json(output_path, payload, pretty=args.pretty)
    return {"kind": args.kind, "output": str(output_path)}


def command_validate(args: argparse.Namespace) -> dict[str, Any]:
    input_path = Path(args.input).expanduser().resolve()
    payload = read_json(input_path)
    result = contract_module.validate_payload(args.kind, payload)
    result["input"] = str(input_path)
    return result


def command_init_db(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    ddl = load_ddl()
    with sqlite3.connect(db_path) as conn:
        conn.executescript(ddl)
        conn.commit()
    return {
        "db": str(db_path),
        "ddl_path": str(contract_module.DDL_PATH),
        "initialized_at": utc_now_iso(),
    }


def command_scaffold_run(args: argparse.Namespace) -> dict[str, Any]:
    if bool(args.point) == bool(args.bbox):
        raise ValueError("Provide exactly one of --point or --bbox.")
    geometry = parse_point(args.point) if args.point else parse_bbox(args.bbox)
    return scaffold_run(
        run_dir=Path(args.run_dir),
        run_id=args.run_id,
        topic=args.topic,
        objective=args.objective,
        start_utc=args.start_utc,
        end_utc=args.end_utc,
        region_label=args.region_label,
        geometry=geometry,
        pretty=args.pretty,
    )


def command_scaffold_run_from_mission(args: argparse.Namespace) -> dict[str, Any]:
    mission_path = Path(args.mission_input).expanduser().resolve()
    mission_payload = read_json(mission_path)
    tasks_payload: list[dict[str, Any]] | None = None
    if args.tasks_input:
        tasks_path = Path(args.tasks_input).expanduser().resolve()
        loaded_tasks = read_json(tasks_path)
        if not isinstance(loaded_tasks, list):
            raise ValueError("--tasks-input must contain a JSON list of round-task objects.")
        tasks_payload = [item for item in loaded_tasks if isinstance(item, dict)]
        if len(tasks_payload) != len(loaded_tasks):
            raise ValueError("--tasks-input must contain only JSON objects.")
    result = scaffold_run_from_mission(
        run_dir=Path(args.run_dir),
        mission=mission_payload,
        tasks=tasks_payload,
        pretty=args.pretty,
    )
    result["mission_input"] = str(mission_path)
    if args.tasks_input:
        result["tasks_input"] = str(Path(args.tasks_input).expanduser().resolve())
    return result


def command_scaffold_round(args: argparse.Namespace) -> dict[str, Any]:
    tasks_path = Path(args.tasks_input).expanduser().resolve()
    task_payload = read_json(tasks_path)
    if not isinstance(task_payload, list):
        raise ValueError("--tasks-input must contain a JSON list of round-task objects.")
    if not all(isinstance(item, dict) for item in task_payload):
        raise ValueError("--tasks-input must contain only JSON objects.")

    mission_path = (
        Path(args.mission_input).expanduser().resolve()
        if args.mission_input
        else Path(args.run_dir).expanduser().resolve() / "mission.json"
    )
    mission_payload = read_json(mission_path)
    validate_payload_or_raise("mission", mission_payload, label="Mission payload")

    run_id = mission_payload["run_id"]
    result = scaffold_round(
        run_dir=Path(args.run_dir),
        run_id=run_id,
        round_id=args.round_id,
        tasks=task_payload,
        mission=mission_payload,
        pretty=args.pretty,
    )
    result["mission_input"] = str(mission_path)
    result["tasks_input"] = str(tasks_path)
    return result


def command_validate_bundle(args: argparse.Namespace) -> dict[str, Any]:
    return validate_bundle(Path(args.run_dir))

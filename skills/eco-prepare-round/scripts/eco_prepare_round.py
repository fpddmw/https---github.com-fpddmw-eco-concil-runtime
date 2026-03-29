#!/usr/bin/env python3
"""Build a minimal fetch plan from mission scaffold inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-prepare-round"

SOURCE_CONFIG = {
    "airnow-hourly-obs-fetch": {"role": "environmentalist", "normalizer_skill": "eco-normalize-airnow-observation-signals", "default_suffix": ".json"},
    "bluesky-cascade-fetch": {"role": "sociologist", "normalizer_skill": "eco-normalize-bluesky-cascade-public-signals", "default_suffix": ".json"},
    "gdelt-doc-search": {"role": "sociologist", "normalizer_skill": "eco-normalize-gdelt-doc-public-signals", "default_suffix": ".json"},
    "openaq-data-fetch": {"role": "environmentalist", "normalizer_skill": "eco-normalize-openaq-observation-signals", "default_suffix": ".json"},
    "open-meteo-historical-fetch": {"role": "environmentalist", "normalizer_skill": "eco-normalize-open-meteo-historical-signals", "default_suffix": ".json"},
    "youtube-video-search": {"role": "sociologist", "normalizer_skill": "eco-normalize-youtube-video-public-signals", "default_suffix": ".json"},
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return payload


def read_json_list(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError(f"Expected a JSON list of objects at {path}")
    return payload


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


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


def sanitize_fragment(value: str) -> str:
    text = maybe_text(value)
    cleaned = [char if char.isalnum() else "-" for char in text]
    normalized = "".join(cleaned).strip("-")
    return normalized or "artifact"


def artifact_imports(mission: dict[str, Any]) -> list[dict[str, Any]]:
    imports = mission.get("artifact_imports") if isinstance(mission.get("artifact_imports"), list) else []
    return [item for item in imports if isinstance(item, dict)]


def task_ids_for_role(tasks: list[dict[str, Any]], role: str) -> list[str]:
    return [maybe_text(item.get("task_id")) for item in tasks if maybe_text(item.get("assigned_role")) == role and maybe_text(item.get("task_id"))]


def normalizer_args_for(source_skill: str, item: dict[str, Any]) -> list[str]:
    args: list[str] = []
    query_text = maybe_text(item.get("query_text"))
    source_mode = maybe_text(item.get("source_mode"))
    if source_skill in {"gdelt-doc-search", "youtube-video-search"} and query_text:
        args.extend(["--query-text-override", query_text])
    if source_skill == "openaq-data-fetch" and source_mode:
        args.extend(["--source-mode", source_mode])
    return args


def role_summary(role: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
    role_steps = [item for item in steps if maybe_text(item.get("role")) == role]
    return {
        "selected_sources": unique_texts([item.get("source_skill") for item in role_steps]),
        "task_ids": unique_texts([task_id for item in role_steps for task_id in item.get("task_ids", [])]),
        "normalizer_skills": unique_texts([item.get("normalizer_skill") for item in role_steps]),
        "step_count": len(role_steps),
    }


def prepare_round_skill(run_dir: str, run_id: str, round_id: str) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    mission_path = (run_dir_path / "mission.json").resolve()
    task_path = (run_dir_path / "investigation" / f"round_tasks_{round_id}.json").resolve()
    output_path = (run_dir_path / "runtime" / f"fetch_plan_{round_id}.json").resolve()

    mission = read_json_object(mission_path)
    if maybe_text(mission.get("run_id")) != run_id:
        raise ValueError(f"run_id mismatch between mission.json and --run-id: {maybe_text(mission.get('run_id'))!r} != {run_id!r}")
    tasks = read_json_list(task_path)
    imports = artifact_imports(mission)

    steps: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    for index, item in enumerate(imports, start=1):
        source_skill = maybe_text(item.get("source_skill"))
        if source_skill not in SOURCE_CONFIG:
            raise ValueError(f"Unsupported source_skill in mission artifact_imports: {source_skill}")
        source_path = Path(maybe_text(item.get("artifact_path"))).expanduser().resolve()
        if not source_path.exists():
            raise ValueError(f"Mission artifact import does not exist: {source_path}")
        config = SOURCE_CONFIG[source_skill]
        role = maybe_text(config["role"])
        suffix = source_path.suffix or maybe_text(config["default_suffix"]) or ".json"
        artifact_path = (run_dir_path / "raw" / round_id / f"{index:02d}-{sanitize_fragment(source_skill)}{suffix}").resolve()
        step = {
            "step_id": f"step-import-{index:02d}-{sanitize_fragment(source_skill)}",
            "role": role,
            "source_skill": source_skill,
            "normalizer_skill": maybe_text(config["normalizer_skill"]),
            "task_ids": task_ids_for_role(tasks, role),
            "depends_on": [],
            "source_artifact_path": str(source_path),
            "artifact_path": str(artifact_path),
            "normalizer_args": normalizer_args_for(source_skill, item),
            "notes": [
                f"Copy the prepared local artifact for {source_skill} into the current run raw store before normalization.",
                *([maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else []),
            ],
        }
        if not step["task_ids"]:
            warnings.append({"code": "missing-role-task", "message": f"No round task was found for role={role} while planning source_skill={source_skill}."})
        steps.append(step)

    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    plan_id = "fetch-plan-" + stable_hash(run_id, round_id, len(steps), mission_path, task_path)[:12]
    payload = {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "run": {
            "run_id": run_id,
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region_label": maybe_text(region.get("label")),
            "window": {
                "start_utc": maybe_text(window.get("start_utc")),
                "end_utc": maybe_text(window.get("end_utc")),
            },
        },
        "plan_id": plan_id,
        "input_snapshot": {
            "mission": {"path": str(mission_path), "sha256": file_sha256(mission_path)},
            "tasks": {"path": str(task_path), "sha256": file_sha256(task_path)},
        },
        "roles": {
            "sociologist": role_summary("sociologist", steps),
            "environmentalist": role_summary("environmentalist", steps),
        },
        "steps": steps,
    }
    write_json_file(output_path, payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_path), "record_locator": "$", "artifact_ref": f"{output_path}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_path),
            "plan_id": plan_id,
            "source_count": len(imports),
            "step_count": len(steps),
        },
        "receipt_id": "ingress-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, plan_id)[:20],
        "batch_id": "ingressbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_path.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [plan_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [plan_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-import-fetch-execution"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a minimal fetch plan from mission scaffold inputs.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = prepare_round_skill(run_dir=args.run_dir, run_id=args.run_id, round_id=args.round_id)
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
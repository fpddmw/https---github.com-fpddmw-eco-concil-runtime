#!/usr/bin/env python3
"""Derive compact observation scope proposals from observation-side evidence objects."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "derive-observation-scope"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_merged_observation_context,
    load_observation_candidate_context,
    sync_observation_scope_result_set,
)


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


def resolve_path(run_dir: Path, path_text: str, default_name: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def unique_refs(refs: list[dict[str, Any]], limit: int) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if not artifact_ref or artifact_ref in seen:
            continue
        seen.add(artifact_ref)
        deduped.append(
            {
                "signal_id": maybe_text(ref.get("signal_id")),
                "artifact_path": maybe_text(ref.get("artifact_path")),
                "record_locator": maybe_text(ref.get("record_locator")),
                "artifact_ref": artifact_ref,
            }
        )
        if len(deduped) >= limit:
            break
    return deduped


def observation_items_from_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("merged_observations"), list):
        return [item for item in payload["merged_observations"] if isinstance(item, dict)]
    return [item for item in payload.get("candidates", []) if isinstance(item, dict)] if isinstance(payload.get("candidates"), list) else []


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def select_observation_input_context(
    run_dir_path: Path,
    *,
    run_id: str,
    round_id: str,
    merged_observations_path: str,
    observation_candidates_path: str,
) -> dict[str, Any]:
    merged_context = load_merged_observation_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        merged_observations_path=merged_observations_path,
    )
    candidate_context = load_observation_candidate_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        observation_candidates_path=observation_candidates_path,
        db_path=maybe_text(merged_context.get("db_path")),
    )
    merged_available = source_available(
        merged_context.get("merged_observation_source")
    )
    candidate_available = source_available(
        candidate_context.get("observation_candidate_source")
    )

    if merged_available:
        return {
            "selected_kind": "merged-observation",
            "selected_source": maybe_text(
                merged_context.get("merged_observation_source")
            ),
            "selected_file": maybe_text(
                merged_context.get("merged_observations_file")
            ),
            "selected_wrapper": merged_context.get(
                "merged_observations_wrapper", {}
            ),
            "db_path": maybe_text(candidate_context.get("db_path"))
            or maybe_text(merged_context.get("db_path")),
            "merged_context": merged_context,
            "candidate_context": candidate_context,
            "warnings": [],
        }
    if candidate_available:
        return {
            "selected_kind": "observation-candidate",
            "selected_source": maybe_text(
                candidate_context.get("observation_candidate_source")
            ),
            "selected_file": maybe_text(
                candidate_context.get("observation_candidates_file")
            ),
            "selected_wrapper": candidate_context.get(
                "observation_candidates_wrapper", {}
            ),
            "db_path": maybe_text(candidate_context.get("db_path"))
            or maybe_text(merged_context.get("db_path")),
            "merged_context": merged_context,
            "candidate_context": candidate_context,
            "warnings": [],
        }
    merged_file = maybe_text(merged_context.get("merged_observations_file"))
    candidates_file = maybe_text(candidate_context.get("observation_candidates_file"))
    return {
        "selected_kind": "missing-observation-input",
        "selected_source": "missing-observation-input",
        "selected_file": candidates_file or merged_file,
        "selected_wrapper": candidate_context.get("observation_candidates_wrapper", {}),
        "db_path": maybe_text(candidate_context.get("db_path"))
        or maybe_text(merged_context.get("db_path")),
        "merged_context": merged_context,
        "candidate_context": candidate_context,
        "warnings": [
            {
                "code": "missing-observation-input",
                "message": "No observation-side artifact or analysis result was "
                f"found at {merged_file} or {candidates_file}.",
            }
        ],
    }


def metric_tags(metric: str) -> list[str]:
    name = maybe_text(metric)
    if name in {"pm2_5", "pm10", "o3"}:
        return ["air-quality", name]
    if name in {"temperature_2m", "apparent_temperature"}:
        return ["temperature", name]
    if name in {"precipitation", "precipitation_sum", "rain"}:
        return ["precipitation", name]
    return [name] if name else []


def derive_observation_scope_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    merged_observations_path: str,
    observation_candidates_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(run_dir_path, output_path, f"observation_scope_proposals_{round_id}.json")
    input_selection = select_observation_input_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        merged_observations_path=merged_observations_path,
        observation_candidates_path=observation_candidates_path,
    )
    merged_context = input_selection["merged_context"]
    candidate_context = input_selection["candidate_context"]
    input_file = Path(maybe_text(input_selection.get("selected_file")))
    observation_input_source = maybe_text(input_selection.get("selected_source"))
    observation_input_kind = maybe_text(input_selection.get("selected_kind"))
    warnings: list[dict[str, str]] = list(input_selection.get("warnings", []))
    observation_items = observation_items_from_payload(
        input_selection.get("selected_wrapper")
    )
    scopes: list[dict[str, Any]] = []
    for item in observation_items:
        observation_id = maybe_text(item.get("merged_observation_id") or item.get("observation_id"))
        metric = maybe_text(item.get("metric"))
        place_scope = item.get("place_scope") if isinstance(item.get("place_scope"), dict) else {}
        geometry = place_scope.get("geometry") if isinstance(place_scope.get("geometry"), dict) else {}
        usable_for_matching = bool(geometry)
        scope_kind = "point" if maybe_text(geometry.get("type")) == "Point" else "unknown"
        label = maybe_text(place_scope.get("label")) or ("Signal-backed point footprint" if usable_for_matching else "Unknown observation footprint")
        scopes.append(
            {
                "schema_version": "n2.2",
                "observation_scope_id": "obsscope-" + stable_hash(run_id, round_id, observation_id, metric, label)[:12],
                "run_id": run_id,
                "round_id": round_id,
                "observation_id": observation_id,
                "metric": metric,
                "scope_label": label,
                "scope_kind": scope_kind,
                "matching_tags": metric_tags(metric),
                "place_scope": {"label": label, "geometry": geometry},
                "time_window": item.get("time_window") if isinstance(item.get("time_window"), dict) else {},
                "usable_for_matching": usable_for_matching,
                "method": "heuristic-observation-scope-v1",
                "confidence": 0.88 if usable_for_matching else 0.5,
                "evidence_refs": unique_refs(item.get("provenance_refs", []) if isinstance(item.get("provenance_refs"), list) else [], 10),
            }
        )
    wrapper = {
        "schema_version": "n2.2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "query_basis": {
            "input_path": str(input_file),
            "input_source": observation_input_source
            or "missing-observation-input",
            "input_kind": observation_input_kind
            or "missing-observation-input",
            "selection_mode": "prefer-merged-observations-then-candidates",
            "method": "heuristic-observation-scope-v1",
        },
        "input_path": str(input_file),
        "observation_input_source": observation_input_source
        or "missing-observation-input",
        "observation_input_kind": observation_input_kind
        or "missing-observation-input",
        "observed_inputs": {
            "merged_observations_present": source_available(
                merged_context.get("merged_observation_source")
            ),
            "merged_observations_artifact_present": bool(
                merged_context.get("merged_observations_artifact_present")
            ),
            "observation_candidates_present": source_available(
                candidate_context.get("observation_candidate_source")
            ),
            "observation_candidates_artifact_present": bool(
                candidate_context.get("observation_candidates_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "merged_observations": merged_context.get("analysis_sync", {}),
            "observation_candidates": candidate_context.get("analysis_sync", {}),
        },
        "scope_count": len(scopes),
        "scopes": scopes,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_observation_scope_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        observation_scope_path=output_file,
        db_path=maybe_text(input_selection.get("db_path")),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$.scopes", "artifact_ref": f"{output_file}:$.scopes"}]
    for scope in scopes:
        artifact_refs.extend(scope["evidence_refs"])
    if not scopes:
        warnings.append({"code": "no-observation-scopes", "message": "No observation scope proposals were derived from the available observation-side inputs."})
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "input_path": str(input_file),
            "output_path": str(output_file),
            "scope_count": len(scopes),
            "observation_input_source": observation_input_source
            or "missing-observation-input",
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "scope-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "scopebatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [scope["observation_scope_id"] for scope in scopes],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [scope["observation_scope_id"] for scope in scopes],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": ["Some observation scopes still have no explicit geometry."] if scopes and any(not scope["usable_for_matching"] for scope in scopes) else (["No observation scopes are available for board review."] if not scopes else []),
            "challenge_hints": ["Check whether point-scoped observations are representative enough for broader claims."] if scopes else [],
            "suggested_next_skills": ["score-evidence-coverage", "post-board-note"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive compact observation scope proposals from observation-side evidence objects.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--merged-observations-path", default="")
    parser.add_argument("--observation-candidates-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = derive_observation_scope_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        merged_observations_path=args.merged_observations_path,
        observation_candidates_path=args.observation_candidates_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

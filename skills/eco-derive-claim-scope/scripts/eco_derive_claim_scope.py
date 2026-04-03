#!/usr/bin/env python3
"""Derive compact claim scope proposals from claim-side evidence objects."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-derive-claim-scope"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import sync_claim_scope_result_set  # noqa: E402


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


def claim_items_from_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("clusters"), list):
        return [item for item in payload["clusters"] if isinstance(item, dict)]
    return [item for item in payload.get("candidates", []) if isinstance(item, dict)] if isinstance(payload.get("candidates"), list) else []


def infer_location_scope(text: str) -> tuple[str, str, bool]:
    folded = maybe_text(text).casefold()
    if any(token in folded for token in ("new york", "nyc", "los angeles", "beijing", "shanghai", "city")):
        return "city-scale public impact footprint", "city", True
    if any(token in folded for token in ("statewide", "province", "regional", "region", "coast")):
        return "regional public impact footprint", "regional", True
    if any(token in folded for token in ("national", "nationwide", "countrywide")):
        return "national public impact footprint", "national", True
    return "public evidence footprint", "unknown", False


def infer_tags(text: str, claim_type: str) -> list[str]:
    tags: set[str] = set()
    folded = maybe_text(text).casefold()
    claim_kind = maybe_text(claim_type)
    if claim_kind:
        tags.add(claim_kind)
    if any(token in folded for token in ("smoke", "wildfire", "haze", "pollution", "air quality")):
        tags.update({"air-quality", "smoke"})
    if any(token in folded for token in ("heat", "temperature", "hot")):
        tags.update({"temperature", "heat"})
    if any(token in folded for token in ("rain", "flood", "storm", "precipitation")):
        tags.update({"precipitation", "hydrology"})
    return sorted(tags)


def derive_claim_scope_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_candidates_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    cluster_file = resolve_path(run_dir_path, claim_cluster_path, f"claim_candidate_clusters_{round_id}.json")
    candidate_file = resolve_path(run_dir_path, claim_candidates_path, f"claim_candidates_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"claim_scope_proposals_{round_id}.json")

    payload = load_json_if_exists(cluster_file)
    input_file = cluster_file
    if payload is None:
        payload = load_json_if_exists(candidate_file)
        input_file = candidate_file
    warnings: list[dict[str, str]] = []
    if payload is None:
        warnings.append({"code": "missing-claim-input", "message": f"No claim-side artifact was found at {cluster_file} or {candidate_file}."})
    claim_items = claim_items_from_payload(payload)
    scopes: list[dict[str, Any]] = []
    for item in claim_items:
        claim_id = maybe_text(item.get("cluster_id") or item.get("claim_id"))
        text = maybe_text(item.get("representative_statement") or item.get("statement") or item.get("cluster_label") or item.get("summary"))
        claim_type = maybe_text(item.get("claim_type"))
        location_label, scope_kind, has_location = infer_location_scope(text)
        tags = infer_tags(text, claim_type)
        usable_for_matching = has_location or bool(tags)
        scopes.append(
            {
                "schema_version": "n2.2",
                "claim_scope_id": "claimscope-" + stable_hash(run_id, round_id, claim_id, location_label, ",".join(tags))[:12],
                "run_id": run_id,
                "round_id": round_id,
                "claim_id": claim_id,
                "claim_type": claim_type,
                "scope_label": location_label,
                "scope_kind": scope_kind,
                "matching_tags": tags,
                "claim_scope": {"label": location_label, "geometry": {}, "usable_for_matching": usable_for_matching},
                "place_scope": {"label": location_label, "geometry": {}},
                "method": "heuristic-text-scope-v1",
                "confidence": 0.8 if usable_for_matching else 0.45,
                "evidence_refs": unique_refs(item.get("public_refs", []) if isinstance(item.get("public_refs"), list) else [], 10),
            }
        )
    wrapper = {
        "schema_version": "n2.2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "input_path": str(input_file),
        "scope_count": len(scopes),
        "scopes": scopes,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_claim_scope_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        claim_scope_path=output_file,
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$.scopes", "artifact_ref": f"{output_file}:$.scopes"}]
    for scope in scopes:
        artifact_refs.extend(scope["evidence_refs"])
    if not scopes:
        warnings.append({"code": "no-claim-scopes", "message": "No claim scope proposals were derived from the available claim-side inputs."})
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "input_path": str(input_file),
            "output_path": str(output_file),
            "scope_count": len(scopes),
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "scope-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "scopebatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [scope["claim_scope_id"] for scope in scopes],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "board_handoff": {
            "candidate_ids": [scope["claim_scope_id"] for scope in scopes],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": ["Some claim scopes still have no explicit geometry and remain label-only."] if scopes else ["No claim scopes are available for board review."],
            "challenge_hints": ["Review whether derived claim scope labels are too broad for localized evidence."] if scopes else [],
            "suggested_next_skills": ["eco-score-evidence-coverage", "eco-post-board-note"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive compact claim scope proposals from claim-side evidence objects.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-cluster-path", default="")
    parser.add_argument("--claim-candidates-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = derive_claim_scope_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_candidates_path=args.claim_candidates_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

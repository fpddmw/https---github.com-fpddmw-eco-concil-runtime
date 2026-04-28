#!/usr/bin/env python3
"""Build a normalization audit artifact from local candidate files."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "build-normalization-audit"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_candidate_context,
    load_observation_candidate_context,
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


def resolve_output_path(run_dir: Path, output_path: str, default_name: str) -> Path:
    text = maybe_text(output_path)
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


def build_normalization_audit_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_candidates_path: str,
    observation_candidates_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    claim_path = resolve_output_path(run_dir_path, claim_candidates_path, f"claim_candidates_{round_id}.json")
    observation_path = resolve_output_path(run_dir_path, observation_candidates_path, f"observation_candidates_{round_id}.json")
    audit_path = resolve_output_path(run_dir_path, output_path, f"normalization_audit_{round_id}.json")
    claim_context = load_claim_candidate_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_candidates_path=claim_path,
    )
    observation_context = load_observation_candidate_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        observation_candidates_path=observation_path,
        db_path=maybe_text(claim_context.get("db_path")),
    )
    warnings = []
    warnings.extend(
        claim_context.get("warnings", [])
        if isinstance(claim_context.get("warnings"), list)
        else []
    )
    warnings.extend(
        observation_context.get("warnings", [])
        if isinstance(observation_context.get("warnings"), list)
        else []
    )
    claims = claim_context.get("claim_candidates", []) if isinstance(claim_context.get("claim_candidates"), list) else []
    observations = observation_context.get("observation_candidates", []) if isinstance(observation_context.get("observation_candidates"), list) else []
    claim_path = Path(
        maybe_text(claim_context.get("claim_candidates_file")) or str(claim_path)
    )
    observation_path = Path(
        maybe_text(observation_context.get("observation_candidates_file"))
        or str(observation_path)
    )
    claim_candidate_source = (
        maybe_text(claim_context.get("claim_candidate_source"))
        or "missing-claim-candidate"
    )
    observation_candidate_source = (
        maybe_text(observation_context.get("observation_candidate_source"))
        or "missing-observation-candidate"
    )
    analysis_db_path = (
        maybe_text(observation_context.get("db_path"))
        or maybe_text(claim_context.get("db_path"))
    )
    claim_type_counts = Counter(maybe_text(item.get("claim_type")) for item in claims if maybe_text(item.get("claim_type")))
    metric_counts = Counter(maybe_text(item.get("metric")) for item in observations if maybe_text(item.get("metric")))
    source_skill_counts = Counter(
        maybe_text(item.get("source_skill"))
        for item in list(claims) + list(observations)
        if isinstance(item, dict) and maybe_text(item.get("source_skill"))
    )
    matching_ready_claim_count = sum(1 for item in claims if bool((item.get("claim_scope") or {}).get("usable_for_matching")))
    point_observation_count = sum(1 for item in observations if maybe_text(((item.get("place_scope") or {}).get("geometry") or {}).get("type")) == "Point")
    report = {
        "schema_version": "n2",
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "claim_candidate_count": len(claims),
        "observation_candidate_count": len(observations),
        "matching_ready_claim_count": matching_ready_claim_count,
        "point_observation_count": point_observation_count,
        "claim_type_counts": [{"value": key, "count": count} for key, count in claim_type_counts.most_common(10)],
        "metric_counts": [{"value": key, "count": count} for key, count in metric_counts.most_common(10)],
        "source_skill_counts": [{"value": key, "count": count} for key, count in source_skill_counts.most_common(10)],
        "coverage_summary": f"Built normalization audit from {len(claims)} claim candidates and {len(observations)} observation candidates.",
    }
    wrapper = {
        "schema_version": "n2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "claim_candidates_path": str(claim_path),
        "observation_candidates_path": str(observation_path),
        "claim_candidate_source": claim_candidate_source,
        "observation_candidate_source": observation_candidate_source,
        "db_path": analysis_db_path,
        "observed_inputs": {
            "claim_candidates_present": bool(claims),
            "claim_candidates_artifact_present": bool(
                claim_context.get("claim_candidates_artifact_present")
            ),
            "observation_candidates_present": bool(observations),
            "observation_candidates_artifact_present": bool(
                observation_context.get("observation_candidates_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "claim_candidates": claim_context.get("analysis_sync", {}),
            "observation_candidates": observation_context.get("analysis_sync", {}),
        },
        "report": report,
    }
    write_json(audit_path, wrapper)
    artifact_refs = [
        {"signal_id": "", "artifact_path": str(claim_path), "record_locator": "$.candidates", "artifact_ref": f"{claim_path}:$.candidates"},
        {"signal_id": "", "artifact_path": str(observation_path), "record_locator": "$.candidates", "artifact_ref": f"{observation_path}:$.candidates"},
        {"signal_id": "", "artifact_path": str(audit_path), "record_locator": "$.report", "artifact_ref": f"{audit_path}:$.report"},
    ]
    batch_id = "candbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(audit_path))[:16]
    if not claims:
        warnings.append({"code": "no-claim-candidates", "message": f"No claim candidate result was available for round {round_id} at {claim_path}."})
    if not observations:
        warnings.append({"code": "no-observation-candidates", "message": f"No observation candidate result was available for round {round_id} at {observation_path}."})
    canonical_ids = [maybe_text(item.get("claim_id") or item.get("observation_id")) for item in list(claims) + list(observations) if maybe_text(item.get("claim_id") or item.get("observation_id"))]
    gap_hints = []
    if claims:
        gap_hints.append("Some claim candidates still need scope derivation before direct matching.")
    if observations:
        gap_hints.append("Some observation candidates still need spatial refinement.")
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "claim_candidate_count": len(claims),
            "observation_candidate_count": len(observations),
            "output_path": str(audit_path),
            "claim_candidate_source": claim_candidate_source,
            "observation_candidate_source": observation_candidate_source,
            "db_path": analysis_db_path,
        },
        "receipt_id": "candidate-receipt-" + stable_hash(SKILL_NAME, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": artifact_refs,
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {"candidate_ids": canonical_ids, "evidence_refs": artifact_refs, "gap_hints": gap_hints, "challenge_hints": ["Review normalization diversity before using this QA artifact in a finding or bundle."] if claims or observations else [], "suggested_next_skills": []},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a normalization audit artifact from local candidate files.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-candidates-path", default="")
    parser.add_argument("--observation-candidates-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_normalization_audit_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_candidates_path=args.claim_candidates_path,
        observation_candidates_path=args.observation_candidates_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

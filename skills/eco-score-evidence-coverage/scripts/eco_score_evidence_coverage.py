#!/usr/bin/env python3
"""Score claim-side evidence coverage from links and scope proposals."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
import sys
from typing import Any

SKILL_NAME = "eco-score-evidence-coverage"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_observation_link_context,
    load_claim_scope_context,
    load_observation_scope_context,
    sync_evidence_coverage_result_set,
)


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


def claim_scope_map(payload: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("scopes"), list):
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for scope in payload["scopes"]:
        if isinstance(scope, dict) and maybe_text(scope.get("claim_id")):
            mapping[maybe_text(scope.get("claim_id"))] = scope
    return mapping


def observation_scope_map(payload: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("scopes"), list):
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for scope in payload["scopes"]:
        if isinstance(scope, dict) and maybe_text(scope.get("observation_id")):
            mapping[maybe_text(scope.get("observation_id"))] = scope
    return mapping


def readiness_from_score(score: float) -> str:
    if score >= 0.75:
        return "strong"
    if score >= 0.5:
        return "partial"
    if score > 0:
        return "weak"
    return "empty"


def score_evidence_coverage_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    links_path: str,
    claim_scope_path: str,
    observation_scope_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(run_dir_path, output_path, f"evidence_coverage_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    links_context = load_claim_observation_link_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        links_path=links_path,
    )
    claim_scope_context = load_claim_scope_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_scope_path=claim_scope_path,
        db_path=maybe_text(links_context.get("db_path")),
    )
    observation_scope_context = load_observation_scope_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        observation_scope_path=observation_scope_path,
        db_path=maybe_text(claim_scope_context.get("db_path"))
        or maybe_text(links_context.get("db_path")),
    )
    warnings.extend(
        links_context.get("warnings", [])
        if isinstance(links_context.get("warnings"), list)
        else []
    )
    warnings.extend(
        claim_scope_context.get("warnings", [])
        if isinstance(claim_scope_context.get("warnings"), list)
        else []
    )
    warnings.extend(
        observation_scope_context.get("warnings", [])
        if isinstance(observation_scope_context.get("warnings"), list)
        else []
    )
    links = (
        links_context.get("links", [])
        if isinstance(links_context.get("links"), list)
        else []
    )
    claim_scopes = claim_scope_map(claim_scope_context.get("claim_scope_wrapper"))
    observation_scopes = observation_scope_map(
        observation_scope_context.get("observation_scope_wrapper")
    )
    links_file = Path(maybe_text(links_context.get("links_file")))
    claim_scope_file = Path(maybe_text(claim_scope_context.get("claim_scope_file")))
    observation_scope_file = Path(
        maybe_text(observation_scope_context.get("observation_scope_file"))
    )

    by_claim: dict[str, list[dict[str, Any]]] = {}
    for link in links:
        if not isinstance(link, dict):
            continue
        claim_id = maybe_text(link.get("claim_id"))
        if not claim_id:
            continue
        by_claim.setdefault(claim_id, []).append(link)

    coverages: list[dict[str, Any]] = []
    for claim_id in sorted(by_claim.keys()):
        claim_links = by_claim[claim_id]
        support_links = [link for link in claim_links if maybe_text(link.get("relation")) == "support"]
        contradiction_links = [link for link in claim_links if maybe_text(link.get("relation")) == "contradiction"]
        contextual_links = [link for link in claim_links if maybe_text(link.get("relation")) == "contextual"]
        confidence_values = [float(value) for value in [maybe_number(link.get("confidence")) for link in claim_links] if value is not None]
        linked_observation_ids = sorted({maybe_text(link.get("observation_id")) for link in claim_links if maybe_text(link.get("observation_id"))})
        observation_scope_ready_count = sum(1 for observation_id in linked_observation_ids if bool(observation_scopes.get(observation_id, {}).get("usable_for_matching")))
        claim_scope_ready = bool((claim_scopes.get(claim_id, {}).get("claim_scope") or {}).get("usable_for_matching"))
        score = 0.0
        if support_links:
            score += 0.45
        if contradiction_links:
            score += 0.1
        if contextual_links:
            score += 0.05
        if confidence_values:
            score += min(0.25, fmean(confidence_values) * 0.25)
        if claim_scope_ready:
            score += 0.1
        if observation_scope_ready_count:
            score += min(0.2, observation_scope_ready_count * 0.1)
        score = min(1.0, round(score, 3))
        gaps: list[str] = []
        if not claim_scope_ready:
            gaps.append("Claim scope is still too weak for confident matching.")
        if not support_links:
            gaps.append("No support-oriented evidence links were found.")
        if not observation_scope_ready_count:
            gaps.append("Linked observations still lack matching-ready scope fields.")
        coverage_id = "evcover-" + stable_hash(run_id, round_id, claim_id)[:12]
        refs = []
        for link in claim_links:
            refs.extend(link.get("evidence_refs", []) if isinstance(link.get("evidence_refs"), list) else [])
        coverages.append(
            {
                "schema_version": "n2.2",
                "coverage_id": coverage_id,
                "run_id": run_id,
                "round_id": round_id,
                "claim_id": claim_id,
                "coverage_score": score,
                "readiness": readiness_from_score(score),
                "support_link_count": len(support_links),
                "contradiction_link_count": len(contradiction_links),
                "contextual_link_count": len(contextual_links),
                "linked_observation_count": len(linked_observation_ids),
                "claim_scope_ready": claim_scope_ready,
                "observation_scope_ready_count": observation_scope_ready_count,
                "gap_hints": gaps,
                "evidence_refs": unique_refs(refs, 12),
            }
        )

    wrapper = {
        "schema_version": "n2.2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "links_path": str(links_file),
        "claim_scope_path": str(claim_scope_file),
        "observation_scope_path": str(observation_scope_file),
        "links_source": maybe_text(links_context.get("links_source"))
        or "missing-claim-observation-link",
        "claim_scope_source": maybe_text(claim_scope_context.get("claim_scope_source"))
        or "missing-claim-scope",
        "observation_scope_source": maybe_text(
            observation_scope_context.get("observation_scope_source")
        )
        or "missing-observation-scope",
        "observed_inputs": {
            "links_present": bool(links),
            "links_artifact_present": bool(links_context.get("links_artifact_present")),
            "claim_scope_present": bool(claim_scopes),
            "claim_scope_artifact_present": bool(
                claim_scope_context.get("claim_scope_artifact_present")
            ),
            "observation_scope_present": bool(observation_scopes),
            "observation_scope_artifact_present": bool(
                observation_scope_context.get("observation_scope_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "links": links_context.get("analysis_sync", {}),
            "claim_scope": claim_scope_context.get("analysis_sync", {}),
            "observation_scope": observation_scope_context.get("analysis_sync", {}),
        },
        "coverage_count": len(coverages),
        "coverages": coverages,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_evidence_coverage_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        coverage_path=output_file,
        db_path=maybe_text(observation_scope_context.get("db_path"))
        or maybe_text(claim_scope_context.get("db_path"))
        or maybe_text(links_context.get("db_path")),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$.coverages", "artifact_ref": f"{output_file}:$.coverages"}]
    for coverage in coverages:
        artifact_refs.extend(coverage["evidence_refs"])
    if not coverages:
        warnings.append({"code": "no-coverages", "message": "No evidence coverage objects were produced from the available links and scopes."})
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "coverage_count": len(coverages),
            "links_source": maybe_text(links_context.get("links_source"))
            or "missing-claim-observation-link",
            "claim_scope_source": maybe_text(claim_scope_context.get("claim_scope_source"))
            or "missing-claim-scope",
            "observation_scope_source": maybe_text(
                observation_scope_context.get("observation_scope_source")
            )
            or "missing-observation-scope",
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "coverage-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "coveragebatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [coverage["coverage_id"] for coverage in coverages],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [coverage["coverage_id"] for coverage in coverages],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": [] if coverages else ["No evidence coverage summaries are available for board review."],
            "challenge_hints": ["Claims with contradiction links should be routed to challenger review first."] if any(coverage["contradiction_link_count"] > 0 for coverage in coverages) else [],
            "suggested_next_skills": ["eco-post-board-note", "eco-update-hypothesis-status", "eco-open-challenge-ticket"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score claim-side evidence coverage from links and scope proposals.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--links-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--observation-scope-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = score_evidence_coverage_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        links_path=args.links_path,
        claim_scope_path=args.claim_scope_path,
        observation_scope_path=args.observation_scope_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

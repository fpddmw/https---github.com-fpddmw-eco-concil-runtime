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

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_candidate_context,
    load_claim_cluster_context,
    sync_claim_scope_result_set,
)
from eco_council_runtime.analysis_objects import (  # noqa: E402
    merged_lineage,
    normalize_claim_scope_payload,
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


def claim_items_from_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("clusters"), list):
        return [item for item in payload["clusters"] if isinstance(item, dict)]
    return [item for item in payload.get("candidates", []) if isinstance(item, dict)] if isinstance(payload.get("candidates"), list) else []


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def select_claim_input_context(
    run_dir_path: Path,
    *,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_candidates_path: str,
) -> dict[str, Any]:
    cluster_context = load_claim_cluster_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
    )
    candidate_context = load_claim_candidate_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_candidates_path=claim_candidates_path,
        db_path=maybe_text(cluster_context.get("db_path")),
    )
    cluster_available = source_available(cluster_context.get("claim_cluster_source"))
    candidate_available = source_available(
        candidate_context.get("claim_candidate_source")
    )

    if cluster_available:
        return {
            "selected_kind": "claim-cluster",
            "selected_source": maybe_text(cluster_context.get("claim_cluster_source")),
            "selected_file": maybe_text(cluster_context.get("claim_cluster_file")),
            "selected_wrapper": cluster_context.get("claim_cluster_wrapper", {}),
            "db_path": maybe_text(candidate_context.get("db_path"))
            or maybe_text(cluster_context.get("db_path")),
            "cluster_context": cluster_context,
            "candidate_context": candidate_context,
            "warnings": [],
        }
    if candidate_available:
        return {
            "selected_kind": "claim-candidate",
            "selected_source": maybe_text(
                candidate_context.get("claim_candidate_source")
            ),
            "selected_file": maybe_text(candidate_context.get("claim_candidates_file")),
            "selected_wrapper": candidate_context.get("claim_candidates_wrapper", {}),
            "db_path": maybe_text(candidate_context.get("db_path"))
            or maybe_text(cluster_context.get("db_path")),
            "cluster_context": cluster_context,
            "candidate_context": candidate_context,
            "warnings": [],
        }
    cluster_file = maybe_text(cluster_context.get("claim_cluster_file"))
    candidate_file = maybe_text(candidate_context.get("claim_candidates_file"))
    return {
        "selected_kind": "missing-claim-input",
        "selected_source": "missing-claim-input",
        "selected_file": candidate_file or cluster_file,
        "selected_wrapper": candidate_context.get("claim_candidates_wrapper", {}),
        "db_path": maybe_text(candidate_context.get("db_path"))
        or maybe_text(cluster_context.get("db_path")),
        "cluster_context": cluster_context,
        "candidate_context": candidate_context,
        "warnings": [
            {
                "code": "missing-claim-input",
                "message": "No claim-side artifact or analysis result was found "
                f"at {cluster_file} or {candidate_file}.",
            }
        ],
    }


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


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def infer_verifiability_kind(item: dict[str, Any], text: str) -> str:
    explicit = maybe_text(item.get("verifiability_hint") or item.get("verifiability_posture"))
    if explicit:
        return explicit
    issue_hint = maybe_text(item.get("issue_hint") or item.get("issue_label"))
    concern_facets = list_field(item, "concern_facets")
    folded = maybe_text(text).casefold()
    if issue_hint in {
        "air-quality-smoke",
        "heat-risk",
        "flood-water",
        "water-contamination",
    }:
        return "empirical-observable"
    if issue_hint == "permit-process" or "procedure-governance" in concern_facets:
        return "procedural-record"
    if issue_hint == "representation-trust" or "trust-credibility" in concern_facets:
        return "discourse-representation"
    if "fairness-equity" in concern_facets or "cost-livelihood" in concern_facets:
        return "normative-distribution"
    if any(token in folded for token in ("forecast", "projection", "future", "model")):
        return "predictive-uncertainty"
    return "mixed-public-claim"


def infer_dispute_type(item: dict[str, Any], *, verifiability_kind: str) -> str:
    explicit = maybe_text(item.get("dispute_type"))
    if explicit:
        return explicit
    if verifiability_kind == "empirical-observable":
        return "impact-severity"
    if verifiability_kind == "procedural-record":
        return "governance-procedure"
    if verifiability_kind == "discourse-representation":
        return "representation-gap"
    if verifiability_kind == "normative-distribution":
        return "distributional-conflict"
    if verifiability_kind == "predictive-uncertainty":
        return "forecast-dispute"
    return "mixed-controversy"


def required_evidence_lane(verifiability_kind: str) -> str:
    if verifiability_kind == "empirical-observable":
        return "environmental-observation"
    if verifiability_kind == "procedural-record":
        return "formal-comment-and-policy-record"
    if verifiability_kind == "discourse-representation":
        return "public-discourse-analysis"
    if verifiability_kind == "normative-distribution":
        return "stakeholder-deliberation-analysis"
    if verifiability_kind == "predictive-uncertainty":
        return "mixed-review"
    return "route-before-matching"


def verification_route_recommended(
    verifiability_kind: str,
    *,
    has_location: bool,
    tags: list[str],
) -> bool:
    empirical_tags = {"air-quality", "smoke", "temperature", "heat", "precipitation", "hydrology"}
    return (
        verifiability_kind == "empirical-observable"
        and (has_location or any(tag in empirical_tags for tag in tags))
    )


def derive_claim_scope_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_candidates_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(run_dir_path, output_path, f"claim_scope_proposals_{round_id}.json")
    input_selection = select_claim_input_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
        claim_candidates_path=claim_candidates_path,
    )
    cluster_context = input_selection["cluster_context"]
    candidate_context = input_selection["candidate_context"]
    input_file = Path(maybe_text(input_selection.get("selected_file")))
    claim_input_source = maybe_text(input_selection.get("selected_source"))
    claim_input_kind = maybe_text(input_selection.get("selected_kind"))
    warnings: list[dict[str, str]] = list(input_selection.get("warnings", []))
    claim_items = claim_items_from_payload(input_selection.get("selected_wrapper"))
    scopes: list[dict[str, Any]] = []
    routed_to_verification_count = 0
    for item in claim_items:
        claim_id = maybe_text(item.get("cluster_id") or item.get("claim_id"))
        text = maybe_text(item.get("representative_statement") or item.get("statement") or item.get("cluster_label") or item.get("summary"))
        claim_type = maybe_text(item.get("claim_type"))
        location_label, scope_kind, has_location = infer_location_scope(text)
        tags = infer_tags(text, claim_type)
        verifiability_kind = infer_verifiability_kind(item, text)
        dispute_kind = infer_dispute_type(item, verifiability_kind=verifiability_kind)
        route_recommended = verification_route_recommended(
            verifiability_kind,
            has_location=has_location,
            tags=tags,
        )
        if route_recommended:
            routed_to_verification_count += 1
        lane = required_evidence_lane(verifiability_kind)
        concern_facets = list_field(item, "concern_facets")
        evidence_citation_types = list_field(item, "evidence_citation_types")
        actor_hints = list_field(item, "actor_hints")
        issue_hint = maybe_text(item.get("issue_hint") or item.get("issue_label"))
        issue_terms = list_field(item, "issue_terms")
        evidence_refs = unique_refs(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else (
                item.get("public_refs", [])
                if isinstance(item.get("public_refs"), list)
                else []
            ),
            10,
        )
        basis_claim_ids = list_field(item, "member_claim_ids") or ([claim_id] if claim_id else [])
        source_signal_ids = list_field(item, "source_signal_ids")
        scopes.append(
            normalize_claim_scope_payload(
                {
                    "claim_scope_id": "claimscope-" + stable_hash(run_id, round_id, claim_id, location_label, ",".join(tags))[:12],
                    "run_id": run_id,
                    "round_id": round_id,
                    "claim_id": claim_id,
                    "claim_object_id": claim_id,
                    "claim_input_kind": claim_input_kind,
                    "basis_claim_ids": basis_claim_ids,
                    "source_signal_ids": source_signal_ids,
                    "claim_type": claim_type,
                    "scope_label": location_label,
                    "scope_kind": scope_kind,
                    "matching_tags": tags,
                    "issue_hint": issue_hint,
                    "issue_terms": issue_terms,
                    "concern_facets": concern_facets,
                    "actor_hints": actor_hints,
                    "evidence_citation_types": evidence_citation_types,
                    "verifiability_kind": verifiability_kind,
                    "dispute_type": dispute_kind,
                    "verification_route_recommended": route_recommended,
                    "required_evidence_lane": lane,
                    "matching_eligibility_reason": (
                        "Empirical, place-sensitive issue suitable for optional observation matching."
                        if route_recommended
                        else "Route through controversy analysis or formal records before any observation matching."
                    ),
                    "claim_scope": {
                        "label": location_label,
                        "geometry": {},
                        "usable_for_matching": route_recommended,
                    },
                    "place_scope": {"label": location_label, "geometry": {}},
                    "method": "controversy-routing-scope-v2",
                    "confidence": 0.82 if route_recommended else 0.58,
                    "evidence_refs": evidence_refs,
                    "lineage": merged_lineage(
                        item.get("lineage"),
                        claim_id,
                        basis_claim_ids,
                        source_signal_ids,
                    ),
                    "rationale": (
                        f"Derived a {scope_kind or 'public'} scope for "
                        f"{issue_hint or claim_type or 'the claim object'} and routed it "
                        f"toward {lane}."
                    ),
                    "provenance": {
                        "method": "controversy-routing-scope-v2",
                        "input_kind": claim_input_kind,
                        "input_source": claim_input_source,
                    },
                },
                source_skill=SKILL_NAME,
                artifact_path=str(output_file),
            )
        )
    wrapper = {
        "schema_version": "n2.2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "query_basis": {
            "input_path": str(input_file),
            "input_source": claim_input_source or "missing-claim-input",
            "input_kind": claim_input_kind or "missing-claim-input",
            "selection_mode": "prefer-clusters-then-candidates",
            "method": "controversy-routing-scope-v2",
        },
        "input_path": str(input_file),
        "claim_input_source": claim_input_source or "missing-claim-input",
        "claim_input_kind": claim_input_kind or "missing-claim-input",
        "observed_inputs": {
            "claim_clusters_present": source_available(
                cluster_context.get("claim_cluster_source")
            ),
            "claim_clusters_artifact_present": bool(
                cluster_context.get("claim_cluster_artifact_present")
            ),
            "claim_candidates_present": source_available(
                candidate_context.get("claim_candidate_source")
            ),
            "claim_candidates_artifact_present": bool(
                candidate_context.get("claim_candidates_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "claim_clusters": cluster_context.get("analysis_sync", {}),
            "claim_candidates": candidate_context.get("analysis_sync", {}),
        },
        "scope_count": len(scopes),
        "scopes": scopes,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_claim_scope_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        claim_scope_path=output_file,
        db_path=maybe_text(input_selection.get("db_path")),
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
            "claim_input_source": claim_input_source or "missing-claim-input",
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "scope-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "scopebatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [scope["claim_scope_id"] for scope in scopes],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [scope["claim_scope_id"] for scope in scopes],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": (
                [
                    "Some claim scopes remain non-empirical and should not enter observation matching by default."
                ]
                if scopes and routed_to_verification_count < len(scopes)
                else (
                    ["No claim scopes are available for board review."]
                    if not scopes
                    else []
                )
            ),
            "challenge_hints": [
                "Review whether any issue classified as empirical is actually procedural or representational."
            ]
            if scopes
            else [],
            "suggested_next_skills": [
                "eco-classify-claim-verifiability",
                "eco-route-verification-lane",
                "eco-post-board-note",
            ],
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

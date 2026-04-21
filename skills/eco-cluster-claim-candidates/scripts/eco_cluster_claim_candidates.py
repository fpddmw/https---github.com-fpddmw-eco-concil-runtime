#!/usr/bin/env python3
"""Cluster claim candidates into board-ready claim groups."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-cluster-claim-candidates"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    sync_claim_cluster_result_set,
)

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "with",
}

CLAIM_TYPE_ALIASES = {
    "hazard-impact": {"hazard-impact"},
    "verification": {"verification", "evidence-dispute"},
    "evidence-dispute": {"verification", "evidence-dispute"},
    "social-response": {"social-response", "trust-conflict", "representation-conflict"},
    "trust-conflict": {"social-response", "trust-conflict", "representation-conflict"},
    "procedure-legitimacy": {"procedure-legitimacy"},
    "cost-distribution": {"cost-distribution", "distributional-conflict"},
    "distributional-conflict": {"cost-distribution", "distributional-conflict"},
    "public-claim": {"public-claim"},
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


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


def semantic_tokens(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", text.casefold())
    return [token for token in tokens if token not in STOPWORDS]


def semantic_fingerprint(text: str) -> str:
    tokens = semantic_tokens(text)
    if not tokens:
        return "empty"
    return "-".join(tokens[:12])


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


def list_field(candidate: dict[str, Any], key: str) -> list[str]:
    values = candidate.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def humanize_label(value: str) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    return text.replace("-", " ")


def dominant_value(values: list[str], default: str) -> str:
    counts: dict[str, int] = {}
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    if not counts:
        return default
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ranked[0][0]


def stance_distribution(values: list[str]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [{"stance": label, "count": count} for label, count in ranked[:4]]


def claim_type_matches_filter(requested: str, actual: str) -> bool:
    requested_value = maybe_text(requested)
    actual_value = maybe_text(actual)
    if not requested_value:
        return True
    allowed = CLAIM_TYPE_ALIASES.get(requested_value, {requested_value})
    return actual_value in allowed


def controversy_group_key(candidate: dict[str, Any], text: str) -> str:
    concerns = list_field(candidate, "concern_facets")
    issue = maybe_text(candidate.get("issue_hint")) or maybe_text(candidate.get("claim_type")) or "general-public-controversy"
    stance = maybe_text(candidate.get("stance_hint")) or "unclear"
    signature = ",".join(concerns[:2]) or semantic_fingerprint(text)
    return "|".join(
        [
            maybe_text(candidate.get("claim_type")),
            issue,
            stance,
            signature,
        ]
    )


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


def cluster_claim_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str,
    output_path: str,
    claim_type: str,
    keyword_any: list[str],
    min_member_count: int,
    max_clusters: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    input_file = resolve_path(run_dir_path, input_path, f"claim_candidates_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"claim_candidate_clusters_{round_id}.json")
    payload = load_json_if_exists(input_file)
    warnings: list[dict[str, str]] = []
    if payload is None:
        warnings.append({"code": "missing-input", "message": f"Claim candidate input was not found at {input_file}."})
    candidates = payload.get("candidates", []) if isinstance(payload, dict) and isinstance(payload.get("candidates"), list) else []
    wanted_claim_type = maybe_text(claim_type)
    keywords = [maybe_text(keyword).casefold() for keyword in keyword_any if maybe_text(keyword)]
    groups: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if maybe_text(candidate.get("run_id")) and maybe_text(candidate.get("run_id")) != run_id:
            continue
        if maybe_text(candidate.get("round_id")) and maybe_text(candidate.get("round_id")) != round_id:
            continue
        candidate_type = maybe_text(candidate.get("claim_type"))
        if not claim_type_matches_filter(wanted_claim_type, candidate_type):
            continue
        text = maybe_text(candidate.get("statement") or candidate.get("summary"))
        if keywords and not any(keyword in text.casefold() for keyword in keywords):
            continue
        group_key = controversy_group_key(candidate, text)
        groups.setdefault(group_key, []).append(candidate)
    ordered_groups = sorted(groups.values(), key=lambda items: (-len(items), maybe_text(items[0].get("claim_id"))))
    clusters: list[dict[str, Any]] = []
    singleton_count = 0
    verification_routing_count = 0
    for group in ordered_groups:
        if len(group) < max(1, min_member_count):
            continue
        if len(group) == 1:
            singleton_count += 1
        lead = group[0]
        statements = [maybe_text(item.get("statement") or item.get("summary")) for item in group if maybe_text(item.get("statement") or item.get("summary"))]
        label_source = max(statements, key=len) if statements else maybe_text(lead.get("summary"))
        issue_values = [
            maybe_text(item.get("issue_hint"))
            for item in group
            if maybe_text(item.get("issue_hint"))
        ]
        stance_values = [
            maybe_text(item.get("stance_hint"))
            for item in group
            if maybe_text(item.get("stance_hint"))
        ]
        concern_values: list[str] = []
        actor_values: list[str] = []
        citation_values: list[str] = []
        verification_values: list[str] = []
        dispute_values: list[str] = []
        issue_terms_values: list[str] = []
        start_times = sorted(maybe_text(item.get("time_window", {}).get("start_utc")) for item in group if isinstance(item.get("time_window"), dict) and maybe_text(item.get("time_window", {}).get("start_utc")))
        end_times = sorted(maybe_text(item.get("time_window", {}).get("end_utc")) for item in group if isinstance(item.get("time_window"), dict) and maybe_text(item.get("time_window", {}).get("end_utc")))
        refs: list[dict[str, Any]] = []
        unique_signal_ids: set[str] = set()
        claim_ids: list[str] = []
        total_source_signal_count = 0
        for item in group:
            claim_ids.append(maybe_text(item.get("claim_id")))
            total_source_signal_count += safe_int(item.get("source_signal_count"))
            source_ids = item.get("source_signal_ids")
            if isinstance(source_ids, list):
                unique_signal_ids.update(maybe_text(source_id) for source_id in source_ids if maybe_text(source_id))
            public_refs = item.get("public_refs")
            if isinstance(public_refs, list):
                refs.extend(public_refs)
            concern_values.extend(list_field(item, "concern_facets"))
            actor_values.extend(list_field(item, "actor_hints"))
            citation_values.extend(list_field(item, "evidence_citation_types"))
            issue_terms_values.extend(list_field(item, "issue_terms"))
            if maybe_text(item.get("verifiability_hint")):
                verification_values.append(maybe_text(item.get("verifiability_hint")))
            if maybe_text(item.get("dispute_type")):
                dispute_values.append(maybe_text(item.get("dispute_type")))
        dominant_issue = dominant_value(
            issue_values,
            maybe_text(lead.get("issue_hint")) or maybe_text(lead.get("claim_type")) or "general-public-controversy",
        )
        dominant_stance = dominant_value(
            stance_values,
            maybe_text(lead.get("stance_hint")) or "unclear",
        )
        dominant_verification = dominant_value(
            verification_values,
            maybe_text(lead.get("verifiability_hint")) or "mixed-public-claim",
        )
        dominant_dispute = dominant_value(
            dispute_values,
            maybe_text(lead.get("dispute_type")) or "mixed-controversy",
        )
        concern_facets = unique_texts(concern_values)[:4]
        actor_hints = unique_texts(actor_values)[:4]
        evidence_citation_types = unique_texts(citation_values)[:4]
        issue_terms = unique_texts(issue_terms_values)[:4]
        if dominant_verification != "empirical-observable":
            verification_routing_count += 1
        cluster_title = humanize_label(dominant_issue) or truncate_text(label_source, 160)
        if dominant_stance not in {"", "unclear"}:
            cluster_title = f"{cluster_title} [{humanize_label(dominant_stance)}]"
        fingerprint = semantic_fingerprint(label_source)
        cluster_id = "claimcluster-" + stable_hash(run_id, round_id, lead.get("claim_type"), fingerprint)[:12]
        clusters.append(
            {
                "schema_version": "n2.1",
                "cluster_id": cluster_id,
                "run_id": run_id,
                "round_id": round_id,
                "claim_type": maybe_text(lead.get("claim_type")),
                "status": "cluster-candidate",
                "cluster_label": truncate_text(cluster_title, 160),
                "representative_statement": truncate_text(label_source, 320),
                "semantic_fingerprint": fingerprint,
                "issue_label": dominant_issue,
                "issue_terms": issue_terms,
                "dominant_stance": dominant_stance,
                "stance_distribution": stance_distribution(stance_values),
                "concern_facets": concern_facets,
                "actor_hints": actor_hints,
                "evidence_citation_types": evidence_citation_types,
                "verifiability_posture": dominant_verification,
                "dispute_type": dominant_dispute,
                "controversy_summary": (
                    f"Cluster centers on {humanize_label(dominant_issue) or 'a public controversy'} "
                    f"with a dominant {humanize_label(dominant_stance) or 'unclear'} posture."
                ),
                "member_claim_ids": claim_ids,
                "member_count": len(group),
                "aggregate_source_signal_count": total_source_signal_count,
                "unique_source_signal_count": len(unique_signal_ids),
                "time_window": {
                    "start_utc": start_times[0] if start_times else "",
                    "end_utc": end_times[-1] if end_times else "",
                },
                "member_summaries": [truncate_text(item.get("summary") or item.get("statement"), 160) for item in group[:8]],
                "public_refs": unique_refs(refs, 12),
                "compact_audit": {
                    "representative": len(group) > 1,
                    "retained_count": min(len(group), 8),
                    "total_candidate_count": len(group),
                    "coverage_summary": (
                        f"Grouped {len(group)} claim candidates into one issue cluster "
                        f"centered on {humanize_label(dominant_issue) or 'a public controversy'}."
                    ),
                    "concentration_flags": ["singleton-cluster"] if len(group) == 1 else [],
                    "coverage_dimensions": ["issue-hint", "stance-hint", "concern-facets", "publication-time"],
                    "missing_dimensions": ["verification-route"] if dominant_verification != "empirical-observable" else [],
                    "sampling_notes": [],
                },
            }
        )
        if len(clusters) >= max(1, max_clusters):
            break
    wrapper = {
        "schema_version": "n2.1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "query_basis": {
            "input_path": str(input_file),
            "claim_type": wanted_claim_type,
            "keyword_any": keywords,
            "min_member_count": max(1, min_member_count),
            "max_clusters": max(1, max_clusters),
            "selection_mode": "group-claim-candidates-by-issue-stance-concern",
            "method": "controversy-issue-cluster-v1",
        },
        "input_path": str(input_file),
        "cluster_count": len(clusters),
        "clusters": clusters,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_claim_cluster_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        claim_cluster_path=output_file,
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)
    artifact_refs: list[dict[str, str]] = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.clusters",
            "artifact_ref": f"{output_file}:$.clusters",
        }
    ]
    for cluster in clusters:
        artifact_refs.extend(cluster["public_refs"])
    if not clusters:
        warnings.append({"code": "no-clusters", "message": "No claim clusters were produced from the available claim candidates."})
    gap_hints: list[str] = []
    if not clusters:
        gap_hints.append("No issue clusters are available for downstream controversy mapping.")
    elif singleton_count == len(clusters):
        gap_hints.append("Most issue clusters are still singletons; issue grouping may still be too fine-grained.")
    if verification_routing_count > 0:
        gap_hints.append(
            f"{verification_routing_count} clusters look non-empirical and should be routed before observation matching."
        )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "input_path": str(input_file),
            "output_path": str(output_file),
            "cluster_count": len(clusters),
            "input_candidate_count": len(candidates),
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "evidence-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "evbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [cluster["cluster_id"] for cluster in clusters],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "board_handoff": {
            "candidate_ids": [cluster["cluster_id"] for cluster in clusters],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": gap_hints,
            "challenge_hints": [
                "Check whether competing stances on the same issue were incorrectly merged into one cluster."
            ]
            if clusters
            else [],
            "suggested_next_skills": [
                "eco-derive-claim-scope",
                "eco-classify-claim-verifiability",
                "eco-route-verification-lane",
                "eco-post-board-note",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cluster claim candidates into board-ready claim groups.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--input-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--claim-type", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--min-member-count", type=int, default=1)
    parser.add_argument("--max-clusters", type=int, default=100)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = cluster_claim_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        input_path=args.input_path,
        output_path=args.output_path,
        claim_type=args.claim_type,
        keyword_any=args.keyword,
        min_member_count=args.min_member_count,
        max_clusters=args.max_clusters,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Cluster claim candidates into board-ready claim groups."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-cluster-claim-candidates"
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
        if wanted_claim_type and candidate_type != wanted_claim_type:
            continue
        text = maybe_text(candidate.get("statement") or candidate.get("summary"))
        if keywords and not any(keyword in text.casefold() for keyword in keywords):
            continue
        group_key = f"{candidate_type}|{semantic_fingerprint(text)}"
        groups.setdefault(group_key, []).append(candidate)
    ordered_groups = sorted(groups.values(), key=lambda items: (-len(items), maybe_text(items[0].get("claim_id"))))
    clusters: list[dict[str, Any]] = []
    singleton_count = 0
    for group in ordered_groups:
        if len(group) < max(1, min_member_count):
            continue
        if len(group) == 1:
            singleton_count += 1
        lead = group[0]
        statements = [maybe_text(item.get("statement") or item.get("summary")) for item in group if maybe_text(item.get("statement") or item.get("summary"))]
        label_source = max(statements, key=len) if statements else maybe_text(lead.get("summary"))
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
                "cluster_label": truncate_text(label_source, 160),
                "representative_statement": truncate_text(label_source, 320),
                "semantic_fingerprint": fingerprint,
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
                    "coverage_summary": f"Grouped {len(group)} claim candidates into one board-reviewable cluster.",
                    "concentration_flags": ["singleton-cluster"] if len(group) == 1 else [],
                    "coverage_dimensions": ["claim-type", "statement-semantics", "publication-time"],
                    "missing_dimensions": ["claim-scope"] if not bool((lead.get("claim_scope") or {}).get("usable_for_matching")) else [],
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
        "cluster_count": len(clusters),
        "clusters": clusters,
    }
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
        gap_hints.append("No claim clusters are available for downstream evidence linking.")
    elif singleton_count == len(clusters):
        gap_hints.append("Most claim clusters are still singletons; additional scope derivation or wider grouping rules may be needed.")
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
        },
        "receipt_id": "evidence-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "evbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:16],
        "artifact_refs": unique_refs(artifact_refs, 40),
        "canonical_ids": [cluster["cluster_id"] for cluster in clusters],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [cluster["cluster_id"] for cluster in clusters],
            "evidence_refs": unique_refs(artifact_refs, 20),
            "gap_hints": gap_hints,
            "challenge_hints": ["Check whether semantically close claims are still split across adjacent clusters."] if clusters else [],
            "suggested_next_skills": ["eco-link-claims-to-observations", "eco-build-normalization-audit"],
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
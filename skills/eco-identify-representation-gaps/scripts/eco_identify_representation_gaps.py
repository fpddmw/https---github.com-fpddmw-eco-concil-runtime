#!/usr/bin/env python3
"""Identify representation gaps from formal/public issue linkage artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-identify-representation-gaps"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_formal_public_link_context,
    sync_representation_gap_result_set,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, path_text: str, default_relative: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def normalized_ref(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        artifact_path = maybe_text(value.get("artifact_path"))
        record_locator = maybe_text(value.get("record_locator"))
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if not artifact_path and artifact_ref:
            marker = artifact_ref.find(":$")
            if marker >= 0:
                artifact_path = artifact_ref[:marker]
                if not record_locator:
                    record_locator = artifact_ref[marker + 1 :]
            else:
                artifact_path = artifact_ref
        if artifact_path and not artifact_ref:
            artifact_ref = (
                artifact_path
                if not record_locator
                else f"{artifact_path}:{record_locator}"
            )
        if not artifact_path:
            return {}
        return {
            "signal_id": maybe_text(value.get("signal_id")),
            "artifact_path": artifact_path,
            "record_locator": record_locator,
            "artifact_ref": artifact_ref or artifact_path,
        }
    text = maybe_text(value)
    if not text:
        return {}
    marker = text.find(":$")
    artifact_path = text[:marker] if marker >= 0 else text
    record_locator = text[marker + 1 :] if marker >= 0 else ""
    return {
        "signal_id": "",
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": text,
    }


def unique_artifact_refs(values: list[Any], limit: int = 20) -> list[dict[str, str]]:
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    for value in values:
        ref = normalized_ref(value)
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if not artifact_ref or artifact_ref in seen:
            continue
        seen.add(artifact_ref)
        results.append(ref)
        if len(results) >= limit:
            break
    return results


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def severity_from_score(score: float) -> str:
    if score >= 0.9:
        return "critical"
    if score >= 0.75:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def recommended_action(gap_type: str, issue_label: str) -> str:
    if gap_type == "formal-underrepresentation":
        return (
            f"Expand formal participation capture for {issue_label} and verify whether public concerns are missing from official comment channels."
        )
    if gap_type == "public-underrepresentation":
        return (
            f"Check whether {issue_label} remains confined to formal records and needs broader public-discourse interpretation or outreach."
        )
    if gap_type == "attention-imbalance":
        return (
            f"Compare why {issue_label} concentrates attention in one arena and whether the other arena is lagging by timing, framing, or access."
        )
    return (
        f"Revisit the current routing logic for {issue_label} and confirm whether the active participation pattern matches the expected verification lane."
    )


def gap_summary(
    gap_type: str,
    issue_label: str,
    formal_count: int,
    public_count: int,
    lane: str,
) -> str:
    return (
        f"Issue {issue_label} is flagged as {gap_type} with {formal_count} formal signals and "
        f"{public_count} public signals while the current lane remains {lane.replace('-', ' ')}."
    )


def representation_gap_specs(link: dict[str, Any]) -> list[dict[str, Any]]:
    link_status = maybe_text(link.get("link_status"))
    issue_label = maybe_text(link.get("issue_label")) or "public controversy"
    formal_count = safe_int(link.get("formal_signal_count"))
    public_count = safe_int(link.get("public_signal_count"))
    recommended_lane = maybe_text(link.get("recommended_lane")) or "mixed-review"
    specs: list[dict[str, Any]] = []

    if link_status == "public-only":
        score = 0.82 if public_count >= 2 else 0.74
        specs.append(
            {
                "gap_type": "formal-underrepresentation",
                "severity_score": score,
                "severity": severity_from_score(score),
                "recommended_action": recommended_action(
                    "formal-underrepresentation",
                    issue_label,
                ),
            }
        )
    elif link_status == "formal-only":
        score = 0.82 if formal_count >= 2 else 0.74
        specs.append(
            {
                "gap_type": "public-underrepresentation",
                "severity_score": score,
                "severity": severity_from_score(score),
                "recommended_action": recommended_action(
                    "public-underrepresentation",
                    issue_label,
                ),
            }
        )

    if formal_count > 0 and public_count > 0:
        ratio = max(formal_count, public_count) / max(1, min(formal_count, public_count))
        if ratio >= 3.0:
            score = min(0.87, 0.58 + min(ratio, 5.0) * 0.07)
            specs.append(
                {
                    "gap_type": "attention-imbalance",
                    "severity_score": round(score, 3),
                    "severity": severity_from_score(score),
                    "recommended_action": recommended_action(
                        "attention-imbalance",
                        issue_label,
                    ),
                }
            )

    if recommended_lane == "formal-comment-and-policy-record" and public_count > max(2, formal_count * 2):
        score = 0.72 if formal_count > 0 else 0.84
        specs.append(
            {
                "gap_type": "route-mismatch",
                "severity_score": score,
                "severity": severity_from_score(score),
                "recommended_action": recommended_action("route-mismatch", issue_label),
            }
        )
    if recommended_lane in {
        "public-discourse-analysis",
        "stakeholder-deliberation-analysis",
    } and formal_count > max(2, public_count * 2):
        score = 0.72 if public_count > 0 else 0.84
        specs.append(
            {
                "gap_type": "route-mismatch",
                "severity_score": score,
                "severity": severity_from_score(score),
                "recommended_action": recommended_action("route-mismatch", issue_label),
            }
        )
    return specs


def identify_representation_gaps_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    formal_public_links_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"analytics/representation_gaps_{round_id}.json",
    )
    link_context = load_formal_public_link_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        formal_public_links_path=formal_public_links_path,
    )
    warnings = (
        link_context.get("warnings", [])
        if isinstance(link_context.get("warnings"), list)
        else []
    )
    links = (
        link_context.get("links", [])
        if isinstance(link_context.get("links"), list)
        else []
    )

    gaps: list[dict[str, Any]] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        issue_label = maybe_text(link.get("issue_label")) or "public controversy"
        linkage_id = maybe_text(link.get("linkage_id"))
        formal_count = safe_int(link.get("formal_signal_count"))
        public_count = safe_int(link.get("public_signal_count"))
        recommended_lane = maybe_text(link.get("recommended_lane")) or "mixed-review"
        route_status = maybe_text(link.get("route_status")) or "mixed-routing-review"
        evidence_refs = unique_artifact_refs(
            link.get("evidence_refs", [])
            if isinstance(link.get("evidence_refs"), list)
            else [],
            limit=16,
        )
        for spec in representation_gap_specs(link):
            gap_type = maybe_text(spec.get("gap_type"))
            if not gap_type:
                continue
            gap_id = "gap-" + stable_hash(run_id, round_id, linkage_id, gap_type)[:12]
            gaps.append(
                {
                    "schema_version": "n3.0",
                    "gap_id": gap_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "linkage_id": linkage_id,
                    "issue_label": issue_label,
                    "gap_type": gap_type,
                    "severity": maybe_text(spec.get("severity")),
                    "severity_score": float(spec.get("severity_score") or 0.0),
                    "link_status": maybe_text(link.get("link_status")),
                    "recommended_lane": recommended_lane,
                    "route_status": route_status,
                    "formal_signal_count": formal_count,
                    "public_signal_count": public_count,
                    "cluster_ids": unique_texts(
                        link.get("cluster_ids", [])
                        if isinstance(link.get("cluster_ids"), list)
                        else []
                    ),
                    "claim_ids": unique_texts(
                        link.get("claim_ids", [])
                        if isinstance(link.get("claim_ids"), list)
                        else []
                    ),
                    "recommended_action": maybe_text(spec.get("recommended_action")),
                    "gap_summary": gap_summary(
                        gap_type,
                        issue_label,
                        formal_count,
                        public_count,
                        recommended_lane,
                    ),
                    "evidence_refs": evidence_refs,
                }
            )

    gap_type_counts: dict[str, int] = {}
    severity_counts: dict[str, int] = {}
    for gap in gaps:
        gap_type = maybe_text(gap.get("gap_type"))
        severity = maybe_text(gap.get("severity"))
        if gap_type:
            gap_type_counts[gap_type] = gap_type_counts.get(gap_type, 0) + 1
        if severity:
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "formal_public_links_path": maybe_text(
                link_context.get("formal_public_links_file")
            ),
            "formal_public_links_source": maybe_text(
                link_context.get("formal_public_link_source")
            ),
            "selection_mode": "identify-representation-gaps-from-linkages",
            "method": "representation-gap-classifier-v1",
        },
        "formal_public_links_path": maybe_text(
            link_context.get("formal_public_links_file")
        ),
        "formal_public_links_source": maybe_text(
            link_context.get("formal_public_link_source")
        ),
        "observed_inputs": {
            "formal_public_links_present": bool(
                maybe_text(link_context.get("formal_public_link_source"))
                and not maybe_text(link_context.get("formal_public_link_source")).startswith(
                    "missing-"
                )
            ),
            "formal_public_links_artifact_present": bool(
                link_context.get("formal_public_links_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "formal_public_link": link_context.get("analysis_sync", {}),
        },
        "gap_count": len(gaps),
        "gap_type_counts": gap_type_counts,
        "severity_counts": severity_counts,
        "gaps": gaps,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_representation_gap_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        representation_gap_path=output_file,
        db_path=maybe_text(link_context.get("db_path")),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.gaps",
            "artifact_ref": f"{output_file}:$.gaps",
        }
    ]
    for gap in gaps[:8]:
        gap_id = maybe_text(gap.get("gap_id"))
        if not gap_id:
            continue
        artifact_refs.append(
            {
                "signal_id": "",
                "artifact_path": str(output_file),
                "record_locator": "$.gaps[?(@.gap_id=='" + gap_id + "')]",
                "artifact_ref": f"{output_file}:gap:{gap_id}",
            }
        )

    if not gaps:
        warnings.append(
            {
                "code": "no-representation-gaps",
                "message": "No representation-gap objects were produced from the available formal/public linkages.",
            }
        )

    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "gap_count": len(gaps),
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "representationgap-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "representationgapbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [
            maybe_text(gap.get("gap_id"))
            for gap in gaps
            if maybe_text(gap.get("gap_id"))
        ],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [
                maybe_text(gap.get("gap_id"))
                for gap in gaps
                if maybe_text(gap.get("gap_id"))
            ],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": unique_texts(
                [
                    maybe_text(gap.get("gap_summary"))
                    for gap in gaps
                    if maybe_text(gap.get("gap_summary"))
                ]
            )[:5],
            "challenge_hints": (
                ["Prioritize high-severity representation gaps before treating the issue map as complete."]
                if any(maybe_text(gap.get("severity")) in {"critical", "high"} for gap in gaps)
                else []
            ),
            "suggested_next_skills": [
                "eco-propose-next-actions",
                "eco-open-falsification-probe",
                "eco-post-board-note",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Identify representation gaps from formal/public issue linkage artifacts."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--formal-public-links-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = identify_representation_gaps_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        formal_public_links_path=args.formal_public_links_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Open falsification probes from the next-action queue."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-open-falsification-probe"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.investigation_planning import (  # noqa: E402
    d1_contract_fields_from_payload,
    load_next_actions_wrapper,
    load_ranked_actions_context,
    maybe_text,
    resolve_path,
    unique_texts,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_falsification_probe_records,
    store_falsification_probe_snapshot,
)


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


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def requested_skills_for_action(action: dict[str, Any]) -> list[str]:
    action_kind = maybe_text(action.get("action_kind"))
    assigned_role = maybe_text(action.get("assigned_role"))
    suggestions: list[str] = []
    if action_kind in {"resolve-challenge", "resolve-contradiction"}:
        suggestions.extend(["eco-post-board-note", "eco-close-challenge-ticket"])
    if action_kind == "clarify-verification-route":
        suggestions.extend(["eco-route-verification-lane", "eco-post-board-note"])
    if action_kind == "advance-empirical-verification":
        suggestions.extend(
            [
                "eco-link-claims-to-observations",
                "eco-score-evidence-coverage",
                "eco-post-board-note",
            ]
        )
    if action_kind == "review-formal-record":
        suggestions.extend(
            [
                "eco-link-formal-comments-to-public-discourse",
                "eco-post-board-note",
            ]
        )
    if action_kind in {
        "analyze-public-discourse",
        "analyze-stakeholder-deliberation",
        "review-formal-public-linkage",
    }:
        suggestions.extend(
            [
                "eco-link-formal-comments-to-public-discourse",
                "eco-identify-representation-gaps",
                "eco-post-board-note",
            ]
        )
    if action_kind == "address-representation-gap":
        suggestions.extend(
            [
                "eco-identify-representation-gaps",
                "eco-link-formal-comments-to-public-discourse",
                "eco-post-board-note",
            ]
        )
    if action_kind == "trace-cross-platform-diffusion":
        suggestions.extend(["eco-detect-cross-platform-diffusion", "eco-post-board-note"])
    if action_kind in {"resolve-contradiction", "expand-coverage"}:
        suggestions.extend(["eco-query-environment-signals", "eco-query-public-signals"])
    if action_kind == "classify-verifiability":
        suggestions.extend(["eco-post-board-note", "eco-query-public-signals"])
    if action_kind == "stabilize-hypothesis":
        suggestions.extend(["eco-update-hypothesis-status", "eco-post-board-note"])
    if assigned_role == "environmentalist":
        suggestions.append("eco-query-environment-signals")
    if assigned_role == "sociologist":
        suggestions.append("eco-query-public-signals")
    return unique_texts(suggestions)


def probe_type_for_action(action: dict[str, Any]) -> str:
    action_kind = maybe_text(action.get("action_kind"))
    controversy_gap = maybe_text(action.get("controversy_gap"))
    if action_kind in {"classify-verifiability", "clarify-verification-route"} or controversy_gap == "verification-routing-gap":
        return "routing-probe"
    if action_kind in {"resolve-contradiction", "advance-empirical-verification"} and controversy_gap == "formal-public-misalignment":
        return "misalignment-probe"
    if controversy_gap == "unresolved-contestation":
        return "contestation-probe"
    if controversy_gap == "issue-structure-gap":
        return "issue-structure-probe"
    if action_kind in {"expand-coverage", "advance-empirical-verification"}:
        return "coverage-probe"
    if action_kind in {"review-formal-record", "review-formal-public-linkage"} or controversy_gap in {
        "formal-record-gap",
        "formal-public-linkage-gap",
    }:
        return "formal-record-probe"
    if action_kind in {
        "analyze-public-discourse",
        "analyze-stakeholder-deliberation",
        "address-representation-gap",
    } or controversy_gap in {
        "public-discourse-gap",
        "stakeholder-deliberation-gap",
        "representation-gap",
    }:
        return "representation-probe"
    if action_kind == "trace-cross-platform-diffusion" or controversy_gap == "cross-platform-diffusion":
        return "diffusion-probe"
    return "uncertainty-probe"


def falsification_question_for_action(action: dict[str, Any], objective: str) -> str:
    probe_type = probe_type_for_action(action)
    if probe_type == "routing-probe":
        return (
            f"What evidence would show that this issue should not be routed into external verification yet: {objective}"
        )
    if probe_type == "misalignment-probe":
        return (
            f"What evidence would show the apparent mismatch between public discourse and available signals is overstated: {objective}"
        )
    if probe_type == "formal-record-probe":
        return (
            f"What evidence would show the current formal-record interpretation is incomplete, misframed, or not the right lane: {objective}"
        )
    if probe_type == "representation-probe":
        return (
            f"What evidence would show the current representation or discourse gap is overstated, misdiagnosed, or mis-scoped: {objective}"
        )
    if probe_type == "diffusion-probe":
        return (
            f"What evidence would show the inferred cross-platform diffusion path is misleading or not materially relevant: {objective}"
        )
    if probe_type == "issue-structure-probe":
        return (
            f"What evidence would show the current issue framing should be split, merged, or retired: {objective}"
        )
    if probe_type == "coverage-probe":
        return f"What missing evidence would most likely overturn the current weak support picture: {objective}"
    return f"What evidence would materially weaken: {objective}"


def success_criteria_for_action(action: dict[str, Any]) -> list[str]:
    probe_type = probe_type_for_action(action)
    if probe_type == "routing-probe":
        return [
            "Classify the target as empirical, procedural, representational, or mixed.",
            "Explain which lane should be used next and why direct matching is or is not justified.",
        ]
    if probe_type == "misalignment-probe":
        return [
            "Collect at least one comparison between public narrative and available signals.",
            "Explain whether the contradiction is substantive, partial, or only apparent.",
        ]
    if probe_type == "formal-record-probe":
        return [
            "Identify which formal record elements are actually present versus merely assumed.",
            "Explain whether the issue should stay in formal review or be rerouted.",
        ]
    if probe_type == "representation-probe":
        return [
            "Identify which voices, platforms, or discourse positions are over- or underrepresented.",
            "Explain whether the representation diagnosis should change, narrow, or escalate.",
        ]
    if probe_type == "diffusion-probe":
        return [
            "Confirm whether the inferred diffusion path survives timestamp and platform checks.",
            "Explain whether the diffusion pattern materially changes how the issue should be interpreted.",
        ]
    if probe_type == "issue-structure-probe":
        return [
            "Clarify whether the current hypothesis is too broad, too narrow, or misplaced.",
            "Record whether the board should split or consolidate the active issue framing.",
        ]
    return [
        "Collect at least one new observation or contradiction-aware note.",
        "Explain whether the target remains credible after focused challenge review.",
    ]


def disconfirm_signals_for_action(action: dict[str, Any]) -> list[str]:
    probe_type = probe_type_for_action(action)
    if probe_type == "routing-probe":
        return [
            "The target turns out to be primarily procedural, normative, or representational rather than empirical.",
            "No place-sensitive claim remains after tightening the issue framing.",
        ]
    if probe_type == "misalignment-probe":
        return [
            "The supposed contradiction is explained by timing, scope, or framing mismatch rather than by conflicting evidence.",
            "Public narrative and available signals converge after comparison.",
        ]
    if probe_type == "formal-record-probe":
        return [
            "The missing formal-record concern is resolved by evidence already present in the docket or policy record.",
            "The issue turns out to belong in public discourse or mixed review rather than in formal record review.",
        ]
    if probe_type == "representation-probe":
        return [
            "The apparent representation imbalance is explained by sampling, platform mix, or issue scoping rather than by a substantive gap.",
            "The issue is adequately represented once signals are regrouped at the right issue granularity.",
        ]
    if probe_type == "diffusion-probe":
        return [
            "The inferred diffusion path disappears after checking timestamps, aliases, or issue assignment.",
            "Cross-platform spread exists but is not material to the current controversy decision.",
        ]
    if probe_type == "issue-structure-probe":
        return [
            "The current hypothesis collapses multiple distinct issues into one board item.",
            "The target should be reframed as representation, process, or value conflict instead of verification.",
        ]
    return [
        "A contradiction-leaning evidence link becomes more plausible than the current support path.",
        "The target can no longer be defended with matching-ready evidence scope.",
    ]


def probe_candidates(actions: list[dict[str, Any]], action_id: str) -> list[dict[str, Any]]:
    filtered = [action for action in actions if isinstance(action, dict)]
    if maybe_text(action_id):
        filtered = [
            action
            for action in filtered
            if maybe_text(action.get("action_id")) == maybe_text(action_id)
        ]
    return [
        action
        for action in filtered
        if bool(action.get("probe_candidate"))
        or maybe_text(action.get("action_kind"))
        in {
            "resolve-challenge",
            "resolve-contradiction",
            "stabilize-hypothesis",
            "clarify-verification-route",
            "advance-empirical-verification",
            "review-formal-record",
            "review-formal-public-linkage",
            "address-representation-gap",
        }
    ]


def build_probe(
    action: dict[str, Any],
    *,
    default_run_id: str,
    default_round_id: str,
) -> dict[str, Any]:
    target = action.get("target", {}) if isinstance(action.get("target"), dict) else {}
    action_id = maybe_text(action.get("action_id"))
    hypothesis_id = maybe_text(target.get("hypothesis_id"))
    claim_id = maybe_text(target.get("claim_id"))
    ticket_id = maybe_text(target.get("ticket_id"))
    probe_id = "probe-" + stable_hash(SKILL_NAME, action_id, hypothesis_id, claim_id, ticket_id)[:12]
    objective = (
        maybe_text(action.get("objective"))
        or maybe_text(action.get("reason"))
        or "Probe the current contradiction or uncertainty."
    )
    probe_type = probe_type_for_action(action)
    return {
        "probe_id": probe_id,
        "run_id": maybe_text(action.get("run_id")) or maybe_text(default_run_id),
        "round_id": maybe_text(action.get("round_id")) or maybe_text(default_round_id),
        "opened_at_utc": utc_now_iso(),
        "probe_status": "open",
        "action_id": action_id,
        "priority": maybe_text(action.get("priority")) or "high",
        "owner_role": maybe_text(action.get("assigned_role")) or "challenger",
        "target_hypothesis_id": hypothesis_id,
        "target_claim_id": claim_id,
        "target_ticket_id": ticket_id,
        "probe_type": probe_type,
        "controversy_gap": maybe_text(action.get("controversy_gap")),
        "recommended_lane": maybe_text(action.get("recommended_lane")),
        "probe_goal": objective,
        "falsification_question": falsification_question_for_action(action, objective),
        "success_criteria": success_criteria_for_action(action),
        "disconfirm_signals": disconfirm_signals_for_action(action),
        "requested_skills": requested_skills_for_action(action),
        "evidence_refs": unique_texts(
            action.get("evidence_refs", [])
            if isinstance(action.get("evidence_refs"), list)
            else []
        ),
        "source_ids": unique_texts(
            action.get("source_ids", [])
            if isinstance(action.get("source_ids"), list)
            else []
        ),
    }


def open_falsification_probe_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    next_actions_path: str,
    board_summary_path: str,
    board_brief_path: str,
    coverage_path: str,
    output_path: str,
    action_id: str,
    max_probes: int,
    max_actions: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    next_actions_file = resolve_path(
        run_dir_path,
        next_actions_path,
        f"investigation/next_actions_{round_id}.json",
    )
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"investigation/falsification_probes_{round_id}.json",
    )

    warnings: list[dict[str, Any]] = []
    next_actions_context = load_next_actions_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        next_actions_path=next_actions_path,
    )
    next_actions_wrapper = (
        next_actions_context.get("payload")
        if isinstance(next_actions_context.get("payload"), dict)
        else None
    )
    next_actions_artifact_present = bool(next_actions_context.get("artifact_present"))
    action_source = "next-actions-artifact"
    contract_fields = d1_contract_fields_from_payload(
        None,
        observed_inputs_overrides={
            "next_actions_artifact_present": next_actions_artifact_present,
            "next_actions_present": bool(next_actions_context.get("payload_present")),
        },
    )
    if isinstance(next_actions_wrapper, dict):
        ranked_actions = (
            next_actions_wrapper.get("ranked_actions", [])
            if isinstance(next_actions_wrapper.get("ranked_actions"), list)
            else []
        )
        action_source = (
            maybe_text(next_actions_wrapper.get("action_source"))
            or maybe_text(next_actions_context.get("source"))
            or "next-actions-artifact"
        )
        contract_fields = d1_contract_fields_from_payload(
            next_actions_wrapper,
            observed_inputs_overrides={
                "next_actions_artifact_present": next_actions_artifact_present,
                "next_actions_present": True,
            },
        )
    else:
        warnings.append(
            {
                "code": "missing-next-actions",
                "message": f"No next-actions artifact or DB snapshot was found for {next_actions_file}. Rebuilding action context from deliberation state.",
            }
        )
        action_context = load_ranked_actions_context(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
            board_summary_path=board_summary_path,
            board_brief_path=board_brief_path,
            coverage_path=coverage_path,
            max_actions=max_actions,
        )
        ranked_actions = (
            action_context.get("ranked_actions", [])
            if isinstance(action_context.get("ranked_actions"), list)
            else []
        )
        context_warnings = (
            action_context.get("warnings", [])
            if isinstance(action_context.get("warnings"), list)
            else []
        )
        warnings.extend(context_warnings)
        action_source = "derived-from-deliberation"
        contract_fields = d1_contract_fields_from_payload(
            action_context,
            observed_inputs_overrides={
                "next_actions_artifact_present": next_actions_artifact_present,
                "next_actions_present": False,
            },
        )

    candidates = probe_candidates(ranked_actions, action_id)[: max(1, max_probes)]
    probes = [
        build_probe(
            action,
            default_run_id=run_id,
            default_round_id=round_id,
        )
        for action in candidates
    ]

    wrapper = {
        "schema_version": "d1.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "next_actions_path": str(next_actions_file),
        "action_source": action_source,
        **contract_fields,
        "probe_count": len(probes),
        "probes": probes,
    }
    wrapper = store_falsification_probe_records(
        run_dir_path,
        probe_snapshot=wrapper,
        artifact_path=str(output_file),
    )
    store_falsification_probe_snapshot(
        run_dir_path,
        probe_snapshot=wrapper,
        artifact_path=str(output_file),
    )
    write_json_file(output_file, wrapper)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.probes",
            "artifact_ref": f"{output_file}:$.probes",
        }
    ]
    if not probes:
        warnings.append({"code": "no-probes", "message": "No probe-worthy next actions were found."})
    canonical_ids = [
        maybe_text(probe.get("probe_id"))
        for probe in probes
        if isinstance(probe, dict) and maybe_text(probe.get("probe_id"))
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "probe_count": len(probes),
            "action_source": action_source,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "investigation-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "investigationbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": artifact_refs,
            "gap_hints": []
            if probes
            else ["No controversy probes are open for this round yet."],
            "challenge_hints": [
                "Open routing, contradiction, or issue-structure probes should be reviewed before marking the round fully ready."
            ]
            if probes
            else [],
            "suggested_next_skills": [
                "eco-summarize-round-readiness",
                "eco-post-board-note",
                "eco-update-hypothesis-status",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Open falsification probes from the next-action queue."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--action-id", default="")
    parser.add_argument("--max-probes", type=int, default=3)
    parser.add_argument("--max-actions", type=int, default=6)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_falsification_probe_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        next_actions_path=args.next_actions_path,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        coverage_path=args.coverage_path,
        output_path=args.output_path,
        action_id=args.action_id,
        max_probes=args.max_probes,
        max_actions=args.max_actions,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

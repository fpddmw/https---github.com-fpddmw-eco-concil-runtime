from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


FETCH_SKILLS = {
    "fetch-airnow-hourly-observations",
    "fetch-bluesky-cascade",
    "fetch-gdelt-doc-search",
    "fetch-gdelt-events",
    "fetch-gdelt-gkg",
    "fetch-gdelt-mentions",
    "fetch-nasa-firms-fire",
    "fetch-open-meteo-air-quality",
    "fetch-open-meteo-flood",
    "fetch-open-meteo-historical",
    "fetch-openaq",
    "fetch-regulationsgov-comment-detail",
    "fetch-regulationsgov-comments",
    "fetch-usgs-water-iv",
    "fetch-youtube-comments",
    "fetch-youtube-video-search",
}

OPTIONAL_ANALYSIS_SKILLS = {
    "build-normalization-audit",
    "extract-claim-candidates",
    "cluster-claim-candidates",
    "derive-claim-scope",
    "classify-claim-verifiability",
    "route-verification-lane",
    "extract-issue-candidates",
    "cluster-issue-candidates",
    "extract-stance-candidates",
    "extract-concern-facets",
    "extract-actor-profiles",
    "extract-evidence-citation-types",
    "materialize-controversy-map",
    "extract-observation-candidates",
    "merge-observation-candidates",
    "derive-observation-scope",
    "link-claims-to-observations",
    "score-evidence-coverage",
    "link-formal-comments-to-public-discourse",
    "identify-representation-gaps",
    "detect-cross-platform-diffusion",
    "plan-round-orchestration",
    "propose-next-actions",
    "open-falsification-probe",
    "summarize-round-readiness",
}

OPTIONAL_ANALYSIS_NOTES = {
    "plan-round-orchestration": (
        "Moderator-only advisory planner. It can be run only through an approved "
        "skill-approval request and never owns the controller plan."
    ),
    "propose-next-actions": (
        "Moderator-only optional advisory for investigation suggestions. It does "
        "not define a default phase owner or required next-action queue."
    ),
    "summarize-round-readiness": (
        "Moderator-only optional advisory for compiling readiness evidence. Formal "
        "phase movement still requires a transition request and operator approval."
    ),
    "build-normalization-audit": (
        "Operator QA export for signal-plane ingestion checks. It is not a "
        "board-facing moderation step and requires an approval record."
    ),
    "link-claims-to-observations": (
        "Legacy empirical-link helper. It is approval-gated and must not be used as "
        "the default basis for policy research reports."
    ),
    "score-evidence-coverage": (
        "Approval-gated empirical evidence sufficiency helper for explicitly routed "
        "observation lanes, not a global readiness gate."
    ),
}

STATE_TRANSITION_PROFILES = {
    "open-investigation-round": {
        "stage": "transition",
        "queue_role": "round-transition-request-consumer",
        "default_invocation": "approved-transition-request",
        "notes": (
            "Consumes an already approved transition request to open a governed "
            "follow-up round."
        ),
    },
    "promote-evidence-basis": {
        "stage": "transition",
        "queue_role": "evidence-basis-freeze",
        "default_invocation": "approved-transition-request",
        "notes": (
            "Freezes DB-backed evidence basis after moderator request and operator "
            "approval; it does not decide the research conclusion by itself."
        ),
    },
    "scaffold-mission-run": {
        "stage": "ingress",
        "queue_role": "run-bootstrap",
        "default_invocation": "moderator-or-operator-triggered",
        "notes": "Bootstrap a run and first round without selecting a domain analysis chain.",
    },
    "prepare-round": {
        "stage": "source-selection",
        "queue_role": "capability-check",
        "default_invocation": "moderator-or-operator-triggered",
        "notes": "Prepare source capabilities and governance checks without deciding research method.",
    },
}

BRIDGE_PROFILES = {
    "scaffold-mission-run",
    "prepare-round",
    "normalize-fetch-execution",
}

DELIBERATION_WRITE_SKILLS = {
    "post-board-note": "human-readable-note",
    "update-hypothesis-status": "evidence-backed-hypothesis-update",
    "open-challenge-ticket": "challenge-write",
    "close-challenge-ticket": "challenge-write",
    "claim-board-task": "task-write",
    "submit-council-proposal": "proposal-write",
    "submit-readiness-opinion": "readiness-opinion-write",
    "summarize-board-state": "derived-board-export",
    "materialize-board-brief": "human-handoff-export",
}

REPORTING_SKILLS = {
    "materialize-reporting-handoff",
    "draft-council-decision",
    "draft-expert-report",
    "publish-expert-report",
    "publish-council-decision",
    "materialize-final-publication",
}

RUNTIME_ARCHIVE_SKILLS = {
    "archive-signal-corpus",
    "archive-case-library",
    "materialize-history-context",
}


def _profile(
    *,
    queue_status: str,
    stage: str,
    queue_role: str,
    default_invocation: str,
    notes: str,
    requires_explicit_approval: bool = False,
    default_chain_eligible: bool = False,
) -> dict[str, object]:
    if queue_status == "bridge":
        phase2_behavior = "governed-bridge"
    elif requires_explicit_approval:
        phase2_behavior = "approval-gated-runtime-surface"
    elif queue_status == "advisory":
        phase2_behavior = "on-demand-runtime-surface"
    else:
        phase2_behavior = "capability-runtime-surface"
    return {
        "source_queue_ready": True,
        "queue_status": queue_status,
        "stage": stage,
        "queue_role": queue_role,
        "default_invocation": default_invocation,
        "phase2_behavior": phase2_behavior,
        "default_chain_eligible": bool(default_chain_eligible),
        "requires_explicit_approval": bool(requires_explicit_approval),
        # Kept as an empty compatibility field so registry consumers no longer
        # receive implied claim-route-coverage chains from source queue metadata.
        "downstream_hints": [],
        "notes": notes,
    }


def _optional_analysis_profile(skill_name: str) -> dict[str, object]:
    notes = OPTIONAL_ANALYSIS_NOTES.get(
        skill_name,
        (
            "Approval-gated optional-analysis capability. It can support human "
            "audit or agent investigation, but it is not part of a default "
            "runtime-owned investigation chain."
        ),
    )
    if skill_name == "open-falsification-probe":
        queue_role = "challenge-probe-helper"
    elif skill_name in {"plan-round-orchestration", "propose-next-actions", "summarize-round-readiness"}:
        queue_role = "moderator-advisory-helper"
    else:
        queue_role = "audited-derived-analysis"
    return _profile(
        queue_status="advisory",
        stage="optional-analysis",
        queue_role=queue_role,
        default_invocation="operator-approved-on-demand",
        notes=notes,
        requires_explicit_approval=True,
    )


def source_queue_profile(skill_name: str) -> dict[str, object]:
    if skill_name == "normalize-fetch-execution":
        return _profile(
            queue_status="bridge",
            stage="fetch-normalize-bridge",
            queue_role="execution-receipt",
            default_invocation="investigator-or-operator-triggered",
            notes=(
                "Import or execute approved fetch outputs and write signal-plane receipts; "
                "it must not select downstream analysis conclusions."
            ),
        )

    if skill_name in STATE_TRANSITION_PROFILES:
        data = STATE_TRANSITION_PROFILES[skill_name]
        return _profile(
            queue_status="bridge" if skill_name in BRIDGE_PROFILES else "transition",
            stage=str(data["stage"]),
            queue_role=str(data["queue_role"]),
            default_invocation=str(data["default_invocation"]),
            notes=str(data["notes"]),
            requires_explicit_approval=skill_name
            in {"open-investigation-round", "promote-evidence-basis"},
        )

    if skill_name in OPTIONAL_ANALYSIS_SKILLS:
        return _optional_analysis_profile(skill_name)

    if skill_name in FETCH_SKILLS:
        return _profile(
            queue_status="capability",
            stage="fetch",
            queue_role="raw-artifact-fetch",
            default_invocation="investigator-triggered",
            notes=(
                "Fetch capability for raw source collection. It writes raw artifacts "
                "or receipts and carries no default investigation judgement."
            ),
        )

    if skill_name.startswith("normalize-"):
        return _profile(
            queue_status="capability",
            stage="normalize",
            queue_role="signal-normalizer",
            default_invocation="investigator-triggered",
            notes=(
                "Normalize raw artifacts into signal-plane rows with provenance; "
                "normalization must not emit board or policy conclusions."
            ),
        )

    if skill_name in DELIBERATION_WRITE_SKILLS:
        return _profile(
            queue_status="capability",
            stage="deliberation-write",
            queue_role=DELIBERATION_WRITE_SKILLS[skill_name],
            default_invocation="role-triggered",
            notes="DB-native council write surface; execution is driven by role authority, not by source queue ordering.",
        )

    if skill_name == "query-board-delta":
        return _profile(
            queue_status="advisory",
            stage="query",
            queue_role="board-read",
            default_invocation="on-demand",
            notes="Read-only deliberation query surface for role context and replay.",
        )

    if skill_name.startswith("query-") or skill_name.startswith("lookup-"):
        return _profile(
            queue_status="advisory",
            stage="query",
            queue_role="db-query",
            default_invocation="on-demand",
            notes="Read-only query capability. It exposes DB evidence surfaces without implying an analysis route.",
        )

    if skill_name in REPORTING_SKILLS or skill_name.startswith("draft-") or skill_name.startswith("publish-"):
        return _profile(
            queue_status="capability",
            stage="reporting",
            queue_role="reporting-surface",
            default_invocation="role-triggered-or-operator-approved",
            notes=(
                "Reporting capability that should consume DB-backed evidence basis "
                "or reporting objects, with approval where the skill policy requires it."
            ),
            requires_explicit_approval=skill_name
            in {
                "materialize-reporting-handoff",
                "draft-council-decision",
                "publish-expert-report",
                "publish-council-decision",
                "materialize-final-publication",
            },
        )

    if skill_name in RUNTIME_ARCHIVE_SKILLS:
        return _profile(
            queue_status="capability",
            stage="archive",
            queue_role="operator-archive",
            default_invocation="post-round-operator",
            notes="Operator-owned archive or retrieval capability; not a phase-2 investigation step.",
        )

    return _profile(
        queue_status="advisory",
        stage="auxiliary",
        queue_role="manual-review",
        default_invocation="on-demand",
        notes="No default source-queue role is defined; expose only as an operator-triggered capability.",
    )


def source_queue_profile_summary(skill_entries: Iterable[dict[str, object]]) -> dict[str, object]:
    queue_status_counts: Counter[str] = Counter()
    stage_counts: Counter[str] = Counter()
    queue_role_counts: Counter[str] = Counter()
    phase2_behavior_counts: Counter[str] = Counter()
    skill_count = 0
    source_queue_ready_count = 0

    for entry in skill_entries:
        profile = entry.get("source_queue_profile") if isinstance(entry.get("source_queue_profile"), dict) else source_queue_profile(str(entry.get("skill_name", "")))
        queue_status = str(profile.get("queue_status", ""))
        stage = str(profile.get("stage", ""))
        queue_role = str(profile.get("queue_role", ""))
        phase2_behavior = str(profile.get("phase2_behavior", ""))
        skill_count += 1
        if profile.get("source_queue_ready") is True:
            source_queue_ready_count += 1
        if queue_status:
            queue_status_counts[queue_status] += 1
        if stage:
            stage_counts[stage] += 1
        if queue_role:
            queue_role_counts[queue_role] += 1
        if phase2_behavior:
            phase2_behavior_counts[phase2_behavior] += 1

    return {
        "skill_count": skill_count,
        "source_queue_ready_count": source_queue_ready_count,
        "queue_status_counts": dict(sorted(queue_status_counts.items())),
        "stage_counts": dict(sorted(stage_counts.items())),
        "queue_role_counts": dict(sorted(queue_role_counts.items())),
        "phase2_behavior_counts": dict(sorted(phase2_behavior_counts.items())),
    }


__all__ = ["source_queue_profile", "source_queue_profile_summary"]

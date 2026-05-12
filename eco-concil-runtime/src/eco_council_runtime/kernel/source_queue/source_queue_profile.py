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
    "aggregate-environment-evidence",
    "review-fact-check-evidence-scope",
    "review-evidence-sufficiency",
    "discover-discourse-issues",
    "suggest-evidence-lanes",
    "materialize-research-issue-surface",
    "project-research-issue-views",
    "export-research-issue-map",
    "apply-approved-formal-public-taxonomy",
    "compare-formal-public-footprints",
    "identify-representation-audit-cues",
    "detect-temporal-cooccurrence-cues",
    "review-spatiotemporal-relation-alternatives",
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
    "aggregate-environment-evidence": (
        "Approval-gated DB-backed environment aggregation helper. It summarizes "
        "source, metric, spatial, and temporal coverage without claim matching."
    ),
    "review-fact-check-evidence-scope": (
        "Approval-gated explicit scope review helper. It requires question, place, "
        "period, evidence window, lag assumptions, metric, and source requirements."
    ),
    "review-evidence-sufficiency": (
        "Approval-gated evidence sufficiency review helper. It emits notes and "
        "caveats only, and it is not a readiness score or phase gate."
    ),
    "discover-discourse-issues": (
        "Approval-gated discourse discovery helper. It emits reversible issue hints, "
        "not claims or report conclusions."
    ),
    "suggest-evidence-lanes": (
        "Approval-gated advisory lane tag helper. It must not assign owners, drive "
        "the source queue, or advance phases."
    ),
    "apply-approved-formal-public-taxonomy": (
        "Approval-gated taxonomy helper. It requires a mission-scoped approved "
        "taxonomy reference and emits candidate labels only."
    ),
    "compare-formal-public-footprints": (
        "Approval-gated formal/public footprint helper. It describes overlap and "
        "absence cues without link or alignment scoring."
    ),
    "identify-representation-audit-cues": (
        "Approval-gated representation audit cue helper. It emits human-review "
        "prompts, not representation findings."
    ),
    "detect-temporal-cooccurrence-cues": (
        "Approval-gated temporal co-occurrence helper. It is descriptive only and "
        "does not infer influence, causality, spread, or direction."
    ),
    "review-spatiotemporal-relation-alternatives": (
        "Approval-gated relation challenger helper. It emits objection candidates "
        "only and must be carried by challenge, probe, or review comment."
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
    "freeze-report-basis": {
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
        "default_invocation": "moderator-triggered",
        "notes": "Bootstrap a run and first round without selecting a domain analysis chain.",
    },
    "prepare-round": {
        "stage": "source-selection",
        "queue_role": "capability-check",
        "default_invocation": "moderator-triggered",
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
    "submit-investigation-plan": "coordination-plan-write",
    "submit-investigation-scope": "coordination-scope-write",
    "submit-round-brief": "coordination-brief-write",
    "materialize-context-packet": "coordination-context-packet-write",
    "submit-evidence-request": "coordination-evidence-request-write",
    "submit-agent-position": "coordination-agent-position-write",
    "submit-challenge-disposition": "coordination-challenge-disposition-write",
    "summarize-board-state": "derived-board-export",
    "materialize-board-brief": "human-handoff-export",
}

REPORTING_SKILLS = {
    "materialize-reporting-handoff",
    "materialize-spatiotemporal-relation-evidence-packet",
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

SOURCE_FAMILY_WORKFLOWS: list[dict[str, object]] = [
    {
        "family_id": "gdelt-public-record",
        "label": "GDELT public record workflow",
        "semantics": (
            "Agent-owned source-family workflow. DOC search is useful for topical "
            "reconnaissance and article lists; Events, Mentions, and GKG exports "
            "are the row-level follow-up surfaces for shared UTC windows. This is "
            "not a source ranking or runtime-owned agenda."
        ),
        "workflow_steps": [
            {
                "step_id": "doc-recon",
                "role": "topical/domain reconnaissance or article-list discovery",
                "skill_names": ["fetch-gdelt-doc-search"],
                "output": "DOC article/timeline artifacts",
                "followup_when": (
                    "Use follow-up table pulls when the question needs fuller row "
                    "coverage, actor/source context, mention context, or recovery "
                    "from query-sensitive DOC results."
                ),
            },
            {
                "step_id": "three-table-window",
                "role": "row-level public record pulls over an agent-chosen UTC window",
                "skill_names": [
                    "fetch-gdelt-events",
                    "fetch-gdelt-mentions",
                    "fetch-gdelt-gkg",
                ],
                "output": "GDELT export manifests and zip artifacts",
                "followup_when": (
                    "Use after DOC recon, after a known event window is scoped, or "
                    "when DOC query syntax/caps make article search insufficient."
                ),
            },
        ],
        "normalizer_skills": [
            "normalize-gdelt-doc-public-signals",
            "normalize-gdelt-events-public-signals",
            "normalize-gdelt-mentions-public-signals",
            "normalize-gdelt-gkg-public-signals",
        ],
        "attempt_review_questions": [
            "Was the query syntax accepted by the provider, or should it be linted/rephrased?",
            "Was the returned artifact a narrow DOC result that should trigger table-window follow-up?",
            "Should the agent revise terms, broaden/narrow the window, or switch to Events/Mentions/GKG?",
            "If a DOC search returned zero rows, did the agent avoid treating that as absence of Events/Mentions/GKG or official records?",
        ],
    },
    {
        "family_id": "youtube-public-discourse",
        "label": "YouTube public discourse workflow",
        "semantics": (
            "Agent-owned discovery-to-comments workflow. Video search finds "
            "candidate videos; comment fetch tests public response language for "
            "agent-selected videos."
        ),
        "workflow_steps": [
            {
                "step_id": "video-discovery",
                "role": "candidate video discovery from agent-authored topical queries",
                "skill_names": ["fetch-youtube-video-search"],
                "output": "video IDs and metadata artifacts",
                "followup_when": "Use comment fetch when public discourse evidence matters.",
            },
            {
                "step_id": "comment-depth",
                "role": "comment collection for selected video IDs",
                "skill_names": ["fetch-youtube-comments"],
                "output": "comment artifacts for selected videos",
                "followup_when": "Use after selecting videos whose comment surfaces are relevant.",
            },
        ],
        "normalizer_skills": [
            "normalize-youtube-video-public-signals",
            "normalize-youtube-comments-public-signals",
        ],
        "attempt_review_questions": [
            "Did the search query miss likely creator, location, event, or date language?",
            "Did candidate videos exist but comments remain unfetched?",
            "Should video selection be revised before abandoning the source family?",
            "If search or comments returned zero rows, did the agent record whether query wording, selected videos, disabled comments, or time filters caused the gap?",
        ],
    },
    {
        "family_id": "regulationsgov-policy-comments",
        "label": "Regulations.gov policy comment workflow",
        "semantics": (
            "Agent-owned list-to-detail workflow. The comments list stage discovers "
            "candidate comment IDs; detail fetch enriches selected comments where "
            "full text or attachments are needed."
        ),
        "workflow_steps": [
            {
                "step_id": "comment-list",
                "role": "comment discovery by docket/document, agency, or time window",
                "skill_names": ["fetch-regulationsgov-comments"],
                "output": "comment list artifacts with IDs",
                "followup_when": "Use detail fetch when list rows are insufficient for evidence review.",
            },
            {
                "step_id": "comment-detail",
                "role": "full detail and attachment enrichment for selected comments",
                "skill_names": ["fetch-regulationsgov-comment-detail"],
                "output": "comment detail artifacts",
                "followup_when": "Use after selecting comment IDs from list artifacts.",
            },
        ],
        "normalizer_skills": [
            "normalize-regulationsgov-comments-public-signals",
            "normalize-regulationsgov-comment-detail-public-signals",
        ],
        "attempt_review_questions": [
            "Was the filter mode/date field appropriate for the policy question?",
            "Did the list stage return IDs that require detail enrichment?",
            "Should docket/document/agency constraints be revised before stopping?",
            "If no comments were returned, did the agent distinguish docket/filter limits from absence of policy discussion?",
        ],
    },
    {
        "family_id": "bluesky-public-discourse",
        "label": "Bluesky public discourse workflow",
        "semantics": (
            "Agent-owned social discourse fetch workflow. Search, author-feed, and "
            "thread/cascade modes are alternate paths within the same skill, not a "
            "runtime-selected order."
        ),
        "workflow_steps": [
            {
                "step_id": "post-or-thread-fetch",
                "role": "search, author-feed, or thread/cascade collection",
                "skill_names": ["fetch-bluesky-cascade"],
                "output": "post and reply artifacts",
                "followup_when": (
                    "Use a different mode or query when one mode is too narrow or "
                    "cannot observe the discourse surface."
                ),
            },
        ],
        "normalizer_skills": ["normalize-bluesky-cascade-public-signals"],
        "attempt_review_questions": [
            "Did the selected mode match the evidence need: search, author-feed, or thread/cascade?",
            "Should handles, hashtags, location terms, or event terms be revised?",
            "If output was sparse, did the agent consider another mode before calling the source exhausted?",
        ],
    },
    {
        "family_id": "openaq-observation",
        "label": "OpenAQ observation workflow",
        "semantics": (
            "Agent-owned metadata-to-measurement workflow. Metadata discovery, API "
            "measurements, and S3 archive backfill are related OpenAQ paths; empty "
            "measurement windows should prompt metadata/window/parameter review."
        ),
        "workflow_steps": [
            {
                "step_id": "metadata-or-measurements",
                "role": "station/parameter discovery, API measurement fetch, or archive backfill",
                "skill_names": ["fetch-openaq"],
                "output": "OpenAQ metadata or measurement artifacts",
                "followup_when": (
                    "Use metadata discovery before measurements when location or "
                    "parameter IDs are not known; use archive backfill when API "
                    "windows do not cover the needed period."
                ),
            },
        ],
        "normalizer_skills": ["normalize-openaq-observation-signals"],
        "attempt_review_questions": [
            "Were location IDs, parameter IDs, and datetime bounds grounded in provider metadata?",
            "Should the agent use archive backfill instead of API measurements?",
            "If measurements were empty, did the agent distinguish API-window limits from archive or metadata routes?",
        ],
    },
    {
        "family_id": "environment-observation-crosscheck",
        "label": "Environmental observation cross-check workflow",
        "semantics": (
            "Agent-owned complementary observation workflow. AirNow, Open-Meteo, "
            "NASA FIRMS, and USGS are direct evidence surfaces that can cross-check "
            "receptor conditions, weather/transport context, source-region fire "
            "activity, or hydrologic context. The runtime does not choose which "
            "surface proves a claim."
        ),
        "workflow_steps": [
            {
                "step_id": "direct-observation-or-model-context",
                "role": "direct observation, modeled context, or source-region evidence",
                "skill_names": [
                    "fetch-airnow-hourly-observations",
                    "fetch-open-meteo-air-quality",
                    "fetch-open-meteo-historical",
                    "fetch-open-meteo-flood",
                    "fetch-nasa-firms-fire",
                    "fetch-usgs-water-iv",
                ],
                "output": "environmental observation artifacts",
                "followup_when": (
                    "Use another compatible environmental source when one provider "
                    "does not cover the place, parameter, or time window."
                ),
            },
        ],
        "normalizer_skills": [
            "normalize-airnow-observation-signals",
            "normalize-open-meteo-air-quality-signals",
            "normalize-open-meteo-historical-signals",
            "normalize-open-meteo-flood-signals",
            "normalize-nasa-firms-fire-observation-signals",
            "normalize-usgs-water-observation-signals",
        ],
        "attempt_review_questions": [
            "Was the provider's spatial/time/parameter coverage actually compatible with the evidence need?",
            "Should another environmental surface be used for cross-checking?",
            "For FIRMS, did the agent check product availability before interpreting zero rows?",
            "If a source-region or receptor claim remains live, did the agent revise product, bbox, window, metric, or provider before stopping?",
        ],
    },
]


def _unique_texts(values: Iterable[object]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = " ".join(str(value).split()) if value is not None else ""
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def _workflow_step_skills(workflow: dict[str, object]) -> list[str]:
    return _unique_texts(
        skill_name
        for step in workflow.get("workflow_steps", [])
        if isinstance(step, dict)
        for skill_name in step.get("skill_names", [])
        if isinstance(skill_name, str)
    )


def _workflow_memberships(skill_name: str) -> list[dict[str, object]]:
    memberships: list[dict[str, object]] = []
    for workflow in SOURCE_FAMILY_WORKFLOWS:
        steps = workflow.get("workflow_steps", [])
        if not isinstance(steps, list):
            continue
        for index, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            step_skill_names = [
                item for item in step.get("skill_names", []) if isinstance(item, str)
            ]
            if skill_name not in step_skill_names:
                continue
            later_skill_names = _unique_texts(
                later_skill
                for later_step in steps[index + 1 :]
                if isinstance(later_step, dict)
                for later_skill in later_step.get("skill_names", [])
                if isinstance(later_skill, str)
            )
            memberships.append(
                {
                    "family_id": str(workflow.get("family_id", "")),
                    "label": str(workflow.get("label", "")),
                    "step_id": str(step.get("step_id", "")),
                    "workflow_role": str(step.get("role", "")),
                    "downstream_hints": later_skill_names,
                    "attempt_review_questions": workflow.get("attempt_review_questions", []),
                }
            )
    return memberships


def source_family_workflows_for_skills(skill_names: Iterable[str]) -> list[dict[str, object]]:
    available = set(_unique_texts(skill_names))
    workflows: list[dict[str, object]] = []
    for workflow in SOURCE_FAMILY_WORKFLOWS:
        related_fetch_skills = _workflow_step_skills(workflow)
        available_fetch_skills = [skill for skill in related_fetch_skills if skill in available]
        if not available_fetch_skills:
            continue
        workflows.append(
            {
                "family_id": workflow.get("family_id"),
                "label": workflow.get("label"),
                "semantics": workflow.get("semantics"),
                "ordering_semantics": (
                    "Workflow steps describe data-dependency possibilities only; "
                    "they are not priority, score, or required agenda order."
                ),
                "available_fetch_skills": available_fetch_skills,
                "related_fetch_skills": related_fetch_skills,
                "workflow_steps": workflow.get("workflow_steps", []),
                "normalizer_skills": workflow.get("normalizer_skills", []),
                "attempt_review_questions": workflow.get("attempt_review_questions", []),
            }
        )
    return workflows


def _profile(
    *,
    queue_status: str,
    stage: str,
    queue_role: str,
    default_invocation: str,
    notes: str,
    requires_explicit_approval: bool = False,
    default_chain_eligible: bool = False,
    source_family_ids: list[str] | None = None,
    workflow_role: str = "",
    downstream_hints: list[str] | None = None,
    attempt_review_questions: list[object] | None = None,
) -> dict[str, object]:
    if queue_status == "bridge":
        governed_execution_behavior = "governed-bridge"
    elif requires_explicit_approval:
        governed_execution_behavior = "approval-gated-runtime-surface"
    elif queue_status == "advisory":
        governed_execution_behavior = "on-demand-runtime-surface"
    else:
        governed_execution_behavior = "capability-runtime-surface"
    return {
        "source_queue_ready": True,
        "queue_status": queue_status,
        "stage": stage,
        "queue_role": queue_role,
        "default_invocation": default_invocation,
        "governed_execution_behavior": governed_execution_behavior,
        "default_chain_eligible": bool(default_chain_eligible),
        "requires_explicit_approval": bool(requires_explicit_approval),
        "source_family_ids": list(source_family_ids or []),
        "workflow_role": workflow_role,
        # Optional source-family hints are agent-owned data-dependency context.
        # They must not become runtime-selected source queues or agenda locks.
        "downstream_hints": list(downstream_hints or []),
        "attempt_review_questions": list(attempt_review_questions or []),
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
            default_invocation="role-owner-investigator-triggered",
            notes=(
                "Import or execute only the actor's assigned fetch outputs and write "
                "signal-plane receipts; it must not select downstream analysis conclusions."
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
            in {"open-investigation-round", "freeze-report-basis"},
        )

    if skill_name in OPTIONAL_ANALYSIS_SKILLS:
        return _optional_analysis_profile(skill_name)

    if skill_name in FETCH_SKILLS:
        memberships = _workflow_memberships(skill_name)
        source_family_ids = _unique_texts(
            item.get("family_id")
            for item in memberships
            if isinstance(item, dict)
        )
        workflow_role = "; ".join(
            _unique_texts(
                item.get("workflow_role")
                for item in memberships
                if isinstance(item, dict)
            )
        )
        downstream_hints = _unique_texts(
            hint
            for item in memberships
            if isinstance(item, dict)
            for hint in item.get("downstream_hints", [])
            if isinstance(hint, str)
        )
        attempt_review_questions = _unique_texts(
            question
            for item in memberships
            if isinstance(item, dict)
            for question in item.get("attempt_review_questions", [])
            if isinstance(question, str)
        )
        return _profile(
            queue_status="capability",
            stage="fetch",
            queue_role="raw-artifact-fetch",
            default_invocation="investigator-triggered",
            notes=(
                "Fetch capability for raw source collection. It writes raw artifacts "
                "or receipts and carries no default investigation judgement."
            ),
            source_family_ids=source_family_ids,
            workflow_role=workflow_role,
            downstream_hints=downstream_hints,
            attempt_review_questions=attempt_review_questions,
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
                "materialize-spatiotemporal-relation-evidence-packet",
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
            notes="Operator-owned archive or retrieval capability; not a governed-execution investigation step.",
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
    governed_execution_behavior_counts: Counter[str] = Counter()
    skill_count = 0
    source_queue_ready_count = 0

    for entry in skill_entries:
        profile = entry.get("source_queue_profile") if isinstance(entry.get("source_queue_profile"), dict) else source_queue_profile(str(entry.get("skill_name", "")))
        queue_status = str(profile.get("queue_status", ""))
        stage = str(profile.get("stage", ""))
        queue_role = str(profile.get("queue_role", ""))
        governed_execution_behavior = str(profile.get("governed_execution_behavior", ""))
        skill_count += 1
        if profile.get("source_queue_ready") is True:
            source_queue_ready_count += 1
        if queue_status:
            queue_status_counts[queue_status] += 1
        if stage:
            stage_counts[stage] += 1
        if queue_role:
            queue_role_counts[queue_role] += 1
        if governed_execution_behavior:
            governed_execution_behavior_counts[governed_execution_behavior] += 1

    return {
        "skill_count": skill_count,
        "source_queue_ready_count": source_queue_ready_count,
        "queue_status_counts": dict(sorted(queue_status_counts.items())),
        "stage_counts": dict(sorted(stage_counts.items())),
        "queue_role_counts": dict(sorted(queue_role_counts.items())),
        "governed_execution_behavior_counts": dict(sorted(governed_execution_behavior_counts.items())),
    }


__all__ = [
    "source_family_workflows_for_skills",
    "source_queue_profile",
    "source_queue_profile_summary",
]

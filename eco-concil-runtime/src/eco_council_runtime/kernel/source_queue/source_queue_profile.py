from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


FETCH_SKILLS = {
    "fetch-airnow-hourly-observations",
    "fetch-bluesky-cascade",
    "fetch-epa-eis-records",
    "fetch-federal-register-documents",
    "fetch-gdelt-doc-search",
    "fetch-gdelt-events",
    "fetch-gdelt-gkg",
    "fetch-gdelt-mentions",
    "fetch-nasa-firms-fire",
    "fetch-open-meteo-air-quality",
    "fetch-open-meteo-flood",
    "fetch-open-meteo-historical",
    "fetch-openaq",
    "fetch-regulationsgov-attachments",
    "fetch-regulationsgov-comment-detail",
    "fetch-regulationsgov-comments",
    "fetch-usbr-project-records",
    "fetch-usbr-rise",
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
    "materialize-public-discourse-corpus",
    "audit-formal-comment-candidate-corpus",
    "audit-public-discourse-sample-coverage",
    "classify-formal-comment-issues",
    "classify-public-discourse-affect",
    "aggregate-public-discourse-annotations",
    "compare-public-media-narratives",
    "summarize-public-discourse-sample",
    "detect-temporal-cooccurrence-cues",
    "review-spatiotemporal-relation-alternatives",
    "materialize-claim-gap-action-cards",
    "build-fact-policy-public-interaction-timeline",
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
        "source, metric, spatial, and temporal coverage without claim matching, "
        "risk scoring, source ranking, or source attribution."
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
    "materialize-public-discourse-corpus": (
        "Approval-gated public discourse corpus helper. It materializes a "
        "DB-visible sample with explicit boundaries and does not infer public "
        "opinion or report conclusions."
    ),
    "audit-formal-comment-candidate-corpus": (
        "Approval-gated formal comment candidate corpus audit helper. It describes "
        "candidate/list shape, drift cues, and field coverage without judging "
        "stance, importance, sufficiency, or source ranking."
    ),
    "audit-public-discourse-sample-coverage": (
        "Approval-gated public discourse coverage audit helper. It emits "
        "source-family coverage cues and limitations, not representation "
        "findings or absence claims."
    ),
    "classify-formal-comment-issues": (
        "Approval-gated formal comment annotation worker. It emits sample-local "
        "issue, stance, and concern cues from DB-visible readable formal text; "
        "it is not a council agent and does not infer public opinion or evidence sufficiency."
    ),
    "classify-public-discourse-affect": (
        "Approval-gated public discourse annotation worker. It emits sample-local "
        "item labels for affect, issues, stance/action, and source narratives. "
        "It is not a council agent and does not create findings or public-opinion "
        "claims."
    ),
    "aggregate-public-discourse-annotations": (
        "Approval-gated public discourse annotation aggregation helper. It "
        "summarizes sample labels only and does not infer public opinion."
    ),
    "compare-public-media-narratives": (
        "Approval-gated public/media narrative comparison helper. It compares "
        "sample lanes as advisory cues without alignment scores or attribution "
        "conclusions."
    ),
    "summarize-public-discourse-sample": (
        "Approval-gated public discourse sample summary helper. It assembles "
        "approved sample artifacts into advisory board handoff material and "
        "does not create report-ready conclusions."
    ),
    "detect-temporal-cooccurrence-cues": (
        "Approval-gated temporal co-occurrence helper. It is descriptive only and "
        "does not infer influence, causality, spread, or direction."
    ),
    "review-spatiotemporal-relation-alternatives": (
        "Approval-gated relation challenger helper. It emits objection candidates "
        "only and must be carried by challenge, probe, or review comment."
    ),
    "materialize-claim-gap-action-cards": (
        "Approval-gated claim-basis advisory helper. It exposes claim gaps, "
        "candidate followups, recovery/source-limit cards, and report boundaries "
        "without ranking, scheduling, source selection, or automatic execution."
    ),
    "build-fact-policy-public-interaction-timeline": (
        "Approval-gated interaction timeline helper. It aligns fact/policy-side "
        "and public/media-side signal refs as descriptive chronology only, without "
        "causality, policy-impact, response-attribution, or scheduling authority."
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
    "open-report-writing-round": {
        "stage": "transition",
        "queue_role": "report-writing-round-request-consumer",
        "default_invocation": "approved-transition-request",
        "notes": (
            "Consumes an already approved transition request to open a reporting-only "
            "round for report-editor work from frozen or canonical reporting basis."
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
    "synthesize-dossier-program": "coordination-dossier-program-write",
    "materialize-context-packet": "coordination-context-packet-write",
    "submit-evidence-request": "coordination-evidence-request-write",
    "submit-evidence-route-assessment": "coordination-route-assessment-write",
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
    "draft-narrative-report",
    "validate-narrative-report",
    "publish-narrative-report",
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
        "family_id": "usbr-operational-records",
        "label": "USBR operational records workflow",
        "semantics": (
            "Agent-owned direct operational-record workflow. RISE result rows can "
            "ground reservoir, release, storage, or elevation observations, but "
            "they do not decide shortage severity, operating compliance, or "
            "governance responsibility."
        ),
        "workflow_steps": [
            {
                "step_id": "rise-catalog-discovery",
                "role": "same-family item-id discovery for RISE catalog items",
                "skill_names": ["fetch-usbr-rise"],
                "command_mode": "discover-items",
                "output": "USBR RISE catalog candidate artifacts with candidate_item_ids",
                "followup_when": (
                    "Use when an investigator needs Glen Canyon, Lake Powell, "
                    "reservoir elevation, storage, release, or other USBR "
                    "operational records but no explicit RISE item ID is grounded."
                ),
            },
            {
                "step_id": "rise-result-fetch",
                "role": "direct RISE time-series result retrieval for explicit item IDs",
                "skill_names": ["fetch-usbr-rise"],
                "output": "USBR RISE operational result artifacts",
                "followup_when": (
                    "Use when an investigator has explicit RISE item IDs from a "
                    "catalog candidate artifact, item page, or source request."
                ),
            },
        ],
        "normalizer_skills": ["normalize-usbr-rise-environment-signals"],
        "attempt_review_questions": [
            "If item IDs were unknown, did the agent run or request same-family catalog discovery before declining the route?",
            "Were RISE item IDs grounded in an explicit item page, source request, or discover-items catalog artifact?",
            "Were date filters and page caps compatible with the operational record need?",
            "If rows were sparse or empty, did the agent avoid treating that as absence of USBR operations records?",
        ],
    },
    {
        "family_id": "gdelt-public-record",
        "label": "GDELT public record workflow",
        "semantics": (
            "Agent-owned source-family workflow. DOC search is useful for topical "
            "reconnaissance, article lists, timeline aggregates, and DOC tone; "
            "Events, Mentions, and GKG exports are the row-level follow-up "
            "surfaces for shared UTC windows. DOC tone, AvgTone, MentionDocTone, "
            "and V2Tone are media/document tone cues, not public sentiment. This "
            "is not a source ranking or runtime-owned agenda."
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
            "candidate videos; comment fetch materializes the public-response "
            "corpus for agent-selected videos when discourse semantics, affect, "
            "or concerns matter."
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
            "Agent-owned list-to-detail-to-attachment workflow. The comments list "
            "stage discovers candidate comment IDs; detail fetch enriches selected "
            "comments; attachment fetch and text extraction materialize readable "
            "formal comment text when inline text is absent or insufficient. "
            "Listing rows are not a readable formal-comment corpus by themselves."
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
            {
                "step_id": "attachment-fetch",
                "role": "attachment metadata and file download for selected comments",
                "skill_names": ["fetch-regulationsgov-attachments"],
                "output": "attachment metadata and downloaded file artifacts",
                "followup_when": "Use when detail rows show attachments or inline text says See Attached.",
            },
        ],
        "normalizer_skills": [
            "normalize-regulationsgov-comments-public-signals",
            "normalize-regulationsgov-comment-detail-public-signals",
            "extract-document-text",
            "normalize-regulationsgov-attachment-text",
        ],
        "attempt_review_questions": [
            "Was the filter mode/date field appropriate for the policy question?",
            "Was a candidate corpus audit used before extrapolating from a broad list or single seed?",
            "Did the list stage return IDs that require detail enrichment?",
            "Do detail rows require attachment text before semantic annotation?",
            "Should docket/document/agency constraints be revised before stopping?",
            "If no comments or readable texts were returned, did the agent distinguish docket/filter/API/text-extraction limits from absence of policy discussion?",
        ],
    },
    {
        "family_id": "official-governance-records",
        "label": "Official governance records workflow",
        "semantics": (
            "Agent-owned official-record workflow. Federal Register API records and "
            "direct USBR project pages are complementary official governance surfaces; "
            "neither path proves completeness or legal significance by itself."
        ),
        "workflow_steps": [
            {
                "step_id": "epa-eis-records",
                "role": "EPA EIS Database result metadata from common-search pages or explicit search URLs",
                "skill_names": ["fetch-epa-eis-records"],
                "output": "EPA EIS Database metadata artifacts",
                "followup_when": (
                    "Use when formal NEPA/EIS metadata, lead agency, CEQ number, "
                    "Federal Register date, or document availability cues matter."
                ),
            },
            {
                "step_id": "federal-register-documents",
                "role": "published federal document discovery by term, agency, type, and publication date",
                "skill_names": ["fetch-federal-register-documents"],
                "output": "FederalRegister.gov document metadata artifacts",
                "followup_when": (
                    "Use when formal rulemaking or notice records may bound the "
                    "official governance context."
                ),
            },
            {
                "step_id": "usbr-project-records",
                "role": "direct official project-page and linked-document inventory",
                "skill_names": ["fetch-usbr-project-records"],
                "output": "USBR project page and linked-record artifacts",
                "followup_when": (
                    "Use when an investigator has an official USBR project URL and "
                    "needs a project-specific record surface."
                ),
            },
        ],
        "normalizer_skills": ["normalize-official-governance-records"],
        "attempt_review_questions": [
            "Was the EPA EIS common-search page or explicit search URL appropriate for the project/window?",
            "Was the Federal Register agency slug, term, type, and publication window appropriate?",
            "Were USBR URLs grounded in an explicit official project surface?",
            "Should another official URL, agency term, or date window be tried before stopping?",
            "If records were sparse or empty, did the agent avoid treating that as absence of official governance records?",
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
        "optional_analysis_followup_skills": ["aggregate-environment-evidence"],
        "attempt_review_questions": [
            "Was the provider's spatial/time/parameter coverage actually compatible with the evidence need?",
            "Should another environmental surface be used for cross-checking?",
            "For FIRMS, did the agent check product availability before interpreting zero rows?",
            "If the fetch materialized large observation tables, should aggregate-environment-evidence compress them before report use?",
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
                "optional_analysis_followup_skills": workflow.get("optional_analysis_followup_skills", []),
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
            in {
                "open-investigation-round",
                "open-report-writing-round",
                "freeze-report-basis",
            },
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
                "publish-narrative-report",
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

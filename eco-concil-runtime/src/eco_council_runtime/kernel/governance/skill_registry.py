from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.contracts import ENVIRONMENT_SIGNAL_TAXONOMY_VERSION
from eco_council_runtime.kernel.governance.role_contracts import (
    CAPABILITY_ANALYSIS,
    CAPABILITY_ARCHIVE_WRITE,
    CAPABILITY_BOARD_NOTE_WRITE,
    CAPABILITY_BOARD_TASK_WRITE,
    CAPABILITY_CHALLENGE_WRITE,
    CAPABILITY_DISCUSSION_WRITE,
    CAPABILITY_DERIVED_EXPORT,
    CAPABILITY_FETCH,
    CAPABILITY_HYPOTHESIS_WRITE,
    CAPABILITY_NORMALIZE,
    CAPABILITY_PROBE_WRITE,
    CAPABILITY_PROPOSAL_WRITE,
    CAPABILITY_QUERY,
    CAPABILITY_READINESS_WRITE,
    CAPABILITY_REPORT_DRAFT,
    CAPABILITY_REPORT_PUBLISH,
    CAPABILITY_ROUND_BOOTSTRAP,
    CAPABILITY_RUNTIME_ADMIN,
    CAPABILITY_STATE_TRANSITION,
    ROLE_CHALLENGER,
    ROLE_ENVIRONMENTAL_INVESTIGATOR,
    ROLE_MODERATOR,
    ROLE_REPORT_EDITOR,
    ROLE_RUNTIME_OPERATOR,
    ROLE_SOCIAL_INVESTIGATOR,
)
from eco_council_runtime.kernel.source_queue.source_queue_contract import SOURCE_CATALOG

SKILL_LAYER_FETCH = "fetch"
SKILL_LAYER_NORMALIZE = "normalize"
SKILL_LAYER_QUERY = "query"
SKILL_LAYER_OPTIONAL_ANALYSIS = "optional-analysis"
SKILL_LAYER_DELIBERATION_WRITE = "deliberation-write"
SKILL_LAYER_REPORTING = "reporting"
SKILL_LAYER_STATE_TRANSITION = "state-transition"
SKILL_LAYER_RUNTIME_ADMIN = "runtime-admin"

WRITE_SCOPE_READ_ONLY = "read-only"
WRITE_SCOPE_ARTIFACT = "artifact-write"
WRITE_SCOPE_SIGNAL = "signal-write"
WRITE_SCOPE_ANALYSIS = "analysis-write"
WRITE_SCOPE_DELIBERATION = "deliberation-write"
WRITE_SCOPE_REPORTING = "reporting-write"
WRITE_SCOPE_STATE_TRANSITION = "state-transition-write"
WRITE_SCOPE_ARCHIVE = "archive-write"
WRITE_SCOPE_RUNTIME = "runtime-write"

INVESTIGATOR_ROLES = [
    ROLE_ENVIRONMENTAL_INVESTIGATOR,
    ROLE_SOCIAL_INVESTIGATOR,
]
FETCH_NORMALIZE_ROLES = [
    *INVESTIGATOR_ROLES,
    ROLE_CHALLENGER,
]
RESEARCH_ROLES = [
    ROLE_MODERATOR,
    *INVESTIGATOR_ROLES,
    ROLE_CHALLENGER,
]
READ_ONLY_ROLES = [
    ROLE_MODERATOR,
    *INVESTIGATOR_ROLES,
    ROLE_CHALLENGER,
    ROLE_REPORT_EDITOR,
    ROLE_RUNTIME_OPERATOR,
]
REPORTING_ROLES = [
    ROLE_MODERATOR,
    ROLE_REPORT_EDITOR,
]
EXPORT_ROLES = [
    ROLE_MODERATOR,
    ROLE_REPORT_EDITOR,
    ROLE_RUNTIME_OPERATOR,
]


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[5]


def _policy(
    *,
    skill_name: str,
    skill_layer: str,
    allowed_roles: list[str],
    required_capabilities: list[str],
    side_effect_scope: list[str],
    db_write_planes: list[str],
    input_object_kinds: list[str],
    output_object_kinds: list[str],
    write_scope: str,
    requires_operator_approval: bool = False,
    denied_roles: list[str] | None = None,
    default_actor_role_hint: str = "",
) -> dict[str, Any]:
    return {
        "skill_name": skill_name,
        "skill_layer": maybe_text(skill_layer),
        "allowed_roles": unique_texts(allowed_roles),
        "denied_roles": unique_texts(denied_roles or []),
        "required_capabilities": unique_texts(required_capabilities),
        "side_effect_scope": unique_texts(side_effect_scope),
        "db_write_planes": unique_texts(db_write_planes),
        "input_object_kinds": unique_texts(input_object_kinds),
        "output_object_kinds": unique_texts(output_object_kinds),
        "write_scope": maybe_text(write_scope) or WRITE_SCOPE_READ_ONLY,
        "requires_operator_approval": bool(requires_operator_approval),
        "default_actor_role_hint": maybe_text(default_actor_role_hint),
    }


def _group(
    skill_names: list[str],
    *,
    skill_layer: str,
    allowed_roles: list[str],
    required_capabilities: list[str],
    side_effect_scope: list[str],
    db_write_planes: list[str],
    input_object_kinds: list[str],
    output_object_kinds: list[str],
    write_scope: str,
    requires_operator_approval: bool = False,
    denied_roles: list[str] | None = None,
    default_actor_role_hint: str = "",
) -> dict[str, dict[str, Any]]:
    return {
        skill_name: _policy(
            skill_name=skill_name,
            skill_layer=skill_layer,
            allowed_roles=allowed_roles,
            required_capabilities=required_capabilities,
            side_effect_scope=side_effect_scope,
            db_write_planes=db_write_planes,
            input_object_kinds=input_object_kinds,
            output_object_kinds=output_object_kinds,
            write_scope=write_scope,
            requires_operator_approval=requires_operator_approval,
            denied_roles=denied_roles,
            default_actor_role_hint=default_actor_role_hint,
        )
        for skill_name in skill_names
    }


FETCH_SKILLS = [
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
]

NORMALIZE_SKILLS = [
    "normalize-airnow-observation-signals",
    "normalize-bluesky-cascade-public-signals",
    "normalize-gdelt-doc-public-signals",
    "normalize-gdelt-events-public-signals",
    "normalize-gdelt-gkg-public-signals",
    "normalize-gdelt-mentions-public-signals",
    "normalize-nasa-firms-fire-observation-signals",
    "normalize-open-meteo-air-quality-signals",
    "normalize-open-meteo-flood-signals",
    "normalize-open-meteo-historical-signals",
    "normalize-openaq-observation-signals",
    "normalize-official-governance-records",
    "normalize-regulationsgov-attachment-text",
    "normalize-regulationsgov-comment-detail-public-signals",
    "normalize-regulationsgov-comments-public-signals",
    "normalize-usbr-rise-environment-signals",
    "normalize-usgs-water-observation-signals",
    "normalize-youtube-comments-public-signals",
    "normalize-youtube-video-public-signals",
]

OPTIONAL_ANALYSIS_SKILLS = [
    "aggregate-environment-evidence",
    "review-fact-check-evidence-scope",
    "discover-discourse-issues",
    "suggest-evidence-lanes",
    "materialize-claim-gap-action-cards",
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
    "build-fact-policy-public-interaction-timeline",
    "review-evidence-sufficiency",
]

EVIDENCE_ONLY_FORBIDDEN_OUTPUT_FIELDS = (
    "blocking_if_missing",
    "candidate_source_weight",
    "confidence_score",
    "coverage_score",
    "evidence_score",
    "heuristic_score",
    "minimum_coverage",
    "priority",
    "priority_order",
    "quality_score",
    "rank",
    "ranked_items",
    "ranking",
    "readiness_score",
    "recommended_conclusion",
    "recommended_outcome",
    "recommended_source_rank",
    "score",
    "scores",
    "source_weight",
    "sufficiency_score",
    "support_level",
    "support_score",
    "weight",
    "weights",
)

EVIDENCE_ONLY_REQUIRED_OUTPUT_FIELDS = (
    "artifact_refs",
    "provenance",
    "source_parameters",
    "query_parameters",
)

OPTIONAL_HELPER_ALLOWED_DECISION_SOURCES = [
    "approved-helper-view",
    "manual-or-moderator-defined",
    "agent-submitted-finding",
    "scenario",
]

OPTIONAL_ANALYSIS_HELPER_FREEZE_LINES: dict[str, dict[str, Any]] = {
    "aggregate-environment-evidence": {
        "rule_id": "HEUR-ENV-AGGREGATE-001",
        "destination": "DB-backed environment evidence aggregation helper",
        "caveats": [
            "Aggregation is descriptive only and cannot be used for claim matching, risk scoring, source ranking, source attribution, or readiness scoring.",
            "Report use requires finding, evidence bundle, proposal, review comment, or report_basis citation.",
        ],
    },
    "review-fact-check-evidence-scope": {
        "rule_id": "HEUR-FACT-SCOPE-001",
        "destination": "explicit fact-check scope review helper",
        "caveats": [
            "Requires explicit verification question, geography, study period, evidence window, lag assumptions, metric requirements, and source requirements.",
            "Does not emit factual outcome labels or phase-gate posture.",
        ],
    },
    "discover-discourse-issues": {
        "rule_id": "HEUR-DISCOURSE-DISCOVERY-001",
        "destination": "DB-backed public/formal discourse issue hints",
    },
    "suggest-evidence-lanes": {
        "rule_id": "HEUR-EVIDENCE-LANE-001",
        "destination": "advisory evidence-lane tags",
        "caveats": [
            "Lane tags cannot assign owners, drive the source queue, or freeze-report-basis phases.",
            "Any investigation action must be carried by DB council objects.",
        ],
    },
    "materialize-claim-gap-action-cards": {
        "rule_id": "HEUR-CLAIM-GAP-ACTION-CARDS-001",
        "destination": "claim-basis advisory action cards",
        "caveats": [
            "Action cards are advisory only and cannot rank, score, schedule, or execute skills.",
            "Action cards expose claim gaps, recovery routes, and report boundaries; agents decide whether to adopt, reject, or rewrite them through council objects.",
            "Zero, failed, low-volume, or receipt-only attempts must remain source-limit or recovery context, not evidence absence.",
        ],
    },
    "materialize-research-issue-surface": {
        "rule_id": "HEUR-RESEARCH-ISSUE-SURFACE-001",
        "destination": "candidate research issue surface helper",
    },
    "project-research-issue-views": {
        "rule_id": "HEUR-RESEARCH-ISSUE-PROJECTION-001",
        "destination": "typed research issue cue projections",
    },
    "export-research-issue-map": {
        "rule_id": "HEUR-RESEARCH-ISSUE-MAP-001",
        "destination": "research issue navigation export",
        "caveats": [
            "The issue map is traceability/navigation only and is not a conclusion graph.",
            "Edges do not imply causal relationships.",
        ],
    },
    "apply-approved-formal-public-taxonomy": {
        "rule_id": "HEUR-TAXONOMY-APPLY-001",
        "taxonomy_version": "formal-public-taxonomy-freeze-2026-04-29",
        "destination": "approved formal/public taxonomy label cues",
        "caveats": [
            "No default taxonomy may be applied without an approved mission-scoped taxonomy reference.",
            "Candidate labels require human audit before report use.",
        ],
    },
    "compare-formal-public-footprints": {
        "rule_id": "HEUR-FORMAL-PUBLIC-FOOTPRINT-001",
        "destination": "formal/public footprint comparison helper",
        "caveats": [
            "Footprint comparison describes overlap and absence cues only.",
            "It does not create paired discourse links or alignment scores.",
        ],
    },
    "identify-representation-audit-cues": {
        "rule_id": "HEUR-REPRESENTATION-AUDIT-001",
        "destination": "representation audit cue helper",
        "caveats": [
            "Representation audit cues are prompts for human review, not findings.",
            "No severity score may be emitted by this helper.",
        ],
    },
    "materialize-public-discourse-corpus": {
        "rule_id": "HEUR-PUBLIC-DISCOURSE-CORPUS-001",
        "destination": "public/formal discourse corpus materialization helper",
        "caveats": [
            "The corpus defines a DB-visible text sample only and cannot infer general public opinion.",
            "GDELT tone rows must remain media/document tone, not public response sentiment.",
            "Source-family denominators are isolated and must not be mixed across GDELT, YouTube, Bluesky, formal records, or formal comments.",
        ],
    },
    "audit-formal-comment-candidate-corpus": {
        "rule_id": "HEUR-FORMAL-COMMENT-CANDIDATE-CORPUS-001",
        "destination": "formal comment candidate corpus audit helper",
        "caveats": [
            "Candidate corpus audit describes sample shape only and cannot judge stance, importance, or evidence sufficiency.",
            "Drift indicators are review cues, not source scores or source ranking.",
            "Formal comment samples must not be converted into general public-opinion distributions.",
        ],
    },
    "audit-public-discourse-sample-coverage": {
        "rule_id": "HEUR-PUBLIC-DISCOURSE-COVERAGE-001",
        "destination": "public discourse sample coverage audit helper",
        "caveats": [
            "Coverage cues are prompts for human review, not representation findings.",
            "Zero rows may reflect unrun fetches, filters, API limits, import scope, or normalization gaps.",
            "Failed, zero, low-volume, or receipt-only source acquisition is a source-limit rationale, not evidence absence.",
        ],
    },
    "classify-formal-comment-issues": {
        "rule_id": "HEUR-FORMAL-COMMENT-ISSUE-ANNOTATION-001",
        "taxonomy_version": "formal-public-taxonomy-freeze-2026-04-29",
        "destination": "bounded formal comment issue annotation worker",
        "caveats": [
            "Worker labels describe only DB-visible formal comment text signals inside the selected sample.",
            "This helper is not a council agent and does not write findings, report basis, source ranking, or evidence sufficiency decisions.",
            "Formal comment samples must not be converted into general public-opinion distributions.",
        ],
    },
    "classify-public-discourse-affect": {
        "rule_id": "HEUR-PUBLIC-DISCOURSE-AFFECT-ANNOTATION-001",
        "destination": "bounded public discourse annotation worker",
        "caveats": [
            "Worker labels describe only items inside the selected corpus sample.",
            "This helper is not a council agent and does not write findings or report basis.",
            "Challenger review is boundary/taxonomy/outlier review, not mandatory item-by-item relabeling.",
        ],
    },
    "aggregate-public-discourse-annotations": {
        "rule_id": "HEUR-PUBLIC-DISCOURSE-ANNOTATION-001",
        "destination": "sample-level public discourse annotation aggregation helper",
        "caveats": [
            "Annotation distributions describe only annotated items inside the selected sample.",
            "Sample fractions are not public-opinion estimates.",
        ],
    },
    "compare-public-media-narratives": {
        "rule_id": "HEUR-PUBLIC-MEDIA-NARRATIVE-COMPARE-001",
        "destination": "public/media narrative comparison helper",
        "caveats": [
            "Cross-source cues are advisory comparisons, not alignment scores or source-attribution findings.",
            "GDELT media tone, social sample affect, and formal comment samples must remain separate.",
        ],
    },
    "summarize-public-discourse-sample": {
        "rule_id": "HEUR-PUBLIC-DISCOURSE-SUMMARY-001",
        "destination": "public discourse sample summary handoff helper",
        "caveats": [
            "The summary is advisory handoff material and does not create findings or report basis.",
            "Report-facing use requires explicit council object uptake.",
        ],
    },
    "detect-temporal-cooccurrence-cues": {
        "rule_id": "HEUR-SPATIOTEMPORAL-RELATION-001",
        "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
        "destination": "temporal co-occurrence and spatiotemporal relation cue helper",
        "caveats": [
            "Temporal cues are descriptive only and do not imply influence, causality, spread, or direction.",
            "Missing timestamps must be reported as insufficient temporal basis, not silently defaulted.",
            "Legacy same-day cues are not DB-backed spatiotemporal relation conclusions.",
        ],
    },
    "review-spatiotemporal-relation-alternatives": {
        "rule_id": "HEUR-SPATIOTEMPORAL-ALTERNATIVES-001",
        "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
        "destination": "spatiotemporal relation challenger objection candidate helper",
        "caveats": [
            "Relation objection candidates are prompts for challenger review, not findings.",
            "This helper does not prove or invalidate relation cues by itself.",
            "Report-facing use must be carried by challenge, probe, review comment, finding, or report basis.",
        ],
    },
    "build-fact-policy-public-interaction-timeline": {
        "rule_id": "HEUR-FACT-POLICY-PUBLIC-TIMELINE-001",
        "destination": "fact/policy/public interaction timeline helper",
        "caveats": [
            "Timeline nodes are descriptive chronology/context only and do not prove causality, policy impact, public response attribution, representativeness, or evidence absence.",
            "Each interaction node must carry fact/policy-side refs and public/media-side refs.",
            "Report-facing use requires council or reporting uptake with denominator and limitation metadata.",
        ],
    },
    "review-evidence-sufficiency": {
        "rule_id": "HEUR-SUFFICIENCY-REVIEW-001",
        "destination": "DB-backed evidence sufficiency notes and caveats",
        "caveats": [
            "This helper emits review notes only; it is not a phase gate or report_basis by itself.",
            "Report use requires explicit citation through DB council or reporting basis objects.",
        ],
    },
    "plan-round-orchestration": {
        "rule_id": "HEUR-AGENDA-001",
        "destination": "approval-gated advisory helper",
    },
    "propose-next-actions": {
        "rule_id": "HEUR-NEXT-ACTION-001",
        "destination": "approval-gated advisory helper",
    },
    "open-falsification-probe": {
        "rule_id": "HEUR-PROBE-001",
        "destination": "challenger/moderator helper",
    },
    "summarize-round-readiness": {
        "rule_id": "HEUR-READINESS-001",
        "destination": "optional readiness opinion",
    },
}

QUERY_SKILLS = [
    "query-board-delta",
    "query-public-signals",
    "query-formal-signals",
    "query-environment-signals",
    "query-normalized-signal",
    "query-spatiotemporal-relations",
    "query-raw-record",
    "query-signal-corpus",
    "query-case-library",
]


def evidence_only_skill_names() -> list[str]:
    return unique_texts(
        [
            *FETCH_SKILLS,
            *NORMALIZE_SKILLS,
            "normalize-fetch-execution",
            *QUERY_SKILLS,
            *OPTIONAL_ANALYSIS_SKILLS,
        ]
    )


def skill_boundary_metadata(skill_name: str) -> dict[str, Any]:
    normalized_name = maybe_text(skill_name)
    if normalized_name in FETCH_SKILLS:
        profile = "raw-artifact-evidence-only"
        allowed_outputs = [
            "raw provider artifacts",
            "fetch receipts",
            "source metadata",
            "scope and coverage limitations",
        ]
    elif normalized_name in NORMALIZE_SKILLS or normalized_name == "normalize-fetch-execution":
        profile = "normalized-signal-evidence-only"
        allowed_outputs = [
            "normalized signals",
            "conversion provenance",
            "quality flags",
            "source/artifact references",
        ]
    elif normalized_name in QUERY_SKILLS:
        profile = "query-response-evidence-only"
        allowed_outputs = [
            "matching records",
            "query parameters",
            "source/artifact references",
            "pagination metadata",
        ]
    elif normalized_name in OPTIONAL_ANALYSIS_SKILLS:
        profile = "approved-helper-derived-evidence-only"
        allowed_outputs = [
            "reproducible derived evidence objects",
            "source/provenance notes",
            "audit caveats",
            "object references",
        ]
    else:
        return {}
    return {
        "boundary_version": "evidence-only-skill-boundary-v1",
        "boundary_profile": profile,
        "allowed_outputs": allowed_outputs,
        "forbidden_output_fields": list(EVIDENCE_ONLY_FORBIDDEN_OUTPUT_FIELDS),
        "required_output_semantics": list(EVIDENCE_ONLY_REQUIRED_OUTPUT_FIELDS),
        "does_not_assign_evidence_weight": True,
        "does_not_rank_sources": True,
        "does_not_recommend_conclusions": True,
        "requires_agent_uptake": True,
        "agent_authority": (
            "Agents decide evidence combination, acceptance, caveats, and "
            "report-facing judgement through explicit council objects."
        ),
    }


def _boundary_path_is_ignored(path: str, ignored_paths: set[str]) -> bool:
    return path in ignored_paths or any(
        path.startswith(ignored + ".") or path.startswith(ignored + "[")
        for ignored in ignored_paths
    )


def skill_boundary_violations(
    skill_name: str,
    payload: Any,
    *,
    ignored_paths: list[str] | None = None,
) -> list[dict[str, str]]:
    boundary = skill_boundary_metadata(skill_name)
    if not boundary:
        return []
    forbidden_fields = {
        maybe_text(field_name)
        for field_name in boundary.get("forbidden_output_fields", [])
        if maybe_text(field_name)
    }
    ignored = set(ignored_paths or [])
    violations: list[dict[str, str]] = []

    def visit(value: Any, path: str) -> None:
        if _boundary_path_is_ignored(path, ignored):
            return
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}"
                if maybe_text(key) in forbidden_fields:
                    violations.append(
                        {
                            "skill_name": maybe_text(skill_name),
                            "path": child_path,
                            "field_name": maybe_text(key),
                            "boundary_profile": maybe_text(
                                boundary.get("boundary_profile")
                            ),
                        }
                    )
                visit(child, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{path}[{index}]")

    visit(payload, "$")
    return violations


def validate_skill_output_boundary(
    skill_name: str,
    payload: Any,
    *,
    ignored_paths: list[str] | None = None,
) -> None:
    violations = skill_boundary_violations(
        skill_name,
        payload,
        ignored_paths=ignored_paths,
    )
    if not violations:
        return
    paths = ", ".join(violation["path"] for violation in violations[:10])
    raise ValueError(
        f"{skill_name} violates evidence-only skill boundary at: {paths}"
    )

POLICIES: dict[str, dict[str, Any]] = {}
POLICIES.update(
    _group(
        FETCH_SKILLS,
        skill_layer=SKILL_LAYER_FETCH,
        allowed_roles=FETCH_NORMALIZE_ROLES,
        required_capabilities=[CAPABILITY_FETCH],
        side_effect_scope=["network-external", "artifact-write"],
        db_write_planes=[],
        input_object_kinds=["mission-brief", "source-selection"],
        output_object_kinds=["raw-artifact"],
        write_scope=WRITE_SCOPE_ARTIFACT,
    )
)
for _fetch_skill_name, _source_config in SOURCE_CATALOG.items():
    if _fetch_skill_name not in POLICIES:
        continue
    _source_role = maybe_text(_source_config.get("role"))
    if not _source_role:
        continue
    POLICIES[_fetch_skill_name]["allowed_roles"] = unique_texts(
        [_source_role, ROLE_CHALLENGER]
    )
POLICIES.update(
    _group(
        NORMALIZE_SKILLS,
        skill_layer=SKILL_LAYER_NORMALIZE,
        allowed_roles=FETCH_NORMALIZE_ROLES,
        required_capabilities=[CAPABILITY_NORMALIZE],
        side_effect_scope=["artifact-read", "artifact-write", "db-write:signal"],
        db_write_planes=["signal"],
        input_object_kinds=["raw-artifact"],
        output_object_kinds=["normalized-signal"],
        write_scope=WRITE_SCOPE_SIGNAL,
    )
)
POLICIES.update(
    {
        "extract-document-text": _policy(
            skill_name="extract-document-text",
            skill_layer=SKILL_LAYER_NORMALIZE,
            allowed_roles=FETCH_NORMALIZE_ROLES,
            required_capabilities=[CAPABILITY_NORMALIZE],
            side_effect_scope=["artifact-read", "artifact-write"],
            db_write_planes=[],
            input_object_kinds=["raw-artifact"],
            output_object_kinds=["text-artifact"],
            write_scope=WRITE_SCOPE_ARTIFACT,
        )
    }
)
POLICIES.update(
    _group(
        QUERY_SKILLS,
        skill_layer=SKILL_LAYER_QUERY,
        allowed_roles=READ_ONLY_ROLES,
        required_capabilities=[CAPABILITY_QUERY],
        side_effect_scope=["db-read"],
        db_write_planes=[],
        input_object_kinds=["normalized-signal", "analysis-result", "deliberation-state", "archive-state"],
        output_object_kinds=["query-response"],
        write_scope=WRITE_SCOPE_READ_ONLY,
    )
)
POLICIES.update(
    _group(
        OPTIONAL_ANALYSIS_SKILLS,
        skill_layer=SKILL_LAYER_OPTIONAL_ANALYSIS,
        allowed_roles=RESEARCH_ROLES,
        required_capabilities=[CAPABILITY_ANALYSIS],
        side_effect_scope=["db-read", "db-write:analysis", "artifact-write"],
        db_write_planes=["analysis"],
        input_object_kinds=["normalized-signal", "analysis-context"],
        output_object_kinds=["analysis-object"],
        write_scope=WRITE_SCOPE_ANALYSIS,
        requires_operator_approval=True,
    )
)

POLICIES.update(
    {
        "scaffold-mission-run": _policy(
            skill_name="scaffold-mission-run",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_ROUND_BOOTSTRAP],
            side_effect_scope=["artifact-write", "db-write:runtime", "db-write:deliberation"],
            db_write_planes=["runtime", "deliberation"],
            input_object_kinds=["mission-brief"],
            output_object_kinds=["mission-scaffold", "round-bootstrap"],
            write_scope=WRITE_SCOPE_STATE_TRANSITION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "prepare-round": _policy(
            skill_name="prepare-round",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_ROUND_BOOTSTRAP],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=["mission-scaffold", "source-governance"],
            output_object_kinds=["source-plan"],
            write_scope=WRITE_SCOPE_ARTIFACT,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "normalize-fetch-execution": _policy(
            skill_name="normalize-fetch-execution",
            skill_layer=SKILL_LAYER_NORMALIZE,
            allowed_roles=FETCH_NORMALIZE_ROLES,
            required_capabilities=[CAPABILITY_NORMALIZE],
            side_effect_scope=["artifact-read", "artifact-write", "db-write:signal"],
            db_write_planes=["signal"],
            input_object_kinds=["raw-artifact"],
            output_object_kinds=["normalized-signal", "execution-receipt"],
            write_scope=WRITE_SCOPE_SIGNAL,
        ),
        "open-investigation-round": _policy(
            skill_name="open-investigation-round",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_STATE_TRANSITION],
            side_effect_scope=["artifact-write", "db-write:deliberation", "db-write:runtime"],
            db_write_planes=["deliberation", "runtime"],
            input_object_kinds=["transition-request", "round-transition"],
            output_object_kinds=["round-transition"],
            write_scope=WRITE_SCOPE_STATE_TRANSITION,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "open-report-writing-round": _policy(
            skill_name="open-report-writing-round",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_STATE_TRANSITION],
            side_effect_scope=["artifact-write", "db-write:deliberation", "db-write:runtime"],
            db_write_planes=["deliberation", "runtime"],
            input_object_kinds=["transition-request", "round-transition", "reporting-object"],
            output_object_kinds=["round-transition", "round-task"],
            write_scope=WRITE_SCOPE_STATE_TRANSITION,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "post-board-note": _policy(
            skill_name="post-board-note",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=RESEARCH_ROLES,
            required_capabilities=[CAPABILITY_BOARD_NOTE_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["board-state"],
            output_object_kinds=["board-note"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "update-hypothesis-status": _policy(
            skill_name="update-hypothesis-status",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *INVESTIGATOR_ROLES],
            required_capabilities=[CAPABILITY_HYPOTHESIS_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["hypothesis", "finding", "evidence-bundle", "proposal"],
            output_object_kinds=["hypothesis"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "open-challenge-ticket": _policy(
            skill_name="open-challenge-ticket",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_CHALLENGER, ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_CHALLENGE_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["board-state", "proposal"],
            output_object_kinds=["challenge"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_CHALLENGER,
        ),
        "close-challenge-ticket": _policy(
            skill_name="close-challenge-ticket",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_CHALLENGER, ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_CHALLENGE_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["challenge"],
            output_object_kinds=["challenge"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_CHALLENGER,
        ),
        "claim-board-task": _policy(
            skill_name="claim-board-task",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_BOARD_TASK_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["board-task"],
            output_object_kinds=["board-task"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "open-followup-from-review-comment": _policy(
            skill_name="open-followup-from-review-comment",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[
                CAPABILITY_CHALLENGE_WRITE,
                CAPABILITY_BOARD_TASK_WRITE,
            ],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["review-comment"],
            output_object_kinds=["challenge", "board-task"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "submit-council-proposal": _policy(
            skill_name="submit-council-proposal",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *INVESTIGATOR_ROLES, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_PROPOSAL_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["finding", "evidence-bundle", "board-state"],
            output_object_kinds=["proposal"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "submit-readiness-opinion": _policy(
            skill_name="submit-readiness-opinion",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *INVESTIGATOR_ROLES, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_READINESS_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["proposal", "finding", "board-state"],
            output_object_kinds=["readiness-opinion"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "submit-investigation-plan": _policy(
            skill_name="submit-investigation-plan",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["mission", "subissue", "investigation-scope"],
            output_object_kinds=["investigation-plan"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "submit-investigation-scope": _policy(
            skill_name="submit-investigation-scope",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *INVESTIGATOR_ROLES, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["mission", "investigation-plan", "subissue"],
            output_object_kinds=["investigation-scope"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "submit-round-brief": _policy(
            skill_name="submit-round-brief",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["mission", "investigation-plan", "subissue", "context-packet"],
            output_object_kinds=["round-brief"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "submit-round-synthesis": _policy(
            skill_name="submit-round-synthesis",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=[
                "round-brief",
                "context-packet",
                "evidence-request",
                "source-acquisition-proposal",
                "evidence-route-assessment",
                "finding",
                "evidence-bundle",
                "hypothesis",
                "challenge",
                "readiness-opinion",
            ],
            output_object_kinds=["round-synthesis"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "materialize-context-packet": _policy(
            skill_name="materialize-context-packet",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=[
                "investigation-plan",
                "subissue",
                "investigation-scope",
                "round-brief",
                "evidence-request",
                "evidence-route-assessment",
                "agent-position",
                "challenge-disposition",
                "review-comment",
                "finding",
                "evidence-bundle",
                "proposal",
                "readiness-opinion",
            ],
            output_object_kinds=["context-packet"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "submit-evidence-request": _policy(
            skill_name="submit-evidence-request",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *INVESTIGATOR_ROLES, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["mission", "subissue", "investigation-scope", "challenge"],
            output_object_kinds=["evidence-request"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "submit-source-acquisition-proposal": _policy(
            skill_name="submit-source-acquisition-proposal",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[*FETCH_NORMALIZE_ROLES],
            required_capabilities=[CAPABILITY_PROPOSAL_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["evidence-request", "challenge", "finding", "round"],
            output_object_kinds=["source-acquisition-proposal"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "submit-evidence-route-assessment": _policy(
            skill_name="submit-evidence-route-assessment",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[*INVESTIGATOR_ROLES, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=[
                "evidence-request",
                "source-acquisition-proposal",
                "agent-position",
                "readiness-opinion",
                "challenge",
                "round",
            ],
            output_object_kinds=["evidence-route-assessment"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "update-source-acquisition-proposal-status": _policy(
            skill_name="update-source-acquisition-proposal-status",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *FETCH_NORMALIZE_ROLES],
            required_capabilities=[CAPABILITY_PROPOSAL_WRITE],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["source-acquisition-proposal", "fetch-receipt"],
            output_object_kinds=["source-acquisition-proposal"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "link-source-acquisition-execution": _policy(
            skill_name="link-source-acquisition-execution",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *FETCH_NORMALIZE_ROLES],
            required_capabilities=[CAPABILITY_PROPOSAL_WRITE],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=[
                "source-acquisition-proposal",
                "fetch-receipt",
                "normalized-signal",
            ],
            output_object_kinds=["source-acquisition-proposal"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "submit-agent-position": _policy(
            skill_name="submit-agent-position",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, *INVESTIGATOR_ROLES, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["finding", "evidence-bundle", "subissue", "investigation-scope", "challenge"],
            output_object_kinds=["agent-position"],
            write_scope=WRITE_SCOPE_DELIBERATION,
        ),
        "submit-challenge-disposition": _policy(
            skill_name="submit-challenge-disposition",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, ROLE_CHALLENGER],
            required_capabilities=[CAPABILITY_DISCUSSION_WRITE],
            side_effect_scope=["artifact-write", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=[
                "review-comment",
                "challenge",
                "evidence-bundle",
                "finding",
                "agent-position",
            ],
            output_object_kinds=["challenge-disposition"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "summarize-board-state": _policy(
            skill_name="summarize-board-state",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR],
            required_capabilities=[CAPABILITY_DERIVED_EXPORT],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=["board-state"],
            output_object_kinds=["board-summary"],
            write_scope=WRITE_SCOPE_ARTIFACT,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "materialize-board-brief": _policy(
            skill_name="materialize-board-brief",
            skill_layer=SKILL_LAYER_DELIBERATION_WRITE,
            allowed_roles=[ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR],
            required_capabilities=[CAPABILITY_DERIVED_EXPORT],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=["board-state"],
            output_object_kinds=["board-brief"],
            write_scope=WRITE_SCOPE_ARTIFACT,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "plan-round-orchestration": _policy(
            skill_name="plan-round-orchestration",
            skill_layer=SKILL_LAYER_OPTIONAL_ANALYSIS,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_ANALYSIS],
            side_effect_scope=["artifact-write", "db-read", "db-write:runtime", "db-write:deliberation"],
            db_write_planes=["runtime", "deliberation"],
            input_object_kinds=["board-state", "proposal", "readiness-opinion"],
            output_object_kinds=["orchestration-plan", "orchestration-plan-step"],
            write_scope=WRITE_SCOPE_RUNTIME,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "propose-next-actions": _policy(
            skill_name="propose-next-actions",
            skill_layer=SKILL_LAYER_OPTIONAL_ANALYSIS,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_ANALYSIS],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["board-state", "issue-cluster", "proposal"],
            output_object_kinds=["next-action"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "open-falsification-probe": _policy(
            skill_name="open-falsification-probe",
            skill_layer=SKILL_LAYER_OPTIONAL_ANALYSIS,
            allowed_roles=[ROLE_CHALLENGER, ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_PROBE_WRITE],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=[
                "proposal",
                "next-action",
                "hypothesis",
                "challenge",
                "evidence-bundle",
                "issue-cluster",
            ],
            output_object_kinds=["probe"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_CHALLENGER,
        ),
        "summarize-round-readiness": _policy(
            skill_name="summarize-round-readiness",
            skill_layer=SKILL_LAYER_OPTIONAL_ANALYSIS,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_ANALYSIS],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["proposal", "readiness-opinion", "probe"],
            output_object_kinds=["readiness-assessment"],
            write_scope=WRITE_SCOPE_DELIBERATION,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "freeze-report-basis": _policy(
            skill_name="freeze-report-basis",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_STATE_TRANSITION],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["transition-request", "proposal", "readiness-assessment"],
            output_object_kinds=["report-basis-freeze"],
            write_scope=WRITE_SCOPE_STATE_TRANSITION,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "archive-signal-corpus": _policy(
            skill_name="archive-signal-corpus",
            skill_layer=SKILL_LAYER_RUNTIME_ADMIN,
            allowed_roles=[ROLE_RUNTIME_OPERATOR],
            required_capabilities=[CAPABILITY_ARCHIVE_WRITE],
            side_effect_scope=["artifact-write", "shared-archive-write", "db-read"],
            db_write_planes=["archive"],
            input_object_kinds=["normalized-signal"],
            output_object_kinds=["archive-import"],
            write_scope=WRITE_SCOPE_ARCHIVE,
            default_actor_role_hint=ROLE_RUNTIME_OPERATOR,
        ),
        "archive-case-library": _policy(
            skill_name="archive-case-library",
            skill_layer=SKILL_LAYER_RUNTIME_ADMIN,
            allowed_roles=[ROLE_RUNTIME_OPERATOR],
            required_capabilities=[CAPABILITY_ARCHIVE_WRITE],
            side_effect_scope=["artifact-write", "shared-archive-write", "db-read"],
            db_write_planes=["archive"],
            input_object_kinds=["reporting-handoff", "council-decision"],
            output_object_kinds=["archive-import"],
            write_scope=WRITE_SCOPE_ARCHIVE,
            default_actor_role_hint=ROLE_RUNTIME_OPERATOR,
        ),
        "materialize-history-context": _policy(
            skill_name="materialize-history-context",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR],
            required_capabilities=[CAPABILITY_DERIVED_EXPORT],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=["archive-state", "round-state"],
            output_object_kinds=["history-context"],
            write_scope=WRITE_SCOPE_ARTIFACT,
            default_actor_role_hint=ROLE_RUNTIME_OPERATOR,
        ),
        "materialize-reporting-handoff": _policy(
            skill_name="materialize-reporting-handoff",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=REPORTING_ROLES,
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["report-basis-freeze", "runtime-control-freeze", "finding", "evidence-bundle", "proposal", "readiness-opinion"],
            output_object_kinds=["reporting-handoff"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "materialize-spatiotemporal-relation-evidence-packet": _policy(
            skill_name="materialize-spatiotemporal-relation-evidence-packet",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[
                ROLE_MODERATOR,
                ROLE_REPORT_EDITOR,
            ],
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=[
                "artifact-write",
                "db-read",
                "db-write:deliberation",
                "db-write:reporting",
            ],
            db_write_planes=["deliberation", "reporting"],
            input_object_kinds=[
                "spatiotemporal-relation-cue",
                "challenge",
                "probe",
                "review-comment",
            ],
            output_object_kinds=[
                "spatiotemporal-relation-evidence-packet",
                "finding",
                "evidence-bundle",
                "report-section-draft",
            ],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_REPORT_EDITOR,
        ),
        "draft-council-decision": _policy(
            skill_name="draft-council-decision",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=REPORTING_ROLES,
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["reporting-handoff", "report-basis-freeze"],
            output_object_kinds=["council-decision"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "draft-expert-report": _policy(
            skill_name="draft-expert-report",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_REPORT_EDITOR],
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["reporting-handoff", "council-decision", "report-section-draft"],
            output_object_kinds=["expert-report"],
            write_scope=WRITE_SCOPE_REPORTING,
            default_actor_role_hint=ROLE_REPORT_EDITOR,
        ),
        "draft-narrative-report": _policy(
            skill_name="draft-narrative-report",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_REPORT_EDITOR],
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=[
                "final-publication",
                "council-decision",
                "expert-report",
                "report-basis-freeze",
                "finding",
                "evidence-bundle",
                "agent-position",
            ],
            output_object_kinds=["narrative-report-draft"],
            write_scope=WRITE_SCOPE_REPORTING,
            default_actor_role_hint=ROLE_REPORT_EDITOR,
        ),
        "validate-narrative-report": _policy(
            skill_name="validate-narrative-report",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_REPORT_EDITOR, ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=["narrative-report-draft", "reporting-object"],
            output_object_kinds=["narrative-report-validation"],
            write_scope=WRITE_SCOPE_REPORTING,
            default_actor_role_hint=ROLE_REPORT_EDITOR,
        ),
        "publish-narrative-report": _policy(
            skill_name="publish-narrative-report",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_REPORT_EDITOR, ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_REPORT_PUBLISH],
            side_effect_scope=["artifact-write", "db-read"],
            db_write_planes=[],
            input_object_kinds=["narrative-report-draft", "narrative-report-validation"],
            output_object_kinds=["narrative-report"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_REPORT_EDITOR,
        ),
        "publish-expert-report": _policy(
            skill_name="publish-expert-report",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_REPORT_EDITOR],
            required_capabilities=[CAPABILITY_REPORT_PUBLISH],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["expert-report"],
            output_object_kinds=["expert-report"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_REPORT_EDITOR,
        ),
        "publish-council-decision": _policy(
            skill_name="publish-council-decision",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=[ROLE_MODERATOR, ROLE_REPORT_EDITOR],
            required_capabilities=[CAPABILITY_REPORT_PUBLISH],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["council-decision"],
            output_object_kinds=["council-decision"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "materialize-final-publication": _policy(
            skill_name="materialize-final-publication",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=REPORTING_ROLES,
            required_capabilities=[CAPABILITY_REPORT_PUBLISH],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["reporting-handoff", "council-decision", "expert-report", "report-basis-freeze"],
            output_object_kinds=["final-publication"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
    }
)


def helper_governance_metadata(skill_name: str) -> dict[str, Any]:
    freeze_line = dict(OPTIONAL_ANALYSIS_HELPER_FREEZE_LINES.get(maybe_text(skill_name), {}))
    if not freeze_line:
        return {}
    decision_source = maybe_text(freeze_line.get("decision_source")) or "approved-helper-view"
    if decision_source not in OPTIONAL_HELPER_ALLOWED_DECISION_SOURCES:
        raise ValueError(
            f"Unsupported optional helper decision_source `{decision_source}` for {skill_name}."
        )
    return {
        "decision_source": decision_source,
        "rule_id": maybe_text(freeze_line.get("rule_id")),
        "rule_version": maybe_text(freeze_line.get("rule_version"))
        or "optional-analysis-freeze-line-2026-04-28",
        "taxonomy_version": maybe_text(freeze_line.get("taxonomy_version")),
        "rubric_version": maybe_text(freeze_line.get("rubric_version")),
        "approval_ref": maybe_text(freeze_line.get("approval_ref")),
        "audit_ref": maybe_text(freeze_line.get("audit_ref")),
        "rule_trace": unique_texts(
            freeze_line.get("rule_trace", [])
            if isinstance(freeze_line.get("rule_trace"), list)
            else []
        ),
        "caveats": unique_texts(
            freeze_line.get("caveats", [])
            if isinstance(freeze_line.get("caveats"), list)
            else [
                "Helper output is advisory only until a versioned human audit approves its rule family.",
                "Helper output must be cited through DB council objects before any report-basis use.",
            ]
        ),
        "audit_status": maybe_text(freeze_line.get("audit_status"))
        or "default-frozen; approval-required; audit-pending",
        "helper_status": maybe_text(freeze_line.get("helper_status"))
        or "approval-gated-helper-view",
        "helper_destination": maybe_text(freeze_line.get("destination")),
    }


SKILL_CONTRACT_METADATA_SCHEMA_VERSION = "openclaw-skill-contract-metadata-v1"
SKILL_CONTRACT_FIELDS = (
    "observes",
    "cannot_prove",
    "requires",
    "produces",
    "followups",
    "failure_recovery",
    "claim_limits",
    "report_uses",
    "owner_roles",
)


SKILL_CONTRACT_OVERRIDES: dict[str, dict[str, list[str]]] = {
    "fetch-gdelt-doc-search": {
        "observes": [
            "GDELT DOC indexed articles, timeline rows, and DOC tone aggregates for agent-authored queries.",
        ],
        "cannot_prove": [
            "Public sentiment, representative public opinion, official-policy completeness, or physical source attribution.",
        ],
        "followups": [
            "normalize-gdelt-doc-public-signals",
            "materialize-public-discourse-corpus",
            "audit-public-discourse-sample-coverage",
        ],
        "failure_recovery": [
            "Revise DOC query syntax, window, domain filters, or use GDELT Events/Mentions/GKG as same-family follow-up.",
            "Record source-limit rationale if stopping.",
        ],
        "claim_limits": [
            "GDELT tone is media/document tone only and must not be written as public sentiment.",
        ],
    },
    "normalize-gdelt-doc-public-signals": {
        "observes": [
            "Normalized GDELT DOC article discovery rows and DOC tone aggregate signals.",
        ],
        "cannot_prove": [
            "Public sentiment, public emotion distribution, or source attribution.",
        ],
        "claim_limits": [
            "Use DOC tone as media/document tone; do not use it as public semantic denominator.",
        ],
    },
    "materialize-public-discourse-corpus": {
        "observes": [
            "DB-visible public, media, or formal text-bearing normalized signals selected by source family, keyword, query variant, window, and round scope.",
        ],
        "cannot_prove": [
            "Representative public opinion, issue prevalence outside the sample, or evidence absence for unobserved source families.",
        ],
        "requires": [
            "Normalized public/formal text signals and an agent-defined sample definition.",
        ],
        "produces": [
            "materialized_public_policy_corpus",
            "sample_definition",
            "query_variants",
            "source_family_denominators",
            "eligible_count",
            "dedup_count",
            "source_limit_records",
            "source_family_counts",
        ],
        "followups": [
            "audit-public-discourse-sample-coverage",
            "classify-public-discourse-affect",
            "classify-formal-comment-issues",
        ],
        "claim_limits": [
            "Corpus rows support sample definition and item-level examples only until coverage, annotation, aggregation, and source-family-local denominator are present.",
            "GDELT, YouTube, Bluesky, formal-record, and formal-comment denominators must not be mixed.",
        ],
    },
    "audit-public-discourse-sample-coverage": {
        "observes": [
            "Source-family coverage, query/window/sample limits, eligible/dedup counts, acquisition-attempt outcomes, missing source layers, and denominator separation cues.",
        ],
        "cannot_prove": [
            "Representativeness, public consensus, or absence of discourse outside the current sample.",
        ],
        "produces": [
            "public_policy_corpus_coverage_audit",
            "source_limit_rationale",
            "source_family_audit",
            "source_acquisition_attempt_audit",
            "coverage_cues",
        ],
        "followups": [
            "classify-public-discourse-affect",
            "aggregate-public-discourse-annotations",
            "submit-evidence-route-assessment",
        ],
    },
    "classify-public-discourse-affect": {
        "observes": [
            "Bounded sample-local public/media/formal text labels including affect, issue frames, policy demand, trust/confidence, uncertainty, narratives, responsibility cues, and formal-policy semantics.",
        ],
        "cannot_prove": [
            "Population-level sentiment, mutually exclusive opinion shares, or policy effectiveness.",
        ],
        "followups": ["aggregate-public-discourse-annotations"],
        "claim_limits": [
            "Labels remain item/sample-local and require source-family/discourse-lane local aggregation denominators before report percentages.",
            "Formal comment and formal-record semantic labels are not public sentiment.",
        ],
    },
    "aggregate-public-discourse-annotations": {
        "observes": [
            "Sample-local annotation distributions with source-family, discourse-lane, label-family, and semantic-scope denominators.",
        ],
        "cannot_prove": [
            "General public opinion, mutually exclusive population shares, or policy success/failure.",
        ],
        "produces": [
            "semantic_aggregate",
            "distribution_denominators",
            "semantic_distributions",
            "sample-local label distributions",
        ],
        "followups": [
            "compare-public-media-narratives",
            "summarize-public-discourse-sample",
            "materialize-reporting-handoff",
        ],
        "claim_limits": [
            "Fractions are sample-local and label-family-local; non-mutually exclusive labels must not be summed to 100 percent.",
            "Use scoped semantic distributions for public/media/formal proportions; do not mix GDELT, social-platform, formal-comment, and formal-record denominators.",
        ],
    },
    "compare-public-media-narratives": {
        "observes": [
            "Bounded contrasts among social sample affect, media/document tone, formal comments, and source narratives.",
        ],
        "cannot_prove": [
            "Alignment scores, causal interaction, public consensus, or physical source attribution.",
        ],
        "claim_limits": [
            "GDELT media tone, social sample affect, and formal comment samples must remain separate denominators.",
        ],
    },
    "aggregate-environment-evidence": {
        "observes": [
            "Descriptive environmental, operations, and observation aggregates from normalized signal rows.",
        ],
        "cannot_prove": [
            "Policy success/failure, responsibility, exposure health impact, causality, or public sentiment.",
        ],
        "claim_limits": [
            "Environment aggregates support trend, peak, and operating-status descriptions only within stated source/window limits.",
        ],
    },
    "materialize-claim-gap-action-cards": {
        "observes": [
            "Mission focus, council objects, normalized signal counts, helper artifacts, source acquisition attempt states, open challenges, and report readiness gaps.",
        ],
        "cannot_prove": [
            "Evidence truth, source sufficiency, report readiness, priority, or which skill should run next.",
        ],
        "requires": [
            "Run directory, run id, round id, and available DB/artifact surfaces.",
        ],
        "produces": [
            "claim_gap_action_cards",
            "recovery_cards",
            "source_limit_cards",
            "report_boundary_advisory",
        ],
        "followups": [
            "submit-evidence-request",
            "submit-source-acquisition-proposal",
            "submit-evidence-route-assessment",
            "submit-round-synthesis",
            "materialize-reporting-handoff",
        ],
        "failure_recovery": [
            "If input surfaces are missing, emit cards that identify missing input surfaces and report boundaries without blocking the round.",
        ],
        "claim_limits": [
            "Action cards are not a scheduler, source ranking, score, gate, or automatic execution queue.",
        ],
        "report_uses": [
            "Expose claim-basis gaps, optional reinforcement paths, and bounded report wording when reinforcement is not done.",
        ],
    },
    "build-fact-policy-public-interaction-timeline": {
        "observes": [
            "Environment, formal, and public signal timestamps; public discourse helper artifacts; and source-family denominator context.",
        ],
        "cannot_prove": [
            "Causality, policy impact, public response attribution, representative public opinion, evidence absence, or report readiness.",
        ],
        "requires": [
            "DB-backed normalized signals with timestamps and optional public discourse corpus/coverage/annotation/summary artifacts.",
        ],
        "produces": [
            "fact_policy_public_interaction_timeline",
            "interaction_nodes",
            "parallel_timeline_nodes",
            "section-brief-ready timeline metadata",
        ],
        "followups": [
            "materialize-reporting-handoff",
            "submit-round-brief",
            "submit-round-synthesis",
            "submit-evidence-bundle",
        ],
        "failure_recovery": [
            "If one side of the timeline is missing, emit one-sided context and report boundaries rather than an interaction claim.",
        ],
        "claim_limits": [
            "Timeline nodes are descriptive same-window context only; they do not rank events, schedule sources, or establish response/influence.",
            "Public semantic interpretation still requires corpus, coverage, denominator, and council/reporting uptake.",
        ],
        "report_uses": [
            "Advisory section-brief input for bounded chronology and communication-gap wording after explicit uptake.",
        ],
    },
    "materialize-reporting-handoff": {
        "observes": [
            "Frozen/reporting basis, readiness state, supervisor state, council basis objects, optional action-card artifacts, and optional interaction-timeline artifacts.",
        ],
        "cannot_prove": [
            "New facts, new evidence sufficiency, or report claim truth.",
        ],
        "claim_limits": [
            "Handoff carries report basis and limitations only; report-editor must not add facts outside frozen/council basis.",
        ],
    },
    "draft-narrative-report": {
        "observes": [
            "Frozen/reporting basis, section drafts, council objects, and carried helper artifacts.",
        ],
        "cannot_prove": [
            "New facts, new causal claims, representative public opinion, or policy success/failure.",
        ],
        "followups": ["validate-narrative-report"],
        "claim_limits": [
            "Draft prose may synthesize carried basis only and must downgrade claim families missing their basis.",
        ],
    },
    "validate-narrative-report": {
        "observes": [
            "Narrative draft text, visible basis metadata, citations, helper-carried status, and claim-boundary hazards.",
        ],
        "cannot_prove": [
            "Underlying evidence truth or policy correctness.",
        ],
        "followups": ["publish-narrative-report", "draft-narrative-report"],
        "claim_limits": [
            "Validator flags basis gaps for downgrade, deletion, or follow-up; it is not a separate situation-analysis validator path.",
        ],
    },
}


def _generic_skill_contract_metadata(
    skill_name: str,
    policy: dict[str, Any],
) -> dict[str, Any]:
    layer = maybe_text(policy.get("skill_layer"))
    allowed_roles = unique_texts(
        policy.get("allowed_roles", [])
        if isinstance(policy.get("allowed_roles"), list)
        else []
    )
    produces = unique_texts(
        policy.get("output_object_kinds", [])
        if isinstance(policy.get("output_object_kinds"), list)
        else []
    )
    requires = unique_texts(
        policy.get("input_object_kinds", [])
        if isinstance(policy.get("input_object_kinds"), list)
        else []
    )
    if layer == SKILL_LAYER_FETCH:
        source_config = SOURCE_CATALOG.get(skill_name, {})
        normalizer = maybe_text(source_config.get("normalizer_skill"))
        return {
            "schema_version": SKILL_CONTRACT_METADATA_SCHEMA_VERSION,
            "observes": [
                "Provider or imported raw records within agent-selected query, time, spatial, identifier, and provider-mode limits.",
            ],
            "cannot_prove": [
                "Real-world evidence absence, evidence sufficiency, report readiness, public sentiment, or policy conclusions.",
            ],
            "requires": requires
            or [
                "mission focus",
                "agent-defined evidence need",
                "source parameters",
            ],
            "produces": produces
            or ["raw-artifact", "fetch receipt", "source-limit metadata"],
            "followups": unique_texts(
                [
                    normalizer,
                    "normalize-fetch-execution",
                    "link-source-acquisition-execution",
                ]
            ),
            "failure_recovery": [
                "Revise query terms, identifiers, provider mode, time window, bbox, or same-family route before treating the route as exhausted.",
                "If stopping, record a source-limit rationale and report boundary.",
            ],
            "claim_limits": [
                "Zero, failed, blocked, or receipt-only output is an acquisition attempt result, not evidence absence.",
            ],
            "report_uses": [
                "Provider provenance, acquisition audit, raw source citation candidate, and source-limit rationale after council uptake.",
            ],
            "owner_roles": allowed_roles,
        }
    if layer == SKILL_LAYER_NORMALIZE:
        return {
            "schema_version": SKILL_CONTRACT_METADATA_SCHEMA_VERSION,
            "observes": [
                "Raw artifact records converted into canonical signal-plane rows with provenance and quality flags.",
            ],
            "cannot_prove": [
                "Evidence truth, source sufficiency, claim acceptance, public opinion, or policy conclusions.",
            ],
            "requires": requires or ["raw-artifact"],
            "produces": produces or ["normalized-signal", "normalization receipt"],
            "followups": [
                "query-normalized-signal",
                "query-public-signals",
                "query-formal-signals",
                "query-environment-signals",
                "link-source-acquisition-execution",
            ],
            "failure_recovery": [
                "Inspect raw artifact shape, provider metadata, normalizer warnings, and whether receipt-only lineage must be carried as a source-limit.",
            ],
            "claim_limits": [
                "Normalized row presence is evidence availability metadata; interpretation belongs to council findings or approved helper artifacts.",
            ],
            "report_uses": [
                "DB-backed citation candidate, source coverage audit input, and helper-analysis input after council uptake.",
            ],
            "owner_roles": allowed_roles,
        }
    if layer == SKILL_LAYER_QUERY:
        return {
            "schema_version": SKILL_CONTRACT_METADATA_SCHEMA_VERSION,
            "observes": [
                "Existing DB rows matching explicit query filters.",
            ],
            "cannot_prove": [
                "Evidence absence outside the queried DB, source sufficiency, or report readiness.",
            ],
            "requires": requires or ["DB state", "query parameters"],
            "produces": produces or ["query-response"],
            "followups": [
                "submit-finding-record",
                "submit-evidence-bundle",
                "materialize-claim-gap-action-cards",
            ],
            "failure_recovery": [
                "Widen query filters, check round/run scope, inspect acquisition attempts, or record a source-limit rationale.",
            ],
            "claim_limits": [
                "Empty query results describe the current DB/query scope only and must not become evidence absence.",
            ],
            "report_uses": [
                "DB inspection, citation lookup, and visible basis audit.",
            ],
            "owner_roles": allowed_roles,
        }
    if layer == SKILL_LAYER_OPTIONAL_ANALYSIS:
        return {
            "schema_version": SKILL_CONTRACT_METADATA_SCHEMA_VERSION,
            "observes": [
                "Approved helper inputs and DB/artifact surfaces inside explicit helper scope.",
            ],
            "cannot_prove": [
                "Claim truth, evidence sufficiency, source ranking, report readiness, or phase transition eligibility.",
            ],
            "requires": requires or ["normalized-signal", "analysis-context"],
            "produces": produces or ["analysis-object", "helper artifact"],
            "followups": [
                "submit-finding-record",
                "submit-evidence-bundle",
                "submit-readiness-opinion",
                "materialize-reporting-handoff",
            ],
            "failure_recovery": [
                "Carry helper warnings into council objects or report boundary instead of treating sparse helper output as proof of absence.",
            ],
            "claim_limits": [
                "Helper output is advisory until carried by finding, evidence bundle, readiness opinion, synthesis, report section, or report basis.",
            ],
            "report_uses": [
                "Advisory analysis basis after explicit council/reporting uptake.",
            ],
            "owner_roles": allowed_roles,
        }
    if layer == SKILL_LAYER_REPORTING:
        return {
            "schema_version": SKILL_CONTRACT_METADATA_SCHEMA_VERSION,
            "observes": [
                "Frozen/council/reporting basis and report artifacts.",
            ],
            "cannot_prove": [
                "New facts or evidence truth.",
            ],
            "requires": requires or ["reporting-object"],
            "produces": produces or ["reporting artifact"],
            "followups": [],
            "failure_recovery": [
                "Downgrade unsupported prose, cite carried basis, or return the gap to moderator/challenger as a council object.",
            ],
            "claim_limits": [
                "Reporting skills must not add substantive claims outside frozen/reporting/council basis.",
            ],
            "report_uses": ["Report handoff, draft, validation, publication, or final publication artifact."],
            "owner_roles": allowed_roles,
        }
    return {
        "schema_version": SKILL_CONTRACT_METADATA_SCHEMA_VERSION,
        "observes": ["Governed runtime or council state within this skill's declared scope."],
        "cannot_prove": ["Evidence truth, sufficiency, or source ranking."],
        "requires": requires,
        "produces": produces,
        "followups": [],
        "failure_recovery": ["Record limitation or route assessment when the skill cannot satisfy the evidence need."],
        "claim_limits": ["This contract is advisory and does not rank, gate, or schedule evidence work."],
        "report_uses": [],
        "owner_roles": allowed_roles,
    }


def _merge_contract_lists(base: dict[str, Any], override: dict[str, list[str]]) -> dict[str, Any]:
    merged = dict(base)
    for field_name in SKILL_CONTRACT_FIELDS:
        if field_name == "owner_roles":
            continue
        override_values = override.get(field_name)
        if isinstance(override_values, list) and override_values:
            merged[field_name] = unique_texts(
                [
                    *(
                        merged.get(field_name, [])
                        if isinstance(merged.get(field_name), list)
                        else []
                    ),
                    *override_values,
                ]
            )
    return merged


def skill_contract_metadata(skill_name: str, policy: dict[str, Any]) -> dict[str, Any]:
    normalized_name = maybe_text(skill_name)
    contract = _generic_skill_contract_metadata(normalized_name, policy)
    contract = _merge_contract_lists(
        contract,
        SKILL_CONTRACT_OVERRIDES.get(normalized_name, {}),
    )
    for field_name in SKILL_CONTRACT_FIELDS:
        if field_name not in contract:
            contract[field_name] = []
    contract["contract_semantics"] = (
        "Claim-basis advisory metadata for agent entry, action cards, reporting "
        "handoff, and validation. It is not a runtime hard gate, source ranking, "
        "source scheduler, or automatic execution rule."
    )
    return contract


for _skill_name, _policy_payload in POLICIES.items():
    _metadata = helper_governance_metadata(_skill_name)
    if _metadata:
        _policy_payload["helper_governance"] = _metadata
    _boundary_metadata = skill_boundary_metadata(_skill_name)
    if _boundary_metadata:
        _policy_payload["skill_boundary"] = _boundary_metadata
    _policy_payload["skill_contract"] = skill_contract_metadata(
        _skill_name,
        _policy_payload,
    )


def available_skill_names(root: Path | None = None) -> list[str]:
    resolved_root = root or workspace_root()
    skills_root = resolved_root / "skills"
    if not skills_root.exists():
        return []
    return sorted(child.name for child in skills_root.iterdir() if child.is_dir())


def validate_skill_registry(root: Path | None = None) -> None:
    actual = set(available_skill_names(root))
    declared = set(POLICIES)
    missing = sorted(actual - declared)
    extra = sorted(declared - actual)
    if missing or extra:
        messages: list[str] = []
        if missing:
            messages.append(f"missing policies: {', '.join(missing)}")
        if extra:
            messages.append(f"unknown policies: {', '.join(extra)}")
        raise ValueError("Skill registry coverage mismatch: " + "; ".join(messages))


def resolve_skill_policy(skill_name: str, root: Path | None = None) -> dict[str, Any]:
    validate_skill_registry(root)
    policy = POLICIES.get(maybe_text(skill_name))
    if not isinstance(policy, dict):
        raise ValueError(f"Unknown skill policy: {skill_name}")
    return {
        "skill_name": maybe_text(policy.get("skill_name")),
        "skill_layer": maybe_text(policy.get("skill_layer")),
        "allowed_roles": unique_texts(policy.get("allowed_roles", [])),
        "denied_roles": unique_texts(policy.get("denied_roles", [])),
        "required_capabilities": unique_texts(policy.get("required_capabilities", [])),
        "side_effect_scope": unique_texts(policy.get("side_effect_scope", [])),
        "db_write_planes": unique_texts(policy.get("db_write_planes", [])),
        "input_object_kinds": unique_texts(policy.get("input_object_kinds", [])),
        "output_object_kinds": unique_texts(policy.get("output_object_kinds", [])),
        "write_scope": maybe_text(policy.get("write_scope")) or WRITE_SCOPE_READ_ONLY,
        "requires_operator_approval": bool(policy.get("requires_operator_approval")),
        "default_actor_role_hint": maybe_text(policy.get("default_actor_role_hint")),
        "helper_governance": dict(policy.get("helper_governance", {}))
        if isinstance(policy.get("helper_governance"), dict)
        else {},
        "skill_boundary": dict(policy.get("skill_boundary", {}))
        if isinstance(policy.get("skill_boundary"), dict)
        else {},
        "skill_contract": dict(policy.get("skill_contract", {}))
        if isinstance(policy.get("skill_contract"), dict)
        else {},
    }


def skill_requires_write_actor_role(skill_name: str) -> bool:
    return skill_write_scope(skill_name) != WRITE_SCOPE_READ_ONLY


def skill_write_scope(skill_name: str) -> str:
    return maybe_text(resolve_skill_policy(skill_name).get("write_scope")) or WRITE_SCOPE_READ_ONLY


def default_actor_role_hint(skill_name: str) -> str:
    policy = resolve_skill_policy(skill_name)
    explicit = maybe_text(policy.get("default_actor_role_hint"))
    if explicit:
        return explicit
    allowed_roles = policy.get("allowed_roles", []) if isinstance(policy.get("allowed_roles"), list) else []
    if len(allowed_roles) == 1:
        return maybe_text(allowed_roles[0])
    return "<actor_role>"


def skill_registry_snapshot(root: Path | None = None) -> dict[str, Any]:
    validate_skill_registry(root)
    skills = [resolve_skill_policy(name, root) for name in available_skill_names(root)]
    layer_counts: dict[str, int] = {}
    write_scope_counts: dict[str, int] = {}
    approval_required_count = 0
    for skill in skills:
        layer = maybe_text(skill.get("skill_layer")) or "unknown"
        layer_counts[layer] = int(layer_counts.get(layer) or 0) + 1
        write_scope = maybe_text(skill.get("write_scope")) or WRITE_SCOPE_READ_ONLY
        write_scope_counts[write_scope] = int(write_scope_counts.get(write_scope) or 0) + 1
        if bool(skill.get("requires_operator_approval")):
            approval_required_count += 1
    return {
        "schema_version": "runtime-skill-access-registry-v1",
        "skill_count": len(skills),
        "operator_approval_required_count": approval_required_count,
        "skill_layer_counts": layer_counts,
        "write_scope_counts": write_scope_counts,
        "skills": skills,
    }


__all__ = [
    "POLICIES",
    "SKILL_LAYER_DELIBERATION_WRITE",
    "SKILL_LAYER_FETCH",
    "SKILL_LAYER_NORMALIZE",
    "SKILL_LAYER_OPTIONAL_ANALYSIS",
    "SKILL_LAYER_QUERY",
    "SKILL_LAYER_REPORTING",
    "SKILL_LAYER_RUNTIME_ADMIN",
    "SKILL_LAYER_STATE_TRANSITION",
    "WRITE_SCOPE_ANALYSIS",
    "WRITE_SCOPE_ARCHIVE",
    "WRITE_SCOPE_ARTIFACT",
    "WRITE_SCOPE_DELIBERATION",
    "WRITE_SCOPE_READ_ONLY",
    "WRITE_SCOPE_REPORTING",
    "WRITE_SCOPE_RUNTIME",
    "WRITE_SCOPE_SIGNAL",
    "WRITE_SCOPE_STATE_TRANSITION",
    "EVIDENCE_ONLY_FORBIDDEN_OUTPUT_FIELDS",
    "EVIDENCE_ONLY_REQUIRED_OUTPUT_FIELDS",
    "FETCH_SKILLS",
    "NORMALIZE_SKILLS",
    "OPTIONAL_ANALYSIS_SKILLS",
    "QUERY_SKILLS",
    "SKILL_CONTRACT_FIELDS",
    "SKILL_CONTRACT_METADATA_SCHEMA_VERSION",
    "available_skill_names",
    "default_actor_role_hint",
    "evidence_only_skill_names",
    "resolve_skill_policy",
    "skill_registry_snapshot",
    "skill_boundary_violations",
    "skill_requires_write_actor_role",
    "skill_write_scope",
    "validate_skill_registry",
    "validate_skill_output_boundary",
    "helper_governance_metadata",
    "skill_boundary_metadata",
    "skill_contract_metadata",
]

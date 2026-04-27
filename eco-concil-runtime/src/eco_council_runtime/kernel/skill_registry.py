from __future__ import annotations

from pathlib import Path
from typing import Any

from .role_contracts import (
    CAPABILITY_ANALYSIS,
    CAPABILITY_ARCHIVE_WRITE,
    CAPABILITY_BOARD_NOTE_WRITE,
    CAPABILITY_BOARD_TASK_WRITE,
    CAPABILITY_CHALLENGE_WRITE,
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
    ROLE_FORMAL_RECORD_INVESTIGATOR,
    ROLE_MODERATOR,
    ROLE_PUBLIC_DISCOURSE_INVESTIGATOR,
    ROLE_REPORT_EDITOR,
    ROLE_RUNTIME_OPERATOR,
)

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
    ROLE_PUBLIC_DISCOURSE_INVESTIGATOR,
    ROLE_FORMAL_RECORD_INVESTIGATOR,
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
    return Path(__file__).resolve().parents[4]


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
    "normalize-regulationsgov-comment-detail-public-signals",
    "normalize-regulationsgov-comments-public-signals",
    "normalize-usgs-water-observation-signals",
    "normalize-youtube-comments-public-signals",
    "normalize-youtube-video-public-signals",
]

OPTIONAL_ANALYSIS_SKILLS = [
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
]

QUERY_SKILLS = [
    "query-board-delta",
    "query-public-signals",
    "query-formal-signals",
    "query-environment-signals",
    "query-normalized-signal",
    "query-raw-record",
    "query-signal-corpus",
    "query-case-library",
]

POLICIES: dict[str, dict[str, Any]] = {}
POLICIES.update(
    _group(
        FETCH_SKILLS,
        skill_layer=SKILL_LAYER_FETCH,
        allowed_roles=INVESTIGATOR_ROLES,
        required_capabilities=[CAPABILITY_FETCH],
        side_effect_scope=["network-external", "artifact-write"],
        db_write_planes=[],
        input_object_kinds=["mission-brief", "source-selection"],
        output_object_kinds=["raw-artifact"],
        write_scope=WRITE_SCOPE_ARTIFACT,
    )
)
POLICIES.update(
    _group(
        NORMALIZE_SKILLS,
        skill_layer=SKILL_LAYER_NORMALIZE,
        allowed_roles=INVESTIGATOR_ROLES,
        required_capabilities=[CAPABILITY_NORMALIZE],
        side_effect_scope=["artifact-read", "artifact-write", "db-write:signal"],
        db_write_planes=["signal"],
        input_object_kinds=["raw-artifact"],
        output_object_kinds=["normalized-signal"],
        write_scope=WRITE_SCOPE_SIGNAL,
    )
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
        [name for name in OPTIONAL_ANALYSIS_SKILLS if name != "build-normalization-audit"],
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
POLICIES["build-normalization-audit"] = _policy(
    skill_name="build-normalization-audit",
    skill_layer=SKILL_LAYER_OPTIONAL_ANALYSIS,
    allowed_roles=[ROLE_RUNTIME_OPERATOR],
    required_capabilities=[CAPABILITY_RUNTIME_ADMIN],
    side_effect_scope=["db-read", "artifact-write"],
    db_write_planes=[],
    input_object_kinds=["normalized-signal"],
    output_object_kinds=["normalization-audit"],
    write_scope=WRITE_SCOPE_ARTIFACT,
    requires_operator_approval=True,
    default_actor_role_hint=ROLE_RUNTIME_OPERATOR,
)

POLICIES.update(
    {
        "scaffold-mission-run": _policy(
            skill_name="scaffold-mission-run",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR],
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
            allowed_roles=[ROLE_MODERATOR, ROLE_RUNTIME_OPERATOR],
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
            allowed_roles=[*INVESTIGATOR_ROLES, ROLE_RUNTIME_OPERATOR],
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
            input_object_kinds=["hypothesis", "proposal"],
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
            input_object_kinds=["proposal", "next-action", "issue-cluster"],
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
        "promote-evidence-basis": _policy(
            skill_name="promote-evidence-basis",
            skill_layer=SKILL_LAYER_STATE_TRANSITION,
            allowed_roles=[ROLE_MODERATOR],
            required_capabilities=[CAPABILITY_STATE_TRANSITION],
            side_effect_scope=["artifact-write", "db-read", "db-write:deliberation"],
            db_write_planes=["deliberation"],
            input_object_kinds=["transition-request", "proposal", "readiness-assessment"],
            output_object_kinds=["promotion-basis"],
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
            input_object_kinds=["promotion-basis", "proposal", "issue-cluster"],
            output_object_kinds=["reporting-handoff"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
        "draft-council-decision": _policy(
            skill_name="draft-council-decision",
            skill_layer=SKILL_LAYER_REPORTING,
            allowed_roles=REPORTING_ROLES,
            required_capabilities=[CAPABILITY_REPORT_DRAFT],
            side_effect_scope=["artifact-write", "db-read", "db-write:reporting"],
            db_write_planes=["reporting"],
            input_object_kinds=["reporting-handoff", "promotion-basis"],
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
            input_object_kinds=["reporting-handoff", "council-decision"],
            output_object_kinds=["expert-report"],
            write_scope=WRITE_SCOPE_REPORTING,
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
            input_object_kinds=["council-decision", "expert-report"],
            output_object_kinds=["final-publication"],
            write_scope=WRITE_SCOPE_REPORTING,
            requires_operator_approval=True,
            default_actor_role_hint=ROLE_MODERATOR,
        ),
    }
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
    "available_skill_names",
    "default_actor_role_hint",
    "resolve_skill_policy",
    "skill_registry_snapshot",
    "skill_requires_write_actor_role",
    "skill_write_scope",
    "validate_skill_registry",
]

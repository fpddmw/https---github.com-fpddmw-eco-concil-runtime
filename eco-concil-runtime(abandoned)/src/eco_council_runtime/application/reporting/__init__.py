"""Application services for reporting, review, and decision workflows."""

from __future__ import annotations

from importlib import import_module

_EXPORT_TO_MODULE = {
    "PROMOTABLE_REPORT_ROLES": "eco_council_runtime.application.reporting.artifact_support",
    "READINESS_ROLES": "eco_council_runtime.application.reporting.artifact_support",
    "REPORT_ROLES": "eco_council_runtime.application.reporting.artifact_support",
    "build_claim_curation_draft": "eco_council_runtime.application.reporting.packets",
    "build_claim_curation_packet": "eco_council_runtime.application.reporting.packets",
    "build_data_readiness_draft": "eco_council_runtime.application.reporting.readiness",
    "build_data_readiness_packet": "eco_council_runtime.application.reporting.packets",
    "build_decision_draft_from_state": "eco_council_runtime.application.reporting.council_decision",
    "build_decision_missing_types": "eco_council_runtime.application.reporting.council_decision",
    "build_decision_packet_from_state": "eco_council_runtime.application.reporting.packets",
    "build_decision_summary_from_state": "eco_council_runtime.application.reporting.council_decision",
    "build_environmentalist_findings": "eco_council_runtime.application.reporting.expert_reports",
    "build_expert_report_draft_from_state": "eco_council_runtime.application.reporting.expert_reports",
    "build_final_brief": "eco_council_runtime.application.reporting.council_decision",
    "build_hypothesis_review_from_state": "eco_council_runtime.application.reporting.investigation_review",
    "build_open_questions": "eco_council_runtime.application.reporting.expert_reports",
    "build_investigation_review_packet": "eco_council_runtime.application.reporting.packets",
    "build_matching_adjudication_packet": "eco_council_runtime.application.reporting.packets",
    "build_matching_authorization_draft": "eco_council_runtime.application.reporting.packets",
    "build_matching_authorization_packet": "eco_council_runtime.application.reporting.packets",
    "build_observation_curation_draft": "eco_council_runtime.application.reporting.packets",
    "build_observation_curation_packet": "eco_council_runtime.application.reporting.packets",
    "build_report_draft": "eco_council_runtime.application.reporting.expert_reports",
    "build_report_packet": "eco_council_runtime.application.reporting.packets",
    "build_report_summary_from_state": "eco_council_runtime.application.reporting.expert_reports",
    "build_pre_match_report_findings": "eco_council_runtime.application.reporting.readiness",
    "build_readiness_findings_from_submissions": "eco_council_runtime.application.reporting.readiness",
    "build_sociologist_findings": "eco_council_runtime.application.reporting.expert_reports",
    "build_summary_for_role": "eco_council_runtime.application.reporting.expert_reports",
    "completion_score_for_round": "eco_council_runtime.application.reporting.council_decision",
    "curation_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "data_readiness_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "decision_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "environment_role_required": "eco_council_runtime.application.reporting.readiness",
    "evidence_resolution_score": "eco_council_runtime.application.reporting.council_decision",
    "evidence_sufficiency_for_round": "eco_council_runtime.application.reporting.council_decision",
    "expert_report_status_from_state": "eco_council_runtime.application.reporting.expert_reports",
    "generic_readiness_recommendations": "eco_council_runtime.application.reporting.readiness",
    "hydrate_observation_submissions_with_observations": "eco_council_runtime.application.reporting_state",
    "investigation_review_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "matching_adjudication_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "matching_authorization_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "missing_types_from_reason_texts": "eco_council_runtime.application.reporting.council_decision",
    "observation_metrics_from_submissions": "eco_council_runtime.application.reporting.readiness",
    "promote_decision_draft": "eco_council_runtime.application.reporting.promotion",
    "promote_investigation_review_draft": "eco_council_runtime.application.reporting.promotion",
    "promote_matching_adjudication_draft": "eco_council_runtime.application.reporting.promotion",
    "promote_matching_authorization_draft": "eco_council_runtime.application.reporting.promotion",
    "promote_report_draft": "eco_council_runtime.application.reporting.promotion",
    "readiness_missing_types": "eco_council_runtime.application.reporting.readiness",
    "readiness_score": "eco_council_runtime.application.reporting.council_decision",
    "render_openclaw_prompts": "eco_council_runtime.application.reporting.prompts",
    "report_artifacts": "eco_council_runtime.application.reporting.artifact_pipeline",
    "report_completion_score": "eco_council_runtime.application.reporting.council_decision",
    "report_has_substance": "eco_council_runtime.application.reporting.common",
    "report_is_placeholder": "eco_council_runtime.application.reporting.common",
    "report_status_for_role": "eco_council_runtime.application.reporting.expert_reports",
    "shared_observation_id": "eco_council_runtime.application.reporting_state",
}

__all__ = list(_EXPORT_TO_MODULE)


def __getattr__(name: str) -> object:
    module_name = _EXPORT_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))

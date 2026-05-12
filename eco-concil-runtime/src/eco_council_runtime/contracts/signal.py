from __future__ import annotations

from typing import Any

from .types import CanonicalContract, PLANE_SIGNAL, _contract


ENVIRONMENT_SIGNAL_TAXONOMY_VERSION = "environment-signal-taxonomy-v1"
ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF = (
    "required:mission-or-runtime-taxonomy-approval"
)
ENVIRONMENT_SIGNAL_TAXONOMY_AUDIT_STATUS = (
    "default-frozen; approval-required; audit-pending"
)

SIGNAL_ROLE_VALUES = (
    "source-event",
    "receptor-observation",
    "context-observation",
    "claim-or-report-signal",
    "unknown-environment-signal-role",
)

ENVIRONMENT_SIGNAL_CLASS_VALUES = (
    "air-quality",
    "fire-detection",
    "meteorology",
    "hydrology",
    "water-quality",
    "soil",
    "ecology",
    "emission-or-release-event",
    "infrastructure-or-operations-event",
    "unknown-environment-class",
)

SPATIOTEMPORAL_RELATION_TYPE_VALUES = (
    "temporal-window-candidate",
    "spatial-window-candidate",
    "spatiotemporal-window-candidate",
    "same-day-cooccurrence",
    "lag-window-candidate",
    "context-window-candidate",
    "scope-overlap-candidate",
    "rejected-by-temporal-rule",
    "rejected-by-spatial-rule",
    "insufficient-basis",
)

SPATIOTEMPORAL_RELATION_STATUS_VALUES = (
    "candidate",
    "weak-candidate",
    "insufficient-basis",
    "rejected-by-rule",
    "needs-human-review",
    "deprecated-legacy-cue",
)

SPATIOTEMPORAL_OBJECTION_CODE_VALUES = (
    "temporal-window-mismatch",
    "lag-assumption-unsupported",
    "spatial-scope-overbroad",
    "spatial-scope-too-narrow",
    "coordinate-missing",
    "timestamp-missing",
    "source-event-background-noise",
    "local-alternative-source",
    "receptor-coverage-gap",
    "context-variable-missing",
    "provider-quality-limitation",
    "taxonomy-misclassification",
    "report-overclaim-risk",
)


def environment_signal_taxonomy_metadata() -> dict[str, Any]:
    return {
        "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
        "approval_ref": ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF,
        "audit_status": ENVIRONMENT_SIGNAL_TAXONOMY_AUDIT_STATUS,
        "signal_roles": list(SIGNAL_ROLE_VALUES),
        "environment_signal_classes": list(ENVIRONMENT_SIGNAL_CLASS_VALUES),
        "spatiotemporal_relation_types": list(SPATIOTEMPORAL_RELATION_TYPE_VALUES),
        "spatiotemporal_relation_statuses": list(
            SPATIOTEMPORAL_RELATION_STATUS_VALUES
        ),
        "spatiotemporal_objection_codes": list(SPATIOTEMPORAL_OBJECTION_CODE_VALUES),
    }


SIGNAL_CONTRACTS: dict[str, CanonicalContract] = {
    # Formal records are policy/procedure records, not every official source.
    # Official physical observations such as AirNow still use
    # environment-observation-signal with provider provenance.
    "formal-comment-signal": _contract(
        "formal-comment-signal",
        plane=PLANE_SIGNAL,
        schema_version="formal-comment-signal-v1",
        id_field="signal_id",
        required_text_fields=("decision_source", "docket_id", "agency_id"),
    ),
    "public-discourse-signal": _contract(
        "public-discourse-signal",
        plane=PLANE_SIGNAL,
        schema_version="public-discourse-signal-v1",
        id_field="signal_id",
        required_text_fields=("decision_source", "source_skill"),
    ),
    "environment-observation-signal": _contract(
        "environment-observation-signal",
        plane=PLANE_SIGNAL,
        schema_version="environment-observation-signal-v1",
        id_field="signal_id",
        required_text_fields=("decision_source", "source_skill"),
    ),
}



__all__ = (
    "ENVIRONMENT_SIGNAL_TAXONOMY_VERSION",
    "ENVIRONMENT_SIGNAL_TAXONOMY_APPROVAL_REF",
    "ENVIRONMENT_SIGNAL_TAXONOMY_AUDIT_STATUS",
    "SIGNAL_ROLE_VALUES",
    "ENVIRONMENT_SIGNAL_CLASS_VALUES",
    "SPATIOTEMPORAL_RELATION_TYPE_VALUES",
    "SPATIOTEMPORAL_RELATION_STATUS_VALUES",
    "SPATIOTEMPORAL_OBJECTION_CODE_VALUES",
    "SIGNAL_CONTRACTS",
    "environment_signal_taxonomy_metadata",
)

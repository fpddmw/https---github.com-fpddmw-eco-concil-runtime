"""Support helpers for contract runtime services.

This module owns contract assets and keeps compatibility calls into the root
``contract.py`` facade behind narrow lazy helpers.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from eco_council_runtime.adapters.filesystem import read_json
from eco_council_runtime.layout import CONTRACT_DDL_PATH, CONTRACT_EXAMPLES_DIR, CONTRACT_SCHEMA_PATH

DDL_PATH = CONTRACT_DDL_PATH
SCHEMA_PATH = CONTRACT_SCHEMA_PATH
SCHEMA_VERSION = "1.0.0"
ISO_UTC_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

OBJECT_KINDS = (
    "mission",
    "round-task",
    "source-selection",
    "override-request",
    "claim",
    "claim-curation",
    "claim-submission",
    "observation",
    "observation-curation",
    "observation-submission",
    "evidence-card",
    "data-readiness-report",
    "matching-authorization",
    "matching-adjudication",
    "matching-result",
    "evidence-adjudication",
    "investigation-review",
    "isolated-entry",
    "remand-entry",
    "expert-report",
    "council-decision",
)

_EXAMPLE_FILENAMES = {
    "mission": "mission.json",
    "round-task": "round_task.json",
    "source-selection": "source_selection.json",
    "override-request": "override_request.json",
    "claim": "claim.json",
    "claim-curation": "claim_curation.json",
    "claim-submission": "claim_submission.json",
    "observation": "observation.json",
    "observation-curation": "observation_curation.json",
    "observation-submission": "observation_submission.json",
    "evidence-card": "evidence_card.json",
    "data-readiness-report": "data_readiness_report.json",
    "matching-authorization": "matching_authorization.json",
    "matching-adjudication": "matching_adjudication.json",
    "matching-result": "matching_result.json",
    "evidence-adjudication": "evidence_adjudication.json",
    "investigation-review": "investigation_review.json",
    "isolated-entry": "isolated_entry.json",
    "remand-entry": "remand_entry.json",
    "expert-report": "expert_report.json",
    "council-decision": "council_decision.json",
}

EXAMPLES: dict[str, Any] = {
    kind: read_json(CONTRACT_EXAMPLES_DIR / filename)
    for kind, filename in _EXAMPLE_FILENAMES.items()
}


def parse_utc_datetime(value: str) -> datetime | None:
    if not isinstance(value, str) or not ISO_UTC_PATTERN.match(value):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_contract_module() -> Any:
    from eco_council_runtime import contract as contract_module

    return contract_module


def validate_payload(kind: str, payload: Any) -> dict[str, Any]:
    return load_contract_module().validate_payload(kind, payload)


def source_governance_for_role(mission: dict[str, Any], role: str) -> list[dict[str, Any]]:
    value = load_contract_module().source_governance_for_role(mission, role)
    return value if isinstance(value, list) else []


__all__ = [
    "DDL_PATH",
    "EXAMPLES",
    "OBJECT_KINDS",
    "SCHEMA_PATH",
    "SCHEMA_VERSION",
    "parse_utc_datetime",
    "source_governance_for_role",
    "validate_payload",
]

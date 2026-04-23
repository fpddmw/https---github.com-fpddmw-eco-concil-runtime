from __future__ import annotations

from typing import Any

ROLE_MODERATOR = "moderator"
ROLE_ENVIRONMENTAL_INVESTIGATOR = "environmental-investigator"
ROLE_PUBLIC_DISCOURSE_INVESTIGATOR = "public-discourse-investigator"
ROLE_FORMAL_RECORD_INVESTIGATOR = "formal-record-investigator"
ROLE_CHALLENGER = "challenger"
ROLE_REPORT_EDITOR = "report-editor"
ROLE_RUNTIME_OPERATOR = "runtime-operator"

CAPABILITY_FETCH = "fetch"
CAPABILITY_NORMALIZE = "normalize"
CAPABILITY_QUERY = "query"
CAPABILITY_ANALYSIS = "analysis"
CAPABILITY_DERIVED_EXPORT = "derived-export"
CAPABILITY_PROPOSAL_WRITE = "proposal-write"
CAPABILITY_READINESS_WRITE = "readiness-write"
CAPABILITY_HYPOTHESIS_WRITE = "hypothesis-write"
CAPABILITY_CHALLENGE_WRITE = "challenge-write"
CAPABILITY_BOARD_TASK_WRITE = "board-task-write"
CAPABILITY_BOARD_NOTE_WRITE = "board-note-write"
CAPABILITY_PROBE_WRITE = "probe-write"
CAPABILITY_ROUND_BOOTSTRAP = "round-bootstrap"
CAPABILITY_STATE_TRANSITION = "state-transition"
CAPABILITY_REPORT_DRAFT = "report-draft"
CAPABILITY_REPORT_PUBLISH = "report-publish"
CAPABILITY_ARCHIVE_WRITE = "archive-write"
CAPABILITY_RUNTIME_ADMIN = "runtime-admin"


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


ROLE_ALIASES = {
    "moderator": ROLE_MODERATOR,
    "environmentalist": ROLE_ENVIRONMENTAL_INVESTIGATOR,
    "environmental-investigator": ROLE_ENVIRONMENTAL_INVESTIGATOR,
    "hydrology-analyst": ROLE_ENVIRONMENTAL_INVESTIGATOR,
    "ecology-analyst": ROLE_ENVIRONMENTAL_INVESTIGATOR,
    "sociologist": ROLE_PUBLIC_DISCOURSE_INVESTIGATOR,
    "public-discourse-investigator": ROLE_PUBLIC_DISCOURSE_INVESTIGATOR,
    "community-impact-analyst": ROLE_PUBLIC_DISCOURSE_INVESTIGATOR,
    "formal-record-investigator": ROLE_FORMAL_RECORD_INVESTIGATOR,
    "policy-analyst": ROLE_FORMAL_RECORD_INVESTIGATOR,
    "challenger": ROLE_CHALLENGER,
    "report-editor": ROLE_REPORT_EDITOR,
    "runtime-operator": ROLE_RUNTIME_OPERATOR,
}

ROLE_CONTRACTS = {
    ROLE_MODERATOR: {
        "canonical_role": ROLE_MODERATOR,
        "legacy_aliases": ["moderator"],
        "description": "Owns agenda framing, board coordination, structured proposal submission, and stage-transition requests.",
        "capabilities": [
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_BOARD_TASK_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
            CAPABILITY_ROUND_BOOTSTRAP,
            CAPABILITY_STATE_TRANSITION,
            CAPABILITY_REPORT_DRAFT,
            CAPABILITY_REPORT_PUBLISH,
        ],
    },
    ROLE_ENVIRONMENTAL_INVESTIGATOR: {
        "canonical_role": ROLE_ENVIRONMENTAL_INVESTIGATOR,
        "legacy_aliases": [
            "environmentalist",
            "environmental-investigator",
            "hydrology-analyst",
            "ecology-analyst",
        ],
        "description": "Fetches, normalizes, queries, and analyzes environmental or physical evidence, then writes findings/proposals.",
        "capabilities": [
            CAPABILITY_FETCH,
            CAPABILITY_NORMALIZE,
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_PUBLIC_DISCOURSE_INVESTIGATOR: {
        "canonical_role": ROLE_PUBLIC_DISCOURSE_INVESTIGATOR,
        "legacy_aliases": [
            "sociologist",
            "public-discourse-investigator",
            "community-impact-analyst",
        ],
        "description": "Fetches, normalizes, queries, and analyzes discourse/community evidence, then writes findings/proposals.",
        "capabilities": [
            CAPABILITY_FETCH,
            CAPABILITY_NORMALIZE,
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_FORMAL_RECORD_INVESTIGATOR: {
        "canonical_role": ROLE_FORMAL_RECORD_INVESTIGATOR,
        "legacy_aliases": [
            "formal-record-investigator",
            "policy-analyst",
        ],
        "description": "Fetches, normalizes, queries, and analyzes formal/policy record evidence, then writes findings/proposals.",
        "capabilities": [
            CAPABILITY_FETCH,
            CAPABILITY_NORMALIZE,
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_CHALLENGER: {
        "canonical_role": ROLE_CHALLENGER,
        "legacy_aliases": ["challenger"],
        "description": "Tests competing explanations, opens/closes challenges, and pushes contradiction or falsification work.",
        "capabilities": [
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_CHALLENGE_WRITE,
            CAPABILITY_PROBE_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_REPORT_EDITOR: {
        "canonical_role": ROLE_REPORT_EDITOR,
        "legacy_aliases": ["report-editor"],
        "description": "Builds evidence-backed report artifacts and publication-ready reporting outputs without changing investigation state.",
        "capabilities": [
            CAPABILITY_QUERY,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_REPORT_DRAFT,
            CAPABILITY_REPORT_PUBLISH,
        ],
    },
    ROLE_RUNTIME_OPERATOR: {
        "canonical_role": ROLE_RUNTIME_OPERATOR,
        "legacy_aliases": ["runtime-operator"],
        "description": "Owns runtime governance, audit, replay, export rebuild, admission policy, and operational write surfaces.",
        "capabilities": [
            CAPABILITY_QUERY,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_ARCHIVE_WRITE,
            CAPABILITY_RUNTIME_ADMIN,
        ],
    },
}

CANONICAL_ROLE_NAMES = tuple(ROLE_CONTRACTS)
KNOWN_ROLE_NAMES = tuple(
    unique_texts([*ROLE_CONTRACTS, *ROLE_ALIASES, *(alias for item in ROLE_CONTRACTS.values() for alias in item.get("legacy_aliases", []))])
)


def normalize_actor_role(actor_role: Any) -> str:
    text = maybe_text(actor_role)
    if not text:
        return ""
    return ROLE_ALIASES.get(text, text)


def known_actor_role(actor_role: Any) -> bool:
    normalized = normalize_actor_role(actor_role)
    return bool(normalized) and normalized in ROLE_CONTRACTS


def role_contract(actor_role: Any) -> dict[str, Any]:
    normalized = normalize_actor_role(actor_role)
    if not normalized:
        return {}
    contract = ROLE_CONTRACTS.get(normalized)
    if not isinstance(contract, dict):
        return {}
    return {
        "canonical_role": normalized,
        "legacy_aliases": unique_texts(contract.get("legacy_aliases", [])),
        "description": maybe_text(contract.get("description")),
        "capabilities": unique_texts(contract.get("capabilities", [])),
    }


def role_capabilities(actor_role: Any) -> set[str]:
    contract = role_contract(actor_role)
    return set(contract.get("capabilities", [])) if isinstance(contract.get("capabilities"), list) else set()


def preferred_role_label(actor_role: Any) -> str:
    normalized = normalize_actor_role(actor_role)
    if not normalized:
        return ""
    contract = role_contract(normalized)
    legacy_aliases = contract.get("legacy_aliases", []) if isinstance(contract.get("legacy_aliases"), list) else []
    return maybe_text(legacy_aliases[0]) if legacy_aliases else normalized


__all__ = [
    "CANONICAL_ROLE_NAMES",
    "CAPABILITY_ANALYSIS",
    "CAPABILITY_ARCHIVE_WRITE",
    "CAPABILITY_BOARD_NOTE_WRITE",
    "CAPABILITY_BOARD_TASK_WRITE",
    "CAPABILITY_CHALLENGE_WRITE",
    "CAPABILITY_DERIVED_EXPORT",
    "CAPABILITY_FETCH",
    "CAPABILITY_HYPOTHESIS_WRITE",
    "CAPABILITY_NORMALIZE",
    "CAPABILITY_PROBE_WRITE",
    "CAPABILITY_PROPOSAL_WRITE",
    "CAPABILITY_QUERY",
    "CAPABILITY_READINESS_WRITE",
    "CAPABILITY_REPORT_DRAFT",
    "CAPABILITY_REPORT_PUBLISH",
    "CAPABILITY_ROUND_BOOTSTRAP",
    "CAPABILITY_RUNTIME_ADMIN",
    "CAPABILITY_STATE_TRANSITION",
    "KNOWN_ROLE_NAMES",
    "ROLE_ALIASES",
    "ROLE_CHALLENGER",
    "ROLE_ENVIRONMENTAL_INVESTIGATOR",
    "ROLE_FORMAL_RECORD_INVESTIGATOR",
    "ROLE_MODERATOR",
    "ROLE_PUBLIC_DISCOURSE_INVESTIGATOR",
    "ROLE_REPORT_EDITOR",
    "ROLE_RUNTIME_OPERATOR",
    "known_actor_role",
    "normalize_actor_role",
    "preferred_role_label",
    "role_capabilities",
    "role_contract",
]

from __future__ import annotations

from typing import Any

ROLE_MODERATOR = "moderator"
ROLE_ENVIRONMENTAL_INVESTIGATOR = "environmental-investigator"
ROLE_SOCIAL_INVESTIGATOR = "social-investigator"
ROLE_CHALLENGER = "challenger"
ROLE_REPORT_EDITOR = "report-editor"
ROLE_RUNTIME_OPERATOR = "runtime-operator"

ROLE_KIND_COUNCIL_AGENT = "council-agent"
ROLE_KIND_RUNTIME_PRINCIPAL = "runtime-principal"

CAPABILITY_FETCH = "fetch"
CAPABILITY_NORMALIZE = "normalize"
CAPABILITY_QUERY = "query"
CAPABILITY_ANALYSIS = "analysis"
CAPABILITY_DERIVED_EXPORT = "derived-export"
CAPABILITY_PROPOSAL_WRITE = "proposal-write"
CAPABILITY_READINESS_WRITE = "readiness-write"
CAPABILITY_FINDING_WRITE = "finding-write"
CAPABILITY_DISCUSSION_WRITE = "discussion-write"
CAPABILITY_EVIDENCE_BUNDLE_WRITE = "evidence-bundle-write"
CAPABILITY_REVIEW_COMMENT_WRITE = "review-comment-write"
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
    "environmental-investigator": ROLE_ENVIRONMENTAL_INVESTIGATOR,
    "social-investigator": ROLE_SOCIAL_INVESTIGATOR,
    "challenger": ROLE_CHALLENGER,
    "report-editor": ROLE_REPORT_EDITOR,
    "runtime-operator": ROLE_RUNTIME_OPERATOR,
}

ROLE_CONTRACTS = {
    ROLE_MODERATOR: {
        "canonical_role": ROLE_MODERATOR,
        "role_kind": ROLE_KIND_COUNCIL_AGENT,
        "conceptual_role": "moderator",
        "aliases": ["moderator"],
        "description": "Owns agenda framing, board coordination, structured proposal submission, and stage-transition requests.",
        "capabilities": [
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_DISCUSSION_WRITE,
            CAPABILITY_EVIDENCE_BUNDLE_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_CHALLENGE_WRITE,
            CAPABILITY_BOARD_TASK_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
            CAPABILITY_PROBE_WRITE,
            CAPABILITY_ROUND_BOOTSTRAP,
            CAPABILITY_STATE_TRANSITION,
            CAPABILITY_REPORT_DRAFT,
            CAPABILITY_REPORT_PUBLISH,
        ],
    },
    ROLE_ENVIRONMENTAL_INVESTIGATOR: {
        "canonical_role": ROLE_ENVIRONMENTAL_INVESTIGATOR,
        "role_kind": ROLE_KIND_COUNCIL_AGENT,
        "conceptual_role": "environmental-investigator",
        "aliases": ["environmental-investigator"],
        "description": "Fetches, normalizes, queries, and analyzes environmental or physical evidence, then writes findings/proposals.",
        "capabilities": [
            CAPABILITY_FETCH,
            CAPABILITY_NORMALIZE,
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_FINDING_WRITE,
            CAPABILITY_DISCUSSION_WRITE,
            CAPABILITY_EVIDENCE_BUNDLE_WRITE,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_SOCIAL_INVESTIGATOR: {
        "canonical_role": ROLE_SOCIAL_INVESTIGATOR,
        "role_kind": ROLE_KIND_COUNCIL_AGENT,
        "conceptual_role": "social-investigator",
        "aliases": ["social-investigator"],
        "description": "Fetches, normalizes, queries, and analyzes public discourse, community, formal record, and policy evidence; owns sample selection and council uptake for annotation-worker outputs, then writes findings/proposals.",
        "capabilities": [
            CAPABILITY_FETCH,
            CAPABILITY_NORMALIZE,
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_FINDING_WRITE,
            CAPABILITY_DISCUSSION_WRITE,
            CAPABILITY_EVIDENCE_BUNDLE_WRITE,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_HYPOTHESIS_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_CHALLENGER: {
        "canonical_role": ROLE_CHALLENGER,
        "role_kind": ROLE_KIND_COUNCIL_AGENT,
        "conceptual_role": "challenger",
        "aliases": ["challenger"],
        "description": "Tests competing explanations, opens/closes challenges, and pushes contradiction or falsification work; for public-discourse annotations, reviews sample/taxonomy/outlier/report-boundary risks rather than relabeling every item.",
        "capabilities": [
            CAPABILITY_FETCH,
            CAPABILITY_NORMALIZE,
            CAPABILITY_QUERY,
            CAPABILITY_ANALYSIS,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_FINDING_WRITE,
            CAPABILITY_DISCUSSION_WRITE,
            CAPABILITY_EVIDENCE_BUNDLE_WRITE,
            CAPABILITY_PROPOSAL_WRITE,
            CAPABILITY_READINESS_WRITE,
            CAPABILITY_REVIEW_COMMENT_WRITE,
            CAPABILITY_CHALLENGE_WRITE,
            CAPABILITY_PROBE_WRITE,
            CAPABILITY_BOARD_NOTE_WRITE,
        ],
    },
    ROLE_REPORT_EDITOR: {
        "canonical_role": ROLE_REPORT_EDITOR,
        "role_kind": ROLE_KIND_COUNCIL_AGENT,
        "conceptual_role": "report-editor",
        "aliases": ["report-editor"],
        "description": "Builds evidence-backed report artifacts and publication-ready reporting outputs without changing investigation state.",
        "capabilities": [
            CAPABILITY_QUERY,
            CAPABILITY_DISCUSSION_WRITE,
            CAPABILITY_DERIVED_EXPORT,
            CAPABILITY_REPORT_DRAFT,
            CAPABILITY_REPORT_PUBLISH,
        ],
    },
    ROLE_RUNTIME_OPERATOR: {
        "canonical_role": ROLE_RUNTIME_OPERATOR,
        "role_kind": ROLE_KIND_RUNTIME_PRINCIPAL,
        "conceptual_role": "runtime",
        "conceptual_note": "Runtime/control-plane principal for authorization and audit, not a council agent or substantive deliberation role.",
        "aliases": ["runtime-operator"],
        "description": "Owns runtime governance, audit, replay, export rebuild, admission policy, and operational write surfaces without joining council deliberation.",
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
    unique_texts([*ROLE_CONTRACTS, *ROLE_ALIASES, *(alias for item in ROLE_CONTRACTS.values() for alias in item.get("aliases", []))])
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
        "role_kind": maybe_text(contract.get("role_kind")),
        "conceptual_role": maybe_text(contract.get("conceptual_role")),
        "conceptual_note": maybe_text(contract.get("conceptual_note")),
        "aliases": unique_texts(contract.get("aliases", [])),
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
    aliases = contract.get("aliases", []) if isinstance(contract.get("aliases"), list) else []
    return maybe_text(aliases[0]) if aliases else normalized


__all__ = [
    "CANONICAL_ROLE_NAMES",
    "CAPABILITY_ANALYSIS",
    "CAPABILITY_ARCHIVE_WRITE",
    "CAPABILITY_BOARD_NOTE_WRITE",
    "CAPABILITY_BOARD_TASK_WRITE",
    "CAPABILITY_CHALLENGE_WRITE",
    "CAPABILITY_DISCUSSION_WRITE",
    "CAPABILITY_DERIVED_EXPORT",
    "CAPABILITY_EVIDENCE_BUNDLE_WRITE",
    "CAPABILITY_FETCH",
    "CAPABILITY_FINDING_WRITE",
    "CAPABILITY_HYPOTHESIS_WRITE",
    "CAPABILITY_NORMALIZE",
    "CAPABILITY_PROBE_WRITE",
    "CAPABILITY_PROPOSAL_WRITE",
    "CAPABILITY_QUERY",
    "CAPABILITY_READINESS_WRITE",
    "CAPABILITY_REVIEW_COMMENT_WRITE",
    "CAPABILITY_REPORT_DRAFT",
    "CAPABILITY_REPORT_PUBLISH",
    "CAPABILITY_ROUND_BOOTSTRAP",
    "CAPABILITY_RUNTIME_ADMIN",
    "CAPABILITY_STATE_TRANSITION",
    "KNOWN_ROLE_NAMES",
    "ROLE_ALIASES",
    "ROLE_CHALLENGER",
    "ROLE_ENVIRONMENTAL_INVESTIGATOR",
    "ROLE_KIND_COUNCIL_AGENT",
    "ROLE_KIND_RUNTIME_PRINCIPAL",
    "ROLE_MODERATOR",
    "ROLE_REPORT_EDITOR",
    "ROLE_RUNTIME_OPERATOR",
    "ROLE_SOCIAL_INVESTIGATOR",
    "known_actor_role",
    "normalize_actor_role",
    "preferred_role_label",
    "role_capabilities",
    "role_contract",
]

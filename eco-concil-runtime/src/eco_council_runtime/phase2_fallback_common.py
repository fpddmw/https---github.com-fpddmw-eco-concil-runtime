from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

PRIORITY_WEIGHT = {"critical": 4.0, "high": 3.0, "medium": 2.0, "low": 1.0}
OBSERVED_INPUT_PREFIXES = (
    "board_summary",
    "board_brief",
    "coverage",
    "next_actions",
    "probes",
    "controversy_map",
    "verification_route",
    "claim_verifiability",
    "formal_public_links",
    "representation_gap",
    "diffusion_edges",
)
ARTIFACT_FALLBACK_PREFIXES = {
    "board_summary",
    "board_brief",
    "next_actions",
    "probes",
}
EXPLICIT_D1_INPUT_KEYS = {
    key
    for prefix in OBSERVED_INPUT_PREFIXES
    for key in (f"{prefix}_artifact_present", f"{prefix}_present")
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def excerpt_text(text: str, limit: int = 180) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def priority_score(priority: str) -> float:
    return PRIORITY_WEIGHT.get(maybe_text(priority).lower(), PRIORITY_WEIGHT["medium"])


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def optional_context_source(context: dict[str, Any]) -> str:
    for key, value in context.items():
        if key.endswith("_source"):
            return maybe_text(value)
    return ""


def optional_context_warnings(context: dict[str, Any]) -> list[dict[str, Any]]:
    warnings = context.get("warnings", []) if isinstance(context.get("warnings"), list) else []
    if source_available(optional_context_source(context)):
        return warnings
    return []


def optional_context_count(context: dict[str, Any], count_key: str) -> int:
    return int(context.get(count_key) or 0)


def optional_context_present(context: dict[str, Any], count_key: str) -> bool:
    return source_available(optional_context_source(context)) or optional_context_count(context, count_key) > 0


def priority_from_score(score: float) -> str:
    if score >= 0.88:
        return "critical"
    if score >= 0.72:
        return "high"
    if score >= 0.54:
        return "medium"
    return "low"


def role_from_lane(lane: str, *, default_role: str = "moderator") -> str:
    lane_text = maybe_text(lane)
    if lane_text == "environmental-observation":
        return "environmentalist"
    if lane_text in {
        "public-discourse-analysis",
        "stakeholder-deliberation-analysis",
    }:
        return "sociologist"
    if lane_text == "formal-comment-and-policy-record":
        return "moderator"
    return default_role


def issue_label_for_item(item: dict[str, Any]) -> str:
    return (
        maybe_text(item.get("issue_label"))
        or maybe_text(item.get("issue_hint"))
        or maybe_text(item.get("claim_id"))
        or "public controversy"
    )


def grouped_by_issue_label(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        issue_label = issue_label_for_item(item)
        grouped.setdefault(issue_label, []).append(item)
    return grouped


def indexed_by_claim_id(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        claim_id = maybe_text(item.get("claim_id"))
        if not claim_id:
            continue
        indexed[claim_id] = item
    return indexed


def weakest_coverage_for_claim_ids(
    claim_ids: list[str],
    coverages_by_claim_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for claim_id in claim_ids:
        claim_text = maybe_text(claim_id)
        if not claim_text:
            continue
        candidate = coverages_by_claim_id.get(claim_text)
        if isinstance(candidate, dict) and candidate:
            candidates.append(candidate)
    if not candidates:
        return {}
    return sorted(
        candidates,
        key=lambda item: (
            -int(item.get("contradiction_link_count") or 0),
            float(item.get("coverage_score") or 0.0),
            maybe_text(item.get("coverage_id")),
        ),
    )[0]

"""Task-query and textual planning helpers for orchestration fetch plans."""

from __future__ import annotations

import re
from typing import Any

from eco_council_runtime.application import orchestration_prepare
from eco_council_runtime.domain.text import maybe_text, normalize_space, unique_strings as shared_unique_strings

GENERIC_QUERY_NOISE_TOKENS = {
    "analysis",
    "assess",
    "assessment",
    "attention",
    "attributable",
    "cause",
    "claims",
    "collect",
    "concern",
    "cross",
    "determine",
    "dialogue",
    "discourse",
    "discovery",
    "event",
    "evidence",
    "framing",
    "health",
    "high",
    "identify",
    "linked",
    "local",
    "mission",
    "patterns",
    "plausibly",
    "public",
    "regional",
    "risk",
    "risks",
    "salience",
    "same",
    "severity",
    "signals",
    "social",
    "spike",
    "three",
    "through",
    "triggered",
    "unusual",
    "value",
    "validation",
    "verification",
    "versus",
    "whether",
    "window",
}
QUERY_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")

mission_region = orchestration_prepare.mission_region


def unique_strings(values: list[str]) -> list[str]:
    return shared_unique_strings(values, casefold=True)


def task_inputs(task: dict[str, Any]) -> dict[str, Any]:
    value = task.get("inputs")
    if isinstance(value, dict):
        return value
    return {}


def task_notes(task: dict[str, Any]) -> str:
    return maybe_text(task.get("notes"))


def merged_task_string_list(tasks: list[dict[str, Any]], key: str) -> list[str]:
    output: list[str] = []
    for task in tasks:
        inputs = task_inputs(task)
        candidate = inputs.get(key)
        if isinstance(candidate, list):
            output.extend(maybe_text(item) for item in candidate if maybe_text(item))
        elif isinstance(candidate, str) and candidate.strip():
            output.append(candidate)
    return unique_strings(output)


def merged_task_scalar(tasks: list[dict[str, Any]], key: str) -> str:
    for task in tasks:
        value = task_inputs(task).get(key)
        text = maybe_text(value)
        if text:
            return text
    return ""


def task_objective_text(tasks: list[dict[str, Any]]) -> str:
    return " ".join(maybe_text(task.get("objective")) for task in tasks if maybe_text(task.get("objective")))


def build_plain_query(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
    query_hints = merged_task_string_list(tasks, "query_hints")
    if query_hints:
        return query_hints[0]
    region_label = primary_region_search_label(mission=mission)
    topic_tokens = compact_query_terms(mission=mission, tasks=tasks, max_terms=4)
    parts = []
    if topic_tokens:
        parts.append(" ".join(topic_tokens))
    if region_label:
        parts.append(region_label)
    return " ".join(parts) if parts else "environment public signals"


def build_gdelt_query(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> str:
    query_hints = merged_task_string_list(tasks, "query_hints")
    if not query_hints:
        region_label = primary_region_search_label(mission=mission)
        topic_terms = compact_query_terms(mission=mission, tasks=tasks, max_terms=4)
        clauses: list[str] = []
        if region_label:
            clauses.append(gdelt_literal_term(region_label))
        if topic_terms:
            if len(topic_terms) == 1:
                clauses.append(topic_terms[0])
            else:
                clauses.append("(" + " OR ".join(topic_terms) + ")")
        if not clauses:
            return '"environment"'
        if len(clauses) == 1:
            return clauses[0]
        return " AND ".join(clauses)
    terms: list[str] = []
    for hint in query_hints[:3]:
        clean = normalize_space(hint)
        if not clean:
            continue
        if any(token in clean for token in ('"', "(", ")", " OR ", " AND ", "sourcecountry:")):
            terms.append(clean)
        elif " " in clean:
            terms.append(gdelt_literal_term(clean))
        else:
            terms.append(clean)
    if not terms:
        return '"environment"'
    if len(terms) == 1:
        return terms[0]
    return "(" + " OR ".join(terms) + ")"


def primary_region_search_label(*, mission: dict[str, Any]) -> str:
    region_label = maybe_text(mission_region(mission).get("label"))
    if not region_label:
        return ""
    primary = normalize_space(region_label.split(",")[0])
    return primary or region_label


def iter_evidence_requirement_summaries(tasks: list[dict[str, Any]]) -> list[str]:
    summaries: list[str] = []
    for task in tasks:
        inputs = task_inputs(task)
        evidence_requirements = inputs.get("evidence_requirements")
        if not isinstance(evidence_requirements, list):
            continue
        for item in evidence_requirements:
            if isinstance(item, dict) and maybe_text(item.get("summary")):
                summaries.append(maybe_text(item.get("summary")))
    return summaries


def extract_query_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in QUERY_TOKEN_PATTERN.findall(text):
        token = raw.strip()
        if not token:
            continue
        key = token.casefold()
        if key in GENERIC_QUERY_NOISE_TOKENS:
            continue
        if len(token) < 3 and not token.isupper():
            continue
        if key in seen:
            continue
        seen.add(key)
        tokens.append(token)
    return tokens


def compact_query_terms(*, mission: dict[str, Any], tasks: list[dict[str, Any]], max_terms: int) -> list[str]:
    region_tokens = {
        token.casefold()
        for token in extract_query_tokens(primary_region_search_label(mission=mission))
    }
    primary_text = maybe_text(mission.get("topic"))
    fallback_texts = [
        task_objective_text(tasks),
        maybe_text(mission.get("objective")),
        *iter_evidence_requirement_summaries(tasks),
    ]
    terms: list[str] = []
    seen: set[str] = set()

    def collect(text: str) -> bool:
        nonlocal terms
        for token in extract_query_tokens(text):
            key = token.casefold()
            if key in region_tokens or key in seen:
                continue
            seen.add(key)
            terms.append(token)
            if len(terms) >= max_terms:
                return True
        return False

    if primary_text:
        collect(primary_text)
    if terms:
        return terms
    for text in fallback_texts:
        if collect(text):
            break
    return terms


def gdelt_literal_term(text: str) -> str:
    clean = normalize_space(text)
    if not clean:
        return clean
    word_count = len(QUERY_TOKEN_PATTERN.findall(clean))
    if " " in clean and word_count <= 4 and len(clean) <= 48:
        return f'"{clean}"'
    return clean


def regs_task_enabled(tasks: list[dict[str, Any]]) -> bool:
    combined = " ".join(
        [
            task_objective_text(tasks),
            " ".join(merged_task_string_list(tasks, "query_hints")),
            " ".join(merged_task_string_list(tasks, "agency_ids")),
        ]
    ).casefold()
    return any(token in combined for token in ("policy", "regulation", "epa", "docket", "comment"))


def step_task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    return [maybe_text(task.get("task_id")) for task in tasks if maybe_text(task.get("task_id"))]


__all__ = [
    "build_gdelt_query",
    "build_plain_query",
    "compact_query_terms",
    "extract_query_tokens",
    "gdelt_literal_term",
    "iter_evidence_requirement_summaries",
    "merged_task_scalar",
    "merged_task_string_list",
    "primary_region_search_label",
    "regs_task_enabled",
    "step_task_ids",
    "task_inputs",
    "task_notes",
    "task_objective_text",
]

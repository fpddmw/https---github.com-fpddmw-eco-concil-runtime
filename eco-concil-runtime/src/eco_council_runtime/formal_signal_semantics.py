from __future__ import annotations

import re
from typing import Any


STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "with",
}

ENVIRONMENTAL_ISSUES = {
    "air-quality-smoke",
    "heat-risk",
    "flood-water",
    "water-contamination",
}

ISSUE_RULES: dict[str, tuple[str, ...]] = {
    "air-quality-smoke": (
        "smoke",
        "wildfire",
        "haze",
        "air quality",
        "pm25",
        "pm2.5",
        "pm 2.5",
        "respiratory",
        "asthma",
    ),
    "heat-risk": ("heat", "temperature", "hot", "heatwave"),
    "flood-water": ("flood", "stormwater", "overflow", "rainfall", "precipitation"),
    "water-contamination": (
        "contamination",
        "water quality",
        "drinking water",
        "chemical",
        "spill",
        "toxic",
    ),
    "permit-process": (
        "permit",
        "hearing",
        "comment period",
        "rulemaking",
        "agency",
        "review process",
        "public comment",
        "docket",
        "reopen the hearing",
    ),
    "waste-facility": (
        "landfill",
        "incinerator",
        "waste facility",
        "dump",
        "plant",
    ),
    "energy-infrastructure": (
        "pipeline",
        "solar",
        "wind farm",
        "battery",
        "grid",
        "power plant",
    ),
    "representation-trust": (
        "community voice",
        "ignored",
        "trust",
        "representation",
        "not heard",
        "misleading",
        "voice",
        "voices",
        "residents say they were ignored",
    ),
}

CONCERN_RULES: dict[str, tuple[str, ...]] = {
    "health-safety": (
        "health",
        "asthma",
        "respiratory",
        "toxic",
        "unsafe",
        "dangerous",
        "children",
    ),
    "ecology": (
        "ecosystem",
        "wildlife",
        "river",
        "forest",
        "water quality",
        "habitat",
    ),
    "cost-livelihood": (
        "cost",
        "expensive",
        "jobs",
        "business",
        "livelihood",
        "income",
        "farm",
        "fisher",
    ),
    "procedure-governance": (
        "permit",
        "hearing",
        "rulemaking",
        "comment period",
        "public comment",
        "transparency",
        "agency",
        "docket",
        "reopen",
        "extend",
    ),
    "fairness-equity": (
        "justice",
        "equity",
        "unfair",
        "burden",
        "low-income",
        "community",
        "frontline",
    ),
    "trust-credibility": (
        "official",
        "media",
        "misleading",
        "rumor",
        "false",
        "trust",
        "ignored",
        "not heard",
    ),
    "daily-life": (
        "school",
        "outdoor",
        "visibility",
        "commute",
        "home",
        "children",
        "neighborhood",
    ),
    "legal-authority": (
        "statute",
        "legal",
        "authority",
        "jurisdiction",
        "violates",
        "compliance",
        "lawful",
        "nepa",
        "clean air act",
    ),
    "evidence-quality": (
        "study",
        "research",
        "evidence",
        "data",
        "model",
        "monitoring",
        "analysis",
        "peer reviewed",
    ),
}

CITATION_RULES: dict[str, tuple[str, ...]] = {
    "official-document": (
        "official",
        "agency",
        "permit",
        "rulemaking",
        "filing",
        "docket",
        "record",
    ),
    "scientific-study": (
        "study",
        "research",
        "scientist",
        "peer reviewed",
        "paper",
        "monitoring",
        "analysis",
    ),
    "news-report": ("report", "reported", "news", "article", "headline"),
    "firsthand-observation": (
        "i saw",
        "we saw",
        "look outside",
        "skyline",
        "smell",
        "visibility",
        "at my home",
    ),
    "personal-experience": (
        "my kids",
        "my family",
        "my neighborhood",
        "our community",
    ),
    "legal-commentary": (
        "statute",
        "legal",
        "authority",
        "jurisdiction",
        "violates",
        "compliance",
    ),
    "rumor-hearsay": ("heard that", "rumor", "someone said", "they say"),
}

STANCE_RULES: dict[str, tuple[str, ...]] = {
    "oppose": (
        "oppose",
        "against",
        "reject",
        "stop",
        "ban",
        "harmful",
        "unacceptable",
    ),
    "support": (
        "support",
        "approve",
        "benefit",
        "needed",
        "necessary",
        "protect",
        "helps",
    ),
    "request-review": (
        "review",
        "reopen",
        "extend",
        "comment period",
        "hearing",
        "investigate",
        "clarify",
        "consider",
        "please study",
    ),
    "verify": (
        "verify",
        "evidence",
        "data",
        "official",
        "confirmed",
        "check",
        "whether",
        "unclear",
    ),
    "report-impact": (
        "intense",
        "covered",
        "haze",
        "smoke over",
        "affected",
        "flooded",
        "hot",
        "today",
    ),
}

SUBMITTER_TYPE_RULES: dict[str, tuple[str, ...]] = {
    "government": (
        "agency",
        "department",
        "city of",
        "county",
        "state of",
        "commission",
        "board",
    ),
    "ngo": (
        "ngo",
        "nonprofit",
        "non-profit",
        "alliance",
        "coalition",
        "environmental",
        "advocacy",
        "network",
    ),
    "company": (
        "llc",
        "inc",
        "corp",
        "corporation",
        "company",
        "utility",
        "developer",
        "industry",
    ),
    "expert": (
        "university",
        "professor",
        "scientist",
        "research",
        "doctor",
        "institute",
    ),
    "community-group": (
        "community",
        "residents",
        "neighbors",
        "citizens",
        "families",
        "parents",
        "tribal",
        "tribe",
    ),
    "individual": ("citizen", "resident", "individual", "concerned citizen"),
}


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


def semantic_tokens(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", maybe_text(text).casefold())
    return [token for token in tokens if token not in STOPWORDS]


def _matched_terms(folded: str, terms: tuple[str, ...]) -> list[str]:
    return [term for term in terms if term in folded]


def _top_rule_matches(
    folded: str,
    rules: dict[str, tuple[str, ...]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    ranked: list[tuple[int, str, list[str]]] = []
    for label, terms in rules.items():
        matched_terms = _matched_terms(folded, terms)
        if matched_terms:
            ranked.append((len(matched_terms), label, matched_terms))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [
        {
            "label": label,
            "score": score,
            "matched_terms": unique_texts(matched_terms),
        }
        for score, label, matched_terms in ranked[:limit]
    ]


def submitter_name_from_attributes(
    attributes: dict[str, Any],
    *,
    fallback: str = "",
) -> str:
    if not isinstance(attributes, dict):
        return maybe_text(fallback)
    full_name = " ".join(
        part
        for part in (
            maybe_text(attributes.get("firstName")),
            maybe_text(attributes.get("lastName")),
        )
        if part
    )
    return (
        maybe_text(attributes.get("submitterName"))
        or full_name
        or maybe_text(attributes.get("organization"))
        or maybe_text(attributes.get("organizationName"))
        or maybe_text(fallback)
    )


def _explicit_submitter_type(attributes: dict[str, Any]) -> str:
    explicit = maybe_text(
        attributes.get("submitterType")
        or attributes.get("commenterType")
        or attributes.get("category")
    ).casefold()
    if not explicit:
        return ""
    if explicit in {"person", "individual", "citizen"}:
        return "individual"
    if explicit in {"government", "federal", "state", "local-government"}:
        return "government"
    if explicit in {"business", "company", "corporation", "industry"}:
        return "company"
    if explicit in {"academic", "expert", "scientist", "research"}:
        return "expert"
    if explicit in {"ngo", "nonprofit", "advocacy"}:
        return "ngo"
    if explicit in {"community", "coalition", "association"}:
        return "community-group"
    return maybe_text(explicit)


def submitter_type_from_attributes(
    attributes: dict[str, Any],
    *,
    submitter_name: str = "",
) -> str:
    if not isinstance(attributes, dict):
        attributes = {}
    explicit = _explicit_submitter_type(attributes)
    if explicit:
        return explicit

    identity_text = " ".join(
        part
        for part in (
            submitter_name,
            maybe_text(attributes.get("organization")),
            maybe_text(attributes.get("organizationName")),
        )
        if part
    ).casefold()
    matches = _top_rule_matches(identity_text, SUBMITTER_TYPE_RULES, limit=1)
    if matches:
        return maybe_text(matches[0].get("label"))
    if maybe_text(attributes.get("organization")) or maybe_text(
        attributes.get("organizationName")
    ):
        return "organization"
    if maybe_text(submitter_name):
        return "individual"
    return "unknown"


def issue_labels_from_text(text: str) -> list[str]:
    return [
        maybe_text(match.get("label"))
        for match in _top_rule_matches(maybe_text(text).casefold(), ISSUE_RULES, limit=3)
        if maybe_text(match.get("label"))
    ]


def issue_terms_for_labels(issue_labels: list[str], *, limit: int = 12) -> list[str]:
    values: list[str] = []
    for issue_label in issue_labels:
        values.extend(semantic_tokens(issue_label.replace("-", " ")))
        for term in ISSUE_RULES.get(maybe_text(issue_label), ()):
            values.extend(semantic_tokens(term))
    return unique_texts(values)[:limit]


def concern_facets_from_text(text: str, *, issue_labels: list[str]) -> list[str]:
    folded = maybe_text(text).casefold()
    matches = _top_rule_matches(folded, CONCERN_RULES, limit=4)
    values = [
        maybe_text(match.get("label"))
        for match in matches
        if maybe_text(match.get("label"))
    ]
    if values:
        return values
    if "air-quality-smoke" in issue_labels and any(
        token in folded for token in ("smoke", "wildfire", "haze", "respiratory", "asthma")
    ):
        return ["health-safety", "daily-life"]
    if "water-contamination" in issue_labels and any(
        token in folded for token in ("contamination", "toxic", "spill", "chemical")
    ):
        return ["health-safety", "ecology"]
    return []


def evidence_citation_types_from_text(
    text: str,
    *,
    structural_defaults: list[str] | None = None,
) -> list[str]:
    folded = maybe_text(text).casefold()
    matches = _top_rule_matches(folded, CITATION_RULES, limit=4)
    values = [
        maybe_text(match.get("label"))
        for match in matches
        if maybe_text(match.get("label"))
    ]
    defaults = structural_defaults if isinstance(structural_defaults, list) else []
    return unique_texts(defaults + values)


def default_lane_for_issue(issue_label: str) -> str:
    label = maybe_text(issue_label)
    if label in ENVIRONMENTAL_ISSUES:
        return "environmental-observation"
    if label == "permit-process":
        return "formal-comment-and-policy-record"
    if label == "representation-trust":
        return "public-discourse-analysis"
    return "mixed-review"


def route_status_for_lane(lane: str) -> str:
    if lane == "environmental-observation":
        return "route-to-verification-lane"
    if lane == "formal-comment-and-policy-record":
        return "route-to-formal-record-review"
    if lane == "public-discourse-analysis":
        return "keep-in-public-discourse-analysis"
    if lane == "stakeholder-deliberation-analysis":
        return "keep-in-stakeholder-deliberation"
    return "mixed-routing-review"


def route_hint_from_semantics(
    issue_labels: list[str],
    concern_facets: list[str],
) -> str:
    if any(issue_label in ENVIRONMENTAL_ISSUES for issue_label in issue_labels):
        return "environmental-observation"
    if "procedure-governance" in concern_facets or "legal-authority" in concern_facets:
        return "formal-comment-and-policy-record"
    if "representation-trust" in issue_labels or "trust-credibility" in concern_facets:
        return "public-discourse-analysis"
    if "fairness-equity" in concern_facets or "cost-livelihood" in concern_facets:
        return "stakeholder-deliberation-analysis"
    return "formal-comment-and-policy-record"


def stance_hint_from_text(
    text: str,
    *,
    issue_labels: list[str],
    concern_facets: list[str],
    evidence_citation_types: list[str],
) -> str:
    folded = maybe_text(text).casefold()
    matches = _top_rule_matches(folded, STANCE_RULES, limit=1)
    if matches:
        return maybe_text(matches[0].get("label"))
    if "permit-process" in issue_labels or "procedure-governance" in concern_facets:
        return "request-review"
    if "representation-trust" in issue_labels or "trust-credibility" in concern_facets:
        return "oppose"
    if "scientific-study" in evidence_citation_types:
        return "verify"
    return "unclear"


def build_formal_signal_semantics(
    *,
    title: str,
    body_text: str,
    author_name: str,
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    attribute_payload = dict(attributes) if isinstance(attributes, dict) else {}
    submitter_name = submitter_name_from_attributes(
        attribute_payload,
        fallback=author_name,
    )
    text = " ".join(
        part
        for part in (
            title,
            body_text,
            submitter_name,
            maybe_text(attribute_payload.get("organization")),
            maybe_text(attribute_payload.get("organizationName")),
            maybe_text(attribute_payload.get("docketId")),
            maybe_text(attribute_payload.get("agencyId")),
            maybe_text(attribute_payload.get("commentOnDocumentTitle")),
        )
        if part
    )
    folded = text.casefold()

    issue_matches = _top_rule_matches(folded, ISSUE_RULES, limit=3)
    issue_labels = [
        maybe_text(match.get("label"))
        for match in issue_matches
        if maybe_text(match.get("label"))
    ]
    concern_matches = _top_rule_matches(folded, CONCERN_RULES, limit=4)
    concern_facets = concern_facets_from_text(text, issue_labels=issue_labels)
    citation_matches = _top_rule_matches(folded, CITATION_RULES, limit=4)
    evidence_citation_types = evidence_citation_types_from_text(
        text,
        structural_defaults=["official-document"],
    )
    stance_matches = _top_rule_matches(folded, STANCE_RULES, limit=3)
    route_hint = route_hint_from_semantics(issue_labels, concern_facets)

    return {
        "decision_source": "heuristic-fallback",
        "typing_method": "formal-signal-semantics-v1",
        "submitter_name": submitter_name,
        "submitter_type": submitter_type_from_attributes(
            attribute_payload,
            submitter_name=submitter_name,
        ),
        "issue_labels": issue_labels,
        "issue_terms": issue_terms_for_labels(issue_labels),
        "stance_hint": stance_hint_from_text(
            text,
            issue_labels=issue_labels,
            concern_facets=concern_facets,
            evidence_citation_types=evidence_citation_types,
        ),
        "concern_facets": concern_facets,
        "evidence_citation_types": evidence_citation_types,
        "route_hint": route_hint,
        "route_status_hint": route_status_for_lane(route_hint),
        "typing_matches": {
            "issue_labels": issue_matches,
            "concern_facets": concern_matches,
            "evidence_citation_types": citation_matches,
            "stance_hints": stance_matches,
        },
    }


__all__ = [
    "build_formal_signal_semantics",
    "default_lane_for_issue",
    "evidence_citation_types_from_text",
    "issue_labels_from_text",
    "issue_terms_for_labels",
    "maybe_text",
    "route_hint_from_semantics",
    "route_status_for_lane",
    "semantic_tokens",
    "unique_texts",
]

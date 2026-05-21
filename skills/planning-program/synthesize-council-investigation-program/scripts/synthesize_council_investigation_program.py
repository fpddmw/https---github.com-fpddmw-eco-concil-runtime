#!/usr/bin/env python3
"""Synthesize a council investigation program from framing objects."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "synthesize-council-investigation-program"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import append_dynamic_investigation_object_record, query_council_objects  # noqa: E402


EXPECTED_POSITION_ROLES = (
    "environmental-investigator",
    "social-investigator",
    "challenger",
)

FORBIDDEN_SCHEDULER_FIELDS = (
    "source_family",
    "source_families",
    "source_skill",
    "source_skills",
    "query",
    "query_variants",
    "query_parameters",
    "priority_score",
    "route_ranking",
    "source_priority",
    "scheduler_queue",
    "auto_execute",
)

FORBIDDEN_ROUTE_TEXT_PHRASES = (
    "auto execute",
    "automatic execution",
    "query parameter",
    "query variant",
    "route ranking",
    "scheduler queue",
    "source family",
    "source skill",
)

ROUND_PROPOSAL_FIELDS = (
    "proposed_program_rounds",
    "proposed_rounds",
    "round_plan_proposals",
    "round_sequence_proposal",
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def list_texts_or_role_map(value: Any) -> list[str]:
    if isinstance(value, dict):
        return unique_texts(
            [
                f"{maybe_text(key)}: {maybe_text(child)}"
                for key, child in value.items()
                if maybe_text(key) and maybe_text(child)
            ]
        )
    if isinstance(value, list):
        results: list[Any] = []
        for item in value:
            if isinstance(item, dict):
                results.extend(list_texts_or_role_map(item))
            else:
                results.append(item)
        return unique_texts(results)
    return []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def text_blob(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join([maybe_text(key) + " " + text_blob(child) for key, child in value.items()])
    if isinstance(value, list):
        return " ".join(text_blob(item) for item in value)
    return maybe_text(value)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def forbidden_field_paths(value: Any, *, path: str = "") -> list[str]:
    matches: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else maybe_text(key)
            if maybe_text(key) in FORBIDDEN_SCHEDULER_FIELDS:
                matches.append(child_path)
            matches.extend(forbidden_field_paths(child, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]" if path else f"[{index}]"
            matches.extend(forbidden_field_paths(child, path=child_path))
    return matches


def has_forbidden_route_text(value: Any) -> list[str]:
    lowered = text_blob(value).casefold()
    return [phrase for phrase in FORBIDDEN_ROUTE_TEXT_PHRASES if phrase in lowered]


def slugify(value: str) -> str:
    chars: list[str] = []
    previous_dash = False
    for char in maybe_text(value).casefold():
        if char.isalnum():
            chars.append(char)
            previous_dash = False
        elif not previous_dash:
            chars.append("-")
            previous_dash = True
    slug = "".join(chars).strip("-")
    return slug[:48].strip("-") or "agent-planned-round"


def query_objects(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    return [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]


def load_blueprint_and_themes(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    blueprint_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    blueprints = query_objects(
        run_dir,
        object_kind="report-blueprint",
        run_id=run_id,
        round_id=round_id,
        limit=20,
    )
    if blueprint_id:
        blueprints = [
            item
            for item in blueprints
            if maybe_text(item.get("blueprint_id")) == blueprint_id
            or maybe_text(item.get("object_id")) == blueprint_id
        ]
    blueprint = blueprints[0] if blueprints else {}
    themes = query_objects(
        run_dir,
        object_kind="investigation-theme",
        run_id=run_id,
        round_id=round_id,
        limit=100,
    )
    if not blueprint or not themes:
        artifact = load_json(run_dir / "reporting" / f"report_blueprint_{round_id}.json")
        if not blueprint and isinstance(artifact.get("report_blueprint"), dict):
            blueprint = dict_items(artifact.get("report_blueprint"))
        if not themes:
            themes = [
                item
                for item in list_items(artifact.get("investigation_themes"))
                if isinstance(item, dict)
            ]
    if not blueprint:
        raise ValueError("No report-blueprint is visible for program synthesis.")
    return blueprint, themes


def position_ref(position: dict[str, Any]) -> str:
    object_id = maybe_text(position.get("object_id"))
    return f"agent-position:{object_id}" if object_id else ""


def position_text_list(position: dict[str, Any], *field_names: str) -> list[str]:
    values: list[Any] = []
    for field_name in field_names:
        value = position.get(field_name)
        if isinstance(value, list):
            values.extend(value)
        else:
            values.append(value)
    return unique_texts(values)


def position_round_proposals(position: dict[str, Any]) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for field_name in ROUND_PROPOSAL_FIELDS:
        value = position.get(field_name)
        if isinstance(value, dict):
            value = [value]
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, dict):
                proposals.append(dict(item))
    return proposals


def position_summary(position: dict[str, Any]) -> dict[str, Any]:
    proposals = position_round_proposals(position)
    return {
        "position_ref": position_ref(position),
        "author_role": maybe_text(position.get("author_role")),
        "status": maybe_text(position.get("status")),
        "target_id": maybe_text(position.get("target_id")),
        "position_text": maybe_text(position.get("position_text")),
        "rationale": maybe_text(position.get("rationale")),
        "boundary_notes": position_text_list(position, "boundary_notes"),
        "open_questions": position_text_list(position, "open_questions"),
        "limitations": position_text_list(position, "limitations"),
        "proposed_agenda_questions": position_text_list(
            position,
            "proposed_agenda_questions",
            "recommended_issue_questions",
            "council_agenda_questions",
        ),
        "proposed_program_round_count": len(proposals),
        "proposed_program_rounds": proposals,
    }


def theme_ref(theme: dict[str, Any]) -> str:
    return maybe_text(theme.get("theme_id")) or maybe_text(theme.get("object_id"))


def mission_question_from_blueprint(blueprint: dict[str, Any]) -> str:
    questions = [
        maybe_text(slot.get("question"))
        for slot in list_items(blueprint.get("claim_slots"))
        if isinstance(slot, dict)
    ] or [maybe_text(item) for item in list_items(blueprint.get("report_questions"))]
    return questions[0] if questions else maybe_text(blueprint.get("rationale")) or "What should this report answer?"


def agenda_question_for_theme(theme: dict[str, Any]) -> str:
    question = maybe_text(theme.get("theme_question"))
    if question.endswith(("?", "？")):
        return question
    if question:
        return question.rstrip(".") + "?"
    return f"What claim-basis boundary should the council resolve for {theme_ref(theme)}?"


def boundary_for_theme(theme: dict[str, Any]) -> str:
    owner = maybe_text(theme.get("owner_role")) or "moderator"
    theme_id = theme_ref(theme)
    claim_boundary = maybe_text(theme.get("claim_boundary"))
    if owner == "environmental-investigator":
        duty = "define fact-process claim basis, limitation, and denominator boundary"
    elif owner == "social-investigator":
        duty = "define public, media, formal, or policy-record claim basis, denominator, and limitation boundary"
    elif owner == "challenger":
        duty = "review denominator, causal, public-proportion, and policy-evaluation overreach"
    else:
        duty = "synthesize theme boundaries, unresolved concerns, and downgrade conditions"
    return f"{owner}: {duty} for {theme_id}. {claim_boundary}".strip()


def round_plan(
    *,
    round_id: str,
    title: str,
    subtitle: str,
    round_mode: str,
    round_category: str,
    active_theme_ids: list[str],
    boundaries: list[str],
    internal_phases: list[str],
) -> dict[str, Any]:
    return {
        "round_id": round_id,
        "round_title": title,
        "round_subtitle_question": subtitle if subtitle.endswith(("?", "？")) else subtitle + "?",
        "round_mode": round_mode,
        "round_category": round_category,
        "active_theme_ids": active_theme_ids,
        "agent_responsibility_boundaries": boundaries,
        "round_internal_phases": internal_phases,
        "round_exit_criteria": [
            "Council records supported, downgraded, scoped-out, or carried-forward status for each active theme boundary.",
            "Moderator synthesis or readiness opinion carries any progress-review recommendation before state transition.",
        ],
        "continuation_criteria": [
            "Open a supplemental issue council only for unresolved theme boundaries, denominator disputes, challenger concerns, or policy-lane absence after in-round recovery is exhausted.",
        ],
    }


def theme_ids_matching(theme_by_id: dict[str, dict[str, Any]], *terms: str) -> list[str]:
    lowered_terms = tuple(term.casefold() for term in terms if maybe_text(term))
    return [
        theme_id
        for theme_id in theme_by_id
        if any(term in theme_id.casefold() for term in lowered_terms)
    ]


def theme_boundaries(theme_by_id: dict[str, dict[str, Any]], theme_ids: list[str]) -> list[str]:
    return [boundary_for_theme(theme_by_id[theme_id]) for theme_id in theme_ids if theme_id in theme_by_id]


def round_label(index: int, slug: str) -> str:
    return f"round-{index:03d}-{slug}"


def normalize_agent_round_proposal(
    proposal: dict[str, Any],
    *,
    index: int,
    author_role: str,
    position_ref_value: str,
    theme_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    forbidden_paths = forbidden_field_paths(proposal)
    forbidden_phrases = has_forbidden_route_text(proposal)
    if forbidden_paths or forbidden_phrases:
        raise ValueError(
            "Agent-authored program round proposals cannot include source, "
            "query, route, scheduler, or automatic execution precommitments: "
            + ", ".join([*forbidden_paths, *forbidden_phrases])
        )
    title = maybe_text(proposal.get("round_title")) or maybe_text(proposal.get("title"))
    question = (
        maybe_text(proposal.get("round_subtitle_question"))
        or maybe_text(proposal.get("agenda_question"))
        or maybe_text(proposal.get("question"))
    )
    if not question:
        raise ValueError("Agent-authored program round proposals require a question-form agenda.")
    if not question.endswith(("?", "？")):
        question = question.rstrip(".") + "?"
    category = maybe_text(proposal.get("round_category")) or "agent-planned"
    mode = maybe_text(proposal.get("round_mode")) or f"{category}-council"
    active_theme_ids = unique_texts(
        [
            theme_id
            for theme_id in list_items(proposal.get("active_theme_ids"))
            if maybe_text(theme_id) in theme_by_id
        ]
    )
    if not active_theme_ids:
        active_theme_ids = list(theme_by_id)
    boundaries = unique_texts(
        [
            *list_texts_or_role_map(proposal.get("agent_responsibility_boundaries")),
            *list_texts_or_role_map(proposal.get("responsibility_boundaries")),
            *list_texts_or_role_map(proposal.get("claim_basis_boundaries")),
        ]
    )
    if not boundaries:
        boundaries = [
            f"{author_role}: define the claim-basis, limitation, and review boundary for this agenda question."
        ]
    phases = unique_texts(
        [
            *list_items(proposal.get("round_internal_phases")),
            *list_items(proposal.get("internal_phases")),
        ]
    ) or ["agenda-question", "agent-work-turns", "progress-review", "moderator-synthesis"]
    slug = maybe_text(proposal.get("round_slug")) or maybe_text(proposal.get("slug")) or title or question
    normalized = round_plan(
        round_id=round_label(index, slugify(slug)),
        title=title or question.rstrip("?？"),
        subtitle=question,
        round_mode=mode,
        round_category=category,
        active_theme_ids=active_theme_ids,
        boundaries=boundaries,
        internal_phases=phases,
    )
    normalized["proposal_source"] = "agent-position"
    normalized["proposed_by_role"] = author_role
    normalized["proposal_position_ref"] = position_ref_value
    normalized["proposal_rationale"] = maybe_text(proposal.get("rationale"))
    if maybe_text(proposal.get("program_order")) or maybe_text(proposal.get("round_order")):
        normalized["program_order"] = maybe_text(
            proposal.get("program_order")
        ) or maybe_text(proposal.get("round_order"))
    explicit_exits = unique_texts(
        [
            *list_texts_or_role_map(proposal.get("round_exit_criteria")),
            *list_texts_or_role_map(proposal.get("exit_criteria")),
        ]
    )
    if explicit_exits:
        normalized["round_exit_criteria"] = explicit_exits
    explicit_downgrades = unique_texts(
        [
            *list_texts_or_role_map(proposal.get("downgrade_conditions")),
            *list_texts_or_role_map(proposal.get("downgrade_boundaries")),
        ]
    )
    if explicit_downgrades:
        normalized["downgrade_conditions"] = explicit_downgrades
    explicit_continuations = unique_texts(
        [
            *list_texts_or_role_map(proposal.get("continuation_criteria")),
            *list_texts_or_role_map(proposal.get("supplemental_round_triggers")),
        ]
    )
    if explicit_continuations:
        normalized["continuation_criteria"] = explicit_continuations
    return normalized


def proposal_order_value(proposal: dict[str, Any], fallback: int) -> tuple[float, int]:
    raw = maybe_text(proposal.get("program_order")) or maybe_text(proposal.get("round_order"))
    if raw:
        try:
            return (float(raw), fallback)
        except ValueError:
            return (100000.0 + float(fallback), fallback)
    return (100000.0 + float(fallback), fallback)


def numeric_program_order(round_item: dict[str, Any]) -> float | None:
    raw = maybe_text(round_item.get("program_order")) or maybe_text(round_item.get("round_order"))
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def ordered_unique(values: list[Any], *, limit: int = 0) -> list[str]:
    results = unique_texts(values)
    return results[:limit] if limit > 0 else results


def round_family(round_item: dict[str, Any]) -> str:
    category = maybe_text(round_item.get("round_category")).casefold()
    mode = maybe_text(round_item.get("round_mode")).casefold()
    blob = " ".join(
        [
            category,
            mode,
            maybe_text(round_item.get("round_title")).casefold(),
            maybe_text(round_item.get("round_subtitle_question")).casefold(),
        ]
    )
    if "reporting" in category or "report writing" in blob:
        return "reporting"
    if "readiness" in blob or "freeze claim" in blob:
        return "report-readiness-synthesis"
    if "policy" in blob and ("evaluation" in blob or "basis" in blob):
        return "policy-evaluation-basis-synthesis"
    if "interaction" in blob or "timeline" in blob or "align" in blob:
        return "interaction-timeline-synthesis"
    if (
        "semantic" in blob
        or "meaning" in blob
        or "trust" in blob
        or "uncertainty" in blob
        or "demand" in blob
    ) and "acquisition" not in category and "acquire" not in blob:
        return "semantic-analysis"
    if (
        "public" in blob
        or "media" in blob
        or "discourse" in blob
        or "formal" in blob
    ) and (
        "acquisition" in category
        or "acquire" in blob
        or "evidence" in blob
        or "material" in blob
        or "sample" in blob
    ):
        return "public-discourse-acquisition"
    if (
        "official" in blob
        or "governance" in blob
        or "fact" in blob
        or "event" in blob
        or "chronology" in blob
    ) and (
        "acquisition" in category
        or "acquire" in blob
        or "evidence" in blob
        or "record" in blob
        or "basis" in blob
    ):
        return "fact-official-acquisition"
    if "scope" in blob or "framing" in blob or "boundary" in blob or "reportable" in blob:
        return "scope-deliberation"
    return category or mode or "agent-planned"


def round_cluster_key(round_item: dict[str, Any]) -> tuple[str, str]:
    explicit_cluster = maybe_text(
        round_item.get("round_cluster")
        or round_item.get("program_cluster")
        or round_item.get("issue_thread_id")
    )
    if explicit_cluster:
        return ("explicit", explicit_cluster.casefold())
    order = numeric_program_order(round_item)
    if order is not None and 10 <= order < 100000:
        # Agents commonly use 10/20/30... program bands. Nearby values in the
        # same band represent deliberative variants of one intended round, not
        # separate rounds to schedule mechanically.
        return ("program-order-band", str(int(order // 10)))
    if order is not None and order < 10:
        return ("program-order", str(int(order)))
    return ("round-family", round_family(round_item))


def role_preference(role: str) -> int:
    order = {
        "report-editor": 0,
        "moderator": 1,
        "challenger": 2,
        "social-investigator": 3,
        "environmental-investigator": 4,
    }
    return order.get(maybe_text(role), 10)


def choose_representative(cluster: list[dict[str, Any]]) -> dict[str, Any]:
    families = [round_family(item) for item in cluster]
    family_counts = {family: families.count(family) for family in set(families)}
    dominant_family = sorted(
        family_counts,
        key=lambda family: (-family_counts[family], families.index(family)),
    )[0]
    candidates = [item for item in cluster if round_family(item) == dominant_family] or list(cluster)
    return sorted(
        candidates,
        key=lambda item: (
            role_preference(maybe_text(item.get("proposed_by_role"))),
            numeric_program_order(item) if numeric_program_order(item) is not None else 100000.0,
            maybe_text(item.get("round_title")).casefold(),
        ),
    )[0]


def merge_agent_round_cluster(cluster: list[dict[str, Any]], *, index: int) -> dict[str, Any]:
    representative = choose_representative(cluster)
    family = round_family(representative)
    title = maybe_text(representative.get("round_title"))
    question = maybe_text(representative.get("round_subtitle_question"))
    order_values = [
        order
        for order in [numeric_program_order(item) for item in cluster]
        if order is not None
    ]
    merged = round_plan(
        round_id=round_label(index, slugify(title or question or family)),
        title=title or question.rstrip("?？") or family,
        subtitle=question or "What should the council resolve in this synthesized issue round?",
        round_mode=maybe_text(representative.get("round_mode")) or f"{family}-council",
        round_category=maybe_text(representative.get("round_category")) or family,
        active_theme_ids=ordered_unique(
            [
                theme_id
                for item in cluster
                for theme_id in list_items(item.get("active_theme_ids"))
            ]
        ),
        boundaries=ordered_unique(
            [
                boundary
                for item in cluster
                for boundary in list_texts_or_role_map(item.get("agent_responsibility_boundaries"))
            ]
        ),
        internal_phases=ordered_unique(
            [
                phase
                for item in [representative, *cluster]
                for phase in list_texts_or_role_map(item.get("round_internal_phases"))
            ],
            limit=14,
        ),
    )
    exits = ordered_unique(
        [
            criterion
            for item in cluster
            for criterion in list_texts_or_role_map(item.get("round_exit_criteria"))
        ],
        limit=16,
    )
    if exits:
        merged["round_exit_criteria"] = exits
    downgrades = ordered_unique(
        [
            condition
            for item in cluster
            for condition in list_texts_or_role_map(item.get("downgrade_conditions"))
        ],
        limit=16,
    )
    if downgrades:
        merged["downgrade_conditions"] = downgrades
    continuations = ordered_unique(
        [
            criterion
            for item in cluster
            for criterion in list_texts_or_role_map(item.get("continuation_criteria"))
        ],
        limit=8,
    )
    if continuations:
        merged["continuation_criteria"] = continuations
    if order_values:
        merged["program_order"] = min(order_values)
    merged["proposal_source"] = "merged-agent-position-rounds"
    merged["proposal_position_refs"] = ordered_unique(
        [item.get("proposal_position_ref") for item in cluster]
    )
    merged["contributing_agent_roles"] = ordered_unique(
        [item.get("proposed_by_role") for item in cluster]
    )
    merged["contributing_round_titles"] = ordered_unique(
        [item.get("round_title") for item in cluster]
    )
    merged["synthesis_note"] = (
        f"Merged {len(cluster)} agent-authored round proposal(s) into one "
        "council issue round; this is agenda synthesis, not source routing."
    )
    return merged


def merge_agent_round_proposals(normalized_rounds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters: dict[tuple[str, str], list[dict[str, Any]]] = {}
    cluster_order: list[tuple[tuple[float, int], tuple[str, str]]] = []
    for fallback, item in enumerate(normalized_rounds):
        key = round_cluster_key(item)
        if key not in clusters:
            clusters[key] = []
            order = numeric_program_order(item)
            cluster_order.append(((order if order is not None else 100000.0 + fallback, fallback), key))
        clusters[key].append(item)
    cluster_order.sort(key=lambda item: item[0])
    merged: list[dict[str, Any]] = []
    for next_index, (_, key) in enumerate(cluster_order, start=2):
        merged.append(merge_agent_round_cluster(clusters[key], index=next_index))
    return merged


def agent_authored_round_sequence(
    *,
    positions: list[dict[str, Any]],
    themes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    theme_by_id = {theme_ref(theme): theme for theme in themes if theme_ref(theme)}
    rounds: list[dict[str, Any]] = [
        round_plan(
            round_id="round-001-framing-scope",
            title="Framing and scope council",
            subtitle="What questions did the council decide to investigate, and how did agents split the later research agenda?",
            round_mode="framing-scope-council",
            round_category="planning",
            active_theme_ids=list(theme_by_id),
            boundaries=[
                "moderator: synthesize only agent-authored or agent-adopted agenda proposals, and record any unresolved split disagreement.",
                "challenger: review claim-slot overreach, denominator obligations, policy-basis boundaries, and unsupported strong wording.",
            ],
            internal_phases=["report-blueprint", "agent-position-proposals", "moderator-program-synthesis"],
        )
    ]
    proposal_rows: list[tuple[tuple[float, int], dict[str, Any], dict[str, Any]]] = []
    insertion_index = 0
    for position in positions:
        for proposal in position_round_proposals(position):
            proposal_rows.append((proposal_order_value(proposal, insertion_index), position, proposal))
            insertion_index += 1
    proposal_rows.sort(key=lambda item: item[0])
    normalized_proposals: list[dict[str, Any]] = []
    provisional_index = 2
    for _, position, proposal in proposal_rows:
        author_role = maybe_text(position.get("author_role"))
        position_ref_value = position_ref(position)
        normalized = normalize_agent_round_proposal(
            proposal,
            index=provisional_index,
            author_role=author_role,
            position_ref_value=position_ref_value,
            theme_by_id=theme_by_id,
        )
        normalized_proposals.append(normalized)
        provisional_index += 1
    rounds.extend(merge_agent_round_proposals(normalized_proposals))
    next_index = len(rounds) + 1
    if not any(maybe_text(item.get("round_category")) == "reporting" for item in rounds):
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "report-writing"),
                title="Report writing council handoff",
                subtitle="Which agent-carried materials can enter the report, and which claims must remain limitations?",
                round_mode="report-writing",
                round_category="reporting",
                active_theme_ids=list(theme_by_id),
                boundaries=[
                    "report-editor: organize only council-carried basis, section briefs, frozen basis, and visible limitations into report prose.",
                    "challenger: review strong claims, policy evaluation, public proportions, causal wording, and attribution language before report use.",
                    "moderator: ensure reporting handoff preserves unresolved boundaries, downgrade requirements, and transition approvals.",
                ],
                internal_phases=[
                    "agent-section-briefs",
                    "reporting-handoff",
                    "draft-validation",
                    "publication",
                ],
            )
        )
    return rounds


def fallback_round_sequence(themes: list[dict[str, Any]], blueprint: dict[str, Any]) -> list[dict[str, Any]]:
    theme_by_id = {theme_ref(theme): theme for theme in themes if theme_ref(theme)}
    fact_official_ids = unique_texts(
        [
            *theme_ids_matching(theme_by_id, "fact"),
            *theme_ids_matching(theme_by_id, "official", "policy-action"),
        ]
    )
    public_ids = theme_ids_matching(theme_by_id, "public", "semantic")
    interaction_ids = theme_ids_matching(theme_by_id, "interaction", "timeline")
    all_theme_ids = list(theme_by_id)
    synthesis_target_ids = [
        maybe_text(item.get("target_id"))
        for item in list_items(blueprint.get("synthesis_targets"))
        if isinstance(item, dict)
    ]
    rounds = [
        round_plan(
            round_id="round-001-framing-scope",
            title="Framing and scope council",
            subtitle="What questions must this report answer, and how should the council organize issue rounds?",
            round_mode="framing-scope-council",
            round_category="planning",
            active_theme_ids=list(theme_by_id),
            boundaries=[
                "moderator: synthesize report questions, issue-round boundaries, exit conditions, downgrade boundaries, and supplemental policy.",
                "challenger: review claim-slot overreach, denominator obligations, and unsupported policy-evaluation language.",
            ],
            internal_phases=["report-blueprint", "agent-positions", "moderator-program-synthesis"],
        )
    ]
    next_index = 2
    if fact_official_ids:
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "fact-official-acquisition"),
                title="Fact and official evidence acquisition council",
                subtitle="What evidence basis must agents acquire or explicitly downgrade for the fact process and official actions?",
                round_mode="evidence-acquisition-council",
                round_category="evidence-acquisition",
                active_theme_ids=fact_official_ids,
                boundaries=[
                    *theme_boundaries(theme_by_id, fact_official_ids),
                    "challenger: watch for premature source-origin, responsibility, policy-effect, or public-attitude wording while evidence basis is still being acquired.",
                ],
                internal_phases=[
                    "agenda-question",
                    "agent-evidence-boundary-turns",
                    "agent-acquisition-turns",
                    "in-round-recovery-feedback",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
        next_index += 1
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "fact-official-analysis"),
                title="Fact and official evidence analysis council",
                subtitle="Which fact-process and official-action claims can the council carry after acquired basis is reviewed?",
                round_mode="evidence-analysis-council",
                round_category="evidence-analysis",
                active_theme_ids=fact_official_ids,
                boundaries=[
                    *theme_boundaries(theme_by_id, fact_official_ids),
                    "moderator: distinguish supported fact sequence, official record presence, unresolved basis gaps, and report downgrade wording.",
                    "challenger: review causal, responsibility, effectiveness, and absence claims before they become report basis.",
                ],
                internal_phases=[
                    "agenda-question",
                    "agent-analysis-turns",
                    "challenger-boundary-review",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
        next_index += 1
    if public_ids:
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "public-semantics-acquisition"),
                title="Public semantics evidence acquisition council",
                subtitle="What bounded sample and denominator basis is needed before public, media, or formal semantic claims can be made?",
                round_mode="evidence-acquisition-council",
                round_category="evidence-acquisition",
                active_theme_ids=public_ids,
                boundaries=[
                    *theme_boundaries(theme_by_id, public_ids),
                    "social-investigator: make the sample boundary, denominator status, and representation limits visible before semantic interpretation.",
                    "challenger: prevent low-volume, zero-result, or partial material from becoming public-opinion or absence claims.",
                ],
                internal_phases=[
                    "agenda-question",
                    "agent-evidence-boundary-turns",
                    "agent-acquisition-turns",
                    "in-round-recovery-feedback",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
        next_index += 1
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "public-semantics-analysis"),
                title="Public semantics analysis council",
                subtitle="What semantic structures, shifts, and limits are visible within the bounded public, media, or formal material?",
                round_mode="semantic-analysis-council",
                round_category="semantic-analysis",
                active_theme_ids=public_ids,
                boundaries=[
                    *theme_boundaries(theme_by_id, public_ids),
                    "social-investigator: separate issue frames, risk perception, policy demands, trust or uncertainty cues, and attribution language as bounded semantic observations.",
                    "challenger: ensure public proportion, dominant concern, emotion, trust, or blame wording remains denominator-bounded.",
                ],
                internal_phases=[
                    "agenda-question",
                    "agent-analysis-turns",
                    "challenger-boundary-review",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
        next_index += 1
    if interaction_ids or all_theme_ids:
        active_ids = unique_texts([*interaction_ids, *all_theme_ids])
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "interaction-synthesis"),
                title="Fact-policy-public interaction synthesis council",
                subtitle="How do supported fact, official-action, and public-semantic materials line up over time without implying causality?",
                round_mode="interaction-synthesis-council",
                round_category="interaction-synthesis",
                active_theme_ids=active_ids,
                boundaries=[
                    *theme_boundaries(theme_by_id, active_ids),
                    "moderator: compose cross-lane chronology only from carried lane basis and explicitly mark descriptive co-visibility versus relation evidence.",
                    "challenger: block causal, response-effect, or public-reaction attribution unless relation basis is visible.",
                ],
                internal_phases=[
                    "agenda-question",
                    "lane-episode-assembly",
                    "agent-analysis-turns",
                    "challenger-boundary-review",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
        next_index += 1
    if "policy_evaluation_basis" in synthesis_target_ids or all_theme_ids:
        rounds.append(
            round_plan(
                round_id=round_label(next_index, "policy-evaluation-basis"),
                title="Policy evaluation basis review council",
                subtitle="What can the report use as policy-evaluation basis, and what must stay as limitation or future work?",
                round_mode="policy-basis-review-council",
                round_category="policy-basis-review",
                active_theme_ids=all_theme_ids,
                boundaries=[
                    "moderator: synthesize policy-evaluation basis only from carried fact, official-action, public-semantic, interaction, and limitation objects.",
                    "social-investigator: identify governance-record and public-semantic limits without turning policy_evaluation_basis into an acquisition lane.",
                    "challenger: require visible downgrade language for effectiveness, adequacy, representativeness, causality, and absence claims.",
                ],
                internal_phases=[
                    "agenda-question",
                    "claim-basis-matrix-review",
                    "challenger-boundary-review",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
        next_index += 1
    rounds.append(
        round_plan(
            round_id=round_label(next_index, "report-writing"),
            title="Report writing council handoff",
            subtitle="Which council-carried materials can enter the report, and which claims must remain limitations?",
            round_mode="report-writing",
            round_category="reporting",
            active_theme_ids=all_theme_ids,
            boundaries=[
                "report-editor: organize only council-carried basis, section briefs, frozen basis, and visible limitations into report prose.",
                "challenger: review strong claims, policy evaluation, public proportions, causal wording, and attribution language before report use.",
                "moderator: ensure reporting handoff preserves unresolved boundaries, downgrade requirements, and transition approvals.",
            ],
            internal_phases=[
                "agent-section-briefs",
                "reporting-handoff",
                "draft-validation",
                "publication",
            ],
        )
    )
    return rounds


def round_brief_payload_from_program_round(
    *,
    args: argparse.Namespace,
    program_id: str,
    program_object_id: str,
    round_item: dict[str, Any],
    output_file: Path,
) -> dict[str, Any]:
    target_round_id = maybe_text(round_item.get("round_id"))
    brief_id = "round-brief-" + stable_hash(args.run_id, program_id, target_round_id)[:12]
    round_category = maybe_text(round_item.get("round_category"))
    if round_category == "reporting":
        expected_objects = [
            "agent-section-brief",
            "reporting-handoff",
            "narrative-report-draft",
            "narrative-report-validation",
            "narrative-report",
        ]
    elif round_category == "evidence-acquisition":
        expected_objects = [
            "theme-evidence-boundary-plan",
            "source-acquisition-proposal",
            "evidence-route-assessment",
            "finding",
            "evidence-bundle",
            "theme-progress-review",
            "round-synthesis",
        ]
    elif round_category in {"evidence-analysis", "semantic-analysis"}:
        expected_objects = [
            "finding",
            "evidence-bundle",
            "theme-progress-review",
            "round-synthesis",
            "agent-section-brief",
        ]
    elif round_category == "interaction-synthesis":
        expected_objects = [
            "lane-episode-card",
            "interaction-timeline-node",
            "finding",
            "theme-progress-review",
            "round-synthesis",
            "agent-section-brief",
        ]
    elif round_category == "policy-basis-review":
        expected_objects = [
            "claim-basis-matrix",
            "challenge-ticket",
            "theme-progress-review",
            "round-synthesis",
            "agent-section-brief",
        ]
    else:
        expected_objects = [
            "theme-progress-review",
            "round-synthesis",
            "agent-section-brief",
        ]
    return {
        "run_id": args.run_id,
        "round_id": target_round_id,
        "object_kind": "round-brief",
        "object_id": brief_id,
        "brief_id": brief_id,
        "author_role": maybe_text(args.author_role) or "moderator",
        "decision_source": "moderator-program-synthesis",
        "status": "draft",
        "target_kind": "round",
        "target_id": target_round_id,
        "target_round_id": target_round_id,
        "planning_round_id": args.round_id,
        "target": {
            "object_kind": "round",
            "object_id": target_round_id,
        },
        "rationale": (
            "Program-projected round brief. It carries agenda questions, active "
            "themes, responsibility boundaries, exit criteria, and supplement "
            "policy only; it does not choose acquisition routes."
        ),
        "program_id": program_id,
        "round_title": maybe_text(round_item.get("round_title")),
        "round_subtitle_question": maybe_text(round_item.get("round_subtitle_question")),
        "round_mode": maybe_text(round_item.get("round_mode")),
        "round_category": round_category,
        "active_theme_ids": unique_texts(list_items(round_item.get("active_theme_ids"))),
        "agent_responsibility_boundaries": unique_texts(
            list_items(round_item.get("agent_responsibility_boundaries"))
        ),
        "round_internal_phases": unique_texts(list_items(round_item.get("round_internal_phases"))),
        "expected_council_objects": expected_objects,
        "round_exit_criteria": unique_texts(list_items(round_item.get("round_exit_criteria"))),
        "in_round_feedback_triggers": [
            "Low or zero result acquisition attempt requires source-owner recovery reflection before changing claim strength.",
            "Analysis turn can request in-round recovery when basis refs, denominator status, or policy lane coverage are not yet council-carried.",
            "Challenger concern about denominator, causal wording, public proportion, or policy evaluation boundary must be visible before report use.",
        ],
        "supplemental_round_policy": (
            "Supplemental issue council is only appropriate after in-round "
            "recovery is exhausted and a moderator synthesis, readiness opinion, "
            "report-basis gate, or transition approval carries the need."
        ),
        "forbidden_source_precommitments": [
            "Do not preselect provider families, skills, query variants, query parameters, route ranking, source priority, scheduler queue, or automatic execution.",
            "Acquisition and analysis organization is council-facing agenda context, not an automatic runtime phase machine.",
        ],
        "evidence_refs": [],
        "lineage": unique_texts([program_object_id, program_id, target_round_id]),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "moderator-program-synthesis",
            "artifact_path": str(output_file),
        },
    }


def synthesize_program(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"runtime/council_investigation_program_{args.round_id}.json",
    )
    blueprint, themes = load_blueprint_and_themes(
        run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        blueprint_id=maybe_text(args.blueprint_id),
    )
    positions = query_objects(
        run_dir,
        object_kind="agent-position",
        run_id=args.run_id,
        round_id=args.round_id,
        limit=100,
    )
    position_summaries = [
        summary
        for summary in [position_summary(position) for position in positions]
        if maybe_text(summary.get("position_ref"))
    ]
    position_roles = {maybe_text(item.get("author_role")) for item in positions}
    missing_roles = [role for role in EXPECTED_POSITION_ROLES if role not in position_roles]
    program_id = maybe_text(args.program_id) or "council-program-" + stable_hash(args.run_id, args.round_id, blueprint.get("object_id"))[:12]
    theme_threads = [
        {
            "theme_id": theme_ref(theme),
            "theme_question": agenda_question_for_theme(theme),
            "claim_slots_supported": list_items(theme.get("claim_slots_supported")),
            "claim_basis_boundary": maybe_text(theme.get("claim_boundary")),
            "owner_role": maybe_text(theme.get("owner_role")),
        }
        for theme in themes
        if theme_ref(theme)
    ]
    agent_round_proposal_count = sum(
        len(position_round_proposals(position)) for position in positions
    )
    if agent_round_proposal_count:
        round_sequence = agent_authored_round_sequence(
            positions=positions,
            themes=themes,
        )
        synthesis_mode = "agent-authored-round-proposals"
    else:
        round_sequence = fallback_round_sequence(themes, blueprint)
        synthesis_mode = "fallback-blueprint-derived-program"
    agenda_questions = unique_texts(
        [
            agenda_question_for_theme(theme)
            for theme in themes
            if theme_ref(theme)
        ]
        + [
            maybe_text(round_item.get("round_subtitle_question"))
            for round_item in round_sequence
        ]
        + [
            question
            for summary in position_summaries
            for question in list_items(summary.get("proposed_agenda_questions"))
        ]
    )
    boundaries = unique_texts(
        [
            boundary
            for round_item in round_sequence
            for boundary in list_items(round_item.get("agent_responsibility_boundaries"))
        ]
    )
    blueprint_ref = "report-blueprint:" + (
        maybe_text(blueprint.get("object_id")) or maybe_text(blueprint.get("blueprint_id"))
    )
    payload = {
        "run_id": args.run_id,
        "round_id": args.round_id,
        "object_kind": "council-investigation-program",
        "object_id": program_id,
        "program_id": program_id,
        "author_role": maybe_text(args.author_role) or "moderator",
        "agent_role": maybe_text(args.author_role) or "moderator",
        "decision_source": "moderator-program-synthesis",
        "status": "proposed",
        "adoption_status": "proposed-for-council-use",
        "program_synthesis_mode": synthesis_mode,
        "agent_authored_round_proposal_count": agent_round_proposal_count,
        "target_kind": "report-blueprint",
        "target_id": maybe_text(blueprint.get("object_id")) or maybe_text(blueprint.get("blueprint_id")),
        "target": {
            "object_kind": "report-blueprint",
            "object_id": maybe_text(blueprint.get("object_id")) or maybe_text(blueprint.get("blueprint_id")),
        },
        "rationale": (
            "Program-aware council flow synthesized from report blueprint and "
            "agent-authored round proposals when present; no acquisition route "
            "is selected."
        ),
        "mission_question": mission_question_from_blueprint(blueprint),
        "report_blueprint_ref": blueprint_ref,
        "agent_position_refs": unique_texts([position_ref(position) for position in positions]),
        "agent_position_summaries": position_summaries,
        "framing_position_roles": unique_texts([summary.get("author_role") for summary in position_summaries]),
        "missing_agent_position_roles": missing_roles,
        "program_questions": unique_texts(list_items(blueprint.get("report_questions")) or [mission_question_from_blueprint(blueprint)]),
        "theme_threads": theme_threads,
        "council_agenda_questions": agenda_questions,
        "agent_responsibility_boundaries": boundaries,
        "round_sequence": round_sequence,
        "round_internal_phase_model": [
            "round_internal_phases are descriptive organization hints only",
            "round categories should come from agent-authored or agent-adopted program proposals when those proposals are present",
            "progress review is advisory until carried by council object, moderator synthesis, readiness opinion, report-basis gate, or transition approval",
        ],
        "round_exit_criteria": [
            "Each active theme has a support, downgrade, scope-out, or named continuation disposition recorded by council-facing objects.",
            "Strong claims, public proportions, causal wording, attribution wording, and policy evaluation boundaries have challenger-visible review before report use.",
        ],
        "downgrade_conditions": [
            "Missing denominator basis downgrades public or formal semantic claims to examples, cues, limitations, or future work.",
            "Missing official action or governance basis blocks policy effectiveness conclusions.",
            "Missing relation or attribution basis downgrades causal, transport, source-origin, or public-response wording.",
        ],
        "supplemental_round_triggers": [
            "No reasonable in-round recovery remains for a named theme responsibility boundary.",
            "A challenger concern, denominator dispute, policy-lane absence, or source-limit dispute changes the issue boundary and needs moderator synthesis or transition approval.",
        ],
        "source_autonomy_boundary": "Investigators choose acquisition routes during their work turns or through source-acquisition-proposal / route assessment; this program does not choose sources, skills, routes, or queries.",
        "policy_evaluation_boundary": "policy_evaluation_basis is a report synthesis boundary from carried fact, official-action, public/formal semantic, interaction, and challenger limitation basis; it is not an acquisition lane.",
        "forbidden_scheduler_fields": list(FORBIDDEN_SCHEDULER_FIELDS),
        "evidence_refs": [],
        "lineage": unique_texts(
            [
                args.round_id,
                maybe_text(blueprint.get("object_id")),
                *[theme_ref(theme) for theme in themes],
                *[maybe_text(position.get("object_id")) for position in positions],
            ]
        ),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "moderator-program-synthesis",
            "artifact_path": str(output_file),
        },
    }
    result = append_dynamic_investigation_object_record(
        run_dir,
        object_payload=payload,
        object_kind="council-investigation-program",
        artifact_path=str(output_file),
        record_locator="$.council_investigation_program",
    )
    stored_program = dict_items(result.get("object"))
    materialized_round_briefs: list[dict[str, Any]] = []
    for round_item in list_items(stored_program.get("round_sequence")):
        if not isinstance(round_item, dict):
            continue
        brief_payload = round_brief_payload_from_program_round(
            args=args,
            program_id=maybe_text(stored_program.get("program_id")),
            program_object_id=maybe_text(stored_program.get("object_id")),
            round_item=round_item,
            output_file=output_file,
        )
        brief_result = append_dynamic_investigation_object_record(
            run_dir,
            object_payload=brief_payload,
            object_kind="round-brief",
            artifact_path=str(output_file),
            record_locator="$.materialized_round_briefs",
        )
        stored_brief = dict_items(brief_result.get("object"))
        if stored_brief:
            materialized_round_briefs.append(stored_brief)
    wrapper = {
        "schema_version": "council-investigation-program-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "council_investigation_program": stored_program,
        "program_round_sequence": list_items(stored_program.get("round_sequence")),
        "materialized_round_briefs": materialized_round_briefs,
        "missing_agent_position_roles": missing_roles,
        "program_boundaries": [
            "Program questions become council agenda questions, not fixed report sections or task queues.",
            "Agent responsibility boundaries are round responsibilities, not source/query/skill sequences.",
            "Round internal phases are descriptive organization hints, not a runtime state machine.",
        ],
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.council_investigation_program",
                "artifact_ref": f"{output_file}:$.council_investigation_program",
            },
            {
                "artifact_path": str(output_file),
                "record_locator": "$.materialized_round_briefs",
                "artifact_ref": f"{output_file}:$.materialized_round_briefs",
            }
        ],
        "provenance": {"skill_name": SKILL_NAME, "decision_source": "moderator-program-synthesis"},
    }
    write_json(output_file, wrapper)
    receipt_basis = stable_hash(
        args.run_id,
        args.round_id,
        stored_program.get("program_id"),
        json.dumps(
            list_items(stored_program.get("round_sequence")),
            ensure_ascii=True,
            sort_keys=True,
        ),
        stored_program.get("agent_authored_round_proposal_count"),
    )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "program_id": maybe_text(stored_program.get("program_id")),
            "round_count": len(list_items(stored_program.get("round_sequence"))),
            "materialized_round_brief_count": len(materialized_round_briefs),
            "agenda_question_count": len(list_items(stored_program.get("council_agenda_questions"))),
            "missing_agent_position_roles": missing_roles,
            "output_path": str(output_file),
            "db_path": maybe_text(result.get("db_path")),
        },
        "receipt_id": "council-program-receipt-" + receipt_basis[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": unique_texts(
            [
                maybe_text(stored_program.get("object_id")),
                *[maybe_text(brief.get("object_id")) for brief in materialized_round_briefs],
            ]
        ),
        "warnings": [
            {
                "code": "missing-expected-agent-positions",
                "message": "Missing framing positions: " + ", ".join(missing_roles),
            }
        ]
        if missing_roles
        else [],
        "council_handoff": {
            "object_refs": [
                {
                    "object_kind": "council-investigation-program",
                    "object_id": maybe_text(stored_program.get("object_id")),
                },
                *[
                    {
                        "object_kind": "round-brief",
                        "object_id": maybe_text(brief.get("object_id")),
                    }
                    for brief in materialized_round_briefs
                ],
            ],
            "suggested_next_skills": ["submit-round-brief", "open-investigation-round"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthesize a program-aware council investigation flow.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--blueprint-id", default="")
    parser.add_argument("--program-id", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = synthesize_program(args)
    except ValueError as exc:
        payload = {"status": "failed", "summary": {"skill": SKILL_NAME}, "message": str(exc)}
        sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

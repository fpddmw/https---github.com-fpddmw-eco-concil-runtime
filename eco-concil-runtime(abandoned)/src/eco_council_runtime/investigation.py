"""Causal-chain investigation planning primitives for eco-council runs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


SCHEMA_VERSION = "1.0.0"
CHAIN_LEG_ORDER = (
    "source",
    "mechanism",
    "impact",
    "public_interpretation",
)
ROLE_IDS = ("moderator", "sociologist", "environmentalist")
CLAIM_TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "wildfire": ("wildfire", "wild fire", "forest fire", "brush fire", "hotspot", "burn"),
    "smoke": ("smoke", "haze", "plume"),
    "air-pollution": ("air quality", "aqi", "pm2.5", "pm10", "air pollution", "particulate"),
    "flood": ("flood", "overflow", "inundation", "river stage", "discharge"),
    "heat": ("heat", "heatwave", "extreme heat", "high temperature"),
    "drought": ("drought", "dry spell", "soil moisture", "rain deficit"),
    "water-pollution": ("water pollution", "contamination", "polluted water"),
    "policy-reaction": ("policy", "regulation", "rulemaking", "comment period", "public comment"),
}
GAP_TYPES_BY_PROFILE_LEG: dict[tuple[str, str], list[str]] = {
    ("smoke-transport", "source"): ["fire-detection"],
    ("smoke-transport", "mechanism"): ["meteorology-background"],
    ("smoke-transport", "impact"): ["station-air-quality"],
    ("smoke-transport", "public_interpretation"): ["public-discussion-coverage"],
    ("flood-upstream", "source"): ["precipitation-hydrology"],
    ("flood-upstream", "mechanism"): ["precipitation-hydrology"],
    ("flood-upstream", "impact"): ["precipitation-hydrology"],
    ("flood-upstream", "public_interpretation"): ["public-discussion-coverage"],
}
GAP_TYPES_BY_CLAIM_TYPE: dict[str, list[str]] = {
    "wildfire": ["fire-detection", "meteorology-background"],
    "smoke": ["station-air-quality", "meteorology-background"],
    "air-pollution": ["station-air-quality", "meteorology-background"],
    "flood": ["precipitation-hydrology"],
    "heat": ["temperature-extremes"],
    "drought": ["precipitation-soil-moisture"],
    "water-pollution": ["normalized-public-claims"],
    "policy-reaction": ["policy-comment-coverage"],
}
SOURCE_SKILLS_BY_METRIC_FAMILY: dict[str, list[str]] = {
    "air-quality": ["openaq-data-fetch", "airnow-hourly-obs-fetch", "open-meteo-air-quality-fetch"],
    "fire-detection": ["nasa-firms-fire-fetch"],
    "meteorology": ["open-meteo-historical-fetch"],
    "hydrology": ["open-meteo-flood-fetch"],
    "soil": ["open-meteo-historical-fetch"],
}
SOURCE_SKILLS_BY_GAP_TYPE: dict[str, list[str]] = {
    "station-air-quality": ["openaq-data-fetch", "airnow-hourly-obs-fetch", "open-meteo-air-quality-fetch"],
    "fire-detection": ["nasa-firms-fire-fetch"],
    "meteorology-background": ["open-meteo-historical-fetch"],
    "precipitation-hydrology": ["open-meteo-flood-fetch", "open-meteo-historical-fetch"],
    "precipitation-soil-moisture": ["open-meteo-historical-fetch"],
    "temperature-extremes": ["open-meteo-historical-fetch"],
    "public-discussion-coverage": ["gdelt-doc-search", "bluesky-cascade-fetch", "youtube-video-search"],
    "policy-comment-coverage": ["federal-register-search", "regulationsgov-comments-fetch"],
    "normalized-public-claims": ["gdelt-doc-search", "bluesky-cascade-fetch"],
    "evidence-cards-linking-public-claims-to-physical-observations": [
        "gdelt-doc-search",
        "openaq-data-fetch",
        "open-meteo-historical-fetch",
    ],
}
ALTERNATIVE_HYPOTHESIS_TEMPLATES: dict[str, list[dict[str, Any]]] = {
    "smoke-transport": [
        {
            "slug": "local-source",
            "statement": "Mission-region local emissions or local fires, rather than transported external smoke, drove the air-quality degradation.",
            "summary": "Rule out mission-region emissions or local fires before confirming cross-region smoke transport.",
            "claim_types": ["smoke", "air-pollution", "wildfire"],
            "metric_families": ["air-quality", "fire-detection"],
            "query_cues": ["local emissions", "mission-region fire activity", "urban pollution sources"],
            "gap_types": ["station-air-quality", "fire-detection", "public-discussion-coverage"],
            "scope_mode": "mission",
            "region_hint": "mission-region",
            "priority": "high",
            "reason": "Cross-region attribution remains weak if mission-region drivers are not explicitly bounded.",
        },
        {
            "slug": "weather-trap",
            "statement": "Background local pollution plus stagnant weather, not transported wildfire smoke, explains the air-quality spike.",
            "summary": "Check whether stagnant local meteorology can explain the AQI rise without long-range transport.",
            "claim_types": ["smoke", "air-pollution"],
            "metric_families": ["air-quality", "meteorology"],
            "query_cues": ["stagnant air", "temperature inversion", "background pollution buildup"],
            "gap_types": ["station-air-quality", "meteorology-background"],
            "scope_mode": "mission",
            "region_hint": "mission-region",
            "priority": "medium",
            "reason": "Some AQI spikes are local accumulation events rather than transport events.",
        },
    ],
    "flood-upstream": [
        {
            "slug": "local-rainfall",
            "statement": "Mission-region rainfall or drainage failure, rather than upstream propagation, caused the flood impact.",
            "summary": "Test whether local rainfall or drainage breakdown explains the flood without upstream transfer.",
            "claim_types": ["flood"],
            "metric_families": ["meteorology", "hydrology"],
            "query_cues": ["local rainfall", "urban drainage failure", "flash flood"],
            "gap_types": ["precipitation-hydrology", "public-discussion-coverage"],
            "scope_mode": "mission",
            "region_hint": "mission-region",
            "priority": "high",
            "reason": "Upstream attribution is weak if local forcing remains plausible.",
        },
        {
            "slug": "operations-or-coastal",
            "statement": "Infrastructure operations or coastal/tidal conditions, rather than upstream runoff, dominated the flood outcome.",
            "summary": "Check engineered releases, drainage operations, or coastal forcing before assuming upstream propagation.",
            "claim_types": ["flood"],
            "metric_families": ["hydrology"],
            "query_cues": ["reservoir release", "tide level", "stormwater operations"],
            "gap_types": ["precipitation-hydrology", "public-discussion-coverage"],
            "scope_mode": "derived-region",
            "region_hint": "derived-river-corridor",
            "priority": "medium",
            "reason": "Flood impacts can be dominated by infrastructure or coastal forcing instead of upstream precipitation.",
        },
    ],
    "local-event": [
        {
            "slug": "background-variation",
            "statement": "The reported event is background variation or misattribution rather than a discrete mission-window incident.",
            "summary": "Check whether the signal is baseline noise, weak attribution, or reporting exaggeration.",
            "claim_types": [],
            "metric_families": [],
            "query_cues": ["baseline conditions", "background variation", "misattribution"],
            "gap_types": ["normalized-public-claims"],
            "scope_mode": "mission",
            "region_hint": "mission-region",
            "priority": "high",
            "reason": "Local-event investigations should first confirm that a distinct event actually exists.",
        },
        {
            "slug": "different-driver",
            "statement": "A different local mechanism or adjacent-region spillover, not the current framing, best explains the observed impact.",
            "summary": "Keep a second mechanism alive so the plan does not collapse onto one framing too early.",
            "claim_types": [],
            "metric_families": [],
            "query_cues": ["adjacent region spillover", "different mechanism", "alternative local driver"],
            "gap_types": ["evidence-cards-linking-public-claims-to-physical-observations"],
            "scope_mode": "mission",
            "region_hint": "mission-region",
            "priority": "medium",
            "reason": "Early local-event framing can overfit to the first narrative that appears in public claims.",
        },
    ],
}

INVESTIGATION_PROFILES: dict[str, dict[str, Any]] = {
    "local-event": {
        "profile_id": "local-event",
        "summary": "Local-event verification around one mission region and window.",
        "legs": [
            {
                "leg_id": "impact",
                "label": "Mission-region physical impact",
                "required": True,
                "scope_mode": "mission",
                "region_hint": "mission-region",
                "metric_families": ["air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other"],
                "claim_types": [
                    "wildfire",
                    "smoke",
                    "flood",
                    "heat",
                    "drought",
                    "air-pollution",
                    "water-pollution",
                ],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "local physical impact",
                    "mission-region measurements",
                    "mission-window observations",
                ],
                "evidence_focus": [
                    "Direct mission-region physical observations that describe the event impact.",
                    "At least one auditable observation should land inside the mission window and geography.",
                ],
                "success_criteria": (
                    "At least one mission-window physical observation should directly describe the mission-region impact."
                ),
            },
            {
                "leg_id": "public_interpretation",
                "label": "Mission-region public interpretation",
                "required": False,
                "scope_mode": "mission",
                "region_hint": "mission-region",
                "metric_families": [],
                "claim_types": [
                    "wildfire",
                    "smoke",
                    "flood",
                    "heat",
                    "drought",
                    "air-pollution",
                    "water-pollution",
                    "policy-reaction",
                ],
                "preferred_roles": ["sociologist", "moderator"],
                "query_cues": [
                    "public interpretation",
                    "local reports",
                    "mission-region discussion",
                ],
                "evidence_focus": [
                    "Attributable public claims or narratives explaining what happened in the mission region.",
                ],
                "success_criteria": (
                    "Attributable public claims should explain how the event was described or interpreted in the mission window."
                ),
            },
        ],
    },
    "smoke-transport": {
        "profile_id": "smoke-transport",
        "summary": "Cross-region smoke attribution from source activity through transport into receptor impacts.",
        "legs": [
            {
                "leg_id": "source",
                "label": "Source-region fire activity",
                "required": True,
                "scope_mode": "derived-region",
                "region_hint": "derived-source-region",
                "metric_families": ["fire-detection"],
                "claim_types": ["wildfire", "smoke"],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "wildfire detections",
                    "hotspots",
                    "source smoke activity",
                ],
                "evidence_focus": [
                    "Identify attributable fire activity in the probable source region.",
                    "Prefer fire detections or source-side wildfire reporting that overlaps the mission window.",
                ],
                "success_criteria": (
                    "At least one auditable source-region fire or burn signal should plausibly explain the downstream smoke episode."
                ),
            },
            {
                "leg_id": "mechanism",
                "label": "Transport or propagation mechanism",
                "required": True,
                "scope_mode": "derived-region",
                "region_hint": "derived-transport-corridor",
                "metric_families": ["meteorology"],
                "claim_types": ["smoke", "air-pollution"],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "smoke transport",
                    "wind corridor",
                    "plume movement",
                    "trajectory",
                ],
                "evidence_focus": [
                    "Capture transport conditions linking source activity to the receptor region.",
                    "Wind, humidity, or plume-spread context should explain how the impact could travel.",
                ],
                "success_criteria": (
                    "Meteorological or transport evidence should plausibly connect the source region to the receptor impact."
                ),
            },
            {
                "leg_id": "impact",
                "label": "Receptor-region air-quality impact",
                "required": True,
                "scope_mode": "mission",
                "region_hint": "mission-region",
                "metric_families": ["air-quality"],
                "claim_types": ["smoke", "air-pollution"],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "AQI spike",
                    "PM2.5 rise",
                    "mission-region haze",
                ],
                "evidence_focus": [
                    "Show receptor-region air-quality degradation inside the mission region and time window.",
                    "Prefer station-grade or directly observed impact measurements when available.",
                ],
                "success_criteria": (
                    "Mission-region air-quality observations should show a receptor impact aligned with the smoke hypothesis."
                ),
            },
            {
                "leg_id": "public_interpretation",
                "label": "Public attribution and severity narratives",
                "required": False,
                "scope_mode": "mission",
                "region_hint": "mission-region",
                "metric_families": [],
                "claim_types": ["smoke", "air-pollution", "wildfire"],
                "preferred_roles": ["sociologist", "moderator"],
                "query_cues": [
                    "wildfire smoke",
                    "air quality concern",
                    "haze attribution",
                ],
                "evidence_focus": [
                    "Capture how public narratives attribute local air-quality impacts to wildfire smoke.",
                ],
                "success_criteria": (
                    "Public claims should clearly attribute or debate the smoke impact in the mission region."
                ),
            },
        ],
    },
    "flood-upstream": {
        "profile_id": "flood-upstream",
        "summary": "Upstream-to-downstream flood verification with hydrometeorological causal links.",
        "legs": [
            {
                "leg_id": "source",
                "label": "Upstream precipitation or source pressure",
                "required": True,
                "scope_mode": "derived-region",
                "region_hint": "derived-upstream-region",
                "metric_families": ["meteorology", "hydrology"],
                "claim_types": ["flood"],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "upstream rainfall",
                    "basin inflow",
                    "headwater pressure",
                ],
                "evidence_focus": [
                    "Identify upstream precipitation or source pressure that could initiate downstream flooding.",
                ],
                "success_criteria": (
                    "Upstream precipitation or hydrologic pressure should plausibly precede the mission-region flood impact."
                ),
            },
            {
                "leg_id": "mechanism",
                "label": "Hydrologic transfer or propagation mechanism",
                "required": True,
                "scope_mode": "derived-region",
                "region_hint": "derived-river-corridor",
                "metric_families": ["meteorology", "hydrology"],
                "claim_types": ["flood"],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "river discharge",
                    "downstream propagation",
                    "flood routing",
                ],
                "evidence_focus": [
                    "Capture the corridor or transfer mechanism that carries upstream pressure into the mission region.",
                ],
                "success_criteria": (
                    "Hydrologic or meteorological transfer evidence should connect upstream pressure to downstream flooding."
                ),
            },
            {
                "leg_id": "impact",
                "label": "Receptor flood impact",
                "required": True,
                "scope_mode": "mission",
                "region_hint": "mission-region",
                "metric_families": ["hydrology", "meteorology"],
                "claim_types": ["flood"],
                "preferred_roles": ["environmentalist", "moderator"],
                "query_cues": [
                    "flood impact",
                    "river stage",
                    "mission-region inundation",
                ],
                "evidence_focus": [
                    "Show mission-region flood impact with auditable local observations.",
                ],
                "success_criteria": (
                    "Mission-region flood impact should be visible through local hydrologic or precipitation-linked observations."
                ),
            },
            {
                "leg_id": "public_interpretation",
                "label": "Public flood framing and response",
                "required": False,
                "scope_mode": "mission",
                "region_hint": "mission-region",
                "metric_families": [],
                "claim_types": ["flood"],
                "preferred_roles": ["sociologist", "moderator"],
                "query_cues": [
                    "flood response",
                    "public flood reports",
                    "downstream flooding discussion",
                ],
                "evidence_focus": [
                    "Capture public framing, attribution, and local response narratives around the flood.",
                ],
                "success_criteria": (
                    "Public claims should describe or interpret the flood impact in the mission region."
                ),
            },
        ],
    },
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def unique_strings(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def normalized_unique_strings(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = maybe_text(value).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def truncate_text(value: str, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def role_prioritizes_leg(role: str, leg: dict[str, Any]) -> bool:
    if role == "moderator":
        return True
    preferred_roles = {
        maybe_text(item)
        for item in (leg.get("preferred_roles") if isinstance(leg.get("preferred_roles"), list) else [])
        if maybe_text(item)
    }
    if preferred_roles:
        return role in preferred_roles
    leg_id = maybe_text(leg.get("leg_id"))
    if role == "sociologist":
        return leg_id == "public_interpretation"
    return leg_id != "public_interpretation"


def infer_claim_types_from_text(*values: Any) -> list[str]:
    text = " ".join(maybe_text(value).lower() for value in values if maybe_text(value))
    if not text:
        return []
    matched: list[str] = []
    for claim_type, keywords in CLAIM_TYPE_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            matched.append(claim_type)
    return matched


def hypothesis_claim_types(*, profile: dict[str, Any], statement: str, mission: dict[str, Any]) -> list[str]:
    inferred = infer_claim_types_from_text(statement, mission.get("objective"), mission.get("topic"))
    if inferred:
        return inferred
    ordered: list[str] = []
    for leg in profile.get("legs", []):
        if not isinstance(leg, dict):
            continue
        if not bool(leg.get("required")) and maybe_text(leg.get("leg_id")) == "public_interpretation":
            continue
        ordered.extend(
            maybe_text(item)
            for item in (leg.get("claim_types") if isinstance(leg.get("claim_types"), list) else [])
            if maybe_text(item)
        )
    return unique_strings(ordered)


def gap_types_for_leg(*, profile_id: str, leg: dict[str, Any], claim_types: list[str]) -> list[str]:
    leg_id = maybe_text(leg.get("leg_id"))
    explicit = GAP_TYPES_BY_PROFILE_LEG.get((profile_id, leg_id), [])
    if explicit:
        return list(explicit)

    metric_families = {
        maybe_text(item)
        for item in (leg.get("metric_families") if isinstance(leg.get("metric_families"), list) else [])
        if maybe_text(item)
    }
    gaps: list[str] = []
    for claim_type in claim_types:
        gaps.extend(GAP_TYPES_BY_CLAIM_TYPE.get(claim_type, []))
    if "air-quality" in metric_families:
        gaps.append("station-air-quality")
    if "fire-detection" in metric_families:
        gaps.append("fire-detection")
    if "meteorology" in metric_families and "flood" not in claim_types:
        gaps.append("meteorology-background")
    if "hydrology" in metric_families or ("meteorology" in metric_families and "flood" in claim_types):
        gaps.append("precipitation-hydrology")
    if "soil" in metric_families:
        gaps.append("precipitation-soil-moisture")
    if leg_id == "public_interpretation":
        if "policy-reaction" in claim_types:
            gaps.append("policy-comment-coverage")
        else:
            gaps.append("public-discussion-coverage")
    if not gaps:
        gaps.append(
            "public-discussion-coverage"
            if leg_id == "public_interpretation"
            else "evidence-cards-linking-public-claims-to-physical-observations"
        )
    return unique_strings(gaps)


def recommended_source_skills(*, metric_families: list[str], gap_types: list[str]) -> list[str]:
    ordered: list[str] = []
    for family in metric_families:
        ordered.extend(SOURCE_SKILLS_BY_METRIC_FAMILY.get(maybe_text(family), []))
    for gap_type in gap_types:
        ordered.extend(SOURCE_SKILLS_BY_GAP_TYPE.get(maybe_text(gap_type), []))
    return unique_strings(ordered)


def build_alternative_hypotheses(*, profile_id: str, hypothesis_id: str, claim_types: list[str]) -> list[dict[str, Any]]:
    alternatives: list[dict[str, Any]] = []
    for index, template in enumerate(ALTERNATIVE_HYPOTHESIS_TEMPLATES.get(profile_id, []), start=1):
        statement = maybe_text(template.get("statement"))
        template_claim_types = unique_strings(
            (template.get("claim_types") if isinstance(template.get("claim_types"), list) else [])
        ) or list(claim_types)
        metric_families = unique_strings(
            (template.get("metric_families") if isinstance(template.get("metric_families"), list) else [])
        )
        gap_types = unique_strings(
            (template.get("gap_types") if isinstance(template.get("gap_types"), list) else [])
        ) or [
            item
            for claim_type in template_claim_types
            for item in GAP_TYPES_BY_CLAIM_TYPE.get(claim_type, [])
        ]
        alternatives.append(
            {
                "alternative_id": f"{hypothesis_id}-alternative-{index:02d}",
                "slug": maybe_text(template.get("slug")) or f"alternative-{index:02d}",
                "statement": statement,
                "summary": maybe_text(template.get("summary")) or truncate_text(statement, 180),
                "status": "pending",
                "priority": maybe_text(template.get("priority")) or "medium",
                "scope_mode": maybe_text(template.get("scope_mode")) or "mission",
                "region_hint": maybe_text(template.get("region_hint")) or "mission-region",
                "claim_types": template_claim_types,
                "metric_families": metric_families,
                "gap_types": unique_strings(gap_types),
                "query_cues": unique_strings(
                    template.get("query_cues", []) if isinstance(template.get("query_cues"), list) else []
                ),
                "reason": maybe_text(template.get("reason")),
                "source_skills": recommended_source_skills(metric_families=metric_families, gap_types=gap_types),
            }
        )
    return alternatives


def build_fetch_intent(
    *,
    profile_id: str,
    hypothesis_id: str,
    leg: dict[str, Any],
    claim_types: list[str],
    alternative_hypotheses: list[dict[str, Any]],
) -> dict[str, Any]:
    leg_claim_types = unique_strings(
        (leg.get("claim_types") if isinstance(leg.get("claim_types"), list) else [])
    ) or list(claim_types)
    metric_families = unique_strings(
        leg.get("metric_families", []) if isinstance(leg.get("metric_families"), list) else []
    )
    gap_types = gap_types_for_leg(profile_id=profile_id, leg=leg, claim_types=leg_claim_types)
    evidence_focus = unique_strings(leg.get("evidence_focus", []) if isinstance(leg.get("evidence_focus"), list) else [])
    reason = maybe_text(evidence_focus[0] if evidence_focus else leg.get("success_criteria"))
    return {
        "intent_id": f"{hypothesis_id}-{maybe_text(leg.get('leg_id'))}-fetch",
        "hypothesis_id": hypothesis_id,
        "leg_id": maybe_text(leg.get("leg_id")),
        "label": maybe_text(leg.get("label")),
        "priority": "critical" if bool(leg.get("required")) else "supporting",
        "status": "pending",
        "required_for_support": bool(leg.get("required")),
        "scope_mode": maybe_text(leg.get("scope_mode")),
        "region_hint": maybe_text(leg.get("region_hint")),
        "claim_types": leg_claim_types,
        "metric_families": metric_families,
        "gap_types": gap_types,
        "query_cues": unique_strings(leg.get("query_cues", []) if isinstance(leg.get("query_cues"), list) else []),
        "reason": reason,
        "success_criteria": maybe_text(leg.get("success_criteria")),
        "competing_alternative_ids": [
            maybe_text(item.get("alternative_id"))
            for item in alternative_hypotheses
            if isinstance(item, dict) and maybe_text(item.get("alternative_id"))
        ],
        "source_skills": recommended_source_skills(metric_families=metric_families, gap_types=gap_types),
    }


def build_history_query(
    *,
    mission: dict[str, Any],
    profile_id: str,
    plan_hypotheses: list[dict[str, Any]],
    fetch_intents: list[dict[str, Any]],
) -> dict[str, Any]:
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    query_fragments = unique_strings(
        [
            maybe_text(mission.get("topic")),
            maybe_text(mission.get("objective")),
            *[
                maybe_text(item.get("summary"))
                for item in plan_hypotheses
                if isinstance(item, dict)
            ],
        ]
    )
    claim_types: list[str] = []
    metric_families: list[str] = []
    gap_types: list[str] = []
    source_skills: list[str] = []
    alternatives: list[str] = []
    for hypothesis in plan_hypotheses:
        if not isinstance(hypothesis, dict):
            continue
        claim_types.extend(hypothesis.get("claim_types", []) if isinstance(hypothesis.get("claim_types"), list) else [])
        for alternative in hypothesis.get("alternative_hypotheses", []):
            if not isinstance(alternative, dict):
                continue
            claim_types.extend(
                alternative.get("claim_types", []) if isinstance(alternative.get("claim_types"), list) else []
            )
            metric_families.extend(
                alternative.get("metric_families", []) if isinstance(alternative.get("metric_families"), list) else []
            )
            gap_types.extend(alternative.get("gap_types", []) if isinstance(alternative.get("gap_types"), list) else [])
            source_skills.extend(
                alternative.get("source_skills", []) if isinstance(alternative.get("source_skills"), list) else []
            )
            if maybe_text(alternative.get("summary")):
                alternatives.append(maybe_text(alternative.get("summary")))
    for intent in fetch_intents:
        if not isinstance(intent, dict):
            continue
        claim_types.extend(intent.get("claim_types", []) if isinstance(intent.get("claim_types"), list) else [])
        metric_families.extend(intent.get("metric_families", []) if isinstance(intent.get("metric_families"), list) else [])
        gap_types.extend(intent.get("gap_types", []) if isinstance(intent.get("gap_types"), list) else [])
        source_skills.extend(intent.get("source_skills", []) if isinstance(intent.get("source_skills"), list) else [])

    return {
        "query": " | ".join(query_fragments[:4]),
        "region_label": maybe_text(region.get("label")),
        "profile_id": profile_id,
        "claim_types": unique_strings(claim_types),
        "metric_families": unique_strings(metric_families),
        "gap_types": unique_strings(gap_types),
        "source_skills": unique_strings(source_skills),
        "priority_leg_ids": unique_strings(
            [
                maybe_text(item.get("leg_id"))
                for item in fetch_intents
                if isinstance(item, dict) and bool(item.get("required_for_support"))
            ]
        ),
        "alternative_hypotheses": unique_strings(alternatives),
    }


def build_plan_open_questions(
    *,
    plan_hypotheses: list[dict[str, Any]],
    fetch_intents: list[dict[str, Any]],
) -> list[str]:
    questions: list[str] = []
    for intent in fetch_intents:
        if not isinstance(intent, dict) or not bool(intent.get("required_for_support")):
            continue
        label = maybe_text(intent.get("label")).lower() or maybe_text(intent.get("leg_id"))
        gap_types = intent.get("gap_types") if isinstance(intent.get("gap_types"), list) else []
        gap_text = ", ".join(maybe_text(item) for item in gap_types[:2] if maybe_text(item))
        if gap_text:
            questions.append(f"Which auditable evidence can close {gap_text} for {label}?")
        else:
            questions.append(f"Which auditable evidence can resolve {label}?")
    for hypothesis in plan_hypotheses:
        if not isinstance(hypothesis, dict):
            continue
        alternatives = hypothesis.get("alternative_hypotheses")
        if not isinstance(alternatives, list) or not alternatives:
            continue
        primary_summary = maybe_text(hypothesis.get("summary")) or maybe_text(hypothesis.get("statement"))
        first_alternative = alternatives[0] if isinstance(alternatives[0], dict) else {}
        alternative_summary = maybe_text(first_alternative.get("summary")) or maybe_text(first_alternative.get("statement"))
        if primary_summary and alternative_summary:
            questions.append(f"Which signals distinguish {primary_summary} from the alternative that {alternative_summary.lower()}?")
    return unique_strings(questions)[:8]


def compact_leg_guidance(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "leg_id": maybe_text(leg.get("leg_id")),
        "label": maybe_text(leg.get("label")),
        "required": bool(leg.get("required")),
        "scope_mode": maybe_text(leg.get("scope_mode")),
        "region_hint": maybe_text(leg.get("region_hint")),
        "metric_families": unique_strings(leg.get("metric_families", []) if isinstance(leg.get("metric_families"), list) else []),
        "claim_types": unique_strings(leg.get("claim_types", []) if isinstance(leg.get("claim_types"), list) else []),
        "gap_types": unique_strings(leg.get("gap_types", []) if isinstance(leg.get("gap_types"), list) else []),
        "query_cues": unique_strings(leg.get("query_cues", []) if isinstance(leg.get("query_cues"), list) else []),
        "evidence_focus": unique_strings(leg.get("evidence_focus", []) if isinstance(leg.get("evidence_focus"), list) else []),
        "success_criteria": maybe_text(leg.get("success_criteria")),
        "priority_for_role": {
            role: role_prioritizes_leg(role, leg)
            for role in ROLE_IDS
        },
    }


def causal_focus_for_role(plan: dict[str, Any], role: str) -> dict[str, Any]:
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    focused_hypotheses: list[dict[str, Any]] = []
    priority_metric_families: list[str] = []
    priority_claim_types: list[str] = []
    priority_gap_types: list[str] = []
    query_cues: list[str] = []
    for hypothesis in hypotheses:
        if not isinstance(hypothesis, dict):
            continue
        priority_legs: list[dict[str, Any]] = []
        secondary_legs: list[dict[str, Any]] = []
        for raw_leg in hypothesis.get("chain_legs", []):
            if not isinstance(raw_leg, dict):
                continue
            leg = compact_leg_guidance(raw_leg)
            if role_prioritizes_leg(role, raw_leg):
                priority_legs.append(leg)
                priority_metric_families.extend(leg.get("metric_families", []))
                priority_claim_types.extend(leg.get("claim_types", []))
                priority_gap_types.extend(leg.get("gap_types", []))
                query_cues.extend(leg.get("query_cues", []))
            else:
                secondary_legs.append(leg)
        alternative_hypotheses = []
        for raw_alternative in hypothesis.get("alternative_hypotheses", []):
            if not isinstance(raw_alternative, dict):
                continue
            alternative_hypotheses.append(
                {
                    "alternative_id": maybe_text(raw_alternative.get("alternative_id")),
                    "summary": maybe_text(raw_alternative.get("summary")),
                    "priority": maybe_text(raw_alternative.get("priority")),
                    "gap_types": unique_strings(
                        raw_alternative.get("gap_types", [])
                        if isinstance(raw_alternative.get("gap_types"), list)
                        else []
                    ),
                    "query_cues": unique_strings(
                        raw_alternative.get("query_cues", [])
                        if isinstance(raw_alternative.get("query_cues"), list)
                        else []
                    ),
                }
            )
        focused_hypotheses.append(
            {
                "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
                "statement": maybe_text(hypothesis.get("statement")),
                "summary": maybe_text(hypothesis.get("summary")),
                "priority_legs": priority_legs,
                "secondary_legs": secondary_legs,
                "alternative_hypotheses": alternative_hypotheses,
            }
        )
    return {
        "role": role,
        "profile_id": maybe_text(plan.get("profile_id")),
        "profile_summary": maybe_text(plan.get("profile_summary")),
        "priority_metric_families": unique_strings(priority_metric_families),
        "priority_claim_types": unique_strings(priority_claim_types),
        "priority_gap_types": unique_strings(priority_gap_types),
        "query_cues": unique_strings(query_cues),
        "hypotheses": focused_hypotheses,
    }


def infer_investigation_profile(mission: dict[str, Any]) -> str:
    topic_text = " ".join(
        maybe_text(item)
        for item in (
            mission.get("topic"),
            mission.get("objective"),
            " ".join(maybe_text(h) for h in mission.get("hypotheses", []) if maybe_text(h)),
        )
        if maybe_text(item)
    ).lower()
    if any(token in topic_text for token in ("smoke", "wildfire", "haze", "aqi", "pm2.5", "pm10", "air quality")):
        return "smoke-transport"
    if any(token in topic_text for token in ("flood", "river", "overflow", "inundation", "discharge")):
        return "flood-upstream"
    return "local-event"


def build_investigation_plan(*, mission: dict[str, Any], round_id: str) -> dict[str, Any]:
    run_id = maybe_text(mission.get("run_id"))
    profile_id = infer_investigation_profile(mission)
    profile = INVESTIGATION_PROFILES[profile_id]
    hypotheses = [
        maybe_text(item)
        for item in mission.get("hypotheses", [])
        if maybe_text(item)
    ]
    if not hypotheses:
        hypotheses = [maybe_text(mission.get("objective")) or maybe_text(mission.get("topic")) or "Mission investigation"]

    plan_hypotheses: list[dict[str, Any]] = []
    fetch_intents: list[dict[str, Any]] = []
    for index, statement in enumerate(hypotheses, start=1):
        hypothesis_id = f"hypothesis-{index:03d}"
        claim_types = hypothesis_claim_types(profile=profile, statement=statement, mission=mission)
        alternative_hypotheses = build_alternative_hypotheses(
            profile_id=profile_id,
            hypothesis_id=hypothesis_id,
            claim_types=claim_types,
        )
        legs = []
        for leg in profile["legs"]:
            leg_id = maybe_text(leg.get("leg_id"))
            gap_types = gap_types_for_leg(profile_id=profile_id, leg=leg, claim_types=claim_types)
            fetch_intent = build_fetch_intent(
                profile_id=profile_id,
                hypothesis_id=hypothesis_id,
                leg=leg,
                claim_types=claim_types,
                alternative_hypotheses=alternative_hypotheses,
            )
            legs.append(
                {
                    "leg_id": leg_id,
                    "label": maybe_text(leg.get("label")),
                    "required": bool(leg.get("required")),
                    "status": "pending",
                    "scope_mode": maybe_text(leg.get("scope_mode")) or ("mission" if leg_id in {"impact", "public_interpretation"} else "derived-region"),
                    "region_hint": maybe_text(leg.get("region_hint")),
                    "metric_families": unique_strings(leg.get("metric_families", []) if isinstance(leg.get("metric_families"), list) else []),
                    "claim_types": unique_strings(leg.get("claim_types", []) if isinstance(leg.get("claim_types"), list) else []),
                    "gap_types": gap_types,
                    "preferred_roles": unique_strings(leg.get("preferred_roles", []) if isinstance(leg.get("preferred_roles"), list) else []),
                    "query_cues": unique_strings(leg.get("query_cues", []) if isinstance(leg.get("query_cues"), list) else []),
                    "evidence_focus": unique_strings(leg.get("evidence_focus", []) if isinstance(leg.get("evidence_focus"), list) else []),
                    "success_criteria": maybe_text(leg.get("success_criteria")),
                    "notes": "",
                }
            )
            fetch_intents.append(fetch_intent)
        plan_hypotheses.append(
            {
                "hypothesis_id": hypothesis_id,
                "statement": statement,
                "summary": truncate_text(statement, 180),
                "profile_id": profile_id,
                "claim_types": claim_types,
                "chain_legs": legs,
                "alternative_hypotheses": alternative_hypotheses,
                "investigation_status": "pending",
            }
        )

    history_query = build_history_query(
        mission=mission,
        profile_id=profile_id,
        plan_hypotheses=plan_hypotheses,
        fetch_intents=fetch_intents,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "plan_id": f"investigation-plan-{round_id}",
        "run_id": run_id,
        "round_id": round_id,
        "profile_id": profile_id,
        "profile_summary": maybe_text(profile.get("summary")),
        "generated_at_utc": utc_now_iso(),
        "mission_region": mission.get("region"),
        "mission_window": mission.get("window"),
        "hypotheses": plan_hypotheses,
        "fetch_intents": fetch_intents,
        "history_query": history_query,
        "open_questions": build_plan_open_questions(plan_hypotheses=plan_hypotheses, fetch_intents=fetch_intents),
        "notes": [
            "This plan is scaffolded before any role has curated evidence. Later rounds should refine source regions, corridors, and leg-specific success criteria.",
            "Cross-region investigation is allowed when profile legs mark scope_mode=derived-region.",
            "Alternative hypotheses should be actively tested instead of treated as implicit background doubt.",
        ],
    }

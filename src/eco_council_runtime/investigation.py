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


def compact_leg_guidance(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "leg_id": maybe_text(leg.get("leg_id")),
        "label": maybe_text(leg.get("label")),
        "required": bool(leg.get("required")),
        "scope_mode": maybe_text(leg.get("scope_mode")),
        "region_hint": maybe_text(leg.get("region_hint")),
        "metric_families": unique_strings(leg.get("metric_families", []) if isinstance(leg.get("metric_families"), list) else []),
        "claim_types": unique_strings(leg.get("claim_types", []) if isinstance(leg.get("claim_types"), list) else []),
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
                query_cues.extend(leg.get("query_cues", []))
            else:
                secondary_legs.append(leg)
        focused_hypotheses.append(
            {
                "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
                "statement": maybe_text(hypothesis.get("statement")),
                "summary": maybe_text(hypothesis.get("summary")),
                "priority_legs": priority_legs,
                "secondary_legs": secondary_legs,
            }
        )
    return {
        "role": role,
        "profile_id": maybe_text(plan.get("profile_id")),
        "profile_summary": maybe_text(plan.get("profile_summary")),
        "priority_metric_families": unique_strings(priority_metric_families),
        "priority_claim_types": unique_strings(priority_claim_types),
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
    for index, statement in enumerate(hypotheses, start=1):
        hypothesis_id = f"hypothesis-{index:03d}"
        legs = []
        for leg in profile["legs"]:
            leg_id = maybe_text(leg.get("leg_id"))
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
                    "preferred_roles": unique_strings(leg.get("preferred_roles", []) if isinstance(leg.get("preferred_roles"), list) else []),
                    "query_cues": unique_strings(leg.get("query_cues", []) if isinstance(leg.get("query_cues"), list) else []),
                    "evidence_focus": unique_strings(leg.get("evidence_focus", []) if isinstance(leg.get("evidence_focus"), list) else []),
                    "success_criteria": maybe_text(leg.get("success_criteria")),
                    "notes": "",
                }
            )
        plan_hypotheses.append(
            {
                "hypothesis_id": hypothesis_id,
                "statement": statement,
                "summary": truncate_text(statement, 180),
                "profile_id": profile_id,
                "chain_legs": legs,
                "investigation_status": "pending",
            }
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
        "open_questions": [],
        "notes": [
            "This plan is scaffolded before any role has curated evidence. Later rounds should refine source regions, corridors, and leg-specific success criteria.",
            "Cross-region investigation is allowed when profile legs mark scope_mode=derived-region.",
        ],
    }

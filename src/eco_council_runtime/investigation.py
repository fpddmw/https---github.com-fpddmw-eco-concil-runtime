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

INVESTIGATION_PROFILES: dict[str, dict[str, Any]] = {
    "local-event": {
        "profile_id": "local-event",
        "summary": "Local-event verification around one mission region and window.",
        "legs": [
            {"leg_id": "impact", "label": "Mission-region physical impact", "required": True},
            {"leg_id": "public_interpretation", "label": "Mission-region public interpretation", "required": False},
        ],
    },
    "smoke-transport": {
        "profile_id": "smoke-transport",
        "summary": "Cross-region smoke attribution from source activity through transport into receptor impacts.",
        "legs": [
            {"leg_id": "source", "label": "Source-region fire activity", "required": True},
            {"leg_id": "mechanism", "label": "Transport or propagation mechanism", "required": True},
            {"leg_id": "impact", "label": "Receptor-region air-quality impact", "required": True},
            {"leg_id": "public_interpretation", "label": "Public attribution and severity narratives", "required": False},
        ],
    },
    "flood-upstream": {
        "profile_id": "flood-upstream",
        "summary": "Upstream-to-downstream flood verification with hydrometeorological causal links.",
        "legs": [
            {"leg_id": "source", "label": "Upstream precipitation or source pressure", "required": True},
            {"leg_id": "mechanism", "label": "Hydrologic transfer or propagation mechanism", "required": True},
            {"leg_id": "impact", "label": "Receptor flood impact", "required": True},
            {"leg_id": "public_interpretation", "label": "Public flood framing and response", "required": False},
        ],
    },
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def truncate_text(value: str, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


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
                    "scope_mode": "mission" if leg_id in {"impact", "public_interpretation"} else "derived-region",
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

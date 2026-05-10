from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_SELECTION_ROLES = ("social-investigator", "environmental-investigator")
SUPPORTED_ARTIFACT_CAPTURE_MODES = ("stdout-json", "stdout-text", "direct-file")
KNOWN_FETCH_SIDE_EFFECTS = (
    "reads-artifacts",
    "writes-artifacts",
    "reads-shared-state",
    "writes-shared-state",
    "network-external",
    "destructive-write",
)

MISSION_INPUT_SEMANTICS: dict[str, Any] = {
    "schema_version": "mission-input-semantics-v1",
    "meaning": (
        "A mission is a user-facing request envelope for starting a council run. "
        "It is not the moderator's investigation plan, not an evidence bundle, "
        "not a report basis, and not a factual attribution."
    ),
    "required_fields": ["schema_version", "run_id", "topic", "objective"],
    "request_text_semantics": (
        "request_text preserves the user's natural-language request when present; "
        "objective may mirror it for legacy compatibility."
    ),
    "optional_seed_fields": [
        "window",
        "region",
        "artifact_imports",
        "source_requests",
        "hypotheses",
        "source_governance",
    ],
    "seed_field_boundary": (
        "Optional seed fields are user/operator-provided starting context only. "
        "They do not narrow investigator autonomy or decide evidence acceptance."
    ),
    "scoping_rule": (
        "If the mission lacks a complete window and region, runtime keeps the run "
        "in scoping mode. Moderator and agents must submit investigation-plan, "
        "investigation-scope, round-brief, or evidence-request objects before "
        "evidence collection is treated as scoped."
    ),
}


def mission_input_semantics() -> dict[str, Any]:
    return json.loads(json.dumps(MISSION_INPUT_SEMANTICS, ensure_ascii=True))


def _source(
    *,
    role: str,
    family_id: str,
    family_label: str,
    layer_id: str,
    layer_label: str,
    tier: str,
    normalizer_skill: str,
    default_suffix: str = ".json",
    artifact_capture: str = "stdout-json",
    runtime_output_mode: str = "none",
    runtime_output_arg: str = "",
    runtime_default_args: list[str] | None = None,
    requires_anchor: bool = False,
    anchor_argument: str = "",
    anchor_source_skills: list[str] | None = None,
    auto_selectable: bool | None = None,
) -> dict[str, Any]:
    return {
        "role": role,
        "family_id": family_id,
        "family_label": family_label,
        "layer_id": layer_id,
        "layer_label": layer_label,
        "tier": tier,
        "normalizer_skill": normalizer_skill,
        "default_suffix": default_suffix,
        "artifact_capture": artifact_capture,
        "runtime_output_mode": runtime_output_mode,
        "runtime_output_arg": runtime_output_arg,
        "runtime_default_args": list(runtime_default_args or []),
        "requires_anchor": requires_anchor,
        "anchor_argument": anchor_argument,
        "anchor_source_skills": list(anchor_source_skills or []),
        "auto_selectable": bool(auto_selectable) if auto_selectable is not None else tier == "l1",
    }


SOURCE_CATALOG: dict[str, dict[str, Any]] = {
    "fetch-bluesky-cascade": _source(
        role="social-investigator",
        family_id="bluesky",
        family_label="Bluesky",
        layer_id="posts",
        layer_label="Posts",
        tier="l1",
        normalizer_skill="normalize-bluesky-cascade-public-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-gdelt-doc-search": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="doc-search",
        layer_label="Doc Search",
        tier="l1",
        normalizer_skill="normalize-gdelt-doc-public-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-gdelt-events": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="events",
        layer_label="Events Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-events-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-gdelt-mentions": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="mentions",
        layer_label="Mentions Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-mentions-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-gdelt-gkg": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="gkg",
        layer_label="GKG Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-gkg-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-youtube-video-search": _source(
        role="social-investigator",
        family_id="youtube",
        family_label="YouTube",
        layer_id="video-search",
        layer_label="Video Search",
        tier="l1",
        normalizer_skill="normalize-youtube-video-public-signals",
        runtime_default_args=["--include-records", "--no-save-records"],
    ),
    "fetch-youtube-comments": _source(
        role="social-investigator",
        family_id="youtube",
        family_label="YouTube",
        layer_id="comments",
        layer_label="Comments",
        tier="l2",
        normalizer_skill="normalize-youtube-comments-public-signals",
        runtime_default_args=["--include-records", "--no-save-records"],
        requires_anchor=True,
        anchor_argument="--video-ids-file",
        anchor_source_skills=["fetch-youtube-video-search"],
        auto_selectable=False,
    ),
    "fetch-regulationsgov-comments": _source(
        role="social-investigator",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="comments",
        layer_label="Comment List",
        tier="l1",
        normalizer_skill="normalize-regulationsgov-comments-public-signals",
        runtime_default_args=["--include-records", "--no-save-response"],
    ),
    "fetch-regulationsgov-comment-detail": _source(
        role="social-investigator",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="comment-detail",
        layer_label="Comment Detail",
        tier="l2",
        normalizer_skill="normalize-regulationsgov-comment-detail-public-signals",
        runtime_default_args=["--include-records", "--no-save-response"],
        requires_anchor=True,
        anchor_argument="--comment-ids-file",
        anchor_source_skills=["fetch-regulationsgov-comments"],
        auto_selectable=False,
    ),
    "fetch-airnow-hourly-observations": _source(
        role="environmental-investigator",
        family_id="airnow",
        family_label="AirNow",
        layer_id="hourly-observations",
        layer_label="Hourly Observations",
        tier="l1",
        normalizer_skill="normalize-airnow-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-openaq": _source(
        role="environmental-investigator",
        family_id="openaq",
        family_label="OpenAQ",
        layer_id="stations",
        layer_label="Stations",
        tier="l1",
        normalizer_skill="normalize-openaq-observation-signals",
    ),
    "fetch-open-meteo-historical": _source(
        role="environmental-investigator",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="historical",
        layer_label="Historical Weather",
        tier="l1",
        normalizer_skill="normalize-open-meteo-historical-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-open-meteo-air-quality": _source(
        role="environmental-investigator",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="air-quality",
        layer_label="Air Quality",
        tier="l1",
        normalizer_skill="normalize-open-meteo-air-quality-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-open-meteo-flood": _source(
        role="environmental-investigator",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="flood",
        layer_label="Flood",
        tier="l1",
        normalizer_skill="normalize-open-meteo-flood-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-usgs-water-iv": _source(
        role="environmental-investigator",
        family_id="usgs-water",
        family_label="USGS Water",
        layer_id="instantaneous-values",
        layer_label="Instantaneous Values",
        tier="l1",
        normalizer_skill="normalize-usgs-water-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-nasa-firms-fire": _source(
        role="environmental-investigator",
        family_id="nasa-firms",
        family_label="NASA FIRMS",
        layer_id="active-fire",
        layer_label="Active Fire",
        tier="l1",
        normalizer_skill="normalize-nasa-firms-fire-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
}

SMOKE_SOURCE_INTENT_TOKENS = (
    "wildfire",
    "wild fire",
    "smoke episode",
    "smoke transport",
    "plume",
    "haze",
)
SOURCE_ORIGIN_INTENT_TOKENS = (
    "source region",
    "origin",
    "source attribution",
)
TRANSPORT_INTENT_TOKENS = (
    "transport",
    "pathway",
    "trajectory",
    "spatiotemporal",
    "source attribution",
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y", "on"}
    return bool(value)


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return payload


def read_json_list(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError(f"Expected a JSON list of objects at {path}")
    return payload


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_snapshot(path: Path) -> dict[str, str]:
    return {
        "path": str(path.resolve()),
        "sha256": file_sha256(path),
    }


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return resolve_run_dir(run_dir) / "runtime" / f"source_selection_{role}_{round_id}.json"


def source_config(source_skill: str) -> dict[str, Any]:
    config = SOURCE_CATALOG.get(maybe_text(source_skill))
    if config is None:
        raise ValueError(f"Unsupported source_skill: {source_skill}")
    return config


def source_role(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("role"))


def source_normalizer_skill(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("normalizer_skill"))


def source_artifact_capture(source_skill: str) -> str:
    return normalize_artifact_capture(source_config(source_skill).get("artifact_capture"))


def source_runtime_output_mode(source_skill: str) -> str:
    mode = maybe_text(source_config(source_skill).get("runtime_output_mode")) or "none"
    if mode not in {"none", "file", "dir"}:
        raise ValueError(f"Unsupported runtime_output_mode for {source_skill}: {mode}")
    return mode


def source_runtime_output_arg(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("runtime_output_arg"))


def source_runtime_default_args(source_skill: str) -> list[str]:
    values = source_config(source_skill).get("runtime_default_args")
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_requires_anchor(source_skill: str) -> bool:
    return coerce_bool(source_config(source_skill).get("requires_anchor"))


def source_anchor_source_skills(source_skill: str) -> list[str]:
    values = source_config(source_skill).get("anchor_source_skills")
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_anchor_argument(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("anchor_argument"))


def source_auto_selectable(source_skill: str) -> bool:
    return coerce_bool(source_config(source_skill).get("auto_selectable"))


def mission_intent_text(mission: dict[str, Any]) -> str:
    parts: list[str] = [
        maybe_text(mission.get("topic")),
        maybe_text(mission.get("objective")),
    ]
    for item in mission.get("hypotheses", []) if isinstance(mission.get("hypotheses"), list) else []:
        if isinstance(item, dict):
            parts.extend(
                [
                    maybe_text(item.get("title")),
                    maybe_text(item.get("statement")),
                    maybe_text(item.get("hypothesis")),
                ]
            )
        else:
            parts.append(maybe_text(item))
    return " ".join(part for part in parts if part).casefold()


def mission_requires_scoping(mission: dict[str, Any]) -> bool:
    status = mission.get("mission_scope_status")
    if not isinstance(status, dict):
        return False
    value = status.get("scoping_required")
    if isinstance(value, bool):
        return value
    return maybe_text(value).casefold() in {"1", "true", "yes"}


def derive_evidence_lanes(mission: dict[str, Any]) -> list[dict[str, str]]:
    text = mission_intent_text(mission)
    lanes: list[dict[str, str]] = []

    def add(lane_id: str, role: str, requirement_type: str, summary: str, priority: str = "high") -> None:
        if any(item["lane_id"] == lane_id for item in lanes):
            return
        lanes.append(
            {
                "lane_id": lane_id,
                "role": role,
                "requirement_type": requirement_type,
                "summary": summary,
                "priority": priority,
            }
        )

    smoke_context = "smoke" in text or any(token in text for token in SMOKE_SOURCE_INTENT_TOKENS)
    smoke_source_intent = any(token in text for token in SMOKE_SOURCE_INTENT_TOKENS) or (
        smoke_context and any(token in text for token in SOURCE_ORIGIN_INTENT_TOKENS)
    )
    transport_intent = any(token in text for token in TRANSPORT_INTENT_TOKENS)
    if smoke_source_intent:
        add(
            "receptor-air-quality",
            "environmental-investigator",
            "receptor-air-quality",
            "Record local receptor air-quality anomaly evidence as a candidate review lane.",
        )
        add(
            "fire-origin",
            "environmental-investigator",
            "fire-origin-candidate",
            "Record active-fire evidence sources for candidate wildfire source-region review.",
        )
        add(
            "public-discourse",
            "social-investigator",
            "public-discourse-signal",
            "Collect public/reporting signals about the smoke episode and affected communities.",
        )
    if smoke_source_intent or transport_intent:
        add(
            "local-weather-context",
            "environmental-investigator",
            "weather-transport-context",
            "Record local weather context for later council or agent transport review.",
        )
        add(
            "spatiotemporal-relation-review",
            "environmental-investigator",
            "spatiotemporal-relation-review",
            "Record a spatiotemporal relation review lane when source or transport questions are in scope.",
        )
    if any(token in text for token in ("health", "asthma", "community", "impact", "public health")):
        add(
            "community-impact",
            "social-investigator",
            "community-impact-signal",
            "Record public/community impact signals as a separate evidence lane.",
            priority="medium",
        )
    if any(token in text for token in ("recommendation", "response", "handling", "处理", "建议")):
        add(
            "response-recommendation-boundary",
            "social-investigator",
            "response-record-signal",
            "Record response or recommendation evidence when handling recommendations are in scope.",
            priority="medium",
        )
    return lanes


def derive_verification_scope(mission: dict[str, Any]) -> dict[str, Any]:
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    geometry = region.get("geometry") if isinstance(region.get("geometry"), dict) else {}
    lanes = derive_evidence_lanes(mission)
    lane_ids = {maybe_text(lane.get("lane_id")) for lane in lanes}
    source_required = "fire-origin" in lane_ids
    transport_required = "spatiotemporal-relation-review" in lane_ids
    required_source_skills: list[str] = []
    candidate_source_skills: list[str] = []
    source_selections = (
        mission.get("source_selections")
        if isinstance(mission.get("source_selections"), dict)
        else {}
    )
    explicit_selected_sources: list[str] = []
    for selection in source_selections.values():
        if isinstance(selection, dict):
            explicit_selected_sources.extend(list_items(selection.get("selected_sources")))
    if explicit_selected_sources:
        required_source_skills.extend(unique_texts(explicit_selected_sources))
    for role in SOURCE_SELECTION_ROLES:
        candidate_source_skills.extend(intent_selected_sources(mission, role))
    return {
        "scope_id": "verification-scope-"
        + stable_hash(
            mission.get("run_id"),
            mission.get("topic"),
            mission.get("objective"),
            window.get("start_utc"),
            window.get("end_utc"),
            region.get("label"),
        )[:12],
        "receptor_region": {
            "label": maybe_text(region.get("label")),
            "geometry": geometry,
        },
        "study_window": {
            "start_utc": maybe_text(window.get("start_utc")),
            "end_utc": maybe_text(window.get("end_utc")),
        },
        "required_evidence_lanes": lanes,
        "candidate_source_region_policy": (
            "mission-derived-candidate-source-review" if source_required else "not-applicable"
        ),
        "transport_verification_policy": (
            "mission-derived-relation-review" if transport_required else "not-applicable"
        ),
        "lag_window": {
            "mode": "mission-derived",
            "minimum_hours": 0,
            "maximum_hours": 72 if source_required or transport_required else 0,
        },
        "required_source_skills": unique_texts(required_source_skills),
        "candidate_source_skills": unique_texts(candidate_source_skills),
    }


def intent_selected_sources(mission: dict[str, Any], role: str) -> list[str]:
    if mission_requires_scoping(mission):
        return []
    lanes = [lane for lane in derive_evidence_lanes(mission) if lane.get("role") == role]
    lane_ids = {maybe_text(lane.get("lane_id")) for lane in lanes}
    values: list[str] = []
    if role == "environmental-investigator":
        if "receptor-air-quality" in lane_ids:
            values.append("fetch-open-meteo-air-quality")
        if "local-weather-context" in lane_ids or "spatiotemporal-relation-review" in lane_ids:
            values.append("fetch-open-meteo-historical")
        if "fire-origin" in lane_ids:
            values.append("fetch-nasa-firms-fire")
    if role == "social-investigator":
        if lane_ids & {"public-discourse", "community-impact", "response-recommendation-boundary"}:
            values.append("fetch-gdelt-doc-search")
    allowed_lookup = {item.casefold() for item in allowed_sources_for_role(mission, role)}
    return unique_texts([value for value in values if value.casefold() in allowed_lookup])


def lane_evidence_requirements(mission: dict[str, Any], *, round_id: str, role: str) -> list[dict[str, str]]:
    requirements: list[dict[str, str]] = []
    for lane in derive_evidence_lanes(mission):
        if maybe_text(lane.get("role")) != role:
            continue
        lane_id = maybe_text(lane.get("lane_id"))
        if not lane_id:
            continue
        requirements.append(
            {
                "requirement_id": f"req-{role}-{round_id}-{lane_id}",
                "requirement_type": maybe_text(lane.get("requirement_type")),
                "summary": maybe_text(lane.get("summary")),
                "priority": maybe_text(lane.get("priority")) or "high",
                "evidence_lane": lane_id,
            }
        )
    return requirements


def normalize_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return unique_texts([maybe_text(value) for value in values if maybe_text(value)])


def normalize_artifact_capture(value: Any) -> str:
    capture_mode = maybe_text(value) or "stdout-json"
    if capture_mode not in SUPPORTED_ARTIFACT_CAPTURE_MODES:
        raise ValueError(f"Unsupported artifact_capture: {capture_mode}")
    return capture_mode


def normalize_fetch_execution_policy(payload: dict[str, Any]) -> dict[str, Any]:
    execution_policy = payload.get("fetch_execution_policy") if isinstance(payload.get("fetch_execution_policy"), dict) else {}
    timeout_seconds = coerce_float(execution_policy.get("timeout_seconds"))
    if timeout_seconds is None:
        timeout_seconds = coerce_float(payload.get("timeout_seconds"))
    retry_budget = coerce_int(execution_policy.get("retry_budget"))
    if retry_budget is None:
        retry_budget = coerce_int(payload.get("retry_budget"))
    retry_backoff_ms = coerce_int(execution_policy.get("retry_backoff_ms"))
    if retry_backoff_ms is None:
        retry_backoff_ms = coerce_int(payload.get("retry_backoff_ms"))
    return {
        "timeout_seconds": max(0.0, float(timeout_seconds if timeout_seconds is not None else 300.0)),
        "retry_budget": max(0, int(retry_budget if retry_budget is not None else 0)),
        "retry_backoff_ms": max(0, int(retry_backoff_ms if retry_backoff_ms is not None else 250)),
    }


def validate_fetch_side_effects(values: list[str], *, field_name: str) -> list[str]:
    invalid = [value for value in values if value not in KNOWN_FETCH_SIDE_EFFECTS]
    if invalid:
        raise ValueError(f"Unsupported fetch side effects in {field_name}: {', '.join(invalid)}")
    return unique_texts(values)


def normalize_fetch_declared_side_effects(payload: dict[str, Any]) -> list[str]:
    declared = normalize_text_list(payload.get("declared_side_effects"))
    validated = validate_fetch_side_effects(declared, field_name="declared_side_effects")
    return unique_texts(["writes-artifacts", *validated])


def normalize_fetch_requested_side_effect_approvals(payload: dict[str, Any], declared_side_effects: list[str]) -> list[str]:
    requested = validate_fetch_side_effects(
        normalize_text_list(payload.get("requested_side_effect_approvals")),
        field_name="requested_side_effect_approvals",
    )
    undeclared = [value for value in requested if value not in declared_side_effects]
    if undeclared:
        raise ValueError(
            "requested_side_effect_approvals must be a subset of declared_side_effects: "
            + ", ".join(undeclared)
        )
    return requested


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    base = [skill_name for skill_name, config in SOURCE_CATALOG.items() if maybe_text(config.get("role")) == role]
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    mission_allowlist = mission.get("allowed_sources_by_role") if isinstance(mission.get("allowed_sources_by_role"), dict) else {}
    configured = mission_allowlist.get(role)
    if configured is None and isinstance(governance.get("allowed_sources_by_role"), dict):
        configured = governance["allowed_sources_by_role"].get(role)
    if isinstance(configured, list):
        requested = {maybe_text(item) for item in configured if maybe_text(item)}
        return [skill_name for skill_name in base if skill_name in requested]
    return base


def effective_constraints(mission: dict[str, Any]) -> dict[str, int]:
    defaults = {
        "max_selected_sources_per_role": 4,
        "max_source_steps_per_round": 8,
    }
    constraints = mission.get("constraints") if isinstance(mission.get("constraints"), dict) else {}
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    for key in tuple(defaults):
        value = governance.get(key)
        if value in (None, ""):
            value = constraints.get(key)
        coerced = coerce_int(value)
        if coerced is not None:
            defaults[key] = coerced
    return defaults


def role_source_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    approved_layers_payload = governance.get("approved_layers") if isinstance(governance.get("approved_layers"), list) else []
    families: dict[str, dict[str, Any]] = {}
    for source_skill, config in SOURCE_CATALOG.items():
        if maybe_text(config.get("role")) != role:
            continue
        family_id = maybe_text(config.get("family_id"))
        family = families.setdefault(
            family_id,
            {
                "family_id": family_id,
                "label": maybe_text(config.get("family_label")),
                "role": role,
                "skills": [],
                "_layers": {},
            },
        )
        family["skills"].append(source_skill)
        layer_id = maybe_text(config.get("layer_id"))
        tier = maybe_text(config.get("tier")) or "l1"
        layer_lookup = family.setdefault("_layers", {})
        if not isinstance(layer_lookup, dict):
            layer_lookup = {}
            family["_layers"] = layer_lookup
        layer = layer_lookup.setdefault(
            layer_id,
            {
                "layer_id": layer_id,
                "label": maybe_text(config.get("layer_label")),
                "tier": tier,
                "skills": [],
                "max_selected_skills": 0,
                "requires_anchor": coerce_bool(config.get("requires_anchor")),
                "anchor_source_skills": [],
                "auto_selectable": coerce_bool(config.get("auto_selectable")) if "auto_selectable" in config else tier == "l1",
            },
        )
        if isinstance(layer, dict):
            layer_skills = layer.setdefault("skills", [])
            if isinstance(layer_skills, list):
                layer_skills.append(source_skill)
            anchor_skills = layer.setdefault("anchor_source_skills", [])
            if isinstance(anchor_skills, list):
                anchor_skills.extend(source_anchor_source_skills(source_skill))
    for family in families.values():
        family["skills"] = unique_texts(family.get("skills", []))
        layer_lookup = family.pop("_layers", {})
        layers = layer_lookup.values() if isinstance(layer_lookup, dict) else []
        finalized_layers: list[dict[str, Any]] = []
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            layer["skills"] = unique_texts(layer.get("skills", []))
            layer["anchor_source_skills"] = unique_texts(layer.get("anchor_source_skills", []))
            layer["max_selected_skills"] = len(layer["skills"])
            finalized_layers.append(layer)
        family["layers"] = sorted(
            finalized_layers,
            key=lambda item: (0 if maybe_text(item.get("tier")) == "l1" else 1, maybe_text(item.get("layer_id"))),
        )
    family_ids = {maybe_text(item.get("family_id")) for item in families.values() if maybe_text(item.get("family_id"))}
    approved_layers = [
        item
        for item in approved_layers_payload
        if isinstance(item, dict)
        and maybe_text(item.get("family_id")) in family_ids
        and maybe_text(item.get("layer_id"))
    ]
    return {
        "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
        "allow_cross_round_anchors": coerce_bool(governance.get("allow_cross_round_anchors")),
        "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
        "max_active_families_per_role": coerce_int(governance.get("max_active_families_per_role")),
        "max_non_entry_layers_per_role": coerce_int(governance.get("max_non_entry_layers_per_role")),
        "approved_layers": approved_layers,
        "families": sorted(families.values(), key=lambda item: maybe_text(item.get("family_id"))),
    }


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    return {
        "policy_profile": maybe_text(mission.get("policy_profile")) or "standard",
        "effective_constraints": effective_constraints(mission),
        "source_governance": {
            "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
            "allow_cross_round_anchors": coerce_bool(governance.get("allow_cross_round_anchors")),
            "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
        },
    }


def normalize_artifact_imports(mission: dict[str, Any]) -> list[dict[str, Any]]:
    imports = mission.get("artifact_imports") if isinstance(mission.get("artifact_imports"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in imports:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        config = source_config(source_skill)
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "artifact_path": maybe_text(item.get("artifact_path")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


def normalize_source_requests(mission: dict[str, Any]) -> list[dict[str, Any]]:
    requests = mission.get("source_requests") if isinstance(mission.get("source_requests"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in requests:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        config = source_config(source_skill)
        fetch_argv = item.get("fetch_argv") if isinstance(item.get("fetch_argv"), list) else []
        declared_side_effects = normalize_fetch_declared_side_effects(item)
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "artifact_capture": normalize_artifact_capture(item.get("artifact_capture") or config.get("artifact_capture")),
                "artifact_path": maybe_text(item.get("artifact_path")),
                "fetch_cwd": maybe_text(item.get("fetch_cwd")),
                "fetch_argv": [maybe_text(arg) for arg in fetch_argv if maybe_text(arg)],
                "fetch_execution_policy": normalize_fetch_execution_policy(item),
                "declared_side_effects": declared_side_effects,
                "requested_side_effect_approvals": normalize_fetch_requested_side_effect_approvals(item, declared_side_effects),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


__all__ = [
    "coerce_bool",
    "coerce_float",
    "coerce_int",
    "KNOWN_FETCH_SIDE_EFFECTS",
    "SOURCE_CATALOG",
    "SOURCE_SELECTION_ROLES",
    "SUPPORTED_ARTIFACT_CAPTURE_MODES",
    "allowed_sources_for_role",
    "derive_evidence_lanes",
    "derive_verification_scope",
    "effective_constraints",
    "file_sha256",
    "file_snapshot",
    "maybe_text",
    "normalize_artifact_capture",
    "normalize_artifact_imports",
    "normalize_fetch_execution_policy",
    "normalize_fetch_declared_side_effects",
    "normalize_fetch_requested_side_effect_approvals",
    "normalize_source_requests",
    "normalize_text_list",
    "intent_selected_sources",
    "lane_evidence_requirements",
    "mission_intent_text",
    "mission_requires_scoping",
    "policy_profile_summary",
    "read_json_list",
    "read_json_object",
    "resolve_run_dir",
    "role_source_governance",
    "source_anchor_argument",
    "source_anchor_source_skills",
    "source_artifact_capture",
    "source_auto_selectable",
    "source_config",
    "source_normalizer_skill",
    "source_role",
    "source_requires_anchor",
    "source_runtime_default_args",
    "source_runtime_output_arg",
    "source_runtime_output_mode",
    "source_selection_path",
    "stable_hash",
    "unique_texts",
    "utc_now_iso",
    "write_json_file",
]

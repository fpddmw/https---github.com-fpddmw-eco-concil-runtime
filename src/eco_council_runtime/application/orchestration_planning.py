"""Fetch-plan planning helpers for orchestration workflows."""

from __future__ import annotations

import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import file_snapshot, utc_now_iso
from eco_council_runtime.adapters.run_paths import load_mission, round_dir
from eco_council_runtime.application import orchestration_prepare
from eco_council_runtime.cli_invocation import runtime_module_argv, runtime_module_command
from eco_council_runtime.controller.paths import source_selection_path
from eco_council_runtime.controller.policy import (
    allowed_sources_for_role,
    effective_constraints,
    load_override_requests,
    policy_profile_summary,
    role_evidence_requirements,
    role_source_governance,
)
from eco_council_runtime.domain.contract_bridge import contract_call
from eco_council_runtime.domain.text import maybe_text, normalize_space, text_truthy, unique_strings as shared_unique_strings
from eco_council_runtime.external_skills import (
    default_env_file as detached_default_env_file,
    fetch_script_path,
    skill_dir,
)
from eco_council_runtime.layout import PROJECT_DIR

PUBLIC_SOURCES = (
    "gdelt-doc-search",
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
    "bluesky-cascade-fetch",
    "youtube-video-search",
    "youtube-comments-fetch",
    "federal-register-doc-fetch",
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
)
GDELT_RAW_TABLE_SOURCES = {
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
}
GDELT_EXPECTED_COLUMNS = {
    "gdelt-events-fetch": "61",
    "gdelt-mentions-fetch": "16",
    "gdelt-gkg-fetch": "27",
}
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
GDELT_MAX_FILE_INPUT_KEYS = {
    "gdelt-events-fetch": "gdelt_events_max_files",
    "gdelt-mentions-fetch": "gdelt_mentions_max_files",
    "gdelt-gkg-fetch": "gdelt_gkg_max_files",
}
GDELT_PREVIEW_LINE_INPUT_KEYS = {
    "gdelt-events-fetch": "gdelt_events_preview_lines",
    "gdelt-mentions-fetch": "gdelt_mentions_preview_lines",
    "gdelt-gkg-fetch": "gdelt_gkg_preview_lines",
}
ENVIRONMENT_SOURCES = (
    "airnow-hourly-obs-fetch",
    "usgs-water-iv-fetch",
    "open-meteo-air-quality-fetch",
    "open-meteo-historical-fetch",
    "open-meteo-flood-fetch",
    "nasa-firms-fire-fetch",
    "openaq-data-fetch",
)
SUPPORTED_SOURCES_BY_ROLE = {
    "sociologist": list(PUBLIC_SOURCES),
    "environmentalist": list(ENVIRONMENT_SOURCES),
}
DEFAULT_OPEN_METEO_AIR_VARS = [
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "ozone",
    "us_aqi",
]
DEFAULT_AIRNOW_PARAMETER_NAMES = [
    "PM25",
    "PM10",
    "OZONE",
    "NO2",
]
DEFAULT_AIRNOW_POINT_PADDING_DEG = 0.25
DEFAULT_USGS_PARAMETER_CODES = [
    "00060",
    "00065",
]
DEFAULT_USGS_POINT_PADDING_DEG = 0.25
DEFAULT_USGS_SITE_TYPE = "ST"
DEFAULT_USGS_SITE_STATUS = "active"
DEFAULT_OPEN_METEO_HIST_HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "soil_moisture_0_to_7cm",
]
DEFAULT_OPEN_METEO_HIST_DAILY_VARS = [
    "precipitation_sum",
    "et0_fao_evapotranspiration",
]
DEFAULT_OPEN_METEO_FLOOD_DAILY_VARS = [
    "river_discharge",
    "river_discharge_p75",
]
DEFAULT_OPENAQ_PARAMETER_NAMES = [
    "pm25",
    "pm2.5",
    "pm10",
    "o3",
    "no2",
]

REPO_DIR = PROJECT_DIR
ensure_object = orchestration_prepare.ensure_object
load_tasks = orchestration_prepare.load_tasks
load_source_selection = orchestration_prepare.load_source_selection
tasks_for_role = orchestration_prepare.tasks_for_role
mission_window = orchestration_prepare.mission_window
mission_region = orchestration_prepare.mission_region


def unique_strings(values: list[str]) -> list[str]:
    return shared_unique_strings(values, casefold=True)


def parse_utc_datetime(value: str) -> datetime:
    text = value.strip()
    if not text:
        raise ValueError("Expected a non-empty UTC datetime string.")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        result = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"Invalid UTC datetime: {value!r}") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def to_date_text(value: str) -> str:
    return parse_utc_datetime(value).date().isoformat()


def to_gdelt_datetime(value: str) -> str:
    return parse_utc_datetime(value).strftime("%Y%m%d%H%M%S")


def firms_source_for_window(window: dict[str, Any], requested_source: str) -> str:
    source = (requested_source or "VIIRS_NOAA20_NRT").strip()
    if not source.endswith("_NRT"):
        return source
    end_text = maybe_text(window.get("end_utc"))
    if not end_text:
        return source
    try:
        end_dt = parse_utc_datetime(end_text)
    except ValueError:
        return source
    age_days = (datetime.now(timezone.utc) - end_dt).days
    if age_days <= 30:
        return source
    archival_source = source.removesuffix("_NRT") + "_SP"
    known_archival = {
        "MODIS_SP",
        "VIIRS_NOAA20_SP",
        "VIIRS_SNPP_SP",
    }
    return archival_source if archival_source in known_archival else source


def role_raw_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "raw"


def role_meta_dir(run_dir: Path, round_id: str, role: str) -> Path:
    return role_raw_dir(run_dir, round_id, role) / "_meta"


def default_raw_artifact_path(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    extension = ".json"
    if source_skill in {
        "youtube-video-search",
        "youtube-comments-fetch",
        "regulationsgov-comments-fetch",
        "regulationsgov-comment-detail-fetch",
    }:
        extension = ".jsonl"
    return role_raw_dir(run_dir, round_id, role) / f"{source_skill}{extension}"


def default_raw_download_dir(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return role_raw_dir(run_dir, round_id, role) / source_skill


def default_raw_quarantine_dir(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return default_raw_download_dir(run_dir, round_id, role, source_skill) / "quarantine"


def default_step_stdout_path(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return role_meta_dir(run_dir, round_id, role) / f"{source_skill}.stdout.json"


def default_step_stderr_path(run_dir: Path, round_id: str, role: str, source_skill: str) -> Path:
    return role_meta_dir(run_dir, round_id, role) / f"{source_skill}.stderr.log"


def fetch_plan_input_snapshot(
    *,
    run_dir: Path,
    round_id: str,
    sociologist_selection: dict[str, Any] | None,
    environmentalist_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    tasks_file = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    sociologist_path = source_selection_path(run_dir, round_id, "sociologist")
    environmentalist_path = source_selection_path(run_dir, round_id, "environmentalist")
    return {
        "tasks": file_snapshot(tasks_file),
        "source_selections": {
            "sociologist": {
                **file_snapshot(sociologist_path),
                "status": maybe_text((sociologist_selection or {}).get("status")),
            },
            "environmentalist": {
                **file_snapshot(environmentalist_path),
                "status": maybe_text((environmentalist_selection or {}).get("status")),
            },
        },
    }


def ensure_fetch_plan_inputs_match(*, run_dir: Path, round_id: str, plan: dict[str, Any]) -> None:
    snapshot = ensure_object(plan.get("input_snapshot"), "fetch_plan.input_snapshot")
    task_snapshot = ensure_object(snapshot.get("tasks"), "fetch_plan.input_snapshot.tasks")
    task_path = round_dir(run_dir, round_id) / "moderator" / "tasks.json"
    current_task_snapshot = file_snapshot(task_path)
    issues: list[str] = []
    if maybe_text(task_snapshot.get("sha256")) != maybe_text(current_task_snapshot.get("sha256")):
        issues.append(f"tasks.json changed ({task_path})")

    source_snapshots = ensure_object(snapshot.get("source_selections"), "fetch_plan.input_snapshot.source_selections")
    for role in ("sociologist", "environmentalist"):
        expected = ensure_object(source_snapshots.get(role), f"fetch_plan.input_snapshot.source_selections.{role}")
        path = source_selection_path(run_dir, round_id, role)
        current = file_snapshot(path)
        current_payload = load_source_selection(run_dir, round_id, role)
        current_status = maybe_text((current_payload or {}).get("status"))
        if maybe_text(expected.get("sha256")) != maybe_text(current.get("sha256")):
            issues.append(f"{role} source_selection changed ({path})")
        if maybe_text(expected.get("status")) != current_status:
            issues.append(
                f"{role} source_selection status changed (expected {maybe_text(expected.get('status')) or '<empty>'}, found {current_status or '<empty>'})"
            )
    if issues:
        raise RuntimeError("Fetch plan inputs changed since prepare-round. Rerun prepare-round. " + "; ".join(issues))


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


def role_supported_sources(role: str) -> list[str]:
    return list(SUPPORTED_SOURCES_BY_ROLE.get(role, []))


def source_selection_selected_sources(source_selection: dict[str, Any] | None) -> list[str]:
    if not isinstance(source_selection, dict):
        return []
    if maybe_text(source_selection.get("status")) == "pending":
        return []
    family_selected = contract_call("selected_sources_from_family_plans", source_selection)
    if isinstance(family_selected, list) and family_selected:
        return unique_strings([maybe_text(item) for item in family_selected if maybe_text(item)])
    value = source_selection.get("selected_sources")
    if not isinstance(value, list):
        return []
    return unique_strings([maybe_text(item) for item in value if maybe_text(item)])


def ensure_source_selection_respects_governance(
    *,
    mission: dict[str, Any],
    role: str,
    source_selection: dict[str, Any] | None,
) -> None:
    if not isinstance(source_selection, dict):
        return
    governance = role_source_governance(mission, role)
    families = governance.get("families") if isinstance(governance.get("families"), list) else []
    if not families:
        return
    family_plans = source_selection.get("family_plans")
    if not isinstance(family_plans, list):
        raise ValueError(f"Role {role} source_selection must include family_plans.")

    family_lookup: dict[str, dict[str, Any]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        if family_id:
            family_lookup[family_id] = family
    payload_lookup: dict[str, dict[str, Any]] = {}
    for family_plan in family_plans:
        if not isinstance(family_plan, dict):
            continue
        family_id = maybe_text(family_plan.get("family_id"))
        if family_id:
            payload_lookup[family_id] = family_plan
    if set(payload_lookup) != set(family_lookup):
        missing = sorted(set(family_lookup) - set(payload_lookup))
        extra = sorted(set(payload_lookup) - set(family_lookup))
        raise ValueError(f"Role {role} family_plans must match governed families. Missing={missing}, extra={extra}")

    selected_sources = source_selection_selected_sources(source_selection)
    max_sources = governance.get("max_selected_sources_per_role")
    if isinstance(max_sources, int) and max_sources > 0 and len(selected_sources) > max_sources:
        raise ValueError(
            f"Role {role} selected {len(selected_sources)} sources but governance max_selected_sources_per_role={max_sources}."
        )

    approved_lookup = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id"))): item
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict) and maybe_text(item.get("family_id")) and maybe_text(item.get("layer_id"))
    }
    allow_cross_round_anchors = bool(governance.get("allow_cross_round_anchors"))
    selected_family_count = 0
    selected_non_entry_layers = 0

    for family_id, family_plan in payload_lookup.items():
        family_policy = family_lookup.get(family_id)
        if not isinstance(family_policy, dict):
            continue
        if family_plan.get("selected") is True:
            selected_family_count += 1
        layer_lookup = {
            maybe_text(layer.get("layer_id")): layer
            for layer in family_policy.get("layers", [])
            if isinstance(layer, dict) and maybe_text(layer.get("layer_id"))
        }
        layer_plans = family_plan.get("layer_plans")
        if not isinstance(layer_plans, list):
            raise ValueError(f"Role {role} family {family_id} must include layer_plans.")
        payload_layer_ids = {
            maybe_text(layer_plan.get("layer_id"))
            for layer_plan in layer_plans
            if isinstance(layer_plan, dict) and maybe_text(layer_plan.get("layer_id"))
        }
        if set(layer_lookup) != payload_layer_ids:
            missing = sorted(set(layer_lookup) - payload_layer_ids)
            extra = sorted(payload_layer_ids - set(layer_lookup))
            raise ValueError(f"Role {role} family {family_id} layer_plans mismatch. Missing={missing}, extra={extra}")

        for layer_plan in layer_plans:
            if not isinstance(layer_plan, dict):
                continue
            layer_id = maybe_text(layer_plan.get("layer_id"))
            layer_policy = layer_lookup.get(layer_id)
            if not isinstance(layer_policy, dict):
                continue
            if maybe_text(layer_plan.get("tier")) != maybe_text(layer_policy.get("tier")):
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} tier mismatch.")
            selected = layer_plan.get("selected") is True
            selected_skill_set = {
                maybe_text(skill)
                for skill in layer_plan.get("source_skills", [])
                if maybe_text(skill)
            }
            allowed_skill_set = {
                maybe_text(skill)
                for skill in layer_policy.get("skills", [])
                if maybe_text(skill)
            }
            if not selected_skill_set <= allowed_skill_set:
                invalid = sorted(selected_skill_set - allowed_skill_set)
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} selected invalid skills {invalid}.")
            max_selected_skills = layer_policy.get("max_selected_skills")
            if isinstance(max_selected_skills, int) and max_selected_skills > 0 and len(selected_skill_set) > max_selected_skills:
                raise ValueError(
                    f"Role {role} family {family_id} layer {layer_id} selected {len(selected_skill_set)} skills but max_selected_skills={max_selected_skills}."
                )
            if not selected:
                continue

            tier = maybe_text(layer_policy.get("tier"))
            authorization_basis = maybe_text(layer_plan.get("authorization_basis"))
            anchor_mode = maybe_text(layer_plan.get("anchor_mode"))
            anchor_refs = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
            if tier == "l2":
                selected_non_entry_layers += 1
            if layer_policy.get("requires_anchor") is True and (anchor_mode == "none" or not anchor_refs):
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} requires anchors.")
            if anchor_mode == "prior_round_l1" and not allow_cross_round_anchors:
                raise ValueError(f"Role {role} family {family_id} layer {layer_id} cannot use prior_round_l1 anchors.")
            approval_key = (family_id, layer_id)
            if tier == "l1":
                if authorization_basis != "entry-layer":
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use entry-layer authorization.")
            else:
                auto_selectable = layer_policy.get("auto_selectable") is True
                if approval_key in approved_lookup:
                    if authorization_basis != "upstream-approval":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use upstream-approval.")
                elif auto_selectable:
                    if authorization_basis != "policy-auto":
                        raise ValueError(f"Role {role} family {family_id} layer {layer_id} must use policy-auto authorization.")
                else:
                    raise ValueError(f"Role {role} family {family_id} layer {layer_id} is not approved by governance.")

    max_families = governance.get("max_active_families_per_role")
    if isinstance(max_families, int) and max_families > 0 and selected_family_count > max_families:
        raise ValueError(
            f"Role {role} selected {selected_family_count} families but governance max_active_families_per_role={max_families}."
        )
    max_l2_layers = governance.get("max_non_entry_layers_per_role")
    if isinstance(max_l2_layers, int) and max_l2_layers >= 0 and selected_non_entry_layers > max_l2_layers:
        raise ValueError(
            f"Role {role} selected {selected_non_entry_layers} non-entry layers but governance max_non_entry_layers_per_role={max_l2_layers}."
        )


def role_selected_sources(
    *,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    role: str,
    source_selection: dict[str, Any] | None,
) -> list[str]:
    ensure_source_selection_respects_governance(mission=mission, role=role, source_selection=source_selection)
    allowed = allowed_sources_for_role(mission, role)
    supported = role_supported_sources(role)
    allowed_lookup = {source.casefold() for source in allowed}
    supported_lookup = {source.casefold() for source in supported}
    selected_lookup = {
        source.casefold()
        for source in source_selection_selected_sources(source_selection)
        if source.casefold() in supported_lookup
    }
    if not selected_lookup:
        return []
    if not allowed_lookup:
        selected = sorted(selected_lookup)
        raise ValueError(f"Role {role} selected sources {selected}, but mission.source_governance exposes no allowed sources.")
    invalid = [source for source in supported if source.casefold() in selected_lookup and source.casefold() not in allowed_lookup]
    if invalid:
        raise ValueError(f"Role {role} selected unsupported or disallowed sources: {invalid}.")
    return [source for source in supported if source.casefold() in selected_lookup and source.casefold() in allowed_lookup]


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


def geometry_from_task_or_mission(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    for task in tasks:
        geometry = task_inputs(task).get("mission_geometry")
        if isinstance(geometry, dict):
            return geometry
    return ensure_object(mission_region(mission).get("geometry"), "mission.region.geometry")


def window_from_task_or_mission(*, mission: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, str]:
    for task in tasks:
        window = task_inputs(task).get("mission_window")
        if isinstance(window, dict) and maybe_text(window.get("start_utc")) and maybe_text(window.get("end_utc")):
            return {"start_utc": maybe_text(window.get("start_utc")), "end_utc": maybe_text(window.get("end_utc"))}
    return mission_window(mission)


def center_point_for_geometry(geometry: dict[str, Any]) -> tuple[float, float]:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "Point":
        return float(geometry["latitude"]), float(geometry["longitude"])
    if geometry_type == "BBox":
        west = float(geometry["west"])
        south = float(geometry["south"])
        east = float(geometry["east"])
        north = float(geometry["north"])
        return ((south + north) / 2.0, (west + east) / 2.0)
    raise ValueError(f"Unsupported mission geometry type: {geometry_type!r}")


def location_strings_for_geometry(geometry: dict[str, Any]) -> list[str]:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "Point":
        return [f"{float(geometry['latitude']):.6f},{float(geometry['longitude']):.6f}"]
    if geometry_type == "BBox":
        west = float(geometry["west"])
        south = float(geometry["south"])
        east = float(geometry["east"])
        north = float(geometry["north"])
        center_lat, center_lon = center_point_for_geometry(geometry)
        candidates = [
            f"{center_lat:.6f},{center_lon:.6f}",
            f"{north:.6f},{west:.6f}",
            f"{south:.6f},{east:.6f}",
        ]
        return unique_strings(candidates)
    raise ValueError(f"Unsupported mission geometry type: {geometry_type!r}")


def bbox_text_for_geometry(geometry: dict[str, Any], *, point_padding_deg: float) -> str:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "BBox":
        return ",".join(
            [
                f"{float(geometry['west']):.6f}",
                f"{float(geometry['south']):.6f}",
                f"{float(geometry['east']):.6f}",
                f"{float(geometry['north']):.6f}",
            ]
        )
    if geometry_type != "Point":
        raise ValueError(f"Unsupported mission geometry type: {geometry_type!r}")
    latitude = float(geometry["latitude"])
    longitude = float(geometry["longitude"])
    padding = abs(point_padding_deg)
    south = max(-90.0, latitude - padding)
    north = min(90.0, latitude + padding)
    west = max(-180.0, longitude - padding)
    east = min(180.0, longitude + padding)
    return f"{west:.6f},{south:.6f},{east:.6f},{north:.6f}"


def source_role(source_skill: str) -> str:
    if source_skill in PUBLIC_SOURCES:
        return "sociologist"
    if source_skill in ENVIRONMENT_SOURCES:
        return "environmentalist"
    raise ValueError(f"Unsupported source skill: {source_skill}")


def skill_workdir(skill_name: str) -> Path:
    try:
        return skill_dir(skill_name)
    except FileNotFoundError:
        return REPO_DIR


def default_env_file(skill_name: str) -> Path | None:
    try:
        return detached_default_env_file(skill_name)
    except FileNotFoundError:
        return None


def contract_argv(*args: object) -> list[str]:
    return runtime_module_argv("contract", *args)


def contract_command(*args: object) -> str:
    return runtime_module_command("contract", *args)


def normalize_argv(*args: object) -> list[str]:
    return runtime_module_argv("normalize", *args)


def normalize_command(*args: object) -> str:
    return runtime_module_command("normalize", *args)


def reporting_argv(*args: object) -> list[str]:
    return runtime_module_argv("reporting", *args)


def reporting_command(*args: object) -> str:
    return runtime_module_command("reporting", *args)


def orchestrate_argv(*args: object) -> list[str]:
    return runtime_module_argv("orchestrate", *args)


def orchestrate_command(*args: object) -> str:
    return runtime_module_command("orchestrate", *args)


def shell_join(argv: list[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in argv)


def shell_command(argv: list[str], *, env_file: Path | None = None) -> str:
    lines: list[str] = []
    if env_file is not None:
        lines.extend(
            [
                "set -a",
                f"source {shlex.quote(str(env_file))}",
                "set +a",
            ]
        )
    lines.append(shell_join(argv))
    return "\n".join(lines)


def make_step(
    *,
    step_id: str,
    role: str,
    source_skill: str,
    task_ids: list[str],
    artifact_path: Path,
    stdout_path: Path,
    stderr_path: Path,
    command: str,
    depends_on: list[str],
    notes: list[str],
    skill_refs: list[str],
    cwd: Path,
    artifact_capture: str = "",
    download_dir: Path | None = None,
    quarantine_dir: Path | None = None,
) -> dict[str, Any]:
    step = {
        "step_id": step_id,
        "role": role,
        "source_skill": source_skill,
        "task_ids": task_ids,
        "depends_on": depends_on,
        "artifact_path": str(artifact_path),
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
        "cwd": str(cwd),
        "command": command,
        "notes": notes,
        "skill_refs": skill_refs,
        "normalizer_input": f"{source_skill}={artifact_path}",
    }
    if artifact_capture:
        step["artifact_capture"] = artifact_capture
    if download_dir is not None:
        step["download_dir"] = str(download_dir)
    if quarantine_dir is not None:
        step["quarantine_dir"] = str(quarantine_dir)
    return step


def new_step_id(role: str, source_skill: str, counter: int) -> str:
    return f"step-{role}-{counter:02d}-{source_skill}"


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


def build_sociologist_steps(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    source_selection: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    role = "sociologist"
    role_tasks = tasks_for_role(tasks, role)
    if not role_tasks:
        return []
    selected = role_selected_sources(mission=mission, tasks=role_tasks, role=role, source_selection=source_selection)
    if not selected:
        return []
    task_ids = step_task_ids(role_tasks)
    window = window_from_task_or_mission(mission=mission, tasks=role_tasks)
    query_text = build_plain_query(mission=mission, tasks=role_tasks)
    gdelt_query = build_gdelt_query(mission=mission, tasks=role_tasks)
    steps: list[dict[str, Any]] = []
    counter = 0
    prior_step_ids: dict[str, str] = {}
    for source_skill in selected:
        if source_skill not in PUBLIC_SOURCES:
            continue
        counter += 1
        step_id = new_step_id(role, source_skill, counter)
        artifact_path = default_raw_artifact_path(run_dir, round_id, role, source_skill)
        stdout_path = default_step_stdout_path(run_dir, round_id, role, source_skill)
        stderr_path = default_step_stderr_path(run_dir, round_id, role, source_skill)
        env_file = default_env_file(source_skill)
        notes: list[str] = []
        depends_on: list[str] = []
        skill_refs = [f"${source_skill}"]
        artifact_capture = ""
        download_dir: Path | None = None
        quarantine_dir: Path | None = None

        if source_skill == "gdelt-doc-search":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "search",
                "--query",
                gdelt_query,
                "--mode",
                "artlist",
                "--format",
                "json",
                "--start-datetime",
                to_gdelt_datetime(window["start_utc"]),
                "--end-datetime",
                to_gdelt_datetime(window["end_utc"]),
                "--max-records",
                merged_task_scalar(role_tasks, "gdelt_max_records") or "50",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            notes.append("Use GDELT DOC as broad article discovery for public claims.")
        elif source_skill in GDELT_RAW_TABLE_SOURCES:
            doc_step_id = prior_step_ids.get("gdelt-doc-search")
            if doc_step_id:
                depends_on.append(doc_step_id)
            download_dir = default_raw_download_dir(run_dir, round_id, role, source_skill)
            quarantine_dir = default_raw_quarantine_dir(run_dir, round_id, role, source_skill)
            artifact_capture = "stdout-json"
            max_files = (
                merged_task_scalar(role_tasks, GDELT_MAX_FILE_INPUT_KEYS[source_skill])
                or merged_task_scalar(role_tasks, "gdelt_table_max_files")
                or "2"
            )
            preview_lines = (
                merged_task_scalar(role_tasks, GDELT_PREVIEW_LINE_INPUT_KEYS[source_skill])
                or merged_task_scalar(role_tasks, "gdelt_table_preview_lines")
                or "2"
            )
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--mode",
                "range",
                "--start-datetime",
                to_gdelt_datetime(window["start_utc"]),
                "--end-datetime",
                to_gdelt_datetime(window["end_utc"]),
                "--max-files",
                max_files,
                "--output-dir",
                str(download_dir),
                "--overwrite",
                "--preview-lines",
                preview_lines,
                "--validate-structure",
                "--expected-columns",
                GDELT_EXPECTED_COLUMNS[source_skill],
                "--quarantine-dir",
                str(quarantine_dir),
                "--pretty",
            ]
            notes.append(
                "Capture the stdout JSON manifest at the contract artifact path; downloaded ZIP exports remain under the raw sidecar directory."
            )
            if source_skill == "gdelt-events-fetch":
                notes.append("Use GDELT Events as event-level bulk supplement after DOC recon anchors the mission window.")
            elif source_skill == "gdelt-mentions-fetch":
                notes.append("Use GDELT Mentions as mention-volume supplement after DOC recon anchors the mission window.")
            else:
                notes.append("Use GDELT GKG as theme/location/entity supplement after DOC recon anchors the mission window.")
        elif source_skill == "bluesky-cascade-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--source-mode",
                "search",
                "--query",
                query_text,
                "--search-sort",
                "latest",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--max-pages",
                merged_task_scalar(role_tasks, "bluesky_max_pages") or "5",
                "--max-posts",
                merged_task_scalar(role_tasks, "bluesky_max_posts") or "120",
                "--max-threads",
                merged_task_scalar(role_tasks, "bluesky_max_threads") or "40",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            notes.append("Collect seed posts plus cascades for diffusion structure.")
        elif source_skill == "youtube-video-search":
            youtube_comment_count_min = maybe_text(merged_task_scalar(role_tasks, "youtube_comment_count_min"))
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "search",
                "--query",
                query_text,
                "--published-after",
                window["start_utc"],
                "--published-before",
                window["end_utc"],
                "--order",
                "date",
                "--max-pages",
                merged_task_scalar(role_tasks, "youtube_max_pages") or "4",
                "--max-results",
                merged_task_scalar(role_tasks, "youtube_max_results") or "80",
                "--save-records",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            if youtube_comment_count_min:
                argv.extend(["--comment-count-min", youtube_comment_count_min])
                notes.append(
                    f"Apply explicit YouTube comment-count floor >= {youtube_comment_count_min} from task inputs."
                )
            else:
                notes.append(
                    "Do not filter out low-comment videos by default; sparse mission-relevant videos can still form auditable public claims."
                )
            notes.append("Persist candidate video IDs so comment fetch can chain from the saved JSONL artifact.")
        elif source_skill == "youtube-comments-fetch":
            video_ids_file = merged_task_scalar(role_tasks, "youtube_video_ids_file")
            if not video_ids_file:
                dependency_step = prior_step_ids.get("youtube-video-search")
                if dependency_step:
                    depends_on.append(dependency_step)
                    video_ids_file = str(default_raw_artifact_path(run_dir, round_id, role, "youtube-video-search"))
            if not video_ids_file:
                raise ValueError("youtube-comments-fetch requires youtube-video-search output or task.inputs.youtube_video_ids_file.")
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--video-ids-file",
                video_ids_file,
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--time-field",
                "published",
                "--include-replies",
                "--order",
                "time",
                "--max-videos",
                merged_task_scalar(role_tasks, "youtube_max_videos") or "12",
                "--max-thread-pages",
                merged_task_scalar(role_tasks, "youtube_max_thread_pages") or "12",
                "--max-reply-pages",
                merged_task_scalar(role_tasks, "youtube_max_reply_pages") or "12",
                "--max-comments",
                merged_task_scalar(role_tasks, "youtube_max_comments") or "1200",
                "--save-records",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            notes.append("Use the saved YouTube video artifact as the only ID source for comment collection.")
        elif source_skill == "federal-register-doc-fetch":
            federal_register_term = merged_task_scalar(role_tasks, "federal_register_term") or query_text
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--term",
                federal_register_term,
                "--start-date",
                to_date_text(window["start_utc"]),
                "--end-date",
                to_date_text(window["end_utc"]),
                "--order",
                merged_task_scalar(role_tasks, "federal_register_order") or "newest",
                "--page-size",
                merged_task_scalar(role_tasks, "federal_register_page_size") or "25",
                "--max-pages",
                merged_task_scalar(role_tasks, "federal_register_max_pages") or "3",
                "--max-records",
                merged_task_scalar(role_tasks, "federal_register_max_records") or "150",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            for agency in merged_task_string_list(role_tasks, "federal_register_agencies"):
                argv.extend(["--agency", agency])
            for document_type in merged_task_string_list(role_tasks, "federal_register_document_types"):
                argv.extend(["--document-type", document_type])
            for topic in merged_task_string_list(role_tasks, "federal_register_topics"):
                argv.extend(["--topic", topic])
            for section in merged_task_string_list(role_tasks, "federal_register_sections"):
                argv.extend(["--section", section])
            docket_id = merged_task_scalar(role_tasks, "federal_register_docket_id")
            if docket_id:
                argv.extend(["--docket-id", docket_id])
            regulation_id_number = merged_task_scalar(role_tasks, "federal_register_regulation_id_number")
            if regulation_id_number:
                argv.extend(["--regulation-id-number", regulation_id_number])
            significant = merged_task_scalar(role_tasks, "federal_register_significant")
            if significant:
                argv.extend(["--significant", significant])
            for field_name in merged_task_string_list(role_tasks, "federal_register_fields"):
                argv.extend(["--field", field_name])
            notes.append("Use Federal Register for official U.S. rulemaking, notice, and docket-linked policy documents.")
        elif source_skill == "regulationsgov-comments-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--filter-mode",
                "last-modified",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--search-term",
                query_text,
                "--max-pages",
                merged_task_scalar(role_tasks, "reggov_max_pages") or "3",
                "--max-records",
                merged_task_scalar(role_tasks, "reggov_max_records") or "300",
                "--save-response",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            agency_id = merged_task_scalar(role_tasks, "agency_id")
            if agency_id:
                argv.extend(["--agency-id", agency_id])
                notes.append(f"Constrain Regulations.gov discovery to agency_id={agency_id}.")
            else:
                notes.append("Use Regulations.gov only when policy or public-comment coverage is mission relevant.")
        elif source_skill == "regulationsgov-comment-detail-fetch":
            comment_ids_file = merged_task_scalar(role_tasks, "comment_ids_file")
            if not comment_ids_file:
                dependency_step = prior_step_ids.get("regulationsgov-comments-fetch")
                if dependency_step:
                    depends_on.append(dependency_step)
                    comment_ids_file = str(default_raw_artifact_path(run_dir, round_id, role, "regulationsgov-comments-fetch"))
            if not comment_ids_file:
                raise ValueError("regulationsgov-comment-detail-fetch requires comment IDs or Regulations.gov list output.")
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--comment-ids-file",
                comment_ids_file,
                "--max-comments",
                merged_task_scalar(role_tasks, "reggov_max_detail_comments") or "100",
                "--include",
                "attachments",
                "--save-response",
                "--output-file",
                str(artifact_path),
                "--overwrite",
                "--pretty",
            ]
            notes.append("Fetch detail records only after a comment ID list exists.")
        else:
            continue

        steps.append(
            make_step(
                step_id=step_id,
                role=role,
                source_skill=source_skill,
                task_ids=task_ids,
                artifact_path=artifact_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                command=shell_command(argv, env_file=env_file),
                depends_on=depends_on,
                notes=notes,
                skill_refs=skill_refs,
                cwd=skill_workdir(source_skill),
                artifact_capture=artifact_capture,
                download_dir=download_dir,
                quarantine_dir=quarantine_dir,
            )
        )
        prior_step_ids[source_skill] = step_id
    return steps


def build_environmentalist_steps(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    source_selection: dict[str, Any] | None,
    firms_point_padding_deg: float,
) -> list[dict[str, Any]]:
    role = "environmentalist"
    role_tasks = tasks_for_role(tasks, role)
    if not role_tasks:
        return []
    selected = role_selected_sources(mission=mission, tasks=role_tasks, role=role, source_selection=source_selection)
    if not selected:
        return []
    task_ids = step_task_ids(role_tasks)
    window = window_from_task_or_mission(mission=mission, tasks=role_tasks)
    geometry = geometry_from_task_or_mission(mission=mission, tasks=role_tasks)
    location_values = location_strings_for_geometry(geometry)
    bbox_text = bbox_text_for_geometry(
        geometry,
        point_padding_deg=float(merged_task_scalar(role_tasks, "firms_point_padding_deg") or firms_point_padding_deg),
    )
    airnow_bbox_text = bbox_text_for_geometry(
        geometry,
        point_padding_deg=float(merged_task_scalar(role_tasks, "airnow_point_padding_deg") or DEFAULT_AIRNOW_POINT_PADDING_DEG),
    )
    usgs_bbox_text = bbox_text_for_geometry(
        geometry,
        point_padding_deg=float(merged_task_scalar(role_tasks, "usgs_point_padding_deg") or DEFAULT_USGS_POINT_PADDING_DEG),
    )

    steps: list[dict[str, Any]] = []
    counter = 0
    for source_skill in selected:
        if source_skill not in ENVIRONMENT_SOURCES:
            continue
        counter += 1
        step_id = new_step_id(role, source_skill, counter)
        artifact_path = default_raw_artifact_path(run_dir, round_id, role, source_skill)
        stdout_path = default_step_stdout_path(run_dir, round_id, role, source_skill)
        stderr_path = default_step_stderr_path(run_dir, round_id, role, source_skill)
        notes: list[str] = []
        skill_refs = [f"${source_skill}"]
        env_file = default_env_file(source_skill)

        if source_skill == "airnow-hourly-obs-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                f"--bbox={airnow_bbox_text}",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
            ]
            for parameter_name in merged_task_string_list(role_tasks, "airnow_parameter_names") or DEFAULT_AIRNOW_PARAMETER_NAMES:
                argv.extend(["--parameter", parameter_name])
            argv.extend(
                [
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append(
                "Collect AirNow hourly monitoring-site observations from official file products for the mission bbox and UTC window."
            )
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "usgs-water-iv-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                f"--bbox={usgs_bbox_text}",
                "--start-datetime",
                window["start_utc"],
                "--end-datetime",
                window["end_utc"],
                "--site-type",
                merged_task_scalar(role_tasks, "usgs_site_type") or DEFAULT_USGS_SITE_TYPE,
                "--site-status",
                merged_task_scalar(role_tasks, "usgs_site_status") or DEFAULT_USGS_SITE_STATUS,
            ]
            for parameter_code in merged_task_string_list(role_tasks, "usgs_parameter_codes") or DEFAULT_USGS_PARAMETER_CODES:
                argv.extend(["--parameter-code", parameter_code])
            argv.extend(
                [
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append(
                "Collect USGS station-based hydrology observations for the mission bbox and UTC window."
            )
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "open-meteo-air-quality-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
            ]
            for location in location_values:
                argv.extend(["--location", location])
            argv.extend(
                [
                    "--start-date",
                    to_date_text(window["start_utc"]),
                    "--end-date",
                    to_date_text(window["end_utc"]),
                ]
            )
            for metric in DEFAULT_OPEN_METEO_AIR_VARS:
                argv.extend(["--hourly-var", metric])
            argv.extend(
                [
                    "--domain",
                    merged_task_scalar(role_tasks, "open_meteo_air_domain") or "auto",
                    "--cell-selection",
                    merged_task_scalar(role_tasks, "open_meteo_air_cell_selection") or "nearest",
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append("Collect modeled background air-quality context for the mission geometry.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "open-meteo-historical-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
            ]
            for location in location_values:
                argv.extend(["--location", location])
            argv.extend(
                [
                    "--start-date",
                    to_date_text(window["start_utc"]),
                    "--end-date",
                    to_date_text(window["end_utc"]),
                ]
            )
            for metric in DEFAULT_OPEN_METEO_HIST_HOURLY_VARS:
                argv.extend(["--hourly-var", metric])
            for metric in DEFAULT_OPEN_METEO_HIST_DAILY_VARS:
                argv.extend(["--daily-var", metric])
            argv.extend(
                [
                    "--timezone",
                    "GMT",
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append("Collect meteorology and soil variables for physical verification.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "open-meteo-flood-fetch":
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
            ]
            for location in location_values:
                argv.extend(["--location", location])
            argv.extend(
                [
                    "--start-date",
                    to_date_text(window["start_utc"]),
                    "--end-date",
                    to_date_text(window["end_utc"]),
                ]
            )
            for metric in merged_task_string_list(role_tasks, "open_meteo_flood_daily_vars") or DEFAULT_OPEN_METEO_FLOOD_DAILY_VARS:
                argv.extend(["--daily-var", metric])
            if text_truthy(merged_task_scalar(role_tasks, "open_meteo_flood_ensemble")):
                argv.append("--ensemble")
            argv.extend(
                [
                    "--cell-selection",
                    merged_task_scalar(role_tasks, "open_meteo_flood_cell_selection") or "nearest",
                    "--timezone",
                    merged_task_scalar(role_tasks, "open_meteo_flood_timezone") or "GMT",
                    "--output",
                    str(artifact_path),
                    "--pretty",
                ]
            )
            notes.append("Collect hydrology and flood-background discharge signals for the mission geometry.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "nasa-firms-fire-fetch":
            selected_firms_source = firms_source_for_window(
                window,
                merged_task_scalar(role_tasks, "firms_source") or "VIIRS_NOAA20_NRT",
            )
            argv = [
                "python3",
                str(fetch_script_path(source_skill)),
                "fetch",
                "--source",
                selected_firms_source,
                f"--bbox={bbox_text}",
                "--start-date",
                to_date_text(window["start_utc"]),
                "--end-date",
                to_date_text(window["end_utc"]),
                "--check-availability",
                "--output",
                str(artifact_path),
                "--pretty",
            ]
            notes.append("Collect fire detections for the mission bbox. Point missions are expanded by a deterministic bbox padding.")
            command = shell_command(argv, env_file=env_file)
        elif source_skill == "openaq-data-fetch":
            argv = orchestrate_argv(
                "collect-openaq",
                "--run-dir",
                str(run_dir),
                "--round-id",
                round_id,
                "--output",
                str(artifact_path),
                "--task-role",
                role,
                "--max-locations",
                merged_task_scalar(role_tasks, "openaq_max_locations") or "4",
                "--max-sensors-per-location",
                merged_task_scalar(role_tasks, "openaq_max_sensors_per_location") or "3",
                "--max-pages",
                merged_task_scalar(role_tasks, "openaq_max_pages") or "5",
                "--radius-meters",
                merged_task_scalar(role_tasks, "openaq_radius_meters") or "25000",
            )
            for parameter_name in merged_task_string_list(role_tasks, "openaq_parameter_names") or DEFAULT_OPENAQ_PARAMETER_NAMES:
                argv.extend(["--parameter-name", parameter_name])
            argv.append("--pretty")
            notes.append("Collect OpenAQ station measurements through location discovery, sensor discovery, and measurement fetch aggregation.")
            command = shell_command(argv, env_file=None)
        else:
            continue

        steps.append(
            make_step(
                step_id=step_id,
                role=role,
                source_skill=source_skill,
                task_ids=task_ids,
                artifact_path=artifact_path,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                command=command,
                depends_on=[],
                notes=notes,
                skill_refs=skill_refs,
                cwd=skill_workdir(source_skill),
            )
        )
    return steps


def _role_plan_summary(
    *,
    run_dir: Path,
    round_id: str,
    mission: dict[str, Any],
    role: str,
    tasks: list[dict[str, Any]],
    source_selection: dict[str, Any] | None,
) -> dict[str, Any]:
    governance = role_source_governance(mission, role)
    return {
        "task_ids": step_task_ids(tasks),
        "objective": task_objective_text(tasks),
        "allowed_sources": allowed_sources_for_role(mission, role),
        "evidence_requirements": role_evidence_requirements(tasks),
        "governed_families": [
            maybe_text(family.get("family_id"))
            for family in governance.get("families", [])
            if isinstance(family, dict) and maybe_text(family.get("family_id"))
        ],
        "override_requests": load_override_requests(run_dir, round_id, role),
        "source_selection_path": str(source_selection_path(run_dir, round_id, role)),
        "source_selection_status": maybe_text((source_selection or {}).get("status")),
        "selected_sources": role_selected_sources(
            mission=mission,
            tasks=tasks,
            role=role,
            source_selection=source_selection,
        ),
    }


def build_fetch_plan(
    *,
    run_dir: Path,
    round_id: str,
    firms_point_padding_deg: float,
) -> dict[str, Any]:
    mission = load_mission(run_dir)
    tasks = load_tasks(run_dir, round_id)
    sociologist_tasks = tasks_for_role(tasks, "sociologist")
    environmentalist_tasks = tasks_for_role(tasks, "environmentalist")
    sociologist_selection = load_source_selection(run_dir, round_id, "sociologist")
    environmentalist_selection = load_source_selection(run_dir, round_id, "environmentalist")

    steps: list[dict[str, Any]] = []
    steps.extend(
        build_sociologist_steps(
            run_dir=run_dir,
            round_id=round_id,
            mission=mission,
            tasks=tasks,
            source_selection=sociologist_selection,
        )
    )
    steps.extend(
        build_environmentalist_steps(
            run_dir=run_dir,
            round_id=round_id,
            mission=mission,
            tasks=tasks,
            source_selection=environmentalist_selection,
            firms_point_padding_deg=firms_point_padding_deg,
        )
    )
    return {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.0.0",
        "generated_at_utc": utc_now_iso(),
        "policy_profile": policy_profile_summary(mission),
        "effective_constraints": effective_constraints(mission),
        "input_snapshot": fetch_plan_input_snapshot(
            run_dir=run_dir,
            round_id=round_id,
            sociologist_selection=sociologist_selection,
            environmentalist_selection=environmentalist_selection,
        ),
        "run": {
            "run_id": maybe_text(mission.get("run_id")),
            "round_id": round_id,
            "topic": maybe_text(mission.get("topic")),
            "objective": maybe_text(mission.get("objective")),
            "region_label": maybe_text(mission_region(mission).get("label")),
            "window": mission_window(mission),
        },
        "roles": {
            "sociologist": _role_plan_summary(
                run_dir=run_dir,
                round_id=round_id,
                mission=mission,
                role="sociologist",
                tasks=sociologist_tasks,
                source_selection=sociologist_selection,
            ),
            "environmentalist": _role_plan_summary(
                run_dir=run_dir,
                round_id=round_id,
                mission=mission,
                role="environmentalist",
                tasks=environmentalist_tasks,
                source_selection=environmentalist_selection,
            ),
        },
        "steps": steps,
    }

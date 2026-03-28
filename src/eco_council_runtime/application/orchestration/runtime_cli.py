#!/usr/bin/env python3
"""Coordinate eco-council run lifecycle around OpenClaw handoffs."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import (
    file_snapshot,
    load_json_if_exists,
    pretty_json,
    read_json,
    utc_now_iso,
    write_json,
)
from eco_council_runtime.adapters.run_paths import discover_round_ids, load_mission, round_dir
from eco_council_runtime.application import orchestration_execution as application_orchestration_execution
from eco_council_runtime.application import orchestration_planning as application_orchestration_planning
from eco_council_runtime.application import orchestration_prepare as application_orchestration_prepare
from eco_council_runtime.cli_invocation import runtime_module_argv, runtime_module_command
from eco_council_runtime.domain.contract_bridge import effective_matching_authorization
from eco_council_runtime.domain.rounds import next_round_id, normalize_round_id, round_dir_name
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings as shared_unique_strings
from eco_council_runtime.external_skills import (
    openaq_api_script_path,
)
from eco_council_runtime.layout import PROJECT_DIR

REPO_DIR = PROJECT_DIR

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
ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_ID_INPUT_PATTERN = re.compile(r"^round[-_](\d{3})$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")
ENV_ASSIGNMENT_PATTERN = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")

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

def unique_strings(values: list[str]) -> list[str]:
    return shared_unique_strings(values, casefold=True)


def ensure_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return value


def ensure_object_list(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a JSON list.")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError(f"{label} must contain only JSON objects.")
    return value


def approved_next_round_tasks_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived" / "approved_next_round_tasks.json"


role_raw_dir = application_orchestration_planning.role_raw_dir
default_raw_artifact_path = application_orchestration_planning.default_raw_artifact_path
resolve_round_id = application_orchestration_prepare.resolve_round_id
load_tasks = application_orchestration_prepare.load_tasks
load_source_selection = application_orchestration_prepare.load_source_selection
fetch_plan_input_snapshot = application_orchestration_planning.fetch_plan_input_snapshot
ensure_fetch_plan_inputs_match = application_orchestration_planning.ensure_fetch_plan_inputs_match
tasks_for_role = application_orchestration_prepare.tasks_for_role
mission_window = application_orchestration_prepare.mission_window
mission_region = application_orchestration_prepare.mission_region
task_inputs = application_orchestration_planning.task_inputs
task_notes = application_orchestration_planning.task_notes
merged_task_string_list = application_orchestration_planning.merged_task_string_list
merged_task_scalar = application_orchestration_planning.merged_task_scalar
task_objective_text = application_orchestration_planning.task_objective_text
role_supported_sources = application_orchestration_planning.role_supported_sources
source_selection_selected_sources = application_orchestration_planning.source_selection_selected_sources
ensure_source_selection_respects_governance = application_orchestration_planning.ensure_source_selection_respects_governance
role_selected_sources = application_orchestration_planning.role_selected_sources
build_plain_query = application_orchestration_planning.build_plain_query
build_gdelt_query = application_orchestration_planning.build_gdelt_query
primary_region_search_label = application_orchestration_planning.primary_region_search_label
iter_evidence_requirement_summaries = application_orchestration_planning.iter_evidence_requirement_summaries
extract_query_tokens = application_orchestration_planning.extract_query_tokens
compact_query_terms = application_orchestration_planning.compact_query_terms
gdelt_literal_term = application_orchestration_planning.gdelt_literal_term
geometry_from_task_or_mission = application_orchestration_planning.geometry_from_task_or_mission
window_from_task_or_mission = application_orchestration_planning.window_from_task_or_mission
center_point_for_geometry = application_orchestration_planning.center_point_for_geometry
location_strings_for_geometry = application_orchestration_planning.location_strings_for_geometry
bbox_text_for_geometry = application_orchestration_planning.bbox_text_for_geometry
source_role = application_orchestration_planning.source_role
skill_workdir = application_orchestration_planning.skill_workdir
default_env_file = application_orchestration_planning.default_env_file
contract_argv = application_orchestration_planning.contract_argv
contract_command = application_orchestration_planning.contract_command
normalize_argv = application_orchestration_planning.normalize_argv
normalize_command = application_orchestration_planning.normalize_command
reporting_argv = application_orchestration_planning.reporting_argv
reporting_command = application_orchestration_planning.reporting_command
orchestrate_argv = application_orchestration_planning.orchestrate_argv
orchestrate_command = application_orchestration_planning.orchestrate_command
shell_join = application_orchestration_planning.shell_join
shell_command = application_orchestration_planning.shell_command
make_step = application_orchestration_planning.make_step
new_step_id = application_orchestration_planning.new_step_id
regs_task_enabled = application_orchestration_planning.regs_task_enabled
step_task_ids = application_orchestration_planning.step_task_ids
build_sociologist_steps = application_orchestration_planning.build_sociologist_steps
build_environmentalist_steps = application_orchestration_planning.build_environmentalist_steps
build_fetch_plan = application_orchestration_planning.build_fetch_plan

render_moderator_task_review_prompt = application_orchestration_prepare.render_moderator_task_review_prompt
render_role_fetch_prompt = application_orchestration_prepare.render_role_fetch_prompt
write_round_manifest = application_orchestration_prepare.write_round_manifest


strip_inline_comment = application_orchestration_execution.strip_inline_comment
parse_env_file = application_orchestration_execution.parse_env_file
run_json_cli = application_orchestration_execution.run_json_cli
materialize_json_artifact_from_stdout = application_orchestration_execution.materialize_json_artifact_from_stdout
validate_json_artifact_if_applicable = application_orchestration_execution.validate_json_artifact_if_applicable
build_fetch_execution_payload = application_orchestration_execution.build_fetch_execution_payload
write_fetch_execution_snapshot = application_orchestration_execution.write_fetch_execution_snapshot
build_data_plane_execution_payload = application_orchestration_execution.build_data_plane_execution_payload
write_data_plane_execution_snapshot = application_orchestration_execution.write_data_plane_execution_snapshot
ensure_ok_envelope = application_orchestration_execution.ensure_ok_envelope

def prepare_round(
    *,
    run_dir: Path,
    round_id: str,
    firms_point_padding_deg: float,
) -> dict[str, Any]:
    run_path = run_dir.expanduser().resolve()
    current_round_id = resolve_round_id(run_path, round_id)
    plan = build_fetch_plan(run_dir=run_path, round_id=current_round_id, firms_point_padding_deg=firms_point_padding_deg)
    return application_orchestration_prepare.materialize_prepare_round_outputs(
        run_dir=run_path,
        round_id=current_round_id,
        plan=plan,
    )


execute_fetch_plan = application_orchestration_execution.execute_fetch_plan
fetch_status_role = application_orchestration_execution.fetch_status_role
fetch_status_has_usable_artifact = application_orchestration_execution.fetch_status_has_usable_artifact
usable_fetch_artifacts = application_orchestration_execution.usable_fetch_artifacts
discover_normalize_inputs = application_orchestration_execution.discover_normalize_inputs
build_reporting_handoff = application_orchestration_execution.build_reporting_handoff
run_data_plane_json_step = application_orchestration_execution.run_data_plane_json_step
run_data_plane_callable_step = application_orchestration_execution.run_data_plane_callable_step
run_data_plane = application_orchestration_execution.run_data_plane
run_matching_adjudication = application_orchestration_execution.run_matching_adjudication

def command_bootstrap_run(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if run_dir.exists() and any(run_dir.iterdir()) and not args.allow_existing:
        raise ValueError(f"Run directory already exists and is not empty: {run_dir}. Use --allow-existing to proceed.")
    cmd = contract_argv(
        "scaffold-run-from-mission",
        "--run-dir",
        str(run_dir),
        "--mission-input",
        str(Path(args.mission_input).expanduser().resolve()),
    )
    if args.tasks_input:
        cmd.extend(["--tasks-input", str(Path(args.tasks_input).expanduser().resolve())])
    contract_payload = ensure_ok_envelope(run_json_cli(cmd), "scaffold-run-from-mission")
    round_id = maybe_text(contract_payload.get("round_id")) or "round-001"
    task_prompt = render_moderator_task_review_prompt(run_dir=run_dir, round_id=round_id)
    manifest_path = write_round_manifest(
        run_dir=run_dir,
        round_id=round_id,
        stage="task-review",
        task_prompt=task_prompt,
        fetch_plan=None,
        fetch_prompts={},
    )
    bundle_payload = ensure_ok_envelope(
        run_json_cli(contract_argv("validate-bundle", "--run-dir", str(run_dir))),
        "validate bundle",
    )
    return {
        "run_dir": str(run_dir),
        "round_id": round_id,
        "contract": contract_payload,
        "task_review_prompt_path": str(task_prompt),
        "manifest_path": str(manifest_path),
        "bundle_validation": bundle_payload,
    }


def command_prepare_round(args: argparse.Namespace) -> dict[str, Any]:
    return prepare_round(
        run_dir=Path(args.run_dir),
        round_id=args.round_id,
        firms_point_padding_deg=args.firms_point_padding_deg,
    )


def command_execute_fetch_plan(args: argparse.Namespace) -> dict[str, Any]:
    return execute_fetch_plan(
        run_dir=Path(args.run_dir),
        round_id=args.round_id,
        continue_on_error=args.continue_on_error,
        skip_existing=args.skip_existing,
        timeout_seconds=args.timeout_seconds,
    )


def call_openaq_api(
    *,
    env: dict[str, str],
    path: str,
    query_pairs: list[str],
    max_pages: int,
    all_pages: bool,
) -> dict[str, Any]:
    argv = [
        "python3",
        str(openaq_api_script_path()),
        "request",
        "--path",
        path,
    ]
    for pair in query_pairs:
        argv.extend(["--query", pair])
    if all_pages:
        argv.append("--all-pages")
        argv.extend(["--max-pages", str(max_pages)])
    return run_json_cli(argv, cwd=skill_workdir("openaq-data-fetch"), env=env)


def normalized_parameter_name(value: Any) -> str:
    return maybe_text(value).casefold().replace("_", "").replace("-", "").replace(".", "")


def sensor_parameter_name(sensor: dict[str, Any]) -> str:
    parameter = sensor.get("parameter")
    if isinstance(parameter, dict):
        return maybe_text(parameter.get("name") or parameter.get("displayName") or parameter.get("parameter"))
    return maybe_text(parameter or sensor.get("parameterName") or sensor.get("name"))


def sensor_parameter_matches(sensor: dict[str, Any], allowed_names: list[str]) -> bool:
    if not allowed_names:
        return True
    normalized = normalized_parameter_name(sensor_parameter_name(sensor))
    if not normalized:
        return False
    return normalized in {normalized_parameter_name(name) for name in allowed_names}


def coordinates_from_location(location: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinates = location.get("coordinates")
    if isinstance(coordinates, dict):
        latitude = coordinates.get("latitude")
        longitude = coordinates.get("longitude")
        try:
            return (float(latitude), float(longitude))
        except (TypeError, ValueError):
            return (None, None)
    for lat_key in ("latitude", "lat"):
        for lon_key in ("longitude", "lon", "lng"):
            if lat_key in location and lon_key in location:
                try:
                    return (float(location[lat_key]), float(location[lon_key]))
                except (TypeError, ValueError):
                    return (None, None)
    return (None, None)


def command_collect_openaq(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    round_id = resolve_round_id(run_dir, args.round_id)
    mission = load_mission(run_dir)
    tasks = tasks_for_role(load_tasks(run_dir, round_id), args.task_role)
    if not tasks:
        raise ValueError(f"No tasks assigned to role={args.task_role!r} in {round_id}.")

    geometry = geometry_from_task_or_mission(mission=mission, tasks=tasks)
    window = window_from_task_or_mission(mission=mission, tasks=tasks)
    env_file = default_env_file("openaq-data-fetch")
    if env_file is None:
        raise ValueError("No env file found for openaq-data-fetch.")
    env = dict(os.environ)
    env.update(parse_env_file(env_file))

    output_path = Path(args.output).expanduser().resolve() if args.output else default_raw_artifact_path(
        run_dir,
        round_id,
        args.task_role,
        "openaq-data-fetch",
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    location_query_pairs = ["limit=1000"]
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "BBox":
        location_query_pairs.append(f"bbox={bbox_text_for_geometry(geometry, point_padding_deg=0.0)}")
    elif geometry_type == "Point":
        latitude, longitude = center_point_for_geometry(geometry)
        location_query_pairs.append(f"coordinates={latitude:.6f},{longitude:.6f}")
        location_query_pairs.append(f"radius={args.radius_meters}")
    else:
        raise ValueError(f"Unsupported mission geometry type for OpenAQ: {geometry_type!r}")

    locations_payload = call_openaq_api(
        env=env,
        path="/v3/locations",
        query_pairs=location_query_pairs,
        max_pages=args.max_pages,
        all_pages=True,
    )
    locations = ensure_object_list(locations_payload.get("results", []), "OpenAQ locations results")
    selected_locations = locations[: args.max_locations]

    records: list[dict[str, Any]] = []
    discovery_summary: list[dict[str, Any]] = []
    allowed_parameter_names = args.parameter_name or DEFAULT_OPENAQ_PARAMETER_NAMES

    for location in selected_locations:
        location_id = location.get("id")
        if location_id in (None, ""):
            continue
        sensors_payload = call_openaq_api(
            env=env,
            path=f"/v3/locations/{location_id}/sensors",
            query_pairs=["limit=1000"],
            max_pages=args.max_pages,
            all_pages=True,
        )
        sensors = ensure_object_list(sensors_payload.get("results", []), f"OpenAQ sensors results for location {location_id}")
        selected_sensors = [sensor for sensor in sensors if sensor_parameter_matches(sensor, allowed_parameter_names)]
        selected_sensors = selected_sensors[: args.max_sensors_per_location]
        latitude, longitude = coordinates_from_location(location)

        location_summary = {
            "location_id": location_id,
            "location_name": maybe_text(location.get("name")),
            "sensor_count": len(selected_sensors),
            "selected_sensor_ids": [sensor.get("id") for sensor in selected_sensors],
        }
        discovery_summary.append(location_summary)

        for sensor in selected_sensors:
            sensor_id = sensor.get("id")
            if sensor_id in (None, ""):
                continue
            query_pairs = [
                "limit=1000",
                f"datetime_from={window['start_utc']}",
                f"datetime_to={window['end_utc']}",
            ]
            measurements_payload = call_openaq_api(
                env=env,
                path=f"/v3/sensors/{sensor_id}/measurements",
                query_pairs=query_pairs,
                max_pages=args.max_pages,
                all_pages=True,
            )
            measurement_rows = ensure_object_list(
                measurements_payload.get("results", []),
                f"OpenAQ measurements results for sensor {sensor_id}",
            )
            for row in measurement_rows:
                enriched = dict(row)
                if "location" not in enriched:
                    enriched["location"] = {
                        "id": location_id,
                        "name": maybe_text(location.get("name")),
                    }
                if "sensor" not in enriched:
                    enriched["sensor"] = {
                        "id": sensor_id,
                    }
                if "parameter" not in enriched:
                    parameter = sensor.get("parameter")
                    if parameter is not None:
                        enriched["parameter"] = parameter
                    else:
                        enriched["parameter"] = {"name": sensor_parameter_name(sensor)}
                if "coordinates" not in enriched and latitude is not None and longitude is not None:
                    enriched["coordinates"] = {"latitude": latitude, "longitude": longitude}
                records.append(enriched)

    payload = {
        "generated_at_utc": utc_now_iso(),
        "run_id": maybe_text(mission.get("run_id")),
        "round_id": round_id,
        "source_skill": "openaq-data-fetch",
        "request": {
            "geometry": geometry,
            "window": window,
            "max_locations": args.max_locations,
            "max_sensors_per_location": args.max_sensors_per_location,
            "max_pages": args.max_pages,
            "radius_meters": args.radius_meters,
            "parameter_names": allowed_parameter_names,
        },
        "discovery_summary": discovery_summary,
        "record_count": len(records),
        "records": records,
    }
    write_json(output_path, payload, pretty=args.pretty)
    return {
        "output_path": str(output_path),
        "record_count": len(records),
        "location_count": len(selected_locations),
        "discovery_summary": discovery_summary,
        "env_file": str(env_file),
    }


def command_run_data_plane(args: argparse.Namespace) -> dict[str, Any]:
    return run_data_plane(run_dir=Path(args.run_dir), round_id=args.round_id)


def command_run_matching_adjudication(args: argparse.Namespace) -> dict[str, Any]:
    return run_matching_adjudication(run_dir=Path(args.run_dir), round_id=args.round_id)


def command_advance_round(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = Path(args.run_dir).expanduser().resolve()
    current_round_id = resolve_round_id(run_dir, args.round_id)
    decision_path = (
        Path(args.decision_input).expanduser().resolve()
        if args.decision_input
        else round_dir(run_dir, current_round_id) / "moderator" / "council_decision.json"
    )
    if not decision_path.exists():
        raise ValueError(f"Decision file does not exist: {decision_path}")
    decision = ensure_object(read_json(decision_path), f"{decision_path}")
    if not decision.get("next_round_required"):
        return {
            "run_dir": str(run_dir),
            "round_id": current_round_id,
            "decision_path": str(decision_path),
            "moderator_status": maybe_text(decision.get("moderator_status")),
            "advanced": False,
        }
    next_round_tasks = ensure_object_list(decision.get("next_round_tasks", []), "council_decision.next_round_tasks")
    if not next_round_tasks:
        raise ValueError("Decision requires another round, but next_round_tasks is empty.")
    next_round_ids = unique_strings([maybe_text(task.get("round_id")) for task in next_round_tasks if maybe_text(task.get("round_id"))])
    if len(next_round_ids) != 1:
        raise ValueError(f"Expected exactly one next round_id in next_round_tasks, got {next_round_ids}")
    next_round_id_value = next_round_ids[0]
    next_round_path = round_dir(run_dir, next_round_id_value)
    if next_round_path.exists() and any(next_round_path.iterdir()) and not args.allow_existing:
        raise ValueError(f"Next round already exists: {next_round_path}. Use --allow-existing to proceed.")

    tasks_path = approved_next_round_tasks_path(run_dir, current_round_id)
    write_json(tasks_path, next_round_tasks, pretty=True)
    scaffold_payload = ensure_ok_envelope(
        run_json_cli(contract_argv("scaffold-round", "--run-dir", str(run_dir), "--round-id", next_round_id_value, "--tasks-input", str(tasks_path))),
        "scaffold-round",
    )
    task_prompt = render_moderator_task_review_prompt(run_dir=run_dir, round_id=next_round_id_value)
    manifest_path = write_round_manifest(
        run_dir=run_dir,
        round_id=next_round_id_value,
        stage="task-review",
        task_prompt=task_prompt,
        fetch_plan=None,
        fetch_prompts={},
    )
    bundle_payload = ensure_ok_envelope(
        run_json_cli(contract_argv("validate-bundle", "--run-dir", str(run_dir))),
        "validate bundle",
    )
    return {
        "run_dir": str(run_dir),
        "current_round_id": current_round_id,
        "next_round_id": next_round_id_value,
        "decision_path": str(decision_path),
        "approved_next_round_tasks_path": str(tasks_path),
        "scaffold": scaffold_payload,
        "task_review_prompt_path": str(task_prompt),
        "manifest_path": str(manifest_path),
        "bundle_validation": bundle_payload,
        "advanced": True,
    }


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Orchestrate eco-council run lifecycle and OpenClaw handoffs.")
    sub = parser.add_subparsers(dest="command", required=True)

    bootstrap = sub.add_parser("bootstrap-run", help="Scaffold a run from mission JSON and render moderator task review prompt.")
    bootstrap.add_argument("--run-dir", required=True, help="Run directory.")
    bootstrap.add_argument("--mission-input", required=True, help="Mission JSON path.")
    bootstrap.add_argument("--tasks-input", default="", help="Optional initial round-task list JSON path.")
    bootstrap.add_argument("--allow-existing", action="store_true", help="Allow writing into an existing run directory.")
    add_pretty_flag(bootstrap)

    prepare = sub.add_parser("prepare-round", help="Build one round fetch plan plus OpenClaw fetch prompts.")
    prepare.add_argument("--run-dir", required=True, help="Run directory.")
    prepare.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    prepare.add_argument(
        "--firms-point-padding-deg",
        type=float,
        default=0.5,
        help="BBox padding in degrees when NASA FIRMS is planned for a point mission geometry.",
    )
    add_pretty_flag(prepare)

    execute = sub.add_parser("execute-fetch-plan", help="Execute the shell commands in the prepared fetch plan.")
    execute.add_argument("--run-dir", required=True, help="Run directory.")
    execute.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    execute.add_argument("--continue-on-error", action="store_true", help="Continue executing remaining steps after a failure.")
    execute.add_argument("--skip-existing", action="store_true", help="Skip steps whose artifact path already exists.")
    execute.add_argument("--timeout-seconds", type=int, default=900, help="Per-step timeout in seconds.")
    add_pretty_flag(execute)

    collect_openaq = sub.add_parser("collect-openaq", help="Collect OpenAQ measurements through the multi-step discovery chain.")
    collect_openaq.add_argument("--run-dir", required=True, help="Run directory.")
    collect_openaq.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    collect_openaq.add_argument("--output", default="", help="Output JSON artifact path.")
    collect_openaq.add_argument("--task-role", default="environmentalist", help="Assigned role whose tasks provide geometry and window.")
    collect_openaq.add_argument("--max-locations", type=int, default=4, help="Maximum nearby locations to keep.")
    collect_openaq.add_argument("--max-sensors-per-location", type=int, default=3, help="Maximum sensors to keep per location.")
    collect_openaq.add_argument("--max-pages", type=int, default=5, help="Maximum pages per OpenAQ API request.")
    collect_openaq.add_argument("--radius-meters", type=int, default=25000, help="Radius used for point-based location discovery.")
    collect_openaq.add_argument(
        "--parameter-name",
        action="append",
        default=[],
        help="Preferred parameter names to keep, for example pm25 or no2. Repeat for multiple values.",
    )
    add_pretty_flag(collect_openaq)

    data_plane = sub.add_parser("run-data-plane", help="Run normalization plus evidence-curation packet generation.")
    data_plane.add_argument("--run-dir", required=True, help="Run directory.")
    data_plane.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    add_pretty_flag(data_plane)

    matching = sub.add_parser(
        "run-matching-adjudication",
        help="Run authorized matching/adjudication, auto-materialize investigation review, and prepare expert report packets.",
    )
    matching.add_argument("--run-dir", required=True, help="Run directory.")
    matching.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    add_pretty_flag(matching)

    advance = sub.add_parser("advance-round", help="Scaffold the next round from an approved council decision.")
    advance.add_argument("--run-dir", required=True, help="Run directory.")
    advance.add_argument("--round-id", default="", help="Current round identifier. Defaults to latest round.")
    advance.add_argument("--decision-input", default="", help="Optional explicit council-decision JSON path.")
    advance.add_argument("--allow-existing", action="store_true", help="Allow scaffolding into an already existing next-round directory.")
    add_pretty_flag(advance)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "bootstrap-run": command_bootstrap_run,
        "prepare-round": command_prepare_round,
        "execute-fetch-plan": command_execute_fetch_plan,
        "collect-openaq": command_collect_openaq,
        "run-data-plane": command_run_data_plane,
        "run-matching-adjudication": command_run_matching_adjudication,
        "advance-round": command_advance_round,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:
        result = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(result, pretty=getattr(args, "pretty", False)))
        return 1
    result = {"command": args.command, "ok": True, "payload": payload}
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Step synthesis and command-building for orchestration fetch plans."""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.run_paths import round_dir
from eco_council_runtime.application import orchestration_prepare
from eco_council_runtime.application.orchestration.geometry import (
    bbox_text_for_geometry,
    firms_source_for_window,
    geometry_from_task_or_mission,
    location_strings_for_geometry,
    to_date_text,
    to_gdelt_datetime,
    window_from_task_or_mission,
)
from eco_council_runtime.application.orchestration.governance import (
    ENVIRONMENT_SOURCES,
    PUBLIC_SOURCES,
    role_selected_sources,
)
from eco_council_runtime.application.orchestration.query_builders import (
    build_gdelt_query,
    build_plain_query,
    merged_task_scalar,
    merged_task_string_list,
    regs_task_enabled,
    step_task_ids,
)
from eco_council_runtime.cli_invocation import runtime_module_argv, runtime_module_command
from eco_council_runtime.domain.text import maybe_text, text_truthy
from eco_council_runtime.external_skills import (
    default_env_file as detached_default_env_file,
    fetch_script_path,
    skill_dir,
)
from eco_council_runtime.layout import PROJECT_DIR

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
tasks_for_role = orchestration_prepare.tasks_for_role


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


__all__ = [
    "DEFAULT_AIRNOW_PARAMETER_NAMES",
    "DEFAULT_AIRNOW_POINT_PADDING_DEG",
    "DEFAULT_OPENAQ_PARAMETER_NAMES",
    "DEFAULT_OPEN_METEO_AIR_VARS",
    "DEFAULT_OPEN_METEO_FLOOD_DAILY_VARS",
    "DEFAULT_OPEN_METEO_HIST_DAILY_VARS",
    "DEFAULT_OPEN_METEO_HIST_HOURLY_VARS",
    "DEFAULT_USGS_PARAMETER_CODES",
    "DEFAULT_USGS_POINT_PADDING_DEG",
    "DEFAULT_USGS_SITE_STATUS",
    "DEFAULT_USGS_SITE_TYPE",
    "ENVIRONMENT_SOURCES",
    "PUBLIC_SOURCES",
    "REPO_DIR",
    "build_environmentalist_steps",
    "build_sociologist_steps",
    "contract_argv",
    "contract_command",
    "default_env_file",
    "default_raw_artifact_path",
    "default_raw_download_dir",
    "default_raw_quarantine_dir",
    "default_step_stderr_path",
    "default_step_stdout_path",
    "make_step",
    "new_step_id",
    "normalize_argv",
    "normalize_command",
    "orchestrate_argv",
    "orchestrate_command",
    "reporting_argv",
    "reporting_command",
    "role_meta_dir",
    "role_raw_dir",
    "shell_command",
    "shell_join",
    "skill_workdir",
    "source_role",
]

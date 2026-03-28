"""Shared helpers and constants for deterministic simulation workflows."""

from __future__ import annotations

import csv
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import io
import json
import os
import random
import re
import tempfile
from pathlib import Path
from typing import Any

from eco_council_runtime.layout import SIMULATE_SCENARIO_DIR

PRESET_DIR = SIMULATE_SCENARIO_DIR
SCHEMA_VERSION = "1.0.0"
SCENARIO_KIND = "eco-council-simulation-scenario"
MODE_VALUES = {"support", "contradict", "mixed", "sparse"}
JSONL_SOURCES = {
    "youtube-video-search",
    "youtube-comments-fetch",
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
}
RAW_GDELT_SOURCES = {
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
}
GDELT_EXPECTED_COLUMNS = {
    "gdelt-events-fetch": 61,
    "gdelt-mentions-fetch": 16,
    "gdelt-gkg-fetch": 27,
}
GDELT_ZIP_SUFFIX = {
    "gdelt-events-fetch": ".export.CSV.zip",
    "gdelt-mentions-fetch": ".mentions.CSV.zip",
    "gdelt-gkg-fetch": ".gkg.csv.zip",
}
GDELT_MEMBER_SUFFIX = {
    "gdelt-events-fetch": ".export.CSV",
    "gdelt-mentions-fetch": ".mentions.CSV",
    "gdelt-gkg-fetch": ".gkg.csv",
}
DEFAULT_GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"
DEFAULT_GDELT_PREVIEW_LINES = 2
SUPPORTED_SOURCES = {
    "gdelt-doc-search",
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
    "bluesky-cascade-fetch",
    "youtube-video-search",
    "youtube-comments-fetch",
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
    "airnow-hourly-obs-fetch",
    "open-meteo-historical-fetch",
    "open-meteo-air-quality-fetch",
    "open-meteo-flood-fetch",
    "nasa-firms-fire-fetch",
    "openaq-data-fetch",
}
SUPPORTED_CLAIM_TYPES = {
    "air-pollution",
    "drought",
    "flood",
    "heat",
    "other",
    "policy-reaction",
    "smoke",
    "water-pollution",
    "wildfire",
}
CLAIM_KEYWORDS = {
    "wildfire": ("wildfire", "fire", "bushfire", "burning", "forest fire"),
    "smoke": ("smoke", "haze", "smog", "ash"),
    "flood": ("flood", "flooding", "inundation", "overflow"),
    "heat": ("heat", "heatwave", "extreme heat", "hot weather"),
    "drought": ("drought", "dry spell", "water shortage", "dryness"),
    "air-pollution": ("air quality", "pm2.5", "pm10", "pollution", "dirty air", "aqi"),
    "water-pollution": ("water pollution", "contaminated water", "sewage", "spill"),
    "policy-reaction": ("policy", "regulation", "rulemaking", "public comment", "epa", "agency"),
}
CLAIM_METRIC_RULES = {
    "smoke": {
        "support": {"pm2_5": 35.0, "pm10": 50.0, "us_aqi": 100.0, "fire_detection_count": 1.0},
        "contradict": {"pm2_5": 12.0, "pm10": 20.0, "us_aqi": 50.0},
    },
    "air-pollution": {
        "support": {"pm2_5": 35.0, "pm10": 50.0, "us_aqi": 100.0, "nitrogen_dioxide": 40.0, "ozone": 100.0},
        "contradict": {"pm2_5": 12.0, "pm10": 20.0, "us_aqi": 50.0},
    },
    "wildfire": {
        "support": {"fire_detection_count": 1.0, "temperature_2m": 30.0, "wind_speed_10m": 5.0},
        "contradict": {"fire_detection_count": 0.0, "precipitation_sum": 20.0, "relative_humidity_2m": 70.0},
    },
    "flood": {
        "support": {
            "precipitation_sum": 20.0,
            "precipitation": 10.0,
            "river_discharge": 100.0,
            "river_discharge_mean": 100.0,
            "river_discharge_max": 120.0,
            "river_discharge_p75": 100.0,
        },
        "contradict": {
            "precipitation_sum": 1.0,
            "river_discharge": 20.0,
            "river_discharge_mean": 20.0,
            "river_discharge_max": 25.0,
            "river_discharge_p75": 20.0,
        },
    },
    "heat": {
        "support": {"temperature_2m": 32.0},
        "contradict": {"temperature_2m": 22.0},
    },
    "drought": {
        "support": {"precipitation_sum": 2.0, "soil_moisture_0_to_7cm": 0.12},
        "contradict": {"precipitation_sum": 10.0, "soil_moisture_0_to_7cm": 0.25},
    },
}
SOURCE_METRIC_CATALOG = {
    "airnow-hourly-obs-fetch": ("pm2_5", "pm10", "ozone", "nitrogen_dioxide"),
    "open-meteo-air-quality-fetch": ("pm2_5", "pm10", "us_aqi", "nitrogen_dioxide", "ozone"),
    "open-meteo-historical-fetch": (
        "temperature_2m",
        "wind_speed_10m",
        "relative_humidity_2m",
        "precipitation_sum",
        "soil_moisture_0_to_7cm",
    ),
    "open-meteo-flood-fetch": ("river_discharge", "river_discharge_mean", "river_discharge_max", "river_discharge_p75", "precipitation"),
    "openaq-data-fetch": ("pm2_5", "pm10"),
}
METRIC_UNITS = {
    "fire_detection_count": "count",
    "nitrogen_dioxide": "ug/m3",
    "ozone": "ug/m3",
    "pm10": "ug/m3",
    "pm2_5": "ug/m3",
    "precipitation": "mm",
    "precipitation_sum": "mm",
    "relative_humidity_2m": "%",
    "river_discharge": "m3/s",
    "river_discharge_max": "m3/s",
    "river_discharge_mean": "m3/s",
    "river_discharge_p75": "m3/s",
    "soil_moisture_0_to_7cm": "m3/m3",
    "temperature_2m": "C",
    "us_aqi": "index",
    "wind_speed_10m": "m/s",
}
DAILY_METRICS = {
    "precipitation_sum",
    "river_discharge",
    "river_discharge_max",
    "river_discharge_mean",
    "river_discharge_p75",
}
BASE_RECORD_COUNTS = {
    "gdelt-doc-search": 4,
    "gdelt-events-fetch": 4,
    "gdelt-mentions-fetch": 4,
    "gdelt-gkg-fetch": 4,
    "bluesky-cascade-fetch": 5,
    "youtube-video-search": 3,
    "youtube-comments-fetch": 8,
    "regulationsgov-comments-fetch": 4,
    "regulationsgov-comment-detail-fetch": 3,
    "airnow-hourly-obs-fetch": 4,
    "open-meteo-air-quality-fetch": 3,
    "open-meteo-historical-fetch": 3,
    "open-meteo-flood-fetch": 3,
    "nasa-firms-fire-fetch": 3,
    "openaq-data-fetch": 4,
}
AIRNOW_PARAMETER_NAMES = {
    "nitrogen_dioxide": "NO2",
    "ozone": "OZONE",
    "pm10": "PM10",
    "pm2_5": "PM25",
}
AIRNOW_AQI_KIND = {
    "nitrogen_dioxide": "hourly-aqi",
    "ozone": "nowcast-aqi",
    "pm10": "nowcast-aqi",
    "pm2_5": "nowcast-aqi",
}
AIRNOW_UNITS = {
    "nitrogen_dioxide": "PPB",
    "ozone": "PPB",
    "pm10": "UG/M3",
    "pm2_5": "UG/M3",
}
PUBLIC_DOMAINS = {
    "air-pollution": "airwatch.example.invalid",
    "drought": "drylands.example.invalid",
    "flood": "riverdesk.example.invalid",
    "heat": "heatbeat.example.invalid",
    "other": "envdesk.example.invalid",
    "policy-reaction": "policywatch.example.invalid",
    "smoke": "haze-monitor.example.invalid",
    "water-pollution": "waterscope.example.invalid",
    "wildfire": "firewatch.example.invalid",
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def pretty_json(payload: Any, *, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_jsonl(path: Path) -> list[Any]:
    records: list[Any] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            records.append(json.loads(text))
    return records


def atomic_write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def atomic_write_bytes_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=path.parent, delete=False) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def write_json(path: Path, payload: Any, *, pretty: bool) -> None:
    atomic_write_text_file(path, pretty_json(payload, pretty=pretty) + "\n")


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    atomic_write_text_file(path, "".join(json.dumps(record, ensure_ascii=True, sort_keys=True) + "\n" for record in records))


def write_bytes(path: Path, payload: bytes) -> None:
    atomic_write_bytes_file(path, payload)


def write_text(path: Path, content: str) -> None:
    atomic_write_text_file(path, content)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_int(*parts: Any) -> int:
    digest = hashlib.sha256("||".join(maybe_text(part) for part in parts).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_datetime(value: Any) -> datetime | None:
    text = maybe_text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None
    if parsed is not None:
        return parsed.astimezone(timezone.utc)
    for pattern in ("%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def to_rfc3339_z(value: datetime | None) -> str:
    if value is None:
        return utc_now_iso()
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def round_directory_name(round_id: str) -> str:
    return round_id.replace("-", "_")


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_directory_name(round_id)


def discover_round_ids(run_dir: Path) -> list[str]:
    output: list[str] = []
    if not run_dir.exists():
        return output
    for child in run_dir.iterdir():
        if not child.is_dir():
            continue
        name = child.name
        if not name.startswith("round_"):
            continue
        output.append(name.replace("_", "-"))
    output.sort()
    return output


def resolve_round_id(run_dir: Path, round_id: str) -> str:
    if round_id:
        return round_id
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}.")
    return round_ids[-1]


def moderator_derived_dir(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "derived"


def mission_path(run_dir: Path) -> Path:
    return run_dir / "mission.json"


def tasks_path(run_dir: Path, round_id: str) -> Path:
    return round_dir(run_dir, round_id) / "moderator" / "tasks.json"


def fetch_plan_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "fetch_plan.json"


def fetch_execution_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "fetch_execution.json"


def fetch_lock_path(run_dir: Path, round_id: str) -> Path:
    return moderator_derived_dir(run_dir, round_id) / "fetch.lock"


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return round_dir(run_dir, round_id) / role / "source_selection.json"


def file_snapshot(path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {"path": str(path), "exists": exists, "sha256": file_sha256(path) if exists else ""}


@contextmanager
def exclusive_file_lock(path: Path) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield handle
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def ensure_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object.")
    return value


def ensure_fetch_plan_inputs_match(*, run_dir: Path, round_id: str, plan: dict[str, Any]) -> None:
    snapshot = ensure_object(plan.get("input_snapshot"), "fetch_plan.input_snapshot")
    task_snapshot = ensure_object(snapshot.get("tasks"), "fetch_plan.input_snapshot.tasks")
    current_task_snapshot = file_snapshot(tasks_path(run_dir, round_id))
    issues: list[str] = []
    if maybe_text(task_snapshot.get("sha256")) != maybe_text(current_task_snapshot.get("sha256")):
        issues.append(f"tasks.json changed ({tasks_path(run_dir, round_id)})")

    source_snapshots = ensure_object(snapshot.get("source_selections"), "fetch_plan.input_snapshot.source_selections")
    for role in ("sociologist", "environmentalist"):
        expected = ensure_object(source_snapshots.get(role), f"fetch_plan.input_snapshot.source_selections.{role}")
        path = source_selection_path(run_dir, round_id, role)
        current = file_snapshot(path)
        current_payload = read_json(path) if path.exists() else {}
        current_status = maybe_text((current_payload or {}).get("status"))
        if maybe_text(expected.get("sha256")) != maybe_text(current.get("sha256")):
            issues.append(f"{role} source_selection changed ({path})")
        if maybe_text(expected.get("status")) != current_status:
            issues.append(
                f"{role} source_selection status changed "
                f"(expected {maybe_text(expected.get('status')) or '<empty>'}, found {current_status or '<empty>'})"
            )
    if issues:
        raise RuntimeError("Fetch plan inputs changed since prepare-round. Rerun prepare-round. " + "; ".join(issues))


def mission_window(mission: dict[str, Any]) -> tuple[datetime, datetime]:
    window = ensure_object(mission.get("window"), "mission.window")
    start = parse_datetime(window.get("start_utc"))
    end = parse_datetime(window.get("end_utc"))
    if start is None or end is None:
        raise ValueError("mission.window must include valid start_utc and end_utc.")
    return (start, end)


def mission_region(mission: dict[str, Any]) -> dict[str, Any]:
    return ensure_object(mission.get("region"), "mission.region")


def region_label(mission: dict[str, Any]) -> str:
    return maybe_text(mission_region(mission).get("label")) or "Mission region"


def geometry_center(geometry: dict[str, Any]) -> tuple[float, float]:
    geometry_type = maybe_text(geometry.get("type"))
    if geometry_type == "Point":
        latitude = maybe_number(geometry.get("latitude"))
        longitude = maybe_number(geometry.get("longitude"))
        if latitude is None or longitude is None:
            return (0.0, 0.0)
        return (float(latitude), float(longitude))
    if geometry_type == "BBox":
        min_lat = maybe_number(geometry.get("south"))
        max_lat = maybe_number(geometry.get("north"))
        min_lon = maybe_number(geometry.get("west"))
        max_lon = maybe_number(geometry.get("east"))
        if None in {min_lat, max_lat, min_lon, max_lon}:
            min_lat = maybe_number(geometry.get("min_latitude"))
            max_lat = maybe_number(geometry.get("max_latitude"))
            min_lon = maybe_number(geometry.get("min_longitude"))
            max_lon = maybe_number(geometry.get("max_longitude"))
        if None in {min_lat, max_lat, min_lon, max_lon}:
            return (0.0, 0.0)
        return ((float(min_lat) + float(max_lat)) / 2.0, (float(min_lon) + float(max_lon)) / 2.0)
    return (0.0, 0.0)


def infer_claim_type(text: str, selected_sources: list[str]) -> str:
    lowered = text.lower()
    for claim_type, keywords in CLAIM_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return claim_type
    if "regulationsgov-comments-fetch" in selected_sources or "regulationsgov-comment-detail-fetch" in selected_sources:
        return "policy-reaction"
    if "open-meteo-flood-fetch" in selected_sources:
        return "flood"
    if "openaq-data-fetch" in selected_sources or "open-meteo-air-quality-fetch" in selected_sources:
        return "air-pollution"
    if "nasa-firms-fire-fetch" in selected_sources:
        return "wildfire"
    if "open-meteo-historical-fetch" in selected_sources:
        return "heat"
    return "other"


def claim_phrase(claim_type: str, place_label: str, topic: str) -> tuple[str, str]:
    if claim_type == "flood":
        return (
            f"Public flood reports intensify around {place_label}",
            f"Residents and local coverage describe flooding and inundation around {place_label} during the mission window.",
        )
    if claim_type == "heat":
        return (
            f"Extreme heat concern rises around {place_label}",
            f"Public discussion describes unusually intense heat and heat stress around {place_label}.",
        )
    if claim_type == "smoke":
        return (
            f"Smoke concern grows around {place_label}",
            f"People describe smoke, haze, and sharp air-quality concern around {place_label}.",
        )
    if claim_type == "air-pollution":
        return (
            f"Air-quality concern rises around {place_label}",
            f"Public discussion describes pollution and degraded air quality around {place_label}.",
        )
    if claim_type == "wildfire":
        return (
            f"Wildfire concern grows around {place_label}",
            f"People report wildfire activity, burning, or fire spread around {place_label}.",
        )
    if claim_type == "drought":
        return (
            f"Drought concern builds around {place_label}",
            f"Public discussion describes drought, dryness, and water stress around {place_label}.",
        )
    if claim_type == "water-pollution":
        return (
            f"Water-pollution concern spreads around {place_label}",
            f"Public discussion describes contaminated water or water pollution affecting {place_label}.",
        )
    if claim_type == "policy-reaction":
        return (
            f"Public comment activity grows around an environmental rule affecting {place_label}",
            f"Commenters discuss policy, regulation, and public comment activity relevant to {place_label}.",
        )
    topic_text = topic or "environmental concern"
    return (
        f"Environmental concern grows around {place_label}",
        f"Public discussion mentions {topic_text} around {place_label} during the mission window.",
    )


def base_statement_text(scenario: dict[str, Any], mission: dict[str, Any]) -> tuple[str, str]:
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    return claim_phrase(claim_type, maybe_text(scenario.get("place_label")) or region_label(mission), maybe_text(scenario.get("public_topic")))


def scenario_mode_for_source(scenario: dict[str, Any], source_skill: str) -> str:
    overrides = scenario.get("source_overrides")
    if isinstance(overrides, dict):
        per_source = overrides.get(source_skill)
        if isinstance(per_source, dict):
            override_mode = maybe_text(per_source.get("mode"))
            if override_mode:
                return override_mode
    source_modes = scenario.get("source_modes")
    if isinstance(source_modes, dict):
        mode = maybe_text(source_modes.get(source_skill))
        if mode:
            return mode
    return maybe_text(scenario.get("mode")) or "support"


def source_override(scenario: dict[str, Any], source_skill: str) -> dict[str, Any]:
    overrides = scenario.get("source_overrides")
    if not isinstance(overrides, dict):
        return {}
    value = overrides.get(source_skill)
    if not isinstance(value, dict):
        return {}
    return value


def fault_list(scenario: dict[str, Any], key: str) -> set[str]:
    profile = scenario.get("fault_profile")
    if not isinstance(profile, dict):
        return set()
    value = profile.get(key)
    if not isinstance(value, list):
        return set()
    return {maybe_text(item) for item in value if maybe_text(item)}


def fault_number(scenario: dict[str, Any], key: str, default: float) -> float:
    profile = scenario.get("fault_profile")
    if not isinstance(profile, dict):
        return default
    value = maybe_number(profile.get(key))
    return float(value) if value is not None else default


def shifted_datetime(value: datetime, scenario: dict[str, Any]) -> datetime:
    return value + timedelta(hours=fault_number(scenario, "time_shift_hours", 0.0))


def shifted_coordinates(latitude: float, longitude: float, scenario: dict[str, Any]) -> tuple[float, float]:
    offset = fault_number(scenario, "coordinate_offset_degrees", 0.0)
    return (latitude + offset, longitude + offset)


def seed_for_source(scenario: dict[str, Any], source_skill: str) -> int:
    return stable_int(scenario.get("scenario_id"), scenario.get("seed"), source_skill)


def record_count_for_source(source_skill: str, mode: str, scenario: dict[str, Any]) -> int:
    override = source_override(scenario, source_skill)
    override_count = maybe_number(override.get("record_count"))
    if override_count is not None:
        return max(0, int(override_count))
    count = BASE_RECORD_COUNTS.get(source_skill, 3)
    if source_skill in fault_list(scenario, "degrade_sources"):
        count = max(1, count // 2)
    if mode == "sparse":
        count = min(count, 2)
    return max(0, count)


def text_snippets_for_source(scenario: dict[str, Any], source_skill: str) -> list[str]:
    override = source_override(scenario, source_skill)
    value = override.get("text_snippets")
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def slugify(value: Any, *, default: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", maybe_text(value).casefold())
    return "-".join(tokens[:8]) or default


def split_counts(total: int, bucket_count: int) -> list[int]:
    if total <= 0 or bucket_count <= 0:
        return []
    base, remainder = divmod(total, bucket_count)
    return [base + (1 if index < remainder else 0) for index in range(bucket_count)]


def date_labels(start: datetime, end: datetime, count: int, scenario: dict[str, Any]) -> list[str]:
    if count <= 0:
        return []
    shifted_start = shifted_datetime(start, scenario)
    shifted_end = shifted_datetime(end, scenario)
    if count == 1:
        return [shifted_start.date().isoformat()]
    span = max((shifted_end.date() - shifted_start.date()).days, 1)
    output: list[str] = []
    for index in range(count):
        day_offset = round(index * span / max(count - 1, 1))
        output.append((shifted_start.date() + timedelta(days=day_offset)).isoformat())
    return output


def datetime_labels(start: datetime, end: datetime, count: int, scenario: dict[str, Any]) -> list[str]:
    if count <= 0:
        return []
    shifted_start = shifted_datetime(start, scenario)
    shifted_end = shifted_datetime(end, scenario)
    if count == 1:
        return [to_rfc3339_z(shifted_start)]
    total_seconds = max((shifted_end - shifted_start).total_seconds(), 1.0)
    output: list[str] = []
    for index in range(count):
        ratio = index / max(count - 1, 1)
        point = shifted_start + timedelta(seconds=total_seconds * ratio)
        output.append(to_rfc3339_z(point))
    return output


def source_count(payload: Any, source_skill: str) -> int:
    if source_skill == "gdelt-doc-search" and isinstance(payload, dict):
        articles = payload.get("articles")
        return len(articles) if isinstance(articles, list) else 0
    if source_skill in RAW_GDELT_SOURCES and isinstance(payload, dict):
        downloaded_count = maybe_number(payload.get("downloaded_count"))
        if downloaded_count is not None:
            return int(downloaded_count)
        downloads = payload.get("downloads")
        return len(downloads) if isinstance(downloads, list) else 0
    if source_skill == "bluesky-cascade-fetch" and isinstance(payload, dict):
        seeds = payload.get("seed_posts")
        return len(seeds) if isinstance(seeds, list) else 0
    if source_skill in JSONL_SOURCES and isinstance(payload, list):
        return len(payload)
    if source_skill in {"airnow-hourly-obs-fetch", "nasa-firms-fire-fetch"} and isinstance(payload, dict):
        records = payload.get("records")
        return len(records) if isinstance(records, list) else 0
    if source_skill.startswith("open-meteo") and isinstance(payload, dict):
        records = payload.get("records")
        return len(records) if isinstance(records, list) else 0
    if source_skill == "openaq-data-fetch" and isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, dict) and isinstance(result.get("records"), list):
            return len(result["records"])
        if isinstance(payload.get("records"), list):
            return len(payload["records"])
    return 0


def empty_payload_for_source(source_skill: str, start: datetime, end: datetime) -> Any:
    if source_skill == "gdelt-doc-search":
        return {"articles": []}
    if source_skill in RAW_GDELT_SOURCES:
        return {
            "ok": True,
            "mode": "range",
            "selected_count": 0,
            "downloaded_count": 0,
            "skipped_count": 0,
            "downloads": [],
            "skipped": [],
        }
    if source_skill == "bluesky-cascade-fetch":
        return {"seed_posts": [], "threads": []}
    if source_skill in JSONL_SOURCES:
        return []
    if source_skill == "airnow-hourly-obs-fetch":
        return {
            "schema_version": SCHEMA_VERSION,
            "source_skill": source_skill,
            "generated_at_utc": utc_now_iso(),
            "request": {
                "start_datetime_utc": to_rfc3339_z(start),
                "end_datetime_utc": to_rfc3339_z(end),
                "parameter_names": [],
                "hour_count": 0,
                "hourly_file_urls": [],
            },
            "dry_run": False,
            "transport": {"attempted_files": 0, "successful_files": 0, "failed_files": 0},
            "validation_summary": {"ok": True, "total_issue_count": 0, "issues": [], "records_emitted": 0},
            "record_count": 0,
            "records": [],
            "artifacts": {},
        }
    if source_skill.startswith("open-meteo"):
        return {"records": []}
    if source_skill == "nasa-firms-fire-fetch":
        return {"records": [], "request": {"start_date": start.date().isoformat(), "end_date": end.date().isoformat()}}
    if source_skill == "openaq-data-fetch":
        return {"result": {"records": []}}
    return {}


def public_line_text(scenario: dict[str, Any], mission: dict[str, Any], source_skill: str, index: int) -> tuple[str, str]:
    snippets = text_snippets_for_source(scenario, source_skill)
    if snippets:
        text = snippets[index % len(snippets)]
        return (text, text)
    title, body = base_statement_text(scenario, mission)
    if source_skill == "youtube-video-search":
        return (f"Video: {title}", body)
    return (title, body)


__all__ = [
    "AIRNOW_AQI_KIND",
    "AIRNOW_PARAMETER_NAMES",
    "AIRNOW_UNITS",
    "BASE_RECORD_COUNTS",
    "CLAIM_KEYWORDS",
    "CLAIM_METRIC_RULES",
    "DAILY_METRICS",
    "DEFAULT_GDELT_BASE_URL",
    "DEFAULT_GDELT_PREVIEW_LINES",
    "GDELT_EXPECTED_COLUMNS",
    "GDELT_MEMBER_SUFFIX",
    "GDELT_ZIP_SUFFIX",
    "JSONL_SOURCES",
    "METRIC_UNITS",
    "MODE_VALUES",
    "PRESET_DIR",
    "PUBLIC_DOMAINS",
    "RAW_GDELT_SOURCES",
    "SCENARIO_KIND",
    "SCHEMA_VERSION",
    "SOURCE_METRIC_CATALOG",
    "SUPPORTED_CLAIM_TYPES",
    "SUPPORTED_SOURCES",
    "atomic_write_bytes_file",
    "atomic_write_text_file",
    "base_statement_text",
    "claim_phrase",
    "date_labels",
    "datetime_labels",
    "discover_round_ids",
    "empty_payload_for_source",
    "ensure_fetch_plan_inputs_match",
    "ensure_object",
    "exclusive_file_lock",
    "fetch_execution_path",
    "fetch_lock_path",
    "fetch_plan_path",
    "file_sha256",
    "file_snapshot",
    "fault_list",
    "fault_number",
    "geometry_center",
    "infer_claim_type",
    "maybe_number",
    "maybe_text",
    "mission_path",
    "mission_region",
    "mission_window",
    "moderator_derived_dir",
    "normalize_space",
    "parse_datetime",
    "pretty_json",
    "public_line_text",
    "read_json",
    "read_jsonl",
    "record_count_for_source",
    "region_label",
    "resolve_round_id",
    "round_dir",
    "round_directory_name",
    "scenario_mode_for_source",
    "seed_for_source",
    "shifted_coordinates",
    "shifted_datetime",
    "slugify",
    "source_count",
    "source_override",
    "source_selection_path",
    "split_counts",
    "stable_int",
    "tasks_path",
    "text_snippets_for_source",
    "to_rfc3339_z",
    "utc_now_iso",
    "write_bytes",
    "write_json",
    "write_jsonl",
    "write_text",
]

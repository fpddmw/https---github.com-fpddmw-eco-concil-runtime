"""Geometry and mission-window helpers for orchestration planning."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from eco_council_runtime.application import orchestration_prepare
from eco_council_runtime.application.orchestration.query_builders import task_inputs, unique_strings
from eco_council_runtime.domain.text import maybe_text

ensure_object = orchestration_prepare.ensure_object
mission_region = orchestration_prepare.mission_region
mission_window = orchestration_prepare.mission_window


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


__all__ = [
    "bbox_text_for_geometry",
    "center_point_for_geometry",
    "firms_source_for_window",
    "geometry_from_task_or_mission",
    "location_strings_for_geometry",
    "parse_utc_datetime",
    "to_date_text",
    "to_gdelt_datetime",
    "window_from_task_or_mission",
]

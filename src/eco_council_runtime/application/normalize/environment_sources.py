"""Environment-source normalization pipelines."""

from __future__ import annotations

import csv
import gzip
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import file_sha256, stable_hash
from eco_council_runtime.domain.normalize_semantics import (
    canonical_environment_metric,
    maybe_number,
    parse_loose_datetime,
    to_rfc3339_z,
)
from eco_council_runtime.domain.text import maybe_text

from .source_cache import (
    NORMALIZE_CACHE_VERSION,
    normalize_cache_path,
    parse_source_payload,
    read_cache_payload,
    write_cache_payload,
)

OPENAQ_TIME_KEYS = (
    "datetime",
    "date",
    "observed_at",
    "observedAt",
    "timestamp",
    "utc",
)
OPENAQ_VALUE_KEYS = ("value", "measurement", "concentration")
OPENAQ_LAT_KEYS = ("latitude", "lat")
OPENAQ_LON_KEYS = ("longitude", "lon", "lng")

AIRNOW_PARAMETER_METRIC_MAP = {
    "PM25": "pm2_5",
    "PM10": "pm10",
    "OZONE": "ozone",
    "NO2": "nitrogen_dioxide",
    "CO": "carbon_monoxide",
    "SO2": "sulphur_dioxide",
}
USGS_PARAMETER_METRIC_MAP = {
    "00060": "river_discharge",
    "00065": "gage_height",
}


def make_environment_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    signal_kind: str,
    metric: str,
    value: float | None,
    unit: str,
    observed_at_utc: str | None,
    window_start_utc: str | None,
    window_end_utc: str | None,
    latitude: float | None,
    longitude: float | None,
    bbox: dict[str, Any] | None,
    quality_flags: list[str],
    metadata: dict[str, Any],
    artifact_path: Path,
    record_locator: str,
    sha256_value: str,
    raw_obj: Any,
) -> dict[str, Any]:
    canonical_metric = canonical_environment_metric(metric)
    signal_hash = stable_hash(
        source_skill,
        canonical_metric,
        observed_at_utc or window_start_utc or record_locator,
        value,
        latitude,
        longitude,
    )
    return {
        "signal_id": f"envsig-{signal_hash[:12]}",
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "metric": canonical_metric,
        "value": value,
        "unit": unit or "unknown",
        "observed_at_utc": observed_at_utc,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "latitude": latitude,
        "longitude": longitude,
        "bbox": bbox,
        "quality_flags": quality_flags,
        "metadata": metadata,
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "sha256": sha256_value,
        "raw_json": raw_obj,
    }


def iter_open_meteo_signals(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    records = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        return signals
    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        latitude = maybe_number(record.get("latitude"))
        longitude = maybe_number(record.get("longitude"))
        for section_name, units_name in (("hourly", "hourly_units"), ("daily", "daily_units")):
            section = record.get(section_name)
            if not isinstance(section, dict):
                continue
            units = record.get(units_name) if isinstance(record.get(units_name), dict) else {}
            times = section.get("time") if isinstance(section.get("time"), list) else []
            for metric, series in section.items():
                if metric == "time" or not isinstance(series, list):
                    continue
                unit = maybe_text(units.get(metric)) or "unknown"
                for value_index, raw_value in enumerate(series):
                    numeric_value = maybe_number(raw_value)
                    if numeric_value is None:
                        continue
                    observed_at = parse_loose_datetime(times[value_index]) if value_index < len(times) else None
                    signals.append(
                        make_environment_signal(
                            run_id=run_id,
                            round_id=round_id,
                            source_skill=source_skill,
                            signal_kind=section_name,
                            metric=metric,
                            value=numeric_value,
                            unit=unit,
                            observed_at_utc=to_rfc3339_z(observed_at),
                            window_start_utc=None,
                            window_end_utc=None,
                            latitude=latitude,
                            longitude=longitude,
                            bbox=None,
                            quality_flags=(
                                ["modeled-background"]
                                if source_skill == "open-meteo-air-quality-fetch"
                                else ["hydrology-model"]
                                if source_skill == "open-meteo-flood-fetch"
                                else ["reanalysis-or-model"]
                            ),
                            metadata={
                                "section": section_name,
                                "timezone": maybe_text(record.get("timezone")),
                                "elevation": maybe_number(record.get("elevation")),
                                "record_index": record_index,
                            },
                            artifact_path=path,
                            record_locator=f"$.records[{record_index}].{section_name}.{metric}[{value_index}]",
                            sha256_value=sha256_value,
                            raw_obj=raw_value,
                        )
                    )
    return signals


def iter_nasa_firms_signals(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return signals
    rows = payload.get("records")
    if not isinstance(rows, list):
        return signals
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        signals.append(
            make_environment_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="nasa-firms-fire-fetch",
                signal_kind="fire-detection",
                metric="fire_detection",
                value=1.0,
                unit="count",
                observed_at_utc=maybe_text(row.get("_acquired_at_utc")),
                window_start_utc=maybe_text(row.get("_chunk_start_date")),
                window_end_utc=maybe_text(row.get("_chunk_end_date")),
                latitude=maybe_number(row.get("_latitude")),
                longitude=maybe_number(row.get("_longitude")),
                bbox=None,
                quality_flags=["satellite-detection"],
                metadata={
                    "confidence": maybe_text(row.get("confidence")),
                    "satellite": maybe_text(row.get("satellite")),
                    "instrument": maybe_text(row.get("instrument")),
                    "frp": maybe_number(row.get("frp")),
                },
                artifact_path=path,
                record_locator=f"$.records[{index}]",
                sha256_value=sha256_value,
                raw_obj=row,
            )
        )
    return signals


def unwrap_openaq_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and "result" in payload:
        return unwrap_openaq_payload(payload["result"])
    return payload


def extract_nested_value(row: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        current: Any = row
        ok = True
        for part in path.split("."):
            if not isinstance(current, dict):
                ok = False
                break
            current = current.get(part)
        if ok and current is not None:
            return current
    return None


def openaq_row_to_signal(
    row: dict[str, Any],
    *,
    path: Path,
    run_id: str,
    round_id: str,
    index: int,
    sha256_value: str,
) -> dict[str, Any] | None:
    metric = maybe_text(extract_nested_value(row, "parameter.name", "parameter", "parameterName", "metric", "name"))
    unit = maybe_text(extract_nested_value(row, "parameter.units", "unit", "units")) or "unknown"
    value = None
    for key in OPENAQ_VALUE_KEYS:
        value = maybe_number(extract_nested_value(row, key))
        if value is not None:
            break
    if value is None or not metric:
        return None
    timestamp_text = ""
    timestamp_candidate = extract_nested_value(row, "date.utc", "date.local")
    if timestamp_candidate is None:
        for key in OPENAQ_TIME_KEYS:
            timestamp_candidate = extract_nested_value(row, key)
            if timestamp_candidate is not None:
                break
    if timestamp_candidate is not None:
        timestamp_text = maybe_text(timestamp_candidate)
    coordinates = row.get("coordinates") if isinstance(row.get("coordinates"), dict) else {}
    latitude = maybe_number(coordinates.get("latitude"))
    longitude = maybe_number(coordinates.get("longitude"))
    if latitude is None:
        for key in OPENAQ_LAT_KEYS:
            latitude = maybe_number(extract_nested_value(row, key))
            if latitude is not None:
                break
    if longitude is None:
        for key in OPENAQ_LON_KEYS:
            longitude = maybe_number(extract_nested_value(row, key))
            if longitude is not None:
                break
    metadata = {
        "location_id": extract_nested_value(row, "location.id", "locationId", "locationsId"),
        "location_name": maybe_text(extract_nested_value(row, "location.name", "location")),
        "sensor_id": extract_nested_value(row, "sensor.id", "sensorId", "sensorsId"),
        "provider": maybe_text(extract_nested_value(row, "provider.name", "provider")),
    }
    return make_environment_signal(
        run_id=run_id,
        round_id=round_id,
        source_skill="openaq-data-fetch",
        signal_kind="station-measurement",
        metric=metric,
        value=value,
        unit=unit,
        observed_at_utc=to_rfc3339_z(parse_loose_datetime(timestamp_text)),
        window_start_utc=None,
        window_end_utc=None,
        latitude=latitude,
        longitude=longitude,
        bbox=None,
        quality_flags=["station-observation"],
        metadata=metadata,
        artifact_path=path,
        record_locator=f"$[{index}]",
        sha256_value=sha256_value,
        raw_obj=row,
    )


def iter_csv_rows(path: Path) -> list[dict[str, str]]:
    open_func = gzip.open if path.suffix.lower() == ".gz" else open
    with open_func(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def iter_openaq_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    sha256_value = file_sha256(path)
    suffix = path.suffix.lower()
    rows: list[dict[str, Any]] = []
    if suffix in {".json", ".jsonl"}:
        payload = unwrap_openaq_payload(parse_source_payload(path))
        rows = [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
        if not rows and isinstance(payload, dict):
            records = payload.get("records")
            if isinstance(records, list):
                rows = [item for item in records if isinstance(item, dict)]
            output_path = maybe_text(payload.get("output_path"))
            if not rows and output_path:
                nested_path = Path(output_path).expanduser().resolve()
                if nested_path.exists():
                    return iter_openaq_signals(nested_path, run_id=run_id, round_id=round_id)
    elif suffix in {".csv", ".gz"}:
        rows = iter_csv_rows(path)
    else:
        raise ValueError(f"Unsupported OpenAQ artifact path: {path}")

    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        normalized = openaq_row_to_signal(
            row,
            path=path,
            run_id=run_id,
            round_id=round_id,
            index=index,
            sha256_value=sha256_value,
        )
        if normalized is not None:
            signals.append(normalized)
    return signals


def iter_airnow_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = parse_source_payload(path)
    rows = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []

    sha256_value = file_sha256(path)
    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        parameter_name = maybe_text(row.get("parameter_name")).upper()
        metric_base = AIRNOW_PARAMETER_METRIC_MAP.get(parameter_name)
        if not metric_base:
            continue
        latitude = maybe_number(row.get("latitude"))
        longitude = maybe_number(row.get("longitude"))
        observed_at_utc = to_rfc3339_z(parse_loose_datetime(row.get("observed_at_utc")))
        metadata = {
            "aqsid": maybe_text(row.get("aqsid")),
            "site_name": maybe_text(row.get("site_name")),
            "status": maybe_text(row.get("status")),
            "epa_region": maybe_text(row.get("epa_region")),
            "country_code": maybe_text(row.get("country_code")),
            "state_name": maybe_text(row.get("state_name")),
            "data_source": maybe_text(row.get("data_source")),
            "reporting_areas": row.get("reporting_areas") if isinstance(row.get("reporting_areas"), list) else [],
            "aqi_kind": maybe_text(row.get("aqi_kind")),
            "measured": row.get("measured"),
            "source_file_url": maybe_text(row.get("source_file_url")),
        }
        raw_concentration = maybe_number(row.get("raw_concentration"))
        if raw_concentration is not None:
            signals.append(
                make_environment_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="airnow-hourly-obs-fetch",
                    signal_kind="station-measurement",
                    metric=metric_base,
                    value=raw_concentration,
                    unit=maybe_text(row.get("unit")) or "unknown",
                    observed_at_utc=observed_at_utc,
                    window_start_utc=None,
                    window_end_utc=None,
                    latitude=latitude,
                    longitude=longitude,
                    bbox=None,
                    quality_flags=["station-observation", "preliminary", "airnow-file-product"],
                    metadata=metadata,
                    artifact_path=path,
                    record_locator=f"$.records[{index}].raw_concentration",
                    sha256_value=sha256_value,
                    raw_obj=row,
                )
            )
        aqi_value = maybe_number(row.get("aqi_value"))
        if aqi_value is not None:
            signals.append(
                make_environment_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="airnow-hourly-obs-fetch",
                    signal_kind="station-aqi",
                    metric=f"{metric_base}_aqi",
                    value=aqi_value,
                    unit="AQI",
                    observed_at_utc=observed_at_utc,
                    window_start_utc=None,
                    window_end_utc=None,
                    latitude=latitude,
                    longitude=longitude,
                    bbox=None,
                    quality_flags=["station-aqi", "preliminary", "airnow-file-product"],
                    metadata=metadata,
                    artifact_path=path,
                    record_locator=f"$.records[{index}].aqi_value",
                    sha256_value=sha256_value,
                    raw_obj=row,
                )
            )
    return signals


def iter_usgs_water_iv_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = parse_source_payload(path)
    rows = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []

    sha256_value = file_sha256(path)
    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        parameter_code = maybe_text(row.get("parameter_code"))
        metric = USGS_PARAMETER_METRIC_MAP.get(parameter_code)
        if not metric:
            continue
        value = maybe_number(row.get("value"))
        if value is None:
            continue
        latitude = maybe_number(row.get("latitude"))
        longitude = maybe_number(row.get("longitude"))
        observed_at_utc = to_rfc3339_z(parse_loose_datetime(row.get("observed_at_utc")))
        quality_flags = ["station-observation", "usgs-water-services-iv"]
        if bool(row.get("provisional")):
            quality_flags.append("provisional")
        metadata = {
            "site_number": maybe_text(row.get("site_number")),
            "site_name": maybe_text(row.get("site_name")),
            "agency_code": maybe_text(row.get("agency_code")),
            "site_type": maybe_text(row.get("site_type")),
            "state_code": maybe_text(row.get("state_code")),
            "county_code": maybe_text(row.get("county_code")),
            "huc_code": maybe_text(row.get("huc_code")),
            "parameter_code": parameter_code,
            "variable_name": maybe_text(row.get("variable_name")),
            "variable_description": maybe_text(row.get("variable_description")),
            "statistic_code": maybe_text(row.get("statistic_code")),
            "qualifiers": row.get("qualifiers") if isinstance(row.get("qualifiers"), list) else [],
            "source_query_url": maybe_text(row.get("source_query_url")),
        }
        signals.append(
            make_environment_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="usgs-water-iv-fetch",
                signal_kind="station-measurement",
                metric=metric,
                value=value,
                unit=maybe_text(row.get("unit")) or "unknown",
                observed_at_utc=observed_at_utc,
                window_start_utc=None,
                window_end_utc=None,
                latitude=latitude,
                longitude=longitude,
                bbox=None,
                quality_flags=quality_flags,
                metadata=metadata,
                artifact_path=path,
                record_locator=f"$.records[{index}].value",
                sha256_value=sha256_value,
                raw_obj=row,
            )
        )
    return signals


def normalize_environment_source(
    source_skill: str,
    path: Path,
    *,
    run_id: str,
    round_id: str,
    schema_version: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signals: list[dict[str, Any]] = []
    extra_observations: list[dict[str, Any]] = []
    if source_skill in {"open-meteo-historical-fetch", "open-meteo-air-quality-fetch", "open-meteo-flood-fetch"}:
        sha256_value = file_sha256(path)
        payload = parse_source_payload(path)
        signals = iter_open_meteo_signals(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    elif source_skill == "nasa-firms-fire-fetch":
        sha256_value = file_sha256(path)
        payload = parse_source_payload(path)
        signals = iter_nasa_firms_signals(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
        if isinstance(payload, dict) and isinstance(payload.get("records"), list) and not payload.get("records"):
            extra_observations.append(
                {
                    "schema_version": schema_version,
                    "observation_id": "obs-placeholder",
                    "run_id": run_id,
                    "round_id": round_id,
                    "agent_role": "environmentalist",
                    "source_skill": "nasa-firms-fire-fetch",
                    "metric": "fire_detection_count",
                    "aggregation": "event-count",
                    "value": 0.0,
                    "unit": "count",
                    "statistics": {"min": 0.0, "max": 0.0, "mean": 0.0, "p95": 0.0},
                    "time_window": {
                        "start_utc": maybe_text((payload.get("request") or {}).get("start_date")) or utc_now_iso(),
                        "end_utc": maybe_text((payload.get("request") or {}).get("end_date")) or utc_now_iso(),
                    },
                    "place_scope": {"label": "Mission region", "geometry": {"type": "Point", "latitude": 0.0, "longitude": 0.0}},
                    "quality_flags": ["satellite-detection", "zero-detections"],
                    "provenance": {
                        "source_skill": "nasa-firms-fire-fetch",
                        "artifact_path": str(path),
                        "sha256": sha256_value,
                    },
                }
            )
    elif source_skill == "openaq-data-fetch":
        signals = iter_openaq_signals(path, run_id=run_id, round_id=round_id)
    elif source_skill == "airnow-hourly-obs-fetch":
        signals = iter_airnow_signals(path, run_id=run_id, round_id=round_id)
    elif source_skill == "usgs-water-iv-fetch":
        signals = iter_usgs_water_iv_signals(path, run_id=run_id, round_id=round_id)
    else:
        raise ValueError(f"Unsupported environment source skill: {source_skill}")
    return signals, extra_observations


def normalize_environment_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    run_id: str,
    round_id: str,
    schema_version: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    artifact_sha256 = file_sha256(path)
    cache_path = normalize_cache_path(
        run_dir,
        domain="environment",
        source_skill=source_skill,
        run_id=run_id,
        round_id=round_id,
        artifact_sha256=artifact_sha256,
    )
    cached = read_cache_payload(cache_path)
    if isinstance(cached, dict):
        signals = cached.get("signals")
        extra_observations = cached.get("extra_observations")
        if (
            cached.get("cache_version") == NORMALIZE_CACHE_VERSION
            and cached.get("artifact_sha256") == artifact_sha256
            and isinstance(signals, list)
            and isinstance(extra_observations, list)
        ):
            return (
                [item for item in signals if isinstance(item, dict)],
                [item for item in extra_observations if isinstance(item, dict)],
                "hit",
            )

    signals, extra_observations = normalize_environment_source(
        source_skill,
        path,
        run_id=run_id,
        round_id=round_id,
        schema_version=schema_version,
    )
    write_cache_payload(
        cache_path,
        {
            "cache_version": NORMALIZE_CACHE_VERSION,
            "domain": "environment",
            "source_skill": source_skill,
            "run_id": run_id,
            "round_id": round_id,
            "artifact_path": str(path),
            "artifact_sha256": artifact_sha256,
            "signals": signals,
            "extra_observations": extra_observations,
        },
    )
    return signals, extra_observations, "miss"


__all__ = [
    "AIRNOW_PARAMETER_METRIC_MAP",
    "OPENAQ_LAT_KEYS",
    "OPENAQ_LON_KEYS",
    "OPENAQ_TIME_KEYS",
    "OPENAQ_VALUE_KEYS",
    "USGS_PARAMETER_METRIC_MAP",
    "extract_nested_value",
    "iter_airnow_signals",
    "iter_csv_rows",
    "iter_nasa_firms_signals",
    "iter_open_meteo_signals",
    "iter_openaq_signals",
    "iter_usgs_water_iv_signals",
    "make_environment_signal",
    "normalize_environment_source",
    "normalize_environment_source_cached",
    "openaq_row_to_signal",
    "unwrap_openaq_payload",
]

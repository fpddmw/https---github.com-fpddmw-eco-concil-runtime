"""Synthetic payload builders for deterministic simulation workflows."""

from __future__ import annotations

import random
from datetime import timedelta
from pathlib import Path
from typing import Any

from .common import (
    AIRNOW_AQI_KIND,
    AIRNOW_PARAMETER_NAMES,
    AIRNOW_UNITS,
    CLAIM_METRIC_RULES,
    DAILY_METRICS,
    METRIC_UNITS,
    PUBLIC_DOMAINS,
    RAW_GDELT_SOURCES,
    SCHEMA_VERSION,
    SOURCE_METRIC_CATALOG,
    date_labels,
    datetime_labels,
    empty_payload_for_source,
    ensure_object,
    fault_list,
    geometry_center,
    maybe_number,
    maybe_text,
    mission_region,
    mission_window,
    parse_datetime,
    public_line_text,
    read_json,
    read_jsonl,
    record_count_for_source,
    region_label,
    scenario_mode_for_source,
    seed_for_source,
    shifted_coordinates,
    slugify,
    source_count,
    source_override,
    to_rfc3339_z,
    utc_now_iso,
)
from .raw_artifacts import generate_raw_gdelt_manifest, gdelt_datetime_text


def generate_gdelt_payload(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    count = record_count_for_source(source_skill, mode, scenario)
    times = [parse_datetime(item) or start for item in datetime_labels(start, end, count, scenario)]
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    domain = PUBLIC_DOMAINS.get(claim_type, PUBLIC_DOMAINS["other"])
    articles = []
    for index, seen_at in enumerate(times):
        title, body = public_line_text(scenario, mission, source_skill, index)
        articles.append(
            {
                "title": title,
                "url": f"https://{domain}/article/{maybe_text(scenario.get('scenario_id'))}/{index + 1}",
                "domain": domain,
                "language": "English",
                "sourcelang": "English",
                "seendate": gdelt_datetime_text(seen_at),
                "description": body,
            }
        )
    return {"articles": articles, "simulation": {"scenario_id": maybe_text(scenario.get("scenario_id")), "mode": mode}}


def generate_bluesky_payload(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    count = record_count_for_source(source_skill, mode, scenario)
    timestamps = datetime_labels(start, end, count, scenario)
    seed_posts: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        title, body = public_line_text(scenario, mission, source_skill, index)
        uri = f"at://did:plc:sim{seed_for_source(scenario, source_skill) % 100000}/app.bsky.feed.post/{index + 1}"
        parent_uri = seed_posts[0]["uri"] if index >= 3 and seed_posts else ""
        seed_posts.append(
            {
                "uri": uri,
                "cid": f"sim-cid-{index + 1}",
                "text": f"{title}. {body}",
                "author_handle": f"user{index + 1}.example",
                "author_did": f"did:plc:sim{index + 1}",
                "created_at": timestamp,
                "timestamp_utc": timestamp,
                "reply_count": 1 if parent_uri else 0,
                "repost_count": 2 + index,
                "like_count": 8 + index * 3,
                "quote_count": index % 2,
                "reply_parent_uri": parent_uri,
                "reply_root_uri": seed_posts[0]["uri"] if parent_uri else "",
                "langs": ["en"],
            }
        )
    return {"seed_posts": seed_posts, "threads": [], "simulation": {"scenario_id": maybe_text(scenario.get("scenario_id")), "mode": mode}}


def youtube_video_ids_from_artifact(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = read_jsonl(path) if path.suffix.lower() == ".jsonl" else read_json(path)
    records = payload if isinstance(payload, list) else []
    output: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        video_id = maybe_text(record.get("video_id"))
        if video_id:
            output.append(video_id)
    return output


def generate_youtube_video_records(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> list[dict[str, Any]]:
    start, end = mission_window(mission)
    count = record_count_for_source(source_skill, mode, scenario)
    timestamps = datetime_labels(start, end, count, scenario)
    records: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        title, body = public_line_text(scenario, mission, source_skill, index)
        video_id = f"sim-video-{seed_for_source(scenario, source_skill) % 10000}-{index + 1}"
        records.append(
            {
                "video_id": video_id,
                "query": maybe_text(scenario.get("public_topic")) or maybe_text(mission.get("topic")),
                "video": {
                    "id": video_id,
                    "title": title,
                    "description": body,
                    "channel_title": f"Sim Channel {index + 1}",
                    "published_at": timestamp,
                    "statistics": {
                        "view_count": 1200 + index * 250,
                        "like_count": 55 + index * 10,
                        "comment_count": 14 + index * 3,
                    },
                    "default_language": "en",
                },
            }
        )
    return records


def generate_youtube_comment_records(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    dependency_artifacts: dict[str, Path],
) -> list[dict[str, Any]]:
    start, end = mission_window(mission)
    count = record_count_for_source(source_skill, mode, scenario)
    timestamps = datetime_labels(start, end, count, scenario)
    video_ids = youtube_video_ids_from_artifact(dependency_artifacts.get("youtube-video-search", Path("/__missing__")))
    if not video_ids:
        video_ids = [f"sim-video-{seed_for_source(scenario, source_skill) % 10000}-1"]
    records: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        title, body = public_line_text(scenario, mission, source_skill, index)
        video_id = video_ids[index % len(video_ids)]
        comment_id = f"sim-comment-{index + 1}"
        parent_comment_id = records[0]["comment_id"] if index >= 4 and records else ""
        records.append(
            {
                "comment_id": comment_id,
                "video_id": video_id,
                "text_original": f"{title}. {body}",
                "text_display": f"{title}. {body}",
                "author_display_name": f"Commenter {index + 1}",
                "published_at": timestamp,
                "like_count": 2 + index,
                "thread_id": f"thread-{video_id}",
                "parent_comment_id": parent_comment_id,
                "comment_type": "reply" if parent_comment_id else "comment",
                "source": {"search_terms": maybe_text(scenario.get("public_topic")) or maybe_text(mission.get("topic"))},
            }
        )
    return records


def reggov_comment_ids_from_artifact(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = read_jsonl(path) if path.suffix.lower() == ".jsonl" else read_json(path)
    records = payload if isinstance(payload, list) else []
    output: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        comment_id = maybe_text(record.get("id") or record.get("comment_id"))
        if comment_id:
            output.append(comment_id)
    return output


def reggov_resource(*, mission: dict[str, Any], scenario: dict[str, Any], index: int, comment_id: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    count = max(record_count_for_source("regulationsgov-comments-fetch", "support", scenario), 1)
    timestamp = datetime_labels(start, end, count, scenario)[index % count]
    title, body = public_line_text(scenario, mission, "regulationsgov-comments-fetch", index)
    return {
        "id": comment_id,
        "attributes": {
            "comment": f"{body} This public comment references policy, regulation, and agency review.",
            "title": title,
            "postedDate": timestamp,
            "lastModifiedDate": timestamp,
            "docketId": f"EPA-SIM-{seed_for_source(scenario, 'regulationsgov-comments-fetch') % 1000:03d}",
            "documentType": "Public Comment",
            "agencyId": "EPA",
            "organization": f"Simulated Organization {index + 1}",
            "subject": title,
        },
        "links": {"self": f"https://example.invalid/regulations/{comment_id}"},
    }


def generate_reggov_records(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    dependency_artifacts: dict[str, Path],
) -> list[dict[str, Any]]:
    count = record_count_for_source(source_skill, mode, scenario)
    dependency_ids = reggov_comment_ids_from_artifact(dependency_artifacts.get("regulationsgov-comments-fetch", Path("/__missing__")))
    if not dependency_ids:
        dependency_ids = [f"SIM-CMT-{index + 1:03d}" for index in range(max(count, 1))]
    records: list[dict[str, Any]] = []
    for index in range(count):
        comment_id = dependency_ids[index % len(dependency_ids)]
        resource = reggov_resource(mission=mission, scenario=scenario, index=index, comment_id=comment_id)
        if source_skill == "regulationsgov-comment-detail-fetch":
            records.append(
                {
                    "comment_id": comment_id,
                    "detail": {"data": resource},
                    "response_url": resource["links"]["self"],
                    "validation": {"ok": True},
                }
            )
        else:
            records.append(resource)
    return records


def available_metrics_for_source(source_skill: str, claim_type: str, mode: str, scenario: dict[str, Any]) -> list[str]:
    override = source_override(scenario, source_skill)
    metric_values = override.get("metric_values")
    if isinstance(metric_values, dict):
        metrics = [maybe_text(key) for key in metric_values.keys() if maybe_text(key)]
        if metrics:
            return metrics
    available = list(SOURCE_METRIC_CATALOG.get(source_skill, ()))
    if not available:
        return []
    rules = CLAIM_METRIC_RULES.get(claim_type)
    if rules is None:
        return available[:1]
    support_metrics = [metric for metric in available if metric in rules.get("support", {})]
    contradict_metrics = [metric for metric in available if metric in rules.get("contradict", {})]
    if mode == "support":
        return support_metrics[:2] or available[:1]
    if mode == "contradict":
        return contradict_metrics[:2] or support_metrics[:1] or available[:1]
    if mode == "mixed":
        combined: list[str] = []
        for metric in support_metrics[:1] + contradict_metrics[:1]:
            if metric and metric not in combined:
                combined.append(metric)
        return combined or support_metrics[:1] or contradict_metrics[:1] or available[:1]
    if mode == "sparse":
        return support_metrics[:1] or available[:1]
    return available[:1]


def metric_value_for_mode(claim_type: str, metric: str, mode: str) -> float:
    rules = CLAIM_METRIC_RULES.get(claim_type, {"support": {}, "contradict": {}})
    support_value = maybe_number(rules.get("support", {}).get(metric))
    contradict_value = maybe_number(rules.get("contradict", {}).get(metric))
    if mode == "support":
        if metric == "fire_detection_count":
            return 3.0
        if claim_type == "drought" and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"} and support_value is not None:
            return round(support_value * 0.5, 3)
        if support_value is not None:
            return round(support_value * 1.35, 3)
        return 10.0
    if mode == "contradict":
        if metric == "fire_detection_count":
            return 0.0
        if claim_type in {"wildfire", "drought"} and metric in {"precipitation_sum", "relative_humidity_2m", "soil_moisture_0_to_7cm"}:
            base = contradict_value if contradict_value is not None else support_value
            if base is not None:
                return round(base * 1.3, 3)
        if contradict_value is not None:
            return round(contradict_value * 0.7 if contradict_value > 0 else contradict_value, 3)
        if support_value is not None:
            return round(support_value * 0.5, 3)
        return 2.0
    if mode == "mixed":
        if support_value is not None and contradict_value is not None and support_value != contradict_value:
            return round((support_value + contradict_value) / 2.0, 3)
        return metric_value_for_mode(claim_type, metric, "support")
    if mode == "sparse":
        if support_value is not None and contradict_value is not None:
            return round((support_value + contradict_value) / 2.0, 3)
        if support_value is not None:
            return round(support_value * 0.95, 3)
        return 5.0
    return 1.0


def coerce_series(values: Any, *, count: int, fallback: float, rng: random.Random) -> list[float]:
    if isinstance(values, list):
        output = [float(item) for item in values if maybe_number(item) is not None]
        if output:
            return output
    scalar = maybe_number(values)
    base = float(scalar) if scalar is not None else fallback
    if count <= 1:
        return [round(base, 3)]
    spread = abs(base) * 0.12
    if spread < 0.5:
        spread = 0.5
    output: list[float] = []
    for index in range(count):
        position = (index - (count - 1) / 2.0) / max(count - 1, 1)
        jitter = rng.uniform(-spread * 0.05, spread * 0.05)
        value = base + spread * position + jitter
        if base >= 0:
            value = max(0.0, value)
        output.append(round(value, 3))
    return output


def metric_series(
    *,
    source_skill: str,
    claim_type: str,
    metric: str,
    mode: str,
    count: int,
    scenario: dict[str, Any],
    rng: random.Random,
) -> list[float]:
    override = source_override(scenario, source_skill)
    metric_values = override.get("metric_values")
    if isinstance(metric_values, dict) and metric in metric_values:
        return coerce_series(metric_values[metric], count=count, fallback=metric_value_for_mode(claim_type, metric, mode), rng=rng)
    if mode == "mixed":
        rules = CLAIM_METRIC_RULES.get(claim_type, {"support": {}, "contradict": {}})
        support_value = maybe_number(rules.get("support", {}).get(metric))
        contradict_value = maybe_number(rules.get("contradict", {}).get(metric))
        if support_value is not None and contradict_value is not None and count >= 2:
            raw = [metric_value_for_mode(claim_type, metric, "contradict"), metric_value_for_mode(claim_type, metric, "support")]
            if count > 2:
                raw.insert(1, round((raw[0] + raw[1]) / 2.0, 3))
            return coerce_series(raw[:count], count=count, fallback=raw[-1], rng=rng)
    return coerce_series(None, count=count, fallback=metric_value_for_mode(claim_type, metric, mode), rng=rng)


def generate_open_meteo_payload(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    geometry = ensure_object(mission_region(mission).get("geometry"), "mission.region.geometry")
    latitude, longitude = geometry_center(geometry)
    latitude, longitude = shifted_coordinates(latitude, longitude, scenario)
    rng = random.Random(seed_for_source(scenario, source_skill))
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    metrics = available_metrics_for_source(source_skill, claim_type, mode, scenario)
    if source_skill in fault_list(scenario, "degrade_sources") and len(metrics) > 1:
        metrics = metrics[:1]
    point_count = max(1, record_count_for_source(source_skill, mode, scenario))
    record: dict[str, Any] = {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "timezone": "GMT",
        "elevation": 10.0,
    }
    hourly_metrics = [metric for metric in metrics if metric not in DAILY_METRICS]
    daily_metrics = [metric for metric in metrics if metric in DAILY_METRICS]
    if source_skill == "open-meteo-flood-fetch" and not daily_metrics:
        daily_metrics = metrics
        hourly_metrics = []
    if hourly_metrics:
        hourly_time = datetime_labels(start, end, point_count, scenario)
        hourly = {"time": hourly_time}
        hourly_units: dict[str, str] = {}
        for metric in hourly_metrics:
            hourly[metric] = metric_series(
                source_skill=source_skill,
                claim_type=claim_type,
                metric=metric,
                mode=mode,
                count=len(hourly_time),
                scenario=scenario,
                rng=rng,
            )
            hourly_units[metric] = METRIC_UNITS.get(metric, "unknown")
        record["hourly"] = hourly
        record["hourly_units"] = hourly_units
    if daily_metrics:
        daily_time = date_labels(start, end, point_count, scenario)
        daily = {"time": daily_time}
        daily_units: dict[str, str] = {}
        for metric in daily_metrics:
            daily[metric] = metric_series(
                source_skill=source_skill,
                claim_type=claim_type,
                metric=metric,
                mode=mode,
                count=len(daily_time),
                scenario=scenario,
                rng=rng,
            )
            daily_units[metric] = METRIC_UNITS.get(metric, "unknown")
        record["daily"] = daily
        record["daily_units"] = daily_units
    return {"records": [record], "simulation": {"scenario_id": maybe_text(scenario.get("scenario_id")), "mode": mode}}


def generate_nasa_firms_payload(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    geometry = ensure_object(mission_region(mission).get("geometry"), "mission.region.geometry")
    latitude, longitude = geometry_center(geometry)
    latitude, longitude = shifted_coordinates(latitude, longitude, scenario)
    override = source_override(scenario, source_skill)
    override_count = maybe_number(override.get("fire_count"))
    if override_count is not None:
        count = max(0, int(override_count))
    elif mode == "support":
        count = 3
    elif mode == "mixed":
        count = 1
    elif mode == "sparse":
        count = 1
    else:
        count = 0
    if source_skill in fault_list(scenario, "degrade_sources"):
        count = min(count, 1)
    timestamps = datetime_labels(start, end, max(count, 1), scenario)
    records: list[dict[str, Any]] = []
    for index in range(count):
        records.append(
            {
                "_acquired_at_utc": timestamps[index % len(timestamps)],
                "_chunk_start_date": start.date().isoformat(),
                "_chunk_end_date": end.date().isoformat(),
                "_latitude": round(latitude + index * 0.01, 6),
                "_longitude": round(longitude + index * 0.01, 6),
                "confidence": "n",
                "satellite": "NOAA-20",
                "instrument": "VIIRS",
                "frp": round(25.0 + index * 8.0, 2),
            }
        )
    return {"records": records, "request": {"start_date": start.date().isoformat(), "end_date": end.date().isoformat()}}


def generate_openaq_payload(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    geometry = ensure_object(mission_region(mission).get("geometry"), "mission.region.geometry")
    latitude, longitude = geometry_center(geometry)
    latitude, longitude = shifted_coordinates(latitude, longitude, scenario)
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    rng = random.Random(seed_for_source(scenario, source_skill))
    metrics = available_metrics_for_source(source_skill, claim_type, mode, scenario)
    if mode == "mixed" and "pm10" not in metrics and "pm2_5" in metrics:
        metrics.append("pm10")
    if source_skill in fault_list(scenario, "degrade_sources") and len(metrics) > 1:
        metrics = metrics[:1]
    sample_count = max(1, record_count_for_source(source_skill, mode, scenario))
    times = datetime_labels(start, end, sample_count, scenario)
    rows: list[dict[str, Any]] = []
    for metric_index, metric in enumerate(metrics or ["pm2_5"]):
        values = metric_series(
            source_skill=source_skill,
            claim_type=claim_type,
            metric=metric,
            mode=mode if mode != "mixed" or metric_index == 0 else "contradict",
            count=len(times),
            scenario=scenario,
            rng=rng,
        )
        for index, timestamp in enumerate(times):
            rows.append(
                {
                    "parameter": {"name": metric, "units": METRIC_UNITS.get(metric, "unknown")},
                    "value": values[index],
                    "date": {"utc": timestamp},
                    "coordinates": {"latitude": round(latitude, 6), "longitude": round(longitude, 6)},
                    "location": {"id": 1000 + metric_index, "name": f"Sim Station {metric_index + 1}"},
                    "sensor": {"id": 2000 + metric_index},
                    "provider": {"name": "Simulated OpenAQ"},
                }
            )
    return {"result": {"records": rows}, "simulation": {"scenario_id": maybe_text(scenario.get("scenario_id")), "mode": mode}}


def estimated_airnow_aqi(metric: str, value: float) -> int:
    factor = {
        "nitrogen_dioxide": 1.5,
        "ozone": 1.0,
        "pm10": 1.6,
        "pm2_5": 2.8,
    }.get(metric, 1.0)
    return max(0, min(500, int(round(value * factor))))


def generate_airnow_payload(*, mission: dict[str, Any], scenario: dict[str, Any], mode: str, source_skill: str) -> dict[str, Any]:
    start, end = mission_window(mission)
    geometry = ensure_object(mission_region(mission).get("geometry"), "mission.region.geometry")
    latitude, longitude = geometry_center(geometry)
    latitude, longitude = shifted_coordinates(latitude, longitude, scenario)
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    rng = random.Random(seed_for_source(scenario, source_skill))
    metrics = available_metrics_for_source(source_skill, claim_type, mode, scenario)
    if source_skill in fault_list(scenario, "degrade_sources") and len(metrics) > 1:
        metrics = metrics[:1]
    sample_count = max(1, record_count_for_source(source_skill, mode, scenario))
    timestamps = datetime_labels(start, end, sample_count, scenario)
    records: list[dict[str, Any]] = []
    for metric_index, metric in enumerate(metrics or ["pm2_5"]):
        parameter_name = AIRNOW_PARAMETER_NAMES.get(metric, "PM25")
        values = metric_series(
            source_skill=source_skill,
            claim_type=claim_type,
            metric=metric,
            mode=mode if mode != "mixed" or metric_index == 0 else "contradict",
            count=len(timestamps),
            scenario=scenario,
            rng=rng,
        )
        for index, timestamp in enumerate(timestamps):
            site_lat = round(latitude + metric_index * 0.01, 6)
            site_lon = round(longitude + metric_index * 0.01, 6)
            concentration = round(values[index], 3)
            records.append(
                {
                    "aqsid": f"sim-aqsid-{metric_index + 1:03d}",
                    "site_name": f"Sim AirNow Site {metric_index + 1}",
                    "status": "Active",
                    "epa_region": f"{(metric_index % 9) + 1:02d}",
                    "latitude": site_lat,
                    "longitude": site_lon,
                    "country_code": "US",
                    "state_name": "SimState",
                    "observed_at_utc": timestamp,
                    "data_source": "AIRNOW",
                    "reporting_areas": [maybe_text(mission_region(mission).get("label")) or "Sim Reporting Area"],
                    "parameter_name": parameter_name,
                    "aqi_value": estimated_airnow_aqi(metric, concentration),
                    "aqi_kind": AIRNOW_AQI_KIND.get(metric, "nowcast-aqi"),
                    "raw_concentration": concentration,
                    "unit": AIRNOW_UNITS.get(metric, METRIC_UNITS.get(metric, "unknown")),
                    "measured": True,
                    "source_file_url": f"https://files.airnowtech.org/airnow/sim/{maybe_text(scenario.get('scenario_id'))}/{parameter_name}/{index + 1}.dat",
                }
            )
    return {
        "schema_version": SCHEMA_VERSION,
        "source_skill": source_skill,
        "generated_at_utc": utc_now_iso(),
        "request": {
            "bbox": {
                "min_lon": round(longitude - 0.2, 6),
                "min_lat": round(latitude - 0.2, 6),
                "max_lon": round(longitude + 0.2, 6),
                "max_lat": round(latitude + 0.2, 6),
            },
            "start_datetime_utc": to_rfc3339_z(start),
            "end_datetime_utc": to_rfc3339_z(end),
            "parameter_names": [AIRNOW_PARAMETER_NAMES.get(metric, "PM25") for metric in metrics or ["pm2_5"]],
            "hour_count": len(timestamps),
            "hourly_file_urls": [item["source_file_url"] for item in records[: len(timestamps)]],
        },
        "dry_run": False,
        "transport": {
            "attempted_files": len(timestamps),
            "successful_files": len(timestamps),
            "failed_files": 0,
            "total_bytes": len(records) * 128,
            "total_retries": 0,
            "status_code_counts": {"200": len(timestamps)},
            "failures": [],
        },
        "validation_summary": {
            "ok": True,
            "total_issue_count": 0,
            "issues": [],
            "input_rows": len(records),
            "rows_bad_coordinates": 0,
            "rows_outside_bbox": 0,
            "rows_outside_window": 0,
            "parameter_candidates": len(records),
            "records_emitted": len(records),
        },
        "record_count": len(records),
        "records": records,
        "artifacts": {},
    }


def dependency_artifact_paths(statuses: list[dict[str, Any]]) -> dict[str, Path]:
    output: dict[str, Path] = {}
    for status in statuses:
        if not isinstance(status, dict):
            continue
        state = maybe_text(status.get("status"))
        reason = maybe_text(status.get("reason"))
        if state not in {"completed", "skipped"}:
            continue
        if state == "skipped" and reason != "artifact_exists":
            continue
        source_skill = maybe_text(status.get("source_skill"))
        artifact_path = maybe_text(status.get("artifact_path"))
        if not source_skill or not artifact_path:
            continue
        output[source_skill] = Path(artifact_path).expanduser().resolve()
    return output


def generate_payload_for_source(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    source_skill: str,
    mode: str,
    step: dict[str, Any],
    dependency_statuses: list[dict[str, Any]],
) -> Any:
    start, end = mission_window(mission)
    if source_skill in fault_list(scenario, "empty_sources"):
        return empty_payload_for_source(source_skill, start, end)
    dependency_artifacts = dependency_artifact_paths(dependency_statuses)
    if source_skill == "gdelt-doc-search":
        return generate_gdelt_payload(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    if source_skill in RAW_GDELT_SOURCES:
        return generate_raw_gdelt_manifest(
            mission=mission,
            scenario=scenario,
            mode=mode,
            source_skill=source_skill,
            step=step,
        )
    if source_skill == "bluesky-cascade-fetch":
        return generate_bluesky_payload(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    if source_skill == "youtube-video-search":
        return generate_youtube_video_records(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    if source_skill == "youtube-comments-fetch":
        return generate_youtube_comment_records(
            mission=mission,
            scenario=scenario,
            mode=mode,
            source_skill=source_skill,
            dependency_artifacts=dependency_artifacts,
        )
    if source_skill in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}:
        return generate_reggov_records(
            mission=mission,
            scenario=scenario,
            mode=mode,
            source_skill=source_skill,
            dependency_artifacts=dependency_artifacts,
        )
    if source_skill == "airnow-hourly-obs-fetch":
        return generate_airnow_payload(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    if source_skill in {"open-meteo-historical-fetch", "open-meteo-air-quality-fetch", "open-meteo-flood-fetch"}:
        return generate_open_meteo_payload(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    if source_skill == "nasa-firms-fire-fetch":
        return generate_nasa_firms_payload(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    if source_skill == "openaq-data-fetch":
        return generate_openaq_payload(mission=mission, scenario=scenario, mode=mode, source_skill=source_skill)
    raise ValueError(f"Unsupported source skill in simulator: {source_skill}")


__all__ = [
    "available_metrics_for_source",
    "coerce_series",
    "dependency_artifact_paths",
    "estimated_airnow_aqi",
    "generate_airnow_payload",
    "generate_bluesky_payload",
    "generate_gdelt_payload",
    "generate_nasa_firms_payload",
    "generate_open_meteo_payload",
    "generate_openaq_payload",
    "generate_payload_for_source",
    "generate_reggov_records",
    "generate_youtube_comment_records",
    "generate_youtube_video_records",
    "metric_series",
    "metric_value_for_mode",
    "reggov_comment_ids_from_artifact",
    "reggov_resource",
    "youtube_video_ids_from_artifact",
]

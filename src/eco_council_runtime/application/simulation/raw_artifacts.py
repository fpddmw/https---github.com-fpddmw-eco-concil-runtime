"""Raw artifact builders for deterministic simulation workflows."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import hashlib
import io
from pathlib import Path
from typing import Any
import zipfile

from .common import (
    DEFAULT_GDELT_BASE_URL,
    DEFAULT_GDELT_PREVIEW_LINES,
    GDELT_EXPECTED_COLUMNS,
    GDELT_MEMBER_SUFFIX,
    GDELT_ZIP_SUFFIX,
    PUBLIC_DOMAINS,
    date_labels,
    datetime_labels,
    geometry_center,
    maybe_text,
    mission_region,
    mission_window,
    public_line_text,
    record_count_for_source,
    region_label,
    seed_for_source,
    shifted_coordinates,
    shifted_datetime,
    slugify,
    split_counts,
    write_bytes,
)


def gdelt_topic_phrase(mission: dict[str, Any], scenario: dict[str, Any]) -> str:
    parts = [
        maybe_text(scenario.get("public_topic")),
        maybe_text(mission.get("topic")),
        maybe_text(mission.get("objective")),
    ]
    return " ".join(part for part in parts if part).strip()


def gdelt_place_label(mission: dict[str, Any], scenario: dict[str, Any]) -> str:
    return maybe_text(scenario.get("place_label")) or region_label(mission)


def gdelt_claim_tone(mode: str) -> float:
    if mode == "contradict":
        return 1.6
    if mode == "mixed":
        return -0.8
    if mode == "sparse":
        return -1.4
    return -3.8


def gdelt_goldstein_score(mode: str) -> float:
    if mode == "contradict":
        return 2.0
    if mode == "mixed":
        return -1.5
    if mode == "sparse":
        return -2.0
    return -5.0


def gdelt_event_base_code(claim_type: str) -> str:
    return {
        "air-pollution": "112",
        "drought": "023",
        "flood": "0233",
        "heat": "073",
        "policy-reaction": "010",
        "smoke": "112",
        "water-pollution": "043",
        "wildfire": "204",
    }.get(claim_type, "010")


def gdelt_datetime_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def gdelt_archive_filename(source_skill: str, timestamp: datetime) -> str:
    return gdelt_datetime_text(timestamp) + GDELT_ZIP_SUFFIX[source_skill]


def gdelt_member_name(source_skill: str, timestamp: datetime) -> str:
    return gdelt_datetime_text(timestamp) + GDELT_MEMBER_SUFFIX[source_skill]


def build_gdelt_zip_payload(*, member_name: str, rows: list[list[str]]) -> tuple[bytes, list[str]]:
    text_buffer = io.StringIO()
    writer = csv.writer(text_buffer, delimiter="\t", lineterminator="\n")
    for row in rows:
        writer.writerow(row)
    member_text = text_buffer.getvalue()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, member_text.encode("utf-8"))
    preview_lines = member_text.rstrip("\n").splitlines()[:DEFAULT_GDELT_PREVIEW_LINES]
    return (zip_buffer.getvalue(), preview_lines)


def gdelt_validation_summary(*, member_name: str, row_count: int, expected_columns: int) -> dict[str, Any]:
    return {
        "passed": True,
        "checked_member": member_name,
        "expected_columns": expected_columns,
        "scanned_lines": row_count,
        "scan_complete": True,
        "max_lines": row_count or 1,
        "empty_line_count": 0,
        "issue_count": 0,
        "decode_error_count": 0,
        "column_mismatch_count": 0,
        "issues": [],
        "quarantine_path": None,
    }


def gdelt_country_hint(place_label: str) -> str:
    parts = [part.strip() for part in maybe_text(place_label).split(",") if part.strip()]
    return parts[-1] if parts else place_label


def build_gdelt_events_rows(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    count: int,
    row_times: list[datetime],
    start_index: int,
) -> list[list[str]]:
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    place_label = gdelt_place_label(mission, scenario)
    topic_phrase = gdelt_topic_phrase(mission, scenario) or claim_type
    topic_slug = slugify(topic_phrase, default="topic")
    place_slug = slugify(place_label, default="place")
    domain = PUBLIC_DOMAINS.get(claim_type, PUBLIC_DOMAINS["other"])
    title_root = place_label.replace(",", "")
    latitude, longitude = shifted_coordinates(*geometry_center(mission_region(mission).get("geometry", {})), scenario)
    event_base_code = gdelt_event_base_code(claim_type)
    tone = gdelt_claim_tone(mode)
    rows: list[list[str]] = []
    for local_index in range(count):
        global_index = start_index + local_index
        timestamp = row_times[local_index]
        event_id = str((seed_for_source(scenario, source_skill) % 9_000_000) + global_index + 1)
        title, _body = public_line_text(scenario, mission, source_skill, global_index)
        source_url = f"https://{domain}/gdelt-events/{place_slug}/{topic_slug}/{global_index + 1}"
        row = [""] * GDELT_EXPECTED_COLUMNS[source_skill]
        row[0] = event_id
        row[1] = timestamp.strftime("%Y%m%d")
        row[6] = f"{title_root} monitors"
        row[16] = f"{topic_phrase} response"
        row[26] = event_base_code
        row[27] = event_base_code
        row[28] = event_base_code[:2]
        row[30] = f"{gdelt_goldstein_score(mode):.1f}"
        row[31] = str(5 + global_index)
        row[32] = str(2 + (global_index % 3))
        row[33] = str(2 + (global_index % 2))
        row[34] = f"{tone - (local_index * 0.2):.2f}"
        row[52] = place_label
        row[53] = gdelt_country_hint(place_label)
        row[56] = f"{latitude:.4f}"
        row[57] = f"{longitude:.4f}"
        row[59] = gdelt_datetime_text(timestamp)
        row[60] = source_url
        rows.append(row)
    return rows


def build_gdelt_mentions_rows(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    count: int,
    row_times: list[datetime],
    start_index: int,
) -> list[list[str]]:
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    place_label = gdelt_place_label(mission, scenario)
    topic_phrase = gdelt_topic_phrase(mission, scenario) or claim_type
    topic_slug = slugify(topic_phrase, default="topic")
    place_slug = slugify(place_label, default="place")
    domain = PUBLIC_DOMAINS.get(claim_type, PUBLIC_DOMAINS["other"])
    source_name = f"{place_label} {claim_type} bulletin".strip()
    tone = gdelt_claim_tone(mode)
    rows: list[list[str]] = []
    for local_index in range(count):
        global_index = start_index + local_index
        timestamp = row_times[local_index]
        event_id = str((seed_for_source(scenario, source_skill) % 9_000_000) + global_index + 1)
        identifier = f"https://{domain}/gdelt-mentions/{place_slug}/{topic_slug}/{global_index + 1}"
        row = [""] * GDELT_EXPECTED_COLUMNS[source_skill]
        row[0] = event_id
        row[1] = gdelt_datetime_text(timestamp - timedelta(minutes=30))
        row[2] = gdelt_datetime_text(timestamp)
        row[3] = "1"
        row[4] = source_name
        row[5] = identifier
        row[11] = str(70 + (local_index * 4))
        row[12] = str(900 + (local_index * 120))
        row[13] = f"{tone - (local_index * 0.15):.2f}"
        rows.append(row)
    return rows


def build_gdelt_gkg_rows(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    count: int,
    row_times: list[datetime],
    start_index: int,
) -> list[list[str]]:
    claim_type = maybe_text(scenario.get("claim_type")) or "other"
    place_label = gdelt_place_label(mission, scenario)
    topic_phrase = gdelt_topic_phrase(mission, scenario) or claim_type
    topic_slug = slugify(topic_phrase, default="topic")
    place_slug = slugify(place_label, default="place")
    domain = PUBLIC_DOMAINS.get(claim_type, PUBLIC_DOMAINS["other"])
    source_common_name = f"{place_label.replace(',', '')} desk".strip()
    latitude, longitude = shifted_coordinates(*geometry_center(mission_region(mission).get("geometry", {})), scenario)
    tone = gdelt_claim_tone(mode)
    topic_theme = slugify(topic_phrase, default=claim_type).replace("-", "_").upper()
    claim_theme = slugify(claim_type, default="environment").replace("-", "_").upper()
    location_text = f"1#{place_label}###{place_slug}#{latitude:.4f}#{longitude:.4f}"
    rows: list[list[str]] = []
    for local_index in range(count):
        global_index = start_index + local_index
        timestamp = row_times[local_index]
        document_identifier = f"https://{domain}/gdelt-gkg/{place_slug}/{topic_slug}/{global_index + 1}"
        row = [""] * GDELT_EXPECTED_COLUMNS[source_skill]
        row[0] = f"{gdelt_datetime_text(timestamp)}-{global_index + 1}"
        row[1] = gdelt_datetime_text(timestamp)
        row[3] = source_common_name
        row[4] = document_identifier
        row[8] = ";".join(item for item in (f"ENV_{claim_theme}", topic_theme, "ENVIRONMENT") if item)
        row[10] = location_text
        row[12] = f"{place_label.replace(',', '')} analyst"
        row[14] = f"{place_label.replace(',', '')} council;{claim_type.replace('-', ' ')} desk"
        row[15] = f"{tone - (local_index * 0.1):.2f},0,0,0,0,0,0"
        row[23] = ";".join(item for item in (place_label, maybe_text(mission.get("topic")), maybe_text(mission.get("objective"))) if item)
        rows.append(row)
    return rows


def build_gdelt_rows(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    count: int,
    row_times: list[datetime],
    start_index: int,
) -> list[list[str]]:
    if source_skill == "gdelt-events-fetch":
        return build_gdelt_events_rows(
            mission=mission,
            scenario=scenario,
            mode=mode,
            source_skill=source_skill,
            count=count,
            row_times=row_times,
            start_index=start_index,
        )
    if source_skill == "gdelt-mentions-fetch":
        return build_gdelt_mentions_rows(
            mission=mission,
            scenario=scenario,
            mode=mode,
            source_skill=source_skill,
            count=count,
            row_times=row_times,
            start_index=start_index,
        )
    return build_gdelt_gkg_rows(
        mission=mission,
        scenario=scenario,
        mode=mode,
        source_skill=source_skill,
        count=count,
        row_times=row_times,
        start_index=start_index,
    )


def raw_gdelt_download_dir(step: dict[str, Any]) -> Path:
    configured = maybe_text(step.get("download_dir"))
    if configured:
        return Path(configured).expanduser().resolve()
    artifact_path = Path(maybe_text(step.get("artifact_path"))).expanduser().resolve()
    return artifact_path.parent / artifact_path.stem


def generate_raw_gdelt_manifest(
    *,
    mission: dict[str, Any],
    scenario: dict[str, Any],
    mode: str,
    source_skill: str,
    step: dict[str, Any],
) -> dict[str, Any]:
    start, end = mission_window(mission)
    row_count = record_count_for_source(source_skill, mode, scenario)
    if row_count <= 0:
        return {
            "ok": True,
            "mode": "range",
            "selected_count": 0,
            "downloaded_count": 0,
            "skipped_count": 0,
            "downloads": [],
            "skipped": [],
        }

    download_dir = raw_gdelt_download_dir(step)
    file_count = 1 if row_count <= 2 or mode == "sparse" else 2
    file_count = min(file_count, row_count)
    file_timestamps = [
        datetime.fromisoformat(label.replace("Z", "+00:00")) if label.endswith("Z") else shifted_datetime(start, scenario)
        for label in datetime_labels(start, end, file_count, scenario)
    ]
    rows_per_file = split_counts(row_count, file_count)
    downloads: list[dict[str, Any]] = []
    row_offset = 0

    for file_index, file_timestamp in enumerate(file_timestamps):
        current_count = rows_per_file[file_index]
        row_times = [file_timestamp + timedelta(minutes=local_index * 5) for local_index in range(current_count)]
        rows = build_gdelt_rows(
            mission=mission,
            scenario=scenario,
            mode=mode,
            source_skill=source_skill,
            count=current_count,
            row_times=row_times,
            start_index=row_offset,
        )
        filename = gdelt_archive_filename(source_skill, file_timestamp)
        member_name = gdelt_member_name(source_skill, file_timestamp)
        payload_bytes, preview_lines = build_gdelt_zip_payload(member_name=member_name, rows=rows)
        output_path = download_dir / filename
        write_bytes(output_path, payload_bytes)
        request_url = f"{DEFAULT_GDELT_BASE_URL}/{filename}"
        downloads.append(
            {
                "entry": {
                    "timestamp_utc": file_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "timestamp_raw": gdelt_datetime_text(file_timestamp),
                    "url": request_url,
                    "md5": hashlib.md5(payload_bytes).hexdigest(),
                    "size_bytes": len(payload_bytes),
                },
                "request_url": request_url,
                "output_path": str(output_path),
                "bytes_written": len(payload_bytes),
                "content_type": "application/zip",
                "preview_member": member_name,
                "preview_lines": preview_lines,
                "validation": gdelt_validation_summary(
                    member_name=member_name,
                    row_count=len(rows),
                    expected_columns=GDELT_EXPECTED_COLUMNS[source_skill],
                ),
            }
        )
        row_offset += current_count

    return {
        "ok": True,
        "mode": "range",
        "selected_count": len(downloads),
        "downloaded_count": len(downloads),
        "skipped_count": 0,
        "downloads": downloads,
        "skipped": [],
    }


__all__ = [
    "build_gdelt_rows",
    "generate_raw_gdelt_manifest",
    "gdelt_archive_filename",
    "gdelt_claim_tone",
    "gdelt_country_hint",
    "gdelt_datetime_text",
    "gdelt_event_base_code",
    "gdelt_goldstein_score",
    "gdelt_member_name",
    "gdelt_place_label",
    "gdelt_topic_phrase",
    "raw_gdelt_download_dir",
]

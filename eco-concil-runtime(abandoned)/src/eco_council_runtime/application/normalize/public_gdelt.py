"""GDELT-specific public-source normalization helpers."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from eco_council_runtime.domain.normalize_semantics import (
    maybe_number,
    parse_loose_datetime,
    point_matches_geometry,
    row_token_set,
    to_rfc3339_z,
)
from eco_council_runtime.domain.text import maybe_text, normalize_space

from .public_common import (
    make_public_signal,
    maybe_mean,
    source_domain,
    top_counter_items,
    top_counter_text,
)

GDELT_SCAN_ROW_LIMIT = 25000
GDELT_EXAMPLE_SIGNAL_LIMIT = 3
GDELT_MATCHED_ROW_STORE_LIMIT = 32

GDELT_EVENTS_INDEX = {
    "event_id": 0,
    "sql_date": 1,
    "actor1_name": 6,
    "actor2_name": 16,
    "event_code": 26,
    "event_base_code": 27,
    "event_root_code": 28,
    "goldstein": 30,
    "num_mentions": 31,
    "num_sources": 32,
    "num_articles": 33,
    "avg_tone": 34,
    "action_geo_name": 52,
    "action_geo_country": 53,
    "action_geo_lat": 56,
    "action_geo_lon": 57,
    "date_added": 59,
    "source_url": 60,
}
GDELT_MENTIONS_INDEX = {
    "event_id": 0,
    "event_time": 1,
    "mention_time": 2,
    "mention_type": 3,
    "source_name": 4,
    "identifier": 5,
    "confidence": 11,
    "doc_length": 12,
    "doc_tone": 13,
}
GDELT_GKG_INDEX = {
    "record_id": 0,
    "date": 1,
    "source_common_name": 3,
    "document_identifier": 4,
    "themes": 8,
    "locations": 10,
    "persons": 12,
    "organizations": 14,
    "tone": 15,
    "all_names": 23,
}


def normalize_public_from_gdelt_doc(
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

    if isinstance(payload.get("articles"), list):
        for index, item in enumerate(payload["articles"]):
            if not isinstance(item, dict):
                continue
            title = maybe_text(item.get("title"))
            description = maybe_text(item.get("seendate") or item.get("domain"))
            signals.append(
                make_public_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="gdelt-doc-search",
                    signal_kind="article",
                    external_id=maybe_text(item.get("url") or item.get("title")),
                    title=title,
                    text=title or description,
                    url=maybe_text(item.get("url")),
                    author_name="",
                    channel_name=maybe_text(item.get("domain")),
                    language=maybe_text(item.get("language") or item.get("sourcelang")),
                    query_text="",
                    published_at_utc=to_rfc3339_z(parse_loose_datetime(item.get("seendate") or item.get("date"))),
                    engagement={},
                    metadata=item,
                    artifact_path=path,
                    record_locator=f"$.articles[{index}]",
                    sha256_value=sha256_value,
                    raw_obj=item,
                )
            )
        return signals

    for key in ("timeline", "data", "records"):
        candidate = payload.get(key)
        if not isinstance(candidate, list):
            continue
        for index, item in enumerate(candidate):
            if not isinstance(item, dict):
                continue
            title = maybe_text(item.get("title")) or "GDELT timeline bin"
            text = title
            if maybe_text(item.get("value")):
                text = f"{title} value={item.get('value')}"
            signals.append(
                make_public_signal(
                    run_id=run_id,
                    round_id=round_id,
                    source_skill="gdelt-doc-search",
                    signal_kind="timeline-bin",
                    external_id=maybe_text(item.get("date") or item.get("datetime") or index),
                    title=title,
                    text=text,
                    url=maybe_text(item.get("url")),
                    author_name="",
                    channel_name="",
                    language="",
                    query_text="",
                    published_at_utc=to_rfc3339_z(parse_loose_datetime(item.get("date") or item.get("datetime"))),
                    engagement={},
                    metadata=item,
                    artifact_path=path,
                    record_locator=f"$.{key}[{index}]",
                    sha256_value=sha256_value,
                    raw_obj=item,
                )
            )
        if signals:
            return signals
    return signals


def _gdelt_row_value(row: list[str], index: int) -> str:
    if index < 0 or index >= len(row):
        return ""
    return maybe_text(row[index])


def _iter_zip_tsv_rows(path: Path, *, max_rows: int = GDELT_SCAN_ROW_LIMIT) -> tuple[str, list[list[str]], bool]:
    with zipfile.ZipFile(path) as archive:
        member_names = [name for name in archive.namelist() if not name.endswith("/")]
        if not member_names:
            return ("", [], True)
        member_name = member_names[0]
        rows: list[list[str]] = []
        scan_complete = True
        with archive.open(member_name, "r") as raw_handle:
            with io.TextIOWrapper(raw_handle, encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle, delimiter="\t")
                for row in reader:
                    if not row:
                        continue
                    if len(rows) >= max_rows:
                        scan_complete = False
                        break
                    rows.append([maybe_text(item) for item in row])
    return (member_name, rows, scan_complete)


def _manifest_download_records(payload: Any) -> list[tuple[int, dict[str, Any]]]:
    if not isinstance(payload, dict):
        return []
    downloads = payload.get("downloads")
    if not isinstance(downloads, list):
        return []
    output: list[tuple[int, dict[str, Any]]] = []
    for index, item in enumerate(downloads):
        if isinstance(item, dict):
            output.append((index, item))
    return output


def _manifest_latest_timestamp(payload: Any) -> str | None:
    latest: datetime | None = None
    for _index, item in _manifest_download_records(payload):
        entry = item.get("entry") if isinstance(item.get("entry"), dict) else {}
        candidate = parse_loose_datetime(entry.get("timestamp_utc") or entry.get("timestamp_raw"))
        if candidate is None:
            continue
        if latest is None or candidate > latest:
            latest = candidate
    return to_rfc3339_z(latest)


def _gdelt_theme_values(value: Any) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in maybe_text(value).split(";"):
        text = maybe_text(item)
        if not text:
            continue
        primary = re.split(r"[,#]", text, maxsplit=1)[0].replace("_", " ").strip()
        normalized = maybe_text(primary)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    return output


def _gdelt_name_values(value: Any) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in maybe_text(value).split(";"):
        text = maybe_text(item)
        if not text:
            continue
        primary = re.split(r"[,#]", text, maxsplit=1)[0].strip()
        normalized = maybe_text(primary)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
    return output


def _gdelt_gkg_locations(value: Any) -> list[dict[str, Any]]:
    locations: list[dict[str, Any]] = []
    for item in maybe_text(value).split(";"):
        text = maybe_text(item)
        if not text:
            continue
        parts = text.split("#")
        if len(parts) >= 2:
            name = maybe_text(parts[1])
            latitude = maybe_number(parts[5]) if len(parts) > 5 else None
            longitude = maybe_number(parts[6]) if len(parts) > 6 else None
        else:
            name = text
            latitude = None
            longitude = None
        locations.append({"name": name, "latitude": latitude, "longitude": longitude})
    return locations


def _gdelt_first_tone(value: Any) -> float | None:
    parts = maybe_text(value).split(",")
    if not parts:
        return None
    return maybe_number(parts[0])


def _push_ranked_example(bucket: list[dict[str, Any]], example: dict[str, Any]) -> None:
    bucket.append(example)
    bucket.sort(key=lambda item: item.get("_rank", (0, 0, 0, "")), reverse=True)
    del bucket[GDELT_MATCHED_ROW_STORE_LIMIT:]


def _gdelt_coverage_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    path: Path,
    sha256_value: str,
    title: str,
    text: str,
    published_at_utc: str | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return make_public_signal(
        run_id=run_id,
        round_id=round_id,
        source_skill=source_skill,
        signal_kind="table-coverage",
        external_id=f"{source_skill}:coverage:{maybe_text(path.name)}",
        title=title,
        text=text,
        url="",
        author_name="",
        channel_name=source_skill,
        language="",
        query_text="",
        published_at_utc=published_at_utc,
        engagement={},
        metadata=metadata,
        artifact_path=path,
        record_locator="$.downloads[*]",
        sha256_value=sha256_value,
        raw_obj=metadata,
    )


def normalize_public_from_gdelt_events_manifest(
    path: Path,
    payload: Any,
    *,
    mission_scope: dict[str, Any],
    mission_region_tokens: list[str],
    mission_topic_tokens: list[str],
    mission_topic: str,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    mission_geometry = mission_scope.get("geometry") if isinstance(mission_scope.get("geometry"), dict) else {}
    location_counter: Counter[str] = Counter()
    event_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()
    tones: list[float] = []
    published_candidates: list[datetime] = []
    top_examples: list[dict[str, Any]] = []
    scanned_rows = 0
    matched_rows = 0
    total_mentions = 0
    total_articles = 0
    readable_files = 0
    missing_files = 0
    scan_complete = True

    for download_index, item in _manifest_download_records(payload):
        output_path_text = maybe_text(item.get("output_path"))
        if not output_path_text:
            missing_files += 1
            continue
        output_path = Path(output_path_text).expanduser().resolve()
        if not output_path.exists():
            missing_files += 1
            continue
        try:
            member_name, rows, file_complete = _iter_zip_tsv_rows(output_path)
        except (OSError, ValueError, zipfile.BadZipFile):
            missing_files += 1
            continue
        readable_files += 1
        scan_complete = scan_complete and file_complete
        for row_index, row in enumerate(rows):
            scanned_rows += 1
            if len(row) <= GDELT_EVENTS_INDEX["source_url"]:
                continue
            source_url = _gdelt_row_value(row, GDELT_EVENTS_INDEX["source_url"])
            action_geo_name = _gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_name"])
            latitude = maybe_number(_gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_lat"]))
            longitude = maybe_number(_gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_lon"]))
            token_set = row_token_set(
                _gdelt_row_value(row, GDELT_EVENTS_INDEX["actor1_name"]),
                _gdelt_row_value(row, GDELT_EVENTS_INDEX["actor2_name"]),
                action_geo_name,
                source_url,
                minimum_length=3,
            )
            region_match = point_matches_geometry(latitude, longitude, mission_geometry) or any(
                token in token_set for token in mission_region_tokens
            )
            topic_hits = sum(1 for token in mission_topic_tokens if token in token_set)
            if not region_match or (mission_topic_tokens and topic_hits == 0):
                continue

            matched_rows += 1
            event_code = _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_base_code"]) or _gdelt_row_value(
                row, GDELT_EVENTS_INDEX["event_code"]
            )
            domain = source_domain(source_url)
            mentions = int(maybe_number(_gdelt_row_value(row, GDELT_EVENTS_INDEX["num_mentions"])) or 0)
            articles = int(maybe_number(_gdelt_row_value(row, GDELT_EVENTS_INDEX["num_articles"])) or 0)
            tone = maybe_number(_gdelt_row_value(row, GDELT_EVENTS_INDEX["avg_tone"]))
            published_at = to_rfc3339_z(
                parse_loose_datetime(_gdelt_row_value(row, GDELT_EVENTS_INDEX["date_added"]))
                or parse_loose_datetime(_gdelt_row_value(row, GDELT_EVENTS_INDEX["sql_date"]))
            )
            published_dt = parse_loose_datetime(published_at)
            if published_dt is not None:
                published_candidates.append(published_dt)
            location_counter[action_geo_name or "unknown"] += 1
            event_counter[event_code or "unknown"] += 1
            if domain:
                domain_counter[domain] += 1
            total_mentions += mentions
            total_articles += articles
            if tone is not None:
                tones.append(tone)

            example_title = action_geo_name or "Mission-aligned GDELT event"
            example_text = normalize_space(
                " ".join(
                    part
                    for part in (
                        f"event_code={event_code}" if event_code else "",
                        _gdelt_row_value(row, GDELT_EVENTS_INDEX["actor1_name"]),
                        _gdelt_row_value(row, GDELT_EVENTS_INDEX["actor2_name"]),
                        f"mentions={mentions}",
                        f"articles={articles}",
                        f"tone={tone}" if tone is not None else "",
                    )
                    if part
                )
            )
            _push_ranked_example(
                top_examples,
                {
                    "_rank": (topic_hits, mentions, articles, published_at or ""),
                    "title": f"GDELT event at {example_title}",
                    "text": example_text,
                    "url": source_url,
                    "channel_name": domain,
                    "published_at_utc": published_at,
                    "record_locator": f"$.downloads[{download_index}].{member_name}[{row_index}]",
                    "metadata": {
                        "download_output_path": str(output_path),
                        "zip_member": member_name,
                        "event_id": _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_id"]),
                        "event_code": _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_code"]),
                        "event_base_code": event_code,
                        "event_root_code": _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_root_code"]),
                        "action_geo_name": action_geo_name,
                        "action_geo_country": _gdelt_row_value(row, GDELT_EVENTS_INDEX["action_geo_country"]),
                        "num_mentions": mentions,
                        "num_sources": int(maybe_number(_gdelt_row_value(row, GDELT_EVENTS_INDEX["num_sources"])) or 0),
                        "num_articles": articles,
                        "avg_tone": tone,
                    },
                    "raw_json": {
                        "event_id": _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_id"]),
                        "sql_date": _gdelt_row_value(row, GDELT_EVENTS_INDEX["sql_date"]),
                        "actor1_name": _gdelt_row_value(row, GDELT_EVENTS_INDEX["actor1_name"]),
                        "actor2_name": _gdelt_row_value(row, GDELT_EVENTS_INDEX["actor2_name"]),
                        "event_code": _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_code"]),
                        "event_base_code": event_code,
                        "event_root_code": _gdelt_row_value(row, GDELT_EVENTS_INDEX["event_root_code"]),
                        "action_geo_name": action_geo_name,
                        "action_geo_lat": latitude,
                        "action_geo_lon": longitude,
                        "source_url": source_url,
                    },
                },
            )

    coverage_metadata = {
        "matched_row_count": matched_rows,
        "scanned_row_count": scanned_rows,
        "readable_file_count": readable_files,
        "missing_file_count": missing_files,
        "scan_complete": scan_complete,
        "total_mentions": total_mentions,
        "total_articles": total_articles,
        "avg_tone": maybe_mean(tones),
        "top_locations": top_counter_items(location_counter),
        "top_event_codes": top_counter_items(event_counter),
        "top_domains": top_counter_items(domain_counter),
        "region_tokens": mission_region_tokens,
        "topic_tokens": mission_topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} event rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top locations: {top_counter_text(location_counter) or 'n/a'}. "
        f"Top event codes: {top_counter_text(event_counter) or 'n/a'}. "
        f"Top domains: {top_counter_text(domain_counter) or 'n/a'}."
    )
    coverage_published_at = _manifest_latest_timestamp(payload)
    if coverage_published_at is None and published_candidates:
        coverage_published_at = to_rfc3339_z(max(published_candidates))
    signals = [
        _gdelt_coverage_signal(
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            path=path,
            sha256_value=sha256_value,
            title="GDELT Events table coverage",
            text=coverage_text,
            published_at_utc=coverage_published_at,
            metadata=coverage_metadata,
        )
    ]
    for example_index, example in enumerate(top_examples[:GDELT_EXAMPLE_SIGNAL_LIMIT]):
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="event-record",
                external_id=f"{source_skill}:{example_index}:{maybe_text(example['record_locator'])}",
                title=maybe_text(example.get("title")),
                text=maybe_text(example.get("text")),
                url=maybe_text(example.get("url")),
                author_name="",
                channel_name=maybe_text(example.get("channel_name")),
                language="",
                query_text=mission_topic,
                published_at_utc=example.get("published_at_utc"),
                engagement={},
                metadata=example.get("metadata") if isinstance(example.get("metadata"), dict) else {},
                artifact_path=path,
                record_locator=maybe_text(example.get("record_locator")),
                sha256_value=sha256_value,
                raw_obj=example.get("raw_json"),
            )
        )
    return signals


def normalize_public_from_gdelt_mentions_manifest(
    path: Path,
    payload: Any,
    *,
    mission_region_tokens: list[str],
    mission_topic_tokens: list[str],
    mission_topic: str,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    source_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()
    event_counter: Counter[str] = Counter()
    confidence_values: list[float] = []
    tone_values: list[float] = []
    top_examples: list[dict[str, Any]] = []
    scanned_rows = 0
    matched_rows = 0
    readable_files = 0
    missing_files = 0
    scan_complete = True

    for download_index, item in _manifest_download_records(payload):
        output_path_text = maybe_text(item.get("output_path"))
        if not output_path_text:
            missing_files += 1
            continue
        output_path = Path(output_path_text).expanduser().resolve()
        if not output_path.exists():
            missing_files += 1
            continue
        try:
            member_name, rows, file_complete = _iter_zip_tsv_rows(output_path)
        except (OSError, ValueError, zipfile.BadZipFile):
            missing_files += 1
            continue
        readable_files += 1
        scan_complete = scan_complete and file_complete
        for row_index, row in enumerate(rows):
            scanned_rows += 1
            if len(row) <= GDELT_MENTIONS_INDEX["doc_tone"]:
                continue
            source_name = _gdelt_row_value(row, GDELT_MENTIONS_INDEX["source_name"])
            identifier = _gdelt_row_value(row, GDELT_MENTIONS_INDEX["identifier"])
            token_set = row_token_set(source_name, identifier, minimum_length=3)
            region_match = any(token in token_set for token in mission_region_tokens)
            topic_hits = sum(1 for token in mission_topic_tokens if token in token_set)
            if not region_match or (mission_topic_tokens and topic_hits == 0):
                continue

            matched_rows += 1
            domain = source_domain(identifier)
            confidence = maybe_number(_gdelt_row_value(row, GDELT_MENTIONS_INDEX["confidence"]))
            tone = maybe_number(_gdelt_row_value(row, GDELT_MENTIONS_INDEX["doc_tone"]))
            published_at = to_rfc3339_z(parse_loose_datetime(_gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_time"])))
            source_counter[source_name or "unknown"] += 1
            event_counter[_gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_id"]) or "unknown"] += 1
            if domain:
                domain_counter[domain] += 1
            if confidence is not None:
                confidence_values.append(confidence)
            if tone is not None:
                tone_values.append(tone)

            _push_ranked_example(
                top_examples,
                {
                    "_rank": (
                        topic_hits,
                        int(confidence or 0),
                        int(maybe_number(_gdelt_row_value(row, GDELT_MENTIONS_INDEX["doc_length"])) or 0),
                        published_at or "",
                    ),
                    "title": f"GDELT mention from {source_name or domain or 'unknown source'}",
                    "text": normalize_space(
                        " ".join(
                            part
                            for part in (
                                f"mention_type={_gdelt_row_value(row, GDELT_MENTIONS_INDEX['mention_type'])}",
                                f"confidence={confidence}" if confidence is not None else "",
                                f"tone={tone}" if tone is not None else "",
                                identifier,
                            )
                            if part
                        )
                    ),
                    "url": identifier,
                    "channel_name": domain or source_name,
                    "published_at_utc": published_at,
                    "record_locator": f"$.downloads[{download_index}].{member_name}[{row_index}]",
                    "metadata": {
                        "download_output_path": str(output_path),
                        "zip_member": member_name,
                        "event_id": _gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_id"]),
                        "mention_type": _gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_type"]),
                        "source_name": source_name,
                        "confidence": confidence,
                        "doc_length": maybe_number(_gdelt_row_value(row, GDELT_MENTIONS_INDEX["doc_length"])),
                        "doc_tone": tone,
                    },
                    "raw_json": {
                        "event_id": _gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_id"]),
                        "event_time": _gdelt_row_value(row, GDELT_MENTIONS_INDEX["event_time"]),
                        "mention_time": _gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_time"]),
                        "mention_type": _gdelt_row_value(row, GDELT_MENTIONS_INDEX["mention_type"]),
                        "source_name": source_name,
                        "identifier": identifier,
                    },
                },
            )

    coverage_metadata = {
        "matched_row_count": matched_rows,
        "scanned_row_count": scanned_rows,
        "readable_file_count": readable_files,
        "missing_file_count": missing_files,
        "scan_complete": scan_complete,
        "avg_confidence": maybe_mean(confidence_values),
        "avg_tone": maybe_mean(tone_values),
        "top_sources": top_counter_items(source_counter),
        "top_domains": top_counter_items(domain_counter),
        "top_event_ids": top_counter_items(event_counter),
        "region_tokens": mission_region_tokens,
        "topic_tokens": mission_topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} mention rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top sources: {top_counter_text(source_counter) or 'n/a'}. "
        f"Top domains: {top_counter_text(domain_counter) or 'n/a'}."
    )
    signals = [
        _gdelt_coverage_signal(
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            path=path,
            sha256_value=sha256_value,
            title="GDELT Mentions table coverage",
            text=coverage_text,
            published_at_utc=_manifest_latest_timestamp(payload),
            metadata=coverage_metadata,
        )
    ]
    for example_index, example in enumerate(top_examples[:GDELT_EXAMPLE_SIGNAL_LIMIT]):
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="mention-record",
                external_id=f"{source_skill}:{example_index}:{maybe_text(example['record_locator'])}",
                title=maybe_text(example.get("title")),
                text=maybe_text(example.get("text")),
                url=maybe_text(example.get("url")),
                author_name="",
                channel_name=maybe_text(example.get("channel_name")),
                language="",
                query_text=mission_topic,
                published_at_utc=example.get("published_at_utc"),
                engagement={},
                metadata=example.get("metadata") if isinstance(example.get("metadata"), dict) else {},
                artifact_path=path,
                record_locator=maybe_text(example.get("record_locator")),
                sha256_value=sha256_value,
                raw_obj=example.get("raw_json"),
            )
        )
    return signals


def normalize_public_from_gdelt_gkg_manifest(
    path: Path,
    payload: Any,
    *,
    mission_scope: dict[str, Any],
    mission_region_tokens: list[str],
    mission_topic_tokens: list[str],
    mission_topic: str,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    mission_geometry = mission_scope.get("geometry") if isinstance(mission_scope.get("geometry"), dict) else {}
    theme_counter: Counter[str] = Counter()
    location_counter: Counter[str] = Counter()
    organization_counter: Counter[str] = Counter()
    domain_counter: Counter[str] = Counter()
    tone_values: list[float] = []
    top_examples: list[dict[str, Any]] = []
    scanned_rows = 0
    matched_rows = 0
    readable_files = 0
    missing_files = 0
    scan_complete = True

    for download_index, item in _manifest_download_records(payload):
        output_path_text = maybe_text(item.get("output_path"))
        if not output_path_text:
            missing_files += 1
            continue
        output_path = Path(output_path_text).expanduser().resolve()
        if not output_path.exists():
            missing_files += 1
            continue
        try:
            member_name, rows, file_complete = _iter_zip_tsv_rows(output_path)
        except (OSError, ValueError, zipfile.BadZipFile):
            missing_files += 1
            continue
        readable_files += 1
        scan_complete = scan_complete and file_complete
        for row_index, row in enumerate(rows):
            scanned_rows += 1
            if len(row) <= GDELT_GKG_INDEX["all_names"]:
                continue
            document_identifier = _gdelt_row_value(row, GDELT_GKG_INDEX["document_identifier"])
            source_common_name = _gdelt_row_value(row, GDELT_GKG_INDEX["source_common_name"])
            themes = _gdelt_theme_values(_gdelt_row_value(row, GDELT_GKG_INDEX["themes"]))
            locations = _gdelt_gkg_locations(_gdelt_row_value(row, GDELT_GKG_INDEX["locations"]))
            organizations = _gdelt_name_values(_gdelt_row_value(row, GDELT_GKG_INDEX["organizations"]))
            persons = _gdelt_name_values(_gdelt_row_value(row, GDELT_GKG_INDEX["persons"]))
            token_set = row_token_set(
                source_common_name,
                document_identifier,
                " ".join(themes),
                " ".join(maybe_text(location.get("name")) for location in locations),
                " ".join(organizations),
                " ".join(persons),
                minimum_length=3,
            )
            region_match = any(
                point_matches_geometry(location.get("latitude"), location.get("longitude"), mission_geometry)
                for location in locations
            ) or any(token in token_set for token in mission_region_tokens)
            topic_hits = sum(1 for token in mission_topic_tokens if token in token_set)
            if not region_match or (mission_topic_tokens and topic_hits == 0):
                continue

            matched_rows += 1
            domain = source_domain(document_identifier)
            tone = _gdelt_first_tone(_gdelt_row_value(row, GDELT_GKG_INDEX["tone"]))
            published_at = to_rfc3339_z(parse_loose_datetime(_gdelt_row_value(row, GDELT_GKG_INDEX["date"])))
            for value in themes[:5]:
                theme_counter[value] += 1
            for value in organizations[:5]:
                organization_counter[value] += 1
            for location in locations[:5]:
                name = maybe_text(location.get("name"))
                if name:
                    location_counter[name] += 1
            if domain:
                domain_counter[domain] += 1
            if tone is not None:
                tone_values.append(tone)

            _push_ranked_example(
                top_examples,
                {
                    "_rank": (topic_hits, len(themes), len(organizations), published_at or ""),
                    "title": f"GDELT GKG document from {source_common_name or domain or 'unknown source'}",
                    "text": normalize_space(
                        " ".join(
                            part
                            for part in (
                                f"themes={', '.join(themes[:3])}" if themes else "",
                                f"locations={', '.join(maybe_text(item.get('name')) for item in locations[:3] if maybe_text(item.get('name')))}"
                                if locations
                                else "",
                                f"organizations={', '.join(organizations[:3])}" if organizations else "",
                                f"tone={tone}" if tone is not None else "",
                            )
                            if part
                        )
                    ),
                    "url": document_identifier,
                    "channel_name": domain or source_common_name,
                    "published_at_utc": published_at,
                    "record_locator": f"$.downloads[{download_index}].{member_name}[{row_index}]",
                    "metadata": {
                        "download_output_path": str(output_path),
                        "zip_member": member_name,
                        "record_id": _gdelt_row_value(row, GDELT_GKG_INDEX["record_id"]),
                        "source_common_name": source_common_name,
                        "themes": themes[:6],
                        "locations": [maybe_text(item.get("name")) for item in locations[:6] if maybe_text(item.get("name"))],
                        "organizations": organizations[:6],
                        "persons": persons[:6],
                        "tone": tone,
                    },
                    "raw_json": {
                        "record_id": _gdelt_row_value(row, GDELT_GKG_INDEX["record_id"]),
                        "date": _gdelt_row_value(row, GDELT_GKG_INDEX["date"]),
                        "source_common_name": source_common_name,
                        "document_identifier": document_identifier,
                        "themes": themes[:8],
                        "locations": locations[:8],
                        "organizations": organizations[:8],
                        "persons": persons[:8],
                    },
                },
            )

    coverage_metadata = {
        "matched_row_count": matched_rows,
        "scanned_row_count": scanned_rows,
        "readable_file_count": readable_files,
        "missing_file_count": missing_files,
        "scan_complete": scan_complete,
        "avg_tone": maybe_mean(tone_values),
        "top_themes": top_counter_items(theme_counter),
        "top_locations": top_counter_items(location_counter),
        "top_organizations": top_counter_items(organization_counter),
        "top_domains": top_counter_items(domain_counter),
        "region_tokens": mission_region_tokens,
        "topic_tokens": mission_topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} GKG rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top themes: {top_counter_text(theme_counter) or 'n/a'}. "
        f"Top locations: {top_counter_text(location_counter) or 'n/a'}."
    )
    signals = [
        _gdelt_coverage_signal(
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            path=path,
            sha256_value=sha256_value,
            title="GDELT GKG table coverage",
            text=coverage_text,
            published_at_utc=_manifest_latest_timestamp(payload),
            metadata=coverage_metadata,
        )
    ]
    for example_index, example in enumerate(top_examples[:GDELT_EXAMPLE_SIGNAL_LIMIT]):
        signals.append(
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="gkg-record",
                external_id=f"{source_skill}:{example_index}:{maybe_text(example['record_locator'])}",
                title=maybe_text(example.get("title")),
                text=maybe_text(example.get("text")),
                url=maybe_text(example.get("url")),
                author_name="",
                channel_name=maybe_text(example.get("channel_name")),
                language="",
                query_text=mission_topic,
                published_at_utc=example.get("published_at_utc"),
                engagement={},
                metadata=example.get("metadata") if isinstance(example.get("metadata"), dict) else {},
                artifact_path=path,
                record_locator=maybe_text(example.get("record_locator")),
                sha256_value=sha256_value,
                raw_obj=example.get("raw_json"),
            )
        )
    return signals


def normalize_public_from_gdelt_manifest(
    path: Path,
    payload: Any,
    *,
    mission_scope: dict[str, Any],
    mission_region_tokens: list[str],
    mission_topic_tokens: list[str],
    mission_topic: str,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    if source_skill == "gdelt-events-fetch":
        return normalize_public_from_gdelt_events_manifest(
            path,
            payload,
            mission_scope=mission_scope,
            mission_region_tokens=mission_region_tokens,
            mission_topic_tokens=mission_topic_tokens,
            mission_topic=mission_topic,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    if source_skill == "gdelt-mentions-fetch":
        return normalize_public_from_gdelt_mentions_manifest(
            path,
            payload,
            mission_region_tokens=mission_region_tokens,
            mission_topic_tokens=mission_topic_tokens,
            mission_topic=mission_topic,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    return normalize_public_from_gdelt_gkg_manifest(
        path,
        payload,
        mission_scope=mission_scope,
        mission_region_tokens=mission_region_tokens,
        mission_topic_tokens=mission_topic_tokens,
        mission_topic=mission_topic,
        run_id=run_id,
        round_id=round_id,
        source_skill=source_skill,
        sha256_value=sha256_value,
    )


__all__ = [
    "normalize_public_from_gdelt_doc",
    "normalize_public_from_gdelt_events_manifest",
    "normalize_public_from_gdelt_gkg_manifest",
    "normalize_public_from_gdelt_manifest",
    "normalize_public_from_gdelt_mentions_manifest",
]

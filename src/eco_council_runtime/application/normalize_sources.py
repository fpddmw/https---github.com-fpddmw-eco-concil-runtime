"""Application services for source-specific normalization pipelines."""

from __future__ import annotations

import csv
import gzip
import io
import re
import statistics
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import (
    file_sha256,
    load_json_if_exists,
    read_json,
    read_jsonl,
    stable_hash,
    utc_now_iso,
    write_json,
)
from eco_council_runtime.domain.normalize_semantics import (
    canonical_environment_metric,
    maybe_number,
    parse_loose_datetime,
    point_matches_geometry,
    row_token_set,
    to_rfc3339_z,
)
from eco_council_runtime.domain.text import maybe_text, normalize_space, truncate_text

NORMALIZE_CACHE_VERSION = "v3"
GDELT_SCAN_ROW_LIMIT = 25000
GDELT_EXAMPLE_SIGNAL_LIMIT = 3
GDELT_MATCHED_ROW_STORE_LIMIT = 32

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


def normalize_cache_dir(run_dir: Path) -> Path:
    return run_dir / "analytics" / "normalize_cache"


def _normalize_cache_path(
    run_dir: Path,
    *,
    domain: str,
    source_skill: str,
    run_id: str,
    round_id: str,
    artifact_sha256: str,
) -> Path:
    key = stable_hash(NORMALIZE_CACHE_VERSION, domain, source_skill, run_id, round_id, artifact_sha256)
    safe_domain = re.sub(r"[^a-z0-9_-]+", "-", domain.lower())
    safe_source = re.sub(r"[^a-z0-9_-]+", "-", source_skill.lower())
    return normalize_cache_dir(run_dir) / safe_domain / f"{safe_source}_{key[:16]}.json"


def _read_cache_payload(path: Path) -> dict[str, Any] | None:
    payload = load_json_if_exists(path)
    if not isinstance(payload, dict):
        return None
    return payload


def _write_cache_payload(path: Path, payload: dict[str, Any]) -> None:
    write_json(path, payload, pretty=False)


def _parse_path_payload(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return read_json(path)
    if suffix == ".jsonl":
        return read_jsonl(path)
    raise ValueError(f"Unsupported JSON payload path: {path}")


def _source_domain(value: str) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    text = text.casefold()
    if "://" in text:
        text = text.split("://", 1)[1]
    domain = text.split("/", 1)[0]
    return domain[4:] if domain.startswith("www.") else domain


def _top_counter_items(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key, count in counter.most_common(limit):
        if not key or count <= 0:
            continue
        items.append({"value": key, "count": count})
    return items


def _top_counter_text(counter: Counter[str], limit: int = 3) -> str:
    parts = [f"{item['value']} ({item['count']})" for item in _top_counter_items(counter, limit=limit)]
    return ", ".join(parts)


def _maybe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.fmean(values), 3)


def _collect_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("records", "items", "data", "results"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]
    return []


def _make_public_signal(
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    signal_kind: str,
    external_id: str,
    title: str,
    text: str,
    url: str,
    author_name: str,
    channel_name: str,
    language: str,
    query_text: str,
    published_at_utc: str | None,
    engagement: dict[str, Any],
    metadata: dict[str, Any],
    artifact_path: Path,
    record_locator: str,
    sha256_value: str,
    raw_obj: Any,
) -> dict[str, Any]:
    identity = external_id or url or f"{signal_kind}:{record_locator}"
    signal_hash = stable_hash(source_skill, identity, maybe_text(title), maybe_text(text))
    return {
        "signal_id": f"pubsig-{signal_hash[:12]}",
        "run_id": run_id,
        "round_id": round_id,
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "external_id": external_id,
        "title": title,
        "text": text,
        "url": url,
        "author_name": author_name,
        "channel_name": channel_name,
        "language": language,
        "query_text": query_text,
        "published_at_utc": published_at_utc,
        "captured_at_utc": utc_now_iso(),
        "engagement": engagement,
        "metadata": metadata,
        "artifact_path": str(artifact_path),
        "record_locator": record_locator,
        "sha256": sha256_value,
        "raw_json": raw_obj,
    }


def _strip_simple_html(value: str) -> str:
    return normalize_space(re.sub(r"<[^>]+>", " ", value))


def normalize_public_from_youtube_videos(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(_collect_records(payload)):
        video = record.get("video")
        if not isinstance(video, dict):
            continue
        video_id = maybe_text(record.get("video_id")) or maybe_text(video.get("id"))
        title = maybe_text(video.get("title"))
        description = maybe_text(video.get("description"))
        url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
        signals.append(
            _make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="youtube-video-search",
                signal_kind="video",
                external_id=video_id,
                title=title,
                text=description,
                url=url,
                author_name=maybe_text(video.get("channel_title")),
                channel_name=maybe_text(video.get("channel_title")),
                language=maybe_text(video.get("default_language") or video.get("default_audio_language")),
                query_text=maybe_text(record.get("query")),
                published_at_utc=to_rfc3339_z(parse_loose_datetime(video.get("published_at"))),
                engagement=video.get("statistics") if isinstance(video.get("statistics"), dict) else {},
                metadata={
                    "search_match": record.get("search_match"),
                    "content_details": video.get("content_details"),
                    "status": video.get("status"),
                },
                artifact_path=path,
                record_locator=f"$[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def normalize_public_from_youtube_comments(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(_collect_records(payload)):
        comment_id = maybe_text(record.get("comment_id"))
        video_id = maybe_text(record.get("video_id"))
        text = maybe_text(record.get("text_original") or record.get("text_display"))
        url = ""
        if video_id and comment_id:
            url = f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"
        signals.append(
            _make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="youtube-comments-fetch",
                signal_kind=maybe_text(record.get("comment_type")) or "comment",
                external_id=comment_id,
                title=truncate_text(text, 120),
                text=text,
                url=url,
                author_name=maybe_text(record.get("author_display_name")),
                channel_name=maybe_text(record.get("channel_id")),
                language="",
                query_text=maybe_text((record.get("source") or {}).get("search_terms")),
                published_at_utc=to_rfc3339_z(parse_loose_datetime(record.get("published_at"))),
                engagement={"like_count": maybe_number(record.get("like_count"))},
                metadata={
                    "video_id": video_id,
                    "thread_id": maybe_text(record.get("thread_id")),
                    "parent_comment_id": maybe_text(record.get("parent_comment_id")),
                    "source": record.get("source"),
                },
                artifact_path=path,
                record_locator=f"$[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def _bluesky_uri_to_url(uri: str, author_handle: str) -> str:
    if not uri or not author_handle:
        return ""
    parts = uri.split("/")
    post_id = parts[-1] if parts else ""
    if not post_id:
        return ""
    return f"https://bsky.app/profile/{author_handle}/post/{post_id}"


def normalize_public_from_bluesky(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        if isinstance(payload.get("seed_posts"), list):
            seeds.extend(item for item in payload["seed_posts"] if isinstance(item, dict))
        if isinstance(payload.get("threads"), list):
            for thread in payload["threads"]:
                if not isinstance(thread, dict):
                    continue
                nodes = thread.get("nodes")
                if isinstance(nodes, list):
                    seeds.extend(node for node in nodes if isinstance(node, dict))
    elif isinstance(payload, list):
        seeds.extend(item for item in payload if isinstance(item, dict))

    signals: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, record in enumerate(seeds):
        uri = maybe_text(record.get("uri"))
        if uri and uri in seen_ids:
            continue
        if uri:
            seen_ids.add(uri)
        author_handle = maybe_text(record.get("author_handle"))
        text = maybe_text(record.get("text"))
        signals.append(
            _make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="bluesky-cascade-fetch",
                signal_kind="reply" if maybe_text(record.get("reply_parent_uri")) else "post",
                external_id=uri or maybe_text(record.get("cid")),
                title=truncate_text(text, 120),
                text=text,
                url=_bluesky_uri_to_url(uri, author_handle),
                author_name=author_handle,
                channel_name=maybe_text(record.get("author_did")),
                language=",".join(record.get("langs", [])) if isinstance(record.get("langs"), list) else "",
                query_text="",
                published_at_utc=maybe_text(record.get("timestamp_utc")) or to_rfc3339_z(parse_loose_datetime(record.get("created_at"))),
                engagement={
                    "reply_count": maybe_number(record.get("reply_count")),
                    "repost_count": maybe_number(record.get("repost_count")),
                    "like_count": maybe_number(record.get("like_count")),
                    "quote_count": maybe_number(record.get("quote_count")),
                },
                metadata={
                    "author_did": maybe_text(record.get("author_did")),
                    "cid": maybe_text(record.get("cid")),
                    "reply_root_uri": maybe_text(record.get("reply_root_uri")),
                    "timestamp_source": maybe_text(record.get("timestamp_source")),
                },
                artifact_path=path,
                record_locator=f"$[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


def _extract_reggov_resource(record: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if "detail" in record:
        detail = record.get("detail")
        if isinstance(detail, dict):
            resource = detail.get("data") if isinstance(detail.get("data"), dict) else detail.get("data")
            if isinstance(resource, dict):
                return resource, {"response_url": record.get("response_url"), "validation": record.get("validation")}
    return record if "attributes" in record else None, {}


def _normalize_reggov_resource(
    path: Path,
    record: dict[str, Any],
    *,
    index: int,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> dict[str, Any] | None:
    resource, metadata = _extract_reggov_resource(record)
    if not isinstance(resource, dict):
        return None
    attrs = resource.get("attributes") if isinstance(resource.get("attributes"), dict) else {}
    links = resource.get("links") if isinstance(resource.get("links"), dict) else {}
    text = maybe_text(
        attrs.get("comment")
        or attrs.get("commentText")
        or attrs.get("commentOn")
        or attrs.get("title")
        or attrs.get("organization")
    )
    title = maybe_text(attrs.get("title") or attrs.get("subject") or attrs.get("organization")) or truncate_text(text, 120)
    metadata.update(
        {
            "docket_id": maybe_text(attrs.get("docketId")),
            "document_type": maybe_text(attrs.get("documentType")),
            "posted_date": maybe_text(attrs.get("postedDate")),
            "last_modified_date": maybe_text(attrs.get("lastModifiedDate")),
        }
    )
    return _make_public_signal(
        run_id=run_id,
        round_id=round_id,
        source_skill=source_skill,
        signal_kind="policy-comment",
        external_id=maybe_text(resource.get("id") or record.get("comment_id")),
        title=title,
        text=text,
        url=maybe_text(links.get("self") or metadata.get("response_url")),
        author_name=maybe_text(attrs.get("organization") or attrs.get("firstName")),
        channel_name=maybe_text(attrs.get("agencyId")),
        language="",
        query_text="",
        published_at_utc=to_rfc3339_z(parse_loose_datetime(attrs.get("postedDate") or attrs.get("lastModifiedDate"))),
        engagement={},
        metadata=metadata,
        artifact_path=path,
        record_locator=f"$[{index}]",
        sha256_value=sha256_value,
        raw_obj=record,
    )


def normalize_public_from_reggov(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(_collect_records(payload)):
        normalized = _normalize_reggov_resource(
            path,
            record,
            index=index,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
        if normalized is not None:
            signals.append(normalized)
    return signals


def normalize_public_from_federal_register(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        raise ValueError(
            "Federal Register raw artifact must use the canonical federal-register-doc-fetch "
            "payload with a top-level records array."
        )
    query_text = ""
    request_obj = payload.get("request") if isinstance(payload, dict) else None
    if isinstance(request_obj, dict):
        query_text = maybe_text(request_obj.get("term"))
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        agencies = record.get("agencies") if isinstance(record.get("agencies"), list) else []
        agency_names = [
            maybe_text(item.get("name") or item.get("raw_name") or item.get("slug"))
            for item in agencies
            if isinstance(item, dict) and maybe_text(item.get("name") or item.get("raw_name") or item.get("slug"))
        ]
        agency_slugs = [
            maybe_text(item.get("slug"))
            for item in agencies
            if isinstance(item, dict) and maybe_text(item.get("slug"))
        ]
        title = maybe_text(record.get("title"))
        abstract = maybe_text(record.get("abstract"))
        excerpts = _strip_simple_html(maybe_text(record.get("excerpts")))
        text = abstract or excerpts or title
        signals.append(
            _make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill=source_skill,
                signal_kind="policy-document",
                external_id=maybe_text(record.get("document_number") or record.get("html_url") or index),
                title=title or maybe_text(record.get("document_number")) or "Federal Register document",
                text=text,
                url=maybe_text(record.get("html_url") or record.get("pdf_url")),
                author_name="",
                channel_name=", ".join(agency_names),
                language="",
                query_text=query_text,
                published_at_utc=to_rfc3339_z(parse_loose_datetime(record.get("publication_date"))),
                engagement={},
                metadata={
                    "type": maybe_text(record.get("type")),
                    "document_number": maybe_text(record.get("document_number")),
                    "pdf_url": maybe_text(record.get("pdf_url")),
                    "public_inspection_pdf_url": maybe_text(record.get("public_inspection_pdf_url")),
                    "agencies": agency_names,
                    "agency_slugs": agency_slugs,
                    "topics": record.get("topics") if isinstance(record.get("topics"), list) else [],
                    "docket_ids": record.get("docket_ids") if isinstance(record.get("docket_ids"), list) else [],
                    "regulation_id_numbers": record.get("regulation_id_numbers") if isinstance(record.get("regulation_id_numbers"), list) else [],
                    "significant": record.get("significant"),
                    "comment_url": maybe_text(record.get("comment_url")),
                    "raw_text_url": maybe_text(record.get("raw_text_url")),
                    "source_page_number": record.get("source_page_number"),
                },
                artifact_path=path,
                record_locator=f"$.records[{index}]",
                sha256_value=sha256_value,
                raw_obj=record,
            )
        )
    return signals


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
                _make_public_signal(
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
                _make_public_signal(
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
    return _make_public_signal(
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
            domain = _source_domain(source_url)
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
        "avg_tone": _maybe_mean(tones),
        "top_locations": _top_counter_items(location_counter),
        "top_event_codes": _top_counter_items(event_counter),
        "top_domains": _top_counter_items(domain_counter),
        "region_tokens": mission_region_tokens,
        "topic_tokens": mission_topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} event rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top locations: {_top_counter_text(location_counter) or 'n/a'}. "
        f"Top event codes: {_top_counter_text(event_counter) or 'n/a'}. "
        f"Top domains: {_top_counter_text(domain_counter) or 'n/a'}."
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
            _make_public_signal(
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
            domain = _source_domain(identifier)
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
        "avg_confidence": _maybe_mean(confidence_values),
        "avg_tone": _maybe_mean(tone_values),
        "top_sources": _top_counter_items(source_counter),
        "top_domains": _top_counter_items(domain_counter),
        "top_event_ids": _top_counter_items(event_counter),
        "region_tokens": mission_region_tokens,
        "topic_tokens": mission_topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} mention rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top sources: {_top_counter_text(source_counter) or 'n/a'}. "
        f"Top domains: {_top_counter_text(domain_counter) or 'n/a'}."
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
            _make_public_signal(
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
            domain = _source_domain(document_identifier)
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
        "avg_tone": _maybe_mean(tone_values),
        "top_themes": _top_counter_items(theme_counter),
        "top_locations": _top_counter_items(location_counter),
        "top_organizations": _top_counter_items(organization_counter),
        "top_domains": _top_counter_items(domain_counter),
        "region_tokens": mission_region_tokens,
        "topic_tokens": mission_topic_tokens,
    }
    coverage_text = (
        f"Scanned {scanned_rows} GKG rows across {readable_files} readable ZIP files; matched {matched_rows} rows. "
        f"Top themes: {_top_counter_text(theme_counter) or 'n/a'}. "
        f"Top locations: {_top_counter_text(location_counter) or 'n/a'}."
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
            _make_public_signal(
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


def normalize_public_source(
    source_skill: str,
    path: Path,
    *,
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any] | None = None,
    mission_region_tokens: list[str] | None = None,
    mission_topic_tokens: list[str] | None = None,
    mission_topic: str = "",
) -> list[dict[str, Any]]:
    sha256_value = file_sha256(path)
    payload = _parse_path_payload(path)
    if source_skill == "youtube-video-search":
        return normalize_public_from_youtube_videos(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill == "youtube-comments-fetch":
        return normalize_public_from_youtube_comments(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill == "bluesky-cascade-fetch":
        return normalize_public_from_bluesky(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill == "federal-register-doc-fetch":
        return normalize_public_from_federal_register(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    if source_skill in {"regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"}:
        return normalize_public_from_reggov(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    if source_skill == "gdelt-doc-search":
        return normalize_public_from_gdelt_doc(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
    if source_skill in {"gdelt-events-fetch", "gdelt-mentions-fetch", "gdelt-gkg-fetch"}:
        if mission_scope is None or mission_region_tokens is None or mission_topic_tokens is None:
            raise ValueError("GDELT normalization requires mission_scope, mission_region_tokens, and mission_topic_tokens.")
        return normalize_public_from_gdelt_manifest(
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
    raise ValueError(f"Unsupported public source skill: {source_skill}")


def normalize_public_source_cached(
    *,
    run_dir: Path,
    source_skill: str,
    path: Path,
    run_id: str,
    round_id: str,
    mission_scope: dict[str, Any] | None = None,
    mission_region_tokens: list[str] | None = None,
    mission_topic_tokens: list[str] | None = None,
    mission_topic: str = "",
) -> tuple[list[dict[str, Any]], str]:
    artifact_sha256 = file_sha256(path)
    cache_path = _normalize_cache_path(
        run_dir,
        domain="public",
        source_skill=source_skill,
        run_id=run_id,
        round_id=round_id,
        artifact_sha256=artifact_sha256,
    )
    cached = _read_cache_payload(cache_path)
    if isinstance(cached, dict):
        signals = cached.get("signals")
        if (
            cached.get("cache_version") == NORMALIZE_CACHE_VERSION
            and cached.get("artifact_sha256") == artifact_sha256
            and isinstance(signals, list)
        ):
            return [item for item in signals if isinstance(item, dict)], "hit"

    signals = normalize_public_source(
        source_skill,
        path,
        run_id=run_id,
        round_id=round_id,
        mission_scope=mission_scope,
        mission_region_tokens=mission_region_tokens,
        mission_topic_tokens=mission_topic_tokens,
        mission_topic=mission_topic,
    )
    _write_cache_payload(
        cache_path,
        {
            "cache_version": NORMALIZE_CACHE_VERSION,
            "domain": "public",
            "source_skill": source_skill,
            "run_id": run_id,
            "round_id": round_id,
            "artifact_path": str(path),
            "artifact_sha256": artifact_sha256,
            "signals": signals,
        },
    )
    return signals, "miss"


def _make_environment_signal(
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


def _iter_open_meteo_signals(
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
                        _make_environment_signal(
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


def _iter_nasa_firms_signals(
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
            _make_environment_signal(
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


def _unwrap_openaq_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and "result" in payload:
        return _unwrap_openaq_payload(payload["result"])
    return payload


def _extract_nested_value(row: dict[str, Any], *paths: str) -> Any:
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


def _openaq_row_to_signal(
    row: dict[str, Any],
    *,
    path: Path,
    run_id: str,
    round_id: str,
    index: int,
    sha256_value: str,
) -> dict[str, Any] | None:
    metric = maybe_text(_extract_nested_value(row, "parameter.name", "parameter", "parameterName", "metric", "name"))
    unit = maybe_text(_extract_nested_value(row, "parameter.units", "unit", "units")) or "unknown"
    value = None
    for key in OPENAQ_VALUE_KEYS:
        value = maybe_number(_extract_nested_value(row, key))
        if value is not None:
            break
    if value is None or not metric:
        return None
    timestamp_text = ""
    timestamp_candidate = _extract_nested_value(row, "date.utc", "date.local")
    if timestamp_candidate is None:
        for key in OPENAQ_TIME_KEYS:
            timestamp_candidate = _extract_nested_value(row, key)
            if timestamp_candidate is not None:
                break
    if timestamp_candidate is not None:
        timestamp_text = maybe_text(timestamp_candidate)
    coordinates = row.get("coordinates") if isinstance(row.get("coordinates"), dict) else {}
    latitude = maybe_number(coordinates.get("latitude"))
    longitude = maybe_number(coordinates.get("longitude"))
    if latitude is None:
        for key in OPENAQ_LAT_KEYS:
            latitude = maybe_number(_extract_nested_value(row, key))
            if latitude is not None:
                break
    if longitude is None:
        for key in OPENAQ_LON_KEYS:
            longitude = maybe_number(_extract_nested_value(row, key))
            if longitude is not None:
                break
    metadata = {
        "location_id": _extract_nested_value(row, "location.id", "locationId", "locationsId"),
        "location_name": maybe_text(_extract_nested_value(row, "location.name", "location")),
        "sensor_id": _extract_nested_value(row, "sensor.id", "sensorId", "sensorsId"),
        "provider": maybe_text(_extract_nested_value(row, "provider.name", "provider")),
    }
    return _make_environment_signal(
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


def _iter_csv_rows(path: Path) -> list[dict[str, str]]:
    open_func = gzip.open if path.suffix.lower() == ".gz" else open
    with open_func(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _iter_openaq_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    sha256_value = file_sha256(path)
    suffix = path.suffix.lower()
    rows: list[dict[str, Any]] = []
    if suffix in {".json", ".jsonl"}:
        payload = _unwrap_openaq_payload(_parse_path_payload(path))
        rows = _collect_records(payload)
        if not rows and isinstance(payload, dict):
            output_path = maybe_text(payload.get("output_path"))
            if output_path:
                nested_path = Path(output_path).expanduser().resolve()
                if nested_path.exists():
                    return _iter_openaq_signals(nested_path, run_id=run_id, round_id=round_id)
    elif suffix in {".csv", ".gz"}:
        rows = _iter_csv_rows(path)
    else:
        raise ValueError(f"Unsupported OpenAQ artifact path: {path}")

    signals: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        normalized = _openaq_row_to_signal(
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


def _iter_airnow_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = _parse_path_payload(path)
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
                _make_environment_signal(
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
                _make_environment_signal(
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


def _iter_usgs_water_iv_signals(
    path: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = _parse_path_payload(path)
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
            _make_environment_signal(
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
        payload = _parse_path_payload(path)
        signals = _iter_open_meteo_signals(
            path,
            payload,
            run_id=run_id,
            round_id=round_id,
            source_skill=source_skill,
            sha256_value=sha256_value,
        )
    elif source_skill == "nasa-firms-fire-fetch":
        sha256_value = file_sha256(path)
        payload = _parse_path_payload(path)
        signals = _iter_nasa_firms_signals(path, payload, run_id=run_id, round_id=round_id, sha256_value=sha256_value)
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
        signals = _iter_openaq_signals(path, run_id=run_id, round_id=round_id)
    elif source_skill == "airnow-hourly-obs-fetch":
        signals = _iter_airnow_signals(path, run_id=run_id, round_id=round_id)
    elif source_skill == "usgs-water-iv-fetch":
        signals = _iter_usgs_water_iv_signals(path, run_id=run_id, round_id=round_id)
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
    cache_path = _normalize_cache_path(
        run_dir,
        domain="environment",
        source_skill=source_skill,
        run_id=run_id,
        round_id=round_id,
        artifact_sha256=artifact_sha256,
    )
    cached = _read_cache_payload(cache_path)
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
    _write_cache_payload(
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
    "NORMALIZE_CACHE_VERSION",
    "normalize_cache_dir",
    "normalize_environment_source",
    "normalize_environment_source_cached",
    "normalize_public_source",
    "normalize_public_source_cached",
]

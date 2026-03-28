"""Public-source normalization pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import file_sha256
from eco_council_runtime.domain.normalize_semantics import maybe_number, parse_loose_datetime, to_rfc3339_z
from eco_council_runtime.domain.text import maybe_text, truncate_text

from .public_common import collect_records, make_public_signal, strip_simple_html
from .public_gdelt import normalize_public_from_gdelt_doc, normalize_public_from_gdelt_manifest
from .source_cache import (
    NORMALIZE_CACHE_VERSION,
    normalize_cache_path,
    parse_source_payload,
    read_cache_payload,
    write_cache_payload,
)


def normalize_public_from_youtube_videos(
    path: Path,
    payload: Any,
    *,
    run_id: str,
    round_id: str,
    sha256_value: str,
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for index, record in enumerate(collect_records(payload)):
        video = record.get("video")
        if not isinstance(video, dict):
            continue
        video_id = maybe_text(record.get("video_id")) or maybe_text(video.get("id"))
        title = maybe_text(video.get("title"))
        description = maybe_text(video.get("description"))
        url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
        signals.append(
            make_public_signal(
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
    for index, record in enumerate(collect_records(payload)):
        comment_id = maybe_text(record.get("comment_id"))
        video_id = maybe_text(record.get("video_id"))
        text = maybe_text(record.get("text_original") or record.get("text_display"))
        url = ""
        if video_id and comment_id:
            url = f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}"
        signals.append(
            make_public_signal(
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


def bluesky_uri_to_url(uri: str, author_handle: str) -> str:
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
            make_public_signal(
                run_id=run_id,
                round_id=round_id,
                source_skill="bluesky-cascade-fetch",
                signal_kind="reply" if maybe_text(record.get("reply_parent_uri")) else "post",
                external_id=uri or maybe_text(record.get("cid")),
                title=truncate_text(text, 120),
                text=text,
                url=bluesky_uri_to_url(uri, author_handle),
                author_name=author_handle,
                channel_name=maybe_text(record.get("author_did")),
                language=",".join(record.get("langs", [])) if isinstance(record.get("langs"), list) else "",
                query_text="",
                published_at_utc=maybe_text(record.get("timestamp_utc"))
                or to_rfc3339_z(parse_loose_datetime(record.get("created_at"))),
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


def extract_reggov_resource(record: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if "detail" in record:
        detail = record.get("detail")
        if isinstance(detail, dict):
            resource = detail.get("data") if isinstance(detail.get("data"), dict) else detail.get("data")
            if isinstance(resource, dict):
                return resource, {"response_url": record.get("response_url"), "validation": record.get("validation")}
    return record if "attributes" in record else None, {}


def normalize_reggov_resource(
    path: Path,
    record: dict[str, Any],
    *,
    index: int,
    run_id: str,
    round_id: str,
    source_skill: str,
    sha256_value: str,
) -> dict[str, Any] | None:
    resource, metadata = extract_reggov_resource(record)
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
    return make_public_signal(
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
    for index, record in enumerate(collect_records(payload)):
        normalized = normalize_reggov_resource(
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
        excerpts = strip_simple_html(maybe_text(record.get("excerpts")))
        text = abstract or excerpts or title
        signals.append(
            make_public_signal(
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
                    "regulation_id_numbers": record.get("regulation_id_numbers")
                    if isinstance(record.get("regulation_id_numbers"), list)
                    else [],
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
    payload = parse_source_payload(path)
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
    cache_path = normalize_cache_path(
        run_dir,
        domain="public",
        source_skill=source_skill,
        run_id=run_id,
        round_id=round_id,
        artifact_sha256=artifact_sha256,
    )
    cached = read_cache_payload(cache_path)
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
    write_cache_payload(
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


__all__ = [
    "NORMALIZE_CACHE_VERSION",
    "bluesky_uri_to_url",
    "extract_reggov_resource",
    "normalize_public_from_bluesky",
    "normalize_public_from_federal_register",
    "normalize_public_from_gdelt_doc",
    "normalize_public_from_gdelt_manifest",
    "normalize_public_from_reggov",
    "normalize_public_from_youtube_comments",
    "normalize_public_from_youtube_videos",
    "normalize_public_source",
    "normalize_public_source_cached",
    "normalize_reggov_resource",
]

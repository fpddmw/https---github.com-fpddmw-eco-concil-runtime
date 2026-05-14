#!/usr/bin/env python3
"""Normalize fetch-gdelt-doc-search artifacts into a local signal-plane SQLite file."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.planes.signal import (  # noqa: E402
    enrich_signal_metadata_fields,
    ensure_signal_plane_schema,
)

SKILL_NAME = "normalize-gdelt-doc-public-signals"
SOURCE_SKILL = "fetch-gdelt-doc-search"
PLANE = "public"
CANONICAL_OBJECT_KIND = "public-discourse-signal"
DOC_TONE_SEMANTICS = "media_or_document_tone_not_public_response_sentiment"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    window_start_utc TEXT NOT NULL DEFAULT '',
    window_end_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    bbox_json TEXT NOT NULL DEFAULT '{}',
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    engagement_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT 'null',
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_normalized_signals_round_plane ON normalized_signals(run_id, round_id, plane);
CREATE INDEX IF NOT EXISTS idx_normalized_signals_artifact ON normalized_signals(artifact_path, record_locator);
"""

INSERT_SQL = """
INSERT OR REPLACE INTO normalized_signals (
    signal_id, run_id, round_id, plane, batch_id, source_skill, signal_kind,
    canonical_object_kind,
    external_id, dedupe_key, title, body_text, url, author_name, channel_name,
    language, query_text, metric, numeric_value, unit, published_at_utc,
    observed_at_utc, window_start_utc, window_end_utc, captured_at_utc,
    latitude, longitude, bbox_json, quality_flags_json, engagement_json,
    metadata_json, raw_json, artifact_path, record_locator, artifact_sha256
) VALUES (
    :signal_id, :run_id, :round_id, :plane, :batch_id, :source_skill, :signal_kind,
    :canonical_object_kind,
    :external_id, :dedupe_key, :title, :body_text, :url, :author_name, :channel_name,
    :language, :query_text, :metric, :numeric_value, :unit, :published_at_utc,
    :observed_at_utc, :window_start_utc, :window_end_utc, :captured_at_utc,
    :latitude, :longitude, :bbox_json, :quality_flags_json, :engagement_json,
    :metadata_json, :raw_json, :artifact_path, :record_locator, :artifact_sha256
)
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def maybe_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = maybe_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def gdelt_doc_datetime_to_iso(value: Any) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    for pattern in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            pass
    return text


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "signal_plane.sqlite"


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return default_db_path(run_dir)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str) -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    ensure_signal_plane_schema(connection)
    return connection, file_path


def artifact_ref(signal: dict[str, Any]) -> dict[str, str]:
    artifact_path = maybe_text(signal.get("artifact_path"))
    record_locator = maybe_text(signal.get("record_locator"))
    return {
        "signal_id": signal["signal_id"],
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": f"{artifact_path}:{record_locator}",
    }


def delete_existing_rows(connection: sqlite3.Connection, run_id: str, round_id: str, artifact_path: str) -> None:
    connection.execute(
        "DELETE FROM normalized_signals WHERE run_id = ? AND round_id = ? AND source_skill = ? AND artifact_path = ?",
        (run_id, round_id, SOURCE_SKILL, artifact_path),
    )


def insert_signals(connection: sqlite3.Connection, signals: list[dict[str, Any]]) -> None:
    for signal in signals:
        connection.execute(INSERT_SQL, enrich_signal_metadata_fields(signal))


def public_gap_hints(signals: list[dict[str, Any]]) -> list[str]:
    if not signals:
        return ["No public signals were normalized from the provided artifact."]
    hints: list[str] = []
    article_signals = [signal for signal in signals if maybe_text(signal.get("signal_kind")) == "article"]
    tone_signals = [signal for signal in signals if maybe_text(signal.get("metric")).startswith("doc_")]
    missing_title = sum(1 for signal in article_signals if not maybe_text(signal.get("title")))
    missing_time = sum(1 for signal in article_signals if not maybe_text(signal.get("published_at_utc")))
    missing_url = sum(1 for signal in article_signals if not maybe_text(signal.get("url")))
    if missing_title:
        hints.append(f"{missing_title} public signals are missing a title field.")
    if missing_time:
        hints.append(f"{missing_time} public signals are missing a publication timestamp.")
    if missing_url:
        hints.append(f"{missing_url} public signals are missing a canonical source URL.")
    if tone_signals:
        hints.append("GDELT DOC tone signals describe media/document tone, not public response sentiment.")
    if len(signals) < 2:
        hints.append("Public coverage is shallow; the artifact produced fewer than two signals.")
    return hints


def public_challenge_hints() -> list[str]:
    return [
        "GDELT doc results can overrepresent repeated pickup around the same narrative.",
        "GDELT DOC tone/tonechart/timelinetone values must not be treated as public sentiment proportions.",
    ]


def unwrap_doc_payload(payload: Any) -> tuple[Any, dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"], {
            "request_url": maybe_text(payload.get("request_url")),
            "request_urls": payload.get("request_urls") if isinstance(payload.get("request_urls"), list) else [],
            "content_type": maybe_text(payload.get("content_type")),
        }
    return payload, {}


def query_text_from_payload(payload: dict[str, Any], query_text_override: str) -> str:
    if maybe_text(query_text_override):
        return maybe_text(query_text_override)
    query_details = payload.get("query_details")
    if isinstance(query_details, dict):
        return maybe_text(query_details.get("title")) or maybe_text(payload.get("query"))
    return maybe_text(payload.get("query"))


def truncate_records(
    records: list[Any],
    *,
    max_records: int,
    label: str,
    warnings: list[dict[str, str]],
) -> list[Any]:
    if max_records > 0 and len(records) > max_records:
        warnings.append(
            {
                "code": "max-records-truncated",
                "message": f"Truncated GDELT {label} from {len(records)} to {max_records}.",
            }
        )
        return records[:max_records]
    return records


def common_signal_fields(
    *,
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    query_text: str,
    captured_at: str,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "round_id": round_id,
        "plane": PLANE,
        "batch_id": "",
        "source_skill": SOURCE_SKILL,
        "canonical_object_kind": CANONICAL_OBJECT_KIND,
        "query_text": query_text,
        "captured_at_utc": captured_at,
        "latitude": None,
        "longitude": None,
        "bbox_json": json.dumps({}, ensure_ascii=True, sort_keys=True),
        "quality_flags_json": json.dumps([], ensure_ascii=True, sort_keys=True),
        "engagement_json": json.dumps({}, ensure_ascii=True, sort_keys=True),
        "artifact_path": str(artifact_path),
        "artifact_sha256": artifact_sha256,
    }


def build_article_signals(
    *,
    articles: list[Any],
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    query_text: str,
    captured_at: str,
    request_metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    common = common_signal_fields(
        run_id=run_id,
        round_id=round_id,
        artifact_path=artifact_path,
        artifact_sha256=artifact_sha256,
        query_text=query_text,
        captured_at=captured_at,
    )
    for index, article in enumerate(articles):
        if not isinstance(article, dict):
            continue
        title = maybe_text(article.get("title"))
        body_text = maybe_text(article.get("seendate_text") or article.get("snippet") or article.get("summary") or title)
        url = maybe_text(article.get("url"))
        domain = maybe_text(article.get("domain"))
        signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, "article", index, title, url)[:16]
        metadata = {
            "domain": domain,
            "gdelt_doc_kind": "gdelt_doc_recon",
            "tone_semantics": "article_recon_no_normalized_tone",
            **request_metadata,
        }
        signals.append(
            {
                **common,
                "signal_id": signal_id,
                "signal_kind": "article",
                "external_id": url,
                "dedupe_key": url or title or str(index),
                "title": title,
                "body_text": body_text,
                "url": url,
                "author_name": "",
                "channel_name": domain,
                "language": maybe_text(article.get("language")),
                "metric": "",
                "numeric_value": None,
                "unit": "",
                "published_at_utc": maybe_text(article.get("seendate") or article.get("published_at") or article.get("date")),
                "observed_at_utc": "",
                "window_start_utc": "",
                "window_end_utc": "",
                "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                "raw_json": json.dumps(article, ensure_ascii=True, sort_keys=True),
                "record_locator": f"$.articles[{index}]",
            }
        )
    return signals


def timeline_tone_points(payload: dict[str, Any]) -> list[tuple[int, int, dict[str, Any], str]]:
    timeline = payload.get("timeline")
    if not isinstance(timeline, list):
        return []
    points: list[tuple[int, int, dict[str, Any], str]] = []
    for series_index, series in enumerate(timeline):
        if not isinstance(series, dict):
            continue
        series_name = maybe_text(series.get("series"))
        if "tone" not in series_name.casefold():
            continue
        data = series.get("data")
        if isinstance(data, list):
            for point_index, point in enumerate(data):
                if isinstance(point, dict):
                    points.append((series_index, point_index, point, series_name))
        elif "date" in series and "value" in series:
            points.append((series_index, 0, series, series_name))
    return points


def build_timeline_tone_signals(
    *,
    points: list[tuple[int, int, dict[str, Any], str]],
    payload: dict[str, Any],
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    query_text: str,
    captured_at: str,
    request_metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    common = common_signal_fields(
        run_id=run_id,
        round_id=round_id,
        artifact_path=artifact_path,
        artifact_sha256=artifact_sha256,
        query_text=query_text,
        captured_at=captured_at,
    )
    query_details = payload.get("query_details") if isinstance(payload.get("query_details"), dict) else {}
    for series_index, point_index, point, series_name in points:
        value = maybe_number(point.get("value"))
        observed_at = gdelt_doc_datetime_to_iso(point.get("date"))
        signal_id = "sig-" + stable_hash(
            run_id,
            round_id,
            SOURCE_SKILL,
            artifact_sha256,
            "timelinetone",
            series_index,
            point_index,
            observed_at,
            value,
        )[:16]
        metadata = {
            "gdelt_doc_kind": "gdelt_doc_tone_aggregate",
            "gdelt_tone_kind": "gdelt_media_tone",
            "tone_metric_name": "DOC TimelineTone Average Tone",
            "tone_semantics": DOC_TONE_SEMANTICS,
            "doc_mode": "timelinetone",
            "series": series_name,
            "query_details": query_details,
            **request_metadata,
        }
        title = f"GDELT DOC TimelineTone {series_name} {observed_at or point_index}"
        body_text = f"series={series_name}; date={maybe_text(point.get('date'))}; value={value}; query={query_text}"
        signals.append(
            {
                **common,
                "signal_id": signal_id,
                "signal_kind": "doc-timeline-tone",
                "external_id": f"timelinetone:{series_index}:{point_index}:{observed_at}",
                "dedupe_key": f"timelinetone:{series_index}:{point_index}:{observed_at}:{value}",
                "title": title,
                "body_text": body_text,
                "url": "",
                "author_name": "",
                "channel_name": "GDELT DOC API",
                "language": "",
                "metric": "doc_timeline_tone",
                "numeric_value": value,
                "unit": "tone",
                "published_at_utc": "",
                "observed_at_utc": observed_at,
                "window_start_utc": observed_at,
                "window_end_utc": observed_at,
                "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                "raw_json": json.dumps(point, ensure_ascii=True, sort_keys=True),
                "record_locator": f"$.timeline[{series_index}].data[{point_index}]",
            }
        )
    return signals


def build_tonechart_signals(
    *,
    bins: list[Any],
    payload: dict[str, Any],
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    query_text: str,
    captured_at: str,
    request_metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    common = common_signal_fields(
        run_id=run_id,
        round_id=round_id,
        artifact_path=artifact_path,
        artifact_sha256=artifact_sha256,
        query_text=query_text,
        captured_at=captured_at,
    )
    query_details = payload.get("query_details") if isinstance(payload.get("query_details"), dict) else {}
    for index, item in enumerate(bins):
        if not isinstance(item, dict):
            continue
        tone_bin = maybe_number(item.get("bin"))
        count = maybe_number(item.get("count"))
        top_articles = item.get("toparts") if isinstance(item.get("toparts"), list) else []
        signal_id = "sig-" + stable_hash(
            run_id,
            round_id,
            SOURCE_SKILL,
            artifact_sha256,
            "tonechart",
            index,
            tone_bin,
            count,
        )[:16]
        metadata = {
            "gdelt_doc_kind": "gdelt_doc_tone_distribution",
            "gdelt_tone_kind": "gdelt_media_tone",
            "tone_metric_name": "DOC ToneChart article count by tone bin",
            "tone_semantics": DOC_TONE_SEMANTICS,
            "doc_mode": "tonechart",
            "tone_bin": tone_bin,
            "tone_bin_semantics": "GDELT DOC tone bucket; numeric_value is article count, not tone value",
            "top_articles": top_articles[:10],
            "query_details": query_details,
            **request_metadata,
        }
        body_text = f"tone_bin={tone_bin}; count={count}; query={query_text}"
        signals.append(
            {
                **common,
                "signal_id": signal_id,
                "signal_kind": "doc-tonechart-bin",
                "external_id": f"tonechart:{tone_bin}",
                "dedupe_key": f"tonechart:{tone_bin}:{count}",
                "title": f"GDELT DOC ToneChart bin {tone_bin}",
                "body_text": body_text,
                "url": "",
                "author_name": "",
                "channel_name": "GDELT DOC API",
                "language": "",
                "metric": "doc_tonechart_count",
                "numeric_value": count,
                "unit": "article_count",
                "published_at_utc": "",
                "observed_at_utc": "",
                "window_start_utc": "",
                "window_end_utc": "",
                "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                "raw_json": json.dumps(item, ensure_ascii=True, sort_keys=True),
                "record_locator": f"$.tonechart[{index}]",
            }
        )
    return signals


def build_signals(
    payload: Any,
    run_id: str,
    round_id: str,
    artifact_path: Path,
    artifact_sha256: str,
    query_text_override: str,
    max_records: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    payload, request_metadata = unwrap_doc_payload(payload)
    if not isinstance(payload, dict):
        warnings.append({"code": "unsupported-payload", "message": "Expected a JSON object from GDELT DOC API."})
        return [], warnings

    query_text = query_text_from_payload(payload, query_text_override)
    captured_at = utc_now_iso()
    signals: list[dict[str, Any]] = []

    articles = payload.get("articles")
    if isinstance(articles, list):
        article_records = truncate_records(articles, max_records=max_records, label="articles", warnings=warnings)
        signals.extend(
            build_article_signals(
                articles=article_records,
                run_id=run_id,
                round_id=round_id,
                artifact_path=artifact_path,
                artifact_sha256=artifact_sha256,
                query_text=query_text,
                captured_at=captured_at,
                request_metadata=request_metadata,
            )
        )

    timeline_points = timeline_tone_points(payload)
    if timeline_points:
        timeline_records = truncate_records(
            timeline_points,
            max_records=max_records,
            label="timeline tone points",
            warnings=warnings,
        )
        signals.extend(
            build_timeline_tone_signals(
                points=timeline_records,
                payload=payload,
                run_id=run_id,
                round_id=round_id,
                artifact_path=artifact_path,
                artifact_sha256=artifact_sha256,
                query_text=query_text,
                captured_at=captured_at,
                request_metadata=request_metadata,
            )
        )
    elif isinstance(payload.get("timeline"), list):
        warnings.append(
            {
                "code": "non-tone-timeline-ignored",
                "message": "Payload has a DOC timeline, but no tone series was detected; use a timeline/volume-specific normalizer or fetch DOC timelinetone for tone signals.",
            }
        )

    tonechart = payload.get("tonechart")
    if isinstance(tonechart, list):
        tonechart_records = truncate_records(
            tonechart,
            max_records=max_records,
            label="tonechart bins",
            warnings=warnings,
        )
        signals.extend(
            build_tonechart_signals(
                bins=tonechart_records,
                payload=payload,
                run_id=run_id,
                round_id=round_id,
                artifact_path=artifact_path,
                artifact_sha256=artifact_sha256,
                query_text=query_text,
                captured_at=captured_at,
                request_metadata=request_metadata,
            )
        )

    if not isinstance(articles, list) and not timeline_points and not isinstance(tonechart, list):
        warnings.append(
            {
                "code": "unsupported-doc-shape",
                "message": "Expected one of payload.articles, payload.timeline/timelinetone, or payload.tonechart.",
            }
        )
    if not signals:
        warnings.append({"code": "no-signals", "message": "No GDELT DOC records produced normalized signals."})
    return signals, warnings


def normalize_gdelt_doc_public_signals(
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_path: str,
    db_path: str,
    query_text_override: str,
    max_records: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    artifact_file = Path(artifact_path).expanduser().resolve()
    payload = read_json(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    signals, warnings = build_signals(
        payload,
        run_id,
        round_id,
        artifact_file,
        artifact_sha256,
        query_text_override,
        max_records,
    )
    batch_id = "sigbatch-" + stable_hash(SKILL_NAME, run_id, round_id, artifact_sha256)[:16]
    for signal in signals:
        signal["batch_id"] = batch_id
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        delete_existing_rows(connection, run_id, round_id, str(artifact_file))
        insert_signals(connection, signals)
        connection.commit()
    finally:
        connection.close()
    artifact_refs = [artifact_ref(signal) for signal in signals]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "plane": PLANE,
            "source_skill": SOURCE_SKILL,
            "signal_count": len(signals),
            "warning_count": len(warnings),
            "db_path": str(db_file),
        },
        "receipt_id": "normalize-receipt-" + stable_hash(SKILL_NAME, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": artifact_refs,
        "canonical_ids": [signal["signal_id"] for signal in signals],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [signal["signal_id"] for signal in signals],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": public_gap_hints(signals),
            "challenge_hints": public_challenge_hints(),
            "suggested_next_skills": ["query-public-signals"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize fetch-gdelt-doc-search artifacts into a local signal-plane SQLite file.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--query-text-override", default="")
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_gdelt_doc_public_signals(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        artifact_path=args.artifact_path,
        db_path=args.db_path,
        query_text_override=args.query_text_override,
        max_records=args.max_records,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

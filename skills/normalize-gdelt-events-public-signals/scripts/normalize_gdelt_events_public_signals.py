#!/usr/bin/env python3
"""Normalize fetch-gdelt-events exports into row-level public signal-plane rows."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.gdelt_export_normalizer import (  # noqa: E402
    cleanup_artifact_paths,
    domain_from_url,
    download_records,
    first_text,
    iter_rows_for_download,
    parse_compact_utc,
    raw_row_record,
    record_locator,
)
from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    base_signal,
    file_sha256,
    finalize_normalization_streaming,
    maybe_number,
    maybe_text,
    pretty_json,
    read_json,
    stable_hash,
    utc_now_iso,
)

SKILL_NAME = "normalize-gdelt-events-public-signals"
SOURCE_SKILL = "fetch-gdelt-events"
PLANE = "public"

EVENT_FIELDS = [
    "GLOBALEVENTID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_FullName",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_FullName",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_FullName",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]
EVENT_INDEX = {name: index for index, name in enumerate(EVENT_FIELDS)}


def field_text(columns: list[str], field_name: str) -> str:
    index = EVENT_INDEX[field_name]
    return maybe_text(columns[index]) if index < len(columns) else ""


def build_manifest_fallback_signal(
    *,
    download: dict[str, Any],
    download_index: int,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    artifact_sha256: str,
) -> dict[str, Any]:
    entry = download.get("entry") if isinstance(download.get("entry"), dict) else {}
    timestamp_utc = maybe_text(entry.get("timestamp_utc"))
    url = maybe_text(entry.get("url"))
    signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, artifact_sha256, "fallback", timestamp_utc, url, download_index)[:16]
    return base_signal(
        signal_id=signal_id,
        run_id=run_id,
        round_id=round_id,
        plane=PLANE,
        source_skill=SOURCE_SKILL,
        signal_kind="export-download",
        external_id=timestamp_utc or f"download-{download_index}",
        dedupe_key=url or f"{timestamp_utc}:{download_index}",
        title=f"GDELT events export {timestamp_utc}" if timestamp_utc else "GDELT events export",
        body_text=" | ".join(maybe_text(item) for item in download.get("preview_lines", [])[:3] if maybe_text(item)),
        url=url,
        author_name="",
        channel_name="GDELT Events",
        language="",
        query_text="",
        metric="file_size_bytes",
        numeric_value=maybe_number(entry.get("size_bytes")),
        unit="bytes",
        published_at_utc=timestamp_utc,
        observed_at_utc="",
        window_start_utc="",
        window_end_utc="",
        captured_at_utc=utc_now_iso(),
        latitude=None,
        longitude=None,
        quality_flags=["raw-export-manifest", "fallback-no-zip-rows"],
        engagement={},
        metadata={
            "download_output_path": maybe_text(download.get("output_path")),
            "md5": maybe_text(entry.get("md5")),
            "preview_member": maybe_text(download.get("preview_member")),
            "validation": download.get("validation") if isinstance(download.get("validation"), dict) else {},
        },
        raw_record=download,
        artifact_path=artifact_file,
        record_locator=f"$.downloads[{download_index}]",
        artifact_sha256=artifact_sha256,
    )


def build_row_signal(
    *,
    columns: list[str],
    download_index: int,
    row_artifact_path: Path,
    row_artifact_sha256: str,
    member_name: str,
    row_index: int,
    request_url: str,
    entry: dict[str, Any],
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    global_event_id = field_text(columns, "GLOBALEVENTID") or f"event-{row_index}"
    source_url = first_text(field_text(columns, "SOURCEURL"), request_url)
    actor1 = first_text(field_text(columns, "Actor1Name"), field_text(columns, "Actor1Code"))
    actor2 = first_text(field_text(columns, "Actor2Name"), field_text(columns, "Actor2Code"))
    event_code = first_text(field_text(columns, "EventBaseCode"), field_text(columns, "EventCode"), field_text(columns, "EventRootCode"))
    action_geo = field_text(columns, "ActionGeo_FullName")
    title_core = " ".join(part for part in (actor1 or "Actor1", event_code or "event", actor2 or "Actor2") if part).strip()
    title = title_core if title_core else f"GDELT event {global_event_id}"
    if action_geo:
        title = f"{title} @ {action_geo}"

    avg_tone = maybe_number(field_text(columns, "AvgTone"))
    num_mentions = maybe_number(field_text(columns, "NumMentions"))
    num_sources = maybe_number(field_text(columns, "NumSources"))
    num_articles = maybe_number(field_text(columns, "NumArticles"))
    published_at = first_text(parse_compact_utc(field_text(columns, "DATEADDED")), maybe_text(entry.get("timestamp_utc")))
    observed_at = parse_compact_utc(field_text(columns, "SQLDATE"))
    latitude = maybe_number(field_text(columns, "ActionGeo_Lat"))
    longitude = maybe_number(field_text(columns, "ActionGeo_Long"))
    signal_id = "sig-" + stable_hash(run_id, round_id, SOURCE_SKILL, row_artifact_sha256, global_event_id, row_index)[:16]
    quality_flags = [
        "root-event" if field_text(columns, "IsRootEvent") == "1" else "",
        f"quadclass:{field_text(columns, 'QuadClass')}" if field_text(columns, "QuadClass") else "",
        f"event-root:{field_text(columns, 'EventRootCode')}" if field_text(columns, "EventRootCode") else "",
    ]
    body_parts = [
        f"actor1={actor1}" if actor1 else "",
        f"actor2={actor2}" if actor2 else "",
        f"event_code={event_code}" if event_code else "",
        f"geo={action_geo}" if action_geo else "",
        f"num_mentions={int(num_mentions)}" if num_mentions is not None else "",
        f"num_articles={int(num_articles)}" if num_articles is not None else "",
        f"avg_tone={avg_tone}" if avg_tone is not None else "",
    ]
    return base_signal(
        signal_id=signal_id,
        run_id=run_id,
        round_id=round_id,
        plane=PLANE,
        source_skill=SOURCE_SKILL,
        signal_kind="event-row",
        external_id=global_event_id,
        dedupe_key=global_event_id,
        title=title,
        body_text="; ".join(part for part in body_parts if part),
        url=source_url,
        author_name="",
        channel_name=first_text(domain_from_url(source_url), "GDELT Events"),
        language="",
        query_text="",
        metric="avg_tone",
        numeric_value=avg_tone,
        unit="score",
        published_at_utc=published_at,
        observed_at_utc=observed_at,
        window_start_utc="",
        window_end_utc="",
        captured_at_utc=utc_now_iso(),
        latitude=latitude,
        longitude=longitude,
        quality_flags=[flag for flag in quality_flags if flag],
        engagement={
            "num_mentions": num_mentions,
            "num_sources": num_sources,
            "num_articles": num_articles,
            "goldstein_scale": maybe_number(field_text(columns, "GoldsteinScale")),
        },
        metadata={
            "sql_date": field_text(columns, "SQLDATE"),
            "date_added": field_text(columns, "DATEADDED"),
            "event_code": field_text(columns, "EventCode"),
            "event_base_code": field_text(columns, "EventBaseCode"),
            "event_root_code": field_text(columns, "EventRootCode"),
            "quad_class": field_text(columns, "QuadClass"),
            "action_geo": {
                "full_name": field_text(columns, "ActionGeo_FullName"),
                "country_code": field_text(columns, "ActionGeo_CountryCode"),
                "adm1_code": field_text(columns, "ActionGeo_ADM1Code"),
                "feature_id": field_text(columns, "ActionGeo_FeatureID"),
            },
            "actor1": {
                "code": field_text(columns, "Actor1Code"),
                "name": field_text(columns, "Actor1Name"),
                "country_code": field_text(columns, "Actor1CountryCode"),
            },
            "actor2": {
                "code": field_text(columns, "Actor2Code"),
                "name": field_text(columns, "Actor2Name"),
                "country_code": field_text(columns, "Actor2CountryCode"),
            },
            "request_url": request_url,
            "member_name": member_name,
            "row_index": row_index,
        },
        raw_record=raw_row_record(
            field_names=EVENT_FIELDS,
            columns=columns,
            download_index=download_index,
            row_index=row_index,
            artifact_path=row_artifact_path,
            artifact_sha256=row_artifact_sha256,
            member_name=member_name,
            request_url=request_url,
            entry=entry,
        ),
        artifact_path=row_artifact_path,
        record_locator=record_locator(member_name, row_index),
        artifact_sha256=row_artifact_sha256,
    )


def build_signal_stream(
    payload: Any,
    run_id: str,
    round_id: str,
    artifact_file: Path,
    artifact_sha256: str,
    *,
    max_rows_per_download: int,
    max_total_rows: int,
) -> tuple[Any, list[dict[str, str]], list[str]]:
    warnings: list[dict[str, str]] = []
    downloads = download_records(payload, warnings)
    cleanup_paths = cleanup_artifact_paths(downloads, artifact_file)
    state = {"count": 0, "total_limit_reached": False}

    def generator():
        for download_index, download in enumerate(downloads):
            if max_total_rows > 0 and state["count"] >= max_total_rows:
                state["total_limit_reached"] = True
                warnings.append(
                    {
                        "code": "total-row-limit-reached",
                        "message": f"{SOURCE_SKILL} truncated normalization at {max_total_rows} total rows.",
                    }
                )
                break
            produced_for_download = 0
            for row in iter_rows_for_download(
                download=download,
                download_index=download_index,
                expected_columns=len(EVENT_FIELDS),
                source_label=SOURCE_SKILL,
                warnings=warnings,
            ):
                if max_rows_per_download > 0 and produced_for_download >= max_rows_per_download:
                    warnings.append(
                        {
                            "code": "download-row-limit-reached",
                            "message": f"{SOURCE_SKILL} truncated one download at {max_rows_per_download} rows.",
                        }
                    )
                    break
                if max_total_rows > 0 and state["count"] >= max_total_rows:
                    state["total_limit_reached"] = True
                    break
                yield build_row_signal(
                    columns=row.columns,
                    download_index=row.download_index,
                    row_artifact_path=row.artifact_path,
                    row_artifact_sha256=row.artifact_sha256,
                    member_name=row.member_name,
                    row_index=row.row_index,
                    request_url=row.request_url,
                    entry=row.entry,
                    run_id=run_id,
                    round_id=round_id,
                )
                produced_for_download += 1
                state["count"] += 1
            if state["total_limit_reached"]:
                if produced_for_download > 0:
                    warnings.append(
                        {
                            "code": "total-row-limit-reached",
                            "message": f"{SOURCE_SKILL} truncated normalization at {max_total_rows} total rows.",
                        }
                    )
                break
            if produced_for_download == 0:
                yield build_manifest_fallback_signal(
                    download=download,
                    download_index=download_index,
                    run_id=run_id,
                    round_id=round_id,
                    artifact_file=artifact_file,
                    artifact_sha256=artifact_sha256,
                )
                state["count"] += 1
        if state["count"] == 0:
            warnings.append({"code": "no-signals", "message": "No GDELT events rows produced normalized signals."})

    return generator(), warnings, cleanup_paths


def normalize_gdelt_events(
    run_dir: str,
    run_id: str,
    round_id: str,
    artifact_path: str,
    db_path: str,
    *,
    max_rows_per_download: int,
    max_total_rows: int,
    artifact_ref_limit: int,
    canonical_id_limit: int,
) -> dict[str, Any]:
    artifact_file = Path(artifact_path).expanduser().resolve()
    artifact_payload = read_json(artifact_file)
    artifact_sha256 = file_sha256(artifact_file)
    signals, warnings, cleanup_paths = build_signal_stream(
        artifact_payload,
        run_id,
        round_id,
        artifact_file,
        artifact_sha256,
        max_rows_per_download=max_rows_per_download,
        max_total_rows=max_total_rows,
    )
    return finalize_normalization_streaming(
        skill_name=SKILL_NAME,
        source_skill=SOURCE_SKILL,
        plane=PLANE,
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        artifact_file=artifact_file,
        db_path=db_path,
        signals=signals,
        warnings=warnings,
        cleanup_artifact_paths=cleanup_paths,
        artifact_ref_limit=artifact_ref_limit,
        canonical_id_limit=canonical_id_limit,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize fetch-gdelt-events exports into row-level public signal-plane rows.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--max-rows-per-download", type=int, default=0)
    parser.add_argument("--max-total-rows", type=int, default=0)
    parser.add_argument("--artifact-ref-limit", type=int, default=200)
    parser.add_argument("--canonical-id-limit", type=int, default=200)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = normalize_gdelt_events(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        artifact_path=args.artifact_path,
        db_path=args.db_path,
        max_rows_per_download=args.max_rows_per_download,
        max_total_rows=args.max_total_rows,
        artifact_ref_limit=args.artifact_ref_limit,
        canonical_id_limit=args.canonical_id_limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Route OpenAQ fetch tasks to API or S3 based on mode and workload profile."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

import openaq_api_client as api_client
import openaq_s3_fetch as s3_fetch


def parse_api_query_args(items: list[str]) -> dict[str, str]:
    return api_client.parse_key_value_args(items)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_scope_from_query(query: dict[str, str]) -> dict[str, Any]:
    scoped_keys = {
        "countries_id",
        "locations_id",
        "sensors_id",
        "parameters_id",
        "providers_id",
        "datetime_from",
        "datetime_to",
        "date_from",
        "date_to",
        "limit",
        "page",
    }
    return {key: value for key, value in query.items() if key in scoped_keys and value}


def build_fetch_contract(
    *,
    operation_kind: str,
    transport: str,
    access_path: str,
    query: dict[str, str] | None = None,
    archive: dict[str, Any] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    resolved_query = query or {}
    archive_payload = archive or {}
    return {
        "source_skill": "fetch-openaq",
        "operation_kind": operation_kind,
        "output_object_kind": "raw-artifact",
        "research_judgement": "none",
        "generated_at_utc": utc_now_iso(),
        "source_provenance": {
            "provider": "OpenAQ",
            "transport": transport,
            "access_path": access_path,
            "dry_run": bool(dry_run),
        },
        "temporal_scope": {
            "start_utc": resolved_query.get("datetime_from")
            or resolved_query.get("date_from")
            or archive_payload.get("start_utc")
            or "",
            "end_utc": resolved_query.get("datetime_to")
            or resolved_query.get("date_to")
            or archive_payload.get("end_utc")
            or "",
            "archive_year": archive_payload.get("year"),
            "archive_month": archive_payload.get("month"),
            "archive_day": archive_payload.get("day"),
            "archive_hour": archive_payload.get("hour"),
        },
        "spatial_scope": {
            **compact_scope_from_query(resolved_query),
            **{
                key: value
                for key, value in {
                    "location_id": archive_payload.get("location_id"),
                    "prefix": archive_payload.get("prefix"),
                }.items()
                if value not in (None, "")
            },
        },
        "data_quality": {
            "quality_flags": [
                "raw-provider-fetch",
                operation_kind,
                f"transport-{transport}",
            ],
            "normalization_scope": "not-normalized",
        },
        "coverage_limitations": [
            "OpenAQ coverage depends on provider, station, sensor, parameter, archive partition, and API availability.",
            "Fetched rows are raw evidence inputs and do not establish exposure, compliance, causation, or policy conclusions by themselves.",
        ],
    }


def dry_run_payload(
    *,
    args: argparse.Namespace,
    operation_kind: str,
    transport: str,
    access_path: str,
    query: dict[str, str] | None = None,
    archive: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not getattr(args, "dry_run", False):
        return None
    return {
        "operation_kind": operation_kind,
        "dry_run": True,
        "fetch_contract": build_fetch_contract(
            operation_kind=operation_kind,
            transport=transport,
            access_path=access_path,
            query=query,
            archive=archive,
            dry_run=True,
        ),
        "planned_request": {
            "transport": transport,
            "access_path": access_path,
            "query": query or {},
            "archive": archive or {},
        },
    }


def choose_source_mode(args: argparse.Namespace) -> str:
    if args.source_mode in {"api", "s3"}:
        return args.source_mode

    explicit_s3_intent = any(
        [
            bool(args.s3_key),
            bool(args.s3_prefix),
            args.location_id is not None,
            args.year is not None,
            args.s3_action != "ls",
        ]
    )
    if args.volume_profile == "full-backfill" or explicit_s3_intent:
        return "s3"
    return "api"


def run_api(args: argparse.Namespace) -> dict[str, Any]:
    if not args.api_path:
        raise ValueError("--api-path is required when using API mode.")

    query = parse_api_query_args(args.api_query)
    api_key = api_client.env_required(api_client.ENV_API_KEY)

    if args.api_all_pages:
        data = api_client.fetch_all_pages(
            base_url=args.api_base_url,
            path=args.api_path,
            query=query,
            api_key=api_key,
            timeout=args.api_timeout,
            max_pages=args.api_max_pages,
        )
        return {
            "mode": "api",
            "request_mode": "all-pages",
            "path": args.api_path,
            "data": data,
        }

    data, headers, request_url = api_client.fetch_one_page(
        base_url=args.api_base_url,
        path=args.api_path,
        query=query,
        api_key=api_key,
        timeout=args.api_timeout,
    )
    return {
        "mode": "api",
        "request_mode": "single-page",
        "path": args.api_path,
        "request_url": request_url,
        "rate_limit_headers": {
            "x-ratelimit-used": headers.get("x-ratelimit-used"),
            "x-ratelimit-reset": headers.get("x-ratelimit-reset"),
            "x-ratelimit-limit": headers.get("x-ratelimit-limit"),
            "x-ratelimit-remaining": headers.get("x-ratelimit-remaining"),
        },
        "data": data,
    }


def run_s3(args: argparse.Namespace) -> dict[str, Any]:
    bucket = s3_fetch.resolve_bucket(args.s3_bucket)
    region = s3_fetch.resolve_region()

    if args.s3_action == "build-prefix":
        if args.location_id is None or args.year is None:
            raise ValueError("--location-id and --year are required for --s3-action build-prefix.")
        prefix = s3_fetch.build_record_prefix(
            location_id=args.location_id,
            year=args.year,
            month=args.month,
            day=args.day,
            hour=args.hour,
        )
        return {
            "mode": "s3",
            "action": "build-prefix",
            "bucket": bucket,
            "region": region,
            "prefix": prefix,
        }

    if args.s3_action == "download":
        if not args.s3_key:
            raise ValueError("--s3-key is required for --s3-action download.")
        if not args.s3_output:
            raise ValueError("--s3-output is required for --s3-action download.")
        data = s3_fetch.download_key(
            bucket=bucket,
            region=region,
            key=args.s3_key,
            output=args.s3_output,
            timeout=args.s3_timeout,
        )
        return {
            "mode": "s3",
            "action": "download",
            "data": data,
        }

    data = s3_fetch.list_objects(
        bucket=bucket,
        region=region,
        prefix=args.s3_prefix,
        delimiter=args.s3_delimiter,
        max_keys=args.s3_max_keys,
        continuation_token=args.s3_continuation_token,
        timeout=args.s3_timeout,
    )
    return {
        "mode": "s3",
        "action": "ls",
        "data": data,
    }


def add_api_transport_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--api-base-url",
        default=api_client.DEFAULT_BASE_URL,
        help=f"API base URL (default: {api_client.DEFAULT_BASE_URL}).",
    )
    parser.add_argument(
        "--api-query",
        action="append",
        default=[],
        help="API query pair key=value. Repeat for multiple pairs.",
    )
    parser.add_argument("--api-timeout", type=int, default=60, help="API timeout in seconds.")
    parser.add_argument("--api-all-pages", action="store_true", help="Merge API paged results.")
    parser.add_argument("--api-max-pages", type=int, default=100, help="Max pages to fetch when --api-all-pages is set.")
    parser.add_argument("--dry-run", action="store_true", help="Emit the raw fetch contract without calling OpenAQ.")


def api_response_payload(
    *,
    args: argparse.Namespace,
    operation_kind: str,
    path: str,
    query: dict[str, str],
) -> dict[str, Any]:
    planned = dry_run_payload(
        args=args,
        operation_kind=operation_kind,
        transport="api",
        access_path=path,
        query=query,
    )
    if planned is not None:
        return planned

    api_key = api_client.env_required(api_client.ENV_API_KEY)
    if args.api_all_pages:
        data = api_client.fetch_all_pages(
            base_url=args.api_base_url,
            path=path,
            query=query,
            api_key=api_key,
            timeout=args.api_timeout,
            max_pages=args.api_max_pages,
        )
        request_mode = "all-pages"
        request_url = ""
        headers: dict[str, str] = {}
    else:
        data, headers, request_url = api_client.fetch_one_page(
            base_url=args.api_base_url,
            path=path,
            query=query,
            api_key=api_key,
            timeout=args.api_timeout,
        )
        request_mode = "single-page"

    return {
        "operation_kind": operation_kind,
        "mode": "api",
        "request_mode": request_mode,
        "path": path,
        "request_url": request_url,
        "rate_limit_headers": {
            "x-ratelimit-used": headers.get("x-ratelimit-used"),
            "x-ratelimit-reset": headers.get("x-ratelimit-reset"),
            "x-ratelimit-limit": headers.get("x-ratelimit-limit"),
            "x-ratelimit-remaining": headers.get("x-ratelimit-remaining"),
        },
        "fetch_contract": build_fetch_contract(
            operation_kind=operation_kind,
            transport="api",
            access_path=path,
            query=query,
        ),
        "data": data,
    }


def run_metadata_fetch(args: argparse.Namespace) -> dict[str, Any]:
    entity = args.entity.strip("/")
    path = args.api_path or f"/v3/{entity}"
    return api_response_payload(
        args=args,
        operation_kind="metadata-fetch",
        path=path,
        query=parse_api_query_args(args.api_query),
    )


def measurement_path(args: argparse.Namespace) -> str:
    if args.api_path:
        return args.api_path
    sensor_id = str(args.sensor_id or "").strip()
    if sensor_id:
        suffix = "" if args.aggregation == "raw" else f"/{args.aggregation}"
        return f"/v3/sensors/{sensor_id}/measurements{suffix}"
    return "/v3/measurements"


def run_measurement_fetch(args: argparse.Namespace) -> dict[str, Any]:
    query = parse_api_query_args(args.api_query)
    for key, value in (
        ("datetime_from", args.datetime_from),
        ("datetime_to", args.datetime_to),
        ("locations_id", args.locations_id),
        ("parameters_id", args.parameters_id),
    ):
        if value and key not in query:
            query[key] = str(value)
    return api_response_payload(
        args=args,
        operation_kind="measurement-fetch",
        path=measurement_path(args),
        query=query,
    )


def archive_scope(args: argparse.Namespace, *, prefix: str) -> dict[str, Any]:
    return {
        "location_id": args.location_id,
        "year": args.year,
        "month": args.month,
        "day": args.day,
        "hour": args.hour,
        "prefix": prefix,
    }


def run_archive_backfill_fetch(args: argparse.Namespace) -> dict[str, Any]:
    bucket = s3_fetch.resolve_bucket(args.s3_bucket)
    region = s3_fetch.resolve_region()
    if args.s3_prefix:
        prefix = args.s3_prefix
    else:
        if args.location_id is None or args.year is None:
            raise ValueError("--location-id and --year are required when --s3-prefix is not supplied.")
        prefix = s3_fetch.build_record_prefix(
            location_id=args.location_id,
            year=args.year,
            month=args.month,
            day=args.day,
            hour=args.hour,
        )
    archive = archive_scope(args, prefix=prefix)
    planned = dry_run_payload(
        args=args,
        operation_kind="archive-backfill-fetch",
        transport="s3",
        access_path=prefix,
        archive=archive,
    )
    if planned is not None:
        return planned

    if args.s3_download_key:
        if not args.s3_output:
            raise ValueError("--s3-output is required when --s3-download-key is supplied.")
        data = s3_fetch.download_key(
            bucket=bucket,
            region=region,
            key=args.s3_download_key,
            output=args.s3_output,
            timeout=args.s3_timeout,
        )
        action = "download"
    else:
        data = s3_fetch.list_objects(
            bucket=bucket,
            region=region,
            prefix=prefix,
            delimiter=args.s3_delimiter,
            max_keys=args.s3_max_keys,
            continuation_token=args.s3_continuation_token,
            timeout=args.s3_timeout,
        )
        action = "ls"

    return {
        "operation_kind": "archive-backfill-fetch",
        "mode": "s3",
        "action": action,
        "fetch_contract": build_fetch_contract(
            operation_kind="archive-backfill-fetch",
            transport="s3",
            access_path=prefix,
            archive=archive,
        ),
        "data": data,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Route OpenAQ requests to API or S3.")
    sub = parser.add_subparsers(dest="command", required=True)

    fetch = sub.add_parser("fetch", help="Fetch OpenAQ data with auto/API/S3 mode.")
    fetch.add_argument(
        "--source-mode",
        choices=["auto", "api", "s3"],
        default="auto",
        help="Select source mode. auto chooses based on workload profile and provided args.",
    )
    fetch.add_argument(
        "--volume-profile",
        choices=["interactive", "batch", "full-backfill"],
        default="interactive",
        help="Workload profile used by auto mode.",
    )
    fetch.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch.add_argument("--api-path", default="", help="OpenAQ API path, e.g. /v3/locations.")
    fetch.add_argument(
        "--api-base-url",
        default=api_client.DEFAULT_BASE_URL,
        help=f"API base URL (default: {api_client.DEFAULT_BASE_URL}).",
    )
    fetch.add_argument(
        "--api-query",
        action="append",
        default=[],
        help="API query pair key=value. Repeat for multiple pairs.",
    )
    fetch.add_argument("--api-timeout", type=int, default=60, help="API timeout in seconds.")
    fetch.add_argument("--api-all-pages", action="store_true", help="Merge API paged results.")
    fetch.add_argument(
        "--api-max-pages",
        type=int,
        default=100,
        help="Max pages to fetch when --api-all-pages is set.",
    )

    fetch.add_argument("--s3-bucket", default="", help="S3 bucket override.")
    fetch.add_argument(
        "--s3-action",
        choices=["ls", "download", "build-prefix"],
        default="ls",
        help="S3 action to run when S3 mode is selected.",
    )
    fetch.add_argument("--s3-prefix", default="", help="Prefix for S3 list action.")
    fetch.add_argument("--s3-delimiter", default="", help="Delimiter for S3 list action.")
    fetch.add_argument("--s3-max-keys", type=int, default=1000, help="Max keys for S3 list.")
    fetch.add_argument(
        "--s3-continuation-token",
        default="",
        help="Continuation token for S3 list action.",
    )
    fetch.add_argument("--s3-key", default="", help="S3 key for download action.")
    fetch.add_argument("--s3-output", default="", help="Output file path for download action.")
    fetch.add_argument("--s3-timeout", type=int, default=120, help="S3 timeout in seconds.")

    fetch.add_argument("--location-id", type=int, default=None, help="Location ID for prefix build.")
    fetch.add_argument("--year", type=int, default=None, help="Year for prefix build.")
    fetch.add_argument("--month", type=int, default=None, help="Month for prefix build.")
    fetch.add_argument("--day", type=int, default=None, help="Day for prefix build.")
    fetch.add_argument("--hour", type=int, default=None, help="Hour for prefix build.")

    discovery = sub.add_parser("fetch-metadata", help="Fetch OpenAQ metadata via API v3.")
    discovery.add_argument(
        "--entity",
        choices=["countries", "providers", "locations", "sensors", "parameters", "manufacturers", "instruments"],
        default="locations",
        help="Metadata entity under /v3.",
    )
    discovery.add_argument("--api-path", default="", help="Override the API path.")
    discovery.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    add_api_transport_args(discovery)

    measurements = sub.add_parser("fetch-measurements", help="Fetch OpenAQ measurement rows via API v3.")
    measurements.add_argument("--sensor-id", type=int, default=None, help="Sensor id for /v3/sensors/{id}/measurements.")
    measurements.add_argument(
        "--aggregation",
        choices=["raw", "hourly", "daily"],
        default="raw",
        help="Sensor measurements endpoint variant.",
    )
    measurements.add_argument("--api-path", default="", help="Override the API path.")
    measurements.add_argument("--datetime-from", default="", help="Measurement window start.")
    measurements.add_argument("--datetime-to", default="", help="Measurement window end.")
    measurements.add_argument("--locations-id", default="", help="OpenAQ locations_id query value.")
    measurements.add_argument("--parameters-id", default="", help="OpenAQ parameters_id query value.")
    measurements.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    add_api_transport_args(measurements)

    archive = sub.add_parser("fetch-archive-backfill", help="List or download OpenAQ S3 archive partitions.")
    archive.add_argument("--s3-bucket", default="", help="S3 bucket override.")
    archive.add_argument("--s3-prefix", default="", help="Prefix for S3 list action.")
    archive.add_argument("--s3-delimiter", default="", help="Delimiter for S3 list action.")
    archive.add_argument("--s3-max-keys", type=int, default=1000, help="Max keys for S3 list.")
    archive.add_argument("--s3-continuation-token", default="", help="Continuation token from previous list.")
    archive.add_argument("--s3-download-key", default="", help="Optional S3 key to download instead of listing.")
    archive.add_argument("--s3-output", default="", help="Output file path or directory when downloading.")
    archive.add_argument("--s3-timeout", type=int, default=120, help="S3 timeout in seconds.")
    archive.add_argument("--location-id", type=int, default=None, help="Location ID for prefix build.")
    archive.add_argument("--year", type=int, default=None, help="Year for prefix build.")
    archive.add_argument("--month", type=int, default=None, help="Month 1-12.")
    archive.add_argument("--day", type=int, default=None, help="Day 1-31.")
    archive.add_argument("--hour", type=int, default=None, help="Hour 0-23.")
    archive.add_argument("--dry-run", action="store_true", help="Emit the raw fetch contract without calling S3.")
    archive.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
    )


def command_fetch(args: argparse.Namespace) -> int:
    try:
        selected_mode = choose_source_mode(args)
        if selected_mode == "api":
            result = run_api(args)
        else:
            result = run_s3(args)
    except (ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    payload = {
        "operation_kind": "auto-routed-fetch",
        "source_mode_requested": args.source_mode,
        "source_mode_selected": selected_mode,
        "volume_profile": args.volume_profile,
        "fetch_contract": build_fetch_contract(
            operation_kind="auto-routed-fetch",
            transport=selected_mode,
            access_path=args.api_path if selected_mode == "api" else (args.s3_prefix or args.s3_key or "s3-list"),
            query=parse_api_query_args(args.api_query) if selected_mode == "api" else {},
            archive={
                "location_id": args.location_id,
                "year": args.year,
                "month": args.month,
                "day": args.day,
                "hour": args.hour,
                "prefix": args.s3_prefix,
            }
            if selected_mode == "s3"
            else {},
        ),
        "result": result,
    }
    print_json(payload, pretty=args.pretty)
    return 0


def command_operation(args: argparse.Namespace, runner: Any) -> int:
    try:
        payload = runner(args)
    except (ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print_json(payload, pretty=args.pretty)
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "fetch":
        return command_fetch(args)
    if args.command == "fetch-metadata":
        return command_operation(args, run_metadata_fetch)
    if args.command == "fetch-measurements":
        return command_operation(args, run_measurement_fetch)
    if args.command == "fetch-archive-backfill":
        return command_operation(args, run_archive_backfill_fetch)
    print(f"Unsupported command: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Fetch USBR RISE catalog items and time-series result records."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "USBR_RISE_BASE_URL"
ENV_TIMEOUT_SECONDS = "USBR_RISE_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "USBR_RISE_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "USBR_RISE_RETRY_BACKOFF_SECONDS"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "USBR_RISE_MIN_REQUEST_INTERVAL_SECONDS"
ENV_PAGE_SIZE = "USBR_RISE_PAGE_SIZE"
ENV_MAX_PAGES_PER_RUN = "USBR_RISE_MAX_PAGES_PER_RUN"
ENV_MAX_RECORDS_PER_RUN = "USBR_RISE_MAX_RECORDS_PER_RUN"
ENV_USER_AGENT = "USBR_RISE_USER_AGENT"

DEFAULT_BASE_URL = "https://data.usbr.gov/rise/api"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.5
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES_PER_RUN = 20
DEFAULT_MAX_RECORDS_PER_RUN = 2000
DEFAULT_USER_AGENT = "fetch-usbr-rise/1.0"

SKILL_NAME = "fetch-usbr-rise"
SCHEMA_VERSION = "fetch-usbr-rise-v1"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    min_request_interval_seconds: float
    page_size: int
    max_pages_per_run: int
    max_records_per_run: int
    user_agent: str


@dataclass(frozen=True)
class HttpJsonResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any]
    byte_length: int


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def maybe_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def env_or_default(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def parse_positive_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def parse_non_negative_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got: {value}")
    return value


def parse_non_negative_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got: {value}")
    return value


def normalize_base_url(value: str) -> str:
    normalized = maybe_text(value).rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith(("http://", "https://")):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized}")
    return normalized


def ensure_page_size(value: int) -> int:
    if value < 1 or value > 1000:
        raise ValueError(f"Page size must be between 1 and 1000, got: {value}")
    return value


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    return RuntimeConfig(
        base_url=normalize_base_url(
            args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)
        ),
        timeout_seconds=parse_positive_int(
            "--timeout-seconds",
            str(args.timeout_seconds if args.timeout_seconds is not None else env_or_default(ENV_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))),
        ),
        max_retries=parse_non_negative_int(
            "--max-retries",
            str(args.max_retries if args.max_retries is not None else env_or_default(ENV_MAX_RETRIES, str(DEFAULT_MAX_RETRIES))),
        ),
        retry_backoff_seconds=parse_non_negative_float(
            "--retry-backoff-seconds",
            str(args.retry_backoff_seconds if args.retry_backoff_seconds is not None else env_or_default(ENV_RETRY_BACKOFF_SECONDS, str(DEFAULT_RETRY_BACKOFF_SECONDS))),
        ),
        min_request_interval_seconds=parse_non_negative_float(
            "--min-request-interval-seconds",
            str(args.min_request_interval_seconds if args.min_request_interval_seconds is not None else env_or_default(ENV_MIN_REQUEST_INTERVAL_SECONDS, str(DEFAULT_MIN_REQUEST_INTERVAL_SECONDS))),
        ),
        page_size=ensure_page_size(
            parse_positive_int(
                "--page-size",
                str(args.page_size if args.page_size is not None else env_or_default(ENV_PAGE_SIZE, str(DEFAULT_PAGE_SIZE))),
            )
        ),
        max_pages_per_run=parse_positive_int(
            "--max-pages-per-run",
            str(args.max_pages_per_run if args.max_pages_per_run is not None else env_or_default(ENV_MAX_PAGES_PER_RUN, str(DEFAULT_MAX_PAGES_PER_RUN))),
        ),
        max_records_per_run=parse_positive_int(
            "--max-records-per-run",
            str(args.max_records_per_run if args.max_records_per_run is not None else env_or_default(ENV_MAX_RECORDS_PER_RUN, str(DEFAULT_MAX_RECORDS_PER_RUN))),
        ),
        user_agent=maybe_text(
            args.user_agent if args.user_agent is not None else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)
        ),
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger(SKILL_NAME)
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if log_file.strip():
        log_path = Path(log_file).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def error_excerpt(payload: bytes) -> str:
    return payload.decode("utf-8", errors="replace").strip()[:400]


class RetryableHttpClient:
    def __init__(self, config: RuntimeConfig, logger: logging.Logger) -> None:
        self._cfg = config
        self._logger = logger
        self._last_request_monotonic: float | None = None

    def _throttle(self) -> None:
        if self._last_request_monotonic is None:
            return
        elapsed = time.monotonic() - self._last_request_monotonic
        sleep_seconds = self._cfg.min_request_interval_seconds - elapsed
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    def get_json(self, url: str) -> HttpJsonResponse:
        attempts = self._cfg.max_retries + 1
        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("Accept", "application/ld+json, application/json")
            req.add_header("User-Agent", self._cfg.user_agent)
            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    raw = resp.read()
                    self._last_request_monotonic = time.monotonic()
                    payload = json.loads(raw.decode("utf-8"))
                    if not isinstance(payload, dict):
                        raise RuntimeError(f"Response JSON root must be object for {url}.")
                    return HttpJsonResponse(
                        url=url,
                        status_code=int(getattr(resp, "status", 200)),
                        headers={k.lower(): v for k, v in resp.headers.items()},
                        payload=payload,
                        byte_length=len(raw),
                    )
            except HTTPError as exc:
                self._last_request_monotonic = time.monotonic()
                status = int(exc.code)
                body = exc.read()
                if status in RETRIABLE_HTTP_CODES and attempt < attempts:
                    delay = self._cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                    self._logger.warning("http-retry status=%d delay=%.2fs url=%s", status, delay, url)
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"HTTP {status} for {url}: {error_excerpt(body)!r}") from exc
            except (URLError, TimeoutError, ConnectionResetError, json.JSONDecodeError) as exc:
                self._last_request_monotonic = time.monotonic()
                if attempt < attempts:
                    delay = self._cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                    self._logger.warning("network-or-json-retry delay=%.2fs url=%s err=%s", delay, url, exc)
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Request failed for {url}: {exc}") from exc
        raise RuntimeError(f"Failed to fetch after retries: {url}")


def valid_date_time(text: str, option_name: str) -> str:
    value = maybe_text(text)
    if not value:
        return ""
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{option_name} must be ISO datetime, got: {text!r}") from exc
    return value


def read_item_ids(args: argparse.Namespace) -> list[str]:
    values = [maybe_text(item) for item in args.item_id if maybe_text(item)]
    if args.item_ids_file:
        path = Path(args.item_ids_file).expanduser().resolve()
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if text and not text.startswith("#"):
                values.append(text)
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    if not result:
        raise ValueError("At least one --item-id or --item-ids-file entry is required.")
    return result


def build_result_url(
    config: RuntimeConfig,
    *,
    item_id: str,
    args: argparse.Namespace,
    page: int,
) -> str:
    query: list[tuple[str, str]] = [
        ("itemId", item_id),
        ("itemsPerPage", str(config.page_size)),
        ("page", str(page)),
    ]
    if args.location_id:
        query.append(("locationId", maybe_text(args.location_id)))
    if args.parameter_id:
        query.append(("parameterId", maybe_text(args.parameter_id)))
    if args.after_utc:
        query.append(("dateTime[after]", valid_date_time(args.after_utc, "--after-utc")))
    if args.before_utc:
        query.append(("dateTime[before]", valid_date_time(args.before_utc, "--before-utc")))
    if args.order_date_time:
        query.append(("order[dateTime]", maybe_text(args.order_date_time)))
    return f"{config.base_url}/result?{parse.urlencode(query)}"


def build_catalog_url(config: RuntimeConfig, *, page: int) -> str:
    query = [
        ("itemsPerPage", str(config.page_size)),
        ("page", str(page)),
    ]
    return f"{config.base_url}/catalog-item?{parse.urlencode(query)}"


def build_item_url(config: RuntimeConfig, item_id: str) -> str:
    return f"{config.base_url}/catalog-item/{parse.quote(item_id)}"


def item_metadata_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "item_id": maybe_text(payload.get("id")),
        "item_title": maybe_text(payload.get("itemTitle") or payload.get("dcat:title")),
        "item_description": maybe_text(payload.get("itemDescription") or payload.get("dcat:description")),
        "location_id": maybe_text(payload.get("locationId")),
        "location_source_code": maybe_text(payload.get("locationSourceCode")),
        "parameter_id": maybe_text(payload.get("parameterId")),
        "parameter_name": maybe_text(payload.get("parameterName")),
        "parameter_unit": maybe_text(payload.get("parameterUnit")),
        "parameter_timestep": maybe_text(payload.get("parameterTimestep")),
        "parameter_transformation": maybe_text(payload.get("parameterTransformation")),
        "parameter_group": maybe_text(payload.get("parameterGroup")),
        "source_code": maybe_text(payload.get("sourceCode")),
        "temporal_start_date": maybe_text(payload.get("temporalStartDate")),
        "temporal_end_date": maybe_text(payload.get("temporalEndDate")),
        "disclaimer": maybe_text(payload.get("disclaimer")),
        "landing_page": maybe_text(payload.get("dcat:landingPage")),
        "spatial": payload.get("dcat:spatial") if isinstance(payload.get("dcat:spatial"), dict) else {},
    }


def normalize_catalog_item(payload: dict[str, Any]) -> dict[str, Any]:
    spatial = payload.get("dcat:spatial") if isinstance(payload.get("dcat:spatial"), dict) else {}
    return {
        "source_skill": SKILL_NAME,
        "record_source": "USBR RISE catalog-item",
        "record_id": maybe_text(payload.get("id") or payload.get("@id")),
        "item_id": maybe_text(payload.get("id")),
        "item_api_path": maybe_text(payload.get("@id")),
        "item_title": maybe_text(payload.get("itemTitle") or payload.get("dcat:title")),
        "item_description": maybe_text(payload.get("itemDescription") or payload.get("dcat:description")),
        "location_id": maybe_text(payload.get("locationId")),
        "location_name": maybe_text(payload.get("locationName")),
        "location_source_code": maybe_text(payload.get("locationSourceCode")),
        "parameter_id": maybe_text(payload.get("parameterId")),
        "parameter_name": maybe_text(payload.get("parameterName")),
        "parameter_unit": maybe_text(payload.get("parameterUnit")),
        "parameter_group": maybe_text(payload.get("parameterGroup")),
        "parameter_timestep": maybe_text(payload.get("parameterTimestep")),
        "parameter_transformation": maybe_text(payload.get("parameterTransformation")),
        "source_code": maybe_text(payload.get("sourceCode")),
        "temporal_start_date": maybe_text(payload.get("temporalStartDate")),
        "temporal_end_date": maybe_text(payload.get("temporalEndDate")),
        "landing_page": maybe_text(payload.get("dcat:landingPage")),
        "spatial": spatial,
        "raw": payload,
    }


def searchable_catalog_text(item: dict[str, Any]) -> str:
    text_parts: list[str] = []
    for key in (
        "item_id",
        "item_title",
        "item_description",
        "location_id",
        "location_name",
        "location_source_code",
        "parameter_id",
        "parameter_name",
        "parameter_unit",
        "parameter_group",
        "source_code",
        "landing_page",
    ):
        text_parts.append(maybe_text(item.get(key)))
    return " ".join(part for part in text_parts if part).lower()


def split_terms(values: list[str]) -> list[str]:
    terms: list[str] = []
    for value in values:
        for term in maybe_text(value).replace(",", " ").split():
            normalized = term.strip().lower()
            if normalized:
                terms.append(normalized)
    return terms


def contains_filter(item: dict[str, Any], key: str, expected: str) -> bool:
    expected_text = maybe_text(expected).lower()
    if not expected_text:
        return True
    return expected_text in maybe_text(item.get(key)).lower()


def catalog_item_matches(item: dict[str, Any], args: argparse.Namespace) -> bool:
    haystack = searchable_catalog_text(item)
    if any(term not in haystack for term in split_terms(args.query)):
        return False
    if not contains_filter(item, "item_title", args.item_title_contains):
        return False
    if not contains_filter(item, "location_name", args.location_name_contains):
        return False
    if not contains_filter(item, "parameter_name", args.parameter_name_contains):
        return False
    if args.parameter_id and maybe_text(item.get("parameter_id")) != maybe_text(args.parameter_id):
        return False
    if args.location_id and maybe_text(item.get("location_id")) != maybe_text(args.location_id):
        return False
    if args.source_code and maybe_text(item.get("source_code")).lower() != maybe_text(args.source_code).lower():
        return False
    return True


def override_metadata(args: argparse.Namespace, item_id: str) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "item_title": maybe_text(args.item_title),
        "item_description": "",
        "location_id": maybe_text(args.location_id),
        "location_name": maybe_text(args.location_name),
        "parameter_id": maybe_text(args.parameter_id),
        "parameter_name": maybe_text(args.parameter_name),
        "parameter_unit": maybe_text(args.parameter_unit),
        "parameter_timestep": "",
        "parameter_transformation": "",
        "parameter_group": "",
        "source_code": "",
        "temporal_start_date": "",
        "temporal_end_date": "",
        "disclaimer": "",
        "landing_page": "",
        "spatial": {
            "type": "Point",
            "coordinates": [maybe_number(args.longitude), maybe_number(args.latitude)],
        }
        if args.latitude and args.longitude
        else {},
    }


def metadata_value(metadata: dict[str, Any], key: str, fallback: str = "") -> str:
    value = maybe_text(metadata.get(key))
    return value or fallback


def point_coordinates(metadata: dict[str, Any]) -> tuple[float | None, float | None]:
    spatial = metadata.get("spatial") if isinstance(metadata.get("spatial"), dict) else {}
    coords = spatial.get("coordinates") if isinstance(spatial.get("coordinates"), list) else []
    if len(coords) >= 2:
        lon = maybe_number(coords[0])
        lat = maybe_number(coords[1])
        return lat, lon
    return None, None


def normalize_result_record(record: dict[str, Any], *, item_metadata: dict[str, Any]) -> dict[str, Any]:
    item_id = maybe_text(record.get("itemId")) or metadata_value(item_metadata, "item_id")
    parameter_id = maybe_text(record.get("parameterId")) or metadata_value(item_metadata, "parameter_id")
    location_id = maybe_text(record.get("locationId")) or metadata_value(item_metadata, "location_id")
    lat, lon = point_coordinates(item_metadata)
    return {
        "source_skill": SKILL_NAME,
        "record_source": "USBR RISE",
        "record_id": maybe_text(record.get("id")) or f"{item_id}:{location_id}:{parameter_id}:{maybe_text(record.get('dateTime'))}",
        "item_id": item_id,
        "location_id": location_id,
        "location_name": metadata_value(item_metadata, "location_name"),
        "parameter_id": parameter_id,
        "parameter_name": metadata_value(item_metadata, "parameter_name"),
        "parameter_unit": metadata_value(item_metadata, "parameter_unit"),
        "parameter_group": metadata_value(item_metadata, "parameter_group"),
        "parameter_timestep": metadata_value(item_metadata, "parameter_timestep"),
        "parameter_transformation": metadata_value(item_metadata, "parameter_transformation"),
        "source_code": maybe_text(record.get("sourceCode")) or metadata_value(item_metadata, "source_code"),
        "observed_at_utc": maybe_text(record.get("dateTime")),
        "value": record.get("result"),
        "status": maybe_text(record.get("status")),
        "last_update": maybe_text(record.get("lastUpdate")),
        "create_date": maybe_text(record.get("createDate")),
        "update_date": maybe_text(record.get("updateDate")),
        "latitude": lat,
        "longitude": lon,
        "item_title": metadata_value(item_metadata, "item_title"),
        "item_description": metadata_value(item_metadata, "item_description"),
        "landing_page": metadata_value(item_metadata, "landing_page"),
        "provider_disclaimer": metadata_value(item_metadata, "disclaimer"),
        "raw": record,
    }


def validate_limits(args: argparse.Namespace, config: RuntimeConfig) -> None:
    if args.max_pages < 1:
        raise ValueError("--max-pages must be >= 1.")
    if args.max_pages > config.max_pages_per_run:
        raise ValueError(f"--max-pages={args.max_pages} exceeds configured cap {config.max_pages_per_run}.")
    if args.max_records < 0:
        raise ValueError("--max-records must be >= 0.")
    if args.max_records > config.max_records_per_run:
        raise ValueError(f"--max-records={args.max_records} exceeds configured cap {config.max_records_per_run}.")


def validate_start_page(value: int) -> None:
    if value < 1:
        raise ValueError("--start-page must be >= 1.")


def write_output(path_text: str, payload: dict[str, Any], overwrite: bool) -> str:
    if not path_text.strip():
        return ""
    path = Path(path_text).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise RuntimeError(f"Output file already exists: {path}")
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(json.dumps(payload, ensure_ascii=True, indent=2 if pretty else None, sort_keys=True))


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    print_json(
        {
            "ok": True,
            "config": {
                "base_url": config.base_url,
                "timeout_seconds": config.timeout_seconds,
                "max_retries": config.max_retries,
                "retry_backoff_seconds": config.retry_backoff_seconds,
                "min_request_interval_seconds": config.min_request_interval_seconds,
                "page_size": config.page_size,
                "max_pages_per_run": config.max_pages_per_run,
                "max_records_per_run": config.max_records_per_run,
                "user_agent": config.user_agent,
            },
            "source_urls": {
                "entrypoint": config.base_url,
                "results": f"{config.base_url}/result",
                "catalog_items": f"{config.base_url}/catalog-item",
            },
            "env_keys": {
                "base_url": ENV_BASE_URL,
                "timeout_seconds": ENV_TIMEOUT_SECONDS,
                "max_retries": ENV_MAX_RETRIES,
                "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
                "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
                "page_size": ENV_PAGE_SIZE,
                "max_pages_per_run": ENV_MAX_PAGES_PER_RUN,
                "max_records_per_run": ENV_MAX_RECORDS_PER_RUN,
                "user_agent": ENV_USER_AGENT,
            },
        },
        args.pretty,
    )
    return 0


def command_discover_items(args: argparse.Namespace) -> int:
    logger = build_logger(args.log_level, args.log_file)
    config = build_runtime_config(args)
    validate_limits(args, config)
    validate_start_page(args.start_page)

    first_url = build_catalog_url(config, page=args.start_page)
    if args.dry_run:
        print_json(
            {
                "ok": True,
                "dry_run": True,
                "request_plan": {
                    "source_skill": SKILL_NAME,
                    "source": "usbr-rise-catalog-items",
                    "base_url": config.base_url,
                    "start_page": args.start_page,
                    "max_pages": args.max_pages,
                    "max_records": args.max_records,
                    "query_terms": [maybe_text(value) for value in args.query if maybe_text(value)],
                    "client_filters": {
                        "item_title_contains": maybe_text(args.item_title_contains),
                        "location_name_contains": maybe_text(args.location_name_contains),
                        "parameter_name_contains": maybe_text(args.parameter_name_contains),
                        "parameter_id": maybe_text(args.parameter_id),
                        "location_id": maybe_text(args.location_id),
                        "source_code": maybe_text(args.source_code),
                    },
                    "list_semantics": "Catalog candidates are returned in provider/page scan order after client-side filtering; this is not source ranking or evidence weighting.",
                },
                "sample_request_url": first_url,
            },
            args.pretty,
        )
        return 0

    client = RetryableHttpClient(config, logger)
    records: list[dict[str, Any]] = []
    page_summaries: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    stop_reason = "completed"
    page = args.start_page
    pages_fetched = 0

    if not any(
        [
            args.query,
            maybe_text(args.item_title_contains),
            maybe_text(args.location_name_contains),
            maybe_text(args.parameter_name_contains),
            maybe_text(args.parameter_id),
            maybe_text(args.location_id),
            maybe_text(args.source_code),
        ]
    ):
        warnings.append(
            {
                "code": "broad-catalog-discovery",
                "message": "No query or filter was supplied; candidates reflect provider/page order only.",
            }
        )

    while True:
        if pages_fetched >= args.max_pages:
            stop_reason = "max_pages_reached"
            break
        if args.max_records > 0 and len(records) >= args.max_records:
            stop_reason = "max_records_reached"
            break
        url = build_catalog_url(config, page=page)
        response = client.get_json(url)
        members = response.payload.get("member")
        if not isinstance(members, list):
            warnings.append({"code": "missing-member", "message": f"Expected member list for catalog page={page}."})
            stop_reason = "invalid_response_shape"
            break
        normalized_members = [normalize_catalog_item(member) for member in members if isinstance(member, dict)]
        matching_members = [item for item in normalized_members if catalog_item_matches(item, args)]
        if args.max_records > 0:
            matching_members = matching_members[: args.max_records - len(records)]
        records.extend(matching_members)
        page_summaries.append(
            {
                "page": page,
                "request_url": url,
                "status_code": response.status_code,
                "byte_length": response.byte_length,
                "provider_total_items": response.payload.get("totalItems"),
                "provider_member_count": len(members),
                "matched_record_count": len(matching_members),
            }
        )
        pages_fetched += 1
        view = response.payload.get("view") if isinstance(response.payload.get("view"), dict) else {}
        has_next = bool(maybe_text(view.get("next")))
        if args.max_records > 0 and len(records) >= args.max_records:
            stop_reason = "max_records_reached"
            break
        if not members:
            stop_reason = "empty_page"
            break
        if not has_next:
            break
        page += 1

    if stop_reason == "max_pages_reached":
        warnings.append(
            {
                "code": "catalog-scan-incomplete",
                "message": "Discovery stopped at the configured page cap; zero or sparse candidates do not prove that matching RISE catalog items are absent.",
            }
        )

    artifact: dict[str, Any] = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "source": "usbr-rise-catalog-items",
        "source_skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "discovery_mode": "catalog-page-scan-client-filter",
        "list_semantics": "Catalog candidates are returned in provider/page scan order after client-side filtering; this is not source ranking or evidence weighting.",
        "source_parameters": {
            "base_url": config.base_url,
            "page_size": config.page_size,
            "start_page": args.start_page,
            "max_pages": args.max_pages,
            "max_records": args.max_records,
        },
        "query_parameters": {
            "query_terms": [maybe_text(value) for value in args.query if maybe_text(value)],
            "item_title_contains": maybe_text(args.item_title_contains),
            "location_name_contains": maybe_text(args.location_name_contains),
            "parameter_name_contains": maybe_text(args.parameter_name_contains),
            "parameter_id": maybe_text(args.parameter_id),
            "location_id": maybe_text(args.location_id),
            "source_code": maybe_text(args.source_code),
        },
        "candidate_item_ids": [item.get("item_id") for item in records if maybe_text(item.get("item_id"))],
        "records": records,
        "records_fetched": len(records),
        "pages_fetched": pages_fetched,
        "stop_reason": stop_reason,
        "page_summaries": page_summaries,
        "validation_summary": {"warning_count": len(warnings), "record_count": len(records)},
        "warnings": warnings,
        "provenance": {
            "provider": "Bureau of Reclamation RISE",
            "provider_api": "https://data.usbr.gov/rise/api",
            "source_note": "RISE catalog discovery grounds candidate item IDs for later bounded result fetches; it does not fetch operational result rows or decide report conclusions.",
        },
        "artifact_refs": [],
        "output_file": "",
    }
    output_file = write_output(args.output, artifact, args.overwrite)
    if output_file:
        artifact["output_file"] = output_file
        artifact["artifact_refs"] = [{"artifact_path": output_file, "record_locator": "$"}]
        Path(output_file).write_text(json.dumps(artifact, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print_json(artifact, args.pretty)
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(args.log_level, args.log_file)
    config = build_runtime_config(args)
    validate_limits(args, config)
    validate_start_page(args.start_page)
    item_ids = read_item_ids(args)

    first_url = build_result_url(config, item_id=item_ids[0], args=args, page=args.start_page)
    if args.dry_run:
        print_json(
            {
                "ok": True,
                "dry_run": True,
                "request_plan": {
                    "source_skill": SKILL_NAME,
                    "base_url": config.base_url,
                    "item_ids": item_ids,
                    "start_page": args.start_page,
                    "max_pages": args.max_pages,
                    "max_records": args.max_records,
                    "include_item_metadata": args.include_item_metadata,
                    "date_time_filters": {
                        "after_utc": maybe_text(args.after_utc),
                        "before_utc": maybe_text(args.before_utc),
                        "order_date_time": maybe_text(args.order_date_time),
                    },
                },
                "sample_request_url": first_url,
            },
            args.pretty,
        )
        return 0

    client = RetryableHttpClient(config, logger)
    records: list[dict[str, Any]] = []
    page_summaries: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    item_metadata_map: dict[str, dict[str, Any]] = {}
    stop_reason = "completed"

    for item_id in item_ids:
        item_metadata = override_metadata(args, item_id)
        if args.include_item_metadata:
            try:
                metadata_response = client.get_json(build_item_url(config, item_id))
                item_metadata = {**item_metadata, **item_metadata_from_payload(metadata_response.payload)}
            except Exception as exc:  # noqa: BLE001
                warnings.append(
                    {
                        "code": "item-metadata-fetch-failed",
                        "message": f"Item metadata fetch failed for itemId={item_id}; result rows were kept with available fields. {exc}",
                    }
                )
        item_metadata_map[item_id] = item_metadata
        page = args.start_page
        pages_for_item = 0
        while True:
            if pages_for_item >= args.max_pages:
                stop_reason = "max_pages_reached"
                break
            if args.max_records > 0 and len(records) >= args.max_records:
                stop_reason = "max_records_reached"
                break
            url = build_result_url(config, item_id=item_id, args=args, page=page)
            response = client.get_json(url)
            members = response.payload.get("member")
            if not isinstance(members, list):
                warnings.append({"code": "missing-member", "message": f"Expected member list for itemId={item_id} page={page}."})
                stop_reason = "invalid_response_shape"
                break
            selected = [member for member in members if isinstance(member, dict)]
            if args.max_records > 0:
                selected = selected[: args.max_records - len(records)]
            records.extend(normalize_result_record(member, item_metadata=item_metadata) for member in selected)
            page_summaries.append(
                {
                    "item_id": item_id,
                    "page": page,
                    "request_url": url,
                    "status_code": response.status_code,
                    "byte_length": response.byte_length,
                    "provider_total_items": response.payload.get("totalItems"),
                    "record_count": len(selected),
                }
            )
            pages_for_item += 1
            view = response.payload.get("view") if isinstance(response.payload.get("view"), dict) else {}
            has_next = bool(maybe_text(view.get("next")))
            if not members:
                stop_reason = "empty_page"
                break
            if args.max_records > 0 and len(records) >= args.max_records:
                stop_reason = "max_records_reached"
                break
            if not has_next:
                break
            page += 1
        if args.max_records > 0 and len(records) >= args.max_records:
            break

    artifact: dict[str, Any] = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "source": "usbr-rise-results",
        "source_skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "source_parameters": {
            "base_url": config.base_url,
            "item_ids": item_ids,
            "page_size": config.page_size,
            "max_pages": args.max_pages,
            "max_records": args.max_records,
            "include_item_metadata": args.include_item_metadata,
        },
        "query_parameters": {
            "item_ids": item_ids,
            "location_id": maybe_text(args.location_id),
            "parameter_id": maybe_text(args.parameter_id),
            "after_utc": maybe_text(args.after_utc),
            "before_utc": maybe_text(args.before_utc),
            "order_date_time": maybe_text(args.order_date_time),
        },
        "item_metadata": item_metadata_map,
        "records": records,
        "records_fetched": len(records),
        "pages_fetched": len(page_summaries),
        "stop_reason": stop_reason,
        "page_summaries": page_summaries,
        "validation_summary": {"warning_count": len(warnings), "record_count": len(records)},
        "warnings": warnings,
        "provenance": {
            "provider": "Bureau of Reclamation RISE",
            "provider_api": "https://data.usbr.gov/rise/api",
            "source_note": "RISE result rows are operational/environment records; this fetch does not determine shortage severity, operating compliance, or governance responsibility.",
        },
        "artifact_refs": [],
        "output_file": "",
    }
    output_file = write_output(args.output, artifact, args.overwrite)
    if output_file:
        artifact["output_file"] = output_file
        artifact["artifact_refs"] = [{"artifact_path": output_file, "record_locator": "$"}]
        Path(output_file).write_text(json.dumps(artifact, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print_json(artifact, args.pretty)
    return 0


def add_runtime_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help=f"Default: {DEFAULT_BASE_URL}")
    parser.add_argument("--timeout-seconds", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--retry-backoff-seconds", type=float, default=None)
    parser.add_argument("--min-request-interval-seconds", type=float, default=None)
    parser.add_argument("--page-size", type=int, default=None)
    parser.add_argument("--max-pages-per-run", type=int, default=None)
    parser.add_argument("--max-records-per-run", type=int, default=None)
    parser.add_argument("--user-agent", default=None)


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--log-file", default="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch USBR RISE time-series result records for explicit item IDs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-config")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true")
    check.set_defaults(func=command_check_config)

    discover = subparsers.add_parser("discover-items")
    add_runtime_config_args(discover)
    add_logging_args(discover)
    discover.add_argument("--query", action="append", default=[])
    discover.add_argument("--item-title-contains", default="")
    discover.add_argument("--location-name-contains", default="")
    discover.add_argument("--parameter-name-contains", default="")
    discover.add_argument("--parameter-id", default="")
    discover.add_argument("--location-id", default="")
    discover.add_argument("--source-code", default="")
    discover.add_argument("--start-page", type=int, default=1)
    discover.add_argument("--max-pages", type=int, default=5)
    discover.add_argument("--max-records", type=int, default=50)
    discover.add_argument("--output", default="")
    discover.add_argument("--overwrite", action="store_true")
    discover.add_argument("--dry-run", action="store_true")
    discover.add_argument("--pretty", action="store_true")
    discover.set_defaults(func=command_discover_items)

    fetch = subparsers.add_parser("fetch")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument("--item-id", action="append", default=[])
    fetch.add_argument("--item-ids-file", default="")
    fetch.add_argument("--location-id", default="")
    fetch.add_argument("--parameter-id", default="")
    fetch.add_argument("--after-utc", default="")
    fetch.add_argument("--before-utc", default="")
    fetch.add_argument("--order-date-time", choices=["asc", "desc", ""], default="desc")
    fetch.add_argument("--start-page", type=int, default=1)
    fetch.add_argument("--max-pages", type=int, default=1)
    fetch.add_argument("--max-records", type=int, default=100)
    fetch.add_argument("--include-item-metadata", action="store_true")
    fetch.add_argument("--item-title", default="")
    fetch.add_argument("--location-name", default="")
    fetch.add_argument("--parameter-name", default="")
    fetch.add_argument("--parameter-unit", default="")
    fetch.add_argument("--latitude", default="")
    fetch.add_argument("--longitude", default="")
    fetch.add_argument("--output", default="")
    fetch.add_argument("--overwrite", action="store_true")
    fetch.add_argument("--dry-run", action="store_true")
    fetch.add_argument("--pretty", action="store_true")
    fetch.set_defaults(func=command_fetch)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except Exception as exc:  # noqa: BLE001
        print_json({"ok": False, "error": str(exc), "skill": SKILL_NAME}, getattr(args, "pretty", False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

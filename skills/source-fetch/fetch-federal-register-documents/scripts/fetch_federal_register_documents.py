#!/usr/bin/env python3
"""Fetch FederalRegister.gov document records into a compact governance-record artifact."""

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

ENV_BASE_URL = "FEDERAL_REGISTER_BASE_URL"
ENV_TIMEOUT_SECONDS = "FEDERAL_REGISTER_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "FEDERAL_REGISTER_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "FEDERAL_REGISTER_RETRY_BACKOFF_SECONDS"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "FEDERAL_REGISTER_MIN_REQUEST_INTERVAL_SECONDS"
ENV_PAGE_SIZE = "FEDERAL_REGISTER_PAGE_SIZE"
ENV_MAX_PAGES_PER_RUN = "FEDERAL_REGISTER_MAX_PAGES_PER_RUN"
ENV_MAX_RECORDS_PER_RUN = "FEDERAL_REGISTER_MAX_RECORDS_PER_RUN"
ENV_USER_AGENT = "FEDERAL_REGISTER_USER_AGENT"

DEFAULT_BASE_URL = "https://www.federalregister.gov/api/v1"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.5
DEFAULT_PAGE_SIZE = 20
DEFAULT_MAX_PAGES_PER_RUN = 20
DEFAULT_MAX_RECORDS_PER_RUN = 1000
DEFAULT_USER_AGENT = "fetch-federal-register-documents/1.0"

SKILL_NAME = "fetch-federal-register-documents"
SCHEMA_VERSION = "official-governance-record-fetch-v1"
SOURCE_NAME = "federal-register-documents"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}

DEFAULT_FIELDS = [
    "abstract",
    "action",
    "agencies",
    "agency_names",
    "body_html_url",
    "citation",
    "comments_close_on",
    "dates",
    "docket_id",
    "docket_ids",
    "document_number",
    "effective_on",
    "full_text_xml_url",
    "html_url",
    "json_url",
    "pdf_url",
    "publication_date",
    "raw_text_url",
    "regulation_id_numbers",
    "regulations_dot_gov_url",
    "title",
    "type",
]


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


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
    normalized = value.strip().rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized}")
    return normalized


def ensure_page_size(value: int) -> int:
    if value < 1 or value > 1000:
        raise ValueError(f"Page size must be between 1 and 1000, got: {value}")
    return value


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(
        args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)
    )
    return RuntimeConfig(
        base_url=base_url,
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


def parse_key_value(items: list[str], option_name: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid {option_name} entry {item!r}. Use key=value.")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid {option_name} entry {item!r}. Key cannot be empty.")
        pairs.append((key, value.strip()))
    return pairs


def render_url(base_url: str, query_items: list[tuple[str, str]]) -> str:
    encoded = parse.urlencode(query_items)
    return f"{base_url}/documents.json?{encoded}" if encoded else f"{base_url}/documents.json"


def error_excerpt(payload: bytes) -> str:
    text = payload.decode("utf-8", errors="replace").strip()
    return text[:400]


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
            req.add_header("Accept", "application/json")
            req.add_header("User-Agent", self._cfg.user_agent)
            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    payload_raw = resp.read()
                    self._last_request_monotonic = time.monotonic()
                    status = int(getattr(resp, "status", 200))
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    payload = json.loads(payload_raw.decode("utf-8"))
                    if not isinstance(payload, dict):
                        raise RuntimeError(f"Response JSON root must be object for {url}.")
                    return HttpJsonResponse(url, status, headers, payload, len(payload_raw))
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


def string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [maybe_text(item) for item in value if maybe_text(item)]
    text = maybe_text(value)
    return [text] if text else []


def agency_names(record: dict[str, Any]) -> list[str]:
    names = string_list(record.get("agency_names"))
    agencies = record.get("agencies")
    if isinstance(agencies, list):
        for agency in agencies:
            if isinstance(agency, dict):
                name = maybe_text(agency.get("name")) or maybe_text(agency.get("raw_name"))
                if name and name not in names:
                    names.append(name)
    return names


def normalize_record(record: dict[str, Any], *, index: int) -> dict[str, Any]:
    doc_number = maybe_text(record.get("document_number")) or f"federal-register-{index}"
    agencies = agency_names(record)
    return {
        "source_skill": SKILL_NAME,
        "record_source": "FederalRegister.gov",
        "record_id": doc_number,
        "record_type": "federal_register_document",
        "title": maybe_text(record.get("title")) or f"Federal Register document {doc_number}",
        "agency": "; ".join(agencies),
        "agency_id": "",
        "agency_names": agencies,
        "publication_date": maybe_text(record.get("publication_date")),
        "updated_at": "",
        "url": maybe_text(record.get("html_url")) or maybe_text(record.get("json_url")),
        "document_url": maybe_text(record.get("html_url")),
        "pdf_url": maybe_text(record.get("pdf_url")),
        "raw_text_url": maybe_text(record.get("raw_text_url")),
        "document_type": maybe_text(record.get("type")),
        "docket_ids": string_list(record.get("docket_ids") or record.get("docket_id")),
        "regulation_id_numbers": string_list(record.get("regulation_id_numbers")),
        "comment_period": {"end": maybe_text(record.get("comments_close_on"))},
        "summary": maybe_text(record.get("abstract")),
        "citation": maybe_text(record.get("citation")),
        "provider_record": record,
    }


def build_query_items(args: argparse.Namespace, config: RuntimeConfig, page: int) -> list[tuple[str, str]]:
    items: list[tuple[str, str]] = [
        ("per_page", str(config.page_size)),
        ("page", str(page)),
        ("order", maybe_text(args.order) or "newest"),
    ]
    if maybe_text(args.term):
        items.append(("conditions[term]", maybe_text(args.term)))
    if maybe_text(args.publication_date_gte):
        items.append(("conditions[publication_date][gte]", maybe_text(args.publication_date_gte)))
    if maybe_text(args.publication_date_lte):
        items.append(("conditions[publication_date][lte]", maybe_text(args.publication_date_lte)))
    for agency in args.agency:
        items.append(("conditions[agencies][]", maybe_text(agency)))
    for document_type in args.document_type:
        items.append(("conditions[type][]", maybe_text(document_type)))
    for field in DEFAULT_FIELDS:
        items.append(("fields[]", field))
    items.extend(parse_key_value(args.raw_param, "--raw-param"))
    return items


def validate_limits(args: argparse.Namespace, config: RuntimeConfig) -> None:
    if args.max_pages < 1:
        raise ValueError("--max-pages must be >= 1.")
    if args.max_pages > config.max_pages_per_run:
        raise ValueError(f"--max-pages={args.max_pages} exceeds configured cap {config.max_pages_per_run}.")
    if args.max_records < 0:
        raise ValueError("--max-records must be >= 0.")
    if args.max_records > config.max_records_per_run:
        raise ValueError(f"--max-records={args.max_records} exceeds configured cap {config.max_records_per_run}.")


def build_artifact(
    *,
    args: argparse.Namespace,
    config: RuntimeConfig,
    records: list[dict[str, Any]],
    page_summaries: list[dict[str, Any]],
    stop_reason: str,
    warnings: list[dict[str, str]],
    output_file: str,
) -> dict[str, Any]:
    query_parameters = {
        "term": maybe_text(args.term),
        "agency": [maybe_text(item) for item in args.agency if maybe_text(item)],
        "document_type": [maybe_text(item) for item in args.document_type if maybe_text(item)],
        "publication_date_gte": maybe_text(args.publication_date_gte),
        "publication_date_lte": maybe_text(args.publication_date_lte),
        "order": maybe_text(args.order) or "newest",
        "raw_param": dict(parse_key_value(args.raw_param, "--raw-param")),
    }
    return {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
        "source_skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "source_parameters": {
            "base_url": config.base_url,
            "page_size": config.page_size,
            "max_pages": args.max_pages,
            "max_records": args.max_records,
        },
        "query_parameters": query_parameters,
        "records": records,
        "records_fetched": len(records),
        "pages_fetched": len(page_summaries),
        "stop_reason": stop_reason,
        "page_summaries": page_summaries,
        "validation_summary": {
            "warning_count": len(warnings),
            "record_count": len(records),
        },
        "warnings": warnings,
        "provenance": {
            "provider": "FederalRegister.gov",
            "provider_api": "https://www.federalregister.gov/api/v1/documents.json",
            "source_note": "FederalRegister.gov API returns XML-derived published-document metadata; legal status caveats remain with the provider.",
        },
        "artifact_refs": ([{"artifact_path": output_file, "record_locator": "$"}] if output_file else []),
        "output_file": output_file,
    }


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
    payload = {
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
        "source_urls": {"documents": f"{config.base_url}/documents.json"},
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
    }
    print_json(payload, args.pretty)
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(args.log_level, args.log_file)
    config = build_runtime_config(args)
    validate_limits(args, config)
    first_url = render_url(config.base_url, build_query_items(args, config, args.start_page))
    if args.dry_run:
        payload = {
            "ok": True,
            "dry_run": True,
            "request_plan": {
                "source_skill": SKILL_NAME,
                "base_url": config.base_url,
                "start_page": args.start_page,
                "max_pages": args.max_pages,
                "max_records": args.max_records,
                "query_parameters": build_artifact(
                    args=args,
                    config=config,
                    records=[],
                    page_summaries=[],
                    stop_reason="dry_run",
                    warnings=[],
                    output_file="",
                )["query_parameters"],
            },
            "sample_request_url": first_url,
        }
        print_json(payload, args.pretty)
        return 0

    client = RetryableHttpClient(config, logger)
    records: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    page_summaries: list[dict[str, Any]] = []
    page = args.start_page
    stop_reason = "unknown"

    while True:
        if len(page_summaries) >= args.max_pages:
            stop_reason = "max_pages_reached"
            break
        if args.max_records > 0 and len(records) >= args.max_records:
            stop_reason = "max_records_reached"
            break

        url = render_url(config.base_url, build_query_items(args, config, page))
        response = client.get_json(url)
        results = response.payload.get("results")
        if not isinstance(results, list):
            warnings.append({"code": "missing-results", "message": f"Expected results list on page {page}."})
            stop_reason = "invalid_response_shape"
            break

        selected = [item for item in results if isinstance(item, dict)]
        if args.max_records > 0:
            remaining = args.max_records - len(records)
            selected = selected[:remaining]
        start_index = len(records)
        records.extend(
            normalize_record(record, index=start_index + offset)
            for offset, record in enumerate(selected)
        )
        total_pages = response.payload.get("total_pages")
        page_summaries.append(
            {
                "page": page,
                "request_url": url,
                "status_code": response.status_code,
                "byte_length": response.byte_length,
                "provider_count": response.payload.get("count"),
                "provider_total_pages": total_pages,
                "record_count": len(selected),
            }
        )
        if args.max_records > 0 and len(records) >= args.max_records:
            stop_reason = "max_records_reached"
            break
        if not results:
            stop_reason = "empty_page"
            break
        if isinstance(total_pages, int) and page >= total_pages:
            stop_reason = "reached_total_pages"
            break
        page += 1

    if stop_reason == "unknown":
        stop_reason = "completed"

    artifact = build_artifact(
        args=args,
        config=config,
        records=records,
        page_summaries=page_summaries,
        stop_reason=stop_reason,
        warnings=warnings,
        output_file="",
    )
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
    parser = argparse.ArgumentParser(description="Fetch FederalRegister.gov documents.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-config")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true")
    check.set_defaults(func=command_check_config)

    fetch = subparsers.add_parser("fetch")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument("--term", default="")
    fetch.add_argument("--agency", action="append", default=[])
    fetch.add_argument("--document-type", action="append", default=[])
    fetch.add_argument("--publication-date-gte", default="")
    fetch.add_argument("--publication-date-lte", default="")
    fetch.add_argument("--raw-param", action="append", default=[])
    fetch.add_argument("--order", default="newest")
    fetch.add_argument("--start-page", type=int, default=1)
    fetch.add_argument("--max-pages", type=int, default=1)
    fetch.add_argument("--max-records", type=int, default=100)
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

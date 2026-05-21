#!/usr/bin/env python3
"""Fetch EPA EIS Database result tables into official governance-record artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "EPA_EIS_BASE_URL"
ENV_TIMEOUT_SECONDS = "EPA_EIS_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "EPA_EIS_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "EPA_EIS_RETRY_BACKOFF_SECONDS"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "EPA_EIS_MIN_REQUEST_INTERVAL_SECONDS"
ENV_USER_AGENT = "EPA_EIS_USER_AGENT"

DEFAULT_BASE_URL = "https://cdxapps.epa.gov/cdx-enepa-II/public/action/eis"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.5
DEFAULT_USER_AGENT = "fetch-epa-eis-records/1.0"

SKILL_NAME = "fetch-epa-eis-records"
SCHEMA_VERSION = "official-governance-record-fetch-v1"
SOURCE_NAME = "epa-eis-records"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
COMMON_SEARCH_CHOICES = ("lastWeek", "openComment", "last60Issued", "last30Published")
FIELD_NAMES = [
    "title",
    "ceq_number",
    "document_type",
    "epa_comment_letter_date",
    "federal_register_date",
    "unique_identification_number",
    "lead_agency",
    "federal_cooperating_agencies",
    "state",
    "download_documents",
]


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    min_request_interval_seconds: float
    user_agent: str


@dataclass(frozen=True)
class HttpTextResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    text: str
    byte_length: int
    sha256: str


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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
        raise ValueError(f"{name} must be a number, got: {value}")
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
        user_agent=maybe_text(args.user_agent if args.user_agent is not None else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)),
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

    def get_text(self, url: str) -> HttpTextResponse:
        attempts = self._cfg.max_retries + 1
        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("Accept", "text/html,application/xhtml+xml")
            req.add_header("User-Agent", self._cfg.user_agent)
            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    raw = resp.read()
                    self._last_request_monotonic = time.monotonic()
                    charset = resp.headers.get_content_charset() or "ISO-8859-1"
                    text = raw.decode(charset, errors="replace")
                    return HttpTextResponse(
                        url=getattr(resp, "url", url),
                        status_code=int(getattr(resp, "status", 200)),
                        headers={k.lower(): v for k, v in resp.headers.items()},
                        text=text,
                        byte_length=len(raw),
                        sha256=hashlib.sha256(raw).hexdigest(),
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
            except (URLError, TimeoutError, ConnectionResetError) as exc:
                self._last_request_monotonic = time.monotonic()
                if attempt < attempts:
                    delay = self._cfg.retry_backoff_seconds * (2 ** (attempt - 1))
                    self._logger.warning("network-retry delay=%.2fs url=%s err=%s", delay, url, exc)
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Request failed for {url}: {exc}") from exc
        raise RuntimeError(f"Failed to fetch after retries: {url}")


class SubmissionsTableParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.records: list[dict[str, Any]] = []
        self.pagebanner = ""
        self._in_pagebanner = False
        self._table_depth = 0
        self._in_tbody = False
        self._in_row = False
        self._in_cell = False
        self._cell_text_parts: list[str] = []
        self._cell_links: list[dict[str, str]] = []
        self._cell_onclicks: list[str] = []
        self._active_href = ""
        self._active_link_text_parts: list[str] = []
        self._row_cells: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.casefold(): value or "" for key, value in attrs}
        tag_name = tag.casefold()
        if tag_name == "span" and "pagebanner" in attr_map.get("class", ""):
            self._in_pagebanner = True
        if tag_name == "table" and attr_map.get("id") == "submissionsTable":
            self._table_depth = 1
            return
        if self._table_depth:
            if tag_name == "table":
                self._table_depth += 1
            elif tag_name == "tbody":
                self._in_tbody = True
            elif tag_name == "tr" and self._in_tbody:
                self._in_row = True
                self._row_cells = []
            elif tag_name == "td" and self._in_row:
                self._in_cell = True
                self._cell_text_parts = []
                self._cell_links = []
                self._cell_onclicks = []
            elif tag_name == "a" and self._in_cell:
                href = attr_map.get("href", "")
                onclick = attr_map.get("onclick", "")
                if href:
                    self._active_href = href
                    self._active_link_text_parts = []
                if onclick:
                    self._cell_onclicks.append(onclick)

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.casefold()
        if tag_name == "span" and self._in_pagebanner:
            self._in_pagebanner = False
        if tag_name == "a" and self._active_href:
            self._cell_links.append(
                {
                    "href": parse.urljoin(self.base_url, self._active_href),
                    "text": maybe_text(" ".join(self._active_link_text_parts)),
                }
            )
            self._active_href = ""
            self._active_link_text_parts = []
        if not self._table_depth:
            return
        if tag_name == "td" and self._in_cell:
            self._row_cells.append(
                {
                    "text": maybe_text(" ".join(self._cell_text_parts)),
                    "links": self._cell_links,
                    "onclicks": self._cell_onclicks,
                }
            )
            self._in_cell = False
        elif tag_name == "tr" and self._in_row:
            record = self._record_from_cells(self._row_cells)
            if record:
                self.records.append(record)
            self._in_row = False
            self._row_cells = []
        elif tag_name == "tbody":
            self._in_tbody = False
        elif tag_name == "table":
            self._table_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._in_pagebanner:
            self.pagebanner = maybe_text(f"{self.pagebanner} {data}")
        if self._in_cell:
            self._cell_text_parts.append(data)
        if self._active_href:
            self._active_link_text_parts.append(data)

    @staticmethod
    def _download_ids(onclicks: list[str]) -> list[str]:
        ids: list[str] = []
        for onclick in onclicks:
            for match in re.finditer(r"'([0-9;]+)'", onclick):
                value = match.group(1)
                if ";" not in value:
                    continue
                ids.extend(item for item in value.split(";") if item)
        return ids

    def _record_from_cells(self, cells: list[dict[str, Any]]) -> dict[str, Any]:
        if len(cells) < 9:
            return {}
        values = {
            FIELD_NAMES[index]: maybe_text(cells[index].get("text"))
            for index in range(min(len(cells), len(FIELD_NAMES)))
        }
        title_links = cells[0].get("links") if isinstance(cells[0].get("links"), list) else []
        detail_url = ""
        if title_links and isinstance(title_links[0], dict):
            detail_url = maybe_text(title_links[0].get("href"))
        download_cell = cells[9] if len(cells) > 9 else {"links": [], "onclicks": []}
        download_links = download_cell.get("links") if isinstance(download_cell.get("links"), list) else []
        download_onclicks = download_cell.get("onclicks") if isinstance(download_cell.get("onclicks"), list) else []
        return {
            **values,
            "detail_url": detail_url,
            "download_links": download_links,
            "download_document_ids": self._download_ids([maybe_text(item) for item in download_onclicks]),
        }


def result_count_from_banner(pagebanner: str) -> int | None:
    match = re.search(r"(\d+)\s+items?\s+found", pagebanner)
    return int(match.group(1)) if match else None


def common_search_url(config: RuntimeConfig, common_search: str) -> str:
    return f"{config.base_url}/search?{parse.urlencode({'search': '', 'commonSearch': common_search})}"


def validate_url(url: str) -> str:
    text = maybe_text(url)
    if not text:
        raise ValueError("URL cannot be empty.")
    parsed = parse.urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"URL must use http or https: {text}")
    return text


def normalize_record(raw: dict[str, Any], *, source_url: str) -> dict[str, Any]:
    ceq_number = maybe_text(raw.get("ceq_number"))
    unique_id = maybe_text(raw.get("unique_identification_number"))
    detail_url = maybe_text(raw.get("detail_url"))
    record_id = ceq_number or unique_id or detail_url or maybe_text(raw.get("title"))
    return {
        "source_skill": SKILL_NAME,
        "record_source": "EPA EIS Database",
        "record_id": record_id,
        "record_type": "epa_eis_record",
        "title": maybe_text(raw.get("title")),
        "agency": maybe_text(raw.get("lead_agency")),
        "agency_id": "EPA-EIS",
        "agency_names": [maybe_text(raw.get("lead_agency"))] if maybe_text(raw.get("lead_agency")) else [],
        "publication_date": maybe_text(raw.get("federal_register_date")),
        "updated_at": "",
        "url": detail_url,
        "document_url": detail_url,
        "document_type": maybe_text(raw.get("document_type")),
        "docket_ids": [value for value in [ceq_number, unique_id] if value],
        "ceq_number": ceq_number,
        "unique_identification_number": unique_id,
        "epa_comment_letter_date": maybe_text(raw.get("epa_comment_letter_date")),
        "federal_register_date": maybe_text(raw.get("federal_register_date")),
        "lead_agency": maybe_text(raw.get("lead_agency")),
        "federal_cooperating_agencies": maybe_text(raw.get("federal_cooperating_agencies")),
        "state": maybe_text(raw.get("state")),
        "download_document_ids": raw.get("download_document_ids") if isinstance(raw.get("download_document_ids"), list) else [],
        "summary": (
            f"{maybe_text(raw.get('document_type'))} EIS record; "
            f"CEQ {ceq_number}; Federal Register date {maybe_text(raw.get('federal_register_date'))}."
        ).strip("; "),
        "source_page_url": source_url,
        "provider_record": raw,
    }


def fetch_records(client: RetryableHttpClient, url: str) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, str]]]:
    response = client.get_text(url)
    parser = SubmissionsTableParser(response.url)
    parser.feed(response.text)
    warnings: list[dict[str, str]] = []
    if not parser.records:
        warnings.append({"code": "no-eis-table-records", "message": f"No submissionsTable rows were parsed from {url}."})
    records = [normalize_record(record, source_url=response.url) for record in parser.records]
    page_summary = {
        "request_url": url,
        "response_url": response.url,
        "status_code": response.status_code,
        "byte_length": response.byte_length,
        "sha256": response.sha256,
        "pagebanner": parser.pagebanner,
        "provider_result_count": result_count_from_banner(parser.pagebanner),
        "record_count": len(records),
    }
    return records, page_summary, warnings


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
                "user_agent": config.user_agent,
            },
            "source_urls": {
                "search": f"{config.base_url}/search",
                "open_comment": common_search_url(config, "openComment"),
                "last_30_published": common_search_url(config, "last30Published"),
            },
            "env_keys": {
                "base_url": ENV_BASE_URL,
                "timeout_seconds": ENV_TIMEOUT_SECONDS,
                "max_retries": ENV_MAX_RETRIES,
                "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
                "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
                "user_agent": ENV_USER_AGENT,
            },
        },
        args.pretty,
    )
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(args.log_level, args.log_file)
    config = build_runtime_config(args)
    urls: list[str] = []
    for value in args.common_search:
        urls.append(common_search_url(config, value))
    urls.extend(validate_url(value) for value in args.search_url)
    if not urls:
        raise ValueError("At least one --common-search or --search-url is required.")

    if args.dry_run:
        print_json(
            {
                "ok": True,
                "dry_run": True,
                "request_plan": {
                    "source_skill": SKILL_NAME,
                    "base_url": config.base_url,
                    "common_search": args.common_search,
                    "search_urls": urls,
                },
            },
            args.pretty,
        )
        return 0

    client = RetryableHttpClient(config, logger)
    records: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    page_summaries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for url in urls:
        page_records, page_summary, page_warnings = fetch_records(client, url)
        page_summaries.append(page_summary)
        warnings.extend(page_warnings)
        for record in page_records:
            record_id = maybe_text(record.get("record_id"))
            if record_id and record_id in seen:
                continue
            if record_id:
                seen.add(record_id)
            records.append(record)
            if args.max_records and len(records) >= args.max_records:
                break
        if args.max_records and len(records) >= args.max_records:
            break

    artifact: dict[str, Any] = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
        "source_skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "source_parameters": {
            "base_url": config.base_url,
            "common_search": args.common_search,
            "search_url_count": len(args.search_url),
            "max_records": args.max_records,
        },
        "query_parameters": {
            "common_search": args.common_search,
            "search_urls": urls,
        },
        "records": records,
        "records_fetched": len(records),
        "pages_fetched": len(page_summaries),
        "page_summaries": page_summaries,
        "validation_summary": {
            "warning_count": len(warnings),
            "record_count": len(records),
        },
        "warnings": warnings,
        "provenance": {
            "provider": "EPA EIS Database",
            "provider_site": "https://cdxapps.epa.gov/cdx-enepa-II/public/action/eis/search",
            "source_note": "EPA EIS Database rows are official NEPA/EIS metadata records; this fetch does not determine adequacy, legal sufficiency, or policy responsibility.",
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
    parser.add_argument("--user-agent", default=None)


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--log-file", default="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch EPA EIS Database result tables.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-config")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true")
    check.set_defaults(func=command_check_config)

    fetch = subparsers.add_parser("fetch")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument("--common-search", action="append", choices=COMMON_SEARCH_CHOICES, default=[])
    fetch.add_argument("--search-url", action="append", default=[])
    fetch.add_argument("--max-records", type=int, default=500)
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

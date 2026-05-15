#!/usr/bin/env python3
"""Fetch Bureau of Reclamation project pages and linked official records."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_TIMEOUT_SECONDS = "USBR_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "USBR_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "USBR_RETRY_BACKOFF_SECONDS"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "USBR_MIN_REQUEST_INTERVAL_SECONDS"
ENV_USER_AGENT = "USBR_USER_AGENT"

DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.5
DEFAULT_USER_AGENT = "fetch-usbr-project-records/1.0"

SKILL_NAME = "fetch-usbr-project-records"
SCHEMA_VERSION = "official-governance-record-fetch-v1"
SOURCE_NAME = "usbr-project-records"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
DOCUMENT_EXTENSIONS = (".pdf", ".html", ".htm", ".doc", ".docx", ".xls", ".xlsx", ".txt")


@dataclass(frozen=True)
class RuntimeConfig:
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


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    return RuntimeConfig(
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
            req.add_header("User-Agent", self._cfg.user_agent)
            req.add_header("Accept", "text/html,application/xhtml+xml,text/plain,*/*")
            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    raw = resp.read()
                    self._last_request_monotonic = time.monotonic()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    status = int(getattr(resp, "status", 200))
                    text = raw.decode("utf-8", errors="replace")
                    return HttpTextResponse(url, status, headers, text, len(raw), hashlib.sha256(raw).hexdigest())
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


class PageExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.description = ""
        self.links: list[dict[str, str]] = []
        self._in_title = False
        self._active_href = ""
        self._active_text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.casefold(): value or "" for key, value in attrs}
        tag_name = tag.casefold()
        if tag_name == "title":
            self._in_title = True
        elif tag_name == "meta":
            name = attr_map.get("name", "").casefold()
            prop = attr_map.get("property", "").casefold()
            if name == "description" or prop == "og:description":
                self.description = maybe_text(attr_map.get("content")) or self.description
        elif tag_name == "a":
            self._active_href = attr_map.get("href", "")
            self._active_text_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.casefold()
        if tag_name == "title":
            self._in_title = False
        elif tag_name == "a" and self._active_href:
            self.links.append({"href": self._active_href, "text": maybe_text(" ".join(self._active_text_parts))})
            self._active_href = ""
            self._active_text_parts = []

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
        if self._active_href:
            self._active_text_parts.append(data)

    @property
    def title(self) -> str:
        return maybe_text(" ".join(self.title_parts))


def normalize_url(value: str) -> str:
    text = maybe_text(value)
    if not text:
        raise ValueError("URL cannot be empty.")
    parsed = parse.urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"URL must use http or https: {text}")
    return text


def read_urls(args: argparse.Namespace) -> list[str]:
    values = [normalize_url(url) for url in args.url]
    if args.url_file:
        path = Path(args.url_file).expanduser().resolve()
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            values.append(normalize_url(text))
    seen: set[str] = set()
    result: list[str] = []
    for url in values:
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
    if not result:
        raise ValueError("At least one --url or --url-file entry is required.")
    return result


def is_usbr_domain(url: str) -> bool:
    host = parse.urlparse(url).hostname or ""
    return host.casefold().endswith("usbr.gov")


def same_host(left: str, right: str) -> bool:
    return (parse.urlparse(left).hostname or "").casefold() == (parse.urlparse(right).hostname or "").casefold()


def link_document_type(url: str) -> str:
    path = parse.urlparse(url).path.casefold()
    for ext in DOCUMENT_EXTENSIONS:
        if path.endswith(ext):
            return ext.lstrip(".")
    return "linked-page"


def keep_link(source_url: str, href: str, *, same_domain_only: bool) -> str:
    if not href.strip():
        return ""
    href_lc = href.strip().casefold()
    if href_lc.startswith(("mailto:", "javascript:", "tel:")):
        return ""
    absolute, _fragment = parse.urldefrag(parse.urljoin(source_url, href))
    if same_domain_only and not same_host(source_url, absolute):
        return ""
    parsed = parse.urlparse(absolute)
    if parsed.scheme not in {"http", "https"}:
        return ""
    return absolute


def page_record(response: HttpTextResponse, extractor: PageExtractor) -> dict[str, Any]:
    title = extractor.title or response.url
    return {
        "source_skill": SKILL_NAME,
        "record_source": "Bureau of Reclamation",
        "record_id": response.url,
        "record_type": "usbr_project_page",
        "title": title,
        "agency": "Bureau of Reclamation",
        "agency_id": "USBR",
        "agency_names": ["Bureau of Reclamation"],
        "publication_date": "",
        "updated_at": "",
        "url": response.url,
        "document_url": response.url,
        "document_type": "html",
        "summary": extractor.description,
        "links": [],
        "content_sha256": response.sha256,
        "content_byte_length": response.byte_length,
        "provider_record": {
            "response_url": response.url,
            "status_code": response.status_code,
            "headers": {
                key: value
                for key, value in response.headers.items()
                if key in {"content-type", "last-modified", "etag"}
            },
        },
    }


def linked_record(source_url: str, link: dict[str, str], *, index: int) -> dict[str, Any]:
    url = link["url"]
    title = maybe_text(link.get("text")) or Path(parse.urlparse(url).path).name or url
    return {
        "source_skill": SKILL_NAME,
        "record_source": "Bureau of Reclamation",
        "record_id": url,
        "record_type": "usbr_linked_document",
        "title": title,
        "agency": "Bureau of Reclamation",
        "agency_id": "USBR",
        "agency_names": ["Bureau of Reclamation"],
        "publication_date": "",
        "updated_at": "",
        "url": url,
        "document_url": url,
        "document_type": link_document_type(url),
        "summary": "",
        "source_page_url": source_url,
        "link_index": index,
        "provider_record": {"source_page_url": source_url, "anchor_text": maybe_text(link.get("text"))},
    }


def fetch_page_records(
    client: RetryableHttpClient,
    url: str,
    *,
    max_linked_records: int,
    same_domain_only: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    response = client.get_text(url)
    extractor = PageExtractor()
    extractor.feed(response.text)
    parent = page_record(response, extractor)
    link_items: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_link in extractor.links:
        absolute = keep_link(url, raw_link.get("href", ""), same_domain_only=same_domain_only)
        if not absolute or absolute in seen:
            continue
        seen.add(absolute)
        link_items.append({"url": absolute, "text": maybe_text(raw_link.get("text"))})
        if len(link_items) >= max_linked_records:
            break
    parent["links"] = link_items
    records = [parent]
    records.extend(linked_record(url, link, index=index) for index, link in enumerate(link_items))
    summary = {
        "request_url": url,
        "response_url": response.url,
        "status_code": response.status_code,
        "byte_length": response.byte_length,
        "sha256": response.sha256,
        "title": parent["title"],
        "link_count": len(link_items),
    }
    return records, summary


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
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "user_agent": config.user_agent,
        },
        "env_keys": {
            "timeout_seconds": ENV_TIMEOUT_SECONDS,
            "max_retries": ENV_MAX_RETRIES,
            "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
            "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
            "user_agent": ENV_USER_AGENT,
        },
    }
    print_json(payload, args.pretty)
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(args.log_level, args.log_file)
    config = build_runtime_config(args)
    if args.max_linked_records < 0:
        raise ValueError("--max-linked-records must be >= 0.")
    urls = read_urls(args)
    warnings: list[dict[str, str]] = []
    non_usbr_urls = [url for url in urls if not is_usbr_domain(url)]
    if non_usbr_urls:
        warnings.append(
            {
                "code": "non-usbr-domain",
                "message": "One or more URLs are not on usbr.gov; keep operator justification with the artifact if used outside tests.",
            }
        )
    if args.dry_run:
        payload = {
            "ok": True,
            "dry_run": True,
            "request_plan": {
                "source_skill": SKILL_NAME,
                "urls": urls,
                "max_linked_records": args.max_linked_records,
                "same_domain_only": not args.include_external_links,
            },
            "warnings": warnings,
        }
        print_json(payload, args.pretty)
        return 0

    client = RetryableHttpClient(config, logger)
    records: list[dict[str, Any]] = []
    page_summaries: list[dict[str, Any]] = []
    for url in urls:
        page_records, summary = fetch_page_records(
            client,
            url,
            max_linked_records=args.max_linked_records,
            same_domain_only=not args.include_external_links,
        )
        records.extend(page_records)
        page_summaries.append(summary)

    artifact: dict[str, Any] = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE_NAME,
        "source_skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "source_parameters": {
            "urls": urls,
            "max_linked_records": args.max_linked_records,
            "same_domain_only": not args.include_external_links,
        },
        "query_parameters": {"urls": urls},
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
            "provider": "Bureau of Reclamation",
            "provider_site": "https://www.usbr.gov/",
            "source_note": "Direct official project pages and links are inventory records only; this fetch does not determine policy meaning or legal completeness.",
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
    parser.add_argument("--timeout-seconds", type=int, default=None)
    parser.add_argument("--max-retries", type=int, default=None)
    parser.add_argument("--retry-backoff-seconds", type=float, default=None)
    parser.add_argument("--min-request-interval-seconds", type=float, default=None)
    parser.add_argument("--user-agent", default=None)


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--log-file", default="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch USBR project pages and linked official records.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check-config")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true")
    check.set_defaults(func=command_check_config)

    fetch = subparsers.add_parser("fetch")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument("--url", action="append", default=[])
    fetch.add_argument("--url-file", default="")
    fetch.add_argument("--max-linked-records", type=int, default=50)
    fetch.add_argument("--include-external-links", action="store_true")
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

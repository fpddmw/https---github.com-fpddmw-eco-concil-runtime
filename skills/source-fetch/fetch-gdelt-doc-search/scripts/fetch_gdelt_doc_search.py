#!/usr/bin/env python3
"""Query GDELT DOC 2.0 API with retries, throttling, and rich logs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_DOC_API_BASE_URL = "GDELT_DOC_API_BASE_URL"
ENV_TIMEOUT_SECONDS = "GDELT_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "GDELT_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "GDELT_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "GDELT_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "GDELT_MIN_REQUEST_INTERVAL_SECONDS"
ENV_USER_AGENT = "GDELT_USER_AGENT"

DEFAULT_DOC_API_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 5.0
DEFAULT_USER_AGENT = "fetch-gdelt-doc-search/1.0"

TS_FORMAT = "%Y%m%d%H%M%S"
TS_FORMAT_HELP = "YYYYMMDDHHMMSS"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
DOC_API_MAX_RECORDS_LIMIT = 250
DOC_API_TIMELINE_SMOOTH_LIMIT = 30
DOC_API_QUERY_LENGTH_WARN_LIMIT = 240
RESERVED_DOC_PARAM_KEYS = {
    "query",
    "mode",
    "format",
    "timespan",
    "startdatetime",
    "enddatetime",
    "maxrecords",
    "sort",
    "timelinesmooth",
}
JSON_BODY_EXCERPT_LIMIT = 300
DOMAIN_PATTERN = re.compile(
    r"^(?=.{1,253}$)([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$"
)
UNSUPPORTED_QUERY_OPERATOR_SUGGESTIONS = {
    "site": "GDELT DOC does not support site:. Use --domain-is example.gov or query operator domainis:example.gov.",
    "inurl": "GDELT DOC does not support inurl:. Use domain:/domainis: filters plus content terms.",
}
OR_TOKEN_RE = re.compile(r"\bOR\b", re.IGNORECASE)


@dataclass(frozen=True)
class RuntimeConfig:
    doc_api_base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    user_agent: str


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def parse_key_value_args(items: list[str], arg_name: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid {arg_name} entry {item!r}. Use key=value format.")
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError(f"Invalid {arg_name} entry {item!r}. Key cannot be empty.")
        pairs[key] = value
    return pairs


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


def parse_positive_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def parse_non_negative_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got: {value}")
    return value


def parse_timestamp(raw: str) -> datetime:
    try:
        parsed = datetime.strptime(raw, TS_FORMAT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid timestamp {raw!r}. Use UTC format {TS_FORMAT} (YYYYMMDDHHMMSS)."
        ) from exc
    return parsed.replace(tzinfo=timezone.utc)


def iter_boolean_or_depths(query: str) -> list[int]:
    """Return parenthesis depth for boolean OR tokens outside quoted phrases."""
    depths: list[int] = []
    depth = 0
    quote: str | None = None
    i = 0
    while i < len(query):
        ch = query[i]
        if quote:
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in {'"', "'"}:
            quote = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
            i += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        match = OR_TOKEN_RE.match(query, i)
        if match:
            depths.append(depth)
            i = match.end()
            continue
        i += 1
    return depths


def has_top_level_or(query: str) -> bool:
    return any(depth == 0 for depth in iter_boolean_or_depths(query))


def needs_domain_query_grouping(query: str) -> bool:
    return has_top_level_or(query)


def lint_doc_query(query: str) -> dict[str, Any]:
    normalized = query.strip()
    errors: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    for match in re.finditer(r"(?<![\w-])([A-Za-z][A-Za-z0-9_-]*):", normalized):
        operator = match.group(1).casefold()
        suggestion = UNSUPPORTED_QUERY_OPERATOR_SUGGESTIONS.get(operator)
        if suggestion:
            errors.append(
                {
                    "code": f"unsupported-operator-{operator}",
                    "operator": operator,
                    "message": suggestion,
                }
            )
    if has_top_level_or(normalized):
        errors.append(
            {
                "code": "unparenthesized-or",
                "message": (
                    "GDELT DOC requires OR expressions to be enclosed in parentheses. "
                    "Use a query such as '(smoke OR advisory)' instead of "
                    "'smoke OR advisory'."
                ),
            }
        )
    if len(normalized) > DOC_API_QUERY_LENGTH_WARN_LIMIT:
        warnings.append(
            {
                "code": "long-query",
                "message": (
                    f"Query is {len(normalized)} characters. GDELT DOC may reject long "
                    "or highly compound queries; split official domains/agencies into "
                    "separate invocations."
                ),
            }
        )
    or_group_count = len(
        re.findall(r"\([^()]*\bOR\b[^()]*\)", normalized, flags=re.IGNORECASE)
    )
    if or_group_count > 1:
        warnings.append(
            {
                "code": "multiple-or-blocks",
                "message": (
                    "GDELT DOC documents OR blocks as non-nestable. Multiple OR blocks "
                    "joined with AND are fragile; prefer one compact topic query plus "
                    "--domain-is splits."
                ),
            }
        )
    return {"ok": not errors, "errors": errors, "warnings": warnings}


def validate_domain(value: str, option_name: str) -> str:
    domain = (
        value.strip()
        .lower()
        .removeprefix("http://")
        .removeprefix("https://")
        .split("/", 1)[0]
    )
    if not domain or not DOMAIN_PATTERN.match(domain):
        raise ValueError(
            f"{option_name} must be a bare domain like example.gov, got: {value!r}"
        )
    return domain


def domain_filters(args: argparse.Namespace) -> list[str]:
    filters: list[str] = []
    for value in getattr(args, "domain", []):
        filters.append(f"domain:{validate_domain(value, '--domain')}")
    for value in getattr(args, "domain_is", []):
        filters.append(f"domainis:{validate_domain(value, '--domain-is')}")
    return filters


def compose_query(base_query: str, domain_filter: str = "") -> str:
    query = base_query.strip()
    if domain_filter and query and needs_domain_query_grouping(query):
        query = f"({query})"
    if domain_filter:
        query = f"{domain_filter} {query}" if query else domain_filter
    return query.strip()


def normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized}")
    return normalized


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    doc_api_base_url = normalize_base_url(
        args.doc_api_base_url
        if args.doc_api_base_url
        else env_or_default(ENV_DOC_API_BASE_URL, DEFAULT_DOC_API_BASE_URL)
    )
    timeout_seconds = parse_positive_int(
        "--timeout-seconds",
        str(
            args.timeout_seconds
            if args.timeout_seconds is not None
            else env_or_default(ENV_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))
        ),
    )
    max_retries = parse_non_negative_int(
        "--max-retries",
        str(
            args.max_retries
            if args.max_retries is not None
            else env_or_default(ENV_MAX_RETRIES, str(DEFAULT_MAX_RETRIES))
        ),
    )
    retry_backoff_seconds = parse_positive_float(
        "--retry-backoff-seconds",
        str(
            args.retry_backoff_seconds
            if args.retry_backoff_seconds is not None
            else env_or_default(ENV_RETRY_BACKOFF_SECONDS, str(DEFAULT_RETRY_BACKOFF_SECONDS))
        ),
    )
    retry_backoff_multiplier = parse_positive_float(
        "--retry-backoff-multiplier",
        str(
            args.retry_backoff_multiplier
            if args.retry_backoff_multiplier is not None
            else env_or_default(
                ENV_RETRY_BACKOFF_MULTIPLIER, str(DEFAULT_RETRY_BACKOFF_MULTIPLIER)
            )
        ),
    )
    min_request_interval_seconds = parse_non_negative_float(
        "--min-request-interval-seconds",
        str(
            args.min_request_interval_seconds
            if args.min_request_interval_seconds is not None
            else env_or_default(
                ENV_MIN_REQUEST_INTERVAL_SECONDS, str(DEFAULT_MIN_REQUEST_INTERVAL_SECONDS)
            )
        ),
    )
    user_agent = (
        args.user_agent
        if args.user_agent is not None
        else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)
    ).strip()
    if not user_agent:
        raise ValueError("User-Agent cannot be empty.")

    return RuntimeConfig(
        doc_api_base_url=doc_api_base_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_multiplier=retry_backoff_multiplier,
        min_request_interval_seconds=min_request_interval_seconds,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("fetch-gdelt-doc-search")
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


class RetryableHttpClient:
    def __init__(self, config: RuntimeConfig, logger: logging.Logger) -> None:
        self._cfg = config
        self._logger = logger
        self._last_request_monotonic: float | None = None

    def _throttle(self) -> None:
        if self._last_request_monotonic is None:
            return
        gap = time.monotonic() - self._last_request_monotonic
        sleep_seconds = self._cfg.min_request_interval_seconds - gap
        if sleep_seconds > 0:
            self._logger.debug("throttle-sleep=%.3fs", sleep_seconds)
            time.sleep(sleep_seconds)

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            parsed = float(value)
        except ValueError:
            return None
        return parsed if parsed >= 0 else None

    def get_bytes(self, url: str) -> tuple[bytes, dict[str, str]]:
        attempts = self._cfg.max_retries + 1

        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("User-Agent", self._cfg.user_agent)
            req.add_header("Accept", "*/*")

            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    payload = resp.read()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    self._last_request_monotonic = time.monotonic()
                    self._logger.info(
                        "http-ok status=%s bytes=%d url=%s",
                        getattr(resp, "status", "unknown"),
                        len(payload),
                        url,
                    )
                    return payload, headers
            except HTTPError as exc:
                self._last_request_monotonic = time.monotonic()
                status = int(exc.code)
                body = exc.read().decode("utf-8", errors="replace").strip()
                body_excerpt = body[:300]
                retriable = status in RETRIABLE_HTTP_CODES
                retry_after = self._parse_retry_after(exc.headers.get("Retry-After"))
                if retriable and attempt < attempts:
                    delay = (
                        retry_after
                        if retry_after is not None
                        else self._cfg.retry_backoff_seconds
                        * (self._cfg.retry_backoff_multiplier ** (attempt - 1))
                    )
                    self._logger.warning(
                        "http-retry status=%d delay=%.2fs attempt=%d/%d url=%s body=%s",
                        status,
                        delay,
                        attempt,
                        attempts,
                        url,
                        body_excerpt,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(
                    f"HTTP {status} for {url}. body_excerpt={body_excerpt!r}"
                ) from exc
            except (URLError, TimeoutError) as exc:
                self._last_request_monotonic = time.monotonic()
                if attempt < attempts:
                    delay = self._cfg.retry_backoff_seconds * (
                        self._cfg.retry_backoff_multiplier ** (attempt - 1)
                    )
                    self._logger.warning(
                        "network-retry delay=%.2fs attempt=%d/%d url=%s err=%s",
                        delay,
                        attempt,
                        attempts,
                        url,
                        exc,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Request failed for {url}: {exc}") from exc

        raise RuntimeError(f"Failed to fetch after retries: {url}")


def validate_search_args(args: argparse.Namespace) -> None:
    if not args.query.strip():
        raise ValueError("--query cannot be empty.")
    if not args.mode.strip():
        raise ValueError("--mode cannot be empty.")
    if not args.format.strip():
        raise ValueError("--format cannot be empty.")

    has_start = bool(args.start_datetime.strip())
    has_end = bool(args.end_datetime.strip())
    if has_start != has_end:
        raise ValueError("--start-datetime and --end-datetime must be provided together.")
    if args.timespan.strip() and has_start:
        raise ValueError("Use either --timespan or --start-datetime/--end-datetime, not both.")

    if has_start:
        start = parse_timestamp(args.start_datetime.strip())
        end = parse_timestamp(args.end_datetime.strip())
        if end < start:
            raise ValueError("--end-datetime must be >= --start-datetime.")

    if args.max_records is not None:
        if args.max_records < 1:
            raise ValueError("--max-records must be >= 1.")
        if args.max_records > DOC_API_MAX_RECORDS_LIMIT:
            raise ValueError(
                f"--max-records cannot exceed {DOC_API_MAX_RECORDS_LIMIT} "
                "(DOC API max for artlist/imagecollage modes)."
            )

    if args.timeline_smooth is not None:
        if args.timeline_smooth < 0:
            raise ValueError("--timeline-smooth must be >= 0.")
        if args.timeline_smooth > DOC_API_TIMELINE_SMOOTH_LIMIT:
            raise ValueError(
                f"--timeline-smooth cannot exceed {DOC_API_TIMELINE_SMOOTH_LIMIT}."
            )

    candidate_queries = [compose_query(args.query, item) for item in domain_filters(args)] or [
        args.query.strip()
    ]
    lint_errors: list[dict[str, str]] = []
    lint_warnings: list[dict[str, str]] = []
    for candidate_query in candidate_queries:
        lint = lint_doc_query(candidate_query)
        lint_errors.extend(lint["errors"])
        lint_warnings.extend(lint["warnings"])
    if lint_errors:
        raise ValueError(
            "GDELT DOC query lint failed: " + json.dumps(lint_errors, ensure_ascii=False)
        )
    if lint_warnings and args.lint_mode == "strict":
        raise ValueError(
            "GDELT DOC query lint warnings in strict mode: "
            + json.dumps(lint_warnings, ensure_ascii=False)
        )


def build_search_params(args: argparse.Namespace, query: str | None = None) -> dict[str, str]:
    extra_params = parse_key_value_args(args.param, "--param")
    for key in extra_params:
        if key.strip().lower() in RESERVED_DOC_PARAM_KEYS:
            raise ValueError(f"--param cannot override reserved key: {key!r}")

    params: dict[str, str] = {
        "query": (query if query is not None else args.query).strip(),
        "mode": args.mode.strip(),
        "format": args.format.strip(),
    }
    if args.timespan.strip():
        params["timespan"] = args.timespan.strip()
    if args.start_datetime.strip():
        params["STARTDATETIME"] = args.start_datetime.strip()
        params["ENDDATETIME"] = args.end_datetime.strip()
    if args.max_records is not None:
        params["MAXRECORDS"] = str(args.max_records)
    if args.sort.strip():
        params["sort"] = args.sort.strip()
    if args.timeline_smooth is not None:
        params["TIMELINESMOOTH"] = str(args.timeline_smooth)

    params.update(extra_params)
    return params


def decode_text(content: bytes) -> str:
    return content.decode("utf-8", errors="replace")


def parse_json_response(payload_bytes: bytes) -> Any:
    text = decode_text(payload_bytes)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        excerpt = " ".join(text.split())[:JSON_BODY_EXCERPT_LIMIT]
        failure_class = "provider-non-json"
        lowered = excerpt.casefold()
        if "too short" in lowered or "too long" in lowered:
            failure_class = "provider-query-length"
        if "too common" in lowered or "one or more of your keywords" in lowered:
            failure_class = "provider-query-invalid"
        raise RuntimeError(
            f"DOC API returned non-JSON content while format=json ({failure_class}). "
            f"body_excerpt={excerpt!r}"
        ) from exc


def article_identity(article: Any) -> str:
    if not isinstance(article, dict):
        return json.dumps(article, ensure_ascii=True, sort_keys=True)
    return str(
        article.get("url")
        or article.get("title")
        or json.dumps(article, ensure_ascii=True, sort_keys=True)
    )


def merge_json_payloads(batches: list[dict[str, Any]]) -> dict[str, Any]:
    articles: list[Any] = []
    seen: set[str] = set()
    timeline: list[Any] = []
    for batch in batches:
        payload = batch.get("data")
        if not isinstance(payload, dict):
            continue
        batch_articles = payload.get("articles")
        if isinstance(batch_articles, list):
            for article in batch_articles:
                identity = article_identity(article)
                if identity in seen:
                    continue
                seen.add(identity)
                articles.append(article)
        for key in ("timeline", "timelinevol", "timelinevolraw"):
            values = payload.get(key)
            if isinstance(values, list):
                timeline.extend(values)
    merged: dict[str, Any] = {
        "query": str(batches[0].get("query", "")) if batches else "",
        "articles": articles,
        "batches": batches,
    }
    if timeline:
        merged["timeline"] = timeline
    return merged


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
    )


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    payload = {
        "ok": True,
        "config": {
            "doc_api_base_url": config.doc_api_base_url,
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "user_agent": config.user_agent,
        },
        "source_urls": {
            "doc_api": config.doc_api_base_url,
        },
        "env_keys": {
            "doc_api_base_url": ENV_DOC_API_BASE_URL,
            "timeout_seconds": ENV_TIMEOUT_SECONDS,
            "max_retries": ENV_MAX_RETRIES,
            "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
            "retry_backoff_multiplier": ENV_RETRY_BACKOFF_MULTIPLIER,
            "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
            "user_agent": ENV_USER_AGENT,
        },
    }
    print_json(payload, pretty=args.pretty)
    return 0


def command_lint_query(args: argparse.Namespace) -> int:
    queries = [compose_query(args.query, item) for item in domain_filters(args)] or [
        args.query.strip()
    ]
    query_results = [{"query": query, **lint_doc_query(query)} for query in queries]
    payload = {
        "ok": all(result["ok"] for result in query_results),
        "source": "gdelt-doc-api",
        "query_count": len(query_results),
        "queries": query_results,
        "guidance": {
            "official_domain_filter": (
                "Use domainis:example.gov or --domain-is example.gov, not site:example.gov."
            ),
            "split_strategy": (
                "Use repeated --domain-is values to run one compact query per official domain "
                "and merge results."
            ),
        },
    }
    print_json(payload, pretty=args.pretty)
    return 0 if payload["ok"] else 2


def command_search(args: argparse.Namespace) -> int:
    logger = build_logger(level=args.log_level, log_file=args.log_file)
    config = build_runtime_config(args)
    validate_search_args(args)
    client = RetryableHttpClient(config=config, logger=logger)
    json_requested = args.format.strip().lower() == "json"
    queries = [compose_query(args.query, item) for item in domain_filters(args)] or [
        args.query.strip()
    ]
    batches: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    headers: dict[str, str] = {}
    raw_payload_bytes: bytes | None = None
    for query in queries:
        params = build_search_params(args, query=query)
        request_url = f"{config.doc_api_base_url}?{parse.urlencode(params)}"
        try:
            payload_bytes, headers = client.get_bytes(request_url)
            raw_payload_bytes = payload_bytes
            parsed_json: Any | None = None
            if json_requested:
                parsed_json = parse_json_response(payload_bytes)
            batches.append(
                {
                    "ok": True,
                    "query": query,
                    "request_url": request_url,
                    "content_type": headers.get("content-type"),
                    "data": parsed_json,
                }
            )
        except Exception as exc:
            if not args.continue_on_query_error:
                raise
            errors.append({"query": query, "message": str(exc)})

    if not batches and errors:
        raise RuntimeError(
            "All GDELT DOC query batches failed: " + json.dumps(errors, ensure_ascii=False)
        )

    merged_json: Any | None = None
    if json_requested:
        if len(batches) == 1 and not errors:
            merged_json = batches[0]["data"]
        else:
            merged_json = merge_json_payloads(batches)
            if isinstance(merged_json, dict) and errors:
                merged_json["query_errors"] = errors

    output_file = args.output.strip()
    if output_file:
        output_path = Path(output_file).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if json_requested:
            output_path.write_text(
                json.dumps(merged_json, ensure_ascii=False, indent=2 if args.pretty else None),
                encoding="utf-8",
            )
            bytes_written = output_path.stat().st_size
        else:
            if raw_payload_bytes is None:
                raise RuntimeError("No response payload was available to write.")
            output_path.write_bytes(raw_payload_bytes)
            bytes_written = len(raw_payload_bytes)
        result = {
            "ok": True,
            "source": "gdelt-doc-api",
            "request_url": batches[0]["request_url"] if batches else "",
            "request_urls": [batch["request_url"] for batch in batches],
            "query_count": len(queries),
            "successful_query_count": len(batches),
            "query_errors": errors,
            "content_type": headers.get("content-type"),
            "bytes_written": bytes_written,
            "output_path": str(output_path),
        }
        print_json(result, pretty=args.pretty)
        return 0

    if args.format.strip().lower() == "json":
        result = {
            "ok": True,
            "source": "gdelt-doc-api",
            "request_url": batches[0]["request_url"] if batches else "",
            "request_urls": [batch["request_url"] for batch in batches],
            "query_count": len(queries),
            "successful_query_count": len(batches),
            "query_errors": errors,
            "content_type": headers.get("content-type"),
            "data": merged_json,
        }
        print_json(result, pretty=args.pretty)
        return 0

    if raw_payload_bytes is not None:
        print(decode_text(raw_payload_bytes))
    return 0


def add_runtime_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--doc-api-base-url",
        default="",
        help=f"Override DOC API base URL. Default: {DEFAULT_DOC_API_BASE_URL}",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help=f"HTTP timeout in seconds. Env: {ENV_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help=f"Retry count for transient errors. Env: {ENV_MAX_RETRIES}.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=None,
        help=f"Initial backoff seconds before retries. Env: {ENV_RETRY_BACKOFF_SECONDS}.",
    )
    parser.add_argument(
        "--retry-backoff-multiplier",
        type=float,
        default=None,
        help=f"Backoff multiplier between retry attempts. Env: {ENV_RETRY_BACKOFF_MULTIPLIER}.",
    )
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=None,
        help=f"Minimum interval between requests. Env: {ENV_MIN_REQUEST_INTERVAL_SECONDS}.",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help=f"HTTP User-Agent header. Env: {ENV_USER_AGENT}.",
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log verbosity written to stderr/log-file.",
    )
    parser.add_argument(
        "--log-file",
        default="",
        help="Optional log file path. When set, append logs to this file.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query GDELT DOC 2.0 API with configurable retry, throttling, and logging."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Show effective runtime config and source URL.")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    lint = sub.add_parser("lint-query", help="Validate a GDELT DOC query locally before a remote call.")
    lint.add_argument("--query", required=True, help="DOC API query string.")
    lint.add_argument(
        "--domain",
        action="append",
        default=[],
        help="Add a GDELT domain: filter. Repeat to validate split per-domain queries.",
    )
    lint.add_argument(
        "--domain-is",
        action="append",
        default=[],
        help="Add an exact GDELT domainis: filter. Repeat to validate split per-domain queries.",
    )
    lint.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    search = sub.add_parser(
        "search",
        aliases=["doc-search"],
        help="Search GDELT DOC API with query syntax.",
    )
    add_runtime_config_args(search)
    add_logging_args(search)
    search.add_argument("--query", required=True, help="DOC API query string.")
    search.add_argument(
        "--domain",
        action="append",
        default=[],
        help="Add a GDELT domain: filter. Repeat to run one query per domain and merge JSON article results.",
    )
    search.add_argument(
        "--domain-is",
        action="append",
        default=[],
        help="Add an exact GDELT domainis: filter. Repeat to run one query per domain and merge JSON article results.",
    )
    search.add_argument(
        "--lint-mode",
        choices=["warn", "strict"],
        default="warn",
        help="Treat query lint warnings as non-fatal warnings or fatal errors.",
    )
    search.add_argument(
        "--continue-on-query-error",
        action="store_true",
        help="For split domain queries, keep successful batches and record failed batches in query_errors.",
    )
    search.add_argument(
        "--mode",
        default="artlist",
        help="DOC API mode, for example artlist, timelinevol, timelinevolraw, timelinetone.",
    )
    search.add_argument(
        "--format",
        default="json",
        help="DOC API output format, for example json, csv, html, rss, rssarchive.",
    )
    search.add_argument(
        "--timespan",
        default="",
        help="Relative search span, for example 1h, 3days, 1week, 3months.",
    )
    search.add_argument(
        "--start-datetime",
        default="",
        help=f"Absolute UTC start timestamp format {TS_FORMAT_HELP}.",
    )
    search.add_argument(
        "--end-datetime",
        default="",
        help=f"Absolute UTC end timestamp format {TS_FORMAT_HELP}.",
    )
    search.add_argument(
        "--max-records",
        type=int,
        default=None,
        help=f"DOC API MAXRECORDS (1-{DOC_API_MAX_RECORDS_LIMIT}, mainly artlist/imagecollage modes).",
    )
    search.add_argument("--sort", default="", help="Optional sort, for example datedesc.")
    search.add_argument(
        "--timeline-smooth",
        type=int,
        default=None,
        help=f"Optional TIMELINESMOOTH (0-{DOC_API_TIMELINE_SMOOTH_LIMIT}).",
    )
    search.add_argument(
        "--param",
        action="append",
        default=[],
        help="Extra DOC API parameter key=value. Repeat for multiple params.",
    )
    search.add_argument(
        "--output",
        default="",
        help="Optional output file path. If set, write raw response bytes to this path.",
    )
    search.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "check-config":
            return command_check_config(args)
        if args.command == "lint-query":
            return command_lint_query(args)
        if args.command in {"search", "doc-search"}:
            return command_search(args)
        raise ValueError(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        print("[ERROR] Interrupted by user.", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Fetch Regulations.gov attachment metadata and downloadable files."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import mimetypes
import os
import re
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "REGGOV_BASE_URL"
ENV_API_KEY = "REGGOV_API_KEY"
ENV_TIMEOUT_SECONDS = "REGGOV_TIMEOUT_SECONDS"
ENV_USER_AGENT = "REGGOV_USER_AGENT"

DEFAULT_BASE_URL = "https://api.regulations.gov/v4"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_USER_AGENT = "fetch-regulationsgov-attachments/1.0"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    api_key: str
    timeout_seconds: int
    user_agent: str


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if not normalized.startswith(("http://", "https://")):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized}")
    return normalized


def parse_positive_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(args.base_url or env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL))
    api_key = args.api_key.strip() or env_or_default(ENV_API_KEY, "")
    if not api_key:
        raise ValueError("API key is required. Set --api-key or REGGOV_API_KEY.")
    timeout_seconds = parse_positive_int(
        "--timeout-seconds",
        str(args.timeout_seconds if args.timeout_seconds is not None else env_or_default(ENV_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))),
    )
    user_agent = (args.user_agent if args.user_agent is not None else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)).strip()
    if not user_agent:
        raise ValueError("User-Agent cannot be empty.")
    return RuntimeConfig(base_url=base_url, api_key=api_key, timeout_seconds=timeout_seconds, user_agent=user_agent)


def mask_api_key(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def render_query(base_url: str, path: str, query: dict[str, str] | None = None) -> str:
    encoded = parse.urlencode(query or {})
    if encoded:
        return f"{base_url}/{path}?{encoded}"
    return f"{base_url}/{path}"


def build_request(url: str, config: RuntimeConfig, *, authenticated: bool) -> request.Request:
    req = request.Request(url, method="GET")
    req.add_header("User-Agent", config.user_agent)
    req.add_header("Accept", "*/*")
    if authenticated:
        req.add_header("X-Api-Key", config.api_key)
    return req


def request_bytes(req: request.Request, *, timeout_seconds: int) -> tuple[bytes, dict[str, str], int]:
    with request.urlopen(req, timeout=timeout_seconds) as resp:
        payload = resp.read()
        headers = {key.lower(): value for key, value in resp.headers.items()}
        status = int(getattr(resp, "status", 200))
        return payload, headers, status


def public_dns_a_records(hostname: str, *, timeout_seconds: int) -> list[str]:
    query_url = "https://cloudflare-dns.com/dns-query?" + parse.urlencode(
        {"name": hostname, "type": "A"}
    )
    req = request.Request(query_url, method="GET")
    req.add_header("Accept", "application/dns-json")
    with request.urlopen(req, timeout=min(timeout_seconds, 20)) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    answers = payload.get("Answer") if isinstance(payload, dict) else []
    ips: list[str] = []
    for answer in answers if isinstance(answers, list) else []:
        if not isinstance(answer, dict):
            continue
        if answer.get("type") == 1 and maybe_text(answer.get("data")):
            ips.append(maybe_text(answer.get("data")))
    return ips


@contextlib.contextmanager
def socket_resolution_override(hostname: str, ips: list[str]):
    original_getaddrinfo = socket.getaddrinfo

    def patched_getaddrinfo(host: str, port: int, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0):
        if host != hostname:
            return original_getaddrinfo(host, port, family, type, proto, flags)
        rows = []
        for ip in ips:
            rows.extend(original_getaddrinfo(ip, port, family, type, proto, flags))
        return rows

    socket.getaddrinfo = patched_getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo


def http_get_bytes_with_public_dns(url: str, config: RuntimeConfig, *, authenticated: bool) -> tuple[bytes, dict[str, str], int]:
    hostname = parse.urlparse(url).hostname or ""
    if hostname != "downloads.regulations.gov":
        raise RuntimeError(f"No public-DNS fallback is configured for host: {hostname}")
    ips = public_dns_a_records(hostname, timeout_seconds=config.timeout_seconds)
    if not ips:
        raise RuntimeError(f"Public DNS fallback returned no A records for {hostname}.")
    req = build_request(url, config, authenticated=authenticated)
    with socket_resolution_override(hostname, ips):
        return request_bytes(req, timeout_seconds=config.timeout_seconds)


def http_get_bytes(url: str, config: RuntimeConfig, *, authenticated: bool) -> tuple[bytes, dict[str, str], int]:
    attempts = 3
    final_transport_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        req = build_request(url, config, authenticated=authenticated)
        try:
            return request_bytes(req, timeout_seconds=config.timeout_seconds)
        except HTTPError as exc:
            if int(exc.code) in RETRIABLE_HTTP_CODES and attempt < attempts:
                time.sleep(1.5 * attempt)
                continue
            body = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
        except (URLError, TimeoutError, ConnectionResetError) as exc:
            final_transport_error = exc
            if attempt < attempts:
                time.sleep(1.5 * attempt)
                continue
    try:
        return http_get_bytes_with_public_dns(url, config, authenticated=authenticated)
    except Exception as fallback_exc:  # noqa: BLE001
        if final_transport_error is not None:
            raise RuntimeError(
                f"Request failed for {url}: {final_transport_error}; public-DNS fallback failed: {fallback_exc}"
            ) from fallback_exc
        raise
    raise RuntimeError(f"Failed to fetch after retries: {url}")


def http_get_json(url: str, config: RuntimeConfig) -> dict[str, Any]:
    payload, headers, _ = http_get_bytes(url, config, authenticated=True)
    content_type = headers.get("content-type", "")
    if "json" not in content_type.lower():
        raise RuntimeError(f"Unexpected content-type {content_type!r} for {url}.")
    obj = json.loads(payload.decode("utf-8"))
    if not isinstance(obj, dict):
        raise RuntimeError(f"Response JSON root must be object for {url}.")
    return obj


def read_json_or_jsonl(path: Path) -> Any:
    if path.suffix.lower() == ".jsonl":
        items: list[Any] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if text:
                    items.append(json.loads(text))
        return {"records": items}
    return json.loads(path.read_text(encoding="utf-8"))


def iter_dicts(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def attachment_id_from_item(item: dict[str, Any]) -> str:
    attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
    return maybe_text(item.get("attachment_id")) or maybe_text(item.get("id")) or maybe_text(attrs.get("id"))


def attachment_file_url(item: dict[str, Any]) -> str:
    attrs = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
    for key in ("fileUrl", "fileURL", "downloadUrl", "downloadURL", "url"):
        text = maybe_text(attrs.get(key)) or maybe_text(item.get(key))
        if text:
            return text
    for file_format in attrs.get("fileFormats") if isinstance(attrs.get("fileFormats"), list) else []:
        if not isinstance(file_format, dict):
            continue
        for key in ("fileUrl", "fileURL", "downloadUrl", "downloadURL", "url"):
            text = maybe_text(file_format.get(key))
            if text:
                return text
    links = item.get("links") if isinstance(item.get("links"), dict) else {}
    return maybe_text(links.get("self")) if str(links.get("self", "")).startswith("http") else ""


def target_key(target: dict[str, Any]) -> str:
    return "|".join(
        [
            maybe_text(target.get("comment_id")),
            maybe_text(target.get("attachment_id")),
            maybe_text(target.get("file_url")),
        ]
    )


def add_target(targets: list[dict[str, Any]], target: dict[str, Any], seen: set[str]) -> None:
    key = target_key(target)
    if not key or key in seen:
        return
    seen.add(key)
    targets.append(target)


def collect_from_record(record: dict[str, Any], targets: list[dict[str, Any]], seen: set[str]) -> None:
    comment_id = maybe_text(record.get("comment_id")) or maybe_text(record.get("id"))
    detail = record.get("detail") if isinstance(record.get("detail"), dict) else {}
    comment_attributes = detail.get("attributes") if isinstance(detail.get("attributes"), dict) else {}
    if not comment_id:
        comment_id = maybe_text(detail.get("id"))
    for container in (record, detail):
        for key in ("attachments", "included"):
            for item in container.get(key) if isinstance(container.get(key), list) else []:
                if not isinstance(item, dict):
                    continue
                add_target(
                    targets,
                    {
                        "comment_id": comment_id,
                        "attachment_id": attachment_id_from_item(item),
                        "file_url": attachment_file_url(item),
                        "metadata": item,
                        "comment_attributes": comment_attributes,
                        "source": "artifact-attachment-metadata",
                    },
                    seen,
                )
    relationships = detail.get("relationships") if isinstance(detail.get("relationships"), dict) else {}
    attachments_rel = relationships.get("attachments") if isinstance(relationships.get("attachments"), dict) else {}
    for item in attachments_rel.get("data") if isinstance(attachments_rel.get("data"), list) else []:
        if isinstance(item, dict):
            add_target(
                targets,
                {
                    "comment_id": comment_id,
                "attachment_id": attachment_id_from_item(item),
                "file_url": "",
                "metadata": item,
                "comment_attributes": comment_attributes,
                "source": "artifact-relationship",
            },
            seen,
            )
    if comment_id and not any(target.get("comment_id") == comment_id for target in targets):
        add_target(
            targets,
            {
                "comment_id": comment_id,
                "attachment_id": "",
                "file_url": "",
                "metadata": {},
                "comment_attributes": comment_attributes,
                "source": "comment-id",
            },
            seen,
        )


def load_targets(
    *,
    comment_ids: list[str],
    attachment_ids: list[str],
    input_artifacts: list[str],
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for comment_id in comment_ids:
        add_target(targets, {"comment_id": maybe_text(comment_id), "attachment_id": "", "file_url": "", "metadata": {}, "source": "cli-comment-id"}, seen)
    for attachment_id in attachment_ids:
        add_target(targets, {"comment_id": "", "attachment_id": maybe_text(attachment_id), "file_url": "", "metadata": {}, "source": "cli-attachment-id"}, seen)
    for raw_path in input_artifacts:
        path = Path(raw_path).expanduser().resolve()
        payload = read_json_or_jsonl(path)
        roots: list[Any] = []
        if isinstance(payload, dict):
            roots.extend([payload])
            for key in ("records", "data", "downloads"):
                if isinstance(payload.get(key), list):
                    roots.extend(payload[key])
        elif isinstance(payload, list):
            roots.extend(payload)
        for root in roots:
            for record in iter_dicts(root):
                collect_from_record(record, targets, seen)
    return [target for target in targets if target.get("comment_id") or target.get("attachment_id") or target.get("file_url")]


def attachment_resources_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    if isinstance(payload.get("id"), str):
        return [payload]
    return []


def metadata_to_targets(target: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in attachment_resources_from_payload(payload):
        results.append(
            {
                "comment_id": maybe_text(target.get("comment_id")),
                "attachment_id": attachment_id_from_item(item) or maybe_text(target.get("attachment_id")),
                "file_url": attachment_file_url(item),
                "metadata": item,
                "comment_attributes": target.get("comment_attributes") if isinstance(target.get("comment_attributes"), dict) else {},
                "source": "metadata-endpoint",
            }
        )
    return results


def safe_filename(*, comment_id: str, attachment_id: str, url: str, content_type: str) -> str:
    suffix = Path(parse.urlparse(url).path).suffix
    if not suffix:
        suffix = mimetypes.guess_extension(content_type.split(";", 1)[0].strip()) or ".bin"
    stem = "-".join(item for item in (comment_id, attachment_id) if item) or hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-") or "attachment"
    return f"{stem}{suffix}"


def download_file(target: dict[str, Any], *, config: RuntimeConfig, output_dir: Path, overwrite: bool) -> dict[str, Any]:
    file_url = maybe_text(target.get("file_url"))
    if not file_url:
        return {"status": "skipped", "reason": "missing-file-url", "target": target}
    payload, headers, status_code = http_get_bytes(file_url, config, authenticated=False)
    content_type = headers.get("content-type", "")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / safe_filename(
        comment_id=maybe_text(target.get("comment_id")),
        attachment_id=maybe_text(target.get("attachment_id")),
        url=file_url,
        content_type=content_type,
    )
    if output_path.exists() and not overwrite:
        raise RuntimeError(f"Output file already exists: {output_path}")
    output_path.write_bytes(payload)
    return {
        "status": "downloaded",
        "comment_id": maybe_text(target.get("comment_id")),
        "attachment_id": maybe_text(target.get("attachment_id")),
        "file_url": file_url,
        "output_path": str(output_path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "byte_size": len(payload),
        "content_type": content_type,
        "status_code": status_code,
        "metadata": target.get("metadata") if isinstance(target.get("metadata"), dict) else {},
        "comment_attributes": target.get("comment_attributes") if isinstance(target.get("comment_attributes"), dict) else {},
    }


def resolve_metadata_targets(targets: list[dict[str, Any]], *, config: RuntimeConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    resolved: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    seen: set[str] = set()
    for target in targets:
        if target.get("file_url"):
            add_target(resolved, target, seen)
            continue
        try:
            if target.get("comment_id") and not target.get("attachment_id"):
                url = render_query(config.base_url, f"comments/{parse.quote(maybe_text(target['comment_id']), safe='')}/attachments")
            elif target.get("attachment_id"):
                url = render_query(config.base_url, f"attachments/{parse.quote(maybe_text(target['attachment_id']), safe='')}")
            else:
                add_target(resolved, target, seen)
                continue
            payload = http_get_json(url, config)
            metadata_targets = metadata_to_targets(target, payload)
            if not metadata_targets:
                failures.append({"target": target, "error": "metadata endpoint returned no attachment resources", "request_url": url})
            for item in metadata_targets:
                add_target(resolved, item, seen)
        except Exception as exc:  # noqa: BLE001
            failures.append({"target": target, "error": str(exc)})
    return resolved, failures


def write_manifest(path: Path, payload: dict[str, Any], overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise RuntimeError(f"Manifest already exists: {path}")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None, separators=None if pretty else (",", ":")))


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    print_json(
        {
            "ok": True,
            "config": {
                "base_url": config.base_url,
                "api_key_masked": mask_api_key(config.api_key),
                "timeout_seconds": config.timeout_seconds,
                "user_agent": config.user_agent,
            },
            "source_urls": {
                "comment_attachments_template": f"{config.base_url}/comments/{{commentId}}/attachments",
                "attachment_detail_template": f"{config.base_url}/attachments/{{attachmentId}}",
            },
        },
        pretty=args.pretty,
    )
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    if args.max_attachments < 0:
        raise ValueError("--max-attachments must be >= 0.")
    targets = load_targets(
        comment_ids=args.comment_id,
        attachment_ids=args.attachment_id,
        input_artifacts=args.input_artifact,
    )
    if args.max_attachments > 0:
        targets = targets[: args.max_attachments]
    if not targets:
        raise ValueError("No attachment or comment targets provided.")
    sample_urls: list[str] = []
    for target in targets[:5]:
        if target.get("file_url"):
            sample_urls.append(maybe_text(target.get("file_url")))
        elif target.get("comment_id") and not target.get("attachment_id"):
            sample_urls.append(render_query(config.base_url, f"comments/{parse.quote(maybe_text(target['comment_id']), safe='')}/attachments"))
        elif target.get("attachment_id"):
            sample_urls.append(render_query(config.base_url, f"attachments/{parse.quote(maybe_text(target['attachment_id']), safe='')}"))
    if args.dry_run:
        print_json(
            {
                "ok": True,
                "dry_run": True,
                "request_plan": {
                    "target_count": len(targets),
                    "download_files": args.download_files,
                    "max_attachments": args.max_attachments,
                    "output_dir": args.output_dir,
                    "manifest_output": args.manifest_output,
                },
                "sample_targets": targets[:5],
                "sample_request_urls": sample_urls,
            },
            pretty=args.pretty,
        )
        return 0
    resolved_targets, metadata_failures = resolve_metadata_targets(targets, config=config)
    downloads: list[dict[str, Any]] = []
    download_failures: list[dict[str, Any]] = []
    if args.max_attachments > 0:
        resolved_targets = resolved_targets[: args.max_attachments]
    if args.download_files:
        for target in resolved_targets:
            try:
                downloads.append(
                    download_file(
                        target,
                        config=config,
                        output_dir=Path(args.output_dir).expanduser().resolve(),
                        overwrite=args.overwrite,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                failure = {"target": target, "error": str(exc)}
                download_failures.append(failure)
                if args.fail_on_download_error:
                    raise RuntimeError(f"Attachment download failed: {exc}") from exc
    manifest = {
        "ok": not metadata_failures and not download_failures,
        "source": "regulationsgov-v4-attachments",
        "target_count": len(targets),
        "resolved_attachment_count": len(resolved_targets),
        "downloaded_count": sum(1 for item in downloads if item.get("status") == "downloaded"),
        "skipped_count": sum(1 for item in downloads if item.get("status") == "skipped"),
        "download_files": args.download_files,
        "targets": targets if args.include_records else [],
        "records": resolved_targets if args.include_records else [],
        "downloads": downloads,
        "failures": [*metadata_failures, *download_failures],
        "source_limitations": [
            "Missing fileUrl or failed download is not evidence that attachment content is absent.",
            "Downloaded files still require text extraction or OCR before semantic analysis.",
        ],
    }
    manifest_path = Path(args.manifest_output).expanduser().resolve() if args.manifest_output else Path(args.output_dir).expanduser().resolve() / "regulationsgov-attachments-manifest.json"
    write_manifest(manifest_path, manifest, overwrite=args.overwrite)
    manifest["manifest_output"] = str(manifest_path)
    print_json(manifest, pretty=args.pretty)
    return 0


def add_runtime_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help=f"Override base URL. Default: {DEFAULT_BASE_URL}")
    parser.add_argument("--api-key", default="", help=f"Override API key. Env: {ENV_API_KEY}.")
    parser.add_argument("--timeout-seconds", type=int, default=None)
    parser.add_argument("--user-agent", default=None)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Regulations.gov attachment metadata and files.")
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check-config")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true")

    fetch = sub.add_parser("fetch")
    add_runtime_config_args(fetch)
    fetch.add_argument("--comment-id", action="append", default=[])
    fetch.add_argument("--attachment-id", action="append", default=[])
    fetch.add_argument("--input-artifact", action="append", default=[], help="Detail/list/audit artifact to inspect for comment or attachment IDs.")
    fetch.add_argument("--max-attachments", type=int, default=0, help="Limit targets/resolved attachments; 0 means all.")
    fetch.add_argument("--download-files", action=argparse.BooleanOptionalAction, default=True)
    fetch.add_argument("--include-records", action=argparse.BooleanOptionalAction, default=True)
    fetch.add_argument("--fail-on-download-error", action=argparse.BooleanOptionalAction, default=False)
    fetch.add_argument("--output-dir", default="./runs/manual-fetch-artifacts/regulationsgov-attachments")
    fetch.add_argument("--manifest-output", default="")
    fetch.add_argument("--overwrite", action="store_true")
    fetch.add_argument("--dry-run", action="store_true")
    fetch.add_argument("--pretty", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "check-config":
        return command_check_config(args)
    if args.command == "fetch":
        return command_fetch(args)
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

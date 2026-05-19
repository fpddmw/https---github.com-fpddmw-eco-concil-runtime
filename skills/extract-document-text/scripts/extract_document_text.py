#!/usr/bin/env python3
"""Extract bounded text artifacts from local documents."""

from __future__ import annotations

import argparse
import html.parser
import json
import mimetypes
import re
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "extract-document-text"


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def pretty_json(payload: Any, pretty: bool) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None, separators=None if pretty else (",", ":"))


class HTMLTextExtractor(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript"}:
            self._skip_depth += 1
        if tag.lower() in {"p", "br", "li", "tr", "div", "section", "article"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
        if tag.lower() in {"p", "li", "tr", "div", "section", "article"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            text = maybe_text(data)
            if text:
                self.parts.append(text)

    def text(self) -> str:
        return re.sub(r"\n{3,}", "\n\n", "\n".join(self.parts)).strip()


def read_text_file(path: Path, max_chars: int) -> tuple[str, list[str]]:
    raw = path.read_bytes()
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            text = raw.decode(encoding)
            flags = [] if encoding.startswith("utf-8") else ["non-utf8-decoding"]
            return text[:max_chars], flags
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")[:max_chars], ["decode-replacement"]


def extract_html(path: Path, max_chars: int) -> tuple[str, list[str]]:
    raw_text, flags = read_text_file(path, max_chars * 3)
    parser = HTMLTextExtractor()
    parser.feed(raw_text)
    return parser.text()[:max_chars], flags


def pdf_reader_class() -> Any:
    try:
        from pypdf import PdfReader  # type: ignore

        return PdfReader
    except Exception:
        try:
            from PyPDF2 import PdfReader  # type: ignore

            return PdfReader
        except Exception:
            return None


def extract_pdf(path: Path, max_chars: int) -> tuple[str, dict[str, Any], list[str]]:
    reader_cls = pdf_reader_class()
    if reader_cls is None:
        return "", {"page_count": 0, "extracted_page_count": 0, "empty_page_count": 0}, [
            "text-extraction-limited",
            "pdf-reader-unavailable",
        ]
    try:
        reader = reader_cls(str(path))
        pages = list(getattr(reader, "pages", []))
        parts: list[str] = []
        extracted_page_count = 0
        empty_page_count = 0
        for page in pages:
            try:
                text = maybe_text(page.extract_text())
            except Exception:
                text = ""
            if text:
                extracted_page_count += 1
                parts.append(text)
            else:
                empty_page_count += 1
            if sum(len(part) for part in parts) >= max_chars:
                break
        text = "\n\n".join(parts)[:max_chars]
        flags = []
        if empty_page_count and empty_page_count >= max(1, len(pages) - extracted_page_count):
            flags.extend(["text-extraction-limited", "scanned-or-ocr-suspected"])
        return text, {
            "page_count": len(pages),
            "extracted_page_count": extracted_page_count,
            "empty_page_count": empty_page_count,
        }, flags
    except Exception as exc:  # noqa: BLE001
        return "", {"page_count": 0, "extracted_page_count": 0, "empty_page_count": 0, "error": str(exc)}, [
            "text-extraction-limited",
            "pdf-parse-failed",
        ]


def read_manifest(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    def record_key(item: dict[str, Any]) -> str:
        return "|".join(
            [
                maybe_text(item.get("output_path")),
                maybe_text(item.get("comment_id")),
                maybe_text(item.get("attachment_id")),
                maybe_text(item.get("file_url")),
            ]
        )

    def append_once(item: dict[str, Any]) -> None:
        key = record_key(item)
        if key and key in seen:
            return
        seen.add(key)
        records.append(item)

    if isinstance(payload, dict):
        for item in payload.get("downloads", []):
            if isinstance(item, dict) and maybe_text(item.get("output_path")):
                append_once(item)
        for item in payload.get("records", []):
            if not isinstance(item, dict):
                continue
            if maybe_text(item.get("output_path")):
                append_once(item)
                continue
            if maybe_text(item.get("file_url")) or maybe_text(item.get("attachment_id")):
                metadata_only = dict(item)
                metadata_only["source_manifest_path"] = str(path)
                metadata_only["metadata_only_reason"] = "attachment-file-not-downloaded"
                append_once(metadata_only)
        for failure in payload.get("failures", []):
            if not isinstance(failure, dict):
                continue
            target = failure.get("target") if isinstance(failure.get("target"), dict) else {}
            if not target:
                continue
            if maybe_text(target.get("file_url")) or maybe_text(target.get("attachment_id")):
                metadata_only = dict(target)
                metadata_only["source_manifest_path"] = str(path)
                metadata_only["metadata_only_reason"] = "attachment-download-failed"
                metadata_only["download_error"] = maybe_text(failure.get("error"))
                append_once(metadata_only)
    return records


def collect_inputs(input_paths: list[str], input_manifests: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_path in input_paths:
        records.append({"output_path": str(Path(raw_path).expanduser().resolve()), "source": "input-path"})
    for raw_manifest in input_manifests:
        manifest_path = Path(raw_manifest).expanduser().resolve()
        for item in read_manifest(manifest_path):
            item = dict(item)
            item["source_manifest_path"] = str(manifest_path)
            records.append(item)
    return records


def output_text_path(output_dir: Path, input_path: Path) -> Path:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", input_path.stem).strip("-") or "document"
    return output_dir / f"{stem}.txt"


def extract_one(record: dict[str, Any], *, output_dir: Path, max_chars: int, overwrite: bool) -> dict[str, Any]:
    raw_output_path = maybe_text(record.get("output_path"))
    if not raw_output_path:
        quality_flags = [
            "document-text-extraction",
            "text-extraction-limited",
            "metadata-only-no-local-file",
        ]
        reason = maybe_text(record.get("metadata_only_reason"))
        if reason:
            quality_flags.append(reason)
        if maybe_text(record.get("download_error")):
            quality_flags.append("attachment-download-failed")
        return {
            **record,
            "input_path": "",
            "output_text_path": "",
            "text_extraction_status": "limited",
            "content_type": maybe_text(record.get("content_type")),
            "page_count": 0,
            "extracted_page_count": 0,
            "empty_page_count": 0,
            "extracted_text_char_count": 0,
            "text_excerpt": "",
            "quality_flags": sorted(set(quality_flags)),
        }
    input_path = Path(raw_output_path).expanduser().resolve()
    content_type = maybe_text(record.get("content_type")) or mimetypes.guess_type(str(input_path))[0] or ""
    suffix = input_path.suffix.lower()
    quality_flags: list[str] = ["document-text-extraction"]
    page_info = {"page_count": 0, "extracted_page_count": 0, "empty_page_count": 0}
    if not input_path.exists():
        return {
            **record,
            "input_path": str(input_path),
            "text_extraction_status": "failed",
            "quality_flags": ["text-extraction-limited", "input-file-missing"],
            "extracted_text_char_count": 0,
        }
    if suffix in {".html", ".htm"} or "html" in content_type:
        text, flags = extract_html(input_path, max_chars)
        quality_flags.extend(flags)
    elif suffix == ".pdf" or "pdf" in content_type:
        text, page_info, flags = extract_pdf(input_path, max_chars)
        quality_flags.extend(flags)
    else:
        text, flags = read_text_file(input_path, max_chars)
        quality_flags.extend(flags)
    text = text.strip()
    status = "completed" if text else "limited"
    if not text and "text-extraction-limited" not in quality_flags:
        quality_flags.append("text-extraction-limited")
    text_path = output_text_path(output_dir, input_path)
    if text:
        output_dir.mkdir(parents=True, exist_ok=True)
        if text_path.exists() and not overwrite:
            raise RuntimeError(f"Output text file already exists: {text_path}")
        text_path.write_text(text, encoding="utf-8")
    return {
        **record,
        "input_path": str(input_path),
        "output_text_path": str(text_path) if text else "",
        "text_extraction_status": status,
        "content_type": content_type,
        **page_info,
        "extracted_text_char_count": len(text),
        "text_excerpt": text[:300],
        "quality_flags": sorted(set(quality_flags)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract text from local documents.")
    parser.add_argument("--input-path", action="append", default=[])
    parser.add_argument("--input-manifest", action="append", default=[])
    parser.add_argument("--output-dir", default="./runs/manual-fetch-artifacts/document-text")
    parser.add_argument("--manifest-output", default="")
    parser.add_argument("--max-chars", type=int, default=200000)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()
    if args.max_chars <= 0:
        raise ValueError("--max-chars must be > 0.")
    inputs = collect_inputs(args.input_path, args.input_manifest)
    if not inputs:
        raise ValueError("Provide --input-path or --input-manifest.")
    output_dir = Path(args.output_dir).expanduser().resolve()
    records = [
        extract_one(item, output_dir=output_dir, max_chars=args.max_chars, overwrite=args.overwrite)
        for item in inputs
    ]
    manifest_output = Path(args.manifest_output).expanduser().resolve() if args.manifest_output else output_dir / "document_text_extraction_manifest.json"
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "document-text-extraction-v1",
        "skill": SKILL_NAME,
        "status": "completed",
        "record_count": len(records),
        "completed_count": sum(1 for item in records if item.get("text_extraction_status") == "completed"),
        "limited_count": sum(1 for item in records if item.get("text_extraction_status") == "limited"),
        "records": records,
        "source_limitations": [
            "Limited or empty extraction is not proof that the source document lacks relevant content.",
            "Scanned PDFs may require OCR or manual review.",
        ],
    }
    manifest_output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["manifest_output"] = str(manifest_output)
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

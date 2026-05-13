from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from eco_council_runtime.kernel.planes.signal import file_sha256, maybe_number, maybe_text


@dataclass(frozen=True)
class GdeltZipRow:
    download_index: int
    row_index: int
    artifact_path: Path
    artifact_sha256: str
    member_name: str
    request_url: str
    entry: dict[str, Any]
    download: dict[str, Any]
    columns: list[str]


def download_records(payload: Any, warnings: list[dict[str, str]]) -> list[dict[str, Any]]:
    downloads = payload.get("downloads") if isinstance(payload, dict) else None
    if not isinstance(downloads, list):
        warnings.append({"code": "missing-downloads", "message": "Expected payload.downloads to be a list."})
        return []
    return [item for item in downloads if isinstance(item, dict)]


def download_output_path(download: dict[str, Any]) -> Path | None:
    output_path = maybe_text(download.get("output_path"))
    if not output_path:
        return None
    return Path(output_path).expanduser().resolve()


def cleanup_artifact_paths(downloads: list[dict[str, Any]], fallback_artifact_path: Path) -> list[str]:
    values: list[str] = [str(fallback_artifact_path)]
    for download in downloads:
        output_path = download_output_path(download)
        if output_path is not None:
            values.append(str(output_path))
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def parse_compact_utc(value: Any) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if fmt in {"%Y%m%d", "%Y-%m-%d"}:
            parsed = parsed.replace(hour=0, minute=0, second=0)
        return parsed.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return text


def first_text(*values: Any) -> str:
    for value in values:
        text = maybe_text(value)
        if text:
            return text
    return ""


def domain_from_url(value: Any) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    return maybe_text(urlparse(text).netloc)


def split_multi_value(value: Any, *, delimiter: str = ";", limit: int = 0) -> list[str]:
    text = maybe_text(value)
    if not text:
        return []
    items = [maybe_text(item) for item in text.split(delimiter) if maybe_text(item)]
    if limit > 0:
        return items[:limit]
    return items


def gkg_tone_score(value: Any) -> float | None:
    parts = parse_gkg_v2_tone_parts(value)
    tone = parts.get("tone")
    return float(tone) if isinstance(tone, (int, float)) else None


def parse_gkg_v2_tone_parts(value: Any) -> dict[str, Any]:
    text = maybe_text(value)
    if not text:
        return {}
    names = [
        "tone",
        "positive_score",
        "negative_score",
        "polarity",
        "activity_reference_density",
        "self_group_reference_density",
        "word_count",
    ]
    values = [maybe_number(part) for part in text.split(",")]
    parts: dict[str, Any] = {"raw": text, "field_count": len(values)}
    for index, name in enumerate(names):
        if index >= len(values) or values[index] is None:
            continue
        value_number = float(values[index])
        if name == "word_count" and value_number.is_integer():
            parts[name] = int(value_number)
        else:
            parts[name] = value_number
    return parts


def parse_gkg_gcam_cues(value: Any, *, limit: int = 200) -> dict[str, Any]:
    text = maybe_text(value)
    if not text:
        return {}
    dimensions: list[dict[str, Any]] = []
    unparsed: list[str] = []
    word_count: int | None = None
    entries = [maybe_text(part) for part in text.replace(";", ",").split(",") if maybe_text(part)]
    for entry in entries:
        if ":" not in entry:
            unparsed.append(entry)
            continue
        key, raw_value = entry.split(":", 1)
        code = maybe_text(key)
        number = maybe_number(raw_value)
        if not code or number is None:
            unparsed.append(entry)
            continue
        if code.casefold() == "wc":
            word_count_float = float(number)
            word_count = int(word_count_float) if word_count_float.is_integer() else int(round(word_count_float))
            continue
        if len(dimensions) < max(1, int(limit or 200)):
            dimensions.append({"dimension": code, "value": float(number)})
    result: dict[str, Any] = {
        "raw": text,
        "entry_count": len(entries),
        "dimension_count": len(dimensions),
        "dimensions": dimensions,
    }
    if word_count is not None:
        result["word_count"] = word_count
    if unparsed:
        result["unparsed_entries"] = unparsed[:20]
        result["unparsed_count"] = len(unparsed)
    if len(entries) > len(dimensions) + (1 if word_count is not None else 0) + len(unparsed):
        result["truncated"] = True
    return result


def record_locator(member_name: str, row_index: int) -> str:
    return f"zip://{member_name}#row={row_index}"


def raw_row_record(
    *,
    field_names: list[str],
    columns: list[str],
    download_index: int,
    row_index: int,
    artifact_path: Path,
    artifact_sha256: str,
    member_name: str,
    request_url: str,
    entry: dict[str, Any],
) -> dict[str, Any]:
    mapped = {
        field_name: columns[index] if index < len(columns) else ""
        for index, field_name in enumerate(field_names)
    }
    return {
        "download_index": download_index,
        "row_index": row_index,
        "member_name": member_name,
        "artifact_path": str(artifact_path),
        "artifact_sha256": artifact_sha256,
        "request_url": request_url,
        "entry": entry,
        "fields": mapped,
        "line": "\t".join(columns),
    }


def iter_rows_for_download(
    *,
    download: dict[str, Any],
    download_index: int,
    expected_columns: int,
    source_label: str,
    warnings: list[dict[str, str]],
) -> list[GdeltZipRow]:
    output_path = download_output_path(download)
    if output_path is None:
        warnings.append(
            {
                "code": "missing-output-path",
                "message": f"{source_label} download #{download_index} has no output_path, so row-level normalize fell back or skipped.",
            }
        )
        return []
    if not output_path.exists():
        warnings.append(
            {
                "code": "missing-output-file",
                "message": f"{source_label} download output does not exist: {output_path}",
            }
        )
        return []

    rows: list[GdeltZipRow] = []
    mismatch_count = 0
    artifact_sha256 = file_sha256(output_path)
    entry = download.get("entry") if isinstance(download.get("entry"), dict) else {}
    request_url = maybe_text(download.get("request_url")) or maybe_text(entry.get("url"))

    with zipfile.ZipFile(output_path) as zipped:
        members = [item for item in zipped.namelist() if not item.endswith("/")]
        if not members:
            warnings.append(
                {
                    "code": "empty-zip",
                    "message": f"{source_label} download output has no file members: {output_path}",
                }
            )
            return []
        member_name = members[0]
        with zipped.open(member_name, mode="r") as handle:
            wrapper = io.TextIOWrapper(handle, encoding="utf-8", errors="strict", newline="")
            row_index = 0
            for raw_line in wrapper:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                row_index += 1
                columns = line.split("\t")
                if expected_columns > 0 and len(columns) != expected_columns:
                    mismatch_count += 1
                    continue
                rows.append(
                    GdeltZipRow(
                        download_index=download_index,
                        row_index=row_index,
                        artifact_path=output_path,
                        artifact_sha256=artifact_sha256,
                        member_name=member_name,
                        request_url=request_url,
                        entry=entry,
                        download=download,
                        columns=columns,
                    )
                )
    if mismatch_count:
        warnings.append(
            {
                "code": "row-column-mismatch",
                "message": f"{source_label} download {output_path.name} skipped {mismatch_count} rows with unexpected column counts.",
            }
        )
    return rows


__all__ = [
    "GdeltZipRow",
    "cleanup_artifact_paths",
    "domain_from_url",
    "download_output_path",
    "download_records",
    "first_text",
    "gkg_tone_score",
    "iter_rows_for_download",
    "parse_compact_utc",
    "parse_gkg_gcam_cues",
    "parse_gkg_v2_tone_parts",
    "raw_row_record",
    "record_locator",
    "split_multi_value",
]

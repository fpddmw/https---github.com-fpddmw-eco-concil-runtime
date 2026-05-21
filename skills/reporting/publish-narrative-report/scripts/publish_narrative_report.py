#!/usr/bin/env python3
"""Publish a validated narrative report."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "publish-narrative-report"


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}.")
    return payload


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text_file(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload.rstrip() + "\n", encoding="utf-8")


def publish_narrative_report(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    draft_path: str = "",
    draft_markdown_path: str = "",
    validation_path: str = "",
    output_path: str = "",
    markdown_output_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    draft_file = resolve_path(run_dir_path, draft_path, f"reporting/narrative_report_draft_{round_id}.json")
    draft_markdown_file = resolve_path(run_dir_path, draft_markdown_path, f"reporting/narrative_report_draft_{round_id}.md")
    validation_file = resolve_path(run_dir_path, validation_path, f"reporting/narrative_report_validation_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/narrative_report_{round_id}.json")
    markdown_file = resolve_path(run_dir_path, markdown_output_path, f"reporting/narrative_report_{round_id}.md")
    draft = load_json_file(draft_file)
    validation = load_json_file(validation_file)
    if maybe_text(validation.get("status")) != "valid" or validation.get("publish_allowed") is not True:
        raise ValueError("Narrative report validation is not valid for publication.")
    if maybe_text(validation.get("draft_id")) != maybe_text(draft.get("draft_id")):
        raise ValueError("Validation draft_id does not match the draft artifact.")
    publication_id = "narrative-report-" + stable_hash(run_id, round_id, draft.get("draft_id"), validation.get("validation_id"))[:12]
    published = {
        **draft,
        "schema_version": "narrative-report-v1",
        "publication_id": publication_id,
        "published_at_utc": utc_now_iso(),
        "status": "canonical-published",
        "draft_id": maybe_text(draft.get("draft_id")),
        "validation_id": maybe_text(validation.get("validation_id")),
        "validation_status": maybe_text(validation.get("status")),
        "publication_policy": "validated draft promotion; no new facts added during publish",
    }
    markdown = load_text_if_exists(draft_markdown_file)
    if markdown:
        markdown = markdown.replace("# Narrative Report Draft", "# Narrative Report", 1)
    else:
        markdown = f"# {maybe_text(published.get('title')) or 'Narrative Report'}\n\nPublished from draft {published['draft_id']}."
    write_json_file(output_file, published)
    write_text_file(markdown_file, markdown)
    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
        {"signal_id": "", "artifact_path": str(markdown_file), "record_locator": "$", "artifact_ref": f"{markdown_file}:$"},
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "publication_id": publication_id,
            "draft_id": published["draft_id"],
            "validation_id": published["validation_id"],
            "output_path": str(output_file),
            "markdown_output_path": str(markdown_file),
        },
        "receipt_id": "report-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, publication_id)[:20],
        "batch_id": "reportbatch-" + stable_hash(SKILL_NAME, run_id, round_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [publication_id],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [publication_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": [],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a validated narrative report.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--draft-path", default="")
    parser.add_argument("--draft-markdown-path", default="")
    parser.add_argument("--validation-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--markdown-output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = publish_narrative_report(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        draft_path=args.draft_path,
        draft_markdown_path=args.draft_markdown_path,
        validation_path=args.validation_path,
        output_path=args.output_path,
        markdown_output_path=args.markdown_output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

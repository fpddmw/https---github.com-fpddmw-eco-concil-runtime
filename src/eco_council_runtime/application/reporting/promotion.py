"""Draft loading and promotion flows for reporting artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists, write_json
from eco_council_runtime.application.reporting.common import report_is_placeholder
from eco_council_runtime.controller.audit_chain import record_decision_phase_receipt
from eco_council_runtime.controller.paths import (
    decision_draft_path,
    decision_target_path,
    investigation_review_draft_path,
    investigation_review_path,
    matching_adjudication_draft_path,
    matching_adjudication_path,
    matching_authorization_draft_path,
    matching_authorization_path,
    report_draft_path,
    report_target_path,
)
from eco_council_runtime.domain.contract_bridge import resolve_schema_version, validate_payload_or_raise as validate_payload
from eco_council_runtime.drafts import (
    can_replace_existing_exact,
    can_replace_existing_report,
    load_draft_payload,
    promote_draft,
)

SCHEMA_VERSION = resolve_schema_version("1.0.0")


def load_report_draft_payload(run_dir: Path, round_id: str, role: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=report_draft_path(run_dir, round_id, role),
        label=f"{role} report draft",
        round_error_label="Report draft",
        expected_round_id=round_id,
        expected_role=role,
        role_error_label="Report draft",
        kind="expert-report",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_decision_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=decision_draft_path(run_dir, round_id),
        label="moderator decision draft",
        round_error_label="Decision draft",
        expected_round_id=round_id,
        expected_role=None,
        role_error_label=None,
        kind="council-decision",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_matching_authorization_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=matching_authorization_draft_path(run_dir, round_id),
        label="moderator matching-authorization draft",
        round_error_label="Matching-authorization draft",
        expected_round_id=round_id,
        expected_role="moderator",
        role_error_label="Matching-authorization draft",
        kind="matching-authorization",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_matching_adjudication_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=matching_adjudication_draft_path(run_dir, round_id),
        label="moderator matching-adjudication draft",
        round_error_label="Matching-adjudication draft",
        expected_round_id=round_id,
        expected_role="moderator",
        role_error_label="Matching-adjudication draft",
        kind="matching-adjudication",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def load_investigation_review_draft_payload(run_dir: Path, round_id: str, draft_path_text: str) -> tuple[Path, dict[str, Any]]:
    return load_draft_payload(
        draft_path_text=draft_path_text,
        default_path=investigation_review_draft_path(run_dir, round_id),
        label="moderator investigation-review draft",
        round_error_label="Investigation-review draft",
        expected_round_id=round_id,
        expected_role="moderator",
        role_error_label="Investigation-review draft",
        kind="investigation-review",
        load_json_if_exists=load_json_if_exists,
        validate_payload=validate_payload,
    )


def promote_report_draft(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_report_draft_payload(run_dir, round_id, role, draft_path_text)
    result = promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=report_target_path(run_dir, round_id, role),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical report",
        overwrite_error_message="Refusing to overwrite non-placeholder canonical report without --allow-overwrite",
        can_replace_existing=lambda existing_payload, new_payload: can_replace_existing_report(
            existing_payload,
            new_payload,
            report_is_placeholder=report_is_placeholder,
        ),
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )
    return {
        **result,
        "role": role,
    }


def promote_decision_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_decision_draft_payload(run_dir, round_id, draft_path_text)
    result = promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=decision_target_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical decision",
        overwrite_error_message="Refusing to overwrite canonical decision without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )
    record_decision_phase_receipt(
        run_dir=run_dir,
        round_id=round_id,
        decision_payload=payload,
    )
    return result


def promote_matching_authorization_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_matching_authorization_draft_payload(run_dir, round_id, draft_path_text)
    return promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=matching_authorization_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical matching-authorization",
        overwrite_error_message="Refusing to overwrite canonical matching-authorization without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )


def promote_matching_adjudication_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_matching_adjudication_draft_payload(run_dir, round_id, draft_path_text)
    return promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=matching_adjudication_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical matching-adjudication",
        overwrite_error_message="Refusing to overwrite canonical matching-adjudication without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )


def promote_investigation_review_draft(
    *,
    run_dir: Path,
    round_id: str,
    draft_path_text: str,
    pretty: bool,
    allow_overwrite: bool,
) -> dict[str, Any]:
    draft_path, payload = load_investigation_review_draft_payload(run_dir, round_id, draft_path_text)
    return promote_draft(
        draft_path=draft_path,
        payload=payload,
        target_path=investigation_review_path(run_dir, round_id),
        pretty=pretty,
        allow_overwrite=allow_overwrite,
        existing_label="canonical investigation-review",
        overwrite_error_message="Refusing to overwrite canonical investigation-review without --allow-overwrite",
        can_replace_existing=can_replace_existing_exact,
        load_json_if_exists=load_json_if_exists,
        write_json=lambda path, content, pretty_flag: write_json(path, content, pretty=pretty_flag),
    )


__all__ = [
    "load_decision_draft_payload",
    "load_investigation_review_draft_payload",
    "load_matching_adjudication_draft_payload",
    "load_matching_authorization_draft_payload",
    "load_report_draft_payload",
    "promote_decision_draft",
    "promote_investigation_review_draft",
    "promote_matching_adjudication_draft",
    "promote_matching_authorization_draft",
    "promote_report_draft",
]

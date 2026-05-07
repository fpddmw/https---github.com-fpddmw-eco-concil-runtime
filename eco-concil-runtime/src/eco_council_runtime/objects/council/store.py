from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .schema import connect_db
from .payloads import (
    normalized_discussion_message_payload,
    normalized_evidence_bundle_payload,
    normalized_finding_payload,
    normalized_proposal_payload,
    normalized_readiness_opinion_payload,
)
from .rows import (
    discussion_message_row_from_payload,
    evidence_bundle_row_from_payload,
    finding_row_from_payload,
    proposal_row_from_payload,
    readiness_opinion_row_from_payload,
    review_comment_row_from_payload,
    write_council_proposal_row,
    write_discussion_message_row,
    write_evidence_bundle_row,
    write_finding_row,
    write_readiness_opinion_row,
    write_review_comment_row,
)
from .payloads import normalized_review_comment_payload
from eco_council_runtime.kernel.planes.deliberation_plane import maybe_text, utc_now_iso


def next_round_object_index(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    run_id: str,
    round_id: str,
) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE run_id = ? AND round_id = ?",
        (maybe_text(run_id), maybe_text(round_id)),
    ).fetchone()
    return int(row["row_count"]) if row is not None else 0


def append_council_proposal_record(
    run_dir: str | Path,
    *,
    proposal_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.proposal",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(proposal_payload) if isinstance(proposal_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            proposal_index = next_round_object_index(
                connection,
                table_name="council_proposals",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_proposal_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                proposal_index=proposal_index,
            )
            write_council_proposal_row(
                connection,
                proposal_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "council-proposal-append-v1",
        "db_path": str(db_file),
        "proposal": normalized,
    }


def append_finding_record(
    run_dir: str | Path,
    *,
    finding_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.finding",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(finding_payload) if isinstance(finding_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            finding_index = next_round_object_index(
                connection,
                table_name="finding_records",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_finding_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                finding_index=finding_index,
            )
            write_finding_row(
                connection,
                finding_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "finding-record-append-v1",
        "db_path": str(db_file),
        "finding": normalized,
    }


def append_discussion_message_record(
    run_dir: str | Path,
    *,
    message_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.message",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(message_payload) if isinstance(message_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            message_index = next_round_object_index(
                connection,
                table_name="discussion_messages",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_discussion_message_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                message_index=message_index,
            )
            write_discussion_message_row(
                connection,
                discussion_message_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "discussion-message-append-v1",
        "db_path": str(db_file),
        "message": normalized,
    }


def append_evidence_bundle_record(
    run_dir: str | Path,
    *,
    bundle_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.bundle",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(bundle_payload) if isinstance(bundle_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            bundle_index = next_round_object_index(
                connection,
                table_name="evidence_bundles",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_evidence_bundle_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                bundle_index=bundle_index,
            )
            write_evidence_bundle_row(
                connection,
                evidence_bundle_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "evidence-bundle-append-v1",
        "db_path": str(db_file),
        "bundle": normalized,
    }


def append_review_comment_record(
    run_dir: str | Path,
    *,
    comment_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.comment",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(comment_payload) if isinstance(comment_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            comment_index = next_round_object_index(
                connection,
                table_name="review_comments",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_review_comment_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                comment_index=comment_index,
            )
            write_review_comment_row(
                connection,
                review_comment_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "review-comment-append-v1",
        "db_path": str(db_file),
        "comment": normalized,
    }


def append_readiness_opinion_record(
    run_dir: str | Path,
    *,
    opinion_payload: dict[str, Any],
    artifact_path: str = "",
    record_locator: str = "$.opinion",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(opinion_payload) if isinstance(opinion_payload, dict) else {}
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            opinion_index = next_round_object_index(
                connection,
                table_name="readiness_opinions",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_readiness_opinion_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                opinion_index=opinion_index,
            )
            write_readiness_opinion_row(
                connection,
                readiness_opinion_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "readiness-opinion-append-v1",
        "db_path": str(db_file),
        "opinion": normalized,
    }


def store_council_proposal_records(
    run_dir: str | Path,
    *,
    proposal_bundle: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    bundle = dict(proposal_bundle) if isinstance(proposal_bundle, dict) else {}
    proposals = bundle.get("proposals", []) if isinstance(bundle.get("proposals"), list) else []
    run_id = maybe_text(bundle.get("run_id"))
    round_id = maybe_text(bundle.get("round_id"))
    normalized_proposals = [
        normalized_proposal_payload(
            proposal,
            run_id=run_id,
            round_id=round_id,
            proposal_index=index,
        )
        for index, proposal in enumerate(proposals)
        if isinstance(proposal, dict)
    ]
    if normalized_proposals:
        run_id = maybe_text(run_id) or maybe_text(normalized_proposals[0].get("run_id"))
        round_id = maybe_text(round_id) or maybe_text(normalized_proposals[0].get("round_id"))
    bundle["schema_version"] = "council-proposal-bundle-v1"
    bundle["run_id"] = run_id
    bundle["round_id"] = round_id
    bundle["generated_at_utc"] = (
        maybe_text(bundle.get("generated_at_utc"))
        or maybe_text(normalized_proposals[-1].get("generated_at_utc")) if normalized_proposals else utc_now_iso()
    )
    bundle["proposals"] = normalized_proposals
    bundle["proposal_count"] = len(normalized_proposals)
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM council_proposals WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, proposal in enumerate(normalized_proposals):
                write_council_proposal_row(
                    connection,
                    proposal_row_from_payload(
                        proposal,
                        artifact_path=artifact_path,
                        record_locator=f"$.proposals[{index}]",
                    ),
                )
    finally:
        connection.close()
    return bundle


def store_readiness_opinion_records(
    run_dir: str | Path,
    *,
    opinion_bundle: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    bundle = dict(opinion_bundle) if isinstance(opinion_bundle, dict) else {}
    opinions = bundle.get("opinions", []) if isinstance(bundle.get("opinions"), list) else []
    run_id = maybe_text(bundle.get("run_id"))
    round_id = maybe_text(bundle.get("round_id"))
    normalized_opinions = [
        normalized_readiness_opinion_payload(
            opinion,
            run_id=run_id,
            round_id=round_id,
            opinion_index=index,
        )
        for index, opinion in enumerate(opinions)
        if isinstance(opinion, dict)
    ]
    if normalized_opinions:
        run_id = maybe_text(run_id) or maybe_text(normalized_opinions[0].get("run_id"))
        round_id = maybe_text(round_id) or maybe_text(normalized_opinions[0].get("round_id"))
    bundle["schema_version"] = "readiness-opinion-bundle-v1"
    bundle["run_id"] = run_id
    bundle["round_id"] = round_id
    bundle["generated_at_utc"] = (
        maybe_text(bundle.get("generated_at_utc"))
        or maybe_text(normalized_opinions[-1].get("generated_at_utc")) if normalized_opinions else utc_now_iso()
    )
    bundle["opinions"] = normalized_opinions
    bundle["opinion_count"] = len(normalized_opinions)
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM readiness_opinions WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, opinion in enumerate(normalized_opinions):
                write_readiness_opinion_row(
                    connection,
                    readiness_opinion_row_from_payload(
                        opinion,
                        artifact_path=artifact_path,
                        record_locator=f"$.opinions[{index}]",
                    ),
                )
    finally:
        connection.close()
    return bundle


__all__ = (
    "next_round_object_index",
    "append_council_proposal_record",
    "append_finding_record",
    "append_discussion_message_record",
    "append_evidence_bundle_record",
    "append_review_comment_record",
    "append_readiness_opinion_record",
    "store_council_proposal_records",
    "store_readiness_opinion_records",
)

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .schema import connect_db
from .payloads import (
    DYNAMIC_INVESTIGATION_OBJECT_KINDS,
    normalized_discussion_message_payload,
    normalized_dynamic_investigation_object_payload,
    normalized_evidence_bundle_payload,
    normalized_finding_payload,
    normalized_proposal_payload,
    normalized_readiness_opinion_payload,
)
from .rows import (
    discussion_message_row_from_payload,
    dynamic_investigation_object_row_from_payload,
    evidence_bundle_row_from_payload,
    finding_row_from_payload,
    proposal_row_from_payload,
    readiness_opinion_row_from_payload,
    review_comment_row_from_payload,
    write_council_proposal_row,
    write_discussion_message_row,
    write_dynamic_investigation_object_row,
    write_evidence_bundle_row,
    write_finding_row,
    write_readiness_opinion_row,
    write_review_comment_row,
)
from .payloads import normalized_review_comment_payload
from eco_council_runtime.kernel.planes.deliberation_plane import maybe_text, utc_now_iso


SOURCE_ACQUISITION_PROPOSAL_STATUSES = (
    "proposed",
    "approved-for-execution",
    "fetched",
    "normalized",
    "receipt-only",
    "failed",
    "blocked",
    "executed",
    "withdrawn",
    "rejected",
)


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


def append_dynamic_investigation_object_record(
    run_dir: str | Path,
    *,
    object_payload: dict[str, Any],
    object_kind: str = "",
    artifact_path: str = "",
    record_locator: str = "$.object",
    db_path: str = "",
) -> dict[str, Any]:
    payload = dict(object_payload) if isinstance(object_payload, dict) else {}
    normalized_kind = maybe_text(object_kind) or maybe_text(payload.get("object_kind"))
    if normalized_kind not in DYNAMIC_INVESTIGATION_OBJECT_KINDS:
        supported = ", ".join(DYNAMIC_INVESTIGATION_OBJECT_KINDS)
        raise ValueError(
            f"Unsupported dynamic investigation object kind: "
            f"{normalized_kind or '<empty>'}. Supported kinds: {supported}."
        )
    run_id = maybe_text(payload.get("run_id"))
    round_id = maybe_text(payload.get("round_id"))
    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            object_index = next_round_object_index(
                connection,
                table_name="dynamic_investigation_objects",
                run_id=run_id,
                round_id=round_id,
            )
            normalized = normalized_dynamic_investigation_object_payload(
                payload,
                run_id=run_id,
                round_id=round_id,
                object_kind=normalized_kind,
                object_index=object_index,
            )
            write_dynamic_investigation_object_row(
                connection,
                dynamic_investigation_object_row_from_payload(
                    normalized,
                    artifact_path=artifact_path,
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "dynamic-investigation-object-append-v1",
        "db_path": str(db_file),
        "object": normalized,
    }


def _row_payload(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    try:
        payload = json.loads(maybe_text(row["raw_json"]))
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    for key in row.keys():
        if key == "raw_json":
            continue
        value = row[key]
        if key.endswith("_json"):
            try:
                decoded = json.loads(maybe_text(value))
            except json.JSONDecodeError:
                decoded = [] if key.endswith("refs_json") or key == "lineage_json" else {}
            if isinstance(decoded, (dict, list)):
                payload[key[:-5]] = decoded
            continue
        if value is not None:
            payload[key] = value
    return payload


def _unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def fetch_dynamic_investigation_object_record(
    run_dir: str | Path,
    *,
    object_id: str,
    object_kind: str = "",
    run_id: str = "",
    round_id: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    normalized_object_id = maybe_text(object_id)
    if not normalized_object_id:
        raise ValueError("Dynamic investigation object lookup requires object_id.")
    connection, db_file = connect_db(run_dir, db_path)
    try:
        where_clauses = [
            "(object_id = ? OR json_extract(raw_json, '$.proposal_id') = ?)"
        ]
        params: list[str] = [normalized_object_id, normalized_object_id]
        if maybe_text(object_kind):
            where_clauses.append("object_kind = ?")
            params.append(maybe_text(object_kind))
        if maybe_text(run_id):
            where_clauses.append("run_id = ?")
            params.append(maybe_text(run_id))
        if maybe_text(round_id):
            where_clauses.append("round_id = ?")
            params.append(maybe_text(round_id))
        row = connection.execute(
            """
            SELECT *
            FROM dynamic_investigation_objects
            WHERE """ + " AND ".join(where_clauses) + """
            ORDER BY generated_at_utc DESC, object_id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise ValueError(
            "Dynamic investigation object not found: "
            + normalized_object_id
            + (f" ({maybe_text(object_kind)})" if maybe_text(object_kind) else "")
        )
    return {
        "schema_version": "dynamic-investigation-object-fetch-v1",
        "db_path": str(db_file),
        "object": _row_payload(row),
    }


def update_dynamic_investigation_object_status(
    run_dir: str | Path,
    *,
    object_id: str,
    status: str,
    object_kind: str = "",
    run_id: str = "",
    round_id: str = "",
    actor_role: str = "",
    status_rationale: str = "",
    evidence_refs: list[Any] | None = None,
    lineage: list[Any] | None = None,
    provenance: dict[str, Any] | None = None,
    payload_updates: dict[str, Any] | None = None,
    artifact_path: str = "",
    record_locator: str = "$.object",
    db_path: str = "",
) -> dict[str, Any]:
    normalized_status = maybe_text(status)
    if not normalized_status:
        raise ValueError("Dynamic investigation object status update requires status.")
    existing_result = fetch_dynamic_investigation_object_record(
        run_dir,
        object_id=object_id,
        object_kind=object_kind,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    existing = (
        dict(existing_result.get("object", {}))
        if isinstance(existing_result.get("object"), dict)
        else {}
    )
    normalized_kind = maybe_text(object_kind) or maybe_text(existing.get("object_kind"))
    if normalized_kind not in DYNAMIC_INVESTIGATION_OBJECT_KINDS:
        supported = ", ".join(DYNAMIC_INVESTIGATION_OBJECT_KINDS)
        raise ValueError(
            f"Unsupported dynamic investigation object kind: "
            f"{normalized_kind or '<empty>'}. Supported kinds: {supported}."
        )
    if (
        normalized_kind == "source-acquisition-proposal"
        and normalized_status not in SOURCE_ACQUISITION_PROPOSAL_STATUSES
    ):
        raise ValueError(
            "Unsupported source-acquisition-proposal status: "
            f"{normalized_status}. Supported statuses: "
            + ", ".join(SOURCE_ACQUISITION_PROPOSAL_STATUSES)
            + "."
        )

    now = utc_now_iso()
    merged_provenance = (
        dict(existing.get("provenance"))
        if isinstance(existing.get("provenance"), dict)
        else {}
    )
    if isinstance(provenance, dict):
        merged_provenance.update(provenance)
    merged_provenance.setdefault("decision_source", maybe_text(existing.get("decision_source")))
    merged_provenance["status_updated_by_role"] = (
        maybe_text(actor_role) or maybe_text(existing.get("author_role"))
    )
    merged_provenance["status_updated_at_utc"] = now

    existing_updates = (
        list(existing.get("status_updates"))
        if isinstance(existing.get("status_updates"), list)
        else []
    )
    status_update = {
        "previous_status": maybe_text(existing.get("status")),
        "status": normalized_status,
        "updated_at_utc": now,
        "updated_by_role": maybe_text(actor_role) or maybe_text(existing.get("author_role")),
        "rationale": maybe_text(status_rationale),
        "evidence_refs": _unique_texts(list(evidence_refs or [])),
    }

    payload = dict(existing)
    if isinstance(payload_updates, dict):
        immutable_fields = {
            "object_kind",
            "run_id",
            "round_id",
            "object_id",
            "proposal_id",
            "synthesis_id",
        }
        for key, value in payload_updates.items():
            if key in immutable_fields:
                continue
            payload[key] = value
    payload["status"] = normalized_status
    payload["status_updated_at_utc"] = now
    payload["status_updated_by_role"] = status_update["updated_by_role"]
    if maybe_text(status_rationale):
        payload["status_rationale"] = maybe_text(status_rationale)
    payload["status_updates"] = [*existing_updates, status_update]
    payload["evidence_refs"] = _unique_texts(
        [
            *(existing.get("evidence_refs") if isinstance(existing.get("evidence_refs"), list) else []),
            *(evidence_refs or []),
        ]
    )
    payload["lineage"] = _unique_texts(
        [
            *(existing.get("lineage") if isinstance(existing.get("lineage"), list) else []),
            *(lineage or []),
            maybe_text(existing.get("object_id")),
        ]
    )
    payload["provenance"] = merged_provenance

    connection, db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            normalized = normalized_dynamic_investigation_object_payload(
                payload,
                run_id=maybe_text(run_id) or maybe_text(existing.get("run_id")),
                round_id=maybe_text(round_id) or maybe_text(existing.get("round_id")),
                object_kind=normalized_kind,
                object_index=0,
            )
            write_dynamic_investigation_object_row(
                connection,
                dynamic_investigation_object_row_from_payload(
                    normalized,
                    artifact_path=artifact_path or maybe_text(existing.get("artifact_path")),
                    record_locator=record_locator,
                ),
            )
    finally:
        connection.close()
    return {
        "schema_version": "dynamic-investigation-object-status-update-v1",
        "db_path": str(db_file),
        "object": normalized,
        "status_update": status_update,
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
    "append_dynamic_investigation_object_record",
    "fetch_dynamic_investigation_object_record",
    "update_dynamic_investigation_object_status",
    "SOURCE_ACQUISITION_PROPOSAL_STATUSES",
    "store_council_proposal_records",
    "store_readiness_opinion_records",
)

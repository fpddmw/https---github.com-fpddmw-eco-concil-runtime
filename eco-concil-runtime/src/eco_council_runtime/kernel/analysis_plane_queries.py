from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .analysis_plane_contracts import (
    ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE,
    analysis_config,
    analysis_kind_governance,
    decode_json,
    maybe_number,
    maybe_text,
    unique_texts,
)
from .analysis_plane_support import (
    _artifact_present,
    load_result_contract,
    unique_artifact_refs,
)
from .analysis_plane_results import _load_result_items
from .analysis_plane_schema import connect_db, resolve_run_dir

def _paged_rows(
    rows: list[sqlite3.Row],
    *,
    limit: int,
    offset: int,
) -> tuple[list[sqlite3.Row], int, int]:
    safe_offset = max(0, int(offset))
    safe_limit = int(limit)
    if safe_limit <= 0:
        return rows[safe_offset:], safe_offset, 0
    return rows[safe_offset : safe_offset + safe_limit], safe_offset, safe_limit

def _select_matching_result_set_rows(
    connection: sqlite3.Connection,
    *,
    result_set_id: str = "",
    run_id: str = "",
    round_id: str = "",
    analysis_kind: str = "",
    source_skill: str = "",
    artifact_path: str = "",
    latest_only: bool = False,
) -> list[sqlite3.Row]:
    result_set_text = maybe_text(result_set_id)
    run_text = maybe_text(run_id)
    round_text = maybe_text(round_id)
    analysis_text = maybe_text(analysis_kind)
    source_text = maybe_text(source_skill)
    artifact_text = maybe_text(artifact_path)
    if analysis_text:
        analysis_config(analysis_text)

    where: list[str] = []
    params: list[str] = []
    if result_set_text:
        where.append("result_set_id = ?")
        params.append(result_set_text)
    if run_text:
        where.append("run_id = ?")
        params.append(run_text)
    if round_text:
        where.append("round_id = ?")
        params.append(round_text)
    if analysis_text:
        where.append("analysis_kind = ?")
        params.append(analysis_text)
    if source_text:
        where.append("source_skill = ?")
        params.append(source_text)
    if artifact_text:
        where.append("artifact_path = ?")
        params.append(artifact_text)
    query = """
        SELECT *
        FROM analysis_result_sets
    """
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY generated_at_utc DESC, result_set_id DESC"
    rows = connection.execute(query, params).fetchall()
    if not latest_only or result_set_text:
        return list(rows)

    seen: set[tuple[str, str, str]] = set()
    deduped: list[sqlite3.Row] = []
    for row in rows:
        key = (
            maybe_text(row["run_id"]),
            maybe_text(row["round_id"]),
            maybe_text(row["analysis_kind"]),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped

def _select_matching_result_item_rows(
    connection: sqlite3.Connection,
    *,
    run_id: str = "",
    round_id: str = "",
    analysis_kind: str = "",
    source_skill: str = "",
    subject_id: str = "",
    readiness: str = "",
    result_set_ids: list[str] | None = None,
) -> list[sqlite3.Row]:
    run_text = maybe_text(run_id)
    round_text = maybe_text(round_id)
    analysis_text = maybe_text(analysis_kind)
    source_text = maybe_text(source_skill)
    subject_text = maybe_text(subject_id)
    readiness_text = maybe_text(readiness)
    if analysis_text:
        analysis_config(analysis_text)

    where: list[str] = []
    params: list[str] = []
    if result_set_ids is not None:
        cleaned_ids = unique_texts(result_set_ids)
        if not cleaned_ids:
            return []
        placeholders = ",".join("?" for _ in cleaned_ids)
        where.append(f"result_set_id IN ({placeholders})")
        params.extend(cleaned_ids)
    if run_text:
        where.append("run_id = ?")
        params.append(run_text)
    if round_text:
        where.append("round_id = ?")
        params.append(round_text)
    if analysis_text:
        where.append("analysis_kind = ?")
        params.append(analysis_text)
    if source_text:
        where.append("source_skill = ?")
        params.append(source_text)
    if subject_text:
        where.append("subject_id = ?")
        params.append(subject_text)
    if readiness_text:
        where.append("readiness = ?")
        params.append(readiness_text)
    query = """
        SELECT *
        FROM analysis_result_items
    """
    if where:
        query += " WHERE " + " AND ".join(where)
    query += """
        ORDER BY generated_at_utc DESC, result_set_id DESC, item_index ASC, item_id ASC
    """
    return list(connection.execute(query, params).fetchall())

def _serialize_result_set_row(
    connection: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    include_contract: bool = False,
    include_items: bool = False,
) -> dict[str, Any]:
    analysis_kind = maybe_text(row["analysis_kind"])
    config = analysis_config(analysis_kind)
    summary = decode_json(maybe_text(row["summary_json"]), {})
    if not isinstance(summary, dict):
        summary = {}
    record = {
        "result_set_id": maybe_text(row["result_set_id"]),
        "run_id": maybe_text(row["run_id"]),
        "round_id": maybe_text(row["round_id"]),
        "analysis_kind": analysis_kind,
        "artifact_label": maybe_text(config.get("artifact_label")) or analysis_kind,
        "analysis_kind_governance": analysis_kind_governance(analysis_kind),
        "items_key": maybe_text(config.get("items_key")) or "items",
        "count_key": maybe_text(config.get("count_key")) or "item_count",
        "source_skill": maybe_text(row["source_skill"]),
        "artifact_path": maybe_text(row["artifact_path"]),
        "artifact_present": _artifact_present(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "generated_at_utc": maybe_text(row["generated_at_utc"]),
        "item_count": int(row["item_count"] or 0),
        "summary": summary,
    }
    if include_contract:
        record["result_contract"] = load_result_contract(
            connection,
            result_set_id=maybe_text(row["result_set_id"]),
        )
    if include_items:
        record["items"] = _load_result_items(
            connection,
            result_set_id=maybe_text(row["result_set_id"]),
        )
    return record

def _serialize_result_item_row(row: sqlite3.Row) -> dict[str, Any]:
    related_ids = decode_json(maybe_text(row["related_ids_json"]), [])
    if not isinstance(related_ids, list):
        related_ids = []
    evidence_refs = decode_json(maybe_text(row["evidence_refs_json"]), [])
    if not isinstance(evidence_refs, list):
        evidence_refs = []
    lineage = decode_json(maybe_text(row["lineage_json"]), [])
    if not isinstance(lineage, list):
        lineage = []
    provenance = decode_json(maybe_text(row["provenance_json"]), {})
    if not isinstance(provenance, dict):
        provenance = {}
    item_payload = decode_json(maybe_text(row["item_json"]), {})
    if not isinstance(item_payload, dict):
        item_payload = {}
    artifact_path = maybe_text(row["artifact_path"])
    return {
        "item_id": maybe_text(row["item_id"]),
        "result_set_id": maybe_text(row["result_set_id"]),
        "run_id": maybe_text(row["run_id"]),
        "round_id": maybe_text(row["round_id"]),
        "analysis_kind": maybe_text(row["analysis_kind"]),
        "analysis_kind_governance": analysis_kind_governance(
            maybe_text(row["analysis_kind"])
        ),
        "source_skill": maybe_text(row["source_skill"]),
        "item_index": int(row["item_index"] or 0),
        "subject_id": maybe_text(row["subject_id"]),
        "readiness": maybe_text(row["readiness"]),
        "decision_source": maybe_text(row["decision_source"]),
        "score": maybe_number(row["score"]),
        "related_ids": related_ids,
        "evidence_refs": evidence_refs,
        "lineage": lineage,
        "provenance": provenance,
        "artifact_path": artifact_path,
        "artifact_present": _artifact_present(artifact_path),
        "record_locator": maybe_text(row["record_locator"]),
        "generated_at_utc": maybe_text(row["generated_at_utc"]),
        "item": item_payload,
    }

def query_analysis_result_sets(
    run_dir: str | Path,
    *,
    result_set_id: str = "",
    run_id: str = "",
    round_id: str = "",
    analysis_kind: str = "",
    source_skill: str = "",
    artifact_path: str = "",
    latest_only: bool = False,
    include_contract: bool = False,
    include_items: bool = False,
    limit: int = 20,
    offset: int = 0,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, resolved_db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            matching_rows = _select_matching_result_set_rows(
                connection,
                result_set_id=result_set_id,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=analysis_kind,
                source_skill=source_skill,
                artifact_path=artifact_path,
                latest_only=latest_only,
            )
            paged_rows, safe_offset, safe_limit = _paged_rows(
                matching_rows,
                limit=limit,
                offset=offset,
            )
            result_sets = [
                _serialize_result_set_row(
                    connection,
                    row=row,
                    include_contract=include_contract,
                    include_items=include_items,
                )
                for row in paged_rows
            ]
    finally:
        connection.close()

    filters = {
        "result_set_id": maybe_text(result_set_id),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "analysis_kind": maybe_text(analysis_kind),
        "source_skill": maybe_text(source_skill),
        "artifact_path": maybe_text(artifact_path),
        "latest_only": bool(latest_only),
    }
    requested_analysis_kind_governance = (
        analysis_kind_governance(maybe_text(analysis_kind))
        if maybe_text(analysis_kind)
        else {}
    )
    warnings: list[dict[str, str]] = []
    if maybe_text(result_set_id) and not matching_rows:
        warnings.append(
            {
                "code": "missing-result-set",
                "message": f"No analysis result set matched {maybe_text(result_set_id)}.",
            }
        )
    return {
        "schema_version": "analysis-plane-result-set-query-v1",
        "status": "completed",
        "summary": {
            "db_path": str(resolved_db_file),
            "matching_result_set_count": len(matching_rows),
            "returned_result_set_count": len(result_sets),
            "analysis_kind_count": len(
                {
                    maybe_text(row["analysis_kind"])
                    for row in matching_rows
                    if maybe_text(row["analysis_kind"])
                }
            ),
        },
        "filters": filters,
        "analysis_kind_governance": requested_analysis_kind_governance,
        "paging": {
            "offset": safe_offset,
            "limit": safe_limit,
            "returned_count": len(result_sets),
            "matching_count": len(matching_rows),
        },
        "warnings": warnings,
        "result_sets": result_sets,
    }

def query_analysis_result_items(
    run_dir: str | Path,
    *,
    result_set_id: str = "",
    run_id: str = "",
    round_id: str = "",
    analysis_kind: str = "",
    source_skill: str = "",
    artifact_path: str = "",
    subject_id: str = "",
    readiness: str = "",
    latest_only: bool = False,
    include_result_sets: bool = False,
    include_contract: bool = False,
    limit: int = 20,
    offset: int = 0,
    db_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    connection, resolved_db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            result_set_rows = _select_matching_result_set_rows(
                connection,
                result_set_id=result_set_id,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=analysis_kind,
                source_skill=source_skill,
                artifact_path=artifact_path,
                latest_only=latest_only,
            )
            selected_result_set_ids = [
                maybe_text(row["result_set_id"])
                for row in result_set_rows
                if maybe_text(row["result_set_id"])
            ]
            matching_rows = _select_matching_result_item_rows(
                connection,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=analysis_kind,
                source_skill=source_skill,
                subject_id=subject_id,
                readiness=readiness,
                result_set_ids=selected_result_set_ids if result_set_rows else [],
            )
            paged_rows, safe_offset, safe_limit = _paged_rows(
                matching_rows,
                limit=limit,
                offset=offset,
            )
            items = [_serialize_result_item_row(row) for row in paged_rows]
            result_sets: list[dict[str, Any]] = []
            if include_result_sets:
                result_sets = [
                    _serialize_result_set_row(
                        connection,
                        row=row,
                        include_contract=include_contract,
                        include_items=False,
                    )
                    for row in result_set_rows
                ]
    finally:
        connection.close()

    filters = {
        "result_set_id": maybe_text(result_set_id),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "analysis_kind": maybe_text(analysis_kind),
        "source_skill": maybe_text(source_skill),
        "artifact_path": maybe_text(artifact_path),
        "subject_id": maybe_text(subject_id),
        "readiness": maybe_text(readiness),
        "latest_only": bool(latest_only),
    }
    requested_analysis_kind_governance = (
        analysis_kind_governance(maybe_text(analysis_kind))
        if maybe_text(analysis_kind)
        else {}
    )
    warnings: list[dict[str, str]] = []
    if maybe_text(result_set_id) and not result_set_rows:
        warnings.append(
            {
                "code": "missing-result-set",
                "message": f"No analysis result set matched {maybe_text(result_set_id)}.",
            }
        )
    return {
        "schema_version": "analysis-plane-item-query-v1",
        "status": "completed",
        "summary": {
            "db_path": str(resolved_db_file),
            "matching_result_set_count": len(result_set_rows),
            "matching_item_count": len(matching_rows),
            "returned_item_count": len(items),
        },
        "filters": filters,
        "analysis_kind_governance": requested_analysis_kind_governance,
        "paging": {
            "offset": safe_offset,
            "limit": safe_limit,
            "returned_count": len(items),
            "matching_count": len(matching_rows),
        },
        "warnings": warnings,
        "result_sets": result_sets,
        "items": items,
    }

def query_spatiotemporal_relation_cues(
    run_dir: str | Path,
    *,
    result_set_id: str = "",
    run_id: str = "",
    round_id: str = "",
    relation_id: str = "",
    relation_type: str = "",
    relation_status: str = "",
    source_signal_id: str = "",
    target_signal_id: str = "",
    source_role: str = "",
    target_role: str = "",
    latest_only: bool = False,
    include_result_sets: bool = False,
    include_contract: bool = False,
    limit: int = 20,
    offset: int = 0,
    db_path: str = "",
) -> dict[str, Any]:
    base = query_analysis_result_items(
        run_dir,
        result_set_id=result_set_id,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE,
        subject_id=relation_id,
        latest_only=latest_only and not maybe_text(relation_id),
        include_result_sets=include_result_sets,
        include_contract=include_contract,
        limit=0,
        offset=0,
        db_path=db_path,
    )

    def matches(row: dict[str, Any]) -> bool:
        item = row.get("item") if isinstance(row.get("item"), dict) else {}
        filters = {
            "relation_id": relation_id,
            "relation_type": relation_type,
            "relation_status": relation_status,
            "source_signal_id": source_signal_id,
            "target_signal_id": target_signal_id,
            "source_role": source_role,
            "target_role": target_role,
        }
        for field_name, expected in filters.items():
            expected_text = maybe_text(expected)
            if expected_text and maybe_text(item.get(field_name)) != expected_text:
                return False
        return True

    matching_items = [row for row in base.get("items", []) if matches(row)]
    paged_items, safe_offset, safe_limit = _paged_rows(
        matching_items,
        limit=limit,
        offset=offset,
    )
    artifact_refs: list[Any] = []
    for row in paged_items:
        refs = row.get("evidence_refs")
        if isinstance(refs, list):
            artifact_refs.extend(refs)
    filters = {
        "result_set_id": maybe_text(result_set_id),
        "run_id": maybe_text(run_id),
        "round_id": maybe_text(round_id),
        "analysis_kind": ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE,
        "relation_id": maybe_text(relation_id),
        "relation_type": maybe_text(relation_type),
        "relation_status": maybe_text(relation_status),
        "source_signal_id": maybe_text(source_signal_id),
        "target_signal_id": maybe_text(target_signal_id),
        "source_role": maybe_text(source_role),
        "target_role": maybe_text(target_role),
        "latest_only": bool(latest_only),
    }
    return {
        "schema_version": "spatiotemporal-relation-query-v1",
        "status": "completed",
        "summary": {
            "db_path": maybe_text(base.get("summary", {}).get("db_path"))
            if isinstance(base.get("summary"), dict)
            else "",
            "matching_result_set_count": int(
                base.get("summary", {}).get("matching_result_set_count") or 0
            )
            if isinstance(base.get("summary"), dict)
            else 0,
            "matching_relation_count": len(matching_items),
            "returned_relation_count": len(paged_items),
        },
        "filters": filters,
        "analysis_kind_governance": analysis_kind_governance(
            ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE
        ),
        "paging": {
            "offset": safe_offset,
            "limit": safe_limit,
            "returned_count": len(paged_items),
            "matching_count": len(matching_items),
        },
        "warnings": []
        if paged_items
        else [
            {
                "code": "no-relation-cues",
                "message": "No spatiotemporal relation cues matched the supplied filters.",
            }
        ],
        "result_sets": base.get("result_sets", []),
        "items": paged_items,
        "relations": [
            row.get("item") if isinstance(row.get("item"), dict) else {}
            for row in paged_items
        ],
        "artifact_refs": unique_artifact_refs(artifact_refs),
    }


__all__ = [
    "_paged_rows",
    "_select_matching_result_set_rows",
    "_select_matching_result_item_rows",
    "_serialize_result_set_row",
    "_serialize_result_item_row",
    "query_analysis_result_sets",
    "query_analysis_result_items",
    "query_spatiotemporal_relation_cues",
]

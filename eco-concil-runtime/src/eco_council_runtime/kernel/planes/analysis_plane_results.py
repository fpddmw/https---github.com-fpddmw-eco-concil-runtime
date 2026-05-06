from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.canonical_contracts import validate_canonical_payload
from eco_council_runtime.kernel.planes.analysis_plane_contracts import (
    analysis_config,
    analysis_kind_governance,
    decode_json,
    json_text,
    maybe_number,
    maybe_text,
    stable_hash,
    unique_texts,
    utc_now_iso,
)
from eco_council_runtime.kernel.planes.analysis_plane_support import (
    _resolve_analysis_artifact_path,
    build_result_contract,
    empty_result_contract,
    load_json_if_exists,
    load_result_contract,
    planned_item_rows,
)
from eco_council_runtime.kernel.planes.analysis_plane_schema import connect_db, resolve_db_path, resolve_run_dir

def _extract_items_from_payload(
    payload: dict[str, Any] | None,
    *,
    items_key: str,
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    values = payload.get(items_key)
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]

def _select_latest_result_set(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    round_id: str,
    analysis_kind: str,
    artifact_path: str = "",
    allow_any_artifact: bool = True,
) -> sqlite3.Row | None:
    artifact_text = maybe_text(artifact_path)
    if artifact_text:
        row = connection.execute(
            """
            SELECT *
            FROM analysis_result_sets
            WHERE run_id = ?
              AND round_id = ?
              AND analysis_kind = ?
              AND artifact_path = ?
            ORDER BY generated_at_utc DESC, result_set_id DESC
            LIMIT 1
            """,
            (run_id, round_id, analysis_kind, artifact_text),
        ).fetchone()
        if row is not None or not allow_any_artifact:
            return row
    return connection.execute(
        """
        SELECT *
        FROM analysis_result_sets
        WHERE run_id = ?
          AND round_id = ?
          AND analysis_kind = ?
        ORDER BY generated_at_utc DESC, result_set_id DESC
        LIMIT 1
        """,
        (run_id, round_id, analysis_kind),
    ).fetchone()

def _load_result_items(
    connection: sqlite3.Connection,
    *,
    result_set_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT item_json
        FROM analysis_result_items
        WHERE result_set_id = ?
        ORDER BY item_index, item_id
        """,
        (result_set_id,),
    ).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        payload_text = row["item_json"] if isinstance(row["item_json"], str) else ""
        payload = decode_json(payload_text, {})
        if isinstance(payload, dict):
            results.append(payload)
    return results

def _load_result_wrapper(
    connection: sqlite3.Connection,
    *,
    result_set_row: sqlite3.Row,
    items_key: str,
    count_key: str,
) -> dict[str, Any]:
    raw_text = result_set_row["raw_json"] if isinstance(result_set_row["raw_json"], str) else ""
    wrapper = decode_json(raw_text, {})
    if not isinstance(wrapper, dict):
        wrapper = {}
    items = _load_result_items(
        connection,
        result_set_id=maybe_text(result_set_row["result_set_id"]),
    )
    wrapper[items_key] = items
    wrapper[count_key] = len(items)
    return wrapper

def sync_analysis_result_set(
    run_dir: str | Path,
    *,
    analysis_kind: str,
    expected_run_id: str = "",
    round_id: str = "",
    artifact_path: str | Path = "",
    db_path: str = "",
    replace_scope: str = "round-kind",
) -> dict[str, Any]:
    config = analysis_config(analysis_kind)
    run_dir_path = resolve_run_dir(run_dir)
    analysis_file = _resolve_analysis_artifact_path(
        run_dir_path,
        analysis_kind=analysis_kind,
        artifact_path=artifact_path,
        round_id=round_id,
    )
    db_file = resolve_db_path(run_dir_path, db_path)
    payload = load_json_if_exists(analysis_file)
    artifact_label = maybe_text(config.get("artifact_label")) or analysis_kind
    items_key = maybe_text(config.get("items_key")) or "items"
    count_key = maybe_text(config.get("count_key")) or "item_count"
    id_field = maybe_text(config.get("id_field")) or "id"
    subject_field = maybe_text(config.get("subject_field"))
    score_field = maybe_text(config.get("score_field"))
    state_field = maybe_text(config.get("state_field"))
    related_id_fields = (
        config.get("related_id_fields")
        if isinstance(config.get("related_id_fields"), list)
        else []
    )
    summary_fields = (
        config.get("summary_fields")
        if isinstance(config.get("summary_fields"), list)
        else []
    )
    default_source_skill = maybe_text(config.get("default_source_skill"))
    if not isinstance(payload, dict):
        return {
            "status": f"missing-{artifact_label}",
            "analysis_kind": analysis_kind,
            "analysis_kind_governance": analysis_kind_governance(analysis_kind),
            "run_id": maybe_text(expected_run_id),
            "round_id": maybe_text(round_id),
            "artifact_path": str(analysis_file),
            "db_path": str(db_file),
            "result_set_id": "",
            "item_count": 0,
            "source_skill": default_source_skill,
            **empty_result_contract(),
        }

    payload_run_id = maybe_text(payload.get("run_id")) or maybe_text(expected_run_id)
    payload_round_id = maybe_text(payload.get("round_id")) or maybe_text(round_id)
    source_skill = maybe_text(payload.get("skill")) or default_source_skill
    generated_at_utc = maybe_text(payload.get("generated_at_utc")) or utc_now_iso()
    items = _extract_items_from_payload(payload, items_key=items_key)
    canonical_object_kind = maybe_text(config.get("canonical_object_kind"))
    if canonical_object_kind:
        normalized_items: list[dict[str, Any]] = []
        for item in items:
            normalized_items.append(
                validate_canonical_payload(canonical_object_kind, item)
            )
        items = normalized_items
        payload = dict(payload)
        payload[items_key] = items
        payload[count_key] = len(items)
    result_set_id = "analysis-set-" + stable_hash(
        analysis_kind,
        payload_run_id,
        payload_round_id,
        str(analysis_file),
    )[:16]
    planned_rows = planned_item_rows(
        items,
        id_field=id_field,
        result_set_id=result_set_id,
    )

    connection, resolved_db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            if maybe_text(replace_scope) == "artifact":
                connection.execute(
                    "DELETE FROM analysis_result_lineage WHERE result_set_id = ?",
                    (result_set_id,),
                )
                connection.execute(
                    "DELETE FROM analysis_result_items WHERE result_set_id = ?",
                    (result_set_id,),
                )
                connection.execute(
                    "DELETE FROM analysis_result_sets WHERE result_set_id = ?",
                    (result_set_id,),
                )
            else:
                connection.execute(
                    """
                    DELETE FROM analysis_result_lineage
                    WHERE run_id = ? AND round_id = ? AND analysis_kind = ?
                    """,
                    (payload_run_id, payload_round_id, analysis_kind),
                )
                connection.execute(
                    """
                    DELETE FROM analysis_result_items
                    WHERE run_id = ? AND round_id = ? AND analysis_kind = ?
                    """,
                    (payload_run_id, payload_round_id, analysis_kind),
                )
                connection.execute(
                    """
                    DELETE FROM analysis_result_sets
                    WHERE run_id = ? AND round_id = ? AND analysis_kind = ?
                    """,
                    (payload_run_id, payload_round_id, analysis_kind),
                )
            result_contract, lineage_entries = build_result_contract(
                payload,
                config=config,
                run_id=payload_run_id,
                round_id=payload_round_id,
                result_set_id=result_set_id,
                planned_rows=planned_rows,
                connection=connection,
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO analysis_result_sets (
                    result_set_id,
                    run_id,
                    round_id,
                    analysis_kind,
                    source_skill,
                    artifact_path,
                    record_locator,
                    generated_at_utc,
                    item_count,
                    summary_json,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_set_id,
                    payload_run_id,
                    payload_round_id,
                    analysis_kind,
                    source_skill,
                    str(analysis_file),
                    f"$.{items_key}",
                    generated_at_utc,
                    len(items),
                    json_text(
                        {
                            count_key: len(items),
                            **{
                                field: maybe_text(payload.get(field))
                                for field in summary_fields
                                if maybe_text(payload.get(field))
                            },
                        }
                    ),
                    json_text(payload),
                ),
            )
            for planned in planned_rows:
                item = planned["item"] if isinstance(planned.get("item"), dict) else {}
                index = int(planned.get("index") or 0)
                item_id_value = maybe_text(planned.get("item_id_value")) or str(index)
                item_id = maybe_text(planned.get("item_id"))
                connection.execute(
                    """
                    INSERT OR REPLACE INTO analysis_result_items (
                        item_id,
                        result_set_id,
                        run_id,
                        round_id,
                        analysis_kind,
                        source_skill,
                        item_index,
                        subject_id,
                        readiness,
                        decision_source,
                        score,
                        related_ids_json,
                        evidence_refs_json,
                        lineage_json,
                        provenance_json,
                        item_json,
                        artifact_path,
                        record_locator,
                        generated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item_id,
                        result_set_id,
                        payload_run_id,
                        payload_round_id,
                        analysis_kind,
                        source_skill,
                        index,
                        maybe_text(item.get(subject_field)) or item_id_value,
                        maybe_text(item.get(state_field)),
                        maybe_text(item.get("decision_source")),
                        maybe_number(item.get(score_field)) if score_field else None,
                        json_text(
                            unique_texts(
                                [item.get(field) for field in related_id_fields]
                            )
                        ),
                        json_text(
                            item.get("evidence_refs", [])
                            if isinstance(item.get("evidence_refs"), list)
                            else []
                        ),
                        json_text(
                            item.get("lineage", [])
                            if isinstance(item.get("lineage"), list)
                            else []
                        ),
                        json_text(
                            item.get("provenance", {})
                            if isinstance(item.get("provenance"), dict)
                            else {}
                        ),
                        json_text(item),
                        str(analysis_file),
                        f"$.{items_key}[{index - 1}]",
                        generated_at_utc,
                    ),
                )
            for entry in lineage_entries:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO analysis_result_lineage (
                        lineage_id,
                        run_id,
                        round_id,
                        analysis_kind,
                        result_set_id,
                        item_id,
                        lineage_scope,
                        lineage_type,
                        relation,
                        value_text,
                        artifact_path,
                        record_locator,
                        source_analysis_kind,
                        metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        maybe_text(entry.get("lineage_id")),
                        payload_run_id,
                        payload_round_id,
                        analysis_kind,
                        result_set_id,
                        maybe_text(entry.get("item_id")),
                        maybe_text(entry.get("lineage_scope")),
                        maybe_text(entry.get("lineage_type")),
                        maybe_text(entry.get("relation")),
                        maybe_text(entry.get("value_text")),
                        maybe_text(entry.get("artifact_path")),
                        maybe_text(entry.get("record_locator")),
                        maybe_text(entry.get("source_analysis_kind")),
                        json_text(
                            entry.get("metadata")
                            if isinstance(entry.get("metadata"), dict)
                            else {}
                        ),
                    ),
                )
    finally:
        connection.close()

    return {
        "status": "completed",
        "analysis_kind": analysis_kind,
        "analysis_kind_governance": analysis_kind_governance(analysis_kind),
        "run_id": payload_run_id,
        "round_id": payload_round_id,
        "artifact_path": str(analysis_file),
        "db_path": str(resolved_db_file),
        "result_set_id": result_set_id,
        "item_count": len(items),
        "source_skill": source_skill,
        **result_contract,
    }

def load_analysis_result_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    analysis_kind: str,
    artifact_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    config = analysis_config(analysis_kind)
    run_dir_path = resolve_run_dir(run_dir)
    analysis_file = _resolve_analysis_artifact_path(
        run_dir_path,
        analysis_kind=analysis_kind,
        artifact_path=artifact_path,
        round_id=round_id,
    )
    artifact_label = maybe_text(config.get("artifact_label")) or analysis_kind
    items_key = maybe_text(config.get("items_key")) or "items"
    count_key = maybe_text(config.get("count_key")) or "item_count"
    default_source_skill = maybe_text(config.get("default_source_skill"))
    artifact_override_requested = bool(maybe_text(artifact_path))
    artifact_payload = load_json_if_exists(analysis_file)
    artifact_present = isinstance(artifact_payload, dict)

    connection, resolved_db_file = connect_db(run_dir_path, db_path)
    try:
        with connection:
            existing_row = _select_latest_result_set(
                connection,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=analysis_kind,
                artifact_path=str(analysis_file),
                allow_any_artifact=not artifact_override_requested,
            )
    finally:
        connection.close()

    artifact_generated_at = (
        maybe_text(artifact_payload.get("generated_at_utc"))
        if isinstance(artifact_payload, dict)
        else ""
    )
    existing_generated_at = (
        maybe_text(existing_row["generated_at_utc"]) if existing_row is not None else ""
    )
    existing_artifact_path = (
        maybe_text(existing_row["artifact_path"]) if existing_row is not None else ""
    )

    analysis_sync: dict[str, Any] = {}
    should_sync = artifact_present and (
        existing_row is None
        or existing_artifact_path != str(analysis_file)
        or (
            artifact_generated_at
            and existing_generated_at
            and artifact_generated_at != existing_generated_at
        )
    )
    if should_sync:
        analysis_sync = sync_analysis_result_set(
            run_dir_path,
            analysis_kind=analysis_kind,
            expected_run_id=run_id,
            round_id=round_id,
            artifact_path=analysis_file,
            db_path=str(resolved_db_file),
        )

    connection, resolved_db_file = connect_db(run_dir_path, str(resolved_db_file))
    try:
        with connection:
            result_set_row = _select_latest_result_set(
                connection,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=analysis_kind,
                artifact_path=str(analysis_file),
                allow_any_artifact=not artifact_override_requested,
            )
            if result_set_row is not None:
                wrapper = _load_result_wrapper(
                    connection,
                    result_set_row=result_set_row,
                    items_key=items_key,
                    count_key=count_key,
                )
                items = _extract_items_from_payload(wrapper, items_key=items_key)
                result_contract = load_result_contract(
                    connection,
                    result_set_id=maybe_text(result_set_row["result_set_id"]),
                )
                if not analysis_sync:
                    analysis_sync = {
                        "status": "existing-result-set",
                        "analysis_kind": analysis_kind,
                        "run_id": run_id,
                        "round_id": round_id,
                        "artifact_path": maybe_text(result_set_row["artifact_path"]),
                        "db_path": str(resolved_db_file),
                        "result_set_id": maybe_text(result_set_row["result_set_id"]),
                        "item_count": len(items),
                        "source_skill": maybe_text(result_set_row["source_skill"]),
                    }
                analysis_sync = {**analysis_sync, **result_contract}
                return {
                    "payload_wrapper": wrapper,
                    "items": items,
                    "item_count": len(items),
                    "source": "analysis-plane",
                    "artifact_path": maybe_text(result_set_row["artifact_path"])
                    or str(analysis_file),
                    "db_path": str(resolved_db_file),
                    "analysis_sync": analysis_sync,
                    "result_contract": result_contract,
                    "artifact_present": artifact_present,
                    "warnings": [],
                }
    finally:
        connection.close()

    if artifact_present:
        items = _extract_items_from_payload(artifact_payload, items_key=items_key)
        artifact_result_contract, _ = build_result_contract(
            artifact_payload,
            config=config,
            run_id=run_id,
            round_id=round_id,
            result_set_id="",
            planned_rows=planned_item_rows(
                items,
                id_field=maybe_text(config.get("id_field")) or "id",
                result_set_id="",
            ),
            connection=None,
        )
        if not analysis_sync:
            analysis_sync = {
                "status": "artifact-only",
                "analysis_kind": analysis_kind,
                "run_id": run_id,
                "round_id": round_id,
                "artifact_path": str(analysis_file),
                "db_path": str(resolved_db_file),
                "result_set_id": "",
                "item_count": len(items),
                "source_skill": maybe_text(artifact_payload.get("skill"))
                or default_source_skill,
            }
        analysis_sync = {**analysis_sync, **artifact_result_contract}
        return {
            "payload_wrapper": artifact_payload,
            "items": items,
            "item_count": len(items),
            "source": f"{artifact_label}-artifact",
            "artifact_path": str(analysis_file),
            "db_path": str(resolved_db_file),
            "analysis_sync": analysis_sync,
            "result_contract": artifact_result_contract,
            "artifact_present": True,
            "warnings": [],
        }

    warnings = [
        {
            "code": f"missing-{artifact_label}",
            "message": f"No {artifact_label} result was found for round {round_id} at {analysis_file}.",
        }
    ]
    if not analysis_sync:
        analysis_sync = {
            "status": f"missing-{artifact_label}",
            "analysis_kind": analysis_kind,
            "run_id": run_id,
            "round_id": round_id,
            "artifact_path": str(analysis_file),
            "db_path": str(resolved_db_file),
            "result_set_id": "",
            "item_count": 0,
            "source_skill": default_source_skill,
        }
    analysis_sync = {**analysis_sync, **empty_result_contract()}
    return {
        "payload_wrapper": {items_key: [], count_key: 0},
        "items": [],
        "item_count": 0,
        "source": f"missing-{artifact_label}",
        "artifact_path": str(analysis_file),
        "db_path": str(resolved_db_file),
        "analysis_sync": analysis_sync,
        "result_contract": empty_result_contract(),
        "artifact_present": False,
        "warnings": warnings,
    }

__all__ = [
    "_extract_items_from_payload",
    "_select_latest_result_set",
    "_load_result_items",
    "_load_result_wrapper",
    "sync_analysis_result_set",
    "load_analysis_result_context",
]

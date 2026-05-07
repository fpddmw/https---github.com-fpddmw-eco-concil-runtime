from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from .schema import connect_db
from .payloads import (
    OBJECT_KIND_DECISION_TRACE,
    normalized_evidence_refs,
    normalized_lineage,
    normalized_provenance,
    unique_texts,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (
    json_text,
    maybe_text,
    stable_hash,
    utc_now_iso,
)


def decision_trace_id(
    run_id: str,
    round_id: str,
    decision_id: str,
    trace_index: int,
) -> str:
    return "decision-trace-" + stable_hash(
        "decision-trace",
        run_id,
        round_id,
        decision_id,
        trace_index,
    )[:12]


def normalized_decision_trace_payload(
    trace: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    trace_index: int,
) -> dict[str, Any]:
    normalized = dict(trace)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "council-trace"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["decision_id"] = maybe_text(normalized.get("decision_id"))
    normalized["decision_kind"] = (
        maybe_text(normalized.get("decision_kind")) or "round-decision"
    )
    normalized["status"] = maybe_text(normalized.get("status")) or "recorded"
    normalized["selected_object_kind"] = maybe_text(
        normalized.get("selected_object_kind")
    )
    normalized["selected_object_id"] = maybe_text(
        normalized.get("selected_object_id")
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    normalized["decision_source"] = decision_source
    normalized["accepted_object_ids"] = unique_texts(
        normalized.get("accepted_object_ids", [])
        if isinstance(normalized.get("accepted_object_ids"), list)
        else []
    )
    normalized["rejected_object_ids"] = unique_texts(
        normalized.get("rejected_object_ids", [])
        if isinstance(normalized.get("rejected_object_ids"), list)
        else []
    )
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_lineage(normalized.get("lineage"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["trace_id"] = (
        maybe_text(normalized.get("trace_id"))
        or decision_trace_id(
            normalized_run_id,
            normalized_round_id,
            maybe_text(normalized.get("decision_id")),
            trace_index,
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_DECISION_TRACE
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_DECISION_TRACE, normalized)


def decision_trace_row_from_payload(
    trace: dict[str, Any],
    *,
    artifact_path: str,
    record_locator: str,
) -> dict[str, Any]:
    return {
        "trace_id": maybe_text(trace.get("trace_id")),
        "decision_id": maybe_text(trace.get("decision_id")),
        "run_id": maybe_text(trace.get("run_id")),
        "round_id": maybe_text(trace.get("round_id")),
        "generated_at_utc": maybe_text(trace.get("generated_at_utc")),
        "decision_kind": maybe_text(trace.get("decision_kind")),
        "status": maybe_text(trace.get("status")),
        "selected_object_kind": maybe_text(trace.get("selected_object_kind")),
        "selected_object_id": maybe_text(trace.get("selected_object_id")),
        "confidence": trace.get("confidence"),
        "rationale": maybe_text(trace.get("rationale")),
        "decision_source": maybe_text(trace.get("decision_source")),
        "accepted_object_ids_json": json_text(
            trace.get("accepted_object_ids", [])
        ),
        "rejected_object_ids_json": json_text(
            trace.get("rejected_object_ids", [])
        ),
        "evidence_refs_json": json_text(trace.get("evidence_refs", [])),
        "provenance_json": json_text(trace.get("provenance", {})),
        "lineage_json": json_text(trace.get("lineage", [])),
        "artifact_path": maybe_text(artifact_path),
        "record_locator": maybe_text(record_locator),
        "raw_json": json_text(trace),
    }


def write_decision_trace_row(
    connection: sqlite3.Connection,
    row: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO decision_traces (
            trace_id, decision_id, run_id, round_id, generated_at_utc,
            decision_kind, status, selected_object_kind, selected_object_id,
            confidence, rationale, decision_source, accepted_object_ids_json,
            rejected_object_ids_json, evidence_refs_json, provenance_json,
            lineage_json, artifact_path, record_locator, raw_json
        ) VALUES (
            :trace_id, :decision_id, :run_id, :round_id, :generated_at_utc,
            :decision_kind, :status, :selected_object_kind, :selected_object_id,
            :confidence, :rationale, :decision_source, :accepted_object_ids_json,
            :rejected_object_ids_json, :evidence_refs_json, :provenance_json,
            :lineage_json, :artifact_path, :record_locator, :raw_json
        )
        """,
        row,
    )


def store_decision_trace_records(
    run_dir: str | Path,
    *,
    trace_bundle: dict[str, Any],
    artifact_path: str = "",
    db_path: str = "",
) -> dict[str, Any]:
    bundle = dict(trace_bundle) if isinstance(trace_bundle, dict) else {}
    traces = bundle.get("traces", []) if isinstance(bundle.get("traces"), list) else []
    run_id = maybe_text(bundle.get("run_id"))
    round_id = maybe_text(bundle.get("round_id"))
    normalized_traces = [
        normalized_decision_trace_payload(
            trace,
            run_id=run_id,
            round_id=round_id,
            trace_index=index,
        )
        for index, trace in enumerate(traces)
        if isinstance(trace, dict)
    ]
    if normalized_traces:
        run_id = maybe_text(run_id) or maybe_text(normalized_traces[0].get("run_id"))
        round_id = maybe_text(round_id) or maybe_text(normalized_traces[0].get("round_id"))
    bundle["schema_version"] = "decision-trace-bundle-v1"
    bundle["run_id"] = run_id
    bundle["round_id"] = round_id
    bundle["generated_at_utc"] = (
        maybe_text(bundle.get("generated_at_utc"))
        or maybe_text(normalized_traces[-1].get("generated_at_utc")) if normalized_traces else utc_now_iso()
    )
    bundle["traces"] = normalized_traces
    bundle["trace_count"] = len(normalized_traces)
    connection, _db_file = connect_db(run_dir, db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM decision_traces WHERE run_id = ? AND round_id = ?",
                (run_id, round_id),
            )
            for index, trace in enumerate(normalized_traces):
                write_decision_trace_row(
                    connection,
                    decision_trace_row_from_payload(
                        trace,
                        artifact_path=artifact_path,
                        record_locator=f"$.traces[{index}]",
                    ),
                )
    finally:
        connection.close()
    return bundle


__all__ = (
    "decision_trace_id",
    "normalized_decision_trace_payload",
    "decision_trace_row_from_payload",
    "write_decision_trace_row",
    "store_decision_trace_records",
)

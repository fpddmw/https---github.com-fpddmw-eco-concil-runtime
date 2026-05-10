from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists
from eco_council_runtime.kernel.core.paths import cursor_path, manifest_path
from eco_council_runtime.kernel.execution.executor import maybe_text
from eco_council_runtime.kernel.governance.round_liveness import build_round_liveness_surface
from eco_council_runtime.kernel.governance.skill_approvals import (
    REQUEST_STATUS_APPROVED as SKILL_REQUEST_STATUS_APPROVED,
    REQUEST_STATUS_PENDING as SKILL_REQUEST_STATUS_PENDING,
)
from eco_council_runtime.kernel.operator.run_state_view import transition_request_state
from eco_council_runtime.kernel.source_queue.source_queue_contract import (
    SOURCE_CATALOG,
    source_capability_hints,
)
from eco_council_runtime.kernel.source_queue.source_queue_execution import render_fetch_argv
from eco_council_runtime.objects.council import query_council_objects
from eco_council_runtime.runtime_command_hints import kernel_command, run_skill_command


__all__ = (
    "show_archive_status_surface",
    "show_council_status_surface",
    "show_open_challenges_surface",
    "show_source_acquisition_intents_surface",
    "show_source_surfaces_surface",
    "show_unbundled_findings_surface",
)


def _resolved_ids(run_dir: Path, run_id: str = "", round_id: str = "") -> tuple[str, str]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    cursor = load_json_if_exists(cursor_path(run_dir)) or {}
    resolved_run_id = maybe_text(run_id) or maybe_text(manifest.get("run_id")) or maybe_text(cursor.get("run_id"))
    resolved_round_id = maybe_text(round_id) or maybe_text(cursor.get("current_round_id"))
    return resolved_run_id, resolved_round_id


def _safe_limit(limit: int) -> int:
    return max(1, min(200, int(limit or 20)))


def _table_present(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_count(db_path: Path, table_name: str) -> dict[str, Any]:
    if not db_path.exists():
        return {"present": False, "table": table_name, "count": 0}
    connection = sqlite3.connect(db_path)
    try:
        if not _table_present(connection, table_name):
            return {"present": True, "table": table_name, "count": 0, "table_present": False}
        row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return {
            "present": True,
            "table": table_name,
            "table_present": True,
            "count": int(row[0] or 0) if row else 0,
        }
    finally:
        connection.close()


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        maybe_text(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        if len(row) > 1 and maybe_text(row[1])
    }


def _count_filtered_rows(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    columns: set[str],
    filters: dict[str, str],
) -> int:
    clauses: list[str] = []
    params: list[str] = []
    for column_name, value in filters.items():
        if column_name not in columns or not maybe_text(value):
            continue
        clauses.append(f"{column_name} = ?")
        params.append(maybe_text(value))
    query = f"SELECT COUNT(*) FROM {table_name}"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    row = connection.execute(query, tuple(params)).fetchone()
    return int(row[0] or 0) if row else 0


def _group_filtered_counts(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    columns: set[str],
    group_column: str,
    filters: dict[str, str],
) -> dict[str, int]:
    if group_column not in columns:
        return {}
    clauses: list[str] = []
    params: list[str] = []
    for column_name, value in filters.items():
        if column_name not in columns or not maybe_text(value):
            continue
        clauses.append(f"{column_name} = ?")
        params.append(maybe_text(value))
    query = f"SELECT {group_column}, COUNT(*) FROM {table_name}"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += f" GROUP BY {group_column} ORDER BY {group_column}"
    return {
        maybe_text(row[0]) or "<empty>": int(row[1] or 0)
        for row in connection.execute(query, tuple(params)).fetchall()
    }


def _source_signal_plane_status(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    signal_db = (run_dir / "analytics" / "signal_plane.sqlite").resolve()
    table_name = "normalized_signals"
    if not signal_db.exists():
        return {
            "present": False,
            "path": str(signal_db),
            "table": table_name,
            "table_present": False,
            "run_normalized_signal_count": 0,
            "round_normalized_signal_count": 0,
            "checkpoint_input_status": "missing-signal-plane",
        }
    connection = sqlite3.connect(signal_db)
    try:
        if not _table_present(connection, table_name):
            return {
                "present": True,
                "path": str(signal_db),
                "table": table_name,
                "table_present": False,
                "run_normalized_signal_count": 0,
                "round_normalized_signal_count": 0,
                "checkpoint_input_status": "missing-normalized-signals-table",
            }
        columns = _table_columns(connection, table_name)
        run_filters = {"run_id": run_id}
        round_filters = {"run_id": run_id, "round_id": round_id}
        run_count = _count_filtered_rows(
            connection,
            table_name=table_name,
            columns=columns,
            filters=run_filters,
        )
        round_count = _count_filtered_rows(
            connection,
            table_name=table_name,
            columns=columns,
            filters=round_filters,
        )
        status = (
            "normalized-signals-present"
            if round_count > 0
            else "no-normalized-signals-for-round"
            if run_count > 0
            else "no-normalized-signals-for-run"
        )
        return {
            "present": True,
            "path": str(signal_db),
            "table": table_name,
            "table_present": True,
            "run_normalized_signal_count": run_count,
            "round_normalized_signal_count": round_count,
            "checkpoint_input_status": status,
            "plane_counts": _group_filtered_counts(
                connection,
                table_name=table_name,
                columns=columns,
                group_column="plane",
                filters=round_filters,
            ),
            "source_skill_counts": _group_filtered_counts(
                connection,
                table_name=table_name,
                columns=columns,
                group_column="source_skill",
                filters=round_filters,
            ),
        }
    finally:
        connection.close()


def _latest_snapshot(run_dir: Path, prefix: str, round_id: str = "") -> dict[str, Any]:
    archive_dir = run_dir / "archive"
    if not archive_dir.exists():
        return {"present": False, "path": ""}
    candidates = sorted(archive_dir.glob(f"{prefix}_*.json"))
    if round_id:
        round_candidates = [path for path in candidates if round_id in path.name]
        if round_candidates:
            candidates = round_candidates
    if not candidates:
        return {"present": False, "path": ""}
    path = candidates[-1]
    payload = load_json_if_exists(path) or {}
    return {
        "present": True,
        "path": str(path.resolve()),
        "payload": payload if isinstance(payload, dict) else {},
    }


def _history_reuse_status(retrieval_path: Path, retrieval: Any) -> dict[str, Any]:
    payload = retrieval if isinstance(retrieval, dict) else {}
    budget = payload.get("budget", {}) if isinstance(payload.get("budget"), dict) else {}
    history_query = (
        payload.get("history_query", {})
        if isinstance(payload.get("history_query"), dict)
        else {}
    )
    return {
        "present": bool(retrieval_path.exists()),
        "path": str(retrieval_path.resolve()),
        "selected_case_count": int(budget.get("selected_case_count") or 0),
        "selected_signal_count": int(budget.get("selected_signal_count") or 0),
        "history_query": {
            "profile_id": maybe_text(history_query.get("profile_id")),
            "region_label": maybe_text(history_query.get("region_label")),
            "query_text": maybe_text(history_query.get("query_text")),
        },
        "semantics": "History retrieval exposes prior evidence refs and excerpts only; it does not create current-run conclusions.",
    }


def _compact_receipt(payload: dict[str, Any], path: Path) -> dict[str, Any]:
    artifact_refs = payload.get("artifact_refs", []) if isinstance(payload.get("artifact_refs"), list) else []
    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    compact: dict[str, Any] = {
        "receipt_id": maybe_text(payload.get("receipt_id")),
        "skill_name": maybe_text(payload.get("skill_name")) or maybe_text(summary.get("skill")),
        "status": maybe_text(payload.get("status")),
        "artifact_ref_count": len(artifact_refs),
        "path": str(path.resolve()),
    }
    record_count = summary.get("record_count")
    if isinstance(record_count, int):
        compact["record_count"] = record_count
    return {key: value for key, value in compact.items() if value not in ("", [], {}, 0)}


def _source_receipt_status(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    receipts_dir = run_dir / "runtime" / "receipts"
    import_execution_path = run_dir / "runtime" / f"import_execution_{round_id}.json"
    import_execution = load_json_if_exists(import_execution_path) or {}
    if not isinstance(import_execution, dict):
        import_execution = {}
    receipts: list[dict[str, Any]] = []
    if receipts_dir.exists():
        for path in sorted(receipts_dir.glob("*.json")):
            payload = load_json_if_exists(path) or {}
            if not isinstance(payload, dict):
                continue
            if maybe_text(payload.get("run_id")) != run_id or maybe_text(payload.get("round_id")) != round_id:
                continue
            skill_name = maybe_text(payload.get("skill_name"))
            if not (skill_name.startswith("fetch-") or skill_name in SOURCE_CATALOG):
                continue
            receipts.append(_compact_receipt(payload, path))
    source_skill_counts: dict[str, int] = {}
    for receipt in receipts:
        skill_name = maybe_text(receipt.get("skill_name")) or "<empty>"
        source_skill_counts[skill_name] = source_skill_counts.get(skill_name, 0) + 1
    return {
        "receipts_dir": str(receipts_dir.resolve()),
        "fetch_receipt_count": len(receipts),
        "source_skill_counts": dict(sorted(source_skill_counts.items())),
        "sample_receipts": receipts[:20],
        "import_execution": {
            "present": bool(import_execution_path.exists()),
            "path": str(import_execution_path.resolve()),
            "normalization_status": maybe_text(import_execution.get("normalization_status")),
            "completed_count": int(import_execution.get("completed_count") or 0),
            "failed_count": int(import_execution.get("failed_count") or 0),
            "normalized_signal_step_count": int(import_execution.get("normalized_signal_step_count") or 0),
            "receipt_only_step_count": int(import_execution.get("receipt_only_step_count") or 0),
            "receipt_only_sources": (
                import_execution.get("receipt_only_sources", [])
                if isinstance(import_execution.get("receipt_only_sources"), list)
                else []
            ),
        },
        "semantics": "Fetch receipts are auditable evidence traces; they are not normalized signal rows until a normalizer writes canonical queryable records.",
    }


def show_archive_status_surface(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any]:
    resolved_run_id, resolved_round_id = _resolved_ids(run_dir, run_id, round_id)
    archive_root = (run_dir / ".." / "archives").resolve()
    case_db = archive_root / "eco_case_library.sqlite"
    signal_db = archive_root / "eco_signal_corpus.sqlite"
    case_snapshot = _latest_snapshot(run_dir, "case_library_import", resolved_round_id)
    signal_snapshot = _latest_snapshot(run_dir, "signal_corpus_import", resolved_round_id)
    history_snapshot = _latest_snapshot(run_dir, "case_library_query", resolved_round_id)
    signal_query_snapshot = _latest_snapshot(run_dir, "signal_corpus_query", resolved_round_id)
    retrieval_path = run_dir / "investigation" / f"history_retrieval_{resolved_round_id}.json"
    retrieval = load_json_if_exists(retrieval_path) if retrieval_path.exists() else {}
    source_signal_plane = _source_signal_plane_status(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
    )
    history_reuse = _history_reuse_status(retrieval_path, retrieval)
    source_receipts = _source_receipt_status(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
    )
    signal_payload = signal_snapshot.get("payload", {}) if isinstance(signal_snapshot.get("payload"), dict) else {}
    imported_signal_count = int(signal_payload.get("imported_signal_count") or 0)
    gap_hints = []
    if maybe_text(source_signal_plane.get("checkpoint_input_status")) != "normalized-signals-present":
        gap_hints.append(
            "No normalized signals are available for the selected round in the source signal plane."
        )
    if (
        int(source_receipts.get("fetch_receipt_count") or 0) > 0
        and maybe_text(source_signal_plane.get("checkpoint_input_status")) != "normalized-signals-present"
    ):
        gap_hints.append(
            "Fetch receipts exist for the selected round, but they are currently receipt-only evidence for archive/query purposes."
        )
    if signal_snapshot.get("present") and imported_signal_count == 0:
        gap_hints.append("No normalized signals were archived in the latest signal corpus checkpoint.")
    if not signal_snapshot.get("present"):
        gap_hints.append("No signal corpus import snapshot was found for the selected round.")
    if not case_snapshot.get("present"):
        gap_hints.append("No case library import snapshot was found for the selected round.")
    if not history_reuse.get("present"):
        gap_hints.append("No history retrieval artifact was found for the selected round.")
    return {
        "status": "completed",
        "surface": "archive-status",
        "run_id": resolved_run_id,
        "round_id": resolved_round_id,
        "archive_root": str(archive_root),
        "semantics": "Archive status is read-only checkpoint evidence; it does not create conclusions.",
        "checkpoint_inputs": {
            "source_signal_plane": source_signal_plane,
        },
        "checkpoint_summary": {
            "signal_corpus_checkpoint_status": maybe_text(signal_payload.get("checkpoint_status")),
            "imported_signal_count": imported_signal_count,
            "case_library_checkpoint_present": bool(case_snapshot.get("present")),
            "history_retrieval_present": bool(history_reuse.get("present")),
            "history_selected_case_count": int(history_reuse.get("selected_case_count") or 0),
            "history_selected_signal_count": int(history_reuse.get("selected_signal_count") or 0),
            "fetch_receipt_count": int(source_receipts.get("fetch_receipt_count") or 0),
            "receipt_evidence_status": (
                "normalized-signals-present"
                if maybe_text(source_signal_plane.get("checkpoint_input_status")) == "normalized-signals-present"
                else "receipt-only-evidence-present"
                if int(source_receipts.get("fetch_receipt_count") or 0) > 0
                else "no-fetch-receipts-observed"
            ),
        },
        "databases": {
            "case_library": {
                "path": str(case_db),
                "cases": _table_count(case_db, "cases"),
                "case_rounds": _table_count(case_db, "case_rounds"),
                "case_excerpts": _table_count(case_db, "case_excerpts"),
            },
            "signal_corpus": {
                "path": str(signal_db),
                "corpus_runs": _table_count(signal_db, "corpus_runs"),
                "corpus_signals": _table_count(signal_db, "corpus_signals"),
            },
        },
        "latest_snapshots": {
            "case_library_import": case_snapshot,
            "signal_corpus_import": signal_snapshot,
            "case_library_query": history_snapshot,
            "signal_corpus_query": signal_query_snapshot,
            "history_retrieval": {
                "present": bool(retrieval_path.exists()),
                "path": str(retrieval_path.resolve()),
                "payload": retrieval if isinstance(retrieval, dict) else {},
            },
        },
        "history_reuse": history_reuse,
        "source_receipts": source_receipts,
        "gap_hints": gap_hints,
        "commands": {
            "normalize_fetch_execution": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="normalize-fetch-execution",
                contract_mode="warn",
                actor_role="<source_owner_role>",
            )
            if resolved_run_id and resolved_round_id
            else "",
            "archive_signal_corpus_checkpoint": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="archive-signal-corpus",
                contract_mode="warn",
                actor_role="runtime-operator",
            )
            if resolved_run_id and resolved_round_id
            else "",
            "archive_case_library_checkpoint": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="archive-case-library",
                contract_mode="warn",
                actor_role="runtime-operator",
            )
            if resolved_run_id and resolved_round_id
            else "",
            "materialize_history_context": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="materialize-history-context",
                contract_mode="warn",
            )
            if resolved_run_id and resolved_round_id
            else "",
            "query_signal_corpus": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="query-signal-corpus",
                contract_mode="warn",
                skill_args=[
                    "--query-text",
                    "<agent_defined_history_query>",
                    "--exclude-run-id",
                    resolved_run_id,
                ],
            )
            if resolved_run_id and resolved_round_id
            else "",
            "query_case_library": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="query-case-library",
                contract_mode="warn",
                skill_args=[
                    "--query-text",
                    "<agent_defined_history_query>",
                    "--exclude-case-id",
                    resolved_run_id,
                ],
            )
            if resolved_run_id and resolved_round_id
            else "",
        },
    }


def _status_object_query_command(run_dir: Path, run_id: str, round_id: str, object_kind: str, **filters: str) -> str:
    args: list[str] = [
        "--run-dir",
        str(run_dir),
        "--object-kind",
        object_kind,
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    ]
    for key, value in filters.items():
        if maybe_text(value):
            args.extend([f"--{key.replace('_', '-')}", maybe_text(value)])
    return kernel_command("query-council-objects", *args)


def _source_intent_execution_surface(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    intent: dict[str, Any],
) -> dict[str, Any]:
    source_skill = maybe_text(intent.get("source_skill"))
    author_role = maybe_text(intent.get("author_role")) or "<agent_role>"
    capability_hints = source_capability_hints(source_skill)
    templates = (
        capability_hints.get("fetch_argument_templates", [])
        if isinstance(capability_hints.get("fetch_argument_templates"), list)
        else []
    )
    fetch_commands = [
        run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=source_skill,
            contract_mode="warn",
            actor_role=author_role,
            timeout_seconds=900.0,
            retry_budget=1,
            allow_side_effects=["network-external"],
            skill_args=[maybe_text(arg) for arg in template if maybe_text(arg)],
        )
        for template in templates
        if isinstance(template, list) and source_skill
    ]
    return {
        "source_skill": source_skill,
        "author_role": author_role,
        "provider_modes": capability_hints.get("provider_modes", [])
        if isinstance(capability_hints.get("provider_modes"), list)
        else [],
        "fetch_argument_templates": templates,
        "fetch_command_templates": fetch_commands,
        "normalize_fetch_execution_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="normalize-fetch-execution",
            contract_mode="warn",
            actor_role=author_role,
        )
        if run_id and round_id
        else "",
    }


def _text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [maybe_text(item) for item in value if maybe_text(item)]


def _compact_skill_approval_request(request: dict[str, Any]) -> dict[str, Any]:
    requested_skill_args = _text_list(request.get("requested_skill_args"))
    evidence_refs = _text_list(request.get("evidence_refs"))
    basis_object_ids = _text_list(request.get("basis_object_ids"))
    return {
        key: value
        for key, value in {
            "request_id": maybe_text(request.get("request_id")),
            "request_status": maybe_text(request.get("request_status")),
            "skill_name": maybe_text(request.get("skill_name")),
            "skill_layer": maybe_text(request.get("skill_layer")),
            "requested_by_role": maybe_text(request.get("requested_by_role")),
            "requested_actor_role": maybe_text(request.get("requested_actor_role")),
            "rationale": maybe_text(request.get("rationale")),
            "requested_skill_args": requested_skill_args,
            "evidence_ref_count": len(evidence_refs),
            "basis_object_id_count": len(basis_object_ids),
            "latest_decision_status": maybe_text(request.get("latest_decision_status")),
            "latest_decision_by_role": maybe_text(request.get("latest_decision_by_role")),
            "consumed_receipt_id": maybe_text(request.get("consumed_receipt_id")),
        }.items()
        if value not in ("", [], {}, 0)
    }


def _skill_approval_bridge_surface(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    transitions: dict[str, Any],
) -> dict[str, Any]:
    requests = (
        transitions.get("latest_skill_approval_requests", [])
        if isinstance(transitions.get("latest_skill_approval_requests"), list)
        else []
    )
    pending_requests = [
        _compact_skill_approval_request(request)
        for request in requests
        if isinstance(request, dict)
        and maybe_text(request.get("request_status")) == SKILL_REQUEST_STATUS_PENDING
    ]
    approved_unconsumed_requests = [
        _compact_skill_approval_request(request)
        for request in requests
        if isinstance(request, dict)
        and maybe_text(request.get("request_status")) == SKILL_REQUEST_STATUS_APPROVED
    ]
    return {
        "semantics": (
            "Skill approval bridge exposes operator-gated helper status and "
            "command templates only. It does not recommend helper use or "
            "evaluate evidence."
        ),
        "pending_requests": pending_requests,
        "approved_unconsumed_requests": approved_unconsumed_requests,
        "commands": {
            "query_skill_approval_requests": maybe_text(
                transitions.get("query_skill_approval_requests_command")
            ),
            "request_skill_approval_template": kernel_command(
                "request-skill-approval",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--skill-name",
                "<skill_name>",
                "--requested-actor-role",
                "<requested_actor_role>",
                "--rationale",
                "<approval_rationale>",
                actor_role="<requesting_role>",
            )
            if run_id and round_id
            else "",
            "approve_skill_approval_template": maybe_text(
                transitions.get("approve_skill_approval_request_command_template")
            ),
            "reject_skill_approval_template": maybe_text(
                transitions.get("reject_skill_approval_request_command_template")
            ),
            "preflight_skill_template": kernel_command(
                "preflight-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--skill-name",
                "<skill_name>",
                "--contract-mode",
                "warn",
                actor_role="<requested_actor_role>",
            )
            if run_id and round_id
            else "",
            "run_approved_skill_template": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="<skill_name>",
                contract_mode="warn",
                actor_role="<requested_actor_role>",
                skill_approval_request_id="<request_id>",
                skill_args=["<skill_specific_args>"],
            )
            if run_id and round_id
            else "",
        },
    }


def show_source_acquisition_intents_surface(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
    limit: int = 20,
) -> dict[str, Any]:
    resolved_run_id, resolved_round_id = _resolved_ids(run_dir, run_id, round_id)
    proposals = query_council_objects(
        run_dir,
        object_kind="source-acquisition-proposal",
        run_id=resolved_run_id,
        round_id=resolved_round_id,
        limit=_safe_limit(limit),
    )
    objects = proposals.get("objects", []) if isinstance(proposals.get("objects"), list) else []
    surfaced_objects = [
        {
            **obj,
            "source_execution_surface": _source_intent_execution_surface(
                run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                intent=obj,
            ),
        }
        for obj in objects
        if isinstance(obj, dict)
    ]
    return {
        "status": "completed",
        "surface": "source-acquisition-intents",
        "run_id": resolved_run_id,
        "round_id": resolved_round_id,
        "semantics": "Source acquisition proposals are agent-authored intents; runtime lists them without selecting or ranking sources.",
        "summary": proposals.get("summary", {}) if isinstance(proposals.get("summary"), dict) else {},
        "objects": surfaced_objects,
        "commands": {
            "query_source_acquisition_proposals": _status_object_query_command(
                run_dir,
                resolved_run_id,
                resolved_round_id,
                "source-acquisition-proposal",
            )
            if resolved_run_id and resolved_round_id
            else "",
            "submit_source_acquisition_proposal_template": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="submit-source-acquisition-proposal",
                contract_mode="warn",
                actor_role="<agent_role>",
                skill_args=[
                    "--author-role",
                    "<agent_role>",
                    "--source-skill",
                    "<source_skill>",
                    "--query-parameters-json",
                    "{\"query\":\"<agent_defined_query_or_params>\"}",
                    "--target-kind",
                    "<evidence-request|challenge|finding|round>",
                    "--target-id",
                    "<target_id>",
                    "--rationale",
                    "<why this source acquisition belongs in the council record>",
                    "--declared-side-effect",
                    "network-external",
                    "--evidence-ref",
                    "<artifact_ref>",
                ],
            )
            if resolved_run_id and resolved_round_id
            else "",
        },
    }


def _arg_value(argv: list[str], *names: str) -> str:
    for index, item in enumerate(argv):
        if item in names and index + 1 < len(argv):
            return maybe_text(argv[index + 1])
        for name in names:
            if item.startswith(name + "="):
                return maybe_text(item.split("=", 1)[1])
    return ""


def _fetch_step_surface(step: dict[str, Any], *, run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    argv = render_fetch_argv(step, run_dir=run_dir, run_id=run_id, round_id=round_id)
    source_skill = maybe_text(step.get("source_skill"))
    capability_hints = source_capability_hints(source_skill)
    return {
        "step_id": maybe_text(step.get("step_id")),
        "step_kind": maybe_text(step.get("step_kind")),
        "role": maybe_text(step.get("role")),
        "source_skill": source_skill,
        "family_id": maybe_text(step.get("family_id")),
        "layer_id": maybe_text(step.get("layer_id")),
        "normalizer_skill": maybe_text(step.get("normalizer_skill")),
        "artifact_capture": maybe_text(step.get("artifact_capture")),
        "artifact_path": maybe_text(step.get("artifact_path")),
        "provider_mode": _arg_value(argv, "--source-mode", "--mode") or maybe_text(step.get("step_kind")),
        "time_range": {
            "start": _arg_value(
                argv,
                "--start",
                "--start-utc",
                "--start-date",
                "--start-datetime",
                "--published-after",
                "--datetime-from",
                "--from",
            ),
            "end": _arg_value(
                argv,
                "--end",
                "--end-utc",
                "--end-date",
                "--end-datetime",
                "--published-before",
                "--datetime-to",
                "--to",
            ),
        },
        "command": " ".join(argv),
        "depends_on": step.get("depends_on", []) if isinstance(step.get("depends_on"), list) else [],
        "anchor_mode": maybe_text(step.get("anchor_mode")),
        "source_capability": {
            "provider_modes": capability_hints.get("provider_modes", [])
            if isinstance(capability_hints.get("provider_modes"), list)
            else [],
            "fetch_argument_templates": capability_hints.get("fetch_argument_templates", [])
            if isinstance(capability_hints.get("fetch_argument_templates"), list)
            else [],
        },
    }


def show_source_surfaces_surface(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
    limit: int = 50,
) -> dict[str, Any]:
    resolved_run_id, resolved_round_id = _resolved_ids(run_dir, run_id, round_id)
    fetch_plan_path = run_dir / "runtime" / f"fetch_plan_{resolved_round_id}.json"
    fetch_plan = load_json_if_exists(fetch_plan_path) or {}
    steps = fetch_plan.get("steps", []) if isinstance(fetch_plan.get("steps"), list) else []
    catalog_entries = []
    for source_skill, entry in sorted(SOURCE_CATALOG.items()):
        if not isinstance(entry, dict):
            continue
        capability_hints = source_capability_hints(source_skill)
        catalog_entries.append(
            {
                "source_skill": source_skill,
                "role": maybe_text(entry.get("role")),
                "family_id": maybe_text(entry.get("family_id")),
                "family_label": maybe_text(entry.get("family_label")),
                "layer_id": maybe_text(entry.get("layer_id")),
                "layer_label": maybe_text(entry.get("layer_label")),
                "tier": maybe_text(entry.get("tier")),
                "normalizer_skill": maybe_text(entry.get("normalizer_skill")),
                "artifact_capture": maybe_text(entry.get("artifact_capture")),
                "requires_anchor": bool(entry.get("requires_anchor")),
                "anchor_source_skills": entry.get("anchor_source_skills", [])
                if isinstance(entry.get("anchor_source_skills"), list)
                else [],
                "provider_modes": capability_hints.get("provider_modes", [])
                if isinstance(capability_hints.get("provider_modes"), list)
                else [],
                "fetch_argument_templates": capability_hints.get("fetch_argument_templates", [])
                if isinstance(capability_hints.get("fetch_argument_templates"), list)
                else [],
            }
        )
    return {
        "status": "completed",
        "surface": "source-surfaces",
        "run_id": resolved_run_id,
        "round_id": resolved_round_id,
        "semantics": "This surface exposes source capabilities and prepared fetch commands; it does not rank, select, or validate evidence.",
        "mission_window": fetch_plan.get("run", {}).get("window", {})
        if isinstance(fetch_plan.get("run"), dict)
        else {},
        "fetch_plan": {
            "present": bool(fetch_plan_path.exists()),
            "path": str(fetch_plan_path.resolve()),
            "plan_id": maybe_text(fetch_plan.get("plan_id")),
            "step_count": len(steps),
            "warnings": fetch_plan.get("warnings", []) if isinstance(fetch_plan.get("warnings"), list) else [],
        },
        "prepared_steps": [
            _fetch_step_surface(step, run_dir=run_dir, run_id=resolved_run_id, round_id=resolved_round_id)
            for step in steps[: _safe_limit(limit)]
            if isinstance(step, dict)
        ],
        "catalog": catalog_entries[: _safe_limit(limit)],
        "commands": {
            "prepare_round": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="prepare-round",
                contract_mode="warn",
                actor_role="moderator",
            )
            if resolved_run_id and resolved_round_id
            else "",
            "normalize_fetch_execution": run_skill_command(
                run_dir=run_dir,
                run_id=resolved_run_id,
                round_id=resolved_round_id,
                skill_name="normalize-fetch-execution",
                contract_mode="warn",
                actor_role="<source_owner_role>",
            )
            if resolved_run_id and resolved_round_id
            else "",
        },
    }


def _liveness_subset(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    key: str,
    limit: int,
) -> dict[str, Any]:
    liveness = build_round_liveness_surface(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        limit=_safe_limit(limit),
    )
    unresolved = (
        liveness.get("unresolved_sets", {})
        if isinstance(liveness.get("unresolved_sets"), dict)
        else {}
    )
    subset = unresolved.get(key, {}) if isinstance(unresolved.get(key), dict) else {}
    if isinstance(unresolved.get(key), list):
        subset = {
            "count": len(unresolved.get(key, [])),
            "items": unresolved.get(key, []),
        }
    return {
        "status": "completed",
        "run_id": run_id,
        "round_id": round_id,
        "semantics": liveness.get("semantics", ""),
        "summary": {
            "liveness_status": maybe_text(liveness.get("liveness_status")),
            "unresolved_ref_count": int(liveness.get("unresolved_ref_count") or 0),
            "surface_count": int(subset.get("count") or 0),
        },
        "surface": key,
        "items": subset.get("items", []) if isinstance(subset.get("items"), list) else [],
        "query": subset.get("query", {}) if isinstance(subset.get("query"), dict) else {},
        "commands": {
            "show_round_liveness": kernel_command(
                "show-council-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
            )
            if run_id and round_id
            else ""
        },
    }


def show_open_challenges_surface(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
    limit: int = 20,
) -> dict[str, Any]:
    resolved_run_id, resolved_round_id = _resolved_ids(run_dir, run_id, round_id)
    return _liveness_subset(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
        key="open_challenges",
        limit=limit,
    )


def show_unbundled_findings_surface(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
    limit: int = 20,
) -> dict[str, Any]:
    resolved_run_id, resolved_round_id = _resolved_ids(run_dir, run_id, round_id)
    return _liveness_subset(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
        key="unbundled_findings",
        limit=limit,
    )


def show_council_status_surface(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
    limit: int = 20,
) -> dict[str, Any]:
    resolved_run_id, resolved_round_id = _resolved_ids(run_dir, run_id, round_id)
    liveness = build_round_liveness_surface(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
        limit=_safe_limit(limit),
    )
    transitions = transition_request_state(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
    )
    archive = show_archive_status_surface(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
    )
    source_intents = show_source_acquisition_intents_surface(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
        limit=limit,
    )
    skill_approval_bridge = _skill_approval_bridge_surface(
        run_dir,
        run_id=resolved_run_id,
        round_id=resolved_round_id,
        transitions=transitions,
    )
    return {
        "status": "completed",
        "surface": "council-status",
        "run_id": resolved_run_id,
        "round_id": resolved_round_id,
        "semantics": "Council status is an operator-visible object/status surface; it does not prescribe an agenda or evidence conclusion.",
        "summary": {
            "liveness_status": maybe_text(liveness.get("liveness_status")),
            "round_unresolved_ref_count": int(liveness.get("unresolved_ref_count") or 0),
            "pending_transition_request_count": int(
                transitions.get("summary", {}).get("pending_request_count") or 0
            )
            if isinstance(transitions.get("summary"), dict)
            else 0,
            "pending_skill_approval_request_count": int(
                transitions.get("summary", {}).get("pending_skill_approval_request_count") or 0
            )
            if isinstance(transitions.get("summary"), dict)
            else 0,
            "source_acquisition_intent_count": int(
                source_intents.get("summary", {}).get("matching_object_count") or 0
            )
            if isinstance(source_intents.get("summary"), dict)
            else len(source_intents.get("objects", []))
            if isinstance(source_intents.get("objects"), list)
            else 0,
            "archive_gap_count": len(archive.get("gap_hints", []))
            if isinstance(archive.get("gap_hints"), list)
            else 0,
        },
        "round_liveness": liveness,
        "transitions": transitions,
        "skill_approval_bridge": skill_approval_bridge,
        "archive_status": {
            "checkpoint_summary": archive.get("checkpoint_summary", {}),
            "checkpoint_inputs": archive.get("checkpoint_inputs", {}),
            "history_reuse": archive.get("history_reuse", {}),
            "source_receipts": archive.get("source_receipts", {}),
            "databases": archive.get("databases", {}),
            "gap_hints": archive.get("gap_hints", []),
            "commands": archive.get("commands", {}),
        },
        "source_acquisition_intents": source_intents,
        "commands": {
            "show_source_surfaces": kernel_command(
                "show-source-surfaces",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
            )
            if resolved_run_id and resolved_round_id
            else "",
            "show_source_acquisition_intents": kernel_command(
                "show-source-acquisition-intents",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
            )
            if resolved_run_id and resolved_round_id
            else "",
            "show_open_challenges": kernel_command(
                "show-open-challenges",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
            )
            if resolved_run_id and resolved_round_id
            else "",
            "show_unbundled_findings": kernel_command(
                "show-unbundled-findings",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
            )
            if resolved_run_id and resolved_round_id
            else "",
            "show_archive_status": kernel_command(
                "show-archive-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
            )
            if resolved_run_id and resolved_round_id
            else "",
        },
    }

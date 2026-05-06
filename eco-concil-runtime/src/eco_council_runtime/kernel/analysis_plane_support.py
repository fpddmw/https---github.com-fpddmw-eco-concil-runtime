from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .analysis_plane_contracts import (
    _config_list,
    analysis_config,
    decode_json,
    json_text,
    maybe_text,
    stable_hash,
    unique_texts,
)

def resolve_artifact_path(
    run_dir: Path,
    override: str | Path,
    default_relative: str,
) -> Path:
    if isinstance(override, Path):
        return override.expanduser().resolve()
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()

def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None

def parse_artifact_ref_text(value: Any) -> tuple[str, str]:
    text = maybe_text(value)
    if not text:
        return "", ""
    marker = text.find(":$")
    if marker >= 0:
        return text[:marker], text[marker + 1 :]
    return text, ""

def normalized_artifact_ref(
    value: Any,
    *,
    relation: str = "",
) -> dict[str, str]:
    if isinstance(value, dict):
        artifact_path = maybe_text(value.get("artifact_path"))
        record_locator = maybe_text(value.get("record_locator"))
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if not artifact_path and artifact_ref:
            artifact_path, parsed_locator = parse_artifact_ref_text(artifact_ref)
            if not record_locator:
                record_locator = parsed_locator
        if artifact_path and not artifact_ref:
            artifact_ref = (
                artifact_path
                if not record_locator
                else f"{artifact_path}:{record_locator}"
            )
        if not artifact_path:
            return {}
        return {
            "relation": maybe_text(relation),
            "signal_id": maybe_text(value.get("signal_id")),
            "artifact_path": artifact_path,
            "record_locator": record_locator,
            "artifact_ref": artifact_ref or artifact_path,
        }
    artifact_path, record_locator = parse_artifact_ref_text(value)
    if not artifact_path:
        return {}
    artifact_ref = (
        artifact_path if not record_locator else f"{artifact_path}:{record_locator}"
    )
    return {
        "relation": maybe_text(relation),
        "signal_id": "",
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": artifact_ref,
    }

def artifact_ref_from_path(path_value: Any, *, relation: str = "") -> dict[str, str]:
    path_text = maybe_text(path_value)
    if not path_text:
        return {}
    return normalized_artifact_ref(
        {
            "artifact_path": path_text,
            "record_locator": "$",
            "artifact_ref": f"{path_text}:$",
        },
        relation=relation,
    )

def unique_artifact_refs(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    results: list[dict[str, str]] = []
    for value in values:
        relation = maybe_text(value.get("relation"))
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if not artifact_ref:
            continue
        key = (relation, artifact_ref)
        if key in seen:
            continue
        seen.add(key)
        results.append(
            {
                "relation": relation,
                "signal_id": maybe_text(value.get("signal_id")),
                "artifact_path": maybe_text(value.get("artifact_path")),
                "record_locator": maybe_text(value.get("record_locator")),
                "artifact_ref": artifact_ref,
            }
        )
    return results

def _artifact_present(path_value: Any) -> bool:
    artifact_path = maybe_text(path_value)
    if not artifact_path:
        return False
    return Path(artifact_path).exists()

def _resolve_analysis_artifact_path(
    run_dir: Path,
    *,
    analysis_kind: str,
    artifact_path: str | Path,
    round_id: str,
) -> Path:
    config = analysis_config(analysis_kind)
    default_relative = maybe_text(config.get("default_relative")).format(round_id=round_id)
    return resolve_artifact_path(run_dir, artifact_path, default_relative)

def empty_result_contract() -> dict[str, Any]:
    return {
        "query_basis": {},
        "parent_result_sets": [],
        "parent_artifact_refs": [],
        "lineage_counts": {
            "query_basis_field_count": 0,
            "parent_result_set_count": 0,
            "parent_artifact_ref_count": 0,
            "parent_id_count": 0,
            "artifact_ref_count": 0,
        },
    }

def normalized_query_basis(
    payload: dict[str, Any],
    *,
    config: dict[str, Any],
) -> dict[str, Any]:
    basis: dict[str, Any] = {}
    explicit = payload.get("query_basis")
    if isinstance(explicit, dict):
        basis.update(explicit)
    for field_name in _config_list(config, "query_basis_fields"):
        if field_name in basis or field_name not in payload:
            continue
        value = payload.get(field_name)
        if value in (None, "", [], {}):
            continue
        basis[field_name] = value
    return basis

def parent_artifact_refs_from_payload(
    payload: dict[str, Any],
    *,
    config: dict[str, Any],
) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    explicit_refs = payload.get("parent_artifact_refs")
    if isinstance(explicit_refs, list):
        for value in explicit_refs:
            ref = normalized_artifact_ref(value, relation="parent_artifact_refs")
            if ref:
                refs.append(ref)
    query_basis = payload.get("query_basis")
    if isinstance(query_basis, dict) and isinstance(
        query_basis.get("input_artifact_refs"), list
    ):
        for value in query_basis["input_artifact_refs"]:
            ref = normalized_artifact_ref(value, relation="input_artifact_refs")
            if ref:
                refs.append(ref)
    for field_name in _config_list(config, "parent_artifact_fields"):
        ref = artifact_ref_from_path(payload.get(field_name), relation=field_name)
        if ref:
            refs.append(ref)
    return unique_artifact_refs(refs)

def item_parent_ids(
    item: dict[str, Any],
    *,
    config: dict[str, Any],
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for field_name in _config_list(config, "item_parent_id_fields"):
        value_text = maybe_text(item.get(field_name))
        if value_text:
            results.append({"relation": field_name, "value_text": value_text})
    for field_name in _config_list(config, "item_parent_id_list_fields"):
        values = item.get(field_name)
        if not isinstance(values, list):
            continue
        for value_text in unique_texts(values):
            results.append({"relation": field_name, "value_text": value_text})
    return results

def item_artifact_refs(
    item: dict[str, Any],
    *,
    config: dict[str, Any],
) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for field_name in _config_list(config, "item_artifact_ref_fields"):
        values = item.get(field_name)
        if isinstance(values, list):
            for value in values:
                ref = normalized_artifact_ref(value, relation=field_name)
                if ref:
                    refs.append(ref)
        else:
            ref = normalized_artifact_ref(values, relation=field_name)
            if ref:
                refs.append(ref)
    return unique_artifact_refs(refs)

def planned_item_rows(
    items: list[dict[str, Any]],
    *,
    id_field: str,
    result_set_id: str,
) -> list[dict[str, Any]]:
    planned: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        item_id_value = maybe_text(item.get(id_field)) or str(index)
        planned.append(
            {
                "index": index,
                "item_id_value": item_id_value,
                "item_id": "analysis-item-"
                + stable_hash(result_set_id, item_id_value)[:16],
                "item": item,
            }
        )
    return planned

def resolve_parent_result_sets(
    connection: sqlite3.Connection | None,
    *,
    run_id: str,
    round_id: str,
    result_set_id: str,
    parent_artifact_refs: list[dict[str, str]],
) -> list[dict[str, str]]:
    if connection is None:
        return []
    seen: set[tuple[str, str]] = set()
    results: list[dict[str, str]] = []
    for ref in parent_artifact_refs:
        artifact_path = maybe_text(ref.get("artifact_path"))
        if not artifact_path:
            continue
        row = connection.execute(
            """
            SELECT result_set_id, analysis_kind, source_skill, artifact_path
            FROM analysis_result_sets
            WHERE run_id = ?
              AND round_id = ?
              AND artifact_path = ?
            ORDER BY generated_at_utc DESC, result_set_id DESC
            LIMIT 1
            """,
            (run_id, round_id, artifact_path),
        ).fetchone()
        if row is None:
            continue
        resolved_result_set_id = maybe_text(row["result_set_id"])
        if not resolved_result_set_id or resolved_result_set_id == result_set_id:
            continue
        relation = maybe_text(ref.get("relation"))
        key = (relation, resolved_result_set_id)
        if key in seen:
            continue
        seen.add(key)
        results.append(
            {
                "relation": relation,
                "result_set_id": resolved_result_set_id,
                "analysis_kind": maybe_text(row["analysis_kind"]),
                "artifact_path": maybe_text(row["artifact_path"]),
                "source_skill": maybe_text(row["source_skill"]),
            }
        )
    return results

def deduped_lineage_entries(
    entries: list[dict[str, Any]],
    *,
    result_set_id: str = "",
) -> list[dict[str, Any]]:
    seen: set[tuple[str, ...]] = set()
    results: list[dict[str, Any]] = []
    for entry in entries:
        metadata = (
            entry.get("metadata")
            if isinstance(entry.get("metadata"), dict)
            else {}
        )
        metadata_text = json_text(metadata)
        signature = (
            maybe_text(result_set_id),
            maybe_text(entry.get("item_id")),
            maybe_text(entry.get("lineage_scope")),
            maybe_text(entry.get("lineage_type")),
            maybe_text(entry.get("relation")),
            maybe_text(entry.get("value_text")),
            maybe_text(entry.get("artifact_path")),
            maybe_text(entry.get("record_locator")),
            maybe_text(entry.get("source_analysis_kind")),
            metadata_text,
        )
        if signature in seen:
            continue
        seen.add(signature)
        results.append(
            {
                "lineage_id": "analysis-lineage-"
                + stable_hash(*signature)[:20],
                "item_id": maybe_text(entry.get("item_id")),
                "lineage_scope": maybe_text(entry.get("lineage_scope")),
                "lineage_type": maybe_text(entry.get("lineage_type")),
                "relation": maybe_text(entry.get("relation")),
                "value_text": maybe_text(entry.get("value_text")),
                "artifact_path": maybe_text(entry.get("artifact_path")),
                "record_locator": maybe_text(entry.get("record_locator")),
                "source_analysis_kind": maybe_text(entry.get("source_analysis_kind")),
                "metadata": metadata,
            }
        )
    return results

def build_result_contract(
    payload: dict[str, Any],
    *,
    config: dict[str, Any],
    run_id: str,
    round_id: str,
    result_set_id: str,
    planned_rows: list[dict[str, Any]],
    connection: sqlite3.Connection | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    query_basis = normalized_query_basis(payload, config=config)
    parent_artifact_refs = parent_artifact_refs_from_payload(payload, config=config)
    parent_result_sets = resolve_parent_result_sets(
        connection,
        run_id=run_id,
        round_id=round_id,
        result_set_id=result_set_id,
        parent_artifact_refs=parent_artifact_refs,
    )
    lineage_entries: list[dict[str, Any]] = []
    for relation in sorted(query_basis.keys()):
        value = query_basis[relation]
        value_text = (
            maybe_text(value)
            if isinstance(value, (str, int, float, bool)) or value is None
            else json_text(value)
        )
        lineage_entries.append(
            {
                "item_id": "",
                "lineage_scope": "result-set",
                "lineage_type": "query-basis",
                "relation": relation,
                "value_text": value_text,
                "artifact_path": "",
                "record_locator": "",
                "source_analysis_kind": "",
                "metadata": {"value": value},
            }
        )
    for ref in parent_artifact_refs:
        lineage_entries.append(
            {
                "item_id": "",
                "lineage_scope": "result-set",
                "lineage_type": "artifact-ref",
                "relation": maybe_text(ref.get("relation")),
                "value_text": maybe_text(ref.get("artifact_ref")),
                "artifact_path": maybe_text(ref.get("artifact_path")),
                "record_locator": maybe_text(ref.get("record_locator")),
                "source_analysis_kind": "",
                "metadata": {"signal_id": maybe_text(ref.get("signal_id"))},
            }
        )
    for parent in parent_result_sets:
        lineage_entries.append(
            {
                "item_id": "",
                "lineage_scope": "result-set",
                "lineage_type": "parent-result-set",
                "relation": maybe_text(parent.get("relation")),
                "value_text": maybe_text(parent.get("result_set_id")),
                "artifact_path": maybe_text(parent.get("artifact_path")),
                "record_locator": "$",
                "source_analysis_kind": maybe_text(parent.get("analysis_kind")),
                "metadata": {
                    "source_skill": maybe_text(parent.get("source_skill")),
                },
            }
        )
    parent_id_count = 0
    artifact_ref_count = 0
    for planned in planned_rows:
        item = planned["item"] if isinstance(planned.get("item"), dict) else {}
        item_id = maybe_text(planned.get("item_id"))
        parent_ids = item_parent_ids(item, config=config)
        refs = item_artifact_refs(item, config=config)
        parent_id_count += len(parent_ids)
        artifact_ref_count += len(refs)
        for parent_id in parent_ids:
            lineage_entries.append(
                {
                    "item_id": item_id,
                    "lineage_scope": "item",
                    "lineage_type": "parent-id",
                    "relation": maybe_text(parent_id.get("relation")),
                    "value_text": maybe_text(parent_id.get("value_text")),
                    "artifact_path": "",
                    "record_locator": "",
                    "source_analysis_kind": "",
                    "metadata": {},
                }
            )
        for ref in refs:
            lineage_entries.append(
                {
                    "item_id": item_id,
                    "lineage_scope": "item",
                    "lineage_type": "artifact-ref",
                    "relation": maybe_text(ref.get("relation")),
                    "value_text": maybe_text(ref.get("artifact_ref")),
                    "artifact_path": maybe_text(ref.get("artifact_path")),
                    "record_locator": maybe_text(ref.get("record_locator")),
                    "source_analysis_kind": "",
                    "metadata": {"signal_id": maybe_text(ref.get("signal_id"))},
                }
            )
    contract = {
        "query_basis": query_basis,
        "parent_result_sets": parent_result_sets,
        "parent_artifact_refs": [
            {
                "relation": maybe_text(ref.get("relation")),
                "artifact_path": maybe_text(ref.get("artifact_path")),
                "record_locator": maybe_text(ref.get("record_locator")),
                "artifact_ref": maybe_text(ref.get("artifact_ref")),
            }
            for ref in parent_artifact_refs
        ],
        "lineage_counts": {
            "query_basis_field_count": len(query_basis),
            "parent_result_set_count": len(parent_result_sets),
            "parent_artifact_ref_count": len(parent_artifact_refs),
            "parent_id_count": parent_id_count,
            "artifact_ref_count": artifact_ref_count,
        },
    }
    return contract, deduped_lineage_entries(
        lineage_entries,
        result_set_id=result_set_id,
    )

def load_result_contract(
    connection: sqlite3.Connection,
    *,
    result_set_id: str,
) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT item_id, lineage_scope, lineage_type, relation, value_text,
               artifact_path, record_locator, source_analysis_kind, metadata_json
        FROM analysis_result_lineage
        WHERE result_set_id = ?
        ORDER BY lineage_scope, item_id, lineage_type, relation, value_text, lineage_id
        """,
        (result_set_id,),
    ).fetchall()
    if not rows:
        return empty_result_contract()
    query_basis: dict[str, Any] = {}
    parent_result_sets: list[dict[str, str]] = []
    parent_artifact_refs: list[dict[str, str]] = []
    parent_id_count = 0
    artifact_ref_count = 0
    for row in rows:
        lineage_scope = maybe_text(row["lineage_scope"])
        lineage_type = maybe_text(row["lineage_type"])
        relation = maybe_text(row["relation"])
        value_text = maybe_text(row["value_text"])
        artifact_path = maybe_text(row["artifact_path"])
        record_locator = maybe_text(row["record_locator"])
        source_analysis_kind = maybe_text(row["source_analysis_kind"])
        metadata = decode_json(maybe_text(row["metadata_json"]), {})
        if lineage_scope == "result-set" and lineage_type == "query-basis":
            if isinstance(metadata, dict) and "value" in metadata:
                query_basis[relation] = metadata.get("value")
            else:
                query_basis[relation] = value_text
            continue
        if lineage_scope == "result-set" and lineage_type == "parent-result-set":
            parent_result_sets.append(
                {
                    "relation": relation,
                    "result_set_id": value_text,
                    "analysis_kind": source_analysis_kind,
                    "artifact_path": artifact_path,
                    "source_skill": maybe_text(
                        metadata.get("source_skill") if isinstance(metadata, dict) else ""
                    ),
                }
            )
            continue
        if lineage_scope == "result-set" and lineage_type == "artifact-ref":
            parent_artifact_refs.append(
                {
                    "relation": relation,
                    "artifact_path": artifact_path,
                    "record_locator": record_locator,
                    "artifact_ref": value_text,
                }
            )
            continue
        if lineage_scope == "item" and lineage_type == "parent-id":
            parent_id_count += 1
            continue
        if lineage_scope == "item" and lineage_type == "artifact-ref":
            artifact_ref_count += 1
    return {
        "query_basis": query_basis,
        "parent_result_sets": parent_result_sets,
        "parent_artifact_refs": unique_artifact_refs(parent_artifact_refs),
        "lineage_counts": {
            "query_basis_field_count": len(query_basis),
            "parent_result_set_count": len(parent_result_sets),
            "parent_artifact_ref_count": len(parent_artifact_refs),
            "parent_id_count": parent_id_count,
            "artifact_ref_count": artifact_ref_count,
        },
    }


__all__ = [
    "resolve_artifact_path",
    "load_json_if_exists",
    "parse_artifact_ref_text",
    "normalized_artifact_ref",
    "artifact_ref_from_path",
    "unique_artifact_refs",
    "_artifact_present",
    "_resolve_analysis_artifact_path",
    "empty_result_contract",
    "normalized_query_basis",
    "parent_artifact_refs_from_payload",
    "item_parent_ids",
    "item_artifact_refs",
    "planned_item_rows",
    "resolve_parent_result_sets",
    "deduped_lineage_entries",
    "build_result_contract",
    "load_result_contract",
]

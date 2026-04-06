from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ANALYSIS_KIND_EVIDENCE_COVERAGE = "evidence-coverage"
ANALYSIS_KIND_CLAIM_SCOPE = "claim-scope"
ANALYSIS_KIND_OBSERVATION_SCOPE = "observation-scope"
ANALYSIS_KIND_CLAIM_OBSERVATION_LINK = "claim-observation-link"
ANALYSIS_KIND_CLAIM_CLUSTER = "claim-cluster"
ANALYSIS_KIND_MERGED_OBSERVATION = "merged-observation"
ANALYSIS_KIND_CLAIM_CANDIDATE = "claim-candidate"
ANALYSIS_KIND_OBSERVATION_CANDIDATE = "observation-candidate"

ANALYSIS_KIND_CONFIGS: dict[str, dict[str, Any]] = {
    ANALYSIS_KIND_EVIDENCE_COVERAGE: {
        "artifact_label": "coverage",
        "default_relative": "analytics/evidence_coverage_{round_id}.json",
        "items_key": "coverages",
        "count_key": "coverage_count",
        "id_field": "coverage_id",
        "subject_field": "claim_id",
        "score_field": "coverage_score",
        "state_field": "readiness",
        "related_id_fields": ["coverage_id", "claim_id"],
        "default_source_skill": "eco-score-evidence-coverage",
        "summary_fields": [
            "links_path",
            "claim_scope_path",
            "observation_scope_path",
        ],
        "query_basis_fields": [
            "links_path",
            "claim_scope_path",
            "observation_scope_path",
            "links_source",
            "claim_scope_source",
            "observation_scope_source",
        ],
        "parent_artifact_fields": [
            "links_path",
            "claim_scope_path",
            "observation_scope_path",
        ],
        "item_parent_id_fields": ["claim_id"],
        "item_parent_id_list_fields": ["linked_observation_ids"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_SCOPE: {
        "artifact_label": "claim-scope",
        "default_relative": "analytics/claim_scope_proposals_{round_id}.json",
        "items_key": "scopes",
        "count_key": "scope_count",
        "id_field": "claim_scope_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "scope_kind",
        "related_id_fields": ["claim_scope_id", "claim_id", "claim_type"],
        "default_source_skill": "eco-derive-claim-scope",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_fields": ["claim_id"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_OBSERVATION_SCOPE: {
        "artifact_label": "observation-scope",
        "default_relative": "analytics/observation_scope_proposals_{round_id}.json",
        "items_key": "scopes",
        "count_key": "scope_count",
        "id_field": "observation_scope_id",
        "subject_field": "observation_id",
        "score_field": "confidence",
        "state_field": "scope_kind",
        "related_id_fields": ["observation_scope_id", "observation_id", "metric"],
        "default_source_skill": "eco-derive-observation-scope",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_fields": ["observation_id"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_OBSERVATION_LINK: {
        "artifact_label": "claim-observation-link",
        "default_relative": "analytics/claim_observation_links_{round_id}.json",
        "items_key": "links",
        "count_key": "link_count",
        "id_field": "link_id",
        "subject_field": "claim_id",
        "score_field": "confidence",
        "state_field": "relation",
        "related_id_fields": ["link_id", "claim_id", "observation_id"],
        "default_source_skill": "eco-link-claims-to-observations",
        "summary_fields": ["claim_input_path", "observation_input_path"],
        "query_basis_fields": [
            "claim_input_path",
            "observation_input_path",
        ],
        "parent_artifact_fields": [
            "claim_input_path",
            "observation_input_path",
        ],
        "item_parent_id_fields": ["claim_id", "observation_id"],
        "item_artifact_ref_fields": ["evidence_refs"],
    },
    ANALYSIS_KIND_CLAIM_CLUSTER: {
        "artifact_label": "claim-cluster",
        "default_relative": "analytics/claim_candidate_clusters_{round_id}.json",
        "items_key": "clusters",
        "count_key": "cluster_count",
        "id_field": "cluster_id",
        "subject_field": "cluster_id",
        "state_field": "status",
        "related_id_fields": ["cluster_id", "claim_type", "semantic_fingerprint"],
        "default_source_skill": "eco-cluster-claim-candidates",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_list_fields": ["member_claim_ids", "source_signal_ids"],
        "item_artifact_ref_fields": ["public_refs"],
    },
    ANALYSIS_KIND_MERGED_OBSERVATION: {
        "artifact_label": "merged-observation",
        "default_relative": "analytics/merged_observation_candidates_{round_id}.json",
        "items_key": "merged_observations",
        "count_key": "merged_count",
        "id_field": "merged_observation_id",
        "subject_field": "merged_observation_id",
        "state_field": "aggregation",
        "related_id_fields": ["merged_observation_id", "metric"],
        "default_source_skill": "eco-merge-observation-candidates",
        "summary_fields": ["input_path"],
        "query_basis_fields": ["input_path"],
        "parent_artifact_fields": ["input_path"],
        "item_parent_id_list_fields": ["member_observation_ids", "source_signal_ids"],
        "item_artifact_ref_fields": ["provenance_refs"],
    },
    ANALYSIS_KIND_CLAIM_CANDIDATE: {
        "artifact_label": "claim-candidate",
        "default_relative": "analytics/claim_candidates_{round_id}.json",
        "items_key": "candidates",
        "count_key": "candidate_count",
        "id_field": "claim_id",
        "subject_field": "claim_id",
        "state_field": "status",
        "related_id_fields": ["claim_id", "claim_type"],
        "default_source_skill": "eco-extract-claim-candidates",
        "summary_fields": [],
        "item_parent_id_list_fields": ["source_signal_ids"],
        "item_artifact_ref_fields": ["public_refs"],
    },
    ANALYSIS_KIND_OBSERVATION_CANDIDATE: {
        "artifact_label": "observation-candidate",
        "default_relative": "analytics/observation_candidates_{round_id}.json",
        "items_key": "candidates",
        "count_key": "candidate_count",
        "id_field": "observation_id",
        "subject_field": "observation_id",
        "state_field": "aggregation",
        "related_id_fields": ["observation_id", "metric", "source_skill"],
        "default_source_skill": "eco-extract-observation-candidates",
        "summary_fields": [],
        "item_parent_id_list_fields": ["source_signal_ids"],
        "item_artifact_ref_fields": ["provenance_refs"],
    },
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS analysis_result_sets (
    result_set_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    source_skill TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '$',
    generated_at_utc TEXT NOT NULL DEFAULT '',
    item_count INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_sets_round_kind
ON analysis_result_sets(run_id, round_id, analysis_kind, generated_at_utc, result_set_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_sets_artifact
ON analysis_result_sets(artifact_path, analysis_kind, generated_at_utc, result_set_id);

CREATE TABLE IF NOT EXISTS analysis_result_items (
    item_id TEXT PRIMARY KEY,
    result_set_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    source_skill TEXT NOT NULL DEFAULT '',
    item_index INTEGER NOT NULL DEFAULT 0,
    subject_id TEXT NOT NULL DEFAULT '',
    readiness TEXT NOT NULL DEFAULT '',
    score REAL,
    related_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    item_json TEXT NOT NULL DEFAULT '{}',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    generated_at_utc TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_result_set
ON analysis_result_items(result_set_id, item_index, item_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_round_kind
ON analysis_result_items(run_id, round_id, analysis_kind, item_index, item_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_items_subject
ON analysis_result_items(subject_id, analysis_kind, readiness);

CREATE TABLE IF NOT EXISTS analysis_result_lineage (
    lineage_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    analysis_kind TEXT NOT NULL,
    result_set_id TEXT NOT NULL,
    item_id TEXT NOT NULL DEFAULT '',
    lineage_scope TEXT NOT NULL DEFAULT '',
    lineage_type TEXT NOT NULL DEFAULT '',
    relation TEXT NOT NULL DEFAULT '',
    value_text TEXT NOT NULL DEFAULT '',
    artifact_path TEXT NOT NULL DEFAULT '',
    record_locator TEXT NOT NULL DEFAULT '',
    source_analysis_kind TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_analysis_result_lineage_result_set
ON analysis_result_lineage(result_set_id, item_id, lineage_scope, lineage_type, relation, lineage_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_lineage_value
ON analysis_result_lineage(value_text, lineage_type, result_set_id);
CREATE INDEX IF NOT EXISTS idx_analysis_result_lineage_artifact
ON analysis_result_lineage(artifact_path, lineage_type, result_set_id, item_id);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def decode_json(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "signal_plane.sqlite"


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return default_db_path(run_dir)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str = "") -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    return connection, file_path


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


def analysis_config(analysis_kind: str) -> dict[str, Any]:
    config = ANALYSIS_KIND_CONFIGS.get(maybe_text(analysis_kind))
    if config is None:
        raise ValueError(f"Unsupported analysis kind: {analysis_kind}")
    return config


def _config_list(config: dict[str, Any], field_name: str) -> list[str]:
    values = config.get(field_name)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


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


def deduped_lineage_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    return contract, deduped_lineage_entries(lineage_entries)


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


def sync_analysis_result_set(
    run_dir: str | Path,
    *,
    analysis_kind: str,
    expected_run_id: str = "",
    round_id: str = "",
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
                        score,
                        related_ids_json,
                        evidence_refs_json,
                        item_json,
                        artifact_path,
                        record_locator,
                        generated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def sync_evidence_coverage_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    coverage_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_EVIDENCE_COVERAGE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=coverage_path,
        db_path=db_path,
    )
    return {
        **result,
        "coverage_path": maybe_text(result.get("artifact_path")),
    }


def load_evidence_coverage_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    coverage_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_EVIDENCE_COVERAGE,
        artifact_path=coverage_path,
        db_path=db_path,
    )
    return {
        "coverage_wrapper": context.get("payload_wrapper", {}),
        "coverages": context.get("items", []),
        "coverage_count": int(context.get("item_count") or 0),
        "coverage_source": maybe_text(context.get("source")),
        "coverage_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "coverage_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }


def sync_claim_scope_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_SCOPE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_scope_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_scope_path": maybe_text(result.get("artifact_path")),
    }


def load_claim_scope_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_SCOPE,
        artifact_path=claim_scope_path,
        db_path=db_path,
    )
    return {
        "claim_scope_wrapper": context.get("payload_wrapper", {}),
        "claim_scopes": context.get("items", []),
        "claim_scope_count": int(context.get("item_count") or 0),
        "claim_scope_source": maybe_text(context.get("source")),
        "claim_scope_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_scope_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }


def sync_observation_scope_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    observation_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_SCOPE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=observation_scope_path,
        db_path=db_path,
    )
    return {
        **result,
        "observation_scope_path": maybe_text(result.get("artifact_path")),
    }


def load_observation_scope_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    observation_scope_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_SCOPE,
        artifact_path=observation_scope_path,
        db_path=db_path,
    )
    return {
        "observation_scope_wrapper": context.get("payload_wrapper", {}),
        "observation_scopes": context.get("items", []),
        "observation_scope_count": int(context.get("item_count") or 0),
        "observation_scope_source": maybe_text(context.get("source")),
        "observation_scope_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "observation_scope_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }


def sync_claim_observation_link_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    links_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_OBSERVATION_LINK,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=links_path,
        db_path=db_path,
    )
    return {
        **result,
        "links_path": maybe_text(result.get("artifact_path")),
    }


def load_claim_observation_link_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    links_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_OBSERVATION_LINK,
        artifact_path=links_path,
        db_path=db_path,
    )
    return {
        "links_wrapper": context.get("payload_wrapper", {}),
        "links": context.get("items", []),
        "link_count": int(context.get("item_count") or 0),
        "links_source": maybe_text(context.get("source")),
        "links_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "links_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }


def sync_claim_cluster_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_cluster_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_CLUSTER,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_cluster_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_cluster_path": maybe_text(result.get("artifact_path")),
    }


def load_claim_cluster_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_cluster_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_CLUSTER,
        artifact_path=claim_cluster_path,
        db_path=db_path,
    )
    return {
        "claim_cluster_wrapper": context.get("payload_wrapper", {}),
        "claim_clusters": context.get("items", []),
        "claim_cluster_count": int(context.get("item_count") or 0),
        "claim_cluster_source": maybe_text(context.get("source")),
        "claim_cluster_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_cluster_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }


def sync_merged_observation_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    merged_observations_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_MERGED_OBSERVATION,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=merged_observations_path,
        db_path=db_path,
    )
    return {
        **result,
        "merged_observations_path": maybe_text(result.get("artifact_path")),
    }


def load_merged_observation_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    merged_observations_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_MERGED_OBSERVATION,
        artifact_path=merged_observations_path,
        db_path=db_path,
    )
    return {
        "merged_observations_wrapper": context.get("payload_wrapper", {}),
        "merged_observations": context.get("items", []),
        "merged_observation_count": int(context.get("item_count") or 0),
        "merged_observation_source": maybe_text(context.get("source")),
        "merged_observations_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "merged_observations_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }


def sync_claim_candidate_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    claim_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_CLAIM_CANDIDATE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=claim_candidates_path,
        db_path=db_path,
    )
    return {
        **result,
        "claim_candidates_path": maybe_text(result.get("artifact_path")),
    }


def load_claim_candidate_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    claim_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_CLAIM_CANDIDATE,
        artifact_path=claim_candidates_path,
        db_path=db_path,
    )
    return {
        "claim_candidates_wrapper": context.get("payload_wrapper", {}),
        "claim_candidates": context.get("items", []),
        "claim_candidate_count": int(context.get("item_count") or 0),
        "claim_candidate_source": maybe_text(context.get("source")),
        "claim_candidates_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "claim_candidates_artifact_present": bool(context.get("artifact_present")),
        "warnings": context.get("warnings", []),
    }


def sync_observation_candidate_result_set(
    run_dir: str | Path,
    *,
    expected_run_id: str = "",
    round_id: str = "",
    observation_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    result = sync_analysis_result_set(
        run_dir,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_CANDIDATE,
        expected_run_id=expected_run_id,
        round_id=round_id,
        artifact_path=observation_candidates_path,
        db_path=db_path,
    )
    return {
        **result,
        "observation_candidates_path": maybe_text(result.get("artifact_path")),
    }


def load_observation_candidate_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    observation_candidates_path: str | Path = "",
    db_path: str = "",
) -> dict[str, Any]:
    context = load_analysis_result_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        analysis_kind=ANALYSIS_KIND_OBSERVATION_CANDIDATE,
        artifact_path=observation_candidates_path,
        db_path=db_path,
    )
    return {
        "observation_candidates_wrapper": context.get("payload_wrapper", {}),
        "observation_candidates": context.get("items", []),
        "observation_candidate_count": int(context.get("item_count") or 0),
        "observation_candidate_source": maybe_text(context.get("source")),
        "observation_candidates_file": maybe_text(context.get("artifact_path")),
        "db_path": maybe_text(context.get("db_path")),
        "analysis_sync": context.get("analysis_sync", {}),
        "result_contract": context.get("result_contract", empty_result_contract()),
        "observation_candidates_artifact_present": bool(
            context.get("artifact_present")
        ),
        "warnings": context.get("warnings", []),
    }

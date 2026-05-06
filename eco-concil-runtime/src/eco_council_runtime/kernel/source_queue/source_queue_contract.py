from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_SELECTION_ROLES = ("sociologist", "environmentalist")
SUPPORTED_ARTIFACT_CAPTURE_MODES = ("stdout-json", "stdout-text", "direct-file")
KNOWN_FETCH_SIDE_EFFECTS = (
    "reads-artifacts",
    "writes-artifacts",
    "reads-shared-state",
    "writes-shared-state",
    "network-external",
    "destructive-write",
)

def _source(
    *,
    role: str,
    family_id: str,
    family_label: str,
    layer_id: str,
    layer_label: str,
    tier: str,
    normalizer_skill: str,
    default_suffix: str = ".json",
    artifact_capture: str = "stdout-json",
    runtime_output_mode: str = "none",
    runtime_output_arg: str = "",
    runtime_default_args: list[str] | None = None,
    requires_anchor: bool = False,
    anchor_argument: str = "",
    anchor_source_skills: list[str] | None = None,
    auto_selectable: bool | None = None,
) -> dict[str, Any]:
    return {
        "role": role,
        "family_id": family_id,
        "family_label": family_label,
        "layer_id": layer_id,
        "layer_label": layer_label,
        "tier": tier,
        "normalizer_skill": normalizer_skill,
        "default_suffix": default_suffix,
        "artifact_capture": artifact_capture,
        "runtime_output_mode": runtime_output_mode,
        "runtime_output_arg": runtime_output_arg,
        "runtime_default_args": list(runtime_default_args or []),
        "requires_anchor": requires_anchor,
        "anchor_argument": anchor_argument,
        "anchor_source_skills": list(anchor_source_skills or []),
        "auto_selectable": bool(auto_selectable) if auto_selectable is not None else tier == "l1",
    }


SOURCE_CATALOG: dict[str, dict[str, Any]] = {
    "fetch-bluesky-cascade": _source(
        role="sociologist",
        family_id="bluesky",
        family_label="Bluesky",
        layer_id="posts",
        layer_label="Posts",
        tier="l1",
        normalizer_skill="normalize-bluesky-cascade-public-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-gdelt-doc-search": _source(
        role="sociologist",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="doc-search",
        layer_label="Doc Search",
        tier="l1",
        normalizer_skill="normalize-gdelt-doc-public-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-gdelt-events": _source(
        role="sociologist",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="events",
        layer_label="Events Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-events-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-gdelt-mentions": _source(
        role="sociologist",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="mentions",
        layer_label="Mentions Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-mentions-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-gdelt-gkg": _source(
        role="sociologist",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="gkg",
        layer_label="GKG Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-gkg-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-youtube-video-search": _source(
        role="sociologist",
        family_id="youtube",
        family_label="YouTube",
        layer_id="video-search",
        layer_label="Video Search",
        tier="l1",
        normalizer_skill="normalize-youtube-video-public-signals",
        runtime_default_args=["--include-records", "--no-save-records"],
    ),
    "fetch-youtube-comments": _source(
        role="sociologist",
        family_id="youtube",
        family_label="YouTube",
        layer_id="comments",
        layer_label="Comments",
        tier="l2",
        normalizer_skill="normalize-youtube-comments-public-signals",
        runtime_default_args=["--include-records", "--no-save-records"],
        requires_anchor=True,
        anchor_argument="--video-ids-file",
        anchor_source_skills=["fetch-youtube-video-search"],
        auto_selectable=False,
    ),
    "fetch-regulationsgov-comments": _source(
        role="sociologist",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="comments",
        layer_label="Comment List",
        tier="l1",
        normalizer_skill="normalize-regulationsgov-comments-public-signals",
        runtime_default_args=["--include-records", "--no-save-response"],
    ),
    "fetch-regulationsgov-comment-detail": _source(
        role="sociologist",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="comment-detail",
        layer_label="Comment Detail",
        tier="l2",
        normalizer_skill="normalize-regulationsgov-comment-detail-public-signals",
        runtime_default_args=["--include-records", "--no-save-response"],
        requires_anchor=True,
        anchor_argument="--comment-ids-file",
        anchor_source_skills=["fetch-regulationsgov-comments"],
        auto_selectable=False,
    ),
    "fetch-airnow-hourly-observations": _source(
        role="environmentalist",
        family_id="airnow",
        family_label="AirNow",
        layer_id="hourly-observations",
        layer_label="Hourly Observations",
        tier="l1",
        normalizer_skill="normalize-airnow-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-openaq": _source(
        role="environmentalist",
        family_id="openaq",
        family_label="OpenAQ",
        layer_id="stations",
        layer_label="Stations",
        tier="l1",
        normalizer_skill="normalize-openaq-observation-signals",
    ),
    "fetch-open-meteo-historical": _source(
        role="environmentalist",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="historical",
        layer_label="Historical Weather",
        tier="l1",
        normalizer_skill="normalize-open-meteo-historical-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-open-meteo-air-quality": _source(
        role="environmentalist",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="air-quality",
        layer_label="Air Quality",
        tier="l1",
        normalizer_skill="normalize-open-meteo-air-quality-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-open-meteo-flood": _source(
        role="environmentalist",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="flood",
        layer_label="Flood",
        tier="l1",
        normalizer_skill="normalize-open-meteo-flood-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-usgs-water-iv": _source(
        role="environmentalist",
        family_id="usgs-water",
        family_label="USGS Water",
        layer_id="instantaneous-values",
        layer_label="Instantaneous Values",
        tier="l1",
        normalizer_skill="normalize-usgs-water-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-nasa-firms-fire": _source(
        role="environmentalist",
        family_id="nasa-firms",
        family_label="NASA FIRMS",
        layer_id="active-fire",
        layer_label="Active Fire",
        tier="l1",
        normalizer_skill="normalize-nasa-firms-fire-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y", "on"}
    return bool(value)


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


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return payload


def read_json_list(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError(f"Expected a JSON list of objects at {path}")
    return payload


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_snapshot(path: Path) -> dict[str, str]:
    return {
        "path": str(path.resolve()),
        "sha256": file_sha256(path),
    }


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return resolve_run_dir(run_dir) / "runtime" / f"source_selection_{role}_{round_id}.json"


def source_config(source_skill: str) -> dict[str, Any]:
    config = SOURCE_CATALOG.get(maybe_text(source_skill))
    if config is None:
        raise ValueError(f"Unsupported source_skill: {source_skill}")
    return config


def source_role(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("role"))


def source_normalizer_skill(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("normalizer_skill"))


def source_artifact_capture(source_skill: str) -> str:
    return normalize_artifact_capture(source_config(source_skill).get("artifact_capture"))


def source_runtime_output_mode(source_skill: str) -> str:
    mode = maybe_text(source_config(source_skill).get("runtime_output_mode")) or "none"
    if mode not in {"none", "file", "dir"}:
        raise ValueError(f"Unsupported runtime_output_mode for {source_skill}: {mode}")
    return mode


def source_runtime_output_arg(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("runtime_output_arg"))


def source_runtime_default_args(source_skill: str) -> list[str]:
    values = source_config(source_skill).get("runtime_default_args")
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_requires_anchor(source_skill: str) -> bool:
    return coerce_bool(source_config(source_skill).get("requires_anchor"))


def source_anchor_source_skills(source_skill: str) -> list[str]:
    values = source_config(source_skill).get("anchor_source_skills")
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_anchor_argument(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("anchor_argument"))


def source_auto_selectable(source_skill: str) -> bool:
    return coerce_bool(source_config(source_skill).get("auto_selectable"))


def normalize_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return unique_texts([maybe_text(value) for value in values if maybe_text(value)])


def normalize_artifact_capture(value: Any) -> str:
    capture_mode = maybe_text(value) or "stdout-json"
    if capture_mode not in SUPPORTED_ARTIFACT_CAPTURE_MODES:
        raise ValueError(f"Unsupported artifact_capture: {capture_mode}")
    return capture_mode


def normalize_fetch_execution_policy(payload: dict[str, Any]) -> dict[str, Any]:
    execution_policy = payload.get("fetch_execution_policy") if isinstance(payload.get("fetch_execution_policy"), dict) else {}
    timeout_seconds = coerce_float(execution_policy.get("timeout_seconds"))
    if timeout_seconds is None:
        timeout_seconds = coerce_float(payload.get("timeout_seconds"))
    retry_budget = coerce_int(execution_policy.get("retry_budget"))
    if retry_budget is None:
        retry_budget = coerce_int(payload.get("retry_budget"))
    retry_backoff_ms = coerce_int(execution_policy.get("retry_backoff_ms"))
    if retry_backoff_ms is None:
        retry_backoff_ms = coerce_int(payload.get("retry_backoff_ms"))
    return {
        "timeout_seconds": max(0.0, float(timeout_seconds if timeout_seconds is not None else 300.0)),
        "retry_budget": max(0, int(retry_budget if retry_budget is not None else 0)),
        "retry_backoff_ms": max(0, int(retry_backoff_ms if retry_backoff_ms is not None else 250)),
    }


def validate_fetch_side_effects(values: list[str], *, field_name: str) -> list[str]:
    invalid = [value for value in values if value not in KNOWN_FETCH_SIDE_EFFECTS]
    if invalid:
        raise ValueError(f"Unsupported fetch side effects in {field_name}: {', '.join(invalid)}")
    return unique_texts(values)


def normalize_fetch_declared_side_effects(payload: dict[str, Any]) -> list[str]:
    declared = normalize_text_list(payload.get("declared_side_effects"))
    validated = validate_fetch_side_effects(declared, field_name="declared_side_effects")
    return unique_texts(["writes-artifacts", *validated])


def normalize_fetch_requested_side_effect_approvals(payload: dict[str, Any], declared_side_effects: list[str]) -> list[str]:
    requested = validate_fetch_side_effects(
        normalize_text_list(payload.get("requested_side_effect_approvals")),
        field_name="requested_side_effect_approvals",
    )
    undeclared = [value for value in requested if value not in declared_side_effects]
    if undeclared:
        raise ValueError(
            "requested_side_effect_approvals must be a subset of declared_side_effects: "
            + ", ".join(undeclared)
        )
    return requested


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    base = [skill_name for skill_name, config in SOURCE_CATALOG.items() if maybe_text(config.get("role")) == role]
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    mission_allowlist = mission.get("allowed_sources_by_role") if isinstance(mission.get("allowed_sources_by_role"), dict) else {}
    configured = mission_allowlist.get(role)
    if configured is None and isinstance(governance.get("allowed_sources_by_role"), dict):
        configured = governance["allowed_sources_by_role"].get(role)
    if isinstance(configured, list):
        requested = {maybe_text(item) for item in configured if maybe_text(item)}
        return [skill_name for skill_name in base if skill_name in requested]
    return base


def effective_constraints(mission: dict[str, Any]) -> dict[str, int]:
    defaults = {
        "max_selected_sources_per_role": 4,
        "max_source_steps_per_round": 8,
    }
    constraints = mission.get("constraints") if isinstance(mission.get("constraints"), dict) else {}
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    for key in tuple(defaults):
        value = governance.get(key)
        if value in (None, ""):
            value = constraints.get(key)
        coerced = coerce_int(value)
        if coerced is not None:
            defaults[key] = coerced
    return defaults


def role_source_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    approved_layers_payload = governance.get("approved_layers") if isinstance(governance.get("approved_layers"), list) else []
    families: dict[str, dict[str, Any]] = {}
    for source_skill, config in SOURCE_CATALOG.items():
        if maybe_text(config.get("role")) != role:
            continue
        family_id = maybe_text(config.get("family_id"))
        family = families.setdefault(
            family_id,
            {
                "family_id": family_id,
                "label": maybe_text(config.get("family_label")),
                "role": role,
                "skills": [],
                "_layers": {},
            },
        )
        family["skills"].append(source_skill)
        layer_id = maybe_text(config.get("layer_id"))
        tier = maybe_text(config.get("tier")) or "l1"
        layer_lookup = family.setdefault("_layers", {})
        if not isinstance(layer_lookup, dict):
            layer_lookup = {}
            family["_layers"] = layer_lookup
        layer = layer_lookup.setdefault(
            layer_id,
            {
                "layer_id": layer_id,
                "label": maybe_text(config.get("layer_label")),
                "tier": tier,
                "skills": [],
                "max_selected_skills": 0,
                "requires_anchor": coerce_bool(config.get("requires_anchor")),
                "anchor_source_skills": [],
                "auto_selectable": coerce_bool(config.get("auto_selectable")) if "auto_selectable" in config else tier == "l1",
            },
        )
        if isinstance(layer, dict):
            layer_skills = layer.setdefault("skills", [])
            if isinstance(layer_skills, list):
                layer_skills.append(source_skill)
            anchor_skills = layer.setdefault("anchor_source_skills", [])
            if isinstance(anchor_skills, list):
                anchor_skills.extend(source_anchor_source_skills(source_skill))
    for family in families.values():
        family["skills"] = unique_texts(family.get("skills", []))
        layer_lookup = family.pop("_layers", {})
        layers = layer_lookup.values() if isinstance(layer_lookup, dict) else []
        finalized_layers: list[dict[str, Any]] = []
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            layer["skills"] = unique_texts(layer.get("skills", []))
            layer["anchor_source_skills"] = unique_texts(layer.get("anchor_source_skills", []))
            layer["max_selected_skills"] = len(layer["skills"])
            finalized_layers.append(layer)
        family["layers"] = sorted(
            finalized_layers,
            key=lambda item: (0 if maybe_text(item.get("tier")) == "l1" else 1, maybe_text(item.get("layer_id"))),
        )
    family_ids = {maybe_text(item.get("family_id")) for item in families.values() if maybe_text(item.get("family_id"))}
    approved_layers = [
        item
        for item in approved_layers_payload
        if isinstance(item, dict)
        and maybe_text(item.get("family_id")) in family_ids
        and maybe_text(item.get("layer_id"))
    ]
    return {
        "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
        "allow_cross_round_anchors": coerce_bool(governance.get("allow_cross_round_anchors")),
        "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
        "max_active_families_per_role": coerce_int(governance.get("max_active_families_per_role")),
        "max_non_entry_layers_per_role": coerce_int(governance.get("max_non_entry_layers_per_role")),
        "approved_layers": approved_layers,
        "families": sorted(families.values(), key=lambda item: maybe_text(item.get("family_id"))),
    }


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    return {
        "policy_profile": maybe_text(mission.get("policy_profile")) or "standard",
        "effective_constraints": effective_constraints(mission),
        "source_governance": {
            "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
            "allow_cross_round_anchors": coerce_bool(governance.get("allow_cross_round_anchors")),
            "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
        },
    }


def normalize_artifact_imports(mission: dict[str, Any]) -> list[dict[str, Any]]:
    imports = mission.get("artifact_imports") if isinstance(mission.get("artifact_imports"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in imports:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        config = source_config(source_skill)
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "artifact_path": maybe_text(item.get("artifact_path")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


def normalize_source_requests(mission: dict[str, Any]) -> list[dict[str, Any]]:
    requests = mission.get("source_requests") if isinstance(mission.get("source_requests"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in requests:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        config = source_config(source_skill)
        fetch_argv = item.get("fetch_argv") if isinstance(item.get("fetch_argv"), list) else []
        declared_side_effects = normalize_fetch_declared_side_effects(item)
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "artifact_capture": normalize_artifact_capture(item.get("artifact_capture") or config.get("artifact_capture")),
                "artifact_path": maybe_text(item.get("artifact_path")),
                "fetch_cwd": maybe_text(item.get("fetch_cwd")),
                "fetch_argv": [maybe_text(arg) for arg in fetch_argv if maybe_text(arg)],
                "fetch_execution_policy": normalize_fetch_execution_policy(item),
                "declared_side_effects": declared_side_effects,
                "requested_side_effect_approvals": normalize_fetch_requested_side_effect_approvals(item, declared_side_effects),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


__all__ = [
    "coerce_bool",
    "coerce_float",
    "coerce_int",
    "KNOWN_FETCH_SIDE_EFFECTS",
    "SOURCE_CATALOG",
    "SOURCE_SELECTION_ROLES",
    "SUPPORTED_ARTIFACT_CAPTURE_MODES",
    "allowed_sources_for_role",
    "effective_constraints",
    "file_sha256",
    "file_snapshot",
    "maybe_text",
    "normalize_artifact_capture",
    "normalize_artifact_imports",
    "normalize_fetch_execution_policy",
    "normalize_fetch_declared_side_effects",
    "normalize_fetch_requested_side_effect_approvals",
    "normalize_source_requests",
    "normalize_text_list",
    "policy_profile_summary",
    "read_json_list",
    "read_json_object",
    "resolve_run_dir",
    "role_source_governance",
    "source_anchor_argument",
    "source_anchor_source_skills",
    "source_artifact_capture",
    "source_auto_selectable",
    "source_config",
    "source_normalizer_skill",
    "source_role",
    "source_requires_anchor",
    "source_runtime_default_args",
    "source_runtime_output_arg",
    "source_runtime_output_mode",
    "source_selection_path",
    "stable_hash",
    "unique_texts",
    "utc_now_iso",
    "write_json_file",
]

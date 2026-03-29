from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_SELECTION_ROLES = ("sociologist", "environmentalist")

SOURCE_CATALOG: dict[str, dict[str, str]] = {
    "bluesky-cascade-fetch": {
        "role": "sociologist",
        "family_id": "bluesky",
        "family_label": "Bluesky",
        "layer_id": "posts",
        "layer_label": "Posts",
        "tier": "l1",
        "normalizer_skill": "eco-normalize-bluesky-cascade-public-signals",
        "default_suffix": ".json",
    },
    "gdelt-doc-search": {
        "role": "sociologist",
        "family_id": "gdelt",
        "family_label": "GDELT",
        "layer_id": "recon",
        "layer_label": "Recon",
        "tier": "l1",
        "normalizer_skill": "eco-normalize-gdelt-doc-public-signals",
        "default_suffix": ".json",
    },
    "youtube-video-search": {
        "role": "sociologist",
        "family_id": "youtube",
        "family_label": "YouTube",
        "layer_id": "video-search",
        "layer_label": "Video Search",
        "tier": "l1",
        "normalizer_skill": "eco-normalize-youtube-video-public-signals",
        "default_suffix": ".json",
    },
    "airnow-hourly-obs-fetch": {
        "role": "environmentalist",
        "family_id": "airnow",
        "family_label": "AirNow",
        "layer_id": "hourly-observations",
        "layer_label": "Hourly Observations",
        "tier": "l1",
        "normalizer_skill": "eco-normalize-airnow-observation-signals",
        "default_suffix": ".json",
    },
    "openaq-data-fetch": {
        "role": "environmentalist",
        "family_id": "openaq",
        "family_label": "OpenAQ",
        "layer_id": "stations",
        "layer_label": "Stations",
        "tier": "l1",
        "normalizer_skill": "eco-normalize-openaq-observation-signals",
        "default_suffix": ".json",
    },
    "open-meteo-historical-fetch": {
        "role": "environmentalist",
        "family_id": "open-meteo",
        "family_label": "Open-Meteo",
        "layer_id": "historical",
        "layer_label": "Historical",
        "tier": "l1",
        "normalizer_skill": "eco-normalize-open-meteo-historical-signals",
        "default_suffix": ".json",
    },
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def source_config(source_skill: str) -> dict[str, str]:
    config = SOURCE_CATALOG.get(maybe_text(source_skill))
    if config is None:
        raise ValueError(f"Unsupported source_skill: {source_skill}")
    return config


def source_role(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("role"))


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
        try:
            if value not in (None, ""):
                defaults[key] = int(value)
        except (TypeError, ValueError):
            continue
    return defaults


def role_source_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    approved_layers_payload = governance.get("approved_layers") if isinstance(governance.get("approved_layers"), list) else []
    approved_layers = [
        item
        for item in approved_layers_payload
        if isinstance(item, dict)
        and maybe_text(item.get("family_id"))
        and maybe_text(item.get("layer_id"))
    ]
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
                "layers": [],
            },
        )
        family["skills"].append(source_skill)
        family["layers"].append(
            {
                "layer_id": maybe_text(config.get("layer_id")),
                "label": maybe_text(config.get("layer_label")),
                "tier": maybe_text(config.get("tier")) or "l1",
                "skills": [source_skill],
                "auto_selectable": True,
            }
        )
    for family in families.values():
        family["skills"] = unique_texts(family.get("skills", []))
        family["layers"] = sorted(family.get("layers", []), key=lambda item: maybe_text(item.get("layer_id")))
    return {
        "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
        "allow_cross_round_anchors": bool(governance.get("allow_cross_round_anchors")),
        "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
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
            "allow_cross_round_anchors": bool(governance.get("allow_cross_round_anchors")),
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
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "artifact_capture": maybe_text(item.get("artifact_capture")) or "stdout-json",
                "artifact_path": maybe_text(item.get("artifact_path")),
                "fetch_cwd": maybe_text(item.get("fetch_cwd")),
                "fetch_argv": [maybe_text(arg) for arg in fetch_argv if maybe_text(arg)],
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


__all__ = [
    "SOURCE_CATALOG",
    "SOURCE_SELECTION_ROLES",
    "allowed_sources_for_role",
    "effective_constraints",
    "file_sha256",
    "file_snapshot",
    "maybe_text",
    "normalize_artifact_imports",
    "normalize_source_requests",
    "policy_profile_summary",
    "read_json_list",
    "read_json_object",
    "resolve_run_dir",
    "role_source_governance",
    "source_config",
    "source_role",
    "source_selection_path",
    "stable_hash",
    "unique_texts",
    "utc_now_iso",
    "write_json_file",
]
"""Preset discovery and scenario loading for deterministic simulations."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from .common import (
    MODE_VALUES,
    PRESET_DIR,
    SCENARIO_KIND,
    SCHEMA_VERSION,
    SUPPORTED_CLAIM_TYPES,
    ensure_object,
    infer_claim_type,
    maybe_text,
    read_json,
    region_label,
)


def preset_paths() -> list[Path]:
    if not PRESET_DIR.exists():
        return []
    return sorted(path for path in PRESET_DIR.iterdir() if path.is_file() and path.suffix.lower() == ".json")


def find_preset_path(name: str) -> Path:
    normalized = maybe_text(name)
    for path in preset_paths():
        payload = read_json(path)
        scenario_id = maybe_text(payload.get("scenario_id"))
        if path.stem == normalized or scenario_id == normalized:
            return path
    raise ValueError(f"Unknown preset: {name}")


def load_scenario(
    *,
    args: argparse.Namespace,
    mission: dict[str, Any],
    tasks: list[dict[str, Any]],
    steps: list[dict[str, Any]],
) -> tuple[dict[str, Any], str]:
    source = "auto"
    if args.scenario_input:
        scenario = ensure_object(read_json(Path(args.scenario_input).expanduser().resolve()), "scenario")
        source = str(Path(args.scenario_input).expanduser().resolve())
    elif args.preset:
        preset_path = find_preset_path(args.preset)
        scenario = ensure_object(read_json(preset_path), "preset")
        source = str(preset_path)
    else:
        selected_sources = [maybe_text(step.get("source_skill")) for step in steps if maybe_text(step.get("source_skill"))]
        combined_text = " ".join(
            [
                maybe_text(mission.get("topic")),
                maybe_text(mission.get("objective")),
                " ".join(maybe_text(item) for item in mission.get("hypotheses", []) if maybe_text(item))
                if isinstance(mission.get("hypotheses"), list)
                else "",
                " ".join(maybe_text(task.get("objective")) for task in tasks if isinstance(task, dict)),
            ]
        )
        claim_type = maybe_text(args.claim_type) or infer_claim_type(combined_text, selected_sources)
        scenario = {
            "scenario_kind": SCENARIO_KIND,
            "schema_version": SCHEMA_VERSION,
            "scenario_id": f"auto-{claim_type}-{args.mode}",
            "description": f"Auto-generated {args.mode} scenario for {claim_type}.",
            "claim_type": claim_type,
            "mode": args.mode,
            "seed": args.seed if args.seed is not None else 7,
            "public_topic": maybe_text(mission.get("topic")) or claim_type,
            "place_label": region_label(mission),
            "source_modes": {},
            "source_overrides": {},
            "fault_profile": {
                "empty_sources": [],
                "degrade_sources": [],
                "time_shift_hours": 0,
                "coordinate_offset_degrees": 0.0,
            },
        }
    scenario.setdefault("scenario_kind", SCENARIO_KIND)
    scenario.setdefault("schema_version", SCHEMA_VERSION)
    scenario.setdefault("scenario_id", "unnamed-scenario")
    scenario.setdefault("description", "")
    scenario.setdefault("claim_type", infer_claim_type(maybe_text(mission.get("objective")), []))
    scenario.setdefault("mode", args.mode)
    scenario.setdefault("seed", args.seed if args.seed is not None else 7)
    scenario.setdefault("public_topic", maybe_text(mission.get("topic")))
    scenario.setdefault("place_label", region_label(mission))
    scenario.setdefault("source_modes", {})
    scenario.setdefault("source_overrides", {})
    scenario.setdefault("fault_profile", {})
    if args.seed is not None:
        scenario["seed"] = args.seed
    if maybe_text(scenario.get("scenario_kind")) != SCENARIO_KIND:
        raise ValueError(f"scenario_kind must be {SCENARIO_KIND!r}")
    claim_type = maybe_text(scenario.get("claim_type"))
    if claim_type not in SUPPORTED_CLAIM_TYPES:
        raise ValueError(f"Unsupported claim_type: {claim_type}")
    mode = maybe_text(scenario.get("mode"))
    if mode not in MODE_VALUES:
        raise ValueError(f"Unsupported mode: {mode}")
    return scenario, source


__all__ = [
    "find_preset_path",
    "load_scenario",
    "preset_paths",
]

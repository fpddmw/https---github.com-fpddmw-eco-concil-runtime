"""Shared run-directory and mission path helpers for eco-council runtime flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists
from eco_council_runtime.domain.rounds import round_dir_name, round_id_from_dirname, strict_round_sort_key
from eco_council_runtime.domain.text import maybe_text


def round_dir(run_dir: Path, round_id: str) -> Path:
    return run_dir / round_dir_name(round_id)


def mission_path(run_dir: Path) -> Path:
    return run_dir / "mission.json"


def load_mission(run_dir: Path) -> dict[str, Any]:
    payload = load_json_if_exists(mission_path(run_dir))
    if not isinstance(payload, dict):
        raise ValueError(f"Mission payload is not a JSON object: {mission_path(run_dir)}")
    return payload


def current_run_id(run_dir: Path) -> str:
    return maybe_text(load_mission(run_dir).get("run_id"))


def discover_round_ids(run_dir: Path) -> list[str]:
    if not run_dir.exists():
        return []
    round_ids = {
        round_id
        for child in run_dir.iterdir()
        if child.is_dir()
        for round_id in [round_id_from_dirname(child.name)]
        if round_id is not None
    }
    return sorted(round_ids, key=strict_round_sort_key)


def latest_round_id(run_dir: Path) -> str:
    round_ids = discover_round_ids(run_dir)
    if not round_ids:
        raise ValueError(f"No round_* directories found in {run_dir}")
    return round_ids[-1]


def prior_round_ids(run_dir: Path, round_id: str) -> list[str]:
    round_ids = discover_round_ids(run_dir)
    if round_id not in round_ids:
        return round_ids
    return round_ids[: round_ids.index(round_id)]

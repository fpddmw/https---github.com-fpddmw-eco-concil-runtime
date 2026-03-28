"""Shared runtime snapshot helpers for archive and corpus imports."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists, read_json
from eco_council_runtime.adapters.run_paths import discover_round_ids, load_mission
from eco_council_runtime.controller.paths import (
    fetch_execution_path,
    shared_claims_path,
    shared_evidence_cards_path,
    shared_observations_path,
    supervisor_state_path,
)
from eco_council_runtime.controller.policy import effective_constraints
from eco_council_runtime.controller.run_summary import collect_round_summary
from eco_council_runtime.domain.contract_bridge import contract_call
from eco_council_runtime.domain.text import maybe_text


def load_state(run_dir: Path) -> dict[str, Any]:
    path = supervisor_state_path(run_dir)
    if not path.exists():
        raise ValueError(f"Supervisor state not found: {path}")
    payload = read_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"Supervisor state is not a JSON object: {path}")
    return payload


def state_for_run(run_dir: Path) -> dict[str, Any]:
    if supervisor_state_path(run_dir).exists():
        return load_state(run_dir)
    round_ids = discover_round_ids(run_dir)
    return {
        "current_round_id": round_ids[-1] if round_ids else "",
        "stage": "",
    }


def load_fetch_execution(run_dir: Path, round_id: str) -> dict[str, Any]:
    payload = load_json_if_exists(fetch_execution_path(run_dir, round_id))
    return payload if isinstance(payload, dict) else {}


def round_payload_lists(run_dir: Path, round_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    claims = load_json_if_exists(shared_claims_path(run_dir, round_id))
    observations = load_json_if_exists(shared_observations_path(run_dir, round_id))
    evidence = load_json_if_exists(shared_evidence_cards_path(run_dir, round_id))
    return (
        claims if isinstance(claims, list) else [],
        observations if isinstance(observations, list) else [],
        evidence if isinstance(evidence, list) else [],
    )


def mission_constraints(mission: dict[str, Any]) -> dict[str, Any]:
    return effective_constraints(mission)


def source_governance_payload(mission: dict[str, Any]) -> dict[str, Any]:
    payload = contract_call("source_governance", mission)
    return payload if isinstance(payload, dict) else {}


def collect_run_snapshot(run_dir: Path) -> dict[str, Any]:
    mission = load_mission(run_dir)
    state = state_for_run(run_dir)
    round_ids = discover_round_ids(run_dir)
    round_summaries = [collect_round_summary(run_dir, state, round_id) for round_id in round_ids]
    current_round_id = maybe_text(state.get("current_round_id")) or (round_ids[-1] if round_ids else "")
    current_summary = next((item for item in round_summaries if item.get("round_id") == current_round_id), None)
    if current_summary is None and round_summaries:
        current_summary = round_summaries[-1]

    latest_decision_round = next(
        (item for item in reversed(round_summaries) if isinstance(item.get("decision"), dict)),
        None,
    )
    latest_decision = latest_decision_round.get("decision") if isinstance(latest_decision_round, dict) else None

    return {
        "mission": mission,
        "state": state,
        "round_ids": round_ids,
        "round_summaries": round_summaries,
        "current_summary": current_summary if isinstance(current_summary, dict) else {},
        "latest_decision_round": latest_decision_round if isinstance(latest_decision_round, dict) else {},
        "latest_decision": latest_decision if isinstance(latest_decision, dict) else {},
    }

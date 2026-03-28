from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.planning.next_round import (  # noqa: E402
    build_decision_override_requests,
    build_next_round_tasks,
    collect_unresolved_anchor_refs,
    combine_recommendations,
)


def mission_run_id(mission: dict[str, Any]) -> str:
    return str(mission["run_id"])


def mission_constraints(mission: dict[str, Any]) -> dict[str, int]:
    constraints = mission.get("constraints", {})
    if not isinstance(constraints, dict):
        return {}
    return {str(key): int(value) for key, value in constraints.items()}


def expected_output_kinds_for_role(role: str) -> list[str]:
    return [f"{role}-output"]


def current_round_number(round_id: str) -> int | None:
    try:
        return int(round_id.split("-")[-1])
    except (TypeError, ValueError):
        return None


def validate_payload(kind: str, payload: Any) -> None:
    _ = (kind, payload)


class NextRoundPlanningTests(unittest.TestCase):
    def test_combine_recommendations_dedupes_against_missing_type_templates(self) -> None:
        reports = [
            {
                "recommended_next_actions": [
                    {
                        "assigned_role": "environmentalist",
                        "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                        "reason": "Need direct station corroboration.",
                    }
                ]
            }
        ]

        recommendations = combine_recommendations(
            reports=reports,
            missing_types=["station-air-quality", "fire-detection"],
        )

        self.assertEqual(2, len(recommendations))
        self.assertEqual("Need direct station corroboration.", recommendations[0]["reason"])
        self.assertEqual(
            {
                "assigned_role": "environmentalist",
                "objective": "Fetch fire-detection evidence aligned with the mission window and geometry.",
                "reason": "Wildfire-related claims still lack direct fire-detection corroboration.",
            },
            recommendations[1],
        )

    def test_build_next_round_tasks_applies_cap_and_preserves_anchor_refs(self) -> None:
        mission = {
            "run_id": "run-001",
            "region": {"geometry": {"type": "Point", "coordinates": [0.0, 0.0]}},
            "window": {"start_utc": "2026-03-25T00:00:00Z", "end_utc": "2026-03-26T00:00:00Z"},
            "constraints": {"max_tasks_per_round": 1},
        }
        recommendations = [
            {
                "assigned_role": "environmentalist",
                "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                "reason": "Need station corroboration.",
            },
            {
                "assigned_role": "sociologist",
                "objective": "Collect more independent public-discussion evidence for the same mission window.",
                "reason": "Need broader coverage.",
            },
        ]

        tasks, task_plan_info = build_next_round_tasks(
            schema_version="1.0.0",
            mission=mission,
            current_round_id="round-01",
            next_round_id="round-02",
            recommendations=recommendations,
            focus_claim_ids=["claim-1", "claim-2"],
            anchor_refs=["round-01:claim:claim-1"],
            mission_run_id=mission_run_id,
            mission_constraints=mission_constraints,
            expected_output_kinds_for_role=expected_output_kinds_for_role,
            validate_payload=validate_payload,
        )

        self.assertEqual(1, len(tasks))
        self.assertTrue(task_plan_info["truncated_by_cap"])
        self.assertEqual("task-environmentalist-round-02-01", tasks[0]["task_id"])
        requirement = tasks[0]["inputs"]["evidence_requirements"][0]
        self.assertEqual(["round-01:claim:claim-1"], requirement["anchor_refs"])
        self.assertEqual(["claim-1", "claim-2"], requirement["focus_claim_ids"])

    def test_build_decision_override_requests_emits_task_and_round_cap_requests(self) -> None:
        mission = {
            "run_id": "run-001",
            "constraints": {
                "max_tasks_per_round": 1,
                "max_rounds": 2,
            },
        }

        requests = build_decision_override_requests(
            schema_version="1.0.0",
            mission=mission,
            round_id="round-02",
            next_round_id="round-03",
            focus_claim_ids=["claim-1"],
            anchor_refs=["round-02:claim:claim-1"],
            task_plan_info={
                "truncated_by_cap": True,
                "max_tasks_per_round": 1,
                "candidate_count": 3,
            },
            next_round_requested_but_blocked_by_max_rounds=True,
            mission_run_id=mission_run_id,
            mission_constraints=mission_constraints,
            current_round_number=current_round_number,
            validate_payload=validate_payload,
        )

        self.assertEqual(2, len(requests))
        self.assertEqual(
            ["constraints.max_tasks_per_round", "constraints.max_rounds"],
            [request["target_path"] for request in requests],
        )
        self.assertEqual([3, 3], [request["requested_value"] for request in requests])

    def test_collect_unresolved_anchor_refs_merges_state_sources(self) -> None:
        state = {
            "round_id": "round-02",
            "remands_open": [{"entity_kind": "claim", "entity_id": "claim-1"}],
            "isolated_active": [{"entity_kind": "observation", "entity_id": "obs-1"}],
            "matching_result": {
                "unmatched_claim_ids": ["claim-2"],
                "unmatched_observation_ids": ["obs-2"],
            },
            "cards_active": [
                {"verdict": "mixed", "claim_id": "claim-1", "evidence_id": "card-1"},
                {"verdict": "supports", "claim_id": "claim-3", "evidence_id": "card-2"},
            ],
        }

        focus_claim_ids, anchor_refs = collect_unresolved_anchor_refs(state)

        self.assertEqual(["claim-1", "claim-2"], focus_claim_ids)
        self.assertEqual(
            [
                "round-02:claim:claim-1",
                "round-02:observation:obs-1",
                "round-02:claim:claim-2",
                "round-02:observation:obs-2",
                "round-02:card:card-1",
            ],
            anchor_refs,
        )


if __name__ == "__main__":
    unittest.main()

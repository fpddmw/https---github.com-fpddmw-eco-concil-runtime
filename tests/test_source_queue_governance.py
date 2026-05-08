from __future__ import annotations

import importlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
sys.path.insert(0, str(RUNTIME_SRC))


def load_modules():
    selection_module = importlib.import_module("eco_council_runtime.kernel.source_queue.source_queue_selection")
    contract_module = importlib.import_module("eco_council_runtime.kernel.source_queue.source_queue_contract")
    return selection_module, contract_module


def sociologist_tasks(round_id: str) -> list[dict[str, object]]:
    return [
        {
            "task_id": f"task-sociologist-{round_id}-01",
            "assigned_role": "sociologist",
            "inputs": {
                "evidence_requirements": [
                    {
                        "requirement_id": f"req-sociologist-{round_id}-01",
                        "summary": "Collect public-signal evidence for the current round.",
                    }
                ]
            },
        }
    ]


def mission_with_public_import() -> dict[str, object]:
    return {
        "allowed_sources_by_role": {
            "sociologist": [
                "fetch-youtube-video-search",
                "fetch-gdelt-doc-search",
                "fetch-bluesky-cascade",
            ]
        },
        "artifact_imports": [
            {
                "source_skill": "fetch-youtube-video-search",
                "artifact_path": "/tmp/youtube.json",
                "query_text": "nyc smoke wildfire",
            }
        ],
        "source_governance": {
            "max_selected_sources_per_role": 2,
            "max_active_families_per_role": 1,
            "max_non_entry_layers_per_role": 0,
        },
    }


class SourceQueueGovernanceTests(unittest.TestCase):
    def test_role_source_governance_surfaces_family_limits(self) -> None:
        _, contract_module = load_modules()
        governance = contract_module.role_source_governance(mission_with_public_import(), "sociologist")

        self.assertEqual(2, governance["max_selected_sources_per_role"])
        self.assertEqual(1, governance["max_active_families_per_role"])
        self.assertEqual(0, governance["max_non_entry_layers_per_role"])
        self.assertTrue(any(family["family_id"] == "youtube" for family in governance["families"]))

    def test_validate_source_selection_rejects_selected_source_mismatch(self) -> None:
        selection_module, _ = load_modules()
        mission = mission_with_public_import()
        payload = selection_module.build_source_selection(
            mission=mission,
            tasks=sociologist_tasks("round-governance-001"),
            run_id="run-governance-001",
            round_id="round-governance-001",
            role="sociologist",
        )

        payload["selected_sources"] = ["fetch-gdelt-doc-search"]

        with self.assertRaisesRegex(ValueError, "selected_sources does not match selected family layers"):
            selection_module.validate_source_selection_payload(mission=mission, role="sociologist", source_selection=payload)

    def test_validate_source_selection_rejects_family_selected_flag_mismatch(self) -> None:
        selection_module, _ = load_modules()
        mission = mission_with_public_import()
        payload = selection_module.build_source_selection(
            mission=mission,
            tasks=sociologist_tasks("round-governance-002"),
            run_id="run-governance-002",
            round_id="round-governance-002",
            role="sociologist",
        )

        youtube_family = next(
            family_plan
            for family_plan in payload["family_plans"]
            if isinstance(family_plan, dict) and family_plan.get("family_id") == "youtube"
        )
        youtube_family["selected"] = False

        with self.assertRaisesRegex(ValueError, "selected flag must match selected layers"):
            selection_module.validate_source_selection_payload(mission=mission, role="sociologist", source_selection=payload)

    def test_build_fetch_plan_enforces_max_source_steps_per_round(self) -> None:
        selection_module, _ = load_modules()
        planner_module = importlib.import_module("eco_council_runtime.kernel.source_queue.source_queue_planner")

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            youtube_path = Path(tmpdir) / "youtube.json"
            bluesky_path = Path(tmpdir) / "bluesky.json"
            youtube_path.write_text(json.dumps([], ensure_ascii=True), encoding="utf-8")
            bluesky_path.write_text(json.dumps({}, ensure_ascii=True), encoding="utf-8")

            mission = {
                "allowed_sources_by_role": {
                    "sociologist": ["fetch-youtube-video-search", "fetch-bluesky-cascade"],
                },
                "artifact_imports": [
                    {
                        "source_skill": "fetch-youtube-video-search",
                        "artifact_path": str(youtube_path),
                        "query_text": "nyc smoke wildfire",
                    },
                    {
                        "source_skill": "fetch-bluesky-cascade",
                        "artifact_path": str(bluesky_path),
                    },
                ],
                "source_governance": {
                    "max_selected_sources_per_role": 2,
                    "max_active_families_per_role": 2,
                    "max_non_entry_layers_per_role": 0,
                },
                "constraints": {
                    "max_source_steps_per_round": 1,
                },
            }
            tasks = sociologist_tasks("round-governance-003")
            (run_dir / "investigation").mkdir(parents=True, exist_ok=True)
            (run_dir / "mission.json").write_text(
                json.dumps(mission, ensure_ascii=True, sort_keys=True),
                encoding="utf-8",
            )
            (run_dir / "investigation" / "round_tasks_round-governance-003.json").write_text(
                json.dumps(tasks, ensure_ascii=True, sort_keys=True),
                encoding="utf-8",
            )

            selections = selection_module.build_source_selections(
                run_dir=run_dir,
                mission=mission,
                tasks=tasks,
                run_id="run-governance-003",
                round_id="round-governance-003",
            )

            with self.assertRaisesRegex(ValueError, "max_source_steps_per_round=1"):
                planner_module.build_fetch_plan(
                    run_dir=run_dir,
                    run_id="run-governance-003",
                    round_id="round-governance-003",
                    mission=mission,
                    tasks=tasks,
                    selections=selections,
                )

    def test_smoke_episode_intent_adds_origin_and_transport_sources(self) -> None:
        selection_module, contract_module = load_modules()
        mission = {
            "topic": "June 2023 New York City smoke episode",
            "objective": (
                "Investigate the smoke episode, candidate source regions, "
                "possible transport pathway, community impacts, and evidence-bounded recommendations."
            ),
            "source_governance": {"max_selected_sources_per_role": 4},
        }

        environmental = selection_module.build_source_selection(
            mission=mission,
            tasks=[],
            run_id="run-governance-004",
            round_id="round-governance-004",
            role="environmentalist",
        )
        sociologist = selection_module.build_source_selection(
            mission=mission,
            tasks=[],
            run_id="run-governance-004",
            round_id="round-governance-004",
            role="sociologist",
        )

        self.assertIn("fetch-nasa-firms-fire", environmental["selected_sources"])
        self.assertIn("fetch-open-meteo-air-quality", environmental["selected_sources"])
        self.assertIn("fetch-open-meteo-historical", environmental["selected_sources"])
        self.assertIn("fetch-gdelt-doc-search", sociologist["selected_sources"])
        self.assertIn(
            "spatiotemporal-relation-review",
            {
                item["requirement_type"]
                for item in contract_module.lane_evidence_requirements(
                    mission,
                    round_id="round-governance-004",
                    role="environmentalist",
                )
            },
        )

    def test_smoke_episode_verification_scope_records_origin_and_transport_lanes(self) -> None:
        _, contract_module = load_modules()
        mission = {
            "run_id": "run-governance-005",
            "topic": "June 2023 New York City smoke episode",
            "objective": "Investigate candidate source regions, transport pathway, public impacts, and handling recommendations.",
            "window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-10T00:00:00Z"},
            "region": {
                "label": "New York City, NY, United States",
                "geometry": {"type": "Point", "latitude": 40.7128, "longitude": -74.006},
            },
        }

        scope = contract_module.derive_verification_scope(mission)
        lane_ids = {item["lane_id"] for item in scope["required_evidence_lanes"]}

        self.assertEqual("mission-derived-candidate-source-review", scope["candidate_source_region_policy"])
        self.assertEqual("mission-derived-relation-review", scope["transport_verification_policy"])
        self.assertIn("fire-origin", lane_ids)
        self.assertIn("spatiotemporal-relation-review", lane_ids)
        self.assertEqual([], scope["required_source_skills"])
        self.assertIn("fetch-nasa-firms-fire", scope["candidate_source_skills"])

    def test_lane_required_sources_raise_effective_step_budget(self) -> None:
        selection_module, _ = load_modules()
        planner_module = importlib.import_module("eco_council_runtime.kernel.source_queue.source_queue_planner")

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_dir.mkdir(parents=True, exist_ok=True)
            round_id = "round-governance-006"
            mission = {
                "run_id": "run-governance-006",
                "topic": "June 2023 New York City smoke episode",
                "objective": "Investigate candidate source regions, transport pathway, public impacts, and handling recommendations.",
                "window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-10T00:00:00Z"},
                "region": {
                    "label": "New York City, NY, United States",
                    "geometry": {"type": "Point", "latitude": 40.7128, "longitude": -74.006},
                },
                "source_governance": {
                    "max_selected_sources_per_role": 4,
                    "max_source_steps_per_round": 1,
                },
                "source_requests": [
                    {"source_skill": "fetch-gdelt-doc-search", "fetch_argv": ["echo", "{}"]},
                    {"source_skill": "fetch-open-meteo-air-quality", "fetch_argv": ["echo", "{}"]},
                    {"source_skill": "fetch-open-meteo-historical", "fetch_argv": ["echo", "{}"]},
                    {"source_skill": "fetch-nasa-firms-fire", "fetch_argv": ["echo", "{}"]},
                ],
            }
            tasks = [
                *sociologist_tasks(round_id),
                {
                    "task_id": f"task-environmentalist-{round_id}-01",
                    "assigned_role": "environmentalist",
                    "inputs": {"evidence_requirements": []},
                },
            ]
            (run_dir / "investigation").mkdir(parents=True, exist_ok=True)
            (run_dir / "mission.json").write_text(
                json.dumps(mission, ensure_ascii=True, sort_keys=True),
                encoding="utf-8",
            )
            (run_dir / "investigation" / f"round_tasks_{round_id}.json").write_text(
                json.dumps(tasks, ensure_ascii=True, sort_keys=True),
                encoding="utf-8",
            )

            selections = selection_module.build_source_selections(
                run_dir=run_dir,
                mission=mission,
                tasks=tasks,
                run_id="run-governance-006",
                round_id=round_id,
            )
            planner_module.write_source_selections(run_dir, round_id, selections)
            plan, warnings = planner_module.build_fetch_plan(
                run_dir=run_dir,
                run_id="run-governance-006",
                round_id=round_id,
                mission=mission,
                tasks=tasks,
                selections=selections,
            )

        self.assertEqual(4, len(plan["steps"]))
        self.assertEqual("mission-derived-candidate-source-review", plan["verification_scope"]["candidate_source_region_policy"])
        self.assertEqual(1, plan["source_step_budget"]["configured_max_source_steps_per_round"])
        self.assertEqual(4, plan["source_step_budget"]["effective_max_source_steps_per_round"])
        self.assertTrue(any(item["code"] == "source-step-budget-raised-for-required-lanes" for item in warnings))


if __name__ == "__main__":
    unittest.main()

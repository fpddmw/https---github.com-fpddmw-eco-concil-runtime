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
    selection_module = importlib.import_module("eco_council_runtime.kernel.source_queue_selection")
    contract_module = importlib.import_module("eco_council_runtime.kernel.source_queue_contract")
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
        planner_module = importlib.import_module("eco_council_runtime.kernel.source_queue_planner")

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


if __name__ == "__main__":
    unittest.main()

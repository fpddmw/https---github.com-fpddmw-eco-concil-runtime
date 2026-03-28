from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.orchestration_prepare import (  # noqa: E402
    materialize_prepare_round_outputs,
    render_moderator_task_review_prompt,
    render_role_fetch_prompt,
)
from eco_council_runtime.contract import scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.paths import fetch_plan_path, task_review_prompt_path  # noqa: E402

ROUND_ID = "round-001"


def example_mission(*, run_id: str) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "topic": "Chiang Mai smoke verification",
        "objective": "Determine whether public smoke claims are supported by physical evidence.",
        "policy_profile": "standard",
        "constraints": {
            "max_rounds": 3,
            "max_claims_per_round": 4,
            "max_tasks_per_round": 4,
        },
        "window": {
            "start_utc": "2026-03-18T00:00:00Z",
            "end_utc": "2026-03-19T23:59:59Z",
        },
        "region": {
            "label": "Chiang Mai, Thailand",
            "geometry": {
                "type": "Point",
                "latitude": 18.7883,
                "longitude": 98.9853,
            },
        },
        "hypotheses": [
            "Smoke discussion is driven by real fire activity upwind of Chiang Mai.",
        ],
    }


def scaffold_temp_run(root: Path, *, run_id: str = "orchestration-prepare-run-001") -> Path:
    run_dir = root / run_id
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=example_mission(run_id=run_id),
        tasks=None,
        pretty=True,
    )
    return run_dir


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


class OrchestrationPrepareTests(unittest.TestCase):
    def test_materialize_prepare_round_outputs_writes_plan_prompts_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="orchestration-prepare-materialize")
            plan = {
                "plan_kind": "eco-council-fetch-plan",
                "schema_version": "1.0.0",
                "run": {
                    "run_id": "orchestration-prepare-materialize",
                    "round_id": ROUND_ID,
                },
                "steps": [
                    {
                        "step_id": "step-sociologist-001",
                        "role": "sociologist",
                        "source_skill": "gdelt-doc-search",
                        "artifact_path": str(run_dir / "round_001" / "sociologist" / "raw" / "gdelt-doc-search.json"),
                        "command": "python3 skills/gdelt.py fetch --output raw.json",
                        "depends_on": [],
                        "notes": ["Search public smoke discussion for the mission window."],
                        "skill_refs": ["$gdelt-doc-search"],
                    }
                ],
            }

            result = materialize_prepare_round_outputs(run_dir=run_dir, round_id=ROUND_ID, plan=plan)

            self.assertEqual(1, result["step_count"])
            self.assertTrue(fetch_plan_path(run_dir, ROUND_ID).exists())
            self.assertTrue(task_review_prompt_path(run_dir, ROUND_ID).exists())
            self.assertIn("sociologist", result["role_fetch_prompt_paths"])
            prompt_path = Path(result["role_fetch_prompt_paths"]["sociologist"])
            self.assertTrue(prompt_path.exists())
            prompt_text = prompt_path.read_text(encoding="utf-8")
            self.assertIn("You are the sociologist", prompt_text)
            self.assertIn("gdelt-doc-search", prompt_text)
            self.assertIn("python3 skills/gdelt.py fetch", prompt_text)

            manifest = read_json(Path(result["manifest_path"]))
            self.assertEqual("fetch-ready", manifest["stage"])
            self.assertEqual(str(fetch_plan_path(run_dir, ROUND_ID)), manifest["fetch_plan_path"])
            self.assertIn("prepare_round", manifest["next_commands"])
            self.assertIn("execute_fetch_plan", manifest["next_commands"])

    def test_render_role_fetch_prompt_returns_none_without_steps(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="orchestration-prepare-no-steps")
            prompt_path = render_role_fetch_prompt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                role="environmentalist",
                plan={"steps": []},
            )

            self.assertIsNone(prompt_path)

    def test_render_moderator_task_review_prompt_writes_validation_guidance(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="orchestration-prepare-task-review")

            prompt_path = render_moderator_task_review_prompt(run_dir=run_dir, round_id=ROUND_ID)

            text = prompt_path.read_text(encoding="utf-8")
            self.assertIn("validate", text)
            self.assertIn("round-task", text)
            self.assertIn("Return only the final JSON list.", text)


if __name__ == "__main__":
    unittest.main()

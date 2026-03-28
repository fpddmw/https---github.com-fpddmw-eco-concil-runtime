from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.simulation import (  # noqa: E402
    command_list_presets,
    find_preset_path,
    simulate_round,
)
from eco_council_runtime.application.orchestration_planning import fetch_plan_input_snapshot  # noqa: E402
from eco_council_runtime.contract import default_round_tasks, scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.paths import fetch_execution_path, fetch_plan_path, round_dir, source_selection_path  # noqa: E402
from eco_council_runtime.domain.text import maybe_text  # noqa: E402

ROUND_ID = "round-001"


def example_mission() -> dict[str, object]:
    return json.loads(Path("assets/contract/examples/mission.json").read_text(encoding="utf-8"))


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def scaffold_simulation_run(root: Path) -> Path:
    mission = example_mission()
    tasks = default_round_tasks(mission=mission, round_id=ROUND_ID)
    run_dir = root / maybe_text(mission.get("run_id"))
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=mission,
        tasks=tasks,
        pretty=True,
    )
    write_json(source_selection_path(run_dir, ROUND_ID, "sociologist"), {"status": "complete", "selected_sources": []})
    write_json(source_selection_path(run_dir, ROUND_ID, "environmentalist"), {"status": "complete", "selected_sources": []})

    base_round_dir = round_dir(run_dir, ROUND_ID)
    video_artifact = base_round_dir / "sociologist" / "raw" / "youtube-video-search.jsonl"
    comment_artifact = base_round_dir / "sociologist" / "raw" / "youtube-comments-fetch.jsonl"
    snapshot = fetch_plan_input_snapshot(
        run_dir=run_dir,
        round_id=ROUND_ID,
        sociologist_selection={"status": "complete"},
        environmentalist_selection={"status": "complete"},
    )
    plan = {
        "plan_kind": "eco-council-fetch-plan",
        "schema_version": "1.0.0",
        "input_snapshot": snapshot,
        "steps": [
            {
                "step_id": "step-sociologist-01-youtube-video-search",
                "role": "sociologist",
                "source_skill": "youtube-video-search",
                "artifact_path": str(video_artifact),
                "stdout_path": str(base_round_dir / "sociologist" / "raw" / "_meta" / "youtube-video-search.stdout.json"),
                "stderr_path": str(base_round_dir / "sociologist" / "raw" / "_meta" / "youtube-video-search.stderr.log"),
                "depends_on": [],
            },
            {
                "step_id": "step-sociologist-02-youtube-comments-fetch",
                "role": "sociologist",
                "source_skill": "youtube-comments-fetch",
                "artifact_path": str(comment_artifact),
                "stdout_path": str(base_round_dir / "sociologist" / "raw" / "_meta" / "youtube-comments-fetch.stdout.json"),
                "stderr_path": str(base_round_dir / "sociologist" / "raw" / "_meta" / "youtube-comments-fetch.stderr.log"),
                "depends_on": ["step-sociologist-01-youtube-video-search"],
            },
        ],
    }
    write_json(fetch_plan_path(run_dir, ROUND_ID), plan)
    return run_dir


class SimulationExtractedModuleTests(unittest.TestCase):
    def test_application_simulation_exports_preset_commands(self) -> None:
        payload = command_list_presets(argparse.Namespace(pretty=False))

        preset_names = {item["preset"] for item in payload["presets"]}
        self.assertGreaterEqual(payload["preset_count"], 4)
        self.assertIn("smoke-mixed", preset_names)
        self.assertTrue(find_preset_path("smoke-mixed").exists())

    def test_application_simulation_runs_round_via_new_package_home(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_simulation_run(Path(temp_dir))
            scenario_path = find_preset_path("smoke-mixed")
            scenario = json.loads(scenario_path.read_text(encoding="utf-8"))

            payload = simulate_round(
                run_dir=run_dir,
                round_id=ROUND_ID,
                scenario=scenario,
                scenario_source=str(scenario_path),
                skip_input_check=False,
                continue_on_error=False,
                overwrite=False,
                skip_existing=False,
            )

            self.assertEqual(2, payload["completed_count"])
            self.assertEqual(0, payload["failed_count"])
            execution = json.loads(fetch_execution_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            self.assertEqual("simulated", execution["execution_mode"])


if __name__ == "__main__":
    unittest.main()
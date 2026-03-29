from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_script, runtime_path, script_path, write_json

RUN_ID = "run-source-queue-001"
ROUND_ID = "round-source-queue-001"


def build_openaq_artifact(root: Path) -> Path:
    path = root / "openaq.json"
    write_json(
        path,
        {
            "results": [
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 41.5,
                    "date": {"utc": "2023-06-07T12:00:00Z"},
                    "coordinates": {"latitude": 40.7004, "longitude": -74.0004},
                    "location": {"id": 1, "name": "NYC"},
                    "provider": {"name": "OpenAQ"},
                }
            ]
        },
    )
    return path


def build_detached_fetch_script(root: Path) -> Path:
    path = root / "emit_youtube_fixture.py"
    path.write_text(
        "import json\n"
        "payload=[{\"query\":\"nyc smoke wildfire\",\"video_id\":\"vid-detached-001\",\"video\":{\"id\":\"vid-detached-001\",\"title\":\"Smoke over New York City\",\"description\":\"Wildfire smoke covered New York City and reduced visibility.\",\"channel_title\":\"City Desk\",\"published_at\":\"2023-06-07T13:00:00Z\",\"default_language\":\"en\",\"statistics\":{\"view_count\":1250}}}]\n"
        "print(json.dumps(payload))\n",
        encoding="utf-8",
    )
    return path


def build_mixed_queue_mission(root: Path, openaq_path: Path, fetch_script: Path) -> Path:
    mission_path = root / "mission.json"
    write_json(
        mission_path,
        {
            "schema_version": "1.0.0",
            "run_id": RUN_ID,
            "topic": "NYC smoke verification",
            "objective": "Determine whether public smoke reports are supported by physical evidence.",
            "policy_profile": "standard",
            "window": {
                "start_utc": "2023-06-07T00:00:00Z",
                "end_utc": "2023-06-07T23:59:59Z",
            },
            "region": {
                "label": "New York City, USA",
                "geometry": {
                    "type": "Point",
                    "latitude": 40.7128,
                    "longitude": -74.0060,
                },
            },
            "hypotheses": [
                {
                    "title": "Smoke over NYC was materially significant",
                    "statement": "Public smoke reports are backed by elevated PM2.5 observations.",
                    "confidence": 0.55,
                }
            ],
            "artifact_imports": [
                {
                    "source_skill": "openaq-data-fetch",
                    "artifact_path": str(openaq_path),
                    "source_mode": "test-fixture",
                }
            ],
            "source_requests": [
                {
                    "source_skill": "youtube-video-search",
                    "query_text": "nyc smoke wildfire",
                    "artifact_capture": "stdout-json",
                    "fetch_argv": [sys.executable, str(fetch_script)],
                }
            ],
            "source_selections": {
                "sociologist": {
                    "status": "complete",
                    "selected_sources": ["youtube-video-search"],
                },
                "environmentalist": {
                    "status": "complete",
                    "selected_sources": ["openaq-data-fetch"],
                },
            },
        },
    )
    return mission_path


class SourceQueueRebuildTests(unittest.TestCase):
    def test_prepare_round_materializes_source_selection_snapshots_and_mixed_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mixed_queue_mission(root, build_openaq_artifact(root), build_detached_fetch_script(root))

            run_script(
                script_path("eco-scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
            )
            payload = run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            plan = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))
            sociologist_selection = load_json(runtime_path(run_dir, f"source_selection_sociologist_{ROUND_ID}.json"))
            environmentalist_selection = load_json(runtime_path(run_dir, f"source_selection_environmentalist_{ROUND_ID}.json"))

            self.assertEqual(2, payload["summary"]["step_count"])
            self.assertEqual({"import", "detached-fetch"}, {step["step_kind"] for step in plan["steps"]})
            self.assertEqual(["youtube-video-search"], plan["roles"]["sociologist"]["selected_sources"])
            self.assertEqual(["openaq-data-fetch"], plan["roles"]["environmentalist"]["selected_sources"])
            self.assertEqual("complete", plan["input_snapshot"]["source_selections"]["sociologist"]["status"])
            self.assertEqual(["youtube-video-search"], sociologist_selection["selected_sources"])
            self.assertEqual(["openaq-data-fetch"], environmentalist_selection["selected_sources"])

    def test_import_execution_runs_mixed_import_and_detached_fetch_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mixed_queue_mission(root, build_openaq_artifact(root), build_detached_fetch_script(root))

            run_script(
                script_path("eco-scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
            )
            run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            payload = run_script(
                script_path("eco-import-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            execution = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))

            self.assertEqual(2, payload["summary"]["normalized_step_count"])
            self.assertEqual(2, execution["completed_count"])
            self.assertEqual(0, execution["failed_count"])
            self.assertEqual({"import", "detached-fetch"}, {status["step_kind"] for status in execution["statuses"]})
            self.assertEqual(2, len(execution["normalized_receipt_ids"]))

    def test_import_execution_detects_source_selection_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mixed_queue_mission(root, build_openaq_artifact(root), build_detached_fetch_script(root))

            run_script(
                script_path("eco-scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
            )
            run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            changed_selection_path = runtime_path(run_dir, f"source_selection_sociologist_{ROUND_ID}.json")
            changed_selection = load_json(changed_selection_path)
            changed_selection["selected_sources"] = []
            changed_selection_path.write_text(json.dumps(changed_selection, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    str(script_path("eco-import-fetch-execution")),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(0, completed.returncode)
            self.assertIn("Fetch plan inputs changed since prepare-round", completed.stderr)


if __name__ == "__main__":
    unittest.main()
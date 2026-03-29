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


def build_retrying_detached_fetch_script(root: Path, marker_path: Path) -> Path:
    path = root / "emit_youtube_retry_fixture.py"
    path.write_text(
        "import json, pathlib, sys\n"
        f"marker = pathlib.Path({str(marker_path)!r})\n"
        "if not marker.exists():\n"
        "    marker.write_text('first-attempt', encoding='utf-8')\n"
        "    print('transient fetch failure', file=sys.stderr)\n"
        "    raise SystemExit(75)\n"
        "payload=[{\"query\":\"nyc smoke wildfire\",\"video_id\":\"vid-detached-002\",\"video\":{\"id\":\"vid-detached-002\",\"title\":\"Smoke over New York City retry\",\"description\":\"Second attempt succeeded.\",\"channel_title\":\"City Desk\",\"published_at\":\"2023-06-07T13:05:00Z\",\"default_language\":\"en\",\"statistics\":{\"view_count\":980}}}]\n"
        "print(json.dumps(payload))\n",
        encoding="utf-8",
    )
    return path


def build_mixed_queue_mission(root: Path, openaq_path: Path, fetch_script: Path, *, request_overrides: dict[str, object] | None = None) -> Path:
    mission_path = root / "mission.json"
    source_request = {
        "source_skill": "youtube-video-search",
        "query_text": "nyc smoke wildfire",
        "artifact_capture": "stdout-json",
        "fetch_argv": [sys.executable, str(fetch_script)],
    }
    if request_overrides:
        source_request.update(request_overrides)
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
                source_request
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
    def test_prepare_round_materializes_detached_fetch_governance(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mixed_queue_mission(
                root,
                build_openaq_artifact(root),
                build_detached_fetch_script(root),
                request_overrides={
                    "fetch_execution_policy": {
                        "timeout_seconds": 9.0,
                        "retry_budget": 1,
                        "retry_backoff_ms": 25,
                    },
                    "declared_side_effects": ["reads-shared-state"],
                    "requested_side_effect_approvals": [],
                },
            )

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

            plan = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))
            detached_step = next(step for step in plan["steps"] if step["step_kind"] == "detached-fetch")

            self.assertEqual("1.2.0", plan["schema_version"])
            self.assertEqual(9.0, detached_step["fetch_execution_policy"]["timeout_seconds"])
            self.assertEqual(1, detached_step["fetch_execution_policy"]["retry_budget"])
            self.assertEqual(25, detached_step["fetch_execution_policy"]["retry_backoff_ms"])
            self.assertEqual(["writes-artifacts", "reads-shared-state"], detached_step["declared_side_effects"])
            self.assertEqual([], detached_step["requested_side_effect_approvals"])

    def test_prepare_round_rejects_detached_fetch_approvals_outside_declared_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mixed_queue_mission(
                root,
                build_openaq_artifact(root),
                build_detached_fetch_script(root),
                request_overrides={
                    "declared_side_effects": ["reads-shared-state"],
                    "requested_side_effect_approvals": ["network-external"],
                },
            )

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

            completed = subprocess.run(
                [
                    sys.executable,
                    str(script_path("eco-prepare-round")),
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
            self.assertIn("requested_side_effect_approvals must be a subset of declared_side_effects", completed.stderr)

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

    def test_import_execution_retries_detached_fetch_and_records_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            marker_path = root / "retry.marker"
            mission_path = build_mixed_queue_mission(
                root,
                build_openaq_artifact(root),
                build_retrying_detached_fetch_script(root, marker_path),
                request_overrides={
                    "fetch_execution_policy": {
                        "timeout_seconds": 5.0,
                        "retry_budget": 1,
                        "retry_backoff_ms": 1,
                    },
                    "declared_side_effects": ["reads-shared-state"],
                    "requested_side_effect_approvals": [],
                },
            )

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
            run_script(
                script_path("eco-import-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            execution = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            detached_status = next(status for status in execution["statuses"] if status["step_kind"] == "detached-fetch")
            detached_meta = detached_status["detached_fetch"]

            self.assertEqual(2, detached_meta["attempt_count"])
            self.assertTrue(detached_meta["recovered_after_retry"])
            self.assertEqual(5.0, detached_meta["execution_policy"]["timeout_seconds"])
            self.assertEqual(1, detached_meta["execution_policy"]["retry_budget"])
            self.assertEqual(["writes-artifacts", "reads-shared-state"], detached_meta["declared_side_effects"])
            self.assertEqual([], detached_meta["requested_side_effect_approvals"])
            self.assertEqual(["writes-artifacts", "reads-shared-state"], detached_meta["allow_side_effects"])
            self.assertTrue(marker_path.exists())

    def test_import_execution_blocks_detached_fetch_outside_sandbox_and_materializes_dead_letter(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mixed_queue_mission(
                root,
                build_openaq_artifact(root),
                build_detached_fetch_script(root),
                request_overrides={
                    "artifact_path": str(root / "outside-runtime.json"),
                },
            )

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
            execution = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            detached_status = next(status for status in execution["statuses"] if status["step_kind"] == "detached-fetch")
            dead_letters = list((run_dir / "runtime" / "dead_letters").glob("*.json"))
            ledger_lines = (run_dir / "runtime" / "audit_ledger.jsonl").read_text(encoding="utf-8").splitlines()
            last_event = json.loads(ledger_lines[-1])

            self.assertEqual(1, execution["failed_count"])
            self.assertEqual("failed", detached_status["status"])
            self.assertTrue(detached_status["detached_fetch"]["runtime_admission"]["block_execution"])
            self.assertIn(
                detached_status["detached_fetch"]["failure"]["error_code"],
                {"sandbox-read-boundary-violation", "sandbox-write-boundary-violation"},
            )
            self.assertEqual("detached-fetch-execution", last_event["event_type"])
            self.assertEqual("blocked", last_event["status"])
            self.assertEqual(1, len(dead_letters))

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

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    board_path,
    load_json,
    promotion_path,
    reporting_path,
    request_and_approve_transition,
    run_kernel,
    run_script,
    runtime_path,
    script_path,
    write_json,
)

RUN_ID = "run-ingress-001"
ROUND_ID = "round-001"


def approve_promotion_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="promote-evidence-basis",
        rationale="Approve promotion for orchestration ingress workflow coverage.",
    )


def build_raw_artifacts(root: Path) -> dict[str, Path]:
    youtube_path = root / "youtube.json"
    bluesky_path = root / "bluesky.json"
    openaq_path = root / "openaq.json"
    airnow_path = root / "airnow.json"

    write_json(
        youtube_path,
        [
            {
                "query": "nyc smoke wildfire",
                "video_id": "vid-ingress-001",
                "video": {
                    "id": "vid-ingress-001",
                    "title": "Smoke over New York City",
                    "description": "Wildfire smoke covered New York City and reduced visibility.",
                    "channel_title": "City Desk",
                    "published_at": "2023-06-07T13:00:00Z",
                    "default_language": "en",
                    "statistics": {"view_count": 1250},
                },
            }
        ],
    )
    write_json(
        bluesky_path,
        {
            "seed_posts": [
                {
                    "uri": "at://did:plc:smoke/app.bsky.feed.post/seed001",
                    "author_handle": "smoke.reporter.test",
                    "author_did": "did:plc:smoke",
                    "text": "Smoke haze over the New York skyline is intense today.",
                    "timestamp_utc": "2023-06-07T12:30:00Z",
                    "reply_count": 1,
                    "repost_count": 2,
                    "like_count": 3,
                    "quote_count": 0,
                }
            ]
        },
    )
    write_json(
        openaq_path,
        {
            "results": [
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 41.5,
                    "date": {"utc": "2023-06-07T12:00:00Z"},
                    "coordinates": {"latitude": 40.7004, "longitude": -74.0004},
                    "location": {"id": 1, "name": "NYC"},
                    "provider": {"name": "OpenAQ"},
                },
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 44.0,
                    "date": {"utc": "2023-06-07T13:00:00Z"},
                    "coordinates": {"latitude": 40.7001, "longitude": -74.0001},
                    "location": {"id": 1, "name": "NYC"},
                    "provider": {"name": "OpenAQ"},
                },
            ]
        },
    )
    write_json(
        airnow_path,
        {
            "records": [
                {
                    "parameter_name": "PM25",
                    "raw_concentration": 52.0,
                    "aqi_value": 155,
                    "latitude": 40.7002,
                    "longitude": -74.0002,
                    "observed_at_utc": "2023-06-07T12:00:00Z",
                    "site_name": "Test Site",
                    "country_code": "US",
                }
            ]
        },
    )
    return {
        "youtube": youtube_path,
        "bluesky": bluesky_path,
        "openaq": openaq_path,
        "airnow": airnow_path,
    }


def build_mission_file(root: Path, artifacts: dict[str, Path]) -> Path:
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
                    "source_skill": "youtube-video-search",
                    "artifact_path": str(artifacts["youtube"]),
                    "query_text": "nyc smoke wildfire",
                },
                {
                    "source_skill": "bluesky-cascade-fetch",
                    "artifact_path": str(artifacts["bluesky"]),
                },
                {
                    "source_skill": "openaq-data-fetch",
                    "artifact_path": str(artifacts["openaq"]),
                    "source_mode": "test-fixture",
                },
                {
                    "source_skill": "airnow-hourly-obs-fetch",
                    "artifact_path": str(artifacts["airnow"]),
                },
            ],
        },
    )
    return mission_path


class OrchestrationIngressWorkflowTests(unittest.TestCase):
    def test_scaffold_agent_mode_updates_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            scaffold_payload = run_script(
                script_path("eco-scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )

            scaffold_artifact = load_json(runtime_path(run_dir, f"mission_scaffold_{ROUND_ID}.json"))

            self.assertEqual("openclaw-agent", scaffold_payload["summary"]["orchestration_mode"])
            self.assertEqual("openclaw-agent", scaffold_artifact["orchestration_mode"])
            self.assertIn("eco-read-board-delta", scaffold_payload["board_handoff"]["suggested_next_skills"])

    def test_scaffold_and_prepare_round_materialize_fetch_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            scaffold_payload = run_script(
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
            prepare_payload = run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            board_artifact = load_json(board_path(run_dir))
            plan_artifact = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))
            tasks_path = run_dir / "investigation" / f"round_tasks_{ROUND_ID}.json"
            tasks_payload = json.loads(tasks_path.read_text(encoding="utf-8"))

            self.assertEqual(4, scaffold_payload["summary"]["import_source_count"])
            self.assertEqual(2, scaffold_payload["summary"]["task_count"])
            self.assertEqual(1, scaffold_payload["summary"]["seeded_hypothesis_count"])
            self.assertEqual(2, len(tasks_payload))
            self.assertEqual(4, prepare_payload["summary"]["step_count"])
            self.assertEqual(4, len(plan_artifact["steps"]))
            self.assertEqual(["youtube-video-search", "bluesky-cascade-fetch"], plan_artifact["roles"]["sociologist"]["selected_sources"])
            self.assertEqual(["openaq-data-fetch", "airnow-hourly-obs-fetch"], plan_artifact["roles"]["environmentalist"]["selected_sources"])
            self.assertEqual("active", board_artifact["rounds"][ROUND_ID]["hypotheses"][0]["status"])

    def test_prepare_round_reads_db_backed_round_tasks_when_export_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

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
            tasks_path = run_dir / "investigation" / f"round_tasks_{ROUND_ID}.json"
            tasks_path.unlink()

            prepare_payload = run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            plan_artifact = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))
            recreated_tasks = json.loads(tasks_path.read_text(encoding="utf-8"))

            self.assertEqual(
                "deliberation-plane-round-tasks",
                prepare_payload["summary"]["task_source"],
            )
            self.assertEqual(
                "deliberation-plane-round-tasks",
                plan_artifact["task_source"],
            )
            self.assertFalse(
                plan_artifact["observed_inputs"]["round_tasks_artifact_present"]
            )
            self.assertTrue(plan_artifact["observed_inputs"]["round_tasks_present"])
            self.assertEqual(2, len(recreated_tasks))
            self.assertEqual(4, len(plan_artifact["steps"]))

    def test_ingress_import_execution_reconnects_to_reporting_mainline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            scaffold_payload = run_script(
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
            import_payload = run_script(
                script_path("eco-import-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            run_script(script_path("eco-extract-claim-candidates"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            run_script(script_path("eco-extract-observation-candidates"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--metric", "pm2_5")
            cluster_payload = run_script(script_path("eco-cluster-claim-candidates"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            run_script(script_path("eco-merge-observation-candidates"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--metric", "pm2_5")
            run_script(script_path("eco-link-claims-to-observations"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            run_script(script_path("eco-derive-claim-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            run_script(script_path("eco-derive-observation-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            coverage_payload = run_script(script_path("eco-score-evidence-coverage"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
            seeded_hypothesis_id = scaffold_payload["summary"]["seeded_hypothesis_ids"][0]
            run_script(
                script_path("eco-post-board-note"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--category",
                "analysis",
                "--note-text",
                "Imported mission artifacts now support round-level reporting review.",
                "--linked-artifact-ref",
                coverage_ref,
            )
            run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--hypothesis-id",
                seeded_hypothesis_id,
                "--title",
                "Smoke over NYC was materially significant",
                "--statement",
                "Public smoke reports are backed by elevated PM2.5 observations.",
                "--status",
                "active",
                "--owner-role",
                "environmentalist",
                "--linked-claim-id",
                cluster_payload["canonical_ids"][0],
                "--confidence",
                "0.93",
            )

            approve_promotion_transition(run_dir)
            run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            handoff_payload = run_script(script_path("eco-materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            decision_payload = run_script(script_path("eco-draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            import_artifact = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            handoff_artifact = load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"))
            decision_artifact = load_json(reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertEqual(4, import_payload["summary"]["normalized_step_count"])
            self.assertEqual(4, import_artifact["completed_count"])
            self.assertEqual(0, import_artifact["failed_count"])
            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertEqual("reporting-ready", handoff_artifact["handoff_status"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual("promoted", handoff_artifact["promotion_status"])
            self.assertEqual("promoted", promotion_artifact["promotion_status"])
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual("ready", decision_artifact["publication_readiness"])


if __name__ == "__main__":
    unittest.main()

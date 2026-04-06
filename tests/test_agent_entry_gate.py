from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    kernel_script_path,
    load_json,
    run_kernel,
    run_script,
    runtime_path,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUN_ID = "run-agent-entry-001"
ROUND_ID = "round-agent-entry-001"


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
                "video_id": "vid-agent-entry-001",
                "video": {
                    "id": "vid-agent-entry-001",
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
                }
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


class AgentEntryGateTests(unittest.TestCase):
    def test_materialize_agent_entry_gate_creates_gate_and_advisory_plan(self) -> None:
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
                "--orchestration-mode",
                "openclaw-agent",
            )
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            payload = run_kernel(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            gate_artifact = load_json(runtime_path(run_dir, f"agent_entry_gate_{ROUND_ID}.json"))
            advisory_plan = load_json(runtime_path(run_dir, f"agent_advisory_plan_{ROUND_ID}.json"))
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", payload["summary"]["entry_status"])
            self.assertTrue(payload["summary"]["advisory_plan_present"])
            self.assertTrue(payload["summary"]["advisory_plan_materialized"])
            self.assertEqual("openclaw-agent", payload["agent_entry"]["orchestration_mode"])
            self.assertEqual("agent-advisory", payload["agent_entry"]["advisory_plan"]["planning_mode"])
            self.assertIn("eco-read-board-delta", payload["agent_entry"]["recommended_entry_skills"])
            self.assertTrue(any(item["step_id"] == "return-to-runtime-gate" for item in payload["agent_entry"]["entry_chain"]))
            self.assertEqual("runtime-agent-entry-gate-v1", gate_artifact["schema_version"])
            self.assertEqual("agent-advisory", advisory_plan["planning_mode"])
            self.assertTrue(state_payload["agent_entry"]["operator"]["entry_gate_present"])
            self.assertIn(
                "materialize-agent-entry-gate",
                state_payload["agent_entry"]["operator"]["refresh_agent_entry_gate_command"],
            )
            self.assertIn(
                "supervise-round",
                state_payload["agent_entry"]["operator"]["return_to_supervisor_command"],
            )
            self.assertEqual("eco_runtime_kernel.py", kernel_script_path().name)

    def test_show_run_state_surfaces_agent_entry_commands_before_gate_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)

            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            operator = state_payload["agent_entry"]["operator"]
            self.assertFalse(operator["entry_gate_present"])
            self.assertIn("materialize-agent-entry-gate", operator["materialize_agent_entry_gate_command"])
            self.assertIn("claim-cluster", operator["list_claim_cluster_result_sets_command"])
            self.assertIn("eco-read-board-delta", operator["read_board_delta_command"])
            self.assertIn("eco-query-public-signals", operator["query_public_signals_command"])

    def test_operator_runbook_includes_agent_entry_section_for_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)

            payload = run_kernel(
                "materialize-operator-runbook",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            runbook_text = Path(payload["operator_runbook_path"]).read_text(encoding="utf-8")

            self.assertIn("## Agent Entry", runbook_text)
            self.assertIn("materialize-agent-entry-gate", runbook_text)
            self.assertIn(f"runtime/agent_advisory_plan_{ROUND_ID}.json", runbook_text)


if __name__ == "__main__":
    unittest.main()

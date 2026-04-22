from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from _workflow_support import (
    kernel_script_path,
    load_json,
    run_kernel,
    run_script,
    runtime_src_path,
    runtime_path,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

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
            self.assertIn("eco-submit-council-proposal", payload["agent_entry"]["recommended_entry_skills"])
            self.assertIn("eco-submit-readiness-opinion", payload["agent_entry"]["recommended_entry_skills"])
            self.assertTrue(
                any(
                    "eco-submit-council-proposal" in command
                    for entry in payload["agent_entry"]["role_entry_points"]
                    if isinstance(entry, dict)
                    for command in entry.get("write_commands", [])
                    if isinstance(entry.get("write_commands"), list)
                )
            )
            self.assertTrue(
                any(
                    "eco-submit-readiness-opinion" in command
                    for entry in payload["agent_entry"]["role_entry_points"]
                    if isinstance(entry, dict)
                    for command in entry.get("write_commands", [])
                    if isinstance(entry.get("write_commands"), list)
                )
            )
            self.assertTrue(any(item["step_id"] == "submit-council-proposal" for item in payload["agent_entry"]["entry_chain"]))
            self.assertTrue(any(item["step_id"] == "submit-readiness-opinion" for item in payload["agent_entry"]["entry_chain"]))
            self.assertTrue(any(item["step_id"] == "return-to-runtime-gate" for item in payload["agent_entry"]["entry_chain"]))
            self.assertEqual("runtime-agent-entry-gate-v1", gate_artifact["schema_version"])
            self.assertEqual("agent-advisory", advisory_plan["planning_mode"])
            self.assertTrue(state_payload["agent_entry"]["operator"]["entry_gate_present"])
            self.assertIn(
                "materialize-agent-entry-gate",
                state_payload["agent_entry"]["operator"]["refresh_agent_entry_gate_command"],
            )
            self.assertIn(
                "query-council-objects",
                state_payload["agent_entry"]["operator"]["query_council_proposals_command"],
            )
            self.assertIn(
                "eco-submit-council-proposal",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--confidence",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--evidence-ref",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--provenance-json",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "supervise-round",
                state_payload["agent_entry"]["operator"]["return_to_supervisor_command"],
            )
            self.assertEqual("eco_runtime_kernel.py", kernel_script_path().name)

    def test_materialize_agent_entry_gate_prefers_direct_council_advisory(self) -> None:
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

            from eco_council_runtime.council_objects import store_readiness_opinion_records
            from eco_council_runtime.kernel.agent_entry import materialize_agent_entry_gate
            from eco_council_runtime.phase2_agent_entry_profile import (
                default_phase2_agent_entry_profile,
            )
            from eco_council_runtime.phase2_agent_handoff import (
                default_phase2_entry_chain,
                default_phase2_hard_gate_commands,
            )

            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_promotion": True,
                            "rationale": "The council has already converged on promotion readiness.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            with mock.patch("eco_council_runtime.phase2_agent_entry_profile.run_skill") as run_skill_mock:
                payload = materialize_agent_entry_gate(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    agent_entry_profile=default_phase2_agent_entry_profile(),
                    hard_gate_command_builder=default_phase2_hard_gate_commands,
                    entry_chain_builder=default_phase2_entry_chain,
                    contract_mode="warn",
                )

            advisory_plan = load_json(runtime_path(run_dir, f"agent_advisory_plan_{ROUND_ID}.json"))
            run_skill_mock.assert_not_called()
            self.assertEqual("completed", payload["status"])
            self.assertTrue(payload["summary"]["advisory_plan_materialized"])
            self.assertEqual("direct-council-advisory", payload["summary"]["advisory_plan_source"])
            self.assertEqual("direct-council-advisory", payload["agent_entry"]["advisory_plan"]["plan_source"])
            self.assertEqual("direct-council-advisory", advisory_plan["plan_source"])
            self.assertTrue(payload["agent_entry"]["advisory_plan"]["direct_council_queue"])

    def test_materialize_agent_entry_gate_accepts_injected_handoff_profile(self) -> None:
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

            from eco_council_runtime.kernel.agent_entry import materialize_agent_entry_gate
            from eco_council_runtime.phase2_agent_entry_profile import (
                default_phase2_agent_entry_profile,
            )

            def custom_hard_gate_commands(**_: object) -> dict[str, str]:
                return {
                    "show_run_state": "custom-show-state",
                    "supervise_round": "custom-supervise-round",
                    "apply_promotion_gate": "custom-apply-gate",
                    "close_round": "custom-close-round",
                    "open_next_round": "custom-open-next-round",
                }

            def custom_entry_chain(**_: object) -> list[dict[str, str]]:
                return [
                    {
                        "step_id": "custom-runtime-handoff",
                        "mode": "runtime-gate",
                        "objective": "Use the injected handoff profile.",
                        "command": "custom-supervise-round",
                    }
                ]

            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=default_phase2_agent_entry_profile(),
                hard_gate_command_builder=custom_hard_gate_commands,
                entry_chain_builder=custom_entry_chain,
                contract_mode="warn",
            )

            self.assertEqual(
                "custom-supervise-round",
                payload["agent_entry"]["hard_gate_commands"]["supervise_round"],
            )
            self.assertEqual(
                "custom-runtime-handoff",
                payload["agent_entry"]["entry_chain"][0]["step_id"],
            )

    def test_materialize_agent_entry_gate_accepts_injected_agent_entry_profile(self) -> None:
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

            from eco_council_runtime.kernel.agent_entry import (
                agent_entry_state,
                materialize_agent_entry_gate,
            )
            from eco_council_runtime.phase2_agent_entry_profile import (
                default_phase2_agent_entry_profile,
            )
            from eco_council_runtime.phase2_agent_handoff import (
                default_phase2_entry_chain,
                default_phase2_hard_gate_commands,
            )

            custom_profile = default_phase2_agent_entry_profile()
            custom_profile["role_definitions"] = [
                {
                    "role": "auditor",
                    "focus": "Custom review lane only.",
                    "read_skills": [],
                    "write_skills": [],
                    "analysis_kinds": [],
                }
            ]
            custom_profile["role_entry_builder"] = lambda **_: [
                {
                    "role": "auditor",
                    "focus": "Custom review lane only.",
                    "read_commands": ["custom-read-state"],
                    "analysis_commands": ["custom-query-analysis"],
                    "write_commands": ["custom-write-note"],
                }
            ]
            custom_profile["recommended_skills_builder"] = lambda **_: ["eco-audit-round"]
            custom_profile["operator_notes_builder"] = lambda **_: ["Use the injected entry profile."]
            custom_profile["next_round_id_builder"] = lambda **_: "round-agent-entry-custom-next"
            custom_profile["operator_commands_builder"] = lambda **_: {
                "materialize_agent_entry_gate_command": "custom-materialize-entry-gate",
                "refresh_agent_entry_gate_command": "custom-refresh-entry-gate",
                "materialize_agent_advisory_plan_command": "custom-refresh-advisory",
                "read_board_delta_command": "custom-read-board",
                "query_public_signals_command": "custom-query-public",
                "query_formal_signals_command": "custom-query-formal",
                "query_environment_signals_command": "custom-query-environment",
                "list_claim_cluster_result_sets_command": "custom-list-analysis",
                "query_claim_cluster_items_command_template": "custom-query-analysis-items",
            }

            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=custom_profile,
                hard_gate_command_builder=default_phase2_hard_gate_commands,
                entry_chain_builder=default_phase2_entry_chain,
                contract_mode="warn",
            )
            state_payload = agent_entry_state(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=custom_profile,
                hard_gate_command_builder=default_phase2_hard_gate_commands,
            )

            self.assertEqual(["eco-audit-round"], payload["agent_entry"]["recommended_entry_skills"])
            self.assertEqual("auditor", payload["agent_entry"]["role_entry_points"][0]["role"])
            self.assertEqual(
                "Use the injected entry profile.",
                payload["agent_entry"]["operator_notes"][0],
            )
            self.assertEqual(
                "custom-materialize-entry-gate",
                state_payload["operator"]["materialize_agent_entry_gate_command"],
            )
            self.assertEqual(
                "custom-query-formal",
                state_payload["operator"]["query_formal_signals_command"],
            )
            self.assertEqual(
                "custom-query-analysis-items",
                state_payload["operator"]["query_claim_cluster_items_command_template"],
            )
            self.assertIn(
                "round-agent-entry-custom-next",
                payload["agent_entry"]["hard_gate_commands"]["open_next_round"],
            )
            self.assertIn(
                "round-agent-entry-custom-next",
                state_payload["operator"]["open_next_round_command_template"],
            )

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
            self.assertIn("eco-query-formal-signals", operator["query_formal_signals_command"])
            self.assertIn("query-council-objects", operator["query_council_proposals_command"])
            self.assertIn("query-council-objects", operator["query_readiness_opinions_command"])
            self.assertIn("eco-submit-council-proposal", operator["submit_council_proposal_command_template"])
            self.assertIn("--confidence", operator["submit_council_proposal_command_template"])
            self.assertIn("--evidence-ref", operator["submit_council_proposal_command_template"])
            self.assertIn("--provenance-json", operator["submit_council_proposal_command_template"])
            self.assertIn("eco-submit-readiness-opinion", operator["submit_readiness_opinion_command_template"])

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

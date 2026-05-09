from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    board_path,
    load_json,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    report_basis_path,
    reporting_path,
    request_and_approve_transition,
    run_kernel,
    run_script,
    runtime_path,
    script_path,
    submit_ready_council_support,
    write_json,
)

RUN_ID = "run-ingress-001"
ROUND_ID = "round-001"


def approve_report_basis_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="freeze-report-basis",
        rationale="Approve report_basis for orchestration ingress workflow coverage.",
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
                    "source_skill": "fetch-youtube-video-search",
                    "artifact_path": str(artifacts["youtube"]),
                    "query_text": "nyc smoke wildfire",
                },
                {
                    "source_skill": "fetch-bluesky-cascade",
                    "artifact_path": str(artifacts["bluesky"]),
                },
                {
                    "source_skill": "fetch-openaq",
                    "artifact_path": str(artifacts["openaq"]),
                    "source_mode": "test-fixture",
                },
                {
                    "source_skill": "fetch-airnow-hourly-observations",
                    "artifact_path": str(artifacts["airnow"]),
                },
            ],
        },
    )
    return mission_path


class OrchestrationIngressWorkflowTests(unittest.TestCase):
    def test_scaffold_materializes_verification_scope_for_broad_smoke_mission(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = root / "broad-smoke-mission.json"
            write_json(
                mission_path,
                {
                    "schema_version": "1.0.0",
                    "run_id": RUN_ID,
                    "topic": "June 2023 New York City smoke episode",
                    "objective": "Investigate candidate source regions, transport pathway, public impacts, and handling recommendations.",
                    "window": {
                        "start_utc": "2023-06-07T00:00:00Z",
                        "end_utc": "2023-06-10T00:00:00Z",
                    },
                    "region": {
                        "label": "New York City, NY, United States",
                        "geometry": {"type": "Point", "latitude": 40.7128, "longitude": -74.006},
                    },
                    "hypotheses": [
                        {
                            "title": "Smoke episode requires source and transport verification",
                            "statement": "The report should separate receptor observations from source and transport claims.",
                            "confidence": 0.45,
                        }
                    ],
                    "source_governance": {"max_selected_sources_per_role": 4},
                },
            )

            scaffold_payload = run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
            )

            mission_artifact = load_json(run_dir / "mission.json")
            scaffold_artifact = load_json(runtime_path(run_dir, f"mission_scaffold_{ROUND_ID}.json"))
            tasks_payload = json.loads((run_dir / "investigation" / f"round_tasks_{ROUND_ID}.json").read_text(encoding="utf-8"))
            scope = mission_artifact["verification_scope"]
            lane_ids = {item["lane_id"] for item in scope["required_evidence_lanes"]}

            self.assertEqual("mission-derived-candidate-source-review", scope["candidate_source_region_policy"])
            self.assertEqual("mission-derived-relation-review", scope["transport_verification_policy"])
            self.assertIn("fire-origin", lane_ids)
            self.assertIn("spatiotemporal-relation-review", lane_ids)
            self.assertIn("fetch-nasa-firms-fire", scaffold_artifact["intent_sources_by_role"]["environmental-investigator"])
            self.assertIn("fire-origin", scaffold_artifact["verification_scope_required_lane_ids"])
            self.assertTrue(all(task["inputs"]["verification_scope"]["scope_id"] == scope["scope_id"] for task in tasks_payload))
            self.assertFalse(scaffold_payload["warnings"])

    def test_scaffold_agent_mode_updates_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            scaffold_payload = run_script(
                script_path("scaffold-mission-run"),
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
            self.assertIn("query-board-delta", scaffold_payload["board_handoff"]["suggested_next_skills"])

    def test_scaffold_and_prepare_round_materialize_fetch_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            scaffold_payload = run_script(
                script_path("scaffold-mission-run"),
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
                script_path("prepare-round"),
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
            self.assertEqual(["fetch-youtube-video-search", "fetch-bluesky-cascade"], plan_artifact["roles"]["social-investigator"]["selected_sources"])
            self.assertEqual(["fetch-openaq", "fetch-airnow-hourly-observations"], plan_artifact["roles"]["environmental-investigator"]["selected_sources"])
            self.assertEqual("active", board_artifact["rounds"][ROUND_ID]["hypotheses"][0]["status"])

    def test_prepare_round_reads_db_backed_round_tasks_when_export_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
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
                script_path("prepare-round"),
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

    def test_prepare_round_carries_round_brief_as_optional_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
            )
            brief_payload = run_script(
                script_path("submit-round-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-id",
                ROUND_ID,
                "--rationale",
                "Moderator supplies optional context for the next preparation pass.",
                "--round-mode",
                "supplemental-investigation",
                "--primary-focus-ref",
                "challenge-ticket-001",
                "--requested-output",
                "evidence-request",
                "--source-boundary-note",
                "Do not treat source hints as exclusive.",
                "--brief-text",
                "Inspect timing gaps without constraining investigator evidence use.",
            )

            prepare_payload = run_script(
                script_path("prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            brief_id = brief_payload["summary"]["object_id"]
            plan_artifact = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))

            self.assertEqual(4, prepare_payload["summary"]["step_count"])
            self.assertEqual(4, len(plan_artifact["steps"]))
            self.assertTrue(plan_artifact["observed_inputs"]["round_brief_present"])
            self.assertEqual(brief_id, plan_artifact["round_brief_context"]["object_id"])
            self.assertEqual(
                "supplemental-investigation",
                plan_artifact["round_brief_context"]["round_mode"],
            )
            self.assertIn(
                "does not restrict",
                plan_artifact["round_brief_context"]["semantics"],
            )
            self.assertEqual(
                ["Do not treat source hints as exclusive."],
                plan_artifact["round_brief_context"]["source_boundary_notes"],
            )
            self.assertEqual(
                ["fetch-youtube-video-search", "fetch-bluesky-cascade"],
                plan_artifact["roles"]["social-investigator"]["selected_sources"],
            )
            self.assertIn(brief_id, prepare_payload["board_handoff"]["candidate_ids"])

    def test_open_investigation_round_records_optional_coordination_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)
            round2_id = "round-002"

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
            )
            transition_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="open-investigation-round",
                target_round_id=round2_id,
                source_round_id=ROUND_ID,
                rationale="Moderator opens a supplemental investigation round.",
                request_payload={
                    "context_packet_id": "context-packet-001",
                    "primary_focus_refs": ["request-focus-001"],
                },
            )

            open_payload = run_script(
                script_path("open-investigation-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                round2_id,
                "--source-round-id",
                ROUND_ID,
                "--transition-request-id",
                transition_request_id,
                "--round-mode",
                "supplemental-investigation",
                "--primary-focus-ref",
                "cli-focus-001",
                "--target-challenge-id",
                "challenge-ticket-001",
                "--round-brief-id",
                "round-brief-001",
            )

            transition_artifact = load_json(runtime_path(run_dir, f"round_transition_{round2_id}.json"))
            round2_tasks = json.loads(
                (run_dir / "investigation" / f"round_tasks_{round2_id}.json").read_text(
                    encoding="utf-8"
                )
            )
            task_context = round2_tasks[0]["inputs"]["round_coordination_context"]

            self.assertEqual("provided", transition_artifact["coordination_context"]["context_status"])
            self.assertEqual("supplemental-investigation", transition_artifact["round_mode"])
            self.assertEqual("context-packet-001", transition_artifact["context_packet_id"])
            self.assertEqual("challenge-ticket-001", transition_artifact["target_challenge_id"])
            self.assertEqual("round-brief-001", transition_artifact["round_brief_id"])
            self.assertEqual(
                ["cli-focus-001", "request-focus-001"],
                transition_artifact["primary_focus_refs"],
            )
            self.assertEqual("context-packet-001", task_context["context_packet_id"])
            self.assertIn("does not restrict", task_context["semantics"])
            self.assertEqual("round-brief-001", open_payload["summary"]["round_brief_id"])
            self.assertIn("challenge-ticket-001", open_payload["board_handoff"]["candidate_ids"])

    def test_ingress_import_execution_reconnects_to_reporting_mainline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            scaffold_payload = run_script(
                script_path("scaffold-mission-run"),
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
                script_path("prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("normalize-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "social-investigator",
            )
            import_payload = run_script(
                script_path("normalize-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "environmental-investigator",
            )

            discourse_payload = run_script(script_path("discover-discourse-issues"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            environment_payload = run_script(script_path("aggregate-environment-evidence"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            run_script(
                script_path("suggest-evidence-lanes"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--input-path",
                discourse_payload["summary"]["output_path"],
            )
            research_issue_payload = run_script(
                script_path("materialize-research-issue-surface"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--input-path",
                discourse_payload["summary"]["output_path"],
            )
            coverage_ref = primary_successor_evidence_ref(
                {
                    "environment_aggregation": environment_payload,
                    "research_issue_surface": research_issue_payload,
                    "discourse_issues": discourse_payload,
                }
            )
            issue_id = primary_research_issue_id({"research_issue_surface": research_issue_payload})
            seeded_hypothesis_id = scaffold_payload["summary"]["seeded_hypothesis_ids"][0]
            run_script(
                script_path("post-board-note"),
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
                script_path("update-hypothesis-status"),
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
                "environmental-investigator",
                "--linked-claim-id",
                issue_id,
                "--linked-artifact-ref",
                coverage_ref,
                "--confidence",
                "0.93",
            )
            submit_ready_council_support(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                issue_id=issue_id,
                evidence_ref=coverage_ref,
            )

            approve_report_basis_transition(run_dir)
            run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            handoff_payload = run_script(script_path("materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            decision_payload = run_script(script_path("draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            import_artifact = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            handoff_artifact = load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"))
            decision_artifact = load_json(reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json"))
            report_basis_artifact = load_json(report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json"))

            self.assertEqual(4, import_payload["summary"]["normalized_step_count"])
            self.assertEqual(4, import_artifact["completed_count"])
            self.assertEqual(0, import_artifact["failed_count"])
            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertEqual("reporting-ready", handoff_artifact["handoff_status"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual("frozen", handoff_artifact["report_basis_status"])
            self.assertEqual("frozen", report_basis_artifact["report_basis_status"])
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual("ready", decision_artifact["publication_readiness"])


if __name__ == "__main__":
    unittest.main()

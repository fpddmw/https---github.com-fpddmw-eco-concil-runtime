from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import board_path, investigation_path, load_json, run_script, script_path, seed_analysis_chain, write_json

RUN_ID = "run-board-001"
ROUND_ID = "round-board-001"
ROUND2_ID = "round-board-002"


def build_mission_fixture(root: Path) -> Path:
    youtube_path = root / "mission_youtube.json"
    openaq_path = root / "mission_openaq.json"
    write_json(
        youtube_path,
        [
            {
                "query": "nyc smoke wildfire",
                "video_id": "vid-mission-001",
                "video": {
                    "id": "vid-mission-001",
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
                    "artifact_path": str(youtube_path),
                    "query_text": "nyc smoke wildfire",
                },
                {
                    "source_skill": "openaq-data-fetch",
                    "artifact_path": str(openaq_path),
                    "source_mode": "test-fixture",
                },
            ],
        },
    )
    return mission_path


class BoardWorkflowTests(unittest.TestCase):
    def test_board_skills_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            coverage_payload = run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            link_ref = outputs["link_evidence"]["artifact_refs"][0]["artifact_ref"]
            coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]

            note_payload = run_script(
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
                "Evidence coverage is strong enough to open a focused challenge review.",
                "--linked-artifact-ref",
                link_ref,
                "--linked-artifact-ref",
                coverage_ref,
            )
            hypothesis_payload = run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Smoke over NYC was materially significant",
                "--statement",
                "Public smoke reports are backed by elevated PM2.5 observations.",
                "--status",
                "active",
                "--owner-role",
                "environmentalist",
                "--linked-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--confidence",
                "0.82",
            )
            challenge_payload = run_script(
                script_path("eco-open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Check whether reported smoke impact is overstated",
                "--challenge-statement",
                "Evaluate whether public narratives overstate the severity relative to observation coverage.",
                "--target-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--target-hypothesis-id",
                hypothesis_payload["canonical_ids"][0],
                "--priority",
                "high",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                coverage_ref,
            )
            delta_payload = run_script(
                script_path("eco-read-board-delta"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual(1, len(note_payload["canonical_ids"]))
            self.assertEqual(1, len(hypothesis_payload["canonical_ids"]))
            self.assertEqual(1, len(challenge_payload["canonical_ids"]))
            self.assertGreaterEqual(delta_payload["result_count"], 3)
            self.assertTrue(Path(delta_payload["summary"]["db_path"]).exists())
            self.assertEqual(1, delta_payload["round_state"]["note_count"])
            self.assertEqual(1, delta_payload["round_state"]["hypothesis_count"])
            self.assertEqual(1, delta_payload["round_state"]["challenge_ticket_count"])
            self.assertEqual(0, delta_payload["round_state"]["task_count"])
            self.assertEqual(1, len(delta_payload["round_state"]["hypotheses"]))
            self.assertEqual(1, len(delta_payload["round_state"]["challenge_tickets"]))

            board = load_json(board_path(run_dir))
            rounds = board.get("rounds", {})
            assert isinstance(rounds, dict)
            round_state = rounds[ROUND_ID]
            self.assertEqual(1, len(round_state["notes"]))
            self.assertEqual(1, len(round_state["hypotheses"]))
            self.assertEqual(1, len(round_state["challenge_tickets"]))

    def test_board_c2_skills_organize_round_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            coverage_payload = run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]

            hypothesis_payload = run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Smoke over NYC was materially significant",
                "--statement",
                "Public smoke reports are backed by elevated PM2.5 observations.",
                "--status",
                "active",
                "--owner-role",
                "environmentalist",
                "--linked-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--confidence",
                "0.82",
            )
            challenge_payload = run_script(
                script_path("eco-open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Check whether reported smoke impact is overstated",
                "--challenge-statement",
                "Evaluate whether public narratives overstate the severity relative to observation coverage.",
                "--target-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--target-hypothesis-id",
                hypothesis_payload["canonical_ids"][0],
                "--priority",
                "high",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                coverage_ref,
            )
            task_payload = run_script(
                script_path("eco-claim-board-task"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Review contradiction-leaning evidence links",
                "--task-text",
                "Compare contradiction-leaning links against coverage summary before closing the challenge.",
                "--task-type",
                "challenge-follow-up",
                "--owner-role",
                "challenger",
                "--priority",
                "high",
                "--source-ticket-id",
                challenge_payload["canonical_ids"][0],
                "--source-hypothesis-id",
                hypothesis_payload["canonical_ids"][0],
                "--linked-artifact-ref",
                coverage_ref,
            )
            close_payload = run_script(
                script_path("eco-close-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--ticket-id",
                challenge_payload["canonical_ids"][0],
                "--resolution",
                "bounded-after-task-claim",
                "--resolution-note",
                "A follow-up task has been claimed and the contradiction scope is now bounded for the next round.",
                "--related-task-id",
                task_payload["canonical_ids"][0],
            )
            summary_payload = run_script(
                script_path("eco-summarize-board-state"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            brief_payload = run_script(
                script_path("eco-materialize-board-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            board = load_json(board_path(run_dir))
            rounds = board.get("rounds", {})
            assert isinstance(rounds, dict)
            round_state = rounds[ROUND_ID]
            self.assertEqual(1, len(round_state["tasks"]))
            self.assertEqual("closed", round_state["challenge_tickets"][0]["status"])
            self.assertEqual("claimed", round_state["tasks"][0]["status"])

            summary_file = Path(summary_payload["artifact_refs"][0]["artifact_path"])
            summary_data = load_json(summary_file)
            self.assertEqual("deliberation-plane", summary_payload["summary"]["state_source"])
            self.assertEqual("completed", summary_payload["deliberation_sync"]["status"])
            self.assertEqual(1, summary_data["counts"]["tasks_total"])
            self.assertEqual(1, summary_data["counts"]["challenge_closed"])
            self.assertEqual("in-flight", summary_data["status_rollup"])
            self.assertEqual("deliberation-plane", summary_data["state_source"])
            self.assertTrue(summary_data["db_path"].endswith("analytics/signal_plane.sqlite"))

            brief_file = Path(brief_payload["artifact_refs"][0]["artifact_path"])
            brief_text = brief_file.read_text(encoding="utf-8")
            self.assertEqual("deliberation-plane", brief_payload["summary"]["state_source"])
            self.assertEqual("completed", brief_payload["deliberation_sync"]["status"])
            self.assertIn("Smoke over NYC was materially significant", brief_text)
            self.assertIn("State source: deliberation-plane", brief_text)
            self.assertIn(task_payload["canonical_ids"][0], brief_text)
            self.assertEqual("closed", close_payload["summary"]["operation"])

    def test_board_brief_reads_deliberation_plane_without_summary_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

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
                "--note-text",
                "Direct board brief generation should not require a summary artifact.",
            )
            hypothesis_payload = run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Direct deliberation-plane board brief",
                "--statement",
                "A board brief should be generated from shared deliberation state.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--confidence",
                "0.7",
            )

            brief_payload = run_script(
                script_path("eco-materialize-board-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            brief_file = Path(brief_payload["artifact_refs"][0]["artifact_path"])
            brief_text = brief_file.read_text(encoding="utf-8")

            self.assertFalse((run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").exists())
            self.assertEqual("deliberation-plane", brief_payload["summary"]["state_source"])
            self.assertEqual("completed", brief_payload["deliberation_sync"]["status"])
            self.assertIn("State source: deliberation-plane", brief_text)
            self.assertIn(hypothesis_payload["canonical_ids"][0], brief_text)

    def test_open_investigation_round_preserves_prior_round_and_carries_state_from_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = build_mission_fixture(root)

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
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            board_before = load_json(board_path(run_dir))
            seeded_hypothesis_id = board_before["rounds"][ROUND_ID]["hypotheses"][0]["hypothesis_id"]

            coverage_payload = run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
            challenge_payload = run_script(
                script_path("eco-open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Check whether reported smoke impact is overstated",
                "--challenge-statement",
                "Evaluate whether public narratives overstate the severity relative to observation coverage.",
                "--target-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--target-hypothesis-id",
                seeded_hypothesis_id,
                "--priority",
                "high",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                coverage_ref,
            )
            open_task_payload = run_script(
                script_path("eco-claim-board-task"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Collect additional cross-source smoke timing evidence",
                "--task-text",
                "Query more public and physical records to sharpen temporal alignment.",
                "--task-type",
                "board-follow-up",
                "--owner-role",
                "moderator",
                "--priority",
                "high",
                "--linked-artifact-ref",
                coverage_ref,
                "--related-id",
                outputs["cluster_claims"]["canonical_ids"][0],
            )

            write_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"),
                {
                    "ranked_actions": [
                        {
                            "action_id": "action-followup-public-001",
                            "action_kind": "expand-public-evidence",
                            "assigned_role": "sociologist",
                            "priority": "high",
                            "objective": "Broaden public evidence around smoke timing and intensity.",
                            "reason": "Current public evidence is still narrow in timing coverage.",
                            "brief_context": "Need more temporal corroboration before promotion.",
                            "source_ids": [outputs["cluster_claims"]["canonical_ids"][0]],
                            "evidence_refs": [coverage_ref],
                            "target": {"hypothesis_id": seeded_hypothesis_id},
                        }
                    ],
                    "action_count": 1,
                },
            )

            board_path(run_dir).unlink()

            open_round_payload = run_script(
                script_path("eco-open-investigation-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND2_ID,
                "--source-round-id",
                ROUND_ID,
            )

            board_after = load_json(board_path(run_dir))
            round2_state = board_after["rounds"][ROUND2_ID]
            round2_tasks = json.loads(investigation_path(run_dir, f"round_tasks_{ROUND2_ID}.json").read_text(encoding="utf-8"))
            transition_artifact = load_json(run_dir / "runtime" / f"round_transition_{ROUND2_ID}.json")
            connection = sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite")
            try:
                transition_row = connection.execute(
                    """
                    SELECT source_round_id, event_id, board_revision, artifact_path
                    FROM round_transitions
                    WHERE transition_id = ?
                    """,
                    (open_round_payload["canonical_ids"][0],),
                ).fetchone()
            finally:
                connection.close()

            self.assertIn(ROUND_ID, board_after["rounds"])
            self.assertIn(ROUND2_ID, board_after["rounds"])
            self.assertEqual("deliberation-plane", open_round_payload["summary"]["write_surface"])
            self.assertTrue(Path(open_round_payload["summary"]["db_path"]).exists())
            self.assertEqual(1, len(round2_state["hypotheses"]))
            self.assertEqual(3, len(round2_state["tasks"]))
            self.assertEqual([], round2_state["challenge_tickets"])
            self.assertEqual(1, len(round2_state["notes"]))
            self.assertIn("Follow-up round opened", round2_state["notes"][0]["note_text"])
            self.assertEqual(2, len(round2_tasks))
            self.assertEqual([ROUND_ID], round2_tasks[0]["inputs"]["prior_round_ids"])
            self.assertEqual("deliberation-plane", transition_artifact["write_surface"])
            self.assertEqual(open_round_payload["summary"]["db_path"], transition_artifact["db_path"])
            self.assertEqual("up-to-current", transition_artifact["cross_round_query_hints"]["public_signals"]["round_scope"])
            self.assertEqual("up-to-current", transition_artifact["cross_round_query_hints"]["environment_signals"]["round_scope"])
            self.assertEqual(ROUND_ID, transition_artifact["prior_round_ids"][0])
            self.assertIn("eco-prepare-round", open_round_payload["board_handoff"]["suggested_next_skills"])
            self.assertTrue(
                any(task.get("carryover_from_task_id") == open_task_payload["canonical_ids"][0] for task in round2_state["tasks"])
            )
            self.assertTrue(
                any(task.get("source_ticket_id") == challenge_payload["canonical_ids"][0] for task in round2_state["tasks"])
            )
            self.assertIsNotNone(transition_row)
            assert transition_row is not None
            self.assertEqual(ROUND_ID, transition_row[0])
            self.assertEqual(open_round_payload["summary"]["event_id"], transition_row[1])
            self.assertEqual(open_round_payload["summary"]["board_revision"], transition_row[2])
            self.assertEqual(
                str((run_dir / "runtime" / f"round_transition_{ROUND2_ID}.json").resolve()),
                transition_row[3],
            )

    def test_open_investigation_round_fallback_uses_shared_source_role_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            regulations_path = root / "regulations_comments.json"
            flood_path = root / "flood.json"
            write_json(regulations_path, {"data": []})
            write_json(flood_path, {"daily": {}})
            mission_path = root / "mission_shared_sources.json"
            write_json(
                mission_path,
                {
                    "schema_version": "1.0.0",
                    "run_id": RUN_ID,
                    "topic": "Shared source role inheritance",
                    "objective": "Ensure follow-up round fallback tasks reuse the shared source catalog.",
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
                            "title": "Follow-up source inheritance",
                            "statement": "Fallback round planning should preserve source-role assignments from mission inputs.",
                            "confidence": 0.55,
                        }
                    ],
                    "artifact_imports": [
                        {
                            "source_skill": "regulationsgov-comments-fetch",
                            "artifact_path": str(regulations_path),
                        },
                        {
                            "source_skill": "open-meteo-flood-fetch",
                            "artifact_path": str(flood_path),
                        },
                    ],
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
            (investigation_path(run_dir, f"round_tasks_{ROUND_ID}.json")).unlink()

            run_script(
                script_path("eco-open-investigation-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND2_ID,
                "--source-round-id",
                ROUND_ID,
            )

            round2_tasks = json.loads(
                investigation_path(run_dir, f"round_tasks_{ROUND2_ID}.json").read_text(
                    encoding="utf-8"
                )
            )
            role_to_sources = {
                task["assigned_role"]: task["inputs"]["source_skills"]
                for task in round2_tasks
                if isinstance(task, dict)
            }

            self.assertEqual(
                ["regulationsgov-comments-fetch"],
                role_to_sources["sociologist"],
            )
            self.assertEqual(
                ["open-meteo-flood-fetch"],
                role_to_sources["environmentalist"],
            )

    def test_board_delta_cursor_filters_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            first_note = run_script(
                script_path("eco-post-board-note"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--note-text",
                "Initial board note.",
            )
            first_event_id = first_note["summary"]["event_id"]
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
                "--note-text",
                "Second board note.",
            )

            delta_payload = run_script(
                script_path("eco-read-board-delta"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--after-event-id",
                first_event_id,
            )
            self.assertEqual(1, delta_payload["result_count"])

    def test_db_first_board_mutation_recreates_board_export_when_json_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            first_note = run_script(
                script_path("eco-post-board-note"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--note-text",
                "Initial DB-first note.",
            )
            run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "DB-first board export recreation",
                "--statement",
                "Deleting the board JSON should not prevent further deliberation writes.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--confidence",
                "0.71",
            )

            board_file = board_path(run_dir)
            board_file.unlink()

            second_note = run_script(
                script_path("eco-post-board-note"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--note-text",
                "DB-first write should rebuild the board export.",
            )

            board = load_json(board_file)
            rounds = board.get("rounds", {})
            assert isinstance(rounds, dict)
            round_state = rounds[ROUND_ID]

            self.assertEqual("deliberation-plane", second_note["summary"]["write_surface"])
            self.assertTrue(Path(second_note["summary"]["db_path"]).exists())
            self.assertEqual(2, len(round_state["notes"]))
            self.assertEqual(1, len(round_state["hypotheses"]))
            self.assertNotEqual(
                first_note["canonical_ids"][0],
                second_note["canonical_ids"][0],
            )

    def test_board_readers_fallback_to_db_when_board_json_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

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
                "--note-text",
                "Reader fallback should survive a missing board JSON export.",
            )
            hypothesis_payload = run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "DB-only reader fallback",
                "--statement",
                "Round readers should use the deliberation plane when JSON is absent.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--confidence",
                "0.68",
            )

            board_path(run_dir).unlink()

            delta_payload = run_script(
                script_path("eco-read-board-delta"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            brief_payload = run_script(
                script_path("eco-materialize-board-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            brief_file = Path(brief_payload["artifact_refs"][0]["artifact_path"])
            brief_text = brief_file.read_text(encoding="utf-8")

            self.assertEqual("completed", delta_payload["deliberation_sync"]["status"])
            self.assertEqual("db-only", delta_payload["deliberation_sync"]["sync_mode"])
            self.assertEqual(1, delta_payload["round_state"]["note_count"])
            self.assertEqual(1, delta_payload["round_state"]["hypothesis_count"])
            self.assertEqual("completed", brief_payload["deliberation_sync"]["status"])
            self.assertEqual("db-only", brief_payload["deliberation_sync"]["sync_mode"])
            self.assertEqual("deliberation-plane", brief_payload["summary"]["state_source"])
            self.assertIn(hypothesis_payload["canonical_ids"][0], brief_text)

    def test_concurrent_board_writes_preserve_both_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            def post_note(note_text: str) -> dict[str, object]:
                return run_script(
                    script_path("eco-post-board-note"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--author-role",
                    "moderator",
                    "--note-text",
                    note_text,
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                first_future = executor.submit(post_note, "Concurrent note A.")
                second_future = executor.submit(post_note, "Concurrent note B.")
                first_payload = first_future.result()
                second_payload = second_future.result()

            board = load_json(board_path(run_dir))
            rounds = board.get("rounds", {})
            assert isinstance(rounds, dict)
            round_state = rounds[ROUND_ID]
            self.assertEqual(2, len(round_state["notes"]))
            self.assertEqual(2, len(board["events"]))
            self.assertGreaterEqual(int(board.get("board_revision") or 0), 2)
            self.assertNotEqual(first_payload["canonical_ids"][0], second_payload["canonical_ids"][0])


if __name__ == "__main__":
    unittest.main()

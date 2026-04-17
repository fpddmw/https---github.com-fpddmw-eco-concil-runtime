from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import (
    board_path,
    investigation_path,
    load_json,
    run_script,
    runtime_src_path,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
)

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


def load_deliberation_row(
    run_dir: Path,
    *,
    table_name: str,
    id_column: str,
    record_id: str,
) -> dict[str, Any]:
    connection = sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite")
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            f"SELECT * FROM {table_name} WHERE {id_column} = ?",
            (record_id,),
        ).fetchone()
    finally:
        connection.close()
    assert row is not None
    payload = dict(row)
    payload["raw_json"] = json.loads(payload["raw_json"])
    return payload


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

    def test_hypothesis_update_executes_from_council_proposal_only_and_persists_metadata(
        self,
    ) -> None:
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
            claim_id = outputs["cluster_claims"]["canonical_ids"][0]
            coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]

            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "create-hypothesis",
                            "action_kind": "create-hypothesis",
                            "agent_role": "environmentalist",
                            "assigned_role": "environmentalist",
                            "target_kind": "hypothesis",
                            "target_id": "hypothesis-proposal-001",
                            "target_claim_id": claim_id,
                            "title": "Proposal-backed smoke severity hypothesis",
                            "statement": "Council wants the smoke-severity judgement tracked directly on the board.",
                            "rationale": "Council wants the smoke-severity judgement tracked directly on the board.",
                            "confidence": 0.74,
                            "decision_source": "agent-council",
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [coverage_ref],
                            "lineage": [claim_id],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--proposal-id",
                proposal_id,
            )

            board = load_json(board_path(run_dir))
            hypothesis = board["rounds"][ROUND_ID]["hypotheses"][0]
            row = load_deliberation_row(
                run_dir,
                table_name="hypothesis_cards",
                id_column="hypothesis_id",
                record_id=payload["canonical_ids"][0],
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("created", payload["summary"]["operation"])
            self.assertEqual("deliberation-plane", payload["summary"]["write_surface"])
            self.assertTrue(Path(payload["summary"]["db_path"]).exists())
            self.assertEqual("hypothesis-proposal-001", payload["canonical_ids"][0])
            self.assertEqual("agent-council", payload["summary"]["decision_source"])
            self.assertEqual(proposal_id, payload["summary"]["proposal_id"])
            self.assertEqual("active", hypothesis["status"])
            self.assertEqual("environmentalist", hypothesis["owner_role"])
            self.assertEqual("agent-council", hypothesis["decision_source"])
            self.assertIn(claim_id, hypothesis["linked_claim_ids"])
            self.assertIn(coverage_ref, hypothesis["evidence_refs"])
            self.assertIn(proposal_id, hypothesis["source_ids"])
            self.assertIn(claim_id, hypothesis["source_ids"])
            self.assertIn(proposal_id, hypothesis["lineage"])
            self.assertIn(claim_id, hypothesis["lineage"])
            self.assertEqual(proposal_id, hypothesis["provenance"]["proposal_id"])
            self.assertEqual("unit-test", hypothesis["provenance"]["source"])
            self.assertEqual("agent-council", row["decision_source"])
            self.assertIn(coverage_ref, json.loads(row["evidence_refs_json"]))
            self.assertIn(proposal_id, json.loads(row["source_ids_json"]))
            self.assertIn(proposal_id, json.loads(row["lineage_json"]))
            self.assertEqual(proposal_id, json.loads(row["provenance_json"])["proposal_id"])
            self.assertEqual("agent-council", row["raw_json"]["decision_source"])
            self.assertIn(coverage_ref, row["raw_json"]["evidence_refs"])
            self.assertIn(proposal_id, row["raw_json"]["source_ids"])
            self.assertIn(proposal_id, row["raw_json"]["lineage"])

    def test_open_challenge_executes_from_council_proposal_only_and_persists_metadata(
        self,
    ) -> None:
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
            claim_id = outputs["cluster_claims"]["canonical_ids"][0]
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
                claim_id,
                "--confidence",
                "0.82",
            )
            hypothesis_id = hypothesis_payload["canonical_ids"][0]

            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "open-challenge",
                            "action_kind": "open-challenge-ticket",
                            "agent_role": "challenger",
                            "assigned_role": "challenger",
                            "target_kind": "hypothesis",
                            "target_id": hypothesis_id,
                            "target_claim_id": claim_id,
                            "proposed_ticket_id": "challenge-proposal-001",
                            "title": "Re-test smoke severity framing",
                            "challenge_statement": "Council wants a contradiction ticket opened directly from the proposal queue.",
                            "rationale": "Council wants a contradiction ticket opened directly from the proposal queue.",
                            "priority": "high",
                            "decision_source": "agent-council",
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [coverage_ref],
                            "lineage": [hypothesis_id, claim_id],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--proposal-id",
                proposal_id,
            )

            board = load_json(board_path(run_dir))
            ticket = board["rounds"][ROUND_ID]["challenge_tickets"][0]
            row = load_deliberation_row(
                run_dir,
                table_name="challenge_tickets",
                id_column="ticket_id",
                record_id=payload["canonical_ids"][0],
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("deliberation-plane", payload["summary"]["write_surface"])
            self.assertTrue(Path(payload["summary"]["db_path"]).exists())
            self.assertEqual("challenge-proposal-001", payload["canonical_ids"][0])
            self.assertEqual(proposal_id, payload["summary"]["proposal_id"])
            self.assertEqual("agent-council", ticket["decision_source"])
            self.assertEqual("challenger", ticket["owner_role"])
            self.assertEqual("high", ticket["priority"])
            self.assertEqual(claim_id, ticket["target_claim_id"])
            self.assertEqual(hypothesis_id, ticket["target_hypothesis_id"])
            self.assertIn(coverage_ref, ticket["evidence_refs"])
            self.assertIn(proposal_id, ticket["source_ids"])
            self.assertIn(proposal_id, ticket["lineage"])
            self.assertEqual(proposal_id, ticket["provenance"]["proposal_id"])
            self.assertEqual("unit-test", ticket["provenance"]["source"])
            self.assertEqual("agent-council", row["decision_source"])
            self.assertIn(coverage_ref, json.loads(row["evidence_refs_json"]))
            self.assertIn(proposal_id, json.loads(row["source_ids_json"]))
            self.assertIn(proposal_id, json.loads(row["lineage_json"]))
            self.assertEqual(proposal_id, json.loads(row["provenance_json"])["proposal_id"])
            self.assertEqual("agent-council", row["raw_json"]["decision_source"])
            self.assertEqual(hypothesis_id, row["raw_json"]["target_hypothesis_id"])
            self.assertIn(coverage_ref, row["raw_json"]["evidence_refs"])
            self.assertIn(proposal_id, row["raw_json"]["source_ids"])
            self.assertIn(proposal_id, row["raw_json"]["lineage"])

    def test_close_challenge_executes_from_council_proposal_only_and_persists_metadata(
        self,
    ) -> None:
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
            claim_id = outputs["cluster_claims"]["canonical_ids"][0]
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
                claim_id,
                "--confidence",
                "0.82",
            )
            hypothesis_id = hypothesis_payload["canonical_ids"][0]

            open_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "open-challenge",
                            "action_kind": "open-challenge-ticket",
                            "agent_role": "challenger",
                            "assigned_role": "challenger",
                            "target_kind": "hypothesis",
                            "target_id": hypothesis_id,
                            "target_claim_id": claim_id,
                            "proposed_ticket_id": "challenge-proposal-close-001",
                            "title": "Re-test smoke severity framing before close",
                            "challenge_statement": "Open a board ticket so closure can be executed from a later proposal.",
                            "rationale": "Open a board ticket so closure can be executed from a later proposal.",
                            "priority": "high",
                            "decision_source": "agent-council",
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [coverage_ref],
                            "lineage": [hypothesis_id, claim_id],
                        }
                    ],
                },
            )
            open_proposal_id = open_bundle["proposals"][0]["proposal_id"]
            open_payload = run_script(
                script_path("eco-open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--proposal-id",
                open_proposal_id,
            )
            ticket_id = open_payload["canonical_ids"][0]

            close_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "close-challenge",
                            "action_kind": "close-challenge-ticket",
                            "agent_role": "moderator",
                            "assigned_role": "moderator",
                            "target_kind": "challenge-ticket",
                            "target_id": ticket_id,
                            "resolution": "dismissed",
                            "resolution_note": "Council judged the contradiction sufficiently answered by the synthesized evidence set.",
                            "rationale": "Council judged the contradiction sufficiently answered by the synthesized evidence set.",
                            "related_task_ids": ["task-close-proposal-001"],
                            "response_to_ids": [open_proposal_id],
                            "decision_source": "agent-council",
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [coverage_ref],
                            "lineage": [ticket_id, hypothesis_id],
                        }
                    ],
                },
            )
            close_proposal_id = close_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-close-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--proposal-id",
                close_proposal_id,
            )

            board = load_json(board_path(run_dir))
            ticket = board["rounds"][ROUND_ID]["challenge_tickets"][0]
            row = load_deliberation_row(
                run_dir,
                table_name="challenge_tickets",
                id_column="ticket_id",
                record_id=ticket_id,
            )
            row_provenance = json.loads(row["provenance_json"])
            row_source_ids = json.loads(row["source_ids_json"])
            row_lineage = json.loads(row["lineage_json"])

            self.assertEqual("completed", payload["status"])
            self.assertEqual("closed", payload["summary"]["operation"])
            self.assertEqual(ticket_id, payload["summary"]["ticket_id"])
            self.assertEqual("deliberation-plane", payload["summary"]["write_surface"])
            self.assertEqual(close_proposal_id, payload["summary"]["proposal_id"])
            self.assertEqual("closed", ticket["status"])
            self.assertEqual("dismissed", ticket["resolution"])
            self.assertEqual("moderator", ticket["closed_by_role"])
            self.assertEqual("agent-council", ticket["decision_source"])
            self.assertIn("task-close-proposal-001", ticket["related_task_ids"])
            self.assertIn(coverage_ref, ticket["evidence_refs"])
            self.assertIn(close_proposal_id, ticket["source_ids"])
            self.assertIn(ticket_id, ticket["source_ids"])
            self.assertIn(open_proposal_id, ticket["lineage"])
            self.assertIn(close_proposal_id, ticket["lineage"])
            self.assertEqual(close_proposal_id, ticket["provenance"]["proposal_id"])
            self.assertEqual("unit-test", ticket["provenance"]["source"])
            self.assertEqual(2, len(ticket["history"]))
            self.assertEqual("closed", ticket["history"][-1]["status"])
            self.assertEqual("dismissed", ticket["history"][-1]["resolution"])
            self.assertIn(close_proposal_id, ticket["history"][-1]["source_ids"])
            self.assertEqual("agent-council", row["decision_source"])
            self.assertIn(coverage_ref, json.loads(row["evidence_refs_json"]))
            self.assertIn(close_proposal_id, row_source_ids)
            self.assertIn(ticket_id, row_source_ids)
            self.assertIn(open_proposal_id, row_lineage)
            self.assertIn(close_proposal_id, row_lineage)
            self.assertEqual(close_proposal_id, row_provenance["proposal_id"])
            self.assertEqual("agent-council", row["raw_json"]["decision_source"])
            self.assertEqual("closed", row["raw_json"]["status"])
            self.assertEqual("dismissed", row["raw_json"]["resolution"])
            self.assertEqual([open_proposal_id], row["raw_json"]["response_to_ids"])
            self.assertIn(close_proposal_id, row["raw_json"]["source_ids"])
            self.assertIn(open_proposal_id, row["raw_json"]["lineage"])

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

            round2_tasks = json.loads(
                investigation_path(run_dir, f"round_tasks_{ROUND2_ID}.json").read_text(
                    encoding="utf-8"
                )
            )
            transition_artifact = load_json(
                run_dir / "runtime" / f"round_transition_{ROUND2_ID}.json"
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
            self.assertEqual(
                "deliberation-plane-round-tasks",
                open_round_payload["summary"]["source_task_source"],
            )
            self.assertEqual(
                "deliberation-plane-round-tasks",
                transition_artifact["source_task_source"],
            )
            self.assertFalse(
                transition_artifact["observed_inputs"]["source_task_artifact_present"]
            )
            self.assertTrue(
                transition_artifact["observed_inputs"]["source_task_present"]
            )
            self.assertFalse(
                any(
                    warning.get("code") == "missing-source-round-tasks"
                    for warning in open_round_payload["warnings"]
                    if isinstance(warning, dict)
                )
            )

    def test_open_investigation_round_reads_db_backed_actions_when_export_is_missing(self) -> None:
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
                "Smoke over NYC may be overstated",
                "--statement",
                "Public smoke reports may overstate severity relative to observed PM2.5 coverage.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--linked-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--confidence",
                "0.52",
            )
            run_script(
                script_path("eco-open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Check whether smoke narrative is overstated",
                "--challenge-statement",
                "Re-test whether the strongest smoke narrative exceeds evidence coverage.",
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
            run_script(
                script_path("eco-summarize-board-state"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-materialize-board-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            next_actions_artifact = load_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")
            )
            expected_action_ids = [
                action["action_id"]
                for action in next_actions_artifact["ranked_actions"][:3]
                if isinstance(action, dict) and action.get("action_id")
            ]
            self.assertTrue(expected_action_ids)

            investigation_path(run_dir, f"next_actions_{ROUND_ID}.json").unlink()

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

            round2_state = load_json(board_path(run_dir))["rounds"][ROUND2_ID]
            transition_artifact = load_json(
                run_dir / "runtime" / f"round_transition_{ROUND2_ID}.json"
            )

            self.assertEqual(
                "deliberation-plane-actions",
                transition_artifact["source_next_actions_source"],
            )
            self.assertFalse(
                transition_artifact["observed_inputs"][
                    "source_next_actions_artifact_present"
                ]
            )
            self.assertTrue(
                transition_artifact["observed_inputs"]["source_next_actions_present"]
            )
            self.assertEqual(
                "deliberation-plane-actions",
                open_round_payload["summary"]["source_next_actions_source"],
            )
            self.assertFalse(
                any(
                    warning.get("code") == "missing-next-actions"
                    for warning in open_round_payload["warnings"]
                    if isinstance(warning, dict)
                )
            )
            self.assertTrue(
                any(
                    task.get("carryover_from_action_id") in expected_action_ids
                    for task in round2_state["tasks"]
                    if isinstance(task, dict)
                )
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

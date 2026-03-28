from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import tempfile
import unittest
from pathlib import Path

from _workflow_support import board_path, load_json, run_script, script_path, seed_analysis_chain

RUN_ID = "run-board-001"
ROUND_ID = "round-board-001"


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
            self.assertEqual(1, summary_data["counts"]["tasks_total"])
            self.assertEqual(1, summary_data["counts"]["challenge_closed"])
            self.assertEqual("in-flight", summary_data["status_rollup"])

            brief_file = Path(brief_payload["artifact_refs"][0]["artifact_path"])
            brief_text = brief_file.read_text(encoding="utf-8")
            self.assertIn("Smoke over NYC was materially significant", brief_text)
            self.assertIn(task_payload["canonical_ids"][0], brief_text)
            self.assertEqual("closed", close_payload["summary"]["operation"])

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
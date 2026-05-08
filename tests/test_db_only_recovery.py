from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    board_path,
    load_json,
    reporting_path,
    run_kernel,
    run_script,
    script_path,
)
from tests.test_reporting_publish_workflow import (
    ROUND_ID as REPORTING_ROUND_ID,
    RUN_ID as REPORTING_RUN_ID,
    prepare_ready_round,
)
from tests.test_spatiotemporal_relation_taxonomy import (
    ROUND_ID as RELATION_ROUND_ID,
    RUN_ID as RELATION_RUN_ID,
    run_structured_relation_detection,
)


RUN_ID = "run-db-only-recovery-001"
ROUND_ID = "round-db-only-recovery-001"


def execute_db(db_path: Path, query: str, params: tuple[str, ...]) -> None:
    connection = sqlite3.connect(db_path)
    try:
        with connection:
            connection.execute(query, params)
    finally:
        connection.close()


class DbOnlyRecoveryTests(unittest.TestCase):
    def test_board_readers_recover_from_deliberation_db_when_board_export_is_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            note_payload = run_script(
                script_path("post-board-note"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--note-text",
                "DB-only recovery should survive a missing board export.",
            )
            hypothesis_payload = run_script(
                script_path("update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "DB-only board recovery",
                "--statement",
                "Board readers should recover round state from the deliberation plane.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--linked-artifact-ref",
                note_payload["artifact_refs"][0]["artifact_ref"],
                "--confidence",
                "0.72",
            )

            board_path(run_dir).unlink()

            delta_payload = run_script(
                script_path("query-board-delta"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            brief_payload = run_script(
                script_path("materialize-board-brief"),
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

    def test_relation_query_recovers_from_analysis_db_when_relation_artifact_is_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])
            relation_artifact = (
                run_dir
                / "analytics"
                / f"spatiotemporal_relation_cues_{RELATION_ROUND_ID}.json"
            )
            self.assertTrue(relation_artifact.exists())

            relation_artifact.unlink()

            query_payload = run_script(
                script_path("query-spatiotemporal-relations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RELATION_RUN_ID,
                "--round-id",
                RELATION_ROUND_ID,
                "--relation-id",
                relation_id,
                "--include-result-sets",
            )

            self.assertEqual("completed", query_payload["status"])
            self.assertEqual(1, query_payload["summary"]["matching_relation_count"])
            self.assertEqual(relation_id, query_payload["relations"][0]["relation_id"])
            self.assertEqual("candidate", query_payload["relations"][0]["relation_status"])
            self.assertFalse(query_payload["items"][0]["artifact_present"])
            self.assertFalse(query_payload["result_sets"][0]["artifact_present"])
            self.assertEqual(str(relation_artifact), query_payload["items"][0]["artifact_path"])

    def test_relation_packet_rematerializes_from_db_when_relation_artifact_is_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])
            relation_artifact = (
                run_dir
                / "analytics"
                / f"spatiotemporal_relation_cues_{RELATION_ROUND_ID}.json"
            )
            relation_artifact.unlink()

            packet_payload = run_script(
                script_path("materialize-spatiotemporal-relation-evidence-packet"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RELATION_RUN_ID,
                "--round-id",
                RELATION_ROUND_ID,
                "--relation-id",
                relation_id,
            )
            packet_path = Path(packet_payload["summary"]["output_path"])
            packet = load_json(packet_path)

            self.assertEqual("completed", packet_payload["status"])
            self.assertTrue(packet_path.exists())
            self.assertEqual([relation_id], packet["accepted_relation_cue_ids"])
            self.assertEqual(1, packet["relation_query_summary"]["matching_relation_count"])
            self.assertIn(
                "relation-cue-artifact-missing-db-recovered",
                {item["code"] for item in packet["warnings"]},
            )
            warning = next(
                item
                for item in packet["warnings"]
                if item["code"] == "relation-cue-artifact-missing-db-recovered"
            )
            self.assertEqual([str(relation_artifact)], warning["artifact_paths"])

            packet_path.unlink()

            rematerialized_payload = run_script(
                script_path("materialize-spatiotemporal-relation-evidence-packet"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RELATION_RUN_ID,
                "--round-id",
                RELATION_ROUND_ID,
                "--relation-id",
                relation_id,
            )

            self.assertEqual("completed", rematerialized_payload["status"])
            self.assertTrue(packet_path.exists())

    def test_reporting_state_and_draft_recover_when_reporting_artifacts_are_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            reporting_path(
                run_dir,
                f"reporting_handoff_{REPORTING_ROUND_ID}.json",
            ).unlink()
            reporting_path(
                run_dir,
                f"council_decision_draft_{REPORTING_ROUND_ID}.json",
            ).unlink()

            state_payload = run_kernel(
                "show-reporting-state",
                "--run-dir",
                str(run_dir),
                "--run-id",
                REPORTING_RUN_ID,
                "--round-id",
                REPORTING_ROUND_ID,
            )
            draft_payload = run_script(
                script_path("draft-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REPORTING_RUN_ID,
                "--round-id",
                REPORTING_ROUND_ID,
                "--role",
                "social-investigator",
            )
            draft = load_json(
                reporting_path(
                    run_dir,
                    f"expert_report_draft_social_investigator_{REPORTING_ROUND_ID}.json",
                )
            )

            self.assertEqual("completed", state_payload["status"])
            self.assertTrue(state_payload["summary"]["reporting_ready"])
            self.assertTrue(state_payload["surface"]["handoff_present"])
            self.assertTrue(state_payload["surface"]["decision_draft_present"])
            self.assertEqual("ready-to-publish", draft_payload["summary"]["report_status"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                draft["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-council-decision-draft",
                draft["decision_source"],
            )
            self.assertFalse(
                draft["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertTrue(draft["observed_inputs"]["reporting_handoff_present"])
            self.assertFalse(draft["observed_inputs"]["decision_artifact_present"])
            self.assertTrue(draft["observed_inputs"]["decision_present"])

    def test_reporting_publish_and_final_publication_recover_from_db_only_inputs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            for role in ("social-investigator", "environmental-investigator"):
                run_script(
                    script_path("draft-expert-report"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    REPORTING_RUN_ID,
                    "--round-id",
                    REPORTING_ROUND_ID,
                    "--role",
                    role,
                )
            for role in ("social-investigator", "environmental-investigator"):
                run_script(
                    script_path("publish-expert-report"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    REPORTING_RUN_ID,
                    "--round-id",
                    REPORTING_ROUND_ID,
                    "--role",
                    role,
                )
            run_script(
                script_path("publish-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REPORTING_RUN_ID,
                "--round-id",
                REPORTING_ROUND_ID,
            )

            for file_name in (
                f"reporting_handoff_{REPORTING_ROUND_ID}.json",
                f"council_decision_{REPORTING_ROUND_ID}.json",
                f"expert_report_social_investigator_{REPORTING_ROUND_ID}.json",
                f"expert_report_environmental_investigator_{REPORTING_ROUND_ID}.json",
            ):
                reporting_path(run_dir, file_name).unlink()

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REPORTING_RUN_ID,
                "--round-id",
                REPORTING_ROUND_ID,
            )
            publication = load_json(
                reporting_path(run_dir, f"final_publication_{REPORTING_ROUND_ID}.json")
            )

            self.assertEqual("completed", publication_payload["status"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                publication["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-council-decision",
                publication["decision_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["social_investigator_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["environmental_investigator_report_source"],
            )
            self.assertFalse(
                publication["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertFalse(publication["observed_inputs"]["decision_artifact_present"])
            self.assertFalse(
                publication["observed_inputs"]["social_investigator_report_artifact_present"]
            )
            self.assertFalse(
                publication["observed_inputs"][
                    "environmental_investigator_report_artifact_present"
                ]
            )
            self.assertTrue(publication["observed_inputs"]["reporting_handoff_present"])
            self.assertTrue(publication["observed_inputs"]["decision_present"])
            self.assertTrue(publication["observed_inputs"]["social_investigator_report_present"])
            self.assertTrue(
                publication["observed_inputs"]["environmental_investigator_report_present"]
            )

    def test_reporting_orphaned_draft_artifact_is_blocked_not_reused(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

            run_script(
                script_path("draft-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REPORTING_RUN_ID,
                "--round-id",
                REPORTING_ROUND_ID,
                "--role",
                "social-investigator",
            )
            execute_db(
                db_path,
                """
                DELETE FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (REPORTING_RUN_ID, REPORTING_ROUND_ID, "draft", "social-investigator"),
            )

            publish_payload = run_script(
                script_path("publish-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REPORTING_RUN_ID,
                "--round-id",
                REPORTING_ROUND_ID,
                "--role",
                "social-investigator",
            )

            self.assertEqual("blocked", publish_payload["status"])
            self.assertEqual("blocked", publish_payload["summary"]["operation"])
            self.assertIn(
                "missing-report-draft",
                {item["code"] for item in publish_payload["warnings"]},
            )
            self.assertTrue(
                any(
                    "orphaned from the reporting plane" in item["message"]
                    for item in publish_payload["warnings"]
                )
            )


if __name__ == "__main__":
    unittest.main()

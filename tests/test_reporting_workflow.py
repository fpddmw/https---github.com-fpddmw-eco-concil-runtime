from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, promotion_path, reporting_path, run_kernel, run_script, script_path, seed_analysis_chain

RUN_ID = "run-reporting-001"
ROUND_ID = "round-reporting-001"


class ReportingWorkflowTests(unittest.TestCase):
    def test_reporting_handoff_and_decision_finalize_promoted_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-derive-observation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
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
                "Round is ready to move into reporting and decision drafting.",
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
                "0.93",
            )

            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_payload = run_script(
                script_path("eco-materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("eco-draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"))
            decision_artifact = load_json(reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertEqual("reporting-ready", handoff_artifact["handoff_status"])
            self.assertTrue(handoff_payload["summary"]["reporting_ready"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual([], handoff_artifact["reporting_blockers"])
            self.assertEqual("promoted", handoff_artifact["promotion_status"])
            self.assertGreaterEqual(len(handoff_artifact["key_findings"]), 1)
            self.assertEqual("deliberation-plane", promotion_artifact["board_state_source"])
            self.assertEqual("analysis-plane", promotion_artifact["coverage_source"])
            self.assertEqual(
                "deliberation-plane-readiness",
                promotion_artifact["readiness_source"],
            )
            self.assertEqual(
                "missing-board-brief",
                promotion_artifact["board_brief_source"],
            )
            self.assertFalse(
                promotion_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertFalse(
                promotion_artifact["observed_inputs"]["board_brief_present"]
            )
            self.assertTrue(
                promotion_artifact["observed_inputs"]["readiness_artifact_present"]
            )
            self.assertTrue(promotion_artifact["observed_inputs"]["readiness_present"])
            self.assertTrue(
                promotion_artifact["observed_inputs"]["next_actions_artifact_present"]
            )
            self.assertTrue(
                promotion_artifact["observed_inputs"]["next_actions_present"]
            )
            self.assertEqual("deliberation-plane", handoff_artifact["board_state_source"])
            self.assertEqual("analysis-plane", handoff_artifact["coverage_source"])
            self.assertEqual(
                "deliberation-plane-promotion-basis",
                handoff_artifact["promotion_source"],
            )
            self.assertEqual(
                "deliberation-plane-readiness",
                handoff_artifact["readiness_source"],
            )
            self.assertEqual(
                "deliberation-plane-supervisor",
                handoff_artifact["supervisor_state_source"],
            )
            self.assertEqual("missing-board-brief", handoff_artifact["board_brief_source"])
            self.assertFalse(
                handoff_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertEqual("completed", handoff_payload["deliberation_sync"]["status"])
            self.assertIn(
                handoff_payload["analysis_sync"]["status"],
                {"completed", "existing-result-set"},
            )
            self.assertTrue(
                handoff_artifact["observed_inputs"]["promotion_artifact_present"]
            )
            self.assertTrue(handoff_artifact["observed_inputs"]["promotion_present"])
            self.assertTrue(
                handoff_artifact["observed_inputs"][
                    "supervisor_state_artifact_present"
                ]
            )
            self.assertTrue(
                handoff_artifact["observed_inputs"]["supervisor_state_present"]
            )
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual("ready", decision_artifact["publication_readiness"])
            self.assertFalse(decision_artifact["next_round_required"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                decision_artifact["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-promotion-basis",
                decision_artifact["promotion_source"],
            )
            self.assertEqual("deliberation-plane", decision_artifact["board_state_source"])
            self.assertEqual("analysis-plane", decision_artifact["coverage_source"])
            self.assertTrue(
                decision_artifact["observed_inputs"][
                    "reporting_handoff_artifact_present"
                ]
            )
            self.assertTrue(
                decision_artifact["observed_inputs"]["reporting_handoff_present"]
            )
            self.assertEqual(promotion_artifact["basis_id"], handoff_artifact["promoted_basis_id"])

    def test_reporting_handoff_and_decision_recover_from_db_when_promotion_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-derive-observation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
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
                "Round is ready to move into reporting even if promotion export is removed.",
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
                "0.93",
            )

            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                basis_count = connection.execute(
                    "SELECT COUNT(*) FROM promotion_basis_records WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
                item_count = connection.execute(
                    "SELECT COUNT(*) FROM promotion_basis_items WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            handoff_payload = run_script(
                script_path("eco-materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("eco-draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(
                reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")
            )
            decision_artifact = load_json(
                reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
            )

            self.assertGreater(basis_count, 0)
            self.assertGreater(item_count, 0)
            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual(
                "deliberation-plane-promotion-basis",
                handoff_artifact["promotion_source"],
            )
            self.assertFalse(
                handoff_artifact["observed_inputs"]["promotion_artifact_present"]
            )
            self.assertTrue(handoff_artifact["observed_inputs"]["promotion_present"])
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual(
                "deliberation-plane-promotion-basis",
                decision_artifact["promotion_source"],
            )

    def test_reporting_handoff_recovers_supervisor_state_from_db_when_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-derive-observation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
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
                "Reporting handoff should recover supervisor state directly from the deliberation DB.",
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
                "0.93",
            )

            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            supervisor_path = (run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json")
            supervisor_path.unlink()

            handoff_payload = run_script(
                script_path("eco-materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("eco-draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(
                reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")
            )
            decision_artifact = load_json(
                reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
            )

            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual(
                "deliberation-plane-supervisor",
                handoff_artifact["supervisor_state_source"],
            )
            self.assertFalse(
                handoff_artifact["observed_inputs"][
                    "supervisor_state_artifact_present"
                ]
            )
            self.assertTrue(
                handoff_artifact["observed_inputs"]["supervisor_state_present"]
            )
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertTrue(decision_artifact["reporting_ready"])

    def test_decision_draft_recovers_from_db_when_handoff_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-derive-observation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
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
                "Decision drafting should recover from the deliberation-plane handoff record.",
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
                "0.93",
            )

            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                handoff_count = connection.execute(
                    "SELECT COUNT(*) FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            decision_payload = run_script(
                script_path("eco-draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_artifact = load_json(
                reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
            )

            self.assertGreater(handoff_count, 0)
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                decision_artifact["reporting_handoff_source"],
            )
            self.assertFalse(
                decision_artifact["observed_inputs"][
                    "reporting_handoff_artifact_present"
                ]
            )
            self.assertTrue(
                decision_artifact["observed_inputs"]["reporting_handoff_present"]
            )

    def test_reporting_handoff_and_decision_hold_withheld_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-derive-observation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
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
                "Public reports may overstate severity relative to observed PM2.5 coverage.",
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
                "Re-test whether the strongest narrative exceeds evidence coverage.",
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

            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_payload = run_script(
                script_path("eco-materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("eco-draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"))
            decision_artifact = load_json(reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json"))

            self.assertEqual("investigation-open", handoff_payload["summary"]["handoff_status"])
            self.assertFalse(handoff_artifact["reporting_ready"])
            self.assertIn("promotion-withheld", handoff_artifact["reporting_blockers"])
            self.assertEqual("withheld", handoff_artifact["promotion_status"])
            self.assertGreaterEqual(len(handoff_artifact["open_risks"]), 1)
            self.assertGreaterEqual(len(handoff_artifact["recommended_next_actions"]), 1)
            self.assertEqual("missing-board-brief", handoff_artifact["board_brief_source"])
            self.assertFalse(
                handoff_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertFalse(handoff_artifact["observed_inputs"]["board_brief_present"])
            self.assertIn(
                "eco-submit-council-proposal",
                handoff_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "eco-submit-readiness-opinion",
                handoff_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "eco-post-board-note",
                handoff_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertEqual("continue", decision_payload["summary"]["moderator_status"])
            self.assertEqual("hold", decision_artifact["publication_readiness"])
            self.assertTrue(decision_artifact["next_round_required"])
            self.assertEqual("missing-board-brief", decision_artifact["board_brief_source"])
            self.assertFalse(
                decision_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertIn("promotion-withheld", decision_artifact["decision_gating"]["reason_codes"])
            self.assertIn(
                "eco-submit-council-proposal",
                decision_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "eco-submit-readiness-opinion",
                decision_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "eco-post-board-note",
                decision_payload["board_handoff"]["suggested_next_skills"],
            )


if __name__ == "__main__":
    unittest.main()

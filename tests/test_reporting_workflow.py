from __future__ import annotations

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

            self.assertEqual("ready-for-reporting", handoff_payload["summary"]["handoff_status"])
            self.assertEqual("ready-for-reporting", handoff_artifact["handoff_status"])
            self.assertEqual("promoted", handoff_artifact["promotion_status"])
            self.assertGreaterEqual(len(handoff_artifact["key_findings"]), 1)
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual("ready", decision_artifact["publication_readiness"])
            self.assertFalse(decision_artifact["next_round_required"])
            self.assertEqual(promotion_artifact["basis_id"], handoff_artifact["promoted_basis_id"])

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

            self.assertEqual("pending-more-investigation", handoff_payload["summary"]["handoff_status"])
            self.assertEqual("withheld", handoff_artifact["promotion_status"])
            self.assertGreaterEqual(len(handoff_artifact["open_risks"]), 1)
            self.assertGreaterEqual(len(handoff_artifact["recommended_next_actions"]), 1)
            self.assertEqual("continue", decision_payload["summary"]["moderator_status"])
            self.assertEqual("hold", decision_artifact["publication_readiness"])
            self.assertTrue(decision_artifact["next_round_required"])
            self.assertIn("promotion-withheld", decision_artifact["decision_gating"]["reason_codes"])


if __name__ == "__main__":
    unittest.main()
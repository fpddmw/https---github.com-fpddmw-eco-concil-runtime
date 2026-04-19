from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    investigation_path,
    load_json,
    promotion_path,
    reporting_path,
    run_script,
    script_path,
    seed_analysis_chain,
)

RUN_ID = "run-investigation-001"
ROUND_ID = "round-investigation-001"


class InvestigationWorkflowTests(unittest.TestCase):
    def test_d1_builds_actions_and_probes_from_open_challenge(self) -> None:
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

            actions_payload = run_script(
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            probes_payload = run_script(
                script_path("eco-open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            actions_artifact = load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"))
            probes_artifact = load_json(investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json"))

            self.assertGreaterEqual(actions_payload["summary"]["action_count"], 1)
            self.assertEqual("deliberation-plane", actions_payload["summary"]["board_state_source"])
            self.assertEqual("completed", actions_payload["deliberation_sync"]["status"])
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)
            self.assertEqual("deliberation-plane-actions", probes_payload["summary"]["action_source"])
            actions = actions_artifact["ranked_actions"]
            self.assertEqual("deliberation-plane", actions_artifact["board_state_source"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_summary_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_brief_present"])
            self.assertTrue(any(action["action_kind"] == "resolve-challenge" for action in actions))
            self.assertTrue(any(bool(action["probe_candidate"]) for action in actions))
            self.assertTrue(any("controversy_gap" in action for action in actions))
            self.assertTrue(
                any(action.get("policy_source") == "runtime-fallback-policy" for action in actions)
            )
            self.assertEqual(
                len(actions),
                sum(actions_artifact["policy_source_counts"].values()),
            )
            probes = probes_artifact["probes"]
            self.assertEqual("deliberation-plane-actions", probes_artifact["action_source"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertTrue(any(probe["target_hypothesis_id"] == hypothesis_payload["canonical_ids"][0] for probe in probes))
            self.assertTrue(any("eco-close-challenge-ticket" in probe["requested_skills"] for probe in probes))
            self.assertTrue(any("probe_type" in probe for probe in probes))

    def test_d1_next_actions_and_probes_work_without_board_summary_or_brief(self) -> None:
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
                "Smoke over NYC may still be overstated",
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
                "Challenge the strongest smoke narrative",
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

            actions_payload = run_script(
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            probes_payload = run_script(
                script_path("eco-open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            actions_artifact = load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"))
            probes_artifact = load_json(investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json"))

            self.assertFalse((run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").exists())
            self.assertFalse((run_dir / "board" / f"board_brief_{ROUND_ID}.md").exists())
            self.assertEqual("deliberation-plane", actions_payload["summary"]["board_state_source"])
            self.assertEqual("completed", actions_payload["deliberation_sync"]["status"])
            self.assertEqual("deliberation-plane", actions_artifact["board_state_source"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_summary_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_brief_present"])
            self.assertTrue(any(action["action_kind"] == "resolve-challenge" for action in actions_artifact["ranked_actions"]))
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertGreaterEqual(len(probes_artifact["probes"]), 1)

    def test_d1_probe_skill_can_rebuild_candidates_without_next_actions_artifact(self) -> None:
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
                "Direct probe fallback hypothesis",
                "--statement",
                "Probe generation should rebuild candidates from shared deliberation state.",
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
                "Rebuild probe candidates without next_actions artifact",
                "--challenge-statement",
                "The probe skill should recover when the explicit next-actions artifact is absent.",
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
            analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").unlink()

            probes_payload = run_script(
                script_path("eco-open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            probes_artifact = load_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
            )

            self.assertFalse((run_dir / "investigation" / f"next_actions_{ROUND_ID}.json").exists())
            self.assertFalse(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").exists())
            self.assertEqual("derived-from-deliberation", probes_payload["summary"]["action_source"])
            self.assertEqual("deliberation-plane", probes_payload["summary"]["board_state_source"])
            self.assertEqual("analysis-plane", probes_payload["summary"]["coverage_source"])
            self.assertEqual("completed", probes_payload["deliberation_sync"]["status"])
            self.assertEqual("derived-from-deliberation", probes_artifact["action_source"])
            self.assertEqual("analysis-plane", probes_artifact["coverage_source"])
            self.assertFalse(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["coverage_present"])
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)
            self.assertGreaterEqual(len(probes_artifact["probes"]), 1)

    def test_d1_probe_skill_reads_db_backed_actions_when_next_actions_artifact_is_missing(self) -> None:
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
                "DB-backed next actions survive artifact deletion",
                "--statement",
                "Probe generation should recover the ranked next-action queue from the deliberation DB snapshot.",
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
                "Delete next actions after generation",
                "--challenge-statement",
                "The probe skill should load persisted moderator actions instead of rebuilding from scratch.",
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
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            investigation_path(run_dir, f"next_actions_{ROUND_ID}.json").unlink()

            probes_payload = run_script(
                script_path("eco-open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            probes_artifact = load_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
            )

            self.assertFalse((run_dir / "investigation" / f"next_actions_{ROUND_ID}.json").exists())
            self.assertEqual("deliberation-plane-actions", probes_payload["summary"]["action_source"])
            self.assertEqual("deliberation-plane-actions", probes_artifact["action_source"])
            self.assertFalse(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)

    def test_d2_reads_db_backed_actions_and_probes_when_artifacts_are_missing(self) -> None:
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
                "Readiness should honor DB-backed action and probe state",
                "--statement",
                "Deleting JSON exports should not erase moderator work that already exists in the deliberation DB.",
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
                "Keep contradiction work open",
                "--challenge-statement",
                "The readiness skill should still see unresolved moderator work after artifact deletion.",
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
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            investigation_path(run_dir, f"next_actions_{ROUND_ID}.json").unlink()
            investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                action_count = connection.execute(
                    "SELECT COUNT(*) FROM moderator_actions WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
                probe_count = connection.execute(
                    "SELECT COUNT(*) FROM falsification_probes WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            readiness_payload = run_script(
                script_path("eco-summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_payload = run_script(
                script_path("eco-promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertEqual("needs-more-data", readiness_payload["summary"]["readiness_status"])
            self.assertFalse(readiness_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(readiness_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["probes_artifact_present"])
            self.assertTrue(readiness_artifact["observed_inputs"]["probes_present"])
            self.assertGreater(action_count, 0)
            self.assertGreater(probe_count, 0)
            self.assertGreater(int(readiness_artifact["counts"]["open_probes"]), 0)
            self.assertIn("controversy_gap_counts", readiness_artifact)
            self.assertIn("probe_type_counts", readiness_artifact)
            self.assertEqual("withheld", promotion_payload["summary"]["promotion_status"])
            self.assertFalse(promotion_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(promotion_artifact["observed_inputs"]["next_actions_present"])
            self.assertEqual("deliberation-plane-actions", promotion_artifact["next_actions_source"])

    def test_d2_promotion_reads_db_backed_readiness_when_artifact_is_missing(self) -> None:
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
            run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
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
                "0.91",
            )

            readiness_payload = run_script(
                script_path("eco-summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                readiness_count = connection.execute(
                    "SELECT COUNT(*) FROM round_readiness_assessments WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            promotion_payload = run_script(
                script_path("eco-promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_artifact = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )

            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertGreater(readiness_count, 0)
            self.assertEqual("promoted", promotion_payload["summary"]["promotion_status"])
            self.assertEqual(
                "deliberation-plane-readiness",
                promotion_artifact["readiness_source"],
            )
            self.assertFalse(
                promotion_artifact["observed_inputs"]["readiness_artifact_present"]
            )
            self.assertTrue(promotion_artifact["observed_inputs"]["readiness_present"])

    def test_d2_marks_ready_and_promotes_basis_when_board_is_clean(self) -> None:
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
                "Board is organized and strong evidence is available for promotion review.",
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
                "0.91",
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
            actions_payload = run_script(
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            readiness_payload = run_script(
                script_path("eco-summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_payload = run_script(
                script_path("eco-promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))
            coverage_artifact = load_json(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json"))

            self.assertGreaterEqual(actions_payload["summary"]["action_count"], 1)
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertTrue(readiness_artifact["sufficient_for_promotion"])
            self.assertEqual("promoted", promotion_payload["summary"]["promotion_status"])
            self.assertEqual("promoted", promotion_artifact["promotion_status"])
            self.assertEqual(
                "freeze-controversy-basis-v1",
                promotion_artifact["basis_selection_mode"],
            )
            self.assertGreaterEqual(
                promotion_artifact["basis_counts"]["coverage_count"],
                1,
            )
            self.assertGreaterEqual(len(promotion_artifact["selected_coverages"]), 1)
            available_coverage_ids = {coverage["coverage_id"] for coverage in coverage_artifact["coverages"]}
            self.assertIn(promotion_artifact["selected_coverages"][0]["coverage_id"], available_coverage_ids)

    def test_d2_reads_deliberation_plane_without_board_summary_or_brief(self) -> None:
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
            run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
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
                "0.91",
            )

            readiness_payload = run_script(
                script_path("eco-summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))

            self.assertFalse((run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").exists())
            self.assertFalse((run_dir / "board" / f"board_brief_{ROUND_ID}.md").exists())
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertEqual("deliberation-plane", readiness_payload["summary"]["board_state_source"])
            self.assertEqual("completed", readiness_payload["deliberation_sync"]["status"])
            self.assertTrue(readiness_artifact["sufficient_for_promotion"])
            self.assertEqual("deliberation-plane", readiness_artifact["board_state_source"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_summary_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_brief_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["probes_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["probes_present"])

    def test_d1_and_d2_continue_from_analysis_plane_when_coverage_artifact_is_missing(self) -> None:
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
            run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
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
                "0.91",
            )
            analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").unlink()

            actions_payload = run_script(
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            readiness_payload = run_script(
                script_path("eco-summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_payload = run_script(
                script_path("eco-promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            actions_artifact = load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"))
            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertFalse(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").exists())
            self.assertEqual("analysis-plane", actions_payload["summary"]["coverage_source"])
            self.assertEqual("analysis-plane", actions_artifact["coverage_source"])
            self.assertFalse(actions_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["coverage_present"])
            self.assertGreaterEqual(actions_payload["summary"]["action_count"], 1)
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertEqual("analysis-plane", readiness_payload["summary"]["coverage_source"])
            self.assertFalse(readiness_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertTrue(readiness_artifact["observed_inputs"]["coverage_present"])
            self.assertTrue(readiness_artifact["sufficient_for_promotion"])
            self.assertEqual("promoted", promotion_payload["summary"]["promotion_status"])
            self.assertEqual("analysis-plane", promotion_payload["summary"]["coverage_source"])
            self.assertFalse(promotion_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertTrue(promotion_artifact["observed_inputs"]["coverage_present"])
            self.assertEqual(
                "freeze-controversy-basis-v1",
                promotion_artifact["basis_selection_mode"],
            )
            self.assertGreaterEqual(
                promotion_artifact["basis_counts"]["coverage_count"],
                1,
            )
            self.assertGreaterEqual(len(promotion_artifact["selected_coverages"]), 1)


if __name__ == "__main__":
    unittest.main()

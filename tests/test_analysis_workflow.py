from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import analytics_path, load_json, run_script, script_path, seed_analysis_chain

RUN_ID = "run-analysis-001"
ROUND_ID = "round-analysis-001"


class AnalysisWorkflowTests(unittest.TestCase):
    def test_analysis_chain_runs_through_b2_1(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            self.assertGreaterEqual(len(outputs["extract_claims"]["canonical_ids"]), 1)
            self.assertGreaterEqual(len(outputs["extract_observations"]["canonical_ids"]), 2)
            self.assertGreaterEqual(len(outputs["cluster_claims"]["canonical_ids"]), 1)
            self.assertEqual(1, len(outputs["merge_observations"]["canonical_ids"]))
            self.assertGreaterEqual(len(outputs["link_evidence"]["canonical_ids"]), 1)

            claim_scope_payload = run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            observation_scope_payload = run_script(
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
            audit_payload = run_script(
                script_path("eco-build-normalization-audit"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertGreaterEqual(len(claim_scope_payload["canonical_ids"]), 1)
            self.assertEqual(1, len(observation_scope_payload["canonical_ids"]))
            self.assertEqual("completed", claim_scope_payload["analysis_sync"]["status"])
            self.assertEqual("completed", observation_scope_payload["analysis_sync"]["status"])
            self.assertEqual("completed", outputs["link_evidence"]["analysis_sync"]["status"])
            self.assertGreaterEqual(len(coverage_payload["canonical_ids"]), 1)
            self.assertEqual("completed", coverage_payload["analysis_sync"]["status"])
            self.assertEqual("completed", audit_payload["status"])

            claim_scope_artifact = load_json(analytics_path(run_dir, f"claim_scope_proposals_{ROUND_ID}.json"))
            observation_scope_artifact = load_json(analytics_path(run_dir, f"observation_scope_proposals_{ROUND_ID}.json"))
            coverage_artifact = load_json(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json"))
            db_file = analytics_path(run_dir, "signal_plane.sqlite")

            self.assertGreaterEqual(claim_scope_artifact["scope_count"], 1)
            self.assertEqual(1, observation_scope_artifact["scope_count"])
            self.assertGreaterEqual(coverage_artifact["coverage_count"], 1)
            coverages = coverage_artifact.get("coverages", [])
            assert isinstance(coverages, list)
            self.assertEqual("strong", coverages[0]["readiness"])
            with sqlite3.connect(db_file) as connection:
                result_sets = connection.execute(
                    """
                    SELECT analysis_kind, item_count, artifact_path
                    FROM analysis_result_sets
                    WHERE run_id = ? AND round_id = ?
                    ORDER BY analysis_kind
                    """,
                    (RUN_ID, ROUND_ID),
                ).fetchall()
                result_set_map = {row[0]: row for row in result_sets}
                self.assertIn("claim-observation-link", result_set_map)
                self.assertIn("claim-scope", result_set_map)
                self.assertIn("observation-scope", result_set_map)
                self.assertIn("evidence-coverage", result_set_map)
                self.assertGreaterEqual(int(result_set_map["claim-observation-link"][1]), 1)
                self.assertGreaterEqual(int(result_set_map["claim-scope"][1]), 1)
                self.assertGreaterEqual(int(result_set_map["observation-scope"][1]), 1)
                self.assertGreaterEqual(int(result_set_map["evidence-coverage"][1]), 1)
                self.assertEqual(
                    str(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").resolve()),
                    result_set_map["evidence-coverage"][2],
                )
                item_count = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM analysis_result_items
                    WHERE run_id = ? AND round_id = ? AND analysis_kind = ?
                    """,
                    (RUN_ID, ROUND_ID, "evidence-coverage"),
                ).fetchone()
                self.assertIsNotNone(item_count)
                assert item_count is not None
                self.assertGreaterEqual(int(item_count[0]), 1)

    def test_analysis_outputs_support_custom_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=False)

            claim_scope_path = root / "custom_claim_scope.json"
            observation_scope_path = root / "custom_observation_scope.json"
            coverage_path = root / "custom_evidence_coverage.json"

            claim_scope_payload = run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(claim_scope_path),
            )
            observation_scope_payload = run_script(
                script_path("eco-derive-observation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(observation_scope_path),
            )
            coverage_payload = run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(coverage_path),
            )

            self.assertEqual(str(claim_scope_path), claim_scope_payload["summary"]["output_path"])
            self.assertEqual(str(observation_scope_path), observation_scope_payload["summary"]["output_path"])
            self.assertEqual(str(coverage_path), coverage_payload["summary"]["output_path"])
            self.assertTrue(claim_scope_path.exists())
            self.assertTrue(observation_scope_path.exists())
            self.assertTrue(coverage_path.exists())
            with sqlite3.connect(analytics_path(run_dir, "signal_plane.sqlite")) as connection:
                result_sets = connection.execute(
                    """
                    SELECT analysis_kind, artifact_path
                    FROM analysis_result_sets
                    WHERE run_id = ? AND round_id IN (?, ?)
                    """,
                    (RUN_ID, ROUND_ID, ROUND_ID),
                ).fetchall()
                result_set_map = {row[0]: row[1] for row in result_sets}
                self.assertEqual(str(claim_scope_path.resolve()), result_set_map["claim-scope"])
                self.assertEqual(
                    str(observation_scope_path.resolve()),
                    result_set_map["observation-scope"],
                )
                self.assertEqual(str(coverage_path.resolve()), result_set_map["evidence-coverage"])

    def test_coverage_can_read_links_and_scopes_from_analysis_plane_without_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

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

            analytics_path(run_dir, f"claim_observation_links_{ROUND_ID}.json").unlink()
            analytics_path(run_dir, f"claim_scope_proposals_{ROUND_ID}.json").unlink()
            analytics_path(run_dir, f"observation_scope_proposals_{ROUND_ID}.json").unlink()

            coverage_payload = run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            coverage_artifact = load_json(
                analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json")
            )

            self.assertEqual("analysis-plane", coverage_payload["summary"]["links_source"])
            self.assertEqual("analysis-plane", coverage_payload["summary"]["claim_scope_source"])
            self.assertEqual(
                "analysis-plane",
                coverage_payload["summary"]["observation_scope_source"],
            )
            self.assertEqual("analysis-plane", coverage_artifact["links_source"])
            self.assertEqual("analysis-plane", coverage_artifact["claim_scope_source"])
            self.assertEqual(
                "analysis-plane",
                coverage_artifact["observation_scope_source"],
            )
            self.assertFalse(coverage_artifact["observed_inputs"]["links_artifact_present"])
            self.assertFalse(
                coverage_artifact["observed_inputs"]["claim_scope_artifact_present"]
            )
            self.assertFalse(
                coverage_artifact["observed_inputs"][
                    "observation_scope_artifact_present"
                ]
            )
            self.assertTrue(coverage_artifact["observed_inputs"]["links_present"])
            self.assertTrue(coverage_artifact["observed_inputs"]["claim_scope_present"])
            self.assertTrue(
                coverage_artifact["observed_inputs"]["observation_scope_present"]
            )
            self.assertGreaterEqual(coverage_payload["summary"]["coverage_count"], 1)


if __name__ == "__main__":
    unittest.main()

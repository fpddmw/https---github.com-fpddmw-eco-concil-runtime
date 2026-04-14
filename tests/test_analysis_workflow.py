from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import analytics_path, load_json, run_script, script_path, seed_analysis_chain

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import load_evidence_coverage_context

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
            self.assertEqual("completed", outputs["extract_claims"]["analysis_sync"]["status"])
            self.assertEqual(
                "completed", outputs["extract_observations"]["analysis_sync"]["status"]
            )
            self.assertEqual(
                "completed", outputs["cluster_claims"]["analysis_sync"]["status"]
            )
            self.assertEqual(
                "completed", outputs["merge_observations"]["analysis_sync"]["status"]
            )

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

            claim_candidates_artifact = load_json(
                analytics_path(run_dir, f"claim_candidates_{ROUND_ID}.json")
            )
            cluster_artifact = load_json(
                analytics_path(run_dir, f"claim_candidate_clusters_{ROUND_ID}.json")
            )
            claim_scope_artifact = load_json(analytics_path(run_dir, f"claim_scope_proposals_{ROUND_ID}.json"))
            observation_scope_artifact = load_json(analytics_path(run_dir, f"observation_scope_proposals_{ROUND_ID}.json"))
            coverage_artifact = load_json(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json"))
            db_file = analytics_path(run_dir, "signal_plane.sqlite")

            first_candidate = claim_candidates_artifact["candidates"][0]
            first_cluster = cluster_artifact["clusters"][0]
            first_scope = claim_scope_artifact["scopes"][0]
            self.assertIn("issue_hint", first_candidate)
            self.assertIn("stance_hint", first_candidate)
            self.assertIn("concern_facets", first_candidate)
            self.assertIn("verifiability_hint", first_candidate)
            self.assertIn("issue_label", first_cluster)
            self.assertIn("dominant_stance", first_cluster)
            self.assertIn("concern_facets", first_cluster)
            self.assertIn("verifiability_posture", first_cluster)
            self.assertIn("verifiability_kind", first_scope)
            self.assertIn("required_evidence_lane", first_scope)
            self.assertIn("verification_route_recommended", first_scope)
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
                self.assertIn("claim-candidate", result_set_map)
                self.assertIn("claim-cluster", result_set_map)
                self.assertIn("claim-observation-link", result_set_map)
                self.assertIn("merged-observation", result_set_map)
                self.assertIn("observation-candidate", result_set_map)
                self.assertIn("claim-scope", result_set_map)
                self.assertIn("observation-scope", result_set_map)
                self.assertIn("evidence-coverage", result_set_map)
                self.assertGreaterEqual(int(result_set_map["claim-candidate"][1]), 1)
                self.assertGreaterEqual(int(result_set_map["claim-cluster"][1]), 1)
                self.assertGreaterEqual(int(result_set_map["claim-observation-link"][1]), 1)
                self.assertGreaterEqual(int(result_set_map["merged-observation"][1]), 1)
                self.assertGreaterEqual(
                    int(result_set_map["observation-candidate"][1]), 1
                )
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

    def test_scope_and_link_can_read_cluster_and_merge_from_analysis_plane_without_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            self.assertEqual(
                "completed", outputs["cluster_claims"]["analysis_sync"]["status"]
            )
            self.assertEqual(
                "completed", outputs["merge_observations"]["analysis_sync"]["status"]
            )

            analytics_path(run_dir, f"claim_candidate_clusters_{ROUND_ID}.json").unlink()
            analytics_path(
                run_dir, f"merged_observation_candidates_{ROUND_ID}.json"
            ).unlink()

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
            link_payload = run_script(
                script_path("eco-link-claims-to-observations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            claim_scope_artifact = load_json(
                analytics_path(run_dir, f"claim_scope_proposals_{ROUND_ID}.json")
            )
            observation_scope_artifact = load_json(
                analytics_path(run_dir, f"observation_scope_proposals_{ROUND_ID}.json")
            )
            link_artifact = load_json(
                analytics_path(run_dir, f"claim_observation_links_{ROUND_ID}.json")
            )

            self.assertEqual(
                "analysis-plane", claim_scope_payload["summary"]["claim_input_source"]
            )
            self.assertEqual(
                "analysis-plane",
                observation_scope_payload["summary"]["observation_input_source"],
            )
            self.assertEqual(
                "analysis-plane", link_payload["summary"]["claim_input_source"]
            )
            self.assertEqual(
                "analysis-plane", link_payload["summary"]["observation_input_source"]
            )
            self.assertEqual("analysis-plane", claim_scope_artifact["claim_input_source"])
            self.assertEqual(
                "analysis-plane",
                observation_scope_artifact["observation_input_source"],
            )
            self.assertEqual("analysis-plane", link_artifact["claim_input_source"])
            self.assertEqual(
                "analysis-plane", link_artifact["observation_input_source"]
            )
            self.assertFalse(
                claim_scope_artifact["observed_inputs"][
                    "claim_clusters_artifact_present"
                ]
            )
            self.assertFalse(
                observation_scope_artifact["observed_inputs"][
                    "merged_observations_artifact_present"
                ]
            )
            self.assertFalse(
                link_artifact["observed_inputs"]["claim_clusters_artifact_present"]
            )
            self.assertFalse(
                link_artifact["observed_inputs"][
                    "merged_observations_artifact_present"
                ]
            )
            self.assertTrue(
                claim_scope_artifact["observed_inputs"]["claim_clusters_present"]
            )
            self.assertTrue(
                observation_scope_artifact["observed_inputs"][
                    "merged_observations_present"
                ]
            )
            self.assertTrue(
                link_artifact["observed_inputs"]["claim_clusters_present"]
            )
            self.assertTrue(
                link_artifact["observed_inputs"]["merged_observations_present"]
            )
            self.assertGreaterEqual(claim_scope_artifact["scope_count"], 1)
            self.assertEqual(1, observation_scope_artifact["scope_count"])
            self.assertGreaterEqual(link_artifact["link_count"], 1)

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

    def test_analysis_result_sets_persist_lineage_contracts(self) -> None:
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

            self.assertEqual(
                "heuristic-coverage-v1",
                coverage_payload["analysis_sync"]["query_basis"]["method"],
            )
            self.assertGreaterEqual(
                coverage_payload["analysis_sync"]["lineage_counts"][
                    "parent_result_set_count"
                ],
                3,
            )
            self.assertGreaterEqual(
                outputs["extract_claims"]["analysis_sync"]["lineage_counts"][
                    "parent_id_count"
                ],
                1,
            )
            self.assertGreaterEqual(
                outputs["cluster_claims"]["analysis_sync"]["lineage_counts"][
                    "parent_result_set_count"
                ],
                1,
            )
            self.assertGreaterEqual(
                outputs["merge_observations"]["analysis_sync"]["lineage_counts"][
                    "parent_result_set_count"
                ],
                1,
            )

            db_file = analytics_path(run_dir, "signal_plane.sqlite")
            with sqlite3.connect(db_file) as connection:
                connection.row_factory = sqlite3.Row
                result_rows = connection.execute(
                    """
                    SELECT analysis_kind, result_set_id
                    FROM analysis_result_sets
                    WHERE run_id = ? AND round_id = ?
                    """,
                    (RUN_ID, ROUND_ID),
                ).fetchall()
                result_set_ids = {
                    maybe_row["analysis_kind"]: maybe_row["result_set_id"]
                    for maybe_row in result_rows
                }
                coverage_lineage = connection.execute(
                    """
                    SELECT lineage_scope, lineage_type, relation, value_text,
                           artifact_path, source_analysis_kind
                    FROM analysis_result_lineage
                    WHERE result_set_id = ?
                    ORDER BY lineage_scope, lineage_type, relation, value_text
                    """,
                    (result_set_ids["evidence-coverage"],),
                ).fetchall()
                query_basis_relations = {
                    row["relation"]
                    for row in coverage_lineage
                    if row["lineage_scope"] == "result-set"
                    and row["lineage_type"] == "query-basis"
                }
                self.assertTrue(
                    {
                        "links_path",
                        "claim_scope_path",
                        "observation_scope_path",
                        "method",
                    }.issubset(query_basis_relations)
                )
                parent_result_kinds = {
                    row["source_analysis_kind"]
                    for row in coverage_lineage
                    if row["lineage_scope"] == "result-set"
                    and row["lineage_type"] == "parent-result-set"
                }
                self.assertSetEqual(
                    {
                        "claim-observation-link",
                        "claim-scope",
                        "observation-scope",
                    },
                    parent_result_kinds,
                )
                parent_artifact_relations = {
                    row["relation"]
                    for row in coverage_lineage
                    if row["lineage_scope"] == "result-set"
                    and row["lineage_type"] == "artifact-ref"
                }
                self.assertSetEqual(
                    {
                        "links_path",
                        "claim_scope_path",
                        "observation_scope_path",
                    },
                    parent_artifact_relations,
                )
                claim_candidate_parent_id_count = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM analysis_result_lineage
                    WHERE result_set_id = ?
                      AND lineage_scope = 'item'
                      AND lineage_type = 'parent-id'
                      AND relation = 'source_signal_ids'
                    """,
                    (result_set_ids["claim-candidate"],),
                ).fetchone()
                self.assertIsNotNone(claim_candidate_parent_id_count)
                assert claim_candidate_parent_id_count is not None
                self.assertGreaterEqual(int(claim_candidate_parent_id_count[0]), 1)
                claim_candidate_artifact_ref_count = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM analysis_result_lineage
                    WHERE result_set_id = ?
                      AND lineage_scope = 'item'
                      AND lineage_type = 'artifact-ref'
                      AND relation = 'public_refs'
                    """,
                    (result_set_ids["claim-candidate"],),
                ).fetchone()
                self.assertIsNotNone(claim_candidate_artifact_ref_count)
                assert claim_candidate_artifact_ref_count is not None
                self.assertGreaterEqual(int(claim_candidate_artifact_ref_count[0]), 1)
                claim_cluster_parent_kinds = {
                    row["source_analysis_kind"]
                    for row in connection.execute(
                        """
                        SELECT source_analysis_kind
                        FROM analysis_result_lineage
                        WHERE result_set_id = ?
                          AND lineage_scope = 'result-set'
                          AND lineage_type = 'parent-result-set'
                        """,
                        (result_set_ids["claim-cluster"],),
                    ).fetchall()
                }
                self.assertSetEqual({"claim-candidate"}, claim_cluster_parent_kinds)
                merged_observation_parent_kinds = {
                    row["source_analysis_kind"]
                    for row in connection.execute(
                        """
                        SELECT source_analysis_kind
                        FROM analysis_result_lineage
                        WHERE result_set_id = ?
                          AND lineage_scope = 'result-set'
                          AND lineage_type = 'parent-result-set'
                        """,
                        (result_set_ids["merged-observation"],),
                    ).fetchall()
                }
                self.assertSetEqual(
                    {"observation-candidate"}, merged_observation_parent_kinds
                )

    def test_analysis_context_returns_lineage_contract_from_db(self) -> None:
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
            run_script(
                script_path("eco-score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").unlink()

            coverage_context = load_evidence_coverage_context(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )

            self.assertEqual("analysis-plane", coverage_context["coverage_source"])
            self.assertFalse(coverage_context["coverage_artifact_present"])
            self.assertEqual(
                "heuristic-coverage-v1",
                coverage_context["result_contract"]["query_basis"]["method"],
            )
            self.assertEqual(
                coverage_context["result_contract"]["query_basis"],
                coverage_context["analysis_sync"]["query_basis"],
            )
            parent_result_kinds = {
                parent["analysis_kind"]
                for parent in coverage_context["result_contract"]["parent_result_sets"]
            }
            self.assertSetEqual(
                {
                    "claim-observation-link",
                    "claim-scope",
                    "observation-scope",
                },
                parent_result_kinds,
            )
            self.assertGreaterEqual(
                coverage_context["analysis_sync"]["lineage_counts"][
                    "parent_artifact_ref_count"
                ],
                3,
            )

    def test_normalization_audit_can_read_candidates_from_analysis_plane_without_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            self.assertEqual("completed", outputs["extract_claims"]["analysis_sync"]["status"])
            self.assertEqual(
                "completed", outputs["extract_observations"]["analysis_sync"]["status"]
            )

            analytics_path(run_dir, f"claim_candidates_{ROUND_ID}.json").unlink()
            analytics_path(run_dir, f"observation_candidates_{ROUND_ID}.json").unlink()

            audit_payload = run_script(
                script_path("eco-build-normalization-audit"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            audit_artifact = load_json(
                analytics_path(run_dir, f"normalization_audit_{ROUND_ID}.json")
            )

            self.assertEqual(
                "analysis-plane", audit_payload["summary"]["claim_candidate_source"]
            )
            self.assertEqual(
                "analysis-plane",
                audit_payload["summary"]["observation_candidate_source"],
            )
            self.assertEqual("analysis-plane", audit_artifact["claim_candidate_source"])
            self.assertEqual(
                "analysis-plane",
                audit_artifact["observation_candidate_source"],
            )
            self.assertFalse(
                audit_artifact["observed_inputs"]["claim_candidates_artifact_present"]
            )
            self.assertFalse(
                audit_artifact["observed_inputs"][
                    "observation_candidates_artifact_present"
                ]
            )
            self.assertTrue(audit_artifact["observed_inputs"]["claim_candidates_present"])
            self.assertTrue(
                audit_artifact["observed_inputs"]["observation_candidates_present"]
            )
            self.assertGreaterEqual(
                audit_artifact["report"]["claim_candidate_count"], 1
            )
            self.assertGreaterEqual(
                audit_artifact["report"]["observation_candidate_count"], 1
            )


if __name__ == "__main__":
    unittest.main()

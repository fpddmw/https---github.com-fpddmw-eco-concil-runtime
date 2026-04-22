from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
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

from eco_council_runtime.kernel.analysis_plane import query_analysis_result_items
from eco_council_runtime.phase2_fallback_context import load_d1_shared_context

RUN_ID = "run-controversy-001"
ROUND_ID = "round-controversy-001"


class ControversyWorkflowTests(unittest.TestCase):
    def test_controversy_chain_materializes_empirical_issue_map(self) -> None:
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
            verifiability_payload = run_script(
                script_path("eco-classify-claim-verifiability"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            route_payload = run_script(
                script_path("eco-route-verification-lane"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            map_payload = run_script(
                script_path("eco-materialize-controversy-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            verifiability_artifact = load_json(
                analytics_path(run_dir, f"claim_verifiability_assessments_{ROUND_ID}.json")
            )
            route_artifact = load_json(
                investigation_path(run_dir, f"verification_routes_{ROUND_ID}.json")
            )
            map_artifact = load_json(
                analytics_path(run_dir, f"controversy_map_{ROUND_ID}.json")
            )

            self.assertEqual("completed", verifiability_payload["status"])
            self.assertEqual("completed", route_payload["status"])
            self.assertEqual("completed", map_payload["status"])
            self.assertEqual("completed", verifiability_payload["analysis_sync"]["status"])
            self.assertEqual("completed", route_payload["analysis_sync"]["status"])
            self.assertEqual("completed", map_payload["analysis_sync"]["status"])
            self.assertEqual("heuristic-fallback", map_payload["decision_source"])
            self.assertGreaterEqual(verifiability_artifact["assessment_count"], 1)
            self.assertGreaterEqual(route_artifact["route_count"], 1)
            self.assertGreaterEqual(map_artifact["issue_cluster_count"], 1)
            self.assertEqual("heuristic-fallback", map_artifact["decision_source"])
            self.assertIn("source_skill", map_artifact["provenance"])
            self.assertTrue(
                any(
                    assessment["verifiability_kind"] == "empirical-observable"
                    for assessment in verifiability_artifact["assessments"]
                )
            )
            self.assertTrue(
                any(
                    route["route_status"] == "route-to-verification-lane"
                    and bool(route["should_query_environment"])
                    for route in route_artifact["routes"]
                )
            )
            self.assertTrue(
                any(
                    issue["issue_label"] == "air-quality-smoke"
                    and issue["recommended_lane"] == "environmental-observation"
                    for issue in map_artifact["issue_clusters"]
                )
            )
            smoke_issue = next(
                issue
                for issue in map_artifact["issue_clusters"]
                if issue["issue_label"] == "air-quality-smoke"
            )
            self.assertEqual("heuristic-fallback", smoke_issue["decision_source"])
            self.assertTrue(smoke_issue["rationale"])
            self.assertTrue(smoke_issue["claim_scope_id"])
            self.assertTrue(smoke_issue["assessment_id"])
            self.assertTrue(smoke_issue["route_id"])
            self.assertGreaterEqual(len(smoke_issue["source_signal_ids"]), 1)
            self.assertGreaterEqual(len(smoke_issue["lineage"]), 1)
            self.assertIn("source_skill", smoke_issue["provenance"])

            analytics_path(run_dir, f"controversy_map_{ROUND_ID}.json").unlink()
            query_payload = query_analysis_result_items(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                analysis_kind="controversy-map",
                subject_id="air-quality-smoke",
                latest_only=True,
                include_result_sets=True,
                include_contract=True,
            )
            self.assertGreaterEqual(query_payload["summary"]["returned_item_count"], 1)
            self.assertTrue(
                all(not item["artifact_present"] for item in query_payload["items"])
            )
            self.assertTrue(
                any(
                    item["decision_source"] == "heuristic-fallback"
                    and bool(item["item"]["route_id"])
                    for item in query_payload["items"]
                )
            )
            parent_kinds = {
                parent["analysis_kind"]
                for parent in query_payload["result_sets"][0]["result_contract"][
                    "parent_result_sets"
                ]
            }
            self.assertSetEqual(
                {
                    "issue-cluster",
                    "stance-group",
                    "concern-facet",
                    "actor-profile",
                    "evidence-citation-type",
                },
                parent_kinds,
            )

    def test_typed_issue_surfaces_sync_to_db_and_recover_without_artifacts(self) -> None:
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
                script_path("eco-classify-claim-verifiability"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-route-verification-lane"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            issue_candidate_payload = run_script(
                script_path("eco-extract-issue-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            issue_cluster_payload = run_script(
                script_path("eco-cluster-issue-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-extract-stance-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-extract-concern-facets"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-extract-actor-profiles"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-extract-evidence-citation-types"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            payload = run_script(
                script_path("eco-materialize-controversy-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            issue_cluster_artifact = load_json(
                analytics_path(run_dir, f"issue_clusters_{ROUND_ID}.json")
            )
            issue_candidate_artifact = load_json(
                analytics_path(run_dir, f"issue_candidates_{ROUND_ID}.json")
            )
            stance_group_artifact = load_json(
                analytics_path(run_dir, f"stance_groups_{ROUND_ID}.json")
            )
            concern_facet_artifact = load_json(
                analytics_path(run_dir, f"concern_facets_{ROUND_ID}.json")
            )
            actor_profile_artifact = load_json(
                analytics_path(run_dir, f"actor_profiles_{ROUND_ID}.json")
            )
            citation_type_artifact = load_json(
                analytics_path(run_dir, f"evidence_citation_types_{ROUND_ID}.json")
            )

            self.assertEqual(
                "completed",
                issue_candidate_payload["analysis_sync"]["status"],
            )
            self.assertEqual("completed", issue_cluster_payload["analysis_sync"]["status"])
            self.assertEqual(
                "heuristic-fallback",
                issue_candidate_payload["decision_source"],
            )
            self.assertEqual(
                "heuristic-fallback",
                issue_cluster_payload["decision_source"],
            )
            self.assertIn(
                payload["typed_analysis_sync"]["issue-cluster"]["status"],
                {"completed", "existing-result-set"},
            )
            self.assertGreaterEqual(issue_candidate_artifact["issue_cluster_count"], 1)
            self.assertGreaterEqual(issue_cluster_artifact["issue_cluster_count"], 1)
            self.assertEqual(
                "heuristic-fallback",
                issue_candidate_artifact["decision_source"],
            )
            self.assertEqual(
                "heuristic-fallback",
                issue_cluster_artifact["decision_source"],
            )
            self.assertGreaterEqual(
                issue_candidate_artifact["issue_cluster_count"],
                issue_cluster_artifact["issue_cluster_count"],
            )
            self.assertEqual(
                "claim-scope-derived-candidates",
                issue_candidate_payload["summary"]["issue_derivation_mode"],
            )
            self.assertEqual(
                "extract-issue-candidates-from-claim-scopes",
                issue_candidate_artifact["query_basis"]["selection_mode"],
            )
            self.assertGreaterEqual(stance_group_artifact["stance_group_count"], 1)
            self.assertGreaterEqual(concern_facet_artifact["concern_facet_count"], 1)
            self.assertGreaterEqual(actor_profile_artifact["actor_profile_count"], 1)
            self.assertGreaterEqual(citation_type_artifact["citation_type_count"], 1)
            self.assertEqual("heuristic-fallback", stance_group_artifact["decision_source"])
            self.assertEqual("heuristic-fallback", concern_facet_artifact["decision_source"])
            self.assertEqual("heuristic-fallback", actor_profile_artifact["decision_source"])
            self.assertEqual("heuristic-fallback", citation_type_artifact["decision_source"])
            self.assertIn("source_skill", stance_group_artifact["provenance"])
            self.assertIn("source_skill", issue_candidate_artifact["provenance"])
            first_issue_cluster = issue_cluster_artifact["issue_clusters"][0]
            self.assertEqual("issue-cluster-v1", first_issue_cluster["schema_version"])
            self.assertEqual(
                first_issue_cluster["cluster_id"],
                first_issue_cluster["map_issue_id"],
            )
            self.assertTrue(first_issue_cluster["claim_cluster_id"])
            self.assertGreaterEqual(len(first_issue_cluster["stance_group_ids"]), 1)
            self.assertGreaterEqual(len(first_issue_cluster["concern_ids"]), 1)
            self.assertGreaterEqual(len(first_issue_cluster["actor_ids"]), 1)
            self.assertGreaterEqual(len(first_issue_cluster["citation_type_ids"]), 1)

            for artifact_name in (
                f"issue_clusters_{ROUND_ID}.json",
                f"stance_groups_{ROUND_ID}.json",
                f"concern_facets_{ROUND_ID}.json",
                f"actor_profiles_{ROUND_ID}.json",
                f"evidence_citation_types_{ROUND_ID}.json",
            ):
                analytics_path(run_dir, artifact_name).unlink()

            query_expectations = {
                "issue-cluster": {
                    "schema": "issue-cluster-v1",
                    "parent_kinds": {
                        "claim-cluster",
                        "claim-scope",
                        "claim-verifiability",
                        "verification-route",
                    },
                },
                "stance-group": {
                    "schema": "stance-group-v1",
                    "parent_kinds": {"issue-cluster"},
                },
                "concern-facet": {
                    "schema": "concern-facet-v1",
                    "parent_kinds": {"issue-cluster"},
                },
                "actor-profile": {
                    "schema": "actor-profile-v1",
                    "parent_kinds": {"issue-cluster"},
                },
                "evidence-citation-type": {
                    "schema": "evidence-citation-type-v1",
                    "parent_kinds": {"issue-cluster"},
                },
            }
            for analysis_kind, expectation in query_expectations.items():
                query_payload = query_analysis_result_items(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    analysis_kind=analysis_kind,
                    subject_id="air-quality-smoke",
                    latest_only=True,
                    include_result_sets=True,
                    include_contract=True,
                )
                self.assertGreaterEqual(
                    query_payload["summary"]["returned_item_count"],
                    1,
                    analysis_kind,
                )
                self.assertTrue(
                    all(not item["artifact_present"] for item in query_payload["items"]),
                    analysis_kind,
                )
                self.assertTrue(
                    all(
                        item["item"]["schema_version"] == expectation["schema"]
                        for item in query_payload["items"]
                    ),
                    analysis_kind,
                )
                parent_kinds = {
                    parent["analysis_kind"]
                    for parent in query_payload["result_sets"][0]["result_contract"][
                        "parent_result_sets"
                    ]
                }
                self.assertSetEqual(expectation["parent_kinds"], parent_kinds)

    def test_shared_context_prefers_issue_cluster_db_surface_when_artifacts_are_missing(
        self,
    ) -> None:
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
                script_path("eco-classify-claim-verifiability"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-route-verification-lane"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-materialize-controversy-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            analytics_path(run_dir, f"issue_clusters_{ROUND_ID}.json").unlink()
            analytics_path(run_dir, f"controversy_map_{ROUND_ID}.json").unlink()

            shared_context = load_d1_shared_context(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )

            self.assertEqual(
                "issue-cluster",
                shared_context["analysis_sync"]["selected_analysis_kind"],
            )
            self.assertGreaterEqual(
                shared_context["agenda_counts"]["issue_cluster_count"],
                1,
            )
            self.assertTrue(
                any(
                    item["schema_version"] == "issue-cluster-v1"
                    for item in shared_context["issue_clusters"]
                )
            )
            self.assertFalse(
                shared_context["observed_inputs"]["issue_clusters_artifact_present"]
            )
            self.assertFalse(
                shared_context["observed_inputs"]["controversy_map_artifact_present"]
            )

    def test_procedural_scope_routes_away_from_environment_and_still_materializes_map(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            scope_path = analytics_path(run_dir, f"claim_scope_proposals_{ROUND_ID}.json")
            write_json(
                scope_path,
                {
                    "schema_version": "n3.0",
                    "skill": "fixture",
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "input_path": str(scope_path),
                    "claim_input_source": "test-fixture",
                    "claim_input_kind": "claim-candidate",
                    "scope_count": 1,
                    "scopes": [
                        {
                            "claim_scope_id": "claimscope-proc-001",
                            "run_id": RUN_ID,
                            "round_id": ROUND_ID,
                            "claim_id": "claim-proc-001",
                            "claim_object_id": "claim-proc-001",
                            "claim_input_kind": "claim-candidate",
                            "claim_type": "procedure-legitimacy",
                            "issue_hint": "permit-process",
                            "scope_label": "Public evidence footprint",
                            "scope_kind": "unknown",
                            "matching_tags": ["procedure-legitimacy"],
                            "issue_terms": ["permit", "process"],
                            "concern_facets": ["procedure-governance"],
                            "actor_hints": ["agency", "resident"],
                            "evidence_citation_types": ["official-document"],
                            "verifiability_kind": "procedural-record",
                            "dispute_type": "governance-procedure",
                            "required_evidence_lane": "formal-comment-and-policy-record",
                            "verification_route_recommended": False,
                            "matching_eligibility_reason": "Route through formal records before any observation matching.",
                            "method": "test-fixture",
                            "claim_scope": {
                                "label": "Public evidence footprint",
                                "geometry": {},
                                "usable_for_matching": False,
                            },
                            "place_scope": {
                                "label": "Public evidence footprint",
                                "geometry": {},
                            },
                            "confidence": 0.74,
                            "decision_source": "heuristic-fallback",
                            "evidence_refs": [],
                            "lineage": ["claim-proc-001"],
                            "rationale": "Fixture procedural claim scope for formal-record routing.",
                            "provenance": {"source": "unit-test"},
                        }
                    ],
                },
            )

            run_script(
                script_path("eco-classify-claim-verifiability"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--claim-scope-path",
                str(scope_path),
            )
            route_payload = run_script(
                script_path("eco-route-verification-lane"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--claim-scope-path",
                str(scope_path),
            )
            map_payload = run_script(
                script_path("eco-materialize-controversy-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--claim-scope-path",
                str(scope_path),
            )

            route_artifact = load_json(
                investigation_path(run_dir, f"verification_routes_{ROUND_ID}.json")
            )
            map_artifact = load_json(
                analytics_path(run_dir, f"controversy_map_{ROUND_ID}.json")
            )

            self.assertEqual("completed", route_payload["analysis_sync"]["status"])
            self.assertEqual("completed", map_payload["analysis_sync"]["status"])
            self.assertEqual(1, route_artifact["route_count"])
            self.assertFalse(route_artifact["routes"][0]["should_query_environment"])
            self.assertEqual(
                "route-to-formal-record-review",
                route_artifact["routes"][0]["route_status"],
            )
            self.assertEqual(1, map_artifact["issue_cluster_count"])
            self.assertEqual(
                "non-empirical-issue",
                map_artifact["issue_clusters"][0]["controversy_posture"],
            )
            self.assertTrue(map_artifact["issue_clusters"][0]["claim_scope_id"])
            self.assertTrue(map_artifact["issue_clusters"][0]["assessment_id"])
            self.assertTrue(map_artifact["issue_clusters"][0]["route_id"])


if __name__ == "__main__":
    unittest.main()

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
            self.assertGreaterEqual(verifiability_artifact["assessment_count"], 1)
            self.assertGreaterEqual(route_artifact["route_count"], 1)
            self.assertGreaterEqual(map_artifact["issue_cluster_count"], 1)
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
                {"claim-cluster", "claim-scope", "claim-verifiability", "verification-route"},
                parent_kinds,
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

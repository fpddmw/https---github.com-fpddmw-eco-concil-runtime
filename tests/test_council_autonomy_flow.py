from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    investigation_path,
    load_json,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    report_basis_path,
    reporting_path,
    request_and_approve_transition,
    run_kernel,
    run_script,
    runtime_src_path,
    seed_analysis_chain,
    script_path,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import (  # noqa: E402
    store_council_proposal_records,
    store_readiness_opinion_records,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (  # noqa: E402
    store_moderator_action_records,
)

RUN_ID = "run-council-autonomy-001"
ROUND_ID = "round-council-autonomy-001"


class CouncilAutonomyFlowTests(unittest.TestCase):
    def write_scope_mission(
        self,
        run_dir: Path,
        *,
        required_lanes: list[str | dict],
        required_sources: list[str] | None = None,
    ) -> None:
        lane_entries = [
            lane if isinstance(lane, dict) else {"lane_id": lane, "priority": "high"}
            for lane in required_lanes
        ]
        write_json(
            run_dir / "mission.json",
            {
                "schema_version": "1.0.0",
                "run_id": RUN_ID,
                "topic": "June 2023 New York City smoke episode",
                "objective": (
                    "Investigate candidate source regions, transport pathway, "
                    "public impacts, and handling recommendations."
                ),
                "window": {
                    "start_utc": "2023-06-07T00:00:00Z",
                    "end_utc": "2023-06-10T00:00:00Z",
                },
                "region": {
                    "label": "New York City, NY, United States",
                    "geometry": {
                        "type": "Point",
                        "latitude": 40.7128,
                        "longitude": -74.006,
                    },
                },
                "verification_scope": {
                    "required_evidence_lanes": lane_entries,
                    "required_source_skills": required_sources or [],
                    "candidate_source_region_policy": (
                        "mission-derived-candidate-source-review"
                    ),
                    "transport_verification_policy": (
                        "mission-derived-relation-review"
                    ),
                },
            },
        )

    def test_agent_proposal_queue_takes_priority_over_heuristic_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "clarify-verification-route",
                            "action_kind": "clarify-verification-route",
                            "agent_role": "moderator",
                            "assigned_role": "moderator",
                            "objective": "Freeze a single investigation lane for issue-001.",
                            "rationale": "Council wants routing resolved before any more downstream verification work.",
                            "target_kind": "issue-cluster",
                            "target_id": "issue-001",
                            "recommended_lane": "mixed-review",
                            "controversy_gap": "verification-routing-gap",
                            "decision_source": "agent-council",
                            "confidence": 0.88,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")
            )
            first_action = artifact["ranked_actions"][0]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("agent-proposal-execution", artifact["agenda_source"])
            self.assertEqual("agent-proposal", first_action["agenda_source"])
            self.assertEqual("agent-council", first_action["decision_source"])
            self.assertEqual("agent-council", first_action["policy_source"])
            self.assertEqual("agent-council-proposal-v1", first_action["policy_profile"])
            self.assertEqual("clarify-verification-route", first_action["action_kind"])
            self.assertEqual("issue-001", first_action["target"]["object_id"])
            self.assertIn(proposal_id, first_action["lineage"])

            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "next-action",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual(1, query_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "agent-council",
                query_payload["objects"][0]["decision_source"],
            )

    def test_actor_targeted_proposal_can_drive_action_queue_without_fallback_flags(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "review-actor-posture",
                            "action_kind": "review-actor-posture",
                            "agent_role": "moderator",
                            "assigned_role": "moderator",
                            "objective": "Review the regulator actor posture on issue-001.",
                            "rationale": "Council wants explicit follow-up on one actor posture even without a route or gap cue.",
                            "target_kind": "actor-profile",
                            "target_id": "actor-001",
                            "issue_label": "air-quality-smoke",
                            "decision_source": "agent-council",
                            "confidence": 0.71,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://actor-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")
            )
            first_action = artifact["ranked_actions"][0]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("review-actor-posture", first_action["action_kind"])
            self.assertEqual("actor-profile", first_action["target"]["object_kind"])
            self.assertEqual("actor-001", first_action["target"]["object_id"])
            self.assertEqual("actor-001", first_action["target_actor_id"])
            self.assertEqual(proposal_id, first_action["source_proposal_id"])
            self.assertIn(proposal_id, first_action["lineage"])

            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "next-action",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "actor-profile",
                "--target-id",
                "actor-001",
                "--actor-id",
                "actor-001",
                "--source-proposal-id",
                proposal_id,
            )
            self.assertEqual(1, query_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "actor-profile",
                query_payload["objects"][0]["target_object_kind"],
            )
            self.assertEqual("actor-001", query_payload["objects"][0]["target_actor_id"])

    def test_next_actions_default_to_proposal_authority_when_heuristic_queue_exists(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            coverage_ref = primary_successor_evidence_ref(outputs)
            issue_id = primary_research_issue_id(outputs)
            run_script(
                script_path("post-board-note"),
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
                "Heuristic next-action pressure is present, but proposal authority should still win.",
                "--linked-artifact-ref",
                coverage_ref,
            )
            run_script(
                script_path("update-hypothesis-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Smoke over NYC needs one more route review",
                "--statement",
                "The round still exposes routing pressure that would normally produce heuristic next actions.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--linked-claim-id",
                issue_id,
                "--linked-artifact-ref",
                coverage_ref,
                "--confidence",
                "0.58",
            )
            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "clarify-verification-route",
                            "action_kind": "clarify-verification-route",
                            "agent_role": "moderator",
                            "assigned_role": "moderator",
                            "objective": "Freeze a single investigation lane for issue-proposal.",
                            "rationale": "Council wants routing resolved from the proposal queue instead of recomputing fallback actions.",
                            "target_kind": "issue-cluster",
                            "target_id": "issue-proposal",
                            "recommended_lane": "mixed-review",
                            "controversy_gap": "verification-routing-gap",
                            "decision_source": "agent-council",
                            "confidence": 0.84,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://proposal-route"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")
            )
            first_action = artifact["ranked_actions"][0]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("agent-proposal-execution", artifact["agenda_source"])
            self.assertEqual(1, artifact["proposal_action_count"])
            self.assertEqual(0, artifact["heuristic_action_count"])
            self.assertGreaterEqual(artifact["observed_heuristic_action_count"], 1)
            self.assertGreaterEqual(artifact["suppressed_heuristic_action_count"], 1)
            self.assertEqual("issue-proposal", first_action["target"]["object_id"])
            self.assertIn(proposal_id, first_action["lineage"])

    def test_readiness_assessment_prefers_council_opinions_over_policy_formula(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The controversy map is coherent enough to move forward.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        },
                        {
                            "agent_role": "challenger",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "No remaining contradiction justifies another round.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        },
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", payload["summary"]["readiness_status"])
            self.assertEqual("ready", artifact["readiness_status"])
            self.assertEqual("agent-council", artifact["decision_source"])
            self.assertEqual(2, artifact["readiness_opinion_count"])
            self.assertEqual(
                2,
                artifact["readiness_opinion_status_counts"]["ready"],
            )
            self.assertIn(
                "Council submitted 2 readiness opinions",
                artifact["gate_reasons"][0],
            )

            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "readiness-assessment",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual(1, query_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "agent-council",
                query_payload["objects"][0]["decision_source"],
            )

    def test_verification_scope_required_sources_hold_ready_opinion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            self.write_scope_mission(
                run_dir,
                required_lanes=[
                    "receptor-air-quality",
                    "fire-origin",
                    "spatiotemporal-relation-review",
                ],
                required_sources=[
                    "fetch-open-meteo-air-quality",
                    "fetch-nasa-firms-fire",
                ],
            )
            write_json(
                run_dir / "mission.json",
                {
                    **load_json(run_dir / "mission.json"),
                    "source_selections": {
                        "environmental-investigator": {
                            "status": "complete",
                            "selected_sources": ["fetch-open-meteo-air-quality"],
                        }
                    },
                },
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The report can proceed with bounded caveats.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://bundle-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )
            gate = artifact["verification_scope_gate"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("needs-more-data", artifact["readiness_status"])
            self.assertFalse(artifact["sufficient_for_report_basis"])
            self.assertEqual("missing-required-source-imports", gate["status"])
            self.assertEqual(
                ["fetch-open-meteo-air-quality", "fetch-nasa-firms-fire"],
                gate["missing_required_source_skills"],
            )
            self.assertEqual(["fetch-nasa-firms-fire"], gate["missing_selected_source_skills"])
            self.assertIn("fetch-nasa-firms-fire", artifact["recommended_next_skills"])
            self.assertIn("normalize-fetch-execution", artifact["recommended_next_skills"])
            self.assertIn(
                "Explicit verification scope requires completed source imports",
                artifact["gate_reasons"][0],
            )

    def test_required_lane_evidence_review_records_transport_lane_without_relation_packet(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            self.write_scope_mission(
                run_dir,
                required_lanes=[
                    {
                        "lane_id": "spatiotemporal-relation-review",
                        "priority": "high",
                        "evidence_requirements": [
                            {
                                "evidence_object_kind": "spatiotemporal-relation-evidence-packet",
                                "minimum_relation_count": 1,
                                "available_support_skills": [
                                    "materialize-spatiotemporal-relation-evidence-packet"
                                ],
                            }
                        ],
                    }
                ],
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The council believes the report basis can freeze.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-transport"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://transport"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )
            review = artifact["required_lane_evidence_review"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", artifact["readiness_status"])
            self.assertTrue(artifact["sufficient_for_report_basis"])
            self.assertEqual("missing-lane-evidence", review["status"])
            self.assertEqual(
                "spatiotemporal-relation-review",
                review["missing_lanes"][0]["lane_id"],
            )
            self.assertIn(
                "materialize-spatiotemporal-relation-evidence-packet",
                review["missing_lanes"][0]["missing_requirements"][0]["available_support_skills"],
            )
            self.assertEqual(["freeze-report-basis"], artifact["recommended_next_skills"])

    def test_required_lane_evidence_review_does_not_infer_missing_evidence_without_requirement(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            self.write_scope_mission(
                run_dir,
                required_lanes=["spatiotemporal-relation-review"],
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The council has not supplied lane-specific evidence requirements.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-transport"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://transport"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )
            review = artifact["required_lane_evidence_review"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", artifact["readiness_status"])
            self.assertEqual("not-evaluated", review["status"])
            self.assertEqual([], review["missing_lanes"])
            self.assertEqual(
                "spatiotemporal-relation-review",
                review["not_evaluated_lanes"][0]["lane_id"],
            )

    def test_required_lane_evidence_review_records_explicit_transport_scope_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            self.write_scope_mission(
                run_dir,
                required_lanes=["spatiotemporal-relation-review"],
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "challenger",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": (
                                "The report may proceed only if transport attribution "
                                "is explicitly out of scope."
                            ),
                            "decision_source": "agent-council",
                            "basis_object_ids": [
                                "scope-out:spatiotemporal-relation-review"
                            ],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://bounded-report"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )
            review = artifact["required_lane_evidence_review"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", artifact["readiness_status"])
            self.assertEqual("satisfied", review["status"])
            self.assertEqual(
                ["spatiotemporal-relation-review"],
                review["scoped_out_lanes"],
            )
            self.assertEqual(["freeze-report-basis"], artifact["recommended_next_skills"])

    def test_required_lane_evidence_review_records_response_recommendations_without_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            self.write_scope_mission(
                run_dir,
                required_lanes=[
                    {
                        "lane_id": "response-recommendation-boundary",
                        "priority": "high",
                        "evidence_requirements": [
                            {
                                "evidence_object_kind": "report-section-draft",
                                "section_keys": ["recommendations"],
                                "requires_evidence_refs": True,
                                "available_support_skills": ["submit-report-section-draft"],
                            }
                        ],
                    }
                ],
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The council believes response text can be included.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-response"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://response"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )
            review = artifact["required_lane_evidence_review"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", artifact["readiness_status"])
            self.assertEqual("missing-lane-evidence", review["status"])
            self.assertEqual(
                "response-recommendation-boundary",
                review["missing_lanes"][0]["lane_id"],
            )
            self.assertIn(
                "submit-report-section-draft",
                review["missing_lanes"][0]["missing_requirements"][0]["available_support_skills"],
            )
            self.assertEqual(["freeze-report-basis"], artifact["recommended_next_skills"])

    def test_required_lane_evidence_review_accepts_evidence_bound_response_section(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            self.write_scope_mission(
                run_dir,
                required_lanes=[
                    {
                        "lane_id": "response-recommendation-boundary",
                        "priority": "high",
                        "evidence_requirements": [
                            {
                                "evidence_object_kind": "report-section-draft",
                                "section_keys": ["recommendations"],
                                "requires_evidence_refs": True,
                            }
                        ],
                    }
                ],
            )
            section_payload = run_kernel(
                "submit-report-section-draft",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "report-editor",
                "--agent-role",
                "report-editor",
                "--report-id",
                ROUND_ID,
                "--section-key",
                "recommendations",
                "--section-title",
                "Recommendations",
                "--section-text",
                "Only evidence-bounded response options are represented here.",
                "--basis-object-id",
                "evidence-bundle-response",
                "--evidence-ref",
                "evidence://response-section",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The response section has DB-backed evidence refs.",
                            "decision_source": "agent-council",
                            "basis_object_ids": section_payload["canonical_ids"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://response-section"],
                            "lineage": section_payload["canonical_ids"],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )
            review = artifact["required_lane_evidence_review"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("ready", artifact["readiness_status"])
            self.assertEqual("satisfied", review["status"])
            self.assertEqual(
                "response-recommendation-boundary",
                review["satisfied_lanes"][0]["lane_id"],
            )
            observed = review["satisfied_lanes"][0]["satisfied_requirements"][0]["observed"]
            self.assertEqual(section_payload["canonical_ids"], observed["section_ids"])
            self.assertEqual(["freeze-report-basis"], artifact["recommended_next_skills"])

    def test_readiness_with_council_opinions_stops_recommending_next_actions_recompute(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "blocked",
                            "sufficient_for_report_basis": False,
                            "rationale": "The current contradiction still needs a targeted challenge pass.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-002"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-002"],
                            "lineage": [],
                        },
                        {
                            "agent_role": "challenger",
                            "readiness_status": "needs-more-data",
                            "sufficient_for_report_basis": False,
                            "rationale": "The board should stay open, but the next step is challenge work rather than recomputing the fallback action agenda.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-002"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-002"],
                            "lineage": [],
                        },
                    ],
                },
            )

            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("agent-council", artifact["decision_source"])
            self.assertNotIn("propose-next-actions", artifact["recommended_next_skills"])
            self.assertIn("submit-council-proposal", artifact["recommended_next_skills"])
            self.assertIn("submit-readiness-opinion", artifact["recommended_next_skills"])

    def test_report_risk_review_comment_blocks_readiness_until_explicit_disposition(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The bounded report can proceed if source limitations are carried as caveats.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://bundle-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            review_payload = run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "challenger",
                "--author-role",
                "challenger",
                "--review-kind",
                "evidence-bundle-review",
                "--comment-text",
                "Carry source limitations before report use; this evidence cannot support source attribution.",
                "--target-kind",
                "evidence-bundle",
                "--target-id",
                "evidence-bundle-001",
                "--report-risk",
                "source-limitations",
                "--evidence-ref",
                "evidence://bundle-001",
            )
            comment_id = review_payload["canonical_ids"][0]

            blocked_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            blocked_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual("needs-more-data", blocked_payload["summary"]["readiness_status"])
            self.assertEqual(1, blocked_artifact["blocking_review_comment_count"])
            self.assertIn(comment_id, blocked_artifact["blocking_review_comment_ids"])
            self.assertIn("open-followup-from-review-comment", blocked_artifact["recommended_next_skills"])
            self.assertIn("open-challenge-ticket", blocked_artifact["recommended_next_skills"])
            self.assertIn("submit-readiness-opinion", blocked_artifact["recommended_next_skills"])

            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "challenger",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The comment is explicitly waived for a bounded report that states no source attribution.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [comment_id],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://bundle-001"],
                            "lineage": [comment_id],
                        }
                    ],
                },
            )
            still_blocked_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            still_blocked_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual(
                "needs-more-data",
                still_blocked_payload["summary"]["readiness_status"],
            )
            self.assertEqual(
                1,
                still_blocked_artifact["unresolved_challenger_constraint_count"],
            )
            self.assertEqual(1, still_blocked_artifact["blocking_review_comment_count"])
            self.assertIn(
                "open-followup-from-review-comment",
                still_blocked_artifact["recommended_next_skills"],
            )

            run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "challenger",
                "--author-role",
                "challenger",
                "--review-kind",
                "constraint-disposition",
                "--comment-text",
                "Accepted only as a source limitation for bounded report use.",
                "--target-kind",
                "evidence-bundle",
                "--target-id",
                "evidence-bundle-001",
                "--response-to-id",
                comment_id,
                "--constraint-disposition",
                "accepted_as_limitation",
            )

            disposition_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            disposition_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual(
                "ready",
                disposition_payload["summary"]["readiness_status"],
            )
            self.assertEqual(0, disposition_artifact["blocking_review_comment_count"])
            self.assertEqual(
                0,
                disposition_artifact["unresolved_challenger_constraint_count"],
            )
            self.assertEqual(2, disposition_artifact["open_review_comment_count"])
            self.assertIn("freeze-report-basis", disposition_artifact["recommended_next_skills"])
            self.assertEqual(1, len(disposition_artifact["basis_use_constraints"]))
            self.assertEqual(
                "accepted_as_limitation",
                disposition_artifact["basis_use_constraints"][0]["disposition"],
            )
            self.assertFalse(
                disposition_artifact["basis_use_constraints"][0][
                    "lead_basis_allowed"
                ]
            )

    def test_challenge_disposition_object_releases_report_risk_constraint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The bounded report can proceed if explicit challenge dispositions are honored.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://bundle-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            review_payload = run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "challenger",
                "--author-role",
                "challenger",
                "--review-kind",
                "evidence-bundle-review",
                "--comment-text",
                "Do not use this evidence for attribution unless the limitation is explicit.",
                "--target-kind",
                "evidence-bundle",
                "--target-id",
                "evidence-bundle-001",
                "--report-risk",
                "source-limitations",
                "--evidence-ref",
                "evidence://bundle-001",
            )
            comment_id = review_payload["canonical_ids"][0]

            blocked_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            blocked_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual(
                "needs-more-data",
                blocked_payload["summary"]["readiness_status"],
            )
            self.assertEqual(
                1,
                blocked_artifact["unresolved_challenger_constraint_count"],
            )

            disposition_payload = run_script(
                script_path("submit-challenge-disposition"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--target-kind",
                "review-comment",
                "--target-id",
                comment_id,
                "--response-to-id",
                comment_id,
                "--source-review-comment-id",
                comment_id,
                "--disposition-status",
                "accepted-as-limitation",
                "--decided-by-role",
                "moderator",
                "--rationale",
                "Accept only as a stated limitation for bounded report-basis use.",
                "--evidence-ref",
                "evidence://bundle-001",
            )

            released_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            released_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual("ready", released_payload["summary"]["readiness_status"])
            self.assertEqual(0, released_artifact["blocking_review_comment_count"])
            self.assertEqual(
                0,
                released_artifact["unresolved_challenger_constraint_count"],
            )
            self.assertEqual(1, len(released_artifact["basis_use_constraints"]))
            self.assertEqual(
                "accepted_as_limitation",
                released_artifact["basis_use_constraints"][0]["disposition"],
            )
            self.assertEqual(
                disposition_payload["canonical_ids"][0],
                released_artifact["challenger_constraints"][0][
                    "disposition_comment_id"
                ],
            )

    def test_open_followup_from_review_comment_creates_challenge_and_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            review_payload = run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "challenger",
                "--author-role",
                "challenger",
                "--review-kind",
                "evidence-bundle-review",
                "--comment-text",
                "The current bundle cannot support transport attribution without plume or trajectory evidence.",
                "--target-kind",
                "evidence-bundle",
                "--target-id",
                "evidence-bundle-transport-001",
                "--report-risk",
                "source-limitations",
                "--required-followup-evidence",
                "smoke plume or trajectory evidence",
                "--evidence-ref",
                "evidence://bundle-transport-001",
            )
            comment_id = review_payload["canonical_ids"][0]

            followup_payload = run_script(
                script_path("open-followup-from-review-comment"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--review-comment-id",
                comment_id,
            )
            challenge_id, task_id = followup_payload["canonical_ids"]
            challenge_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "challenge",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--status",
                "open",
            )
            task_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "board-task",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--status",
                "claimed",
            )

            self.assertEqual("completed", followup_payload["status"])
            self.assertEqual(comment_id, followup_payload["summary"]["review_comment_id"])
            self.assertEqual(2, len(followup_payload["canonical_ids"]))
            self.assertEqual(challenge_id, challenge_query["objects"][0]["ticket_id"])
            self.assertEqual(task_id, task_query["objects"][0]["task_id"])
            self.assertEqual(comment_id, challenge_query["objects"][0]["source_review_comment_id"])
            self.assertEqual(challenge_id, task_query["objects"][0]["source_ticket_id"])
            self.assertIn(comment_id, task_query["objects"][0]["lineage"])

    def test_challenge_disposition_targeting_followup_ticket_releases_source_review_constraint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The bounded report can proceed when the challenge ticket disposition is honored.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["evidence-bundle-transport-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://bundle-transport-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            review_payload = run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "challenger",
                "--author-role",
                "challenger",
                "--review-kind",
                "evidence-bundle-review",
                "--comment-text",
                "The current bundle cannot support transport attribution without plume or trajectory evidence.",
                "--target-kind",
                "evidence-bundle",
                "--target-id",
                "evidence-bundle-transport-001",
                "--report-risk",
                "source-limitations",
                "--required-followup-evidence",
                "smoke plume or trajectory evidence",
                "--evidence-ref",
                "evidence://bundle-transport-001",
            )
            comment_id = review_payload["canonical_ids"][0]
            followup_payload = run_script(
                script_path("open-followup-from-review-comment"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--review-comment-id",
                comment_id,
            )
            challenge_id = followup_payload["canonical_ids"][0]

            blocked_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            blocked_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual(
                "needs-more-data",
                blocked_payload["summary"]["readiness_status"],
            )
            self.assertEqual(
                1,
                blocked_artifact["unresolved_challenger_constraint_count"],
            )

            disposition_payload = run_script(
                script_path("submit-challenge-disposition"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--target-kind",
                "challenge",
                "--target-id",
                challenge_id,
                "--response-to-id",
                challenge_id,
                "--challenge-id",
                challenge_id,
                "--disposition-status",
                "accepted-as-limitation",
                "--decided-by-role",
                "moderator",
                "--rationale",
                "Accept the follow-up challenge as a stated limitation without using the bundle for attribution.",
                "--evidence-ref",
                "evidence://bundle-transport-001",
            )

            released_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            released_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual("ready", released_payload["summary"]["readiness_status"])
            self.assertEqual(
                0,
                released_artifact["unresolved_challenger_constraint_count"],
            )
            self.assertEqual(
                disposition_payload["canonical_ids"][0],
                released_artifact["challenger_constraints"][0][
                    "disposition_comment_id"
                ],
            )

    def test_lead_basis_conflicting_with_constraint_withholds_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            evidence_ref = "evidence://lead-basis-test"
            finding_payload = run_kernel(
                "submit-finding-record",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "environmental-investigator",
                "--agent-role",
                "environmental-investigator",
                "--finding-kind",
                "environmental-context",
                "--title",
                "Context finding",
                "--summary",
                "Context finding cites DB-backed evidence.",
                "--rationale",
                "The finding is only a context basis for structural gate testing.",
                "--confidence",
                "0.82",
                "--target-kind",
                "round",
                "--target-id",
                ROUND_ID,
                "--evidence-ref",
                evidence_ref,
            )
            finding_id = finding_payload["canonical_ids"][0]
            bundle_payload = run_kernel(
                "submit-evidence-bundle",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "environmental-investigator",
                "--agent-role",
                "environmental-investigator",
                "--bundle-kind",
                "context-bundle",
                "--title",
                "Context bundle",
                "--summary",
                "Bundle links the finding to the cited evidence ref.",
                "--rationale",
                "The bundle is a DB-backed reporting input.",
                "--confidence",
                "0.83",
                "--target-kind",
                "finding",
                "--target-id",
                finding_id,
                "--finding-id",
                finding_id,
                "--evidence-ref",
                evidence_ref,
            )
            bundle_id = bundle_payload["canonical_ids"][0]
            section_payload = run_kernel(
                "submit-report-section-draft",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "report-editor",
                "--agent-role",
                "report-editor",
                "--report-id",
                ROUND_ID,
                "--section-key",
                "key-findings",
                "--section-title",
                "Key Findings",
                "--section-text",
                "The section explicitly marks this context bundle as lead basis.",
                "--claim-id",
                "claim-lead-basis-001",
                "--claim-text",
                "The bounded report uses this context bundle as lead basis.",
                "--basis-use",
                "lead-basis",
                "--lead-basis",
                "--basis-object-id",
                bundle_id,
                "--bundle-id",
                bundle_id,
                "--finding-id",
                finding_id,
                "--evidence-ref",
                evidence_ref,
            )
            section_id = section_payload["canonical_ids"][0]
            review_payload = run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "challenger",
                "--author-role",
                "challenger",
                "--review-kind",
                "evidence-bundle-review",
                "--comment-text",
                "This bundle may be used only as a limitation/context caveat.",
                "--target-kind",
                "evidence-bundle",
                "--target-id",
                bundle_id,
                "--report-risk",
                "source-limitations",
                "--evidence-ref",
                evidence_ref,
            )
            comment_id = review_payload["canonical_ids"][0]
            run_script(
                script_path("submit-challenge-disposition"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--target-kind",
                "review-comment",
                "--target-id",
                comment_id,
                "--response-to-id",
                comment_id,
                "--source-review-comment-id",
                comment_id,
                "--disposition-status",
                "accepted-as-limitation",
                "--decided-by-role",
                "moderator",
                "--disposition-text",
                "Accepted as limitation, not as lead basis.",
                "--rationale",
                "Record limited report-basis use without resolving the evidence truth.",
                "--evidence-ref",
                evidence_ref,
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The explicit limitation is recorded.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [section_id, bundle_id],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [evidence_ref],
                            "lineage": [section_id, bundle_id],
                        }
                    ],
                },
            )
            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])

            transition_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                evidence_refs=[evidence_ref],
                basis_object_ids=[section_id, bundle_id],
            )
            freeze_payload = run_script(
                script_path("freeze-report-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                transition_request_id,
            )
            freeze_artifact = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("completed", freeze_payload["status"])
            self.assertEqual("withheld", freeze_artifact["report_basis_status"])
            self.assertEqual(1, freeze_artifact["explicit_lead_basis_count"])
            self.assertEqual(
                1,
                freeze_artifact["lead_basis_constraint_violation_count"],
            )
            self.assertEqual(
                "lead-basis-disallowed-by-constraint",
                freeze_artifact["lead_basis_constraint_violations"][0][
                    "violation_kind"
                ],
            )
            self.assertGreaterEqual(
                freeze_artifact["report_claim_structural_violation_count"],
                1,
            )

    def test_minimal_explicit_report_claim_freezes_without_template_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            evidence_ref = "evidence://report-claim-structure"
            section_payload = run_kernel(
                "submit-report-section-draft",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "report-editor",
                "--agent-role",
                "report-editor",
                "--report-id",
                ROUND_ID,
                "--section-key",
                "key-findings",
                "--section-title",
                "Key Findings",
                "--section-text",
                "This section declares a report claim with text and evidence.",
                "--claim-id",
                "claim-minimal-001",
                "--claim-text",
                "This is an explicit report claim.",
                "--evidence-ref",
                evidence_ref,
            )
            section_id = section_payload["canonical_ids"][0]
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "Readiness is ready and the explicit claim has the minimum governance fields.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [section_id],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [evidence_ref],
                            "lineage": [section_id],
                        }
                    ],
                },
            )
            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            transition_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                evidence_refs=[evidence_ref],
                basis_object_ids=[section_id],
            )
            freeze_payload = run_script(
                script_path("freeze-report-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                transition_request_id,
            )
            freeze_artifact = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("completed", freeze_payload["status"])
            self.assertEqual("frozen", freeze_artifact["report_basis_status"])
            self.assertEqual(1, freeze_artifact["explicit_report_claim_count"])
            self.assertEqual(
                0,
                freeze_artifact["report_claim_structural_violation_count"],
            )
            claim_object = freeze_artifact["explicit_report_claim_objects"][0]
            self.assertEqual(
                {
                    "claim_id",
                    "source_object_kind",
                    "source_object_id",
                    "section_id",
                    "section_key",
                    "claim_text",
                    "claim_constraint_ids",
                    "basis_use",
                    "basis_object_ids",
                    "bundle_ids",
                    "finding_ids",
                    "evidence_refs",
                    "reference_ids",
                    "is_lead_basis",
                    "structural_status",
                    "missing_structural_fields",
                },
                set(claim_object.keys()),
            )

    def test_explicit_report_claim_without_text_withholds_freeze(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            evidence_ref = "evidence://report-claim-without-text"
            section_payload = run_kernel(
                "submit-report-section-draft",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "report-editor",
                "--agent-role",
                "report-editor",
                "--report-id",
                ROUND_ID,
                "--section-key",
                "key-findings",
                "--section-title",
                "Key Findings",
                "--section-text",
                "This section declares a report claim id but omits the explicit claim text.",
                "--claim-id",
                "claim-without-text-001",
                "--evidence-ref",
                evidence_ref,
            )
            section_id = section_payload["canonical_ids"][0]
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "Readiness is ready, but explicit claim text is still required for traceability.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [section_id],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [evidence_ref],
                            "lineage": [section_id],
                        }
                    ],
                },
            )
            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            transition_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                evidence_refs=[evidence_ref],
                basis_object_ids=[section_id],
            )
            freeze_payload = run_script(
                script_path("freeze-report-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                transition_request_id,
            )
            freeze_artifact = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("completed", freeze_payload["status"])
            self.assertEqual("withheld", freeze_artifact["report_basis_status"])
            self.assertEqual(1, freeze_artifact["explicit_report_claim_count"])
            self.assertEqual(
                1,
                freeze_artifact["report_claim_structural_violation_count"],
            )
            missing_fields = set(
                freeze_artifact["report_claim_structural_violations"][0][
                    "missing_structural_fields"
                ]
            )
            self.assertEqual({"claim_text"}, missing_fields)

    def test_probe_opening_can_execute_directly_from_council_proposal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "open-probe",
                            "action_kind": "resolve-challenge",
                            "agent_role": "challenger",
                            "assigned_role": "challenger",
                            "objective": "Stress-test the open contradiction around ticket-001.",
                            "rationale": "Council wants an explicit contradiction review before the round is allowed to advance.",
                            "target_kind": "challenge-ticket",
                            "target_id": "ticket-001",
                            "target_hypothesis_id": "hypothesis-001",
                            "target_claim_id": "claim-001",
                            "probe_candidate": True,
                            "controversy_gap": "unresolved-contestation",
                            "recommended_lane": "mixed-review",
                            "decision_source": "agent-council",
                            "confidence": 0.9,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://ticket-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-probes",
                "1",
            )
            artifact = load_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
            )
            probe = artifact["probes"][0]

            self.assertEqual("completed", payload["status"])
            self.assertEqual(
                "agent-proposal-execution",
                payload["summary"]["action_source"],
            )
            self.assertFalse(
                any(item["code"] == "missing-next-actions" for item in payload["warnings"])
            )
            self.assertEqual(1, artifact["proposal_probe_candidate_count"])
            self.assertEqual(0, artifact["fallback_probe_candidate_count"])
            self.assertEqual("agent-council", probe["decision_source"])
            self.assertEqual("agent-council", probe["policy_source"])
            self.assertEqual("agent-council-proposal-v1", probe["policy_profile"])
            self.assertEqual("ticket-001", probe["target_ticket_id"])
            self.assertEqual("hypothesis-001", probe["target_hypothesis_id"])
            self.assertIn(proposal_id, probe["source_ids"])
            self.assertIn(proposal_id, probe["lineage"])

            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "probe",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual(1, query_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "agent-council",
                query_payload["objects"][0]["decision_source"],
            )

    def test_probe_opening_defaults_to_proposal_authority_over_db_backed_heuristic_action(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_moderator_action_records(
                run_dir,
                action_snapshot={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "ranked_actions": [
                        {
                            "action_kind": "clarify-verification-route",
                            "priority": "high",
                            "assigned_role": "moderator",
                            "objective": "Fallback route review for ticket-heuristic.",
                            "reason": "Heuristic fallback still sees unresolved routing ambiguity.",
                            "decision_source": "heuristic-fallback",
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://heuristic-route"],
                            "lineage": [],
                            "probe_candidate": True,
                            "controversy_gap": "verification-routing-gap",
                            "recommended_lane": "verification",
                            "target": {
                                "object_kind": "challenge-ticket",
                                "object_id": "ticket-heuristic",
                                "hypothesis_id": "hypothesis-heuristic",
                                "claim_id": "claim-heuristic",
                            },
                        }
                    ],
                },
            )
            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "open-probe",
                            "action_kind": "resolve-challenge",
                            "agent_role": "challenger",
                            "assigned_role": "challenger",
                            "objective": "Prioritize contradiction review for ticket-proposal.",
                            "rationale": "Council explicitly wants the proposal-backed contradiction investigated first.",
                            "target_kind": "challenge-ticket",
                            "target_id": "ticket-proposal",
                            "target_hypothesis_id": "hypothesis-proposal",
                            "target_claim_id": "claim-proposal",
                            "probe_candidate": True,
                            "controversy_gap": "unresolved-contestation",
                            "recommended_lane": "mixed-review",
                            "decision_source": "agent-council",
                            "confidence": 0.86,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://proposal-route"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-probes",
                "1",
            )
            artifact = load_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
            )
            probe = artifact["probes"][0]

            self.assertEqual("completed", payload["status"])
            self.assertEqual(
                "agent-proposal-execution",
                payload["summary"]["action_source"],
            )
            self.assertEqual(1, artifact["proposal_probe_candidate_count"])
            self.assertEqual(0, artifact["fallback_probe_candidate_count"])
            self.assertEqual(1, artifact["observed_fallback_probe_candidate_count"])
            self.assertEqual(1, artifact["suppressed_fallback_probe_candidate_count"])
            self.assertEqual("ticket-proposal", probe["target_ticket_id"])
            self.assertEqual("hypothesis-proposal", probe["target_hypothesis_id"])
            self.assertNotEqual("ticket-heuristic", probe["target_ticket_id"])
            self.assertEqual("agent-council", probe["decision_source"])
            self.assertEqual("agent-council", probe["policy_source"])
            self.assertEqual("agent-council-proposal-v1", probe["policy_profile"])
            self.assertIn(proposal_id, probe["source_ids"])
            self.assertIn(proposal_id, probe["lineage"])


if __name__ == "__main__":
    unittest.main()

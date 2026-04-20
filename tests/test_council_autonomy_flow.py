from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    investigation_path,
    load_json,
    reporting_path,
    run_kernel,
    run_script,
    runtime_src_path,
    seed_analysis_chain,
    script_path,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
    store_readiness_opinion_records,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_moderator_action_records,
)

RUN_ID = "run-council-autonomy-001"
ROUND_ID = "round-council-autonomy-001"


class CouncilAutonomyFlowTests(unittest.TestCase):
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
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-propose-next-actions"),
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

    def test_next_actions_default_to_proposal_authority_when_heuristic_queue_exists(
        self,
    ) -> None:
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
                "Heuristic next-action pressure is present, but proposal authority should still win.",
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
                "Smoke over NYC needs one more route review",
                "--statement",
                "The round still exposes routing pressure that would normally produce heuristic next actions.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--linked-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
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
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://proposal-route"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-propose-next-actions"),
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
                            "sufficient_for_promotion": True,
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
                            "sufficient_for_promotion": True,
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
                script_path("eco-summarize-round-readiness"),
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
                            "sufficient_for_promotion": False,
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
                            "sufficient_for_promotion": False,
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
                script_path("eco-summarize-round-readiness"),
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
            self.assertNotIn("eco-propose-next-actions", artifact["recommended_next_skills"])
            self.assertIn("eco-submit-council-proposal", artifact["recommended_next_skills"])
            self.assertIn("eco-submit-readiness-opinion", artifact["recommended_next_skills"])

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
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://ticket-001"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-open-falsification-probe"),
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
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://proposal-route"],
                            "lineage": [],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]

            payload = run_script(
                script_path("eco-open-falsification-probe"),
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

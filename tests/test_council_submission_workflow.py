from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_kernel, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-council-submission-001"
ROUND_ID = "round-council-submission-001"


class CouncilSubmissionWorkflowTests(unittest.TestCase):
    def test_submit_council_proposal_appends_structured_records_and_surfaces_fields(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            first_payload = run_script(
                script_path("submit-council-proposal"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--proposal-kind",
                "clarify-verification-route",
                "--agent-role",
                "moderator",
                "--rationale",
                "Freeze one verification lane directly from the council queue.",
                "--confidence",
                "0.81",
                "--action-kind",
                "clarify-verification-route",
                "--assigned-role",
                "moderator",
                "--objective",
                "Stabilize the route for issue-001.",
                "--target-kind",
                "issue-cluster",
                "--target-id",
                "issue-001",
                "--evidence-ref",
                "evidence://issue-001",
                "--response-to-id",
                "challenge-001",
                "--lineage-id",
                "seed-001",
                "--provenance-json",
                "{\"source\":\"unit-test\",\"author\":\"moderator\"}",
                "--publication-readiness",
                "ready",
                "--promote-allowed",
                "true",
            )
            second_payload = run_script(
                script_path("submit-council-proposal"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--proposal-kind",
                "withhold-publication",
                "--agent-role",
                "challenger",
                "--rationale",
                "Do not publish until the contradiction path is closed.",
                "--confidence",
                "0.93",
                "--target-kind",
                "round",
                "--target-id",
                ROUND_ID,
                "--promotion-disposition",
                "hold",
                "--promote-allowed",
                "false",
                "--evidence-ref",
                "evidence://contradiction-001",
                "--provenance-json",
                "{\"source\":\"unit-test\",\"author\":\"challenger\"}",
            )
            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "proposal",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
                "--limit",
                "10",
            )

            self.assertEqual("completed", first_payload["status"])
            self.assertEqual("completed", second_payload["status"])
            self.assertEqual(2, query_payload["summary"]["matching_object_count"])
            self.assertEqual("council-proposal-v1", query_payload["contract"]["schema_version"])

            proposals = {
                item["proposal_id"]: item
                for item in query_payload["objects"]
                if isinstance(item, dict)
            }
            first_id = first_payload["canonical_ids"][0]
            second_id = second_payload["canonical_ids"][0]
            first_proposal = proposals[first_id]
            second_proposal = proposals[second_id]

            self.assertEqual("moderator", first_proposal["agent_role"])
            self.assertEqual("clarify-verification-route", first_proposal["action_kind"])
            self.assertEqual("issue-cluster", first_proposal["target_kind"])
            self.assertEqual("issue-001", first_proposal["target_id"])
            self.assertEqual(0.81, first_proposal["confidence"])
            self.assertEqual(["challenge-001"], first_proposal["response_to_ids"])
            self.assertEqual(["seed-001"], first_proposal["lineage"])
            self.assertEqual(["evidence://issue-001"], first_proposal["evidence_refs"])
            self.assertEqual("issue-cluster", first_proposal["target"]["object_kind"])
            self.assertEqual("issue-001", first_proposal["target"]["object_id"])
            self.assertTrue(first_proposal["promote_allowed"])
            self.assertEqual("ready", first_proposal["publication_readiness"])

            self.assertEqual("challenger", second_proposal["agent_role"])
            self.assertEqual(0.93, second_proposal["confidence"])
            self.assertEqual("hold", second_proposal["promotion_disposition"])
            self.assertFalse(second_proposal["promote_allowed"])
            self.assertEqual(ROUND_ID, second_proposal["target_id"])
            self.assertEqual("round", second_proposal["target"]["object_kind"])

            artifact = load_json(Path(first_payload["summary"]["output_path"]))
            self.assertEqual(first_id, artifact["proposal"]["proposal_id"])
            self.assertEqual(
                "council-proposal-submission-v1",
                artifact["schema_version"],
            )

    def test_submit_readiness_opinion_appends_and_basis_ids_remain_queryable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            ready_payload = run_script(
                script_path("submit-readiness-opinion"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "moderator",
                "--readiness-status",
                "ready",
                "--rationale",
                "The current basis is coherent enough to move forward.",
                "--sufficient-for-promotion",
                "true",
                "--basis-object-id",
                "issue-001",
                "--evidence-ref",
                "evidence://issue-001",
                "--lineage-id",
                "proposal-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            blocked_payload = run_script(
                script_path("submit-readiness-opinion"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "challenger",
                "--readiness-status",
                "blocked",
                "--rationale",
                "Open contradiction work still blocks publication.",
                "--basis-object-id",
                "issue-001",
                "--basis-object-id",
                "probe-001",
                "--evidence-ref",
                "evidence://probe-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "readiness-opinion",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
                "--limit",
                "10",
            )

            self.assertEqual("completed", ready_payload["status"])
            self.assertEqual("completed", blocked_payload["status"])
            self.assertEqual(2, query_payload["summary"]["matching_object_count"])
            self.assertEqual("readiness-opinion-v1", query_payload["contract"]["schema_version"])

            opinions = {
                item["opinion_id"]: item
                for item in query_payload["objects"]
                if isinstance(item, dict)
            }
            ready_opinion = opinions[ready_payload["canonical_ids"][0]]
            blocked_opinion = opinions[blocked_payload["canonical_ids"][0]]

            self.assertEqual("moderator", ready_opinion["agent_role"])
            self.assertEqual("ready", ready_opinion["readiness_status"])
            self.assertTrue(ready_opinion["sufficient_for_promotion"])
            self.assertEqual(["issue-001"], ready_opinion["basis_object_ids"])

            self.assertEqual("challenger", blocked_opinion["agent_role"])
            self.assertEqual("blocked", blocked_opinion["readiness_status"])
            self.assertCountEqual(
                ["issue-001", "probe-001"],
                blocked_opinion["basis_object_ids"],
            )
            self.assertEqual(
                ["evidence://probe-001"],
                blocked_opinion["evidence_refs"],
            )

            artifact = load_json(Path(ready_payload["summary"]["output_path"]))
            self.assertEqual(
                ready_payload["canonical_ids"][0],
                artifact["opinion"]["opinion_id"],
            )
            self.assertEqual(
                "readiness-opinion-submission-v1",
                artifact["schema_version"],
            )

    def test_submit_finding_discussion_and_evidence_records_are_queryable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)

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
                "finding",
                "--title",
                "Smoke reached the downtown corridor",
                "--summary",
                "Observed smoke concentration in the corridor.",
                "--rationale",
                "The visible plume and supporting signal were aligned.",
                "--confidence",
                "0.87",
                "--target-kind",
                "round",
                "--target-id",
                ROUND_ID,
                "--basis-object-id",
                "basis-001",
                "--evidence-ref",
                "evidence://finding-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            finding_id = finding_payload["canonical_ids"][0]
            discussion_payload = run_kernel(
                "post-discussion-message",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "moderator",
                "--author-role",
                "moderator",
                "--message-text",
                "Please verify the evidence trail before publication.",
                "--target-kind",
                "round",
                "--target-id",
                ROUND_ID,
                "--response-to-id",
                finding_id,
                "--evidence-ref",
                "evidence://discussion-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            message_id = discussion_payload["canonical_ids"][0]
            evidence_payload = run_kernel(
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
                "evidence-bundle",
                "--title",
                "Evidence bundle for smoke plume",
                "--summary",
                "Collected the supporting artifact trail.",
                "--rationale",
                "Bundle ties the observation to the finding.",
                "--confidence",
                "0.91",
                "--target-kind",
                "round",
                "--target-id",
                ROUND_ID,
                "--basis-object-id",
                finding_id,
                "--finding-id",
                finding_id,
                "--evidence-ref",
                "evidence://bundle-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            bundle_id = evidence_payload["canonical_ids"][0]

            finding_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "finding",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
            )
            discussion_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "discussion-message",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
            )
            evidence_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "evidence-bundle",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
            )

            self.assertEqual("completed", finding_payload["status"])
            self.assertEqual("completed", discussion_payload["status"])
            self.assertEqual("completed", evidence_payload["status"])
            self.assertEqual(1, finding_query["summary"]["returned_object_count"])
            self.assertEqual(1, discussion_query["summary"]["returned_object_count"])
            self.assertEqual(1, evidence_query["summary"]["returned_object_count"])
            self.assertEqual("finding", finding_query["objects"][0]["finding_kind"])
            self.assertEqual("discussion", discussion_query["objects"][0]["message_kind"])
            self.assertEqual("evidence-bundle", evidence_query["objects"][0]["bundle_kind"])
            self.assertEqual(
                finding_id,
                finding_query["objects"][0]["finding_id"],
            )
            self.assertEqual(
                message_id,
                discussion_query["objects"][0]["message_id"],
            )
            self.assertEqual(
                bundle_id,
                evidence_query["objects"][0]["bundle_id"],
            )

    def test_kernel_can_execute_submission_skill_through_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            payload = run_kernel(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "submit-council-proposal",
                "--",
                "--proposal-kind",
                "route-follow-up",
                "--agent-role",
                "moderator",
                "--rationale",
                "Registry execution should be able to write one structured proposal.",
                "--confidence",
                "0.66",
                "--target-kind",
                "issue-cluster",
                "--target-id",
                "issue-kernel-001",
                "--evidence-ref",
                "evidence://issue-kernel-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "proposal",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--limit",
                "10",
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual(1, query_payload["summary"]["matching_object_count"])
            self.assertEqual(
                "route-follow-up",
                query_payload["objects"][0]["proposal_kind"],
            )
            self.assertEqual(0.66, query_payload["objects"][0]["confidence"])

    def test_submit_council_proposal_rejects_missing_confidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            with self.assertRaises(AssertionError):
                run_script(
                    script_path("submit-council-proposal"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--proposal-kind",
                    "route-follow-up",
                    "--agent-role",
                    "moderator",
                    "--rationale",
                    "Missing confidence should now fail the canonical proposal contract.",
                    "--target-kind",
                    "issue-cluster",
                    "--target-id",
                    "issue-missing-confidence-001",
                    "--evidence-ref",
                    "evidence://issue-missing-confidence-001",
                    "--provenance-json",
                    "{\"source\":\"unit-test\"}",
                )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
    store_decision_trace_records,
    store_readiness_opinion_records,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    connect_db,
    store_falsification_probe_records,
    store_moderator_action_records,
    store_promotion_basis_record,
    store_round_readiness_assessment,
)

RUN_ID = "run-council-query-001"
ROUND_ID = "round-council-query-001"


def seed_council_query_state(run_dir: Path) -> dict[str, str]:
    store_moderator_action_records(
        run_dir,
        action_snapshot={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "ranked_actions": [
                {
                    "action_kind": "advance-empirical-verification",
                    "priority": "high",
                    "assigned_role": "environmentalist",
                    "objective": "Advance smoke verification.",
                    "reason": "Coverage is still incomplete.",
                    "readiness_blocker": True,
                    "decision_source": "heuristic-fallback",
                    "issue_label": "air-quality-smoke",
                    "source_proposal_id": "proposal-seeded-route-001",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                    "source_ids": [
                        "issue-001",
                        "route-001",
                        "assessment-001",
                        "proposal-seeded-route-001",
                    ],
                    "target": {
                        "object_kind": "verification-route",
                        "object_id": "route-001",
                        "route_id": "route-001",
                        "assessment_id": "assessment-001",
                        "claim_id": "claim-001",
                        "issue_label": "air-quality-smoke",
                    },
                },
                {
                    "action_kind": "open-council-readiness-review",
                    "priority": "medium",
                    "assigned_role": "moderator",
                    "objective": "Prepare the next council review once blockers are cleared.",
                    "reason": "A structured review step should remain visible after the blocking action.",
                    "readiness_blocker": False,
                    "decision_source": "heuristic-fallback",
                    "issue_label": "air-quality-smoke",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                    "source_ids": ["issue-001"],
                    "target": {
                        "object_kind": "issue-cluster",
                        "object_id": "issue-001",
                        "map_issue_id": "issue-001",
                        "claim_id": "claim-001",
                        "issue_label": "air-quality-smoke",
                    },
                }
            ],
        },
    )
    store_falsification_probe_records(
        run_dir,
        probe_snapshot={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "probes": [
                {
                    "probe_type": "contradiction-check",
                    "probe_status": "open",
                    "owner_role": "challenger",
                    "priority": "high",
                    "probe_goal": "Test the strongest smoke claim.",
                    "falsification_question": "Do observations contradict the public smoke narrative?",
                    "decision_source": "heuristic-fallback",
                    "issue_label": "air-quality-smoke",
                    "source_proposal_id": "proposal-seeded-route-001",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                    "source_ids": [
                        "gap-001",
                        "linkage-001",
                        "proposal-seeded-route-001",
                    ],
                    "target": {
                        "object_kind": "representation-gap",
                        "object_id": "gap-001",
                        "gap_id": "gap-001",
                        "linkage_id": "linkage-001",
                        "issue_label": "air-quality-smoke",
                    },
                }
            ],
        },
    )
    store_round_readiness_assessment(
        run_dir,
        readiness_payload={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "readiness_status": "needs-more-data",
            "sufficient_for_promotion": False,
            "decision_source": "policy-fallback",
            "provenance": {"source": "unit-test"},
            "evidence_refs": [],
            "lineage": [],
            "agenda_counts": {"issue_cluster_count": 1},
            "counts": {"open_challenges": 1},
            "controversy_gap_counts": {"representation-gap": 1},
        },
    )
    store_promotion_basis_record(
        run_dir,
        promotion_payload={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "promotion_status": "withheld",
            "readiness_status": "needs-more-data",
            "decision_source": "policy-fallback",
            "provenance": {"source": "unit-test"},
            "evidence_refs": [],
            "lineage": [],
            "selected_basis_object_ids": ["issue-001"],
            "selected_evidence_refs": [],
            "frozen_basis": {
                "issue_clusters": [
                    {
                        "map_issue_id": "issue-001",
                        "issue_label": "smoke",
                        "evidence_refs": [],
                    }
                ]
            },
        },
    )
    proposal_bundle = store_council_proposal_records(
        run_dir,
        proposal_bundle={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "proposals": [
                {
                    "proposal_kind": "open-investigation-track",
                    "agent_role": "moderator",
                    "rationale": "Representation and verification gaps are both still open.",
                    "decision_source": "agent-council",
                    "target_kind": "issue-cluster",
                    "target_id": "issue-001",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                }
            ],
        },
    )
    proposal_id = proposal_bundle["proposals"][0]["proposal_id"]
    store_readiness_opinion_records(
        run_dir,
        opinion_bundle={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "opinions": [
                {
                    "agent_role": "challenger",
                    "readiness_status": "needs-more-data",
                    "rationale": "Open falsification work remains.",
                    "decision_source": "agent-council",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                    "basis_object_ids": ["issue-001"],
                }
            ],
        },
    )
    store_decision_trace_records(
        run_dir,
        trace_bundle={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "traces": [
                {
                    "decision_id": "decision-round-001",
                    "decision_kind": "round-decision",
                    "status": "recorded",
                    "selected_object_kind": "proposal",
                    "selected_object_id": proposal_id,
                    "rationale": "The round stays open until the outstanding contradictions are resolved.",
                    "decision_source": "council-trace",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                    "accepted_object_ids": [proposal_id],
                    "rejected_object_ids": [],
                }
            ],
        },
    )
    hypothesis_payload = run_script(
        script_path("eco-update-hypothesis-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        "Query-surface hypothesis",
        "--statement",
        "A board hypothesis should be queryable as a canonical deliberation object.",
        "--status",
        "active",
        "--owner-role",
        "moderator",
        "--confidence",
        "0.67",
    )
    hypothesis_id = hypothesis_payload["canonical_ids"][0]
    challenge_payload = run_script(
        script_path("eco-open-challenge-ticket"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        "Query-surface challenge",
        "--challenge-statement",
        "A board challenge should be queryable via the council object surface.",
        "--target-hypothesis-id",
        hypothesis_id,
        "--priority",
        "high",
        "--owner-role",
        "challenger",
    )
    challenge_id = challenge_payload["canonical_ids"][0]
    task_payload = run_script(
        script_path("eco-claim-board-task"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        "Query-surface board task",
        "--task-text",
        "A board task should be queryable as a canonical deliberation object.",
        "--task-type",
        "challenge-follow-up",
        "--owner-role",
        "moderator",
        "--priority",
        "high",
        "--source-ticket-id",
        challenge_id,
        "--source-hypothesis-id",
        hypothesis_id,
    )
    task_id = task_payload["canonical_ids"][0]
    return {
        "proposal_id": proposal_id,
        "hypothesis_id": hypothesis_id,
        "challenge_id": challenge_id,
        "task_id": task_id,
    }


class CouncilQuerySurfaceTests(unittest.TestCase):
    def test_kernel_lists_canonical_deliberation_contracts(self) -> None:
        payload = run_kernel("list-canonical-contracts", "--plane", "deliberation")
        kinds = {contract["object_kind"] for contract in payload["contracts"]}

        self.assertEqual("canonical-contract-list-v1", payload["schema_version"])
        self.assertEqual("deliberation", payload["plane"])
        self.assertGreaterEqual(payload["summary"]["contract_count"], 7)
        self.assertIn("proposal", kinds)
        self.assertIn("decision-trace", kinds)

    def test_kernel_queries_new_and_existing_council_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            seeded = seed_council_query_state(run_dir)
            proposal_id = seeded["proposal_id"]
            connection, _db_file = connect_db(run_dir)
            try:
                with connection:
                    action_row = connection.execute(
                        """
                        SELECT action_id, raw_json
                        FROM moderator_actions
                        WHERE run_id = ? AND round_id = ? AND target_object_id = ?
                        """,
                        (RUN_ID, ROUND_ID, "route-001"),
                    ).fetchone()
                    action_payload = json.loads(action_row["raw_json"])
                    action_payload.pop("target_object_kind", None)
                    action_payload.pop("target_object_id", None)
                    action_payload.pop("issue_label", None)
                    action_payload.pop("source_proposal_id", None)
                    action_target = (
                        action_payload.get("target", {})
                        if isinstance(action_payload.get("target"), dict)
                        else {}
                    )
                    for key in ("object_kind", "object_id", "issue_label", "route_id", "assessment_id"):
                        action_target.pop(key, None)
                    action_payload["target"] = action_target
                    connection.execute(
                        "UPDATE moderator_actions SET raw_json = ? WHERE action_id = ?",
                        (
                            json.dumps(action_payload, ensure_ascii=True, sort_keys=True),
                            action_row["action_id"],
                        ),
                    )

                    probe_row = connection.execute(
                        """
                        SELECT probe_id, raw_json
                        FROM falsification_probes
                        WHERE run_id = ? AND round_id = ? AND target_object_id = ?
                        """,
                        (RUN_ID, ROUND_ID, "gap-001"),
                    ).fetchone()
                    probe_payload = json.loads(probe_row["raw_json"])
                    probe_payload.pop("target_object_kind", None)
                    probe_payload.pop("target_object_id", None)
                    probe_payload.pop("issue_label", None)
                    probe_payload.pop("source_proposal_id", None)
                    probe_target = (
                        probe_payload.get("target", {})
                        if isinstance(probe_payload.get("target"), dict)
                        else {}
                    )
                    for key in ("object_kind", "object_id", "issue_label", "gap_id", "linkage_id"):
                        probe_target.pop(key, None)
                    probe_payload["target"] = probe_target
                    connection.execute(
                        "UPDATE falsification_probes SET raw_json = ? WHERE probe_id = ?",
                        (
                            json.dumps(probe_payload, ensure_ascii=True, sort_keys=True),
                            probe_row["probe_id"],
                        ),
                    )
            finally:
                connection.close()

            proposal_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "proposal",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "issue-cluster",
                "--target-id",
                "issue-001",
                "--include-contract",
            )
            self.assertEqual("council-object-query-v1", proposal_payload["schema_version"])
            self.assertEqual(1, proposal_payload["summary"]["returned_object_count"])
            self.assertEqual("proposal", proposal_payload["contract"]["object_kind"])
            self.assertEqual(
                "open-investigation-track",
                proposal_payload["objects"][0]["proposal_kind"],
            )

            action_payload = run_kernel(
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
            self.assertEqual(2, action_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "advance-empirical-verification",
                action_payload["objects"][0]["action_kind"],
            )
            self.assertTrue(action_payload["objects"][0]["readiness_blocker"])

            targeted_action_payload = run_kernel(
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
                "verification-route",
                "--target-id",
                "route-001",
                "--issue-label",
                "air-quality-smoke",
                "--route-id",
                "route-001",
                "--assessment-id",
                "assessment-001",
                "--source-proposal-id",
                "proposal-seeded-route-001",
            )
            self.assertEqual(
                1,
                targeted_action_payload["summary"]["returned_object_count"],
            )
            self.assertEqual(
                "verification-route",
                targeted_action_payload["objects"][0]["target_object_kind"],
            )
            self.assertEqual(
                "route-001",
                targeted_action_payload["objects"][0]["target_object_id"],
            )
            self.assertEqual(
                "proposal-seeded-route-001",
                targeted_action_payload["objects"][0]["source_proposal_id"],
            )
            self.assertEqual(
                "verification-route",
                targeted_action_payload["objects"][0]["target"]["object_kind"],
            )

            blocker_actions = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "next-action",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--readiness-blocker-only",
            )
            self.assertEqual(1, blocker_actions["summary"]["returned_object_count"])
            self.assertTrue(blocker_actions["filters"]["readiness_blocker_only"])
            self.assertEqual(
                "advance-empirical-verification",
                blocker_actions["objects"][0]["action_kind"],
            )
            self.assertTrue(blocker_actions["objects"][0]["readiness_blocker"])

            hypothesis_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "hypothesis",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--status",
                "active",
            )
            self.assertEqual(1, hypothesis_payload["summary"]["returned_object_count"])
            self.assertEqual(
                seeded["hypothesis_id"],
                hypothesis_payload["objects"][0]["hypothesis_id"],
            )
            self.assertEqual(
                "operator-command",
                hypothesis_payload["objects"][0]["decision_source"],
            )

            challenge_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "challenge",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "challenger",
                "--status",
                "open",
                "--include-contract",
            )
            self.assertEqual(1, challenge_payload["summary"]["returned_object_count"])
            self.assertEqual("challenge", challenge_payload["contract"]["object_kind"])
            self.assertEqual(
                seeded["challenge_id"],
                challenge_payload["objects"][0]["ticket_id"],
            )
            self.assertEqual(
                "operator-command",
                challenge_payload["objects"][0]["decision_source"],
            )

            task_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "board-task",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "moderator",
                "--status",
                "claimed",
                "--include-contract",
            )
            self.assertEqual(1, task_payload["summary"]["returned_object_count"])
            self.assertEqual("board-task", task_payload["contract"]["object_kind"])
            self.assertEqual(seeded["task_id"], task_payload["objects"][0]["task_id"])
            self.assertEqual(
                seeded["challenge_id"],
                task_payload["objects"][0]["source_ticket_id"],
            )
            self.assertEqual(
                "operator-command",
                task_payload["objects"][0]["decision_source"],
            )

            opinion_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "readiness-opinion",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "challenger",
            )
            self.assertEqual(1, opinion_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "needs-more-data",
                opinion_payload["objects"][0]["readiness_status"],
            )

            basis_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "promotion-basis",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-items",
            )
            self.assertEqual(1, basis_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "withheld",
                basis_payload["objects"][0]["promotion_status"],
            )
            self.assertEqual(1, len(basis_payload["objects"][0]["basis_items"]))

            trace_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "decision-trace",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--decision-id",
                "decision-round-001",
            )
            self.assertEqual(1, trace_payload["summary"]["returned_object_count"])
            self.assertEqual(
                proposal_id,
                trace_payload["objects"][0]["accepted_object_ids"][0],
            )

            probe_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "probe",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "representation-gap",
                "--target-id",
                "gap-001",
                "--issue-label",
                "air-quality-smoke",
                "--gap-id",
                "gap-001",
                "--linkage-id",
                "linkage-001",
                "--source-proposal-id",
                "proposal-seeded-route-001",
            )
            self.assertEqual(1, probe_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "representation-gap",
                probe_payload["objects"][0]["target_object_kind"],
            )
            self.assertEqual("gap-001", probe_payload["objects"][0]["target_object_id"])
            self.assertEqual(
                "linkage-001",
                probe_payload["objects"][0]["target_linkage_id"],
            )
            self.assertEqual(
                "representation-gap",
                probe_payload["objects"][0]["target"]["object_kind"],
            )


if __name__ == "__main__":
    unittest.main()

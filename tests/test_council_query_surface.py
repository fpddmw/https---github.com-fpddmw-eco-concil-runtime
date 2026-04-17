from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
    store_decision_trace_records,
    store_readiness_opinion_records,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_falsification_probe_records,
    store_moderator_action_records,
    store_promotion_basis_record,
    store_round_readiness_assessment,
)

RUN_ID = "run-council-query-001"
ROUND_ID = "round-council-query-001"


def seed_council_query_state(run_dir: Path) -> str:
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
                    "decision_source": "heuristic-fallback",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
                    "source_ids": ["issue-001"],
                    "target": {"claim_id": "claim-001"},
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
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": [],
                    "lineage": [],
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
    return proposal_id


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
            proposal_id = seed_council_query_state(run_dir)

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
            self.assertEqual(1, action_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "advance-empirical-verification",
                action_payload["objects"][0]["action_kind"],
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


if __name__ == "__main__":
    unittest.main()

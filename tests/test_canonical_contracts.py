from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.canonical_contracts import (  # noqa: E402
    PLANE_DELIBERATION,
    PLANE_REPORTING,
    canonical_contract,
    canonical_contract_kinds,
    validate_canonical_payload,
)


class CanonicalContractTests(unittest.TestCase):
    def test_registry_exposes_batch1_target_contracts(self) -> None:
        expected_kinds = {
            "formal-comment-signal",
            "public-discourse-signal",
            "environment-observation-signal",
            "issue-cluster",
            "stance-group",
            "concern-facet",
            "actor-profile",
            "evidence-citation-type",
            "verifiability-assessment",
            "verification-route",
            "formal-public-link",
            "representation-gap",
            "diffusion-edge",
            "controversy-map",
            "proposal",
            "hypothesis",
            "challenge",
            "board-task",
            "next-action",
            "probe",
            "readiness-opinion",
            "readiness-assessment",
            "promotion-basis",
            "decision-trace",
            "reporting-handoff",
            "council-decision",
            "expert-report",
            "final-publication",
        }
        self.assertSetEqual(expected_kinds, set(canonical_contract_kinds()))
        self.assertSetEqual(
            {
                "proposal",
                "hypothesis",
                "challenge",
                "board-task",
                "next-action",
                "probe",
                "readiness-opinion",
                "readiness-assessment",
                "promotion-basis",
                "decision-trace",
            },
            set(canonical_contract_kinds(plane=PLANE_DELIBERATION)),
        )
        self.assertSetEqual(
            {
                "reporting-handoff",
                "council-decision",
                "expert-report",
                "final-publication",
            },
            set(canonical_contract_kinds(plane=PLANE_REPORTING)),
        )

    def test_validate_canonical_payload_rejects_missing_structural_fields(self) -> None:
        with self.assertRaises(ValueError):
            validate_canonical_payload(
                "proposal",
                {
                    "run_id": "run-001",
                    "round_id": "round-001",
                    "proposal_id": "proposal-001",
                    "proposal_kind": "open-investigation",
                    "agent_role": "moderator",
                    "rationale": "Need more analysis.",
                    "decision_source": "agent-council",
                    "evidence_refs": [],
                },
            )

    def test_validate_canonical_payload_accepts_well_formed_proposal(self) -> None:
        payload = validate_canonical_payload(
            "proposal",
            {
                "run_id": "run-001",
                "round_id": "round-001",
                "proposal_id": "proposal-001",
                "proposal_kind": "open-investigation",
                "agent_role": "moderator",
                "status": "open",
                "rationale": "Need more analysis.",
                "decision_source": "agent-council",
                "evidence_refs": [],
                "lineage": [],
                "response_to_ids": [],
                "provenance": {"source": "unit-test"},
            },
        )
        contract = canonical_contract("proposal")
        self.assertEqual(contract.schema_version, payload["schema_version"])
        self.assertEqual("proposal-001", payload["proposal_id"])


if __name__ == "__main__":
    unittest.main()

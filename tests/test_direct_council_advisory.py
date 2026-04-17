from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, runtime_path, runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
    store_readiness_opinion_records,
)
from eco_council_runtime.phase2_direct_advisory import materialize_direct_council_advisory_plan  # noqa: E402

RUN_ID = "run-direct-advisory-001"
ROUND_ID = "round-direct-advisory-001"


class DirectCouncilAdvisoryTests(unittest.TestCase):
    def test_materializes_direct_advisory_from_readiness_opinions(self) -> None:
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
                            "rationale": "The round is ready for promotion review.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = materialize_direct_council_advisory_plan(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )

            plan = load_json(runtime_path(run_dir, f"agent_advisory_plan_{ROUND_ID}.json"))
            self.assertEqual("completed", payload["status"])
            self.assertEqual("direct-council-advisory-plan", payload["event"]["event_type"])
            self.assertEqual("direct-council-advisory", payload["skill_payload"]["plan_source"])
            self.assertEqual("direct-council-advisory", plan["plan_source"])
            self.assertEqual(["round-readiness"], [item["stage_name"] for item in plan["execution_queue"]])
            self.assertEqual("promote-candidate", plan["downstream_posture"])
            self.assertTrue(plan["observed_state"]["direct_council_queue"])

    def test_non_probe_proposal_without_readiness_still_compiles_direct_queue(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "gather-evidence",
                            "action_kind": "gather-evidence",
                            "agent_role": "environmentalist",
                            "assigned_role": "environmentalist",
                            "objective": "Collect one more physical evidence slice before final promotion review.",
                            "rationale": "The council wants another governed evidence pass, but not a probe-first loop.",
                            "target_kind": "claim",
                            "target_id": "claim-001",
                            "recommended_lane": "environment-review",
                            "decision_source": "agent-council",
                            "evidence_refs": ["evidence://claim-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = materialize_direct_council_advisory_plan(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )

            plan = load_json(runtime_path(run_dir, f"agent_advisory_plan_{ROUND_ID}.json"))
            self.assertEqual("completed", payload["status"])
            self.assertEqual("direct-council-advisory", plan["plan_source"])
            self.assertEqual(["round-readiness"], [item["stage_name"] for item in plan["execution_queue"]])
            self.assertFalse(plan["probe_stage_included"])
            self.assertEqual("hold-investigation-open", plan["downstream_posture"])
            self.assertEqual(
                1,
                plan["phase_decision_basis"]["council_input_counts"]["proposal_action_count"],
            )
            self.assertEqual(
                ["pending-investigation-actions"],
                plan["phase_decision_basis"]["posture_reason_codes"],
            )


if __name__ == "__main__":
    unittest.main()

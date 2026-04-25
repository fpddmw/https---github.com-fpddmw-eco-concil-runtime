from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from _workflow_support import (
    request_and_approve_skill_approval,
    run_kernel,
    run_kernel_process,
    runtime_src_path,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-skill-approval-001"
ROUND_ID = "round-skill-approval-001"
OPTIONAL_SKILL = "eco-extract-claim-candidates"


class SkillApprovalWorkflowTests(unittest.TestCase):
    def test_preflight_blocks_optional_analysis_without_approved_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            completed = run_kernel_process(
                "preflight-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                OPTIONAL_SKILL,
                "--actor-role",
                "moderator",
                "--contract-mode",
                "warn",
                auto_actor_role=False,
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "missing-skill-approval-request-id",
                {item["code"] for item in payload["preflight"]["issues"]},
            )

    def test_request_approve_and_query_skill_approval_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            request_payload = run_kernel(
                "request-skill-approval",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                OPTIONAL_SKILL,
                "--requested-actor-role",
                "environmental-investigator",
                "--rationale",
                "Need optional analysis for claim extraction.",
                "--actor-role",
                "moderator",
            )
            request_id = request_payload["summary"]["request_id"]

            approval_payload = run_kernel(
                "approve-skill-approval",
                "--run-dir",
                str(run_dir),
                "--request-id",
                request_id,
                "--approval-reason",
                "Approved optional analysis for this round.",
                "--actor-role",
                "runtime-operator",
            )

            self.assertEqual(request_id, approval_payload["summary"]["request_id"])
            self.assertEqual("approved", approval_payload["summary"]["request_status"])
            self.assertEqual(OPTIONAL_SKILL, approval_payload["summary"]["skill_name"])

            request_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "skill-approval-request",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--request-id",
                request_id,
            )
            self.assertEqual(1, request_query["summary"]["returned_object_count"])
            self.assertEqual(request_id, request_query["objects"][0]["request_id"])
            self.assertEqual("approved", request_query["objects"][0]["request_status"])

            approval_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "skill-approval",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--request-id",
                request_id,
            )
            self.assertEqual(1, approval_query["summary"]["returned_object_count"])
            self.assertEqual(request_id, approval_query["objects"][0]["request_id"])
            self.assertEqual("approved", approval_query["objects"][0]["decision_status"])

    def test_run_skill_consumes_approved_skill_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            request_id = request_and_approve_skill_approval(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name=OPTIONAL_SKILL,
                requested_actor_role="moderator",
            )

            from eco_council_runtime.kernel.executor import run_skill
            from eco_council_runtime.kernel.skill_approvals import load_skill_approval_request

            fake_skill_entry = {
                "skill_name": OPTIONAL_SKILL,
                "script_path": str((run_dir / "fake_optional_skill.py").resolve()),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            fake_payload = {
                "status": "completed",
                "summary": {
                    "skill": OPTIONAL_SKILL,
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                },
                "receipt_id": "runtime-receipt-skill-approval-consume",
                "artifact_refs": [],
                "canonical_ids": ["claim-candidate-1"],
            }

            with (
                mock.patch(
                    "eco_council_runtime.kernel.governance.resolve_skill_entry",
                    return_value=fake_skill_entry,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.executor.resolve_skill_entry",
                    return_value=fake_skill_entry,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.executor.subprocess.run",
                    return_value=subprocess.CompletedProcess(
                        args=["python"],
                        returncode=0,
                        stdout=json.dumps(fake_payload),
                        stderr="",
                    ),
                ),
            ):
                payload = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name=OPTIONAL_SKILL,
                    actor_role="moderator",
                    skill_args=[],
                    contract_mode="warn",
                    skill_approval_request_id=request_id,
                )

            self.assertEqual("completed", payload["status"])
            self.assertEqual(request_id, payload["summary"]["skill_approval_request_id"])

            request_after = load_skill_approval_request(run_dir, request_id=request_id)
            self.assertEqual("consumed", request_after["request_status"])
            self.assertEqual(
                payload["summary"]["receipt_id"],
                request_after["consumed_receipt_id"],
            )
            self.assertEqual(
                payload["summary"]["event_id"],
                request_after["consumed_event_id"],
            )

            consumption_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "skill-approval-consumption",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--request-id",
                request_id,
            )
            self.assertEqual(1, consumption_query["summary"]["returned_object_count"])
            self.assertEqual(
                payload["summary"]["receipt_id"],
                consumption_query["objects"][0]["execution_receipt_id"],
            )

    def test_skill_approval_approve_rejects_non_operator_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            from eco_council_runtime.kernel.skill_approvals import (
                approve_skill_approval_request,
                store_skill_approval_request,
            )

            request = store_skill_approval_request(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name=OPTIONAL_SKILL,
                requested_by_role="moderator",
                requested_actor_role="moderator",
                rationale="Moderator requests optional-analysis execution.",
            )

            with self.assertRaises(ValueError) as raised:
                approve_skill_approval_request(
                    run_dir,
                    request_id=request["request_id"],
                    approved_by_role="moderator",
                    decision_reason="Moderator cannot self-approve optional-analysis requests.",
                )

            self.assertIn(
                "approve_skill_approval_request requires actor role `runtime-operator`",
                str(raised.exception),
            )


if __name__ == "__main__":
    unittest.main()

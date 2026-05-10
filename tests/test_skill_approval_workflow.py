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
OPTIONAL_SKILL = "discover-discourse-issues"
REPORTING_PUBLISH_SKILL = "publish-expert-report"


def all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        keys: set[str] = set()
        for key, child in value.items():
            keys.add(str(key))
            keys.update(all_keys(child))
        return keys
    if isinstance(value, list):
        keys = set()
        for child in value:
            keys.update(all_keys(child))
        return keys
    return set()


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
            self.assertIn(
                "request-skill-approval",
                payload["preflight"]["skill_approval"][
                    "request_skill_approval_command_template"
                ],
            )

    def test_preflight_accepts_optional_analysis_after_approval_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            blocked = run_kernel_process(
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

            self.assertEqual(1, blocked.returncode)
            blocked_payload = json.loads(blocked.stdout)
            self.assertIn(
                "missing-skill-approval-request-id",
                {item["code"] for item in blocked_payload["preflight"]["issues"]},
            )

            request_payload = run_kernel(
                "request-skill-approval",
                "--run_dir",
                str(run_dir),
                "--run_id",
                RUN_ID,
                "--round_id",
                ROUND_ID,
                "--skill_name",
                OPTIONAL_SKILL,
                "--requested_actor_role",
                "moderator",
                "--rationale",
                "Record approval for optional discourse discovery.",
                "--actor_role",
                "runtime-operator",
            )
            request_id = request_payload["summary"]["request_id"]
            run_kernel(
                "approve-skill-approval",
                "--run_dir",
                str(run_dir),
                "--request_id",
                request_id,
                "--approval_reason",
                "Approved operator-owned optional audit.",
                "--actor_role",
                "runtime-operator",
            )

            approved = run_kernel(
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
                "--skill-approval-request-id",
                request_id,
            )
            self.assertEqual("completed", approved["status"])
            self.assertEqual("approved", approved["preflight"]["skill_approval"]["status"])

    def test_preflight_blocks_reporting_publish_without_approved_request(self) -> None:
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
                REPORTING_PUBLISH_SKILL,
                "--actor-role",
                "report-editor",
                "--contract-mode",
                "warn",
                "--",
                "--role",
                "social-investigator",
                auto_actor_role=False,
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "missing-skill-approval-request-id",
                {item["code"] for item in payload["preflight"]["issues"]},
            )

    def test_preflight_accepts_reporting_publish_after_approval_record_in_strict_mode(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            request_id = request_and_approve_skill_approval(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name=REPORTING_PUBLISH_SKILL,
                requested_actor_role="report-editor",
                rationale="Approve report publication through governed runtime.",
                approval_reason="Approved report publication for this round.",
            )

            approved = run_kernel(
                "preflight-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                REPORTING_PUBLISH_SKILL,
                "--actor-role",
                "report-editor",
                "--contract-mode",
                "strict",
                "--skill-approval-request-id",
                request_id,
                "--",
                "--role",
                "social-investigator",
            )

            self.assertEqual("completed", approved["status"])
            self.assertFalse(approved["preflight"]["block_execution"])
            self.assertEqual(
                "approved",
                approved["preflight"]["skill_approval"]["status"],
            )
            self.assertEqual(
                request_id,
                approved["preflight"]["skill_approval"]["request_id"],
            )
            self.assertNotIn(
                "operator-approval-required",
                {item["code"] for item in approved["preflight"]["issues"]},
            )

    def test_skill_approval_request_rejects_state_transition_skill(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            completed = run_kernel_process(
                "request-skill-approval",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "freeze-report-basis",
                "--requested-actor-role",
                "moderator",
                "--rationale",
                "Wrong approval path should be rejected.",
                "--actor-role",
                "moderator",
                auto_actor_role=False,
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("failed", payload["status"])
            self.assertIn("phase transition requests", payload["message"])

    def test_preflight_rejects_approved_reporting_request_with_different_args(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            request_id = request_and_approve_skill_approval(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name=REPORTING_PUBLISH_SKILL,
                requested_actor_role="report-editor",
                requested_skill_args=["--role", "environmental-investigator"],
                rationale="Approve environmental_investigator report publication only.",
                approval_reason="Approved the requested reporting argument scope.",
            )

            completed = run_kernel_process(
                "preflight-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                REPORTING_PUBLISH_SKILL,
                "--actor-role",
                "report-editor",
                "--contract-mode",
                "strict",
                "--skill-approval-request-id",
                request_id,
                "--",
                "--role",
                "social-investigator",
                auto_actor_role=False,
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "invalid-skill-approval-request",
                {item["code"] for item in payload["preflight"]["issues"]},
            )
            self.assertIn("scoped to skill args", payload["preflight"]["skill_approval"]["message"])

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
                "Need optional discourse issue discovery for this round.",
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

    def test_council_status_exposes_skill_approval_bridge_without_ranking(self) -> None:
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
                "moderator",
                "--rationale",
                "Need optional discourse issue discovery for this round.",
                "--actor-role",
                "moderator",
            )
            request_id = request_payload["summary"]["request_id"]

            pending_status = run_kernel(
                "show-council-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            bridge = pending_status["skill_approval_bridge"]

            self.assertEqual(1, pending_status["summary"]["pending_skill_approval_request_count"])
            self.assertIn(request_id, [item["request_id"] for item in bridge["pending_requests"]])
            self.assertIn("request-skill-approval", bridge["commands"]["request_skill_approval_template"])
            self.assertIn("approve-skill-approval", bridge["commands"]["approve_skill_approval_template"])
            self.assertIn("reject-skill-approval", bridge["commands"]["reject_skill_approval_template"])
            self.assertIn("--skill-approval-request-id", bridge["commands"]["run_approved_skill_template"])
            self.assertFalse({"score", "rank", "weight", "priority"} & all_keys(pending_status))

            run_kernel(
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
            approved_status = run_kernel(
                "show-council-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual(0, approved_status["summary"]["pending_skill_approval_request_count"])
            self.assertIn(
                request_id,
                [
                    item["request_id"]
                    for item in approved_status["skill_approval_bridge"]["approved_unconsumed_requests"]
                ],
            )

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

            from eco_council_runtime.kernel.execution.executor import run_skill
            from eco_council_runtime.kernel.governance.skill_approvals import load_skill_approval_request

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
                "canonical_ids": ["discourse-hint-1"],
            }

            with (
                mock.patch(
                    "eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry",
                    return_value=fake_skill_entry,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.resolve_skill_entry",
                    return_value=fake_skill_entry,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
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

            reused = run_kernel_process(
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
                "--skill-approval-request-id",
                request_id,
                auto_actor_role=False,
            )
            self.assertEqual(1, reused.returncode)
            reused_payload = json.loads(reused.stdout)
            self.assertEqual("blocked", reused_payload["status"])
            self.assertTrue(
                any(
                    "already consumed" in item.get("message", "")
                    for item in reused_payload["preflight"]["issues"]
                )
            )

    def test_skill_approval_approve_rejects_non_operator_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"

            from eco_council_runtime.kernel.governance.skill_approvals import (
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

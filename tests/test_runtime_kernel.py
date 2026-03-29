from __future__ import annotations

import json
import io
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from _workflow_support import kernel_script_path, load_json, run_kernel, run_kernel_process, run_script, runtime_src_path, script_path, seed_analysis_chain

RUN_ID = "run-kernel-001"
ROUND_ID = "round-kernel-001"


def ensure_runtime_src_on_path() -> None:
    runtime_src = runtime_src_path()
    if str(runtime_src) not in sys.path:
        sys.path.insert(0, str(runtime_src))


class RuntimeKernelTests(unittest.TestCase):
    def test_kernel_tracks_manifest_cursor_and_ledger(self) -> None:
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
                "--note-text",
                "Kernel test note.",
                "--linked-artifact-ref",
                coverage_payload["artifact_refs"][0]["artifact_ref"],
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
                "Kernel test hypothesis",
                "--statement",
                "Evidence is strong enough for kernel-driven summary and action planning.",
                "--status",
                "active",
                "--owner-role",
                "moderator",
                "--linked-claim-id",
                outputs["cluster_claims"]["canonical_ids"][0],
                "--confidence",
                "0.88",
            )

            init_payload = run_kernel(
                "init-run",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
            )
            self.assertGreaterEqual(init_payload["summary"]["skill_count"], 1)

            first_run = run_kernel(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-summarize-board-state",
            )
            second_run = run_kernel(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-propose-next-actions",
            )
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--tail",
                "5",
            )

            runtime_dir = run_dir / "runtime"
            manifest = load_json(runtime_dir / "run_manifest.json")
            cursor = load_json(runtime_dir / "round_cursor.json")
            registry = load_json(runtime_dir / "skill_registry.json")

            self.assertEqual(2, manifest["invocation_count"])
            self.assertEqual(ROUND_ID, manifest["last_round_id"])
            self.assertEqual("eco-propose-next-actions", manifest["last_skill_name"])
            self.assertEqual(ROUND_ID, cursor["current_round_id"])
            self.assertEqual("eco-propose-next-actions", cursor["last_skill_name"])
            self.assertGreaterEqual(registry["skill_count"], 1)
            self.assertEqual(2, len(state_payload["ledger_tail"]))
            self.assertEqual(first_run["summary"]["receipt_id"], state_payload["ledger_tail"][0]["receipt_id"])
            self.assertEqual(second_run["summary"]["receipt_id"], state_payload["ledger_tail"][1]["receipt_id"])
            self.assertTrue((runtime_dir / "receipts" / f"{first_run['summary']['receipt_id']}.json").exists())
            self.assertTrue((runtime_dir / "receipts" / f"{second_run['summary']['receipt_id']}.json").exists())
            self.assertEqual(kernel_script_path().name, "eco_runtime_kernel.py")

    def test_runtime_registry_and_ledger_capture_contract_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            init_payload = run_kernel(
                "init-run",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
            )
            registry = init_payload["registry"]
            handoff_entry = next(item for item in registry["skills"] if item["skill_name"] == "eco-materialize-reporting-handoff")
            self.assertEqual("runtime-registry-v2", registry["schema_version"])
            self.assertIn("run_dir/promotion/promoted_evidence_basis_<round_id>.json", handoff_entry["declared_contract"]["reads"])
            self.assertEqual("Eco Materialize Reporting Handoff", handoff_entry["agent"]["display_name"])

            payload = run_kernel(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-post-board-note",
                "--",
                "--author-role",
                "moderator",
                "--note-text",
                "Runtime metadata note.",
            )
            event = payload["event"]

            self.assertEqual("runtime-event-v3", event["schema_version"])
            self.assertEqual(["--author-role", "moderator", "--note-text", "Runtime metadata note."], event["skill_args"])
            self.assertEqual("eco-post-board-note", event["skill_registry_entry"]["skill_name"])
            self.assertIn(str((run_dir / "board" / f"investigation_board.json").resolve()), event["resolved_write_paths"])
            self.assertIn("argv", event["command_snapshot"])
            self.assertTrue(event["execution_input_hash"])
            self.assertTrue(event["payload_hash"])

    def test_preflight_warn_reports_missing_required_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            payload = run_kernel(
                "preflight-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-post-board-note",
                "--contract-mode",
                "warn",
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("warn", payload["summary"]["contract_mode"])
            self.assertFalse(payload["preflight"]["block_execution"])
            self.assertIn("missing-required-input", {item["code"] for item in payload["preflight"]["issues"]})

    def test_strict_mode_blocks_missing_required_inputs_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            completed = run_kernel_process(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-post-board-note",
                "--contract-mode",
                "strict",
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("failed", payload["status"])
            self.assertTrue(payload["preflight"]["block_execution"])
            self.assertIn("missing-required-input", {item["code"] for item in payload["preflight"]["issues"]})

    def test_warn_mode_allows_path_override_but_records_governance_issues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            payload = run_kernel(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-post-board-note",
                "--contract-mode",
                "warn",
                "--",
                "--author-role",
                "moderator",
                "--note-text",
                "Governance warning note.",
                "--board-path",
                "board/alternate_board.json",
            )

            self.assertEqual("warn", payload["summary"]["contract_mode"])
            self.assertGreaterEqual(payload["governance"]["preflight"]["issue_count"], 1)
            self.assertGreaterEqual(payload["governance"]["postflight"]["issue_count"], 1)
            self.assertIn("undeclared-path-override", {item["code"] for item in payload["governance"]["preflight"]["issues"]})
            self.assertIn("undeclared-summary-path", {item["code"] for item in payload["governance"]["postflight"]["issues"]})

    def test_strict_preflight_blocks_undeclared_path_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            completed = run_kernel_process(
                "preflight-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "eco-post-board-note",
                "--contract-mode",
                "strict",
                "--",
                "--author-role",
                "moderator",
                "--note-text",
                "Blocked note.",
                "--board-path",
                "board/alternate_board.json",
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("blocked", payload["status"])
            self.assertTrue(payload["preflight"]["block_execution"])
            self.assertIn("undeclared-path-override", {item["code"] for item in payload["preflight"]["issues"]})

    def test_postflight_strict_blocks_artifact_ref_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.executor import SkillExecutionError, run_skill

            output_path = (run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").resolve()
            fake_payload = {
                "status": "completed",
                "summary": {
                    "skill": "eco-summarize-board-state",
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "output_path": str(output_path),
                },
                "receipt_id": "runtime-receipt-test-postflight",
                "batch_id": "runtimebatch-postflight",
                "artifact_refs": [
                    {
                        "signal_id": "",
                        "artifact_path": str(output_path),
                        "record_locator": "$.summary",
                        "artifact_ref": f"{output_path}:$.wrong",
                    }
                ],
                "canonical_ids": ["summary-test"],
                "warnings": [],
                "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [], "challenge_hints": [], "suggested_next_skills": []},
            }

            with mock.patch(
                "eco_council_runtime.kernel.executor.subprocess.run",
                return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(fake_payload), stderr=""),
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="eco-summarize-board-state",
                        skill_args=[],
                        contract_mode="strict",
                    )

            payload = raised.exception.payload
            self.assertEqual("failed", payload["status"])
            self.assertIn("artifact-ref-mismatch", {item["code"] for item in payload["postflight"]["issues"]})

    def test_strict_preflight_requires_explicit_high_risk_side_effect_approval(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance import preflight_skill_execution

            fake_skill_entry = {
                "skill_name": "eco-fake-network-fetch",
                "script_path": str(root / "fake_skill.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": ["network-external"],
                "execution_policy": {},
                "agent": {},
            }

            with mock.patch("eco_council_runtime.kernel.governance.resolve_skill_entry", return_value=fake_skill_entry):
                blocked = preflight_skill_execution(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="eco-fake-network-fetch",
                    skill_args=[],
                    contract_mode="strict",
                )
                approved = preflight_skill_execution(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="eco-fake-network-fetch",
                    skill_args=[],
                    contract_mode="strict",
                    allow_side_effects=["network-external"],
                )

            self.assertTrue(blocked["block_execution"])
            self.assertIn("missing-side-effect-approval", {item["code"] for item in blocked["issues"]})
            self.assertEqual(["network-external"], approved["declared_side_effects"])
            self.assertIn("network-external", approved["allowed_side_effects"])
            self.assertFalse(approved["block_execution"])

    def test_run_skill_retries_and_recovers_after_transient_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.executor import run_skill
            from eco_council_runtime.kernel.ledger import load_ledger_tail

            fake_skill_entry = {
                "skill_name": "eco-fake-retryable-skill",
                "script_path": str(root / "fake_retry.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            recovered_payload = {
                "status": "completed",
                "summary": {"result": "ok"},
                "receipt_id": "runtime-receipt-retry-success",
                "artifact_refs": [],
                "canonical_ids": [],
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.executor.subprocess.run",
                    side_effect=[
                        subprocess.CompletedProcess(args=["python"], returncode=3, stdout="", stderr="temporary upstream error"),
                        subprocess.CompletedProcess(args=["python"], returncode=0, stdout=json.dumps(recovered_payload), stderr=""),
                    ],
                ),
                mock.patch("eco_council_runtime.kernel.executor.time.sleep") as sleep_mock,
            ):
                payload = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="eco-fake-retryable-skill",
                    skill_args=[],
                    contract_mode="warn",
                    retry_budget=1,
                    retry_backoff_ms=25,
                )

            ledger_event = load_ledger_tail(run_dir, 1)[0]
            self.assertEqual("completed", payload["status"])
            self.assertEqual(2, payload["summary"]["attempt_count"])
            self.assertTrue(payload["summary"]["recovered_after_retry"])
            self.assertEqual(2, payload["event"]["attempt_count"])
            self.assertTrue(payload["event"]["recovered_after_retry"])
            self.assertEqual("exit-nonzero", payload["event"]["attempts"][0]["outcome"])
            self.assertEqual("completed", payload["event"]["attempts"][1]["outcome"])
            self.assertEqual(2, ledger_event["attempt_count"])
            self.assertTrue(ledger_event["recovered_after_retry"])
            sleep_mock.assert_called_once()

    def test_run_skill_timeout_returns_structured_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.executor import SkillExecutionError, run_skill
            from eco_council_runtime.kernel.ledger import load_ledger_tail

            fake_skill_entry = {
                "skill_name": "eco-fake-slow-skill",
                "script_path": str(root / "fake_slow.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.executor.subprocess.run",
                    side_effect=subprocess.TimeoutExpired(cmd=["python"], timeout=0.01, output="partial", stderr="still running"),
                ),
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="eco-fake-slow-skill",
                        skill_args=[],
                        contract_mode="warn",
                        timeout_seconds=0.01,
                    )

            payload = raised.exception.payload
            ledger_event = load_ledger_tail(run_dir, 1)[0]
            self.assertEqual("failed", payload["status"])
            self.assertEqual("skill-timeout", payload["failure"]["error_code"])
            self.assertEqual(1, payload["failure"]["attempt_count"])
            self.assertFalse(payload["failure"]["retryable"])
            self.assertEqual("failed", ledger_event["status"])
            self.assertEqual("skill-timeout", ledger_event["failure"]["error_code"])
            self.assertEqual(1, ledger_event["attempt_count"])

    def test_controller_forwards_execution_policy_and_records_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode

            planner_result = {
                "summary": {"skill_name": "eco-plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            board_summary_result = {
                "summary": {"skill_name": "eco-summarize-board-state", "event_id": "evt-step", "receipt_id": "receipt-step"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_summary.json")}},
            }
            board_brief_result = {
                "summary": {"skill_name": "eco-materialize-board-brief", "event_id": "evt-brief", "receipt_id": "receipt-brief"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_brief.md")}},
            }
            next_actions_result = {
                "summary": {"skill_name": "eco-propose-next-actions", "event_id": "evt-next", "receipt_id": "receipt-next"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "next_actions.json")}},
            }
            readiness_result = {
                "summary": {"skill_name": "eco-summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "readiness.json"), "readiness_status": "ready"}},
            }
            promotion_result = {
                "summary": {"skill_name": "eco-promote-evidence-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {"summary": {"promotion_status": "promoted"}, "artifact_refs": [], "canonical_ids": []},
            }
            planning = {
                "plan_id": "plan-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "eco-plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": [],
                "execution_queue": [
                    {
                        "stage_name": "board-summary",
                        "skill_name": "eco-summarize-board-state",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "board_summary.json"),
                    },
                    {
                        "stage_name": "board-brief",
                        "skill_name": "eco-materialize-board-brief",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "board_brief.md"),
                    },
                    {
                        "stage_name": "next-actions",
                        "skill_name": "eco-propose-next-actions",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "next_actions.json"),
                    },
                    {
                        "stage_name": "round-readiness",
                        "skill_name": "eco-summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "readiness.json"),
                    },
                ],
                "post_gate_steps": [
                    {"stage_name": "promotion-basis", "skill_name": "eco-promote-evidence-basis", "skill_args": [], "assigned_role_hint": "moderator", "reason": "test"}
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": [],
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "promote-ready",
                "readiness_status": "ready",
                "promote_allowed": True,
                "output_path": str(root / "promotion_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.kernel.controller.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[planner_result, board_summary_result, board_brief_result, next_actions_result, readiness_result, promotion_result],
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    timeout_seconds=12.5,
                    retry_budget=2,
                    retry_backoff_ms=150,
                    allow_side_effects=["network-external"],
                )

            self.assertEqual(6, run_skill_mock.call_count)
            for call in run_skill_mock.call_args_list:
                self.assertEqual(12.5, call.kwargs["timeout_seconds"])
                self.assertEqual(2, call.kwargs["retry_budget"])
                self.assertEqual(150, call.kwargs["retry_backoff_ms"])
                self.assertEqual(["network-external"], call.kwargs["allow_side_effects"])
            self.assertEqual(12.5, payload["controller"]["execution_policy"]["timeout_seconds"])
            self.assertEqual(2, payload["controller"]["execution_policy"]["retry_budget"])
            self.assertEqual(["network-external"], payload["controller"]["execution_policy"]["allow_side_effects"])
            self.assertEqual("runtime-controller-v3", payload["controller"]["schema_version"])
            self.assertEqual("fresh-run", payload["controller"]["resume_status"])
            self.assertEqual("eco-summarize-board-state", payload["controller"]["stage_contracts"]["board-summary"]["expected_skill_name"])

    def test_controller_resume_skips_completed_stages_after_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode
            from eco_council_runtime.kernel.executor import SkillExecutionError

            planner_result = {
                "summary": {"skill_name": "eco-plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            board_summary_result = {
                "summary": {"skill_name": "eco-summarize-board-state", "event_id": "evt-summary", "receipt_id": "receipt-summary"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_summary.json")}},
            }
            board_brief_result = {
                "summary": {"skill_name": "eco-materialize-board-brief", "event_id": "evt-brief", "receipt_id": "receipt-brief"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_brief.md")}},
            }
            next_actions_result = {
                "summary": {"skill_name": "eco-propose-next-actions", "event_id": "evt-next", "receipt_id": "receipt-next"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "next_actions.json")}},
            }
            readiness_result = {
                "summary": {"skill_name": "eco-summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "readiness.json"), "readiness_status": "ready"},
                },
            }
            promotion_result = {
                "summary": {"skill_name": "eco-promote-evidence-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "basis.json"), "promotion_status": "promoted"}},
            }
            planning = {
                "plan_id": "plan-resume-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "eco-plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": ["moderator"],
                "execution_queue": [
                    {"stage_name": "board-summary", "skill_name": "eco-summarize-board-state", "skill_args": [], "assigned_role_hint": "moderator", "reason": "refresh board"},
                    {"stage_name": "board-brief", "skill_name": "eco-materialize-board-brief", "skill_args": [], "assigned_role_hint": "moderator", "reason": "refresh brief"},
                    {"stage_name": "next-actions", "skill_name": "eco-propose-next-actions", "skill_args": [], "assigned_role_hint": "moderator", "reason": "rank next actions"},
                    {"stage_name": "round-readiness", "skill_name": "eco-summarize-round-readiness", "skill_args": [], "assigned_role_hint": "moderator", "reason": "refresh readiness"},
                ],
                "post_gate_steps": [
                    {"stage_name": "promotion-basis", "skill_name": "eco-promote-evidence-basis", "skill_args": [], "assigned_role_hint": "moderator", "reason": "freeze promotion basis"}
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": ["eco-post-board-note"],
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "allow-promote",
                "readiness_status": "ready",
                "promote_allowed": True,
                "output_path": str(root / "promotion_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            board_brief_failure = SkillExecutionError(
                "board brief failed",
                {
                    "status": "failed",
                    "message": "board brief failed",
                    "summary": {"skill_name": "eco-materialize-board-brief", "run_id": RUN_ID, "round_id": ROUND_ID},
                    "failure": {"error_code": "skill-exit-nonzero", "retryable": True},
                },
            )

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.kernel.controller.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[planner_result, board_summary_result, board_brief_failure],
                ),
            ):
                with self.assertRaises(SkillExecutionError):
                    run_phase2_round_with_contract_mode(run_dir, run_id=RUN_ID, round_id=ROUND_ID, contract_mode="warn")

            controller_artifact = load_json(run_dir / "runtime" / f"round_controller_{ROUND_ID}.json")
            self.assertEqual("failed", controller_artifact["controller_status"])
            self.assertEqual("board-brief", controller_artifact["failed_stage"])
            self.assertEqual(["orchestration-planner", "board-summary"], controller_artifact["completed_stage_names"])
            self.assertIn("board-brief", controller_artifact["pending_stage_names"])
            self.assertTrue(controller_artifact["resume_recommended"])

            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--tail",
                "5",
            )
            self.assertEqual("failed", state_payload["phase2"]["operator"]["controller_status"])
            self.assertEqual("board-brief", state_payload["phase2"]["operator"]["failed_stage"])
            self.assertIn("resume-phase2-round", state_payload["phase2"]["operator"]["resume_command"])

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle") as planning_bundle_mock,
                mock.patch("eco_council_runtime.kernel.controller.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[board_brief_result, next_actions_result, readiness_result, promotion_result],
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(run_dir, run_id=RUN_ID, round_id=ROUND_ID, contract_mode="warn")

            planning_bundle_mock.assert_not_called()
            self.assertEqual(
                ["eco-materialize-board-brief", "eco-propose-next-actions", "eco-summarize-round-readiness", "eco-promote-evidence-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("completed", payload["controller"]["controller_status"])
            self.assertEqual("resumed", payload["controller"]["resume_status"])
            self.assertEqual("promoted", payload["controller"]["promotion_status"])
            self.assertFalse(payload["controller"]["resume_recommended"])

    def test_supervisor_forwards_execution_policy_and_records_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.supervisor import supervise_round_with_contract_mode

            controller_result = {
                "controller": {
                    "planning_mode": "planner-backed",
                    "controller_status": "completed",
                    "resume_status": "fresh-run",
                    "current_stage": "",
                    "failed_stage": "",
                    "resume_recommended": False,
                    "restart_recommended": False,
                    "recovery": {"resume_from_stage": ""},
                    "readiness_status": "ready",
                    "gate_status": "promote-ready",
                    "promotion_status": "promoted",
                    "recommended_next_skills": ["eco-materialize-reporting-handoff"],
                    "gate_reasons": [],
                    "artifacts": {
                        "next_actions_path": "",
                        "orchestration_plan_path": str(root / "plan.json"),
                        "controller_state_path": str(root / "controller.json"),
                        "promotion_gate_path": str(root / "gate.json"),
                        "promotion_basis_path": str(root / "basis.json"),
                    },
                }
            }

            with mock.patch(
                "eco_council_runtime.kernel.supervisor.run_phase2_round_with_contract_mode",
                return_value=controller_result,
            ) as controller_mock:
                payload = supervise_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="warn",
                    timeout_seconds=8.0,
                    retry_budget=1,
                    retry_backoff_ms=40,
                    allow_side_effects=["destructive-write"],
                )

            controller_mock.assert_called_once_with(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                contract_mode="warn",
                timeout_seconds=8.0,
                retry_budget=1,
                retry_backoff_ms=40,
                allow_side_effects=["destructive-write"],
            )
            self.assertEqual(8.0, payload["supervisor"]["execution_policy"]["timeout_seconds"])
            self.assertEqual(1, payload["supervisor"]["execution_policy"]["retry_budget"])
            self.assertEqual(["destructive-write"], payload["supervisor"]["execution_policy"]["allow_side_effects"])
            self.assertEqual("reporting-ready", payload["supervisor"]["phase2_posture"])
            self.assertEqual("handoff-reporting", payload["supervisor"]["operator_action"])
            self.assertIn("resume-phase2-round", payload["supervisor"]["resume_command"])

    def test_supervisor_materializes_failed_state_when_controller_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.executor import SkillExecutionError
            from eco_council_runtime.kernel.supervisor import supervise_round_with_contract_mode

            controller_failure = SkillExecutionError(
                "phase-2 failed",
                {
                    "status": "failed",
                    "message": "phase-2 failed",
                    "controller": {
                        "planning_mode": "planner-backed",
                        "controller_status": "failed",
                        "resume_status": "fresh-run",
                        "current_stage": "board-brief",
                        "failed_stage": "board-brief",
                        "resume_recommended": True,
                        "restart_recommended": False,
                        "recovery": {"resume_from_stage": "board-brief"},
                        "readiness_status": "pending",
                        "gate_status": "not-evaluated",
                        "promotion_status": "not-evaluated",
                        "recommended_next_skills": ["eco-materialize-board-brief"],
                        "artifacts": {
                            "orchestration_plan_path": str(root / "plan.json"),
                            "controller_state_path": str(root / "controller.json"),
                            "promotion_gate_path": str(root / "gate.json"),
                        },
                    },
                },
            )

            with mock.patch(
                "eco_council_runtime.kernel.supervisor.run_phase2_round_with_contract_mode",
                side_effect=controller_failure,
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    supervise_round_with_contract_mode(run_dir, run_id=RUN_ID, round_id=ROUND_ID, contract_mode="warn")

            supervisor_artifact = load_json(run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json")
            self.assertEqual("controller-failed", supervisor_artifact["supervisor_status"])
            self.assertEqual("board-brief", supervisor_artifact["failed_stage"])
            self.assertTrue(supervisor_artifact["resume_recommended"])
            self.assertIn("resume-phase2-round", supervisor_artifact["resume_command"])
            self.assertEqual("controller-failed", raised.exception.payload["supervisor"]["supervisor_status"])

    def test_cli_run_skill_forwards_execution_policy_args(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with (
                mock.patch("eco_council_runtime.kernel.cli.init_run", return_value={"status": "completed"}),
                mock.patch(
                    "eco_council_runtime.kernel.cli.run_skill",
                    return_value={"status": "completed", "summary": {"skill_name": "eco-post-board-note"}},
                ) as run_skill_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "run-skill",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--skill-name",
                        "eco-post-board-note",
                        "--timeout-seconds",
                        "9",
                        "--retry-budget",
                        "2",
                        "--retry-backoff-ms",
                        "50",
                        "--allow-side-effect",
                        "network-external",
                        "--allow-side-effect",
                        "destructive-write",
                        "--",
                        "--author-role",
                        "moderator",
                    ]
                )

            self.assertEqual(0, exit_code)
            run_skill_mock.assert_called_once()
            self.assertEqual(9.0, run_skill_mock.call_args.kwargs["timeout_seconds"])
            self.assertEqual(2, run_skill_mock.call_args.kwargs["retry_budget"])
            self.assertEqual(50, run_skill_mock.call_args.kwargs["retry_backoff_ms"])
            self.assertEqual(["network-external", "destructive-write"], run_skill_mock.call_args.kwargs["allow_side_effects"])
            self.assertEqual(["--author-role", "moderator"], run_skill_mock.call_args.kwargs["skill_args"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

    def test_cli_resume_and_restart_phase2_round_forward_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.run_phase2_round_with_contract_mode",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as controller_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "resume-phase2-round",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--timeout-seconds",
                        "7",
                        "--retry-budget",
                        "1",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertFalse(controller_mock.call_args.kwargs["force_restart"])
            self.assertEqual(7.0, controller_mock.call_args.kwargs["timeout_seconds"])
            self.assertEqual(1, controller_mock.call_args.kwargs["retry_budget"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.run_phase2_round_with_contract_mode",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as controller_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "restart-phase2-round",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertTrue(controller_mock.call_args.kwargs["force_restart"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])


if __name__ == "__main__":
    unittest.main()

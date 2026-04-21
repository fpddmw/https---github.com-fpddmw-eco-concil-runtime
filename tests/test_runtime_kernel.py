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

from _workflow_support import analytics_path, kernel_script_path, load_json, run_kernel, run_kernel_process, run_script, runtime_src_path, script_path, seed_analysis_chain

RUN_ID = "run-kernel-001"
ROUND_ID = "round-kernel-001"


def ensure_runtime_src_on_path() -> None:
    runtime_src = runtime_src_path()
    if str(runtime_src) not in sys.path:
        sys.path.insert(0, str(runtime_src))


def default_phase2_gate_handlers() -> dict[str, object]:
    ensure_runtime_src_on_path()

    from eco_council_runtime.phase2_gate_profile import phase2_gate_handler_registry

    return phase2_gate_handler_registry()


def default_phase2_posture_profile_config() -> dict[str, object]:
    ensure_runtime_src_on_path()

    from eco_council_runtime.phase2_posture_profile import default_phase2_posture_profile

    return default_phase2_posture_profile()


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

    def test_kernel_lists_analysis_result_sets_via_non_python_query_surface(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            payload = run_kernel(
                "list-analysis-result-sets",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--analysis-kind",
                "claim-cluster",
                "--latest-only",
                "--include-contract",
            )

            self.assertEqual("analysis-plane-result-set-query-v1", payload["schema_version"])
            self.assertEqual(1, payload["summary"]["matching_result_set_count"])
            self.assertEqual(1, payload["summary"]["returned_result_set_count"])
            self.assertEqual(1, len(payload["result_sets"]))
            result_set = payload["result_sets"][0]
            self.assertEqual("claim-cluster", result_set["analysis_kind"])
            self.assertEqual("clusters", result_set["items_key"])
            self.assertGreaterEqual(result_set["item_count"], 1)
            self.assertTrue(result_set["artifact_present"])
            parent_kinds = {
                parent["analysis_kind"]
                for parent in result_set["result_contract"]["parent_result_sets"]
            }
            self.assertSetEqual({"claim-candidate"}, parent_kinds)

    def test_kernel_queries_analysis_items_when_cluster_artifact_is_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            cluster_id = outputs["cluster_claims"]["canonical_ids"][0]
            analytics_path(run_dir, f"claim_candidate_clusters_{ROUND_ID}.json").unlink()

            payload = run_kernel(
                "query-analysis-result-items",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--analysis-kind",
                "claim-cluster",
                "--latest-only",
                "--subject-id",
                cluster_id,
                "--include-result-sets",
                "--include-contract",
            )

            self.assertEqual("analysis-plane-item-query-v1", payload["schema_version"])
            self.assertEqual(1, payload["summary"]["matching_result_set_count"])
            self.assertEqual(1, payload["summary"]["returned_item_count"])
            self.assertEqual(1, len(payload["items"]))
            self.assertEqual(cluster_id, payload["items"][0]["subject_id"])
            self.assertFalse(payload["items"][0]["artifact_present"])
            self.assertEqual(1, len(payload["result_sets"]))
            self.assertFalse(payload["result_sets"][0]["artifact_present"])
            parent_kinds = {
                parent["analysis_kind"]
                for parent in payload["result_sets"][0]["result_contract"][
                    "parent_result_sets"
                ]
            }
            self.assertSetEqual({"claim-candidate"}, parent_kinds)

    def test_kernel_analysis_query_reports_invalid_analysis_kind(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            completed = run_kernel_process(
                "list-analysis-result-sets",
                "--run-dir",
                str(run_dir),
                "--analysis-kind",
                "not-a-real-kind",
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("failed", payload["status"])
            self.assertIn("Unsupported analysis kind", payload["message"])

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

    def test_run_skill_blocks_when_runtime_admission_rejects_execution_limits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.executor import SkillExecutionError, run_skill
            from eco_council_runtime.kernel.ledger import load_ledger_tail
            from eco_council_runtime.kernel.operations import load_dead_letters, materialize_admission_policy

            fake_skill_entry = {
                "skill_name": "eco-fake-admission-blocked",
                "script_path": str(root / "fake_blocked.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            materialize_admission_policy(run_dir, run_id=RUN_ID, max_timeout_seconds=1.0)

            with (
                mock.patch("eco_council_runtime.kernel.governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.executor.subprocess.run") as subprocess_run_mock,
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="eco-fake-admission-blocked",
                        skill_args=[],
                        contract_mode="warn",
                        timeout_seconds=2.5,
                    )

            payload = raised.exception.payload
            ledger_event = load_ledger_tail(run_dir, 1)[0]
            dead_letters = load_dead_letters(run_dir, round_id=ROUND_ID, limit=5)
            self.assertEqual("failed", payload["status"])
            self.assertTrue(payload["runtime_admission"]["block_execution"])
            self.assertEqual("timeout-exceeds-admission-limit", payload["failure"]["error_code"])
            self.assertEqual("blocked", ledger_event["status"])
            self.assertEqual("skill-admission", ledger_event["event_type"])
            self.assertTrue(payload["dead_letter"]["dead_letter_id"].startswith("deadletter-"))
            self.assertEqual(payload["dead_letter"]["dead_letter_id"], ledger_event["dead_letter_id"])
            self.assertEqual(1, len(dead_letters))
            subprocess_run_mock.assert_not_called()

    def test_show_run_state_surfaces_operations_control_plane(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import init_run, show_run_state
            from eco_council_runtime.kernel.operations import materialize_dead_letter
            from eco_council_runtime.phase2_agent_entry_profile import (
                default_phase2_agent_entry_profile,
            )

            init_run(run_dir, RUN_ID)
            materialize_dead_letter(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                source_type="skill-execution",
                source_name="eco-test-skill",
                message="Synthetic runtime failure for operator surface coverage.",
                failure={"error_code": "skill-timeout", "message": "timed out", "retryable": False},
                summary={"skill_name": "eco-test-skill", "run_id": RUN_ID, "round_id": ROUND_ID},
            )

            payload = show_run_state(
                run_dir,
                tail=5,
                round_id=ROUND_ID,
                agent_entry_profile=default_phase2_agent_entry_profile(),
            )

            self.assertIn("operations", payload)
            self.assertEqual("red", payload["operations"]["runtime_health"]["alert_status"])
            self.assertEqual(1, payload["summary"]["open_dead_letter_count"])
            self.assertTrue(payload["operations"]["operator"]["admission_policy_path"].endswith("admission_policy.json"))
            self.assertTrue(payload["operations"]["operator"]["operator_runbook_path"].endswith(f"operator_runbook_{ROUND_ID}.md"))
            self.assertEqual("eco-test-skill", payload["operations"]["dead_letters"][0]["source_name"])

    def test_default_admission_policy_keeps_writes_inside_run_surface(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.operations import default_admission_policy

            policy = default_admission_policy(run_dir, run_id=RUN_ID)
            sandbox = policy["sandbox_boundary"]

            self.assertEqual(
                ["<run_dir>", "<run_parent>/archives", "<workspace_root>"],
                sandbox["allowed_read_roots"],
            )
            self.assertEqual(
                ["<run_dir>", "<run_parent>/archives"],
                sandbox["allowed_write_roots"],
            )
            self.assertEqual(
                ["<workspace_root>", "<run_dir>"],
                sandbox["allowed_cwd_roots"],
            )

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
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
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
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
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

    def test_controller_uses_plan_declared_gate_steps_instead_of_injecting_default_gate_path(self) -> None:
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
            readiness_result = {
                "summary": {"skill_name": "eco-summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "custom_readiness.json"), "readiness_status": "ready"},
                },
            }
            promotion_result = {
                "summary": {"skill_name": "eco-promote-evidence-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "custom_basis.json"), "promotion_status": "promoted"},
                },
            }
            planning = {
                "plan_id": "plan-custom-gate-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "eco-plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": [],
                "execution_queue": [
                    {
                        "stage_name": "round-readiness",
                        "stage_kind": "skill",
                        "phase_group": "execution",
                        "skill_name": "eco-summarize-round-readiness",
                        "expected_skill_name": "eco-summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "required_previous_stages": ["orchestration-planner"],
                        "blocking": True,
                        "resume_policy": "skip-if-completed",
                        "operator_summary": "Custom readiness stage declared by plan payload.",
                        "reason": "test",
                        "expected_output_path": str(root / "custom_readiness.json"),
                    }
                ],
                "gate_steps": [
                    {
                        "stage_name": "final-promotion-review",
                        "stage_kind": "gate",
                        "phase_group": "gate",
                        "required_previous_stages": ["round-readiness"],
                        "blocking": True,
                        "resume_policy": "skip-if-completed",
                        "operator_summary": "Custom gate step declared by plan payload.",
                        "reason": "test",
                        "expected_output_path": str(root / "custom_gate.json"),
                        "gate_handler": "promotion-gate",
                        "readiness_stage_name": "round-readiness",
                    }
                ],
                "post_gate_steps": [
                    {
                        "stage_name": "promotion-basis",
                        "stage_kind": "skill",
                        "phase_group": "promotion",
                        "skill_name": "eco-promote-evidence-basis",
                        "expected_skill_name": "eco-promote-evidence-basis",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "required_previous_stages": ["final-promotion-review"],
                        "blocking": True,
                        "resume_policy": "skip-if-completed",
                        "operator_summary": "Custom promotion basis stage declared by plan payload.",
                        "reason": "test",
                        "expected_output_path": str(root / "custom_basis.json"),
                    }
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": [],
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "allow-promote",
                "readiness_status": "ready",
                "promote_allowed": True,
                "output_path": str(root / "custom_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle", return_value=planning),
                mock.patch(
                    "eco_council_runtime.phase2_gate_handlers.apply_promotion_gate",
                    return_value=gate_payload,
                ) as gate_mock,
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[planner_result, readiness_result, promotion_result],
                ),
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                )

            self.assertEqual(
                str(root / "custom_readiness.json"),
                gate_mock.call_args.kwargs["readiness_path_override"],
            )
            self.assertEqual(
                str(root / "custom_gate.json"),
                gate_mock.call_args.kwargs["output_path_override"],
            )
            self.assertEqual(
                ["orchestration-planner", "round-readiness", "final-promotion-review", "promotion-basis"],
                payload["controller"]["planning"]["stage_sequence"],
            )
            self.assertEqual(
                str(root / "custom_gate.json"),
                payload["controller"]["stage_contracts"]["final-promotion-review"]["expected_output_path"],
            )
            self.assertEqual(
                str(root / "custom_gate.json"),
                payload["controller"]["steps"][2]["artifact_path"],
            )
            self.assertEqual(1, payload["controller"]["planning"]["gate_step_count"])

    def test_gate_runtime_dispatches_custom_handler_registry(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.gate import execute_gate_step

        run_dir = Path("/tmp/runtime-gate-registry")
        handler = mock.Mock(
            return_value={
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "output_path": "/tmp/custom_gate.json",
                "controller_updates": {
                    "readiness_status": "custom-ready",
                    "gate_status": "custom-approved",
                    "gate_reasons": ["registry-dispatched"],
                    "recommended_next_skills": ["eco-custom-follow-up"],
                },
            }
        )

        result = execute_gate_step(
            run_dir,
            run_id=RUN_ID,
            round_id=ROUND_ID,
            blueprint={
                "stage_name": "custom-gate-review",
                "stage_kind": "gate",
                "gate_handler": "custom-gate",
                "required_previous_stages": ["custom-readiness-review"],
                "expected_output_path": "/tmp/custom_gate.json",
            },
            stage_contracts={
                "custom-readiness-review": {
                    "expected_output_path": "/tmp/custom_readiness.json"
                }
            },
            gate_handlers={"custom-gate": handler},
        )

        handler.assert_called_once_with(
            run_dir,
            run_id=RUN_ID,
            round_id=ROUND_ID,
            readiness_path_override="/tmp/custom_readiness.json",
            output_path_override="/tmp/custom_gate.json",
        )
        self.assertEqual("custom-gate", result["gate_handler"])
        self.assertEqual("custom-readiness-review", result["readiness_stage_name"])
        self.assertEqual("custom-approved", result["controller_updates"]["gate_status"])
        self.assertEqual(
            ["eco-custom-follow-up"],
            result["controller_updates"]["recommended_next_skills"],
        )

    def test_controller_prefers_agent_advisory_plan_over_runtime_planner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode

            advisory_plan_path = runtime_dir / f"agent_advisory_plan_{ROUND_ID}.json"
            advisory_plan_path.write_text(
                json.dumps(
                    {
                        "plan_id": "agent-plan-001",
                        "planning_status": "advisory-plan-ready",
                        "planning_mode": "agent-advisory",
                        "controller_authority": "advisory-only",
                        "execution_queue": [
                            {
                                "stage_name": "round-readiness",
                                "skill_name": "eco-summarize-round-readiness",
                                "skill_args": [],
                                "assigned_role_hint": "moderator",
                                "reason": "Agent judged the board ready for direct readiness review.",
                                "expected_output_path": str(root / "readiness.json"),
                            }
                        ],
                        "post_gate_steps": [
                            {
                                "stage_name": "promotion-basis",
                                "skill_name": "eco-promote-evidence-basis",
                                "skill_args": [],
                                "assigned_role_hint": "moderator",
                                "reason": "Freeze the agent-selected gate outcome.",
                                "expected_output_path": str(root / "basis.json"),
                            }
                        ],
                        "fallback_path": [],
                    },
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

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
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "promotion_status": "promoted"},
                },
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

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[readiness_result, promotion_result],
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                )

            self.assertEqual(
                ["eco-summarize-round-readiness", "eco-promote-evidence-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("agent-advisory", payload["controller"]["planning_mode"])
            self.assertEqual("agent-advisory", payload["controller"]["planning"]["planning_mode"])
            self.assertEqual("advisory-only", payload["controller"]["planning"]["controller_authority"])
            self.assertEqual("agent-advisory", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(str(advisory_plan_path.resolve()), payload["controller"]["planning"]["plan_path"])
            self.assertEqual(
                ["orchestration-planner", "round-readiness", "promotion-gate", "promotion-basis"],
                payload["controller"]["completed_stage_names"],
            )
            self.assertEqual(
                str(advisory_plan_path.resolve()),
                payload["controller"]["steps"][0]["artifact_path"],
            )

    def test_controller_materializes_agent_advisory_plan_for_openclaw_agent_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode

            advisory_plan_path = runtime_dir / f"agent_advisory_plan_{ROUND_ID}.json"
            (runtime_dir / f"mission_scaffold_{ROUND_ID}.json").write_text(
                json.dumps(
                    {
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "orchestration_mode": "openclaw-agent",
                    },
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

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
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "promotion_status": "promoted"},
                },
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

            def run_skill_side_effect(*args: object, **kwargs: object) -> dict[str, object]:
                skill_name = kwargs["skill_name"]
                if skill_name == "eco-plan-round-orchestration":
                    self.assertEqual(
                        ["--planner-mode", "agent-advisory", "--output-path", f"runtime/agent_advisory_plan_{ROUND_ID}.json"],
                        kwargs["skill_args"],
                    )
                    advisory_plan_path.write_text(
                        json.dumps(
                            {
                                "plan_id": "agent-plan-materialized-001",
                                "planning_status": "advisory-plan-ready",
                                "planning_mode": "agent-advisory",
                                "controller_authority": "advisory-only",
                                "execution_queue": [
                                    {
                                        "stage_name": "round-readiness",
                                        "skill_name": "eco-summarize-round-readiness",
                                        "skill_args": [],
                                        "assigned_role_hint": "moderator",
                                        "reason": "Agent route goes directly to readiness review.",
                                        "expected_output_path": str(root / "readiness.json"),
                                    }
                                ],
                                "post_gate_steps": [
                                    {
                                        "stage_name": "promotion-basis",
                                        "skill_name": "eco-promote-evidence-basis",
                                        "skill_args": [],
                                        "assigned_role_hint": "moderator",
                                        "reason": "Freeze the agent-selected readiness outcome.",
                                        "expected_output_path": str(root / "basis.json"),
                                    }
                                ],
                                "fallback_path": [],
                            },
                            ensure_ascii=True,
                            indent=2,
                            sort_keys=True,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    return {
                        "summary": {
                            "skill_name": "eco-plan-round-orchestration",
                            "event_id": "evt-agent-plan",
                            "receipt_id": "receipt-agent-plan",
                        },
                        "event": {"status": "completed"},
                        "skill_payload": {
                            "artifact_refs": [],
                            "canonical_ids": ["agent-plan-materialized-001"],
                            "summary": {"output_path": str(advisory_plan_path.resolve())},
                        },
                    }
                if skill_name == "eco-summarize-round-readiness":
                    return readiness_result
                if skill_name == "eco-promote-evidence-basis":
                    return promotion_result
                raise AssertionError(f"Unexpected skill execution: {skill_name}")

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=run_skill_side_effect,
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                )

            self.assertEqual(
                ["eco-plan-round-orchestration", "eco-summarize-round-readiness", "eco-promote-evidence-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("agent-advisory", payload["summary"]["plan_source"])
            self.assertEqual("agent-advisory", payload["controller"]["planning_mode"])
            self.assertEqual("agent-advisory", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(str(advisory_plan_path.resolve()), payload["controller"]["planning"]["plan_path"])
            self.assertEqual(str(advisory_plan_path.resolve()), payload["controller"]["artifacts"]["agent_advisory_plan_path"])
            self.assertEqual("receipt-agent-plan", payload["controller"]["steps"][0]["receipt_id"])
            self.assertEqual("agent-advisory", payload["controller"]["steps"][0]["plan_source"])
            self.assertEqual(
                [
                    {
                        "source": "direct-council-advisory",
                        "status": "unavailable",
                    },
                    {
                        "source": "agent-advisory",
                        "status": "materialized",
                    }
                ],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )

    def test_controller_uses_direct_council_advisory_without_planner_skill(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            ensure_runtime_src_on_path()

            from eco_council_runtime.council_objects import store_readiness_opinion_records
            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode

            advisory_plan_path = runtime_dir / f"agent_advisory_plan_{ROUND_ID}.json"
            (runtime_dir / f"mission_scaffold_{ROUND_ID}.json").write_text(
                json.dumps(
                    {
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "orchestration_mode": "openclaw-agent",
                    },
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
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
                            "sufficient_for_promotion": True,
                            "rationale": "The council is ready to send this round to promotion review.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

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
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "promotion_status": "promoted"},
                },
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

            def run_skill_side_effect(*args: object, **kwargs: object) -> dict[str, object]:
                skill_name = kwargs["skill_name"]
                if skill_name == "eco-plan-round-orchestration":
                    raise AssertionError("planner skill should not run when direct council advisory is available")
                if skill_name == "eco-summarize-round-readiness":
                    return readiness_result
                if skill_name == "eco-promote-evidence-basis":
                    return promotion_result
                raise AssertionError(f"Unexpected skill execution: {skill_name}")

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=run_skill_side_effect,
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                )

            advisory_plan = load_json(advisory_plan_path)
            self.assertEqual(
                ["eco-summarize-round-readiness", "eco-promote-evidence-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("direct-council-advisory", payload["summary"]["plan_source"])
            self.assertEqual("agent-advisory", payload["controller"]["planning_mode"])
            self.assertEqual("direct-council-advisory", payload["controller"]["planning"]["plan_source"])
            self.assertEqual("direct-council-advisory", payload["controller"]["steps"][0]["plan_source"])
            self.assertEqual("direct-council-advisory", advisory_plan["plan_source"])
            self.assertEqual(str(advisory_plan_path.resolve()), payload["controller"]["planning"]["plan_path"])
            self.assertEqual(
                [{"source": "direct-council-advisory", "status": "materialized"}],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )

    def test_controller_falls_back_to_runtime_planner_after_agent_advisory_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode
            from eco_council_runtime.kernel.executor import SkillExecutionError

            (runtime_dir / f"mission_scaffold_{ROUND_ID}.json").write_text(
                json.dumps(
                    {
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "orchestration_mode": "openclaw-agent",
                    },
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            planner_result = {
                "summary": {"skill_name": "eco-plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            planning = {
                "plan_id": "plan-runtime-fallback-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "eco-plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": [],
                "execution_queue": [
                    {
                        "stage_name": "round-readiness",
                        "skill_name": "eco-summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Planner fallback sends the round to readiness review.",
                        "expected_output_path": str(root / "readiness.json"),
                    }
                ],
                "post_gate_steps": [
                    {
                        "stage_name": "promotion-basis",
                        "skill_name": "eco-promote-evidence-basis",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Freeze the fallback planner result.",
                        "expected_output_path": str(root / "basis.json"),
                    }
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": [],
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
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "promotion_status": "promoted"},
                },
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

            def run_skill_side_effect(*args: object, **kwargs: object) -> dict[str, object]:
                skill_name = kwargs["skill_name"]
                if skill_name == "eco-plan-round-orchestration":
                    if kwargs["skill_args"]:
                        self.assertEqual(
                            ["--planner-mode", "agent-advisory", "--output-path", f"runtime/agent_advisory_plan_{ROUND_ID}.json"],
                            kwargs["skill_args"],
                        )
                        raise SkillExecutionError(
                            "advisory planner failed",
                            {
                                "message": "advisory planner failed",
                                "failure": {"retryable": False},
                            },
                        )
                    return planner_result
                if skill_name == "eco-summarize-round-readiness":
                    return readiness_result
                if skill_name == "eco-promote-evidence-basis":
                    return promotion_result
                raise AssertionError(f"Unexpected skill execution: {skill_name}")

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=run_skill_side_effect,
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                )

            self.assertEqual(
                [
                    ("eco-plan-round-orchestration", ["--planner-mode", "agent-advisory", "--output-path", f"runtime/agent_advisory_plan_{ROUND_ID}.json"]),
                    ("eco-plan-round-orchestration", []),
                    ("eco-summarize-round-readiness", []),
                    ("eco-promote-evidence-basis", []),
                ],
                [(call.kwargs["skill_name"], call.kwargs["skill_args"]) for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("planner-backed", payload["controller"]["planning_mode"])
            self.assertEqual("runtime-planner", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(
                [
                    {"source": "direct-council-advisory", "status": "unavailable"},
                    {"source": "agent-advisory", "status": "failed"},
                    {"source": "runtime-planner", "status": "materialized"},
                ],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )
            self.assertEqual("receipt-plan", payload["controller"]["steps"][0]["receipt_id"])
            self.assertEqual("runtime-planner", payload["controller"]["steps"][0]["plan_source"])

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
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[planner_result, board_summary_result, board_brief_failure],
                ),
            ):
                with self.assertRaises(SkillExecutionError):
                    run_phase2_round_with_contract_mode(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        contract_mode="warn",
                        gate_handlers=default_phase2_gate_handlers(),
                        posture_profile=default_phase2_posture_profile_config(),
                    )

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
            (run_dir / "runtime" / f"round_controller_{ROUND_ID}.json").unlink()

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle") as planning_bundle_mock,
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[board_brief_result, next_actions_result, readiness_result, promotion_result],
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="warn",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                )

            planning_bundle_mock.assert_not_called()
            self.assertEqual(
                ["eco-materialize-board-brief", "eco-propose-next-actions", "eco-summarize-round-readiness", "eco-promote-evidence-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("completed", payload["controller"]["controller_status"])
            self.assertEqual("resumed", payload["controller"]["resume_status"])
            self.assertEqual("promoted", payload["controller"]["promotion_status"])
            self.assertFalse(payload["controller"]["resume_recommended"])

    def test_controller_respects_injected_planning_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.controller import run_phase2_round_with_contract_mode
            from eco_council_runtime.phase2_planning_profile import phase2_planning_source

            advisory_plan_path = runtime_dir / f"agent_advisory_plan_{ROUND_ID}.json"
            advisory_plan_path.write_text(
                json.dumps(
                    {
                        "plan_id": "agent-plan-should-not-run",
                        "planning_status": "advisory-plan-ready",
                        "planning_mode": "agent-advisory",
                        "controller_authority": "advisory-only",
                        "execution_queue": [
                            {
                                "stage_name": "round-readiness",
                                "skill_name": "eco-summarize-round-readiness",
                                "skill_args": [],
                                "assigned_role_hint": "moderator",
                                "reason": "This advisory plan should be ignored by injected planning sources.",
                                "expected_output_path": str(root / "ignored_readiness.json"),
                            }
                        ],
                        "post_gate_steps": [
                            {
                                "stage_name": "promotion-basis",
                                "skill_name": "eco-promote-evidence-basis",
                                "skill_args": [],
                                "assigned_role_hint": "moderator",
                                "reason": "Ignore this advisory post-gate path.",
                                "expected_output_path": str(root / "ignored_basis.json"),
                            }
                        ],
                        "fallback_path": [],
                    },
                    ensure_ascii=True,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            planner_result = {
                "summary": {"skill_name": "eco-plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            planning = {
                "plan_id": "plan-injected-runtime-only-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "eco-plan-round-orchestration",
                "execution_queue": [
                    {
                        "stage_name": "round-readiness",
                        "skill_name": "eco-summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Injected planning sources force runtime planner.",
                        "expected_output_path": str(root / "readiness.json"),
                    }
                ],
                "post_gate_steps": [
                    {
                        "stage_name": "promotion-basis",
                        "skill_name": "eco-promote-evidence-basis",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Freeze the runtime-only planner result.",
                        "expected_output_path": str(root / "basis.json"),
                    }
                ],
                "fallback_path": [],
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
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "promotion_status": "promoted"},
                },
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
            runtime_only_sources = [
                phase2_planning_source(
                    "runtime-planner-only",
                    source_kind="planner-skill",
                    output_path_key="orchestration_plan_path",
                    planner_skill_name="eco-plan-round-orchestration",
                    materialized_message="Use only the injected runtime planner path.",
                    failed_message="Injected runtime planner path failed.",
                )
            ]

            with (
                mock.patch("eco_council_runtime.kernel.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.phase2_gate_handlers.apply_promotion_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.controller.run_skill",
                    side_effect=[planner_result, readiness_result, promotion_result],
                ) as run_skill_mock,
            ):
                payload = run_phase2_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_phase2_gate_handlers(),
                    posture_profile=default_phase2_posture_profile_config(),
                    planning_sources=runtime_only_sources,
                )

            self.assertEqual(
                ["eco-plan-round-orchestration", "eco-summarize-round-readiness", "eco-promote-evidence-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual([], run_skill_mock.call_args_list[0].kwargs["skill_args"])
            self.assertEqual("runtime-planner", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(
                [{"source": "runtime-planner-only", "status": "materialized"}],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )

    def test_show_run_state_uses_deliberation_control_snapshots_when_phase2_json_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.deliberation_plane import store_promotion_freeze_record

            controller_path = run_dir / "runtime" / f"round_controller_{ROUND_ID}.json"
            gate_path = run_dir / "runtime" / f"promotion_gate_{ROUND_ID}.json"
            supervisor_path = run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json"
            controller_snapshot = {
                "schema_version": "runtime-controller-v3",
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "controller_status": "completed",
                "planning_mode": "planner-backed",
                "readiness_status": "ready",
                "gate_status": "allow-promote",
                "promotion_status": "promoted",
                "resume_status": "fresh-run",
                "current_stage": "",
                "failed_stage": "",
                "completed_stage_names": ["orchestration-planner", "next-actions", "round-readiness", "promotion-gate", "promotion-basis"],
                "pending_stage_names": [],
                "resume_recommended": False,
                "restart_recommended": False,
                "recovery": {"resume_from_stage": ""},
                "gate_reasons": [],
                "recommended_next_skills": ["eco-materialize-reporting-handoff"],
                "planning": {"plan_path": str((run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve())},
                "steps": [],
                "artifacts": {
                    "controller_state_path": str(controller_path.resolve()),
                    "promotion_gate_path": str(gate_path.resolve()),
                    "orchestration_plan_path": str((run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve()),
                },
            }
            gate_snapshot = {
                "schema_version": "runtime-gate-v1",
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "readiness_path": str((run_dir / "reporting" / f"round_readiness_{ROUND_ID}.json").resolve()),
                "readiness_status": "ready",
                "promote_allowed": True,
                "gate_status": "allow-promote",
                "gate_reasons": [],
                "recommended_next_skills": [],
                "output_path": str(gate_path.resolve()),
            }
            supervisor_snapshot = {
                "schema_version": "runtime-supervisor-v3",
                "generated_at_utc": "2024-01-01T00:05:00Z",
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "supervisor_path": str(supervisor_path.resolve()),
                "supervisor_status": "reporting-ready",
                "supervisor_substatus": "promotion-complete",
                "phase2_posture": "reporting-ready",
                "terminal_state": "reporting-ready",
                "recovery_posture": "terminal",
                "operator_action": "handoff-reporting",
                "controller_status": "completed",
                "resume_status": "fresh-run",
                "current_stage": "",
                "failed_stage": "",
                "resume_recommended": False,
                "restart_recommended": False,
                "resume_from_stage": "",
                "readiness_status": "ready",
                "gate_status": "allow-promote",
                "promotion_status": "promoted",
                "planning_mode": "planner-backed",
                "promotion_gate_path": str(gate_path.resolve()),
                "controller_path": str(controller_path.resolve()),
                "recommended_next_skills": ["eco-materialize-reporting-handoff"],
                "round_transition": {},
                "operator_notes": ["Round promotion succeeded and the evidence basis is now ready for downstream reporting."],
                "inspection_paths": {
                    "controller_path": str(controller_path.resolve()),
                    "plan_path": str((run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve()),
                    "gate_path": str(gate_path.resolve()),
                    "promotion_basis_path": str((run_dir / "promotion" / f"promoted_evidence_basis_{ROUND_ID}.json").resolve()),
                },
            }

            store_promotion_freeze_record(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                controller_snapshot=controller_snapshot,
                gate_snapshot=gate_snapshot,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths={
                    "controller_state_path": str(controller_path.resolve()),
                    "promotion_gate_path": str(gate_path.resolve()),
                    "supervisor_state_path": str(supervisor_path.resolve()),
                },
            )

            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--tail",
                "5",
            )

            self.assertEqual("completed", state_payload["phase2"]["operator"]["controller_status"])
            self.assertEqual("reporting-ready", state_payload["phase2"]["operator"]["supervisor_status"])
            self.assertEqual("allow-promote", state_payload["phase2"]["operator"]["gate_status"])
            self.assertEqual("promoted", state_payload["phase2"]["operator"]["promotion_status"])
            self.assertTrue(state_payload["phase2"]["operator"]["reporting_ready"])
            self.assertEqual(
                "reporting-ready",
                state_payload["phase2"]["operator"]["reporting_handoff_status"],
            )
            self.assertIn(
                "show-reporting-state",
                state_payload["phase2"]["operator"]["show_reporting_state_command"],
            )
            self.assertIn(
                "--readiness-blocker-only",
                state_payload["phase2"]["operator"]["query_readiness_blockers_command"],
            )
            self.assertTrue(state_payload["reporting"]["surface"]["reporting_ready"])
            self.assertEqual(
                "supervisor",
                state_payload["reporting"]["surface"]["surface_source"],
            )
            self.assertEqual(str(controller_path.resolve()), state_payload["phase2"]["operator"]["inspection_paths"]["controller_path"])
            self.assertEqual(str(supervisor_path.resolve()), state_payload["phase2"]["operator"]["inspection_paths"]["supervisor_path"])

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
                    posture_profile=default_phase2_posture_profile_config(),
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
                gate_handlers=mock.ANY,
                posture_profile=mock.ANY,
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

    def test_supervisor_respects_injected_posture_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.supervisor import supervise_round_with_contract_mode

            posture_profile = default_phase2_posture_profile_config()
            posture_profile["supervisor_classification_builder"] = (
                lambda controller: {
                    "supervisor_status": "custom-supervisor-status",
                    "supervisor_substatus": "custom-substatus",
                    "phase2_posture": "custom-posture",
                    "terminal_state": "custom-terminal-state",
                    "recovery_posture": "custom-recovery-posture",
                    "operator_action": "custom-operator-action",
                }
            )
            posture_profile["supervisor_next_round_id_builder"] = (
                lambda **kwargs: f"{kwargs['current_round_id']}-custom"
            )
            posture_profile["supervisor_top_actions_builder"] = (
                lambda next_actions: [
                    {
                        "action_id": "action-custom-001",
                        "action_kind": "custom-action-kind",
                        "assigned_role": "moderator",
                        "priority": "critical",
                        "objective": "Follow the injected posture profile.",
                    }
                ]
            )
            posture_profile["supervisor_round_transition_builder"] = (
                lambda **kwargs: {
                    "skill_name": "eco-custom-round-transition",
                    "source_round_id": kwargs["round_id"],
                    "suggested_round_id": f"{kwargs['round_id']}-custom",
                    "command": "eco-custom-round-transition --injected",
                }
            )
            posture_profile["supervisor_recommended_skills_builder"] = (
                lambda **kwargs: [
                    "eco-custom-round-transition",
                    "eco-custom-follow-up",
                ]
            )
            posture_profile["supervisor_operator_notes_builder"] = (
                lambda **kwargs: [
                    "Injected posture profile decided the next operator move.",
                ]
            )
            posture_profile["supervisor_failure_notes_builder"] = (
                lambda controller: [
                    "Injected failure note.",
                ]
            )

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
                    "readiness_status": "blocked",
                    "gate_status": "freeze-withheld",
                    "promotion_status": "withheld",
                    "recommended_next_skills": ["eco-kernel-default-follow-up"],
                    "gate_reasons": ["This should be ignored by the injected operator notes builder."],
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
                    posture_profile=posture_profile,
                )

            controller_mock.assert_called_once_with(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                contract_mode="warn",
                gate_handlers=None,
                posture_profile=posture_profile,
                timeout_seconds=None,
                retry_budget=None,
                retry_backoff_ms=None,
                allow_side_effects=None,
            )
            self.assertEqual(
                "custom-supervisor-status",
                payload["supervisor"]["supervisor_status"],
            )
            self.assertEqual(
                "custom-operator-action",
                payload["supervisor"]["operator_action"],
            )
            self.assertEqual(
                ["eco-custom-round-transition", "eco-custom-follow-up"],
                payload["supervisor"]["recommended_next_skills"],
            )
            self.assertEqual(
                "eco-custom-round-transition",
                payload["supervisor"]["round_transition"]["skill_name"],
            )
            self.assertEqual(
                f"{ROUND_ID}-custom",
                payload["supervisor"]["round_transition"]["suggested_round_id"],
            )
            self.assertEqual(
                ["Injected posture profile decided the next operator move."],
                payload["supervisor"]["operator_notes"],
            )
            self.assertEqual(
                "action-custom-001",
                payload["supervisor"]["top_actions"][0]["action_id"],
            )

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
                    supervise_round_with_contract_mode(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        contract_mode="warn",
                        posture_profile=default_phase2_posture_profile_config(),
                    )

            supervisor_artifact = load_json(run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json")
            self.assertEqual("controller-failed", supervisor_artifact["supervisor_status"])
            self.assertEqual("board-brief", supervisor_artifact["failed_stage"])
            self.assertTrue(supervisor_artifact["resume_recommended"])
            self.assertIn("resume-phase2-round", supervisor_artifact["resume_command"])
            self.assertEqual("controller-failed", raised.exception.payload["supervisor"]["supervisor_status"])

    def test_close_round_blocks_on_archive_failure_by_default_and_persists_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.executor import SkillExecutionError
            from eco_council_runtime.kernel.deliberation_plane import (
                store_promotion_freeze_record,
            )
            from eco_council_runtime.kernel.post_round import close_round_with_contract_mode

            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            supervisor_path = runtime_dir / f"supervisor_state_{ROUND_ID}.json"
            supervisor_snapshot = {
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "supervisor_status": "reporting-ready",
                "readiness_status": "ready",
                "promotion_status": "promoted",
                "reporting_ready": True,
                "reporting_blockers": [],
                "reporting_handoff_status": "reporting-ready",
                "supervisor_path": str(supervisor_path.resolve()),
            }
            supervisor_path.write_text(
                json.dumps(supervisor_snapshot, ensure_ascii=True),
                encoding="utf-8",
            )
            store_promotion_freeze_record(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths={
                    "supervisor_state_path": str(supervisor_path.resolve()),
                },
            )

            signal_archive_result = {
                "summary": {"skill_name": "eco-archive-signal-corpus", "event_id": "evt-archive-signal", "receipt_id": "receipt-archive-signal"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "signal_archive.json")}},
            }
            case_archive_failure = SkillExecutionError(
                "case archive failed",
                {
                    "status": "failed",
                    "message": "case archive failed",
                    "summary": {"skill_name": "eco-archive-case-library", "run_id": RUN_ID, "round_id": ROUND_ID},
                    "failure": {"error_code": "skill-exit-nonzero", "retryable": False},
                },
            )

            with (
                mock.patch("eco_council_runtime.kernel.post_round.write_registry"),
                mock.patch(
                    "eco_council_runtime.kernel.post_round.run_skill",
                    side_effect=[signal_archive_result, case_archive_failure],
                ),
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    close_round_with_contract_mode(run_dir, run_id=RUN_ID, round_id=ROUND_ID, contract_mode="warn")

            close_artifact = load_json(run_dir / "runtime" / f"round_close_{ROUND_ID}.json")
            self.assertEqual("failed", close_artifact["close_status"])
            self.assertEqual("failed", close_artifact["archive_status"])
            self.assertEqual("archive-case-library", close_artifact["failed_stage"])
            self.assertEqual("block", close_artifact["archive_failure_policy"])
            self.assertEqual("failed", raised.exception.payload["round_close"]["close_status"])

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

    def test_cli_close_round_and_history_bootstrap_forward_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.close_round_with_contract_mode",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as close_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "close-round",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--archive-failure-policy",
                        "warn",
                        "--timeout-seconds",
                        "6",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("warn", close_mock.call_args.kwargs["archive_failure_policy"])
            self.assertEqual(6.0, close_mock.call_args.kwargs["timeout_seconds"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.bootstrap_history_context_with_contract_mode",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as history_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "bootstrap-history-context",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--retry-budget",
                        "2",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(2, history_mock.call_args.kwargs["retry_budget"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

    def test_cli_benchmark_commands_forward_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.materialize_scenario_fixture",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as fixture_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "materialize-scenario-fixture",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--scenario-id",
                        "scenario-fixed-001",
                        "--baseline-manifest-path",
                        str(root / "baseline.json"),
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("scenario-fixed-001", fixture_mock.call_args.kwargs["scenario_id"])
            self.assertEqual(str(root / "baseline.json"), fixture_mock.call_args.kwargs["baseline_manifest_override"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.materialize_benchmark_manifest",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as manifest_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "materialize-benchmark-manifest",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                    ]
                )

            self.assertEqual(0, exit_code)
            manifest_mock.assert_called_once()
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.compare_benchmark_manifests",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as compare_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "compare-benchmark-manifests",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--left-manifest-path",
                        str(root / "left.json"),
                        "--right-manifest-path",
                        str(root / "right.json"),
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(str(root / "left.json"), compare_mock.call_args.kwargs["left_manifest_path"])
            self.assertEqual(str(root / "right.json"), compare_mock.call_args.kwargs["right_manifest_path"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.replay_runtime_scenario",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as replay_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "replay-runtime-scenario",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--fixture-path",
                        str(root / "fixture.json"),
                        "--baseline-manifest-path",
                        str(root / "baseline.json"),
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(str(root / "fixture.json"), replay_mock.call_args.kwargs["fixture_path_override"])
            self.assertEqual(str(root / "baseline.json"), replay_mock.call_args.kwargs["baseline_manifest_override"])
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
                    ],
                    default_posture_profile=default_phase2_posture_profile_config(),
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
                    ],
                    default_posture_profile=default_phase2_posture_profile_config(),
                )

            self.assertEqual(0, exit_code)
            self.assertTrue(controller_mock.call_args.kwargs["force_restart"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

    def test_cli_operations_commands_forward_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.materialize_admission_policy",
                    return_value={"schema_version": "runtime-admission-policy-v1", "permission_profile": "restricted"},
                ) as policy_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "materialize-admission-policy",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--permission-profile",
                        "restricted",
                        "--max-timeout-seconds",
                        "12",
                        "--approval-required-side-effect",
                        "network-external",
                        "--allowed-write-root",
                        "<run_dir>",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("restricted", policy_mock.call_args.kwargs["permission_profile"])
            self.assertEqual(12.0, policy_mock.call_args.kwargs["max_timeout_seconds"])
            self.assertEqual(["network-external"], policy_mock.call_args.kwargs["approval_required_side_effects"])
            self.assertEqual(["<run_dir>"], policy_mock.call_args.kwargs["allowed_write_roots"])
            self.assertEqual("runtime-admission-policy-v1", json.loads(stdout.getvalue())["schema_version"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.materialize_runtime_health",
                    return_value={"schema_version": "runtime-health-v1", "alert_status": "green"},
                ) as health_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "materialize-runtime-health",
                        "--run-dir",
                        str(run_dir),
                        "--round-id",
                        ROUND_ID,
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(ROUND_ID, health_mock.call_args.kwargs["round_id"])
            self.assertEqual("runtime-health-v1", json.loads(stdout.getvalue())["schema_version"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.materialize_operator_runbook",
                    return_value=str(run_dir / "runtime" / "operator_runbook.md"),
                ) as runbook_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "materialize-operator-runbook",
                        "--run-dir",
                        str(run_dir),
                        "--round-id",
                        ROUND_ID,
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(ROUND_ID, runbook_mock.call_args.kwargs["round_id"])
            self.assertTrue(json.loads(stdout.getvalue())["operator_runbook_path"].endswith("operator_runbook.md"))

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.load_dead_letters",
                    return_value=[{"dead_letter_id": "deadletter-1234567890abcdef1234"}],
                ) as dead_letters_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "show-dead-letters",
                        "--run-dir",
                        str(run_dir),
                        "--round-id",
                        ROUND_ID,
                        "--limit",
                        "5",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(1, dead_letters_mock.call_count)
            self.assertEqual(5, dead_letters_mock.call_args.kwargs["limit"])
            self.assertEqual(1, json.loads(stdout.getvalue())["summary"]["dead_letter_count"])


if __name__ == "__main__":
    unittest.main()

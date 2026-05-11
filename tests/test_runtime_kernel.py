from __future__ import annotations

import json
import io
import os
import subprocess
import sys
import time
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from _workflow_support import (
    analytics_path,
    kernel_script_path,
    load_json,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    request_and_approve_skill_approval,
    request_and_approve_transition,
    run_kernel,
    run_kernel_process,
    run_script,
    runtime_src_path,
    script_path,
    seed_analysis_chain,
)

RUN_ID = "run-kernel-001"
ROUND_ID = "round-kernel-001"


def ensure_runtime_src_on_path() -> None:
    runtime_src = runtime_src_path()
    if str(runtime_src) not in sys.path:
        sys.path.insert(0, str(runtime_src))


def default_runtime_gate_handlers() -> dict[str, object]:
    ensure_runtime_src_on_path()

    from eco_council_runtime.kernel.execution.runtime_gate_profile import runtime_gate_handler_registry

    return runtime_gate_handler_registry()


def default_runtime_posture_profile_config() -> dict[str, object]:
    ensure_runtime_src_on_path()

    from eco_council_runtime.kernel.execution.runtime_posture_profile import default_runtime_posture_profile

    return default_runtime_posture_profile()


def approve_report_basis_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="freeze-report-basis",
        rationale="Approve report_basis for runtime-kernel governed-execution coverage.",
    )


def approve_close_round_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="close-round",
        rationale="Approve close-round for runtime-kernel coverage.",
    )


class RuntimeKernelTests(unittest.TestCase):
    def test_kernel_tracks_manifest_cursor_and_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            evidence_ref = primary_successor_evidence_ref(outputs)
            issue_id = primary_research_issue_id(outputs)
            run_script(
                script_path("post-board-note"),
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
                evidence_ref,
            )
            run_script(
                script_path("update-hypothesis-status"),
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
                issue_id,
                "--linked-artifact-ref",
                evidence_ref,
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
                "summarize-board-state",
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
                "propose-next-actions",
                "--skill-approval-request-id",
                request_and_approve_skill_approval(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="propose-next-actions",
                    requested_actor_role="moderator",
                    rationale="Approve optional-analysis next-actions execution for runtime-kernel ledger coverage.",
                ),
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
            self.assertEqual("propose-next-actions", manifest["last_skill_name"])
            self.assertEqual(ROUND_ID, cursor["current_round_id"])
            self.assertEqual("propose-next-actions", cursor["last_skill_name"])
            self.assertGreaterEqual(registry["skill_count"], 1)
            self.assertGreaterEqual(len(state_payload["ledger_tail"]), 2)
            ledger_receipt_ids = [
                str(entry.get("receipt_id") or "")
                for entry in state_payload["ledger_tail"]
                if isinstance(entry, dict)
            ]
            self.assertIn(first_run["summary"]["receipt_id"], ledger_receipt_ids)
            self.assertIn(second_run["summary"]["receipt_id"], ledger_receipt_ids)
            self.assertTrue((runtime_dir / "receipts" / f"{first_run['summary']['receipt_id']}.json").exists())
            self.assertTrue((runtime_dir / "receipts" / f"{second_run['summary']['receipt_id']}.json").exists())
            self.assertEqual(kernel_script_path().name, "eco_runtime_kernel.py")

    def test_kernel_lists_no_legacy_claim_cluster_result_sets_after_successor_helpers(
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
            self.assertEqual(0, payload["summary"]["matching_result_set_count"])
            self.assertEqual(0, payload["summary"]["returned_result_set_count"])
            self.assertEqual([], payload["result_sets"])

    def test_kernel_queries_no_legacy_claim_cluster_items_after_successor_helpers(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

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
                "air-quality-smoke",
                "--include-result-sets",
                "--include-contract",
            )

            self.assertEqual("analysis-plane-item-query-v1", payload["schema_version"])
            self.assertEqual(0, payload["summary"]["matching_result_set_count"])
            self.assertEqual(0, payload["summary"]["returned_item_count"])
            self.assertEqual([], payload["items"])
            self.assertEqual([], payload["result_sets"])

    def test_kernel_does_not_inline_legacy_controversy_map_fallback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            payload = run_kernel(
                "query-analysis-result-items",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--analysis-kind",
                "controversy-map",
                "--latest-only",
                "--subject-id",
                "air-quality-smoke",
                "--include-result-sets",
                "--include-contract",
            )

            self.assertEqual("analysis-plane-item-query-v1", payload["schema_version"])
            self.assertEqual(0, payload["summary"]["matching_result_set_count"])
            self.assertEqual(0, payload["summary"]["returned_item_count"])
            self.assertEqual([], payload["items"])
            self.assertEqual([], payload["result_sets"])

    def test_kernel_does_not_inline_legacy_issue_cluster_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            payload = run_kernel(
                "query-analysis-result-items",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--analysis-kind",
                "issue-cluster",
                "--latest-only",
                "--subject-id",
                "air-quality-smoke",
                "--include-result-sets",
                "--include-contract",
            )

            self.assertEqual("analysis-plane-item-query-v1", payload["schema_version"])
            self.assertEqual(0, payload["summary"]["matching_result_set_count"])
            self.assertEqual(0, payload["summary"]["returned_item_count"])
            self.assertEqual([], payload["items"])
            self.assertEqual([], payload["result_sets"])

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
            handoff_entry = next(item for item in registry["skills"] if item["skill_name"] == "materialize-reporting-handoff")
            self.assertEqual("runtime-registry-v3", registry["schema_version"])
            self.assertEqual(registry["skill_count"], registry["skill_access_summary"]["skill_count"])
            self.assertIn("run_dir/report_basis/frozen_report_basis_<round_id>.json", handoff_entry["declared_contract"]["reads"])
            self.assertEqual("Eco Materialize Reporting Handoff", handoff_entry["agent"]["display_name"])
            self.assertEqual("materialize-reporting-handoff", handoff_entry["skill_access"]["skill_name"])
            self.assertIn("report-editor", handoff_entry["skill_access"]["allowed_roles"])

            proposal_entry = next(item for item in registry["skills"] if item["skill_name"] == "submit-council-proposal")
            self.assertIn("evidence_ref", proposal_entry["declared_inputs"]["required"])
            self.assertNotIn("Recommended", proposal_entry["declared_inputs"]["required"])
            self.assertIn("response_to_id", proposal_entry["declared_inputs"]["optional"])

            payload = run_kernel(
                "run-skill",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--skill-name",
                "post-board-note",
                "--",
                "--author-role",
                "moderator",
                "--note-text",
                "Runtime metadata note.",
            )
            event = payload["event"]

            self.assertEqual("runtime-event-v3", event["schema_version"])
            self.assertEqual(["--author-role", "moderator", "--note-text", "Runtime metadata note."], event["skill_args"])
            self.assertEqual("post-board-note", event["skill_registry_entry"]["skill_name"])
            self.assertEqual("moderator", event["actor_role"])
            self.assertEqual("moderator", event["resolved_actor_role"])
            self.assertIn(str((run_dir / "board" / f"investigation_board.json").resolve()), event["resolved_write_paths"])
            self.assertIn("argv", event["command_snapshot"])
            self.assertTrue(event["execution_input_hash"])
            self.assertTrue(event["payload_hash"])

    def test_kernel_does_not_prepend_runtime_flags_to_standalone_fetch_skills(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import run_skill

            fake_script = root / "fake_fetch.py"
            fake_payload = {
                "status": "completed",
                "summary": {"source": "fake"},
                "receipt_id": "runtime-receipt-fetch-no-runtime-flags",
                "artifact_refs": [],
                "canonical_ids": [],
            }
            fake_skill_entry = {
                "skill_name": "fetch-open-meteo-air-quality",
                "script_path": str(fake_script),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": ["network-external"],
                "execution_policy": {},
                "agent": {},
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(fake_payload), stderr=""),
                ) as run_mock,
            ):
                payload = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="fetch-open-meteo-air-quality",
                    actor_role="environmental-investigator",
                    skill_args=["fetch", "--location", "40.7128,-74.0060"],
                    contract_mode="warn",
                    allow_side_effects=["network-external"],
                )

            argv = run_mock.call_args.args[0]
            self.assertEqual([sys.executable, str(fake_script), "fetch", "--location", "40.7128,-74.0060"], argv)
            self.assertNotIn("--run-dir", argv)
            self.assertEqual(argv, payload["event"]["command_snapshot"]["argv"])

    def test_kernel_loads_skill_config_env_without_overriding_process_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import run_skill

            fake_script = root / "skills" / "fetch-nasa-firms-fire" / "scripts" / "fake_fetch.py"
            fake_script.parent.mkdir(parents=True)
            config_env = fake_script.parent.parent / "assets" / "config.env"
            config_env.parent.mkdir(parents=True)
            config_env.write_text(
                "NASA_FIRMS_MAP_KEY=from_config_env_file\n"
                "NASA_FIRMS_TIMEOUT_SECONDS=45\n",
                encoding="utf-8",
            )
            fake_payload = {
                "status": "completed",
                "summary": {"source": "fake"},
                "receipt_id": "runtime-receipt-fetch-env",
                "artifact_refs": [],
                "canonical_ids": [],
            }
            fake_skill_entry = {
                "skill_name": "fetch-nasa-firms-fire",
                "script_path": str(fake_script),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": ["network-external"],
                "execution_policy": {},
                "agent": {},
            }

            with (
                mock.patch.dict(os.environ, {"NASA_FIRMS_MAP_KEY": "from_process_env"}, clear=False),
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(fake_payload), stderr=""),
                ) as run_mock,
            ):
                payload = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="fetch-nasa-firms-fire",
                    actor_role="environmental-investigator",
                    skill_args=["fetch", "--dry-run"],
                    contract_mode="warn",
                    allow_side_effects=["network-external"],
                )

            env = run_mock.call_args.kwargs["env"]
            self.assertEqual("from_process_env", env["NASA_FIRMS_MAP_KEY"])
            self.assertEqual("45", env["NASA_FIRMS_TIMEOUT_SECONDS"])
            self.assertEqual(
                [str(config_env.resolve())],
                payload["event"]["command_snapshot"]["loaded_env_files"],
            )

    def test_kernel_blocks_write_command_without_explicit_actor_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            completed = run_kernel_process(
                "init-run",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                auto_actor_role=False,
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("failed", payload["status"])
            self.assertIn("missing-actor-role", {item["code"] for item in payload["access_policy"]["issues"]})

    def test_challenger_can_review_but_not_transition(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.governance.access_policy import (
            evaluate_kernel_command_access,
        )

        review_access = evaluate_kernel_command_access(
            "post-review-comment",
            actor_role="challenger",
        )
        transition_access = evaluate_kernel_command_access(
            "request-phase-transition",
            actor_role="challenger",
        )

        self.assertFalse(review_access["block_execution"])
        self.assertEqual("challenger", review_access["resolved_actor_role"])
        self.assertTrue(transition_access["block_execution"])
        self.assertIn(
            "actor-role-not-allowed",
            {item["code"] for item in transition_access["issues"]},
        )

    def test_transition_request_store_rejects_non_moderator_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.transition_requests import (
                store_transition_request,
            )

            with self.assertRaises(ValueError) as raised:
                store_transition_request(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    transition_kind="freeze-report-basis",
                    requested_by_role="environmental-investigator",
                    rationale="Only moderator should be able to request transitions.",
                )

            self.assertIn(
                "store_transition_request requires actor role `moderator`",
                str(raised.exception),
            )

    def test_transition_request_approval_rejects_non_operator_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.transition_requests import (
                REQUEST_STATUS_PENDING,
                approve_transition_request,
                load_transition_request,
                store_transition_request,
            )

            request = store_transition_request(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                requested_by_role="moderator",
                rationale="Moderator requests report-basis transition.",
            )

            with self.assertRaises(ValueError) as raised:
                approve_transition_request(
                    run_dir,
                    request_id=request["request_id"],
                    approved_by_role="moderator",
                    decision_reason="Moderator cannot self-approve transitions.",
                )

            self.assertIn(
                "approve_transition_request requires actor role `runtime-operator`",
                str(raised.exception),
            )
            request_after = load_transition_request(
                run_dir,
                request_id=request["request_id"],
            )
            self.assertEqual(REQUEST_STATUS_PENDING, request_after["request_status"])

    def test_transition_request_commit_rejects_non_operator_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.transition_requests import (
                approve_transition_request,
                mark_transition_request_committed,
                store_transition_request,
            )

            request = store_transition_request(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                requested_by_role="moderator",
                rationale="Moderator requests report-basis transition.",
            )
            approve_transition_request(
                run_dir,
                request_id=request["request_id"],
                approved_by_role="runtime-operator",
                decision_reason="Operator approved transition.",
            )

            with self.assertRaises(ValueError) as raised:
                mark_transition_request_committed(
                    run_dir,
                    request_id=request["request_id"],
                    committed_by_role="moderator",
                    committed_object_kind="report-basis-freeze",
                    committed_object_id=ROUND_ID,
                )

            self.assertIn(
                "mark_transition_request_committed requires actor role `runtime-operator`",
                str(raised.exception),
            )

    def test_transition_request_commit_is_idempotent_and_rejects_retarget(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.transition_requests import (
                REQUEST_STATUS_COMMITTED,
                approve_transition_request,
                load_transition_request,
                mark_transition_request_committed,
                store_transition_request,
            )

            request = store_transition_request(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                requested_by_role="moderator",
                rationale="Moderator requests report-basis transition.",
            )
            approve_transition_request(
                run_dir,
                request_id=request["request_id"],
                approved_by_role="runtime-operator",
                decision_reason="Operator approved transition.",
            )

            first_commit = mark_transition_request_committed(
                run_dir,
                request_id=request["request_id"],
                committed_by_role="runtime-operator",
                committed_object_kind="report-basis-freeze",
                committed_object_id="report-basis-freeze-001",
            )
            second_commit = mark_transition_request_committed(
                run_dir,
                request_id=request["request_id"],
                committed_by_role="runtime-operator",
                committed_object_kind="report-basis-freeze",
                committed_object_id="report-basis-freeze-001",
            )

            self.assertEqual("committed", first_commit["commit_status"])
            self.assertEqual("already-committed", second_commit["commit_status"])
            self.assertEqual(
                first_commit["committed_at_utc"],
                second_commit["committed_at_utc"],
            )

            with self.assertRaises(ValueError) as raised:
                mark_transition_request_committed(
                    run_dir,
                    request_id=request["request_id"],
                    committed_by_role="runtime-operator",
                    committed_object_kind="report-basis-freeze",
                    committed_object_id="report-basis-freeze-002",
                )

            self.assertIn("already committed", str(raised.exception))
            request_after = load_transition_request(
                run_dir,
                request_id=request["request_id"],
            )
            self.assertEqual(REQUEST_STATUS_COMMITTED, request_after["request_status"])
            self.assertEqual(
                "report-basis-freeze",
                request_after["committed_object_kind"],
            )
            self.assertEqual(
                "report-basis-freeze-001",
                request_after["committed_object_id"],
            )

    def test_preflight_blocks_unauthorized_actor_role_for_write_skill(self) -> None:
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
                "summarize-board-state",
                "--actor-role",
                "environmental-investigator",
                "--contract-mode",
                "strict",
                auto_actor_role=False,
            )

            self.assertEqual(1, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("blocked", payload["status"])
            self.assertEqual(
                "environmental-investigator",
                payload["preflight"]["resolved_actor_role"],
            )
            self.assertIn(
                "actor-role-not-allowed",
                {item["code"] for item in payload["preflight"]["issues"]},
            )

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
                "post-board-note",
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
                "post-board-note",
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
                "post-board-note",
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
                "post-board-note",
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

            from eco_council_runtime.kernel.execution.executor import SkillExecutionError, run_skill

            output_path = (run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").resolve()
            fake_payload = {
                "status": "completed",
                "summary": {
                    "skill": "summarize-board-state",
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
                "eco_council_runtime.kernel.execution.executor.subprocess.run",
                return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout=json.dumps(fake_payload), stderr=""),
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="summarize-board-state",
                        actor_role="moderator",
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

            from eco_council_runtime.kernel.governance.runtime_governance import preflight_skill_execution

            fake_skill_entry = {
                "skill_name": "fetch-youtube-video-search",
                "script_path": str(root / "fake_skill.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": ["network-external"],
                "execution_policy": {},
                "agent": {},
            }

            with mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry):
                blocked = preflight_skill_execution(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="fetch-youtube-video-search",
                    actor_role="social-investigator",
                    skill_args=[],
                    contract_mode="strict",
                )
                approved = preflight_skill_execution(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="fetch-youtube-video-search",
                    actor_role="social-investigator",
                    skill_args=[],
                    contract_mode="strict",
                    allow_side_effects=["network-external"],
                )

            self.assertTrue(blocked["block_execution"])
            self.assertIn("missing-side-effect-approval", {item["code"] for item in blocked["issues"]})
            self.assertEqual(["network-external"], approved["declared_side_effects"])
            self.assertIn("network-external", approved["allowed_side_effects"])
            self.assertFalse(approved["block_execution"])

    def test_preflight_skill_approval_handoff_preserves_requested_args(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.runtime_governance import (
                preflight_skill_execution,
            )

            skill_args = [
                "--action-id",
                "action-001",
                "--basis-object-id",
                "hypothesis-001",
            ]
            payload = preflight_skill_execution(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name="open-falsification-probe",
                actor_role="challenger",
                skill_args=skill_args,
                contract_mode="strict",
            )

            self.assertTrue(payload["block_execution"])
            self.assertIn(
                "missing-skill-approval-request-id",
                {item["code"] for item in payload["issues"]},
            )
            approval = payload["skill_approval"]
            self.assertEqual("missing-request-id", approval["status"])
            request_command = approval["request_skill_approval_command_template"]
            run_command = approval["run_approved_skill_command_template"]
            self.assertIn("request-skill-approval", request_command)
            self.assertIn("--requested-skill-arg=--action-id", request_command)
            self.assertIn("--requested-skill-arg=action-001", request_command)
            self.assertIn("--requested-skill-arg=--basis-object-id", request_command)
            self.assertIn("--requested-skill-arg=hypothesis-001", request_command)
            self.assertEqual(skill_args, approval["requested_skill_args"])
            self.assertIn("--skill-approval-request-id '<request_id>'", run_command)
            self.assertLess(
                run_command.index("--skill-approval-request-id"),
                run_command.index("-- --action-id"),
            )

    def test_registered_skill_allowed_roles_have_required_capabilities(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.governance.role_contracts import role_capabilities
        from eco_council_runtime.kernel.governance.skill_registry import POLICIES

        mismatches: list[tuple[str, str, list[str]]] = []
        for skill_name, policy in POLICIES.items():
            required = set(policy.get("required_capabilities") or [])
            for role in policy.get("allowed_roles") or []:
                missing = sorted(required - role_capabilities(role))
                if missing:
                    mismatches.append((skill_name, role, missing))

        self.assertEqual([], mismatches)

    def test_runtime_operator_cannot_run_fetch_normalize_bridge(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.role_contracts import role_capabilities
            from eco_council_runtime.kernel.governance.runtime_governance import preflight_skill_execution
            from eco_council_runtime.kernel.governance.skill_registry import resolve_skill_policy

            policy = resolve_skill_policy("normalize-fetch-execution")
            payload = preflight_skill_execution(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name="normalize-fetch-execution",
                actor_role="runtime-operator",
                skill_args=[],
                contract_mode="strict",
            )

            self.assertNotIn("runtime-operator", policy["allowed_roles"])
            self.assertNotIn("normalize", role_capabilities("runtime-operator"))
            self.assertTrue(payload["block_execution"])
            self.assertIn("actor-role-not-allowed", {item["code"] for item in payload["issues"]})

    def test_runtime_operator_cannot_run_reporting_content_skills(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.governance.skill_registry import resolve_skill_policy

        content_skills = [
            "materialize-reporting-handoff",
            "materialize-spatiotemporal-relation-evidence-packet",
            "draft-council-decision",
            "draft-expert-report",
            "publish-expert-report",
            "publish-council-decision",
            "materialize-final-publication",
        ]

        for skill_name in content_skills:
            with self.subTest(skill_name=skill_name):
                policy = resolve_skill_policy(skill_name)
                self.assertNotIn("runtime-operator", policy["allowed_roles"])

    def test_runtime_operator_cannot_run_mission_or_source_planning_skills(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.governance.role_contracts import role_capabilities
        from eco_council_runtime.kernel.governance.skill_registry import resolve_skill_policy

        self.assertNotIn("round-bootstrap", role_capabilities("runtime-operator"))
        for skill_name in ["scaffold-mission-run", "prepare-round"]:
            with self.subTest(skill_name=skill_name):
                policy = resolve_skill_policy(skill_name)
                self.assertNotIn("runtime-operator", policy["allowed_roles"])

    def test_role_contracts_expose_conceptual_role_boundaries(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.governance.role_contracts import known_actor_role, role_contract

        operator = role_contract("runtime-operator")
        moderator = role_contract("moderator")
        social_investigator = role_contract("social-investigator")

        self.assertEqual("runtime-principal", operator["role_kind"])
        self.assertEqual("runtime", operator["conceptual_role"])
        self.assertIn("not a council agent", operator["conceptual_note"])
        self.assertEqual("council-agent", moderator["role_kind"])
        self.assertEqual("moderator", moderator["conceptual_role"])
        self.assertEqual("council-agent", social_investigator["role_kind"])
        self.assertEqual("social-investigator", social_investigator["conceptual_role"])
        self.assertEqual(["social-investigator"], social_investigator["aliases"])
        self.assertFalse(known_actor_role("public-discourse-investigator"))
        self.assertFalse(known_actor_role("formal-record-investigator"))
        self.assertFalse(known_actor_role("social_investigator"))

    def test_prepare_round_contract_role_placeholder_is_fanout_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.governance.runtime_governance import preflight_skill_execution

            payload = preflight_skill_execution(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                skill_name="prepare-round",
                actor_role="moderator",
                skill_args=[],
                contract_mode="strict",
            )

            self.assertNotIn(
                "unresolved-contract-placeholder",
                {item["code"] for item in payload["issues"]},
            )

    def test_run_skill_retries_and_recovers_after_transient_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import run_skill
            from eco_council_runtime.kernel.core.ledger import load_ledger_tail

            fake_skill_entry = {
                "skill_name": "summarize-board-state",
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
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    side_effect=[
                        subprocess.CompletedProcess(args=["python"], returncode=3, stdout="", stderr="temporary upstream error"),
                        subprocess.CompletedProcess(args=["python"], returncode=0, stdout=json.dumps(recovered_payload), stderr=""),
                    ],
                ),
                mock.patch("eco_council_runtime.kernel.execution.executor.time.sleep") as sleep_mock,
            ):
                payload = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="summarize-board-state",
                    actor_role="moderator",
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

    def test_runtime_receipt_envelope_captures_governed_execution_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import run_skill

            fake_skill_entry = {
                "skill_name": "summarize-board-state",
                "script_path": str(root / "fake_receipt.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            skill_payload = {
                "status": "completed",
                "summary": {"result": "ok"},
                "receipt_id": "runtime-receipt-envelope-test",
                "artifact_refs": [],
                "canonical_ids": ["summary-1"],
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    return_value=subprocess.CompletedProcess(
                        args=["python"],
                        returncode=0,
                        stdout=json.dumps(skill_payload),
                        stderr="",
                    ),
                ),
            ):
                payload = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="summarize-board-state",
                    actor_role="moderator",
                    skill_args=[],
                    contract_mode="warn",
                )

            receipt_path = run_dir / "runtime" / "receipts" / "runtime-receipt-envelope-test.json"
            receipt = load_json(receipt_path)

            self.assertEqual("created", payload["event"]["receipt_write"]["write_status"])
            self.assertEqual("runtime-receipt-v2", receipt["schema_version"])
            self.assertEqual("runtime-receipt-envelope-test", receipt["receipt_id"])
            self.assertEqual(skill_payload, receipt["skill_payload"])
            self.assertEqual(payload["event"]["payload_hash"], receipt["payload_hash"])
            self.assertEqual(payload["event"]["event_id"], receipt["runtime"]["event_id"])
            self.assertEqual(payload["event"]["execution_input_hash"], receipt["runtime"]["execution_input_hash"])
            self.assertEqual(payload["event"]["lock_path"], receipt["runtime"]["lock_path"])
            self.assertEqual("completed", receipt["runtime"]["postflight"]["status"])
            self.assertFalse(receipt["runtime"]["runtime_admission"]["block_execution"])

    def test_runtime_receipt_replay_marks_same_payload_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import run_skill
            from eco_council_runtime.kernel.core.ledger import load_ledger_tail

            fake_skill_entry = {
                "skill_name": "summarize-board-state",
                "script_path": str(root / "fake_receipt_replay.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            skill_payload = {
                "status": "completed",
                "summary": {"result": "same"},
                "receipt_id": "runtime-receipt-replay-test",
                "artifact_refs": [],
                "canonical_ids": [],
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    side_effect=[
                        subprocess.CompletedProcess(
                            args=["python"],
                            returncode=0,
                            stdout=json.dumps(skill_payload),
                            stderr="",
                        ),
                        subprocess.CompletedProcess(
                            args=["python"],
                            returncode=0,
                            stdout=json.dumps(skill_payload),
                            stderr="",
                        ),
                    ],
                ),
            ):
                first = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="summarize-board-state",
                    actor_role="moderator",
                    skill_args=[],
                    contract_mode="warn",
                )
                second = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="summarize-board-state",
                    actor_role="moderator",
                    skill_args=[],
                    contract_mode="warn",
                )

            receipt = load_json(
                run_dir / "runtime" / "receipts" / "runtime-receipt-replay-test.json"
            )
            latest_event = load_ledger_tail(run_dir, 1)[0]

            self.assertEqual("created", first["event"]["receipt_write"]["write_status"])
            self.assertEqual("unchanged", second["event"]["receipt_write"]["write_status"])
            self.assertEqual("unchanged", latest_event["receipt_write"]["write_status"])
            self.assertEqual(first["event"]["event_id"], receipt["runtime"]["event_id"])
            self.assertEqual(first["event"]["payload_hash"], second["event"]["payload_hash"])
            self.assertEqual(
                first["event"]["receipt_write"]["payload_hash"],
                second["event"]["receipt_write"]["previous_payload_hash"],
            )

    def test_runtime_receipt_conflict_blocks_without_replacing_existing_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import show_run_state
            from eco_council_runtime.kernel.execution.executor import SkillExecutionError, run_skill
            from eco_council_runtime.kernel.core.ledger import load_ledger_tail
            from eco_council_runtime.kernel.governance.agent_entry.profile import (
                default_agent_entry_profile,
            )

            fake_skill_entry = {
                "skill_name": "summarize-board-state",
                "script_path": str(root / "fake_receipt_conflict.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            first_skill_payload = {
                "status": "completed",
                "summary": {"result": "first"},
                "receipt_id": "runtime-receipt-conflict-test",
                "artifact_refs": [],
                "canonical_ids": ["first-id"],
            }
            second_skill_payload = {
                "status": "completed",
                "summary": {"result": "second"},
                "receipt_id": "runtime-receipt-conflict-test",
                "artifact_refs": [],
                "canonical_ids": ["second-id"],
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    side_effect=[
                        subprocess.CompletedProcess(
                            args=["python"],
                            returncode=0,
                            stdout=json.dumps(first_skill_payload),
                            stderr="",
                        ),
                        subprocess.CompletedProcess(
                            args=["python"],
                            returncode=0,
                            stdout=json.dumps(second_skill_payload),
                            stderr="",
                        ),
                    ],
                ),
            ):
                first = run_skill(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    skill_name="summarize-board-state",
                    actor_role="moderator",
                    skill_args=[],
                    contract_mode="warn",
                )
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="summarize-board-state",
                        actor_role="moderator",
                        skill_args=[],
                        contract_mode="warn",
                    )

            receipt = load_json(
                run_dir / "runtime" / "receipts" / "runtime-receipt-conflict-test.json"
            )
            latest_event = load_ledger_tail(run_dir, 1)[0]
            state_payload = show_run_state(
                run_dir,
                tail=5,
                round_id=ROUND_ID,
                agent_entry_profile=default_agent_entry_profile(),
            )

            self.assertEqual("created", first["event"]["receipt_write"]["write_status"])
            self.assertEqual(first_skill_payload, receipt["skill_payload"])
            self.assertEqual(
                "receipt-payload-hash-conflict",
                raised.exception.payload["failure"]["error_code"],
            )
            self.assertEqual(
                "conflict",
                raised.exception.payload["receipt_write"]["write_status"],
            )
            self.assertTrue(raised.exception.payload["receipt_write"]["conflict"])
            self.assertEqual("failed", latest_event["status"])
            self.assertEqual(
                "receipt-payload-hash-conflict",
                latest_event["failure"]["error_code"],
            )
            self.assertEqual("conflict", latest_event["receipt_write"]["write_status"])
            self.assertTrue(latest_event["dead_letter_id"].startswith("deadletter-"))
            self.assertEqual(1, state_payload["summary"]["receipt_conflict_count"])
            self.assertEqual(
                1,
                state_payload["operations"]["runtime_health"]["summary"][
                    "receipt_conflict_count"
                ],
            )
            self.assertEqual(
                "receipt-conflicts-present",
                state_payload["operations"]["runtime_health"]["alerts"][-1]["code"],
            )

    def test_show_run_state_surfaces_current_runtime_lock_holder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import init_run, show_run_state
            from eco_council_runtime.kernel.governance.agent_entry.profile import (
                default_agent_entry_profile,
            )

            init_run(run_dir, RUN_ID)
            child_code = """
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, sys.argv[2])
from eco_council_runtime.kernel.core.locking import exclusive_runtime_lock

metadata = {
    "run_id": sys.argv[3],
    "round_id": sys.argv[4],
    "skill_name": "summarize-board-state",
    "actor_role": "moderator",
    "execution_input_hash": "lock-test-input-hash",
}
with exclusive_runtime_lock(Path(sys.argv[1]), metadata=metadata):
    print("ready", flush=True)
    time.sleep(10)
"""
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    child_code,
                    str(run_dir),
                    str(runtime_src_path()),
                    RUN_ID,
                    ROUND_ID,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                assert process.stdout is not None
                self.assertEqual("ready", process.stdout.readline().strip())

                state_payload = show_run_state(
                    run_dir,
                    tail=5,
                    round_id=ROUND_ID,
                    agent_entry_profile=default_agent_entry_profile(),
                )

                runtime_lock = state_payload["operations"]["runtime_lock"]
                self.assertEqual("held", runtime_lock["lock_state"])
                self.assertEqual(
                    "summarize-board-state",
                    runtime_lock["metadata"]["skill_name"],
                )
                self.assertEqual("held", state_payload["summary"]["runtime_lock_state"])
                self.assertEqual(
                    "held",
                    state_payload["operations"]["operator"]["runtime_lock_state"],
                )
                self.assertTrue(
                    state_payload["operations"]["operator"][
                        "runtime_lock_state_path"
                    ].endswith("execution_lock_state.json")
                )
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()

    def test_run_skill_timeout_returns_structured_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import SkillExecutionError, run_skill
            from eco_council_runtime.kernel.core.ledger import load_ledger_tail

            fake_skill_entry = {
                "skill_name": "summarize-board-state",
                "script_path": str(root / "fake_slow.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }

            with (
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch(
                    "eco_council_runtime.kernel.execution.executor.subprocess.run",
                    side_effect=subprocess.TimeoutExpired(cmd=["python"], timeout=0.01, output="partial", stderr="still running"),
                ),
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="summarize-board-state",
                        actor_role="moderator",
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

            from eco_council_runtime.kernel.execution.executor import SkillExecutionError, run_skill
            from eco_council_runtime.kernel.core.ledger import load_ledger_tail
            from eco_council_runtime.kernel.operator.operations import load_dead_letters, materialize_admission_policy

            fake_skill_entry = {
                "skill_name": "summarize-board-state",
                "script_path": str(root / "fake_blocked.py"),
                "declared_contract": {"reads": [], "writes": []},
                "declared_inputs": {"required": [], "optional": []},
                "declared_side_effects": [],
                "execution_policy": {},
                "agent": {},
            }
            materialize_admission_policy(run_dir, run_id=RUN_ID, max_timeout_seconds=1.0)

            with (
                mock.patch("eco_council_runtime.kernel.governance.runtime_governance.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.resolve_skill_entry", return_value=fake_skill_entry),
                mock.patch("eco_council_runtime.kernel.execution.executor.subprocess.run") as subprocess_run_mock,
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_skill(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        skill_name="summarize-board-state",
                        actor_role="moderator",
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
            from eco_council_runtime.kernel.operator.operations import materialize_dead_letter
            from eco_council_runtime.kernel.governance.agent_entry.profile import (
                default_agent_entry_profile,
            )

            init_run(run_dir, RUN_ID)
            materialize_dead_letter(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                source_type="skill-execution",
                source_name="test-skill",
                message="Synthetic runtime failure for operator surface coverage.",
                failure={"error_code": "skill-timeout", "message": "timed out", "retryable": False},
                summary={"skill_name": "test-skill", "run_id": RUN_ID, "round_id": ROUND_ID},
            )

            payload = show_run_state(
                run_dir,
                tail=5,
                round_id=ROUND_ID,
                agent_entry_profile=default_agent_entry_profile(),
            )

            self.assertIn("operations", payload)
            self.assertEqual("red", payload["operations"]["runtime_health"]["alert_status"])
            self.assertEqual(1, payload["summary"]["open_dead_letter_count"])
            self.assertTrue(payload["operations"]["operator"]["admission_policy_path"].endswith("admission_policy.json"))
            self.assertTrue(payload["operations"]["operator"]["operator_runbook_path"].endswith(f"operator_runbook_{ROUND_ID}.md"))
            self.assertEqual("test-skill", payload["operations"]["dead_letters"][0]["source_name"])

    def test_resolve_dead_letter_closes_operator_health_alert(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.operator.operations import (
                materialize_dead_letter,
                resolve_dead_letter,
                runtime_health_payload,
            )
            from eco_council_runtime.kernel.core.ledger import append_ledger_event

            dead_letter = materialize_dead_letter(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                source_type="skill-execution",
                source_name="test-skill",
                message="Synthetic runtime failure for operator close coverage.",
                failure={"error_code": "skill-timeout", "message": "timed out", "retryable": False},
                summary={"skill_name": "test-skill", "run_id": RUN_ID, "round_id": ROUND_ID},
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": "runtimeevt-deadletter-health-test",
                    "event_type": "skill-execution",
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "skill_name": "test-skill",
                    "status": "failed",
                    "dead_letter_id": dead_letter["dead_letter_id"],
                },
            )

            before = runtime_health_payload(run_dir, round_id=ROUND_ID)
            closed = resolve_dead_letter(
                run_dir,
                dead_letter_id=dead_letter["dead_letter_id"],
                resolved_by_role="runtime-operator",
                resolution_reason="Fixed input and replayed the failed operation.",
            )
            after = runtime_health_payload(run_dir, round_id=ROUND_ID)

            self.assertEqual("open", dead_letter["resolution_status"])
            self.assertEqual("closed", closed["resolution_status"])
            self.assertEqual(1, before["summary"]["open_dead_letter_count"])
            self.assertEqual(1, before["summary"]["failed_event_count"])
            self.assertEqual(0, after["summary"]["open_dead_letter_count"])
            self.assertEqual(0, after["summary"]["failed_event_count"])
            self.assertEqual("green", after["alert_status"])

    def test_default_admission_policy_keeps_writes_inside_run_surface(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.operator.operations import default_admission_policy

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

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode
            from eco_council_runtime.kernel.execution.runtime_planning_profile import runtime_planning_source

            planner_result = {
                "summary": {"skill_name": "plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            board_summary_result = {
                "summary": {"skill_name": "summarize-board-state", "event_id": "evt-step", "receipt_id": "receipt-step"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_summary.json")}},
            }
            board_brief_result = {
                "summary": {"skill_name": "materialize-board-brief", "event_id": "evt-brief", "receipt_id": "receipt-brief"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_brief.md")}},
            }
            next_actions_result = {
                "summary": {"skill_name": "propose-next-actions", "event_id": "evt-next", "receipt_id": "receipt-next"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "next_actions.json")}},
            }
            readiness_result = {
                "summary": {"skill_name": "summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "readiness.json"), "readiness_status": "ready"}},
            }
            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {"summary": {"report_basis_status": "frozen"}, "artifact_refs": [], "canonical_ids": []},
            }
            planning = {
                "plan_id": "plan-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": [],
                "execution_queue": [
                    {
                        "stage_name": "board-summary",
                        "skill_name": "summarize-board-state",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "board_summary.json"),
                    },
                    {
                        "stage_name": "board-brief",
                        "skill_name": "materialize-board-brief",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "board_brief.md"),
                    },
                    {
                        "stage_name": "next-actions",
                        "skill_name": "propose-next-actions",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "next_actions.json"),
                    },
                    {
                        "stage_name": "round-readiness",
                        "skill_name": "summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "test",
                        "expected_output_path": str(root / "readiness.json"),
                    },
                ],
                "post_gate_steps": [
                    {"stage_name": "report-basis-freeze", "skill_name": "freeze-report-basis", "skill_args": [], "assigned_role_hint": "moderator", "reason": "test"}
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": [],
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-ready",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            report_basis_request_id = approve_report_basis_transition(run_dir)
            runtime_only_sources = [
                runtime_planning_source(
                    "runtime-planner-only",
                    source_kind="planner-skill",
                    output_path_key="orchestration_plan_path",
                    planner_skill_name="plan-round-orchestration",
                    materialized_message="Use only the injected runtime planner path.",
                    failed_message="Injected runtime planner path failed.",
                )
            ]

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=[planner_result, board_summary_result, board_brief_result, next_actions_result, readiness_result, report_basis_result],
                ) as run_skill_mock,
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                    planning_sources=runtime_only_sources,
                    timeout_seconds=12.5,
                    retry_budget=2,
                    retry_backoff_ms=150,
                    allow_side_effects=["network-external"],
                )

            self.assertEqual(6, run_skill_mock.call_count)
            for call in run_skill_mock.call_args_list:
                self.assertEqual("moderator", call.kwargs["actor_role"])
                self.assertEqual(12.5, call.kwargs["timeout_seconds"])
                self.assertEqual(2, call.kwargs["retry_budget"])
                self.assertEqual(150, call.kwargs["retry_backoff_ms"])
                self.assertEqual(["network-external"], call.kwargs["allow_side_effects"])
            self.assertEqual(12.5, payload["controller"]["execution_policy"]["timeout_seconds"])
            self.assertEqual(2, payload["controller"]["execution_policy"]["retry_budget"])
            self.assertEqual(["network-external"], payload["controller"]["execution_policy"]["allow_side_effects"])
            self.assertEqual("runtime-controller-v3", payload["controller"]["schema_version"])
            self.assertEqual("fresh-run", payload["controller"]["resume_status"])
            self.assertEqual("summarize-board-state", payload["controller"]["stage_contracts"]["board-summary"]["expected_skill_name"])
            self.assertEqual(
                ["--transition-request-id", report_basis_request_id],
                run_skill_mock.call_args_list[-1].kwargs["skill_args"],
            )

    def test_controller_uses_plan_declared_gate_steps_instead_of_injecting_default_gate_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode
            from eco_council_runtime.kernel.execution.runtime_planning_profile import runtime_planning_source

            planner_result = {
                "summary": {"skill_name": "plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            readiness_result = {
                "summary": {"skill_name": "summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "custom_readiness.json"), "readiness_status": "ready"},
                },
            }
            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "custom_basis.json"), "report_basis_status": "frozen"},
                },
            }
            planning = {
                "plan_id": "plan-custom-gate-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": [],
                "execution_queue": [
                    {
                        "stage_name": "round-readiness",
                        "stage_kind": "skill",
                        "phase_group": "execution",
                        "skill_name": "summarize-round-readiness",
                        "expected_skill_name": "summarize-round-readiness",
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
                        "stage_name": "final-report-basis-review",
                        "stage_kind": "gate",
                        "phase_group": "gate",
                        "required_previous_stages": ["round-readiness"],
                        "blocking": True,
                        "resume_policy": "skip-if-completed",
                        "operator_summary": "Custom gate step declared by plan payload.",
                        "reason": "test",
                        "expected_output_path": str(root / "custom_gate.json"),
                        "gate_handler": "report-basis-gate",
                        "readiness_stage_name": "round-readiness",
                    }
                ],
                "post_gate_steps": [
                    {
                        "stage_name": "report-basis-freeze",
                        "stage_kind": "skill",
                        "phase_group": "report-basis",
                        "skill_name": "freeze-report-basis",
                        "expected_skill_name": "freeze-report-basis",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "required_previous_stages": ["final-report-basis-review"],
                        "blocking": True,
                        "resume_policy": "skip-if-completed",
                        "operator_summary": "Custom report_basis stage declared by plan payload.",
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
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "custom_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            report_basis_request_id = approve_report_basis_transition(run_dir)
            runtime_only_sources = [
                runtime_planning_source(
                    "runtime-planner-only",
                    source_kind="planner-skill",
                    output_path_key="orchestration_plan_path",
                    planner_skill_name="plan-round-orchestration",
                    materialized_message="Use only the injected runtime planner path.",
                    failed_message="Injected runtime planner path failed.",
                )
            ]

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.controller.planning_bundle", return_value=planning),
                mock.patch(
                    "eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate",
                    return_value=gate_payload,
                ) as gate_mock,
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=[planner_result, readiness_result, report_basis_result],
                ),
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                    planning_sources=runtime_only_sources,
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
                ["orchestration-planner", "round-readiness", "final-report-basis-review", "report-basis-freeze"],
                payload["controller"]["planning"]["stage_sequence"],
            )
            self.assertEqual(
                str(root / "custom_gate.json"),
                payload["controller"]["stage_contracts"]["final-report-basis-review"]["expected_output_path"],
            )
            self.assertEqual(
                str(root / "custom_gate.json"),
                payload["controller"]["steps"][2]["artifact_path"],
            )
            self.assertEqual(1, payload["controller"]["planning"]["gate_step_count"])
            self.assertEqual(
                ["--transition-request-id", report_basis_request_id],
                payload["controller"]["steps"][3]["skill_args"],
            )

    def test_controller_blocks_report_basis_stage_without_approved_transition_request(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import (
                run_governed_execution_round_with_contract_mode,
            )
            from eco_council_runtime.kernel.execution.executor import SkillExecutionError
            from eco_council_runtime.kernel.execution.runtime_planning_profile import runtime_planning_source

            planner_result = {
                "summary": {
                    "skill_name": "plan-round-orchestration",
                    "event_id": "evt-plan",
                    "receipt_id": "receipt-plan",
                },
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            readiness_result = {
                "summary": {
                    "skill_name": "summarize-round-readiness",
                    "event_id": "evt-ready",
                    "receipt_id": "receipt-ready",
                },
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {
                        "output_path": str(root / "readiness.json"),
                        "readiness_status": "ready",
                    },
                },
            }
            planning = {
                "plan_id": "plan-missing-request-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "plan-round-orchestration",
                "probe_stage_included": False,
                "execution_queue": [
                    {
                        "stage_name": "round-readiness",
                        "skill_name": "summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Refresh readiness before report-basis freeze.",
                        "expected_output_path": str(root / "readiness.json"),
                    }
                ],
                "post_gate_steps": [
                    {
                        "stage_name": "report-basis-freeze",
                        "skill_name": "freeze-report-basis",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Freeze report_basis after gate review.",
                    }
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": [],
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            runtime_only_sources = [
                runtime_planning_source(
                    "runtime-planner-only",
                    source_kind="planner-skill",
                    output_path_key="orchestration_plan_path",
                    planner_skill_name="plan-round-orchestration",
                    materialized_message="Use only the injected runtime planner path.",
                    failed_message="Injected runtime planner path failed.",
                )
            ]

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.planning_bundle",
                    return_value=planning,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate",
                    return_value=gate_payload,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=[planner_result, readiness_result],
                ) as run_skill_mock,
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    run_governed_execution_round_with_contract_mode(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        contract_mode="strict",
                        gate_handlers=default_runtime_gate_handlers(),
                        posture_profile=default_runtime_posture_profile_config(),
                        planning_sources=runtime_only_sources,
                    )

            self.assertEqual(2, run_skill_mock.call_count)
            self.assertEqual("failed", raised.exception.payload["status"])
            self.assertEqual(
                "missing-approved-transition-request",
                raised.exception.payload["failure"]["stage_failure"]["failure"][
                    "error_code"
                ],
            )
            self.assertIn(
                "freeze-report-basis",
                raised.exception.payload["message"],
            )

    def test_gate_runtime_dispatches_custom_handler_registry(self) -> None:
        ensure_runtime_src_on_path()

        from eco_council_runtime.kernel.execution.gate import execute_gate_step

        run_dir = Path("/tmp/runtime-gate-registry")
        handler = mock.Mock(
            return_value={
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "output_path": "/tmp/custom_gate.json",
                "controller_updates": {
                    "readiness_status": "custom-ready",
                    "gate_status": "custom-approved",
                    "gate_reasons": ["registry-dispatched"],
                    "recommended_next_skills": ["custom-follow-up"],
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
            ["custom-follow-up"],
            result["controller_updates"]["recommended_next_skills"],
        )

    def test_controller_executes_approved_report_basis_request_without_planner_stage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode

            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "report_basis_status": "frozen"},
                },
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            report_basis_request_id = approve_report_basis_transition(run_dir)

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    return_value=report_basis_result,
                ) as run_skill_mock,
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                )

            self.assertEqual(
                ["freeze-report-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("transition-executor", payload["controller"]["planning_mode"])
            self.assertEqual("transition-executor", payload["controller"]["planning"]["planning_mode"])
            self.assertEqual("transition-executor", payload["controller"]["planning"]["controller_authority"])
            self.assertEqual("approved-transition-request", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(
                ["report-basis-gate", "report-basis-freeze"],
                payload["controller"]["completed_stage_names"],
            )
            self.assertEqual("report-basis-gate", payload["controller"]["steps"][0]["stage"])
            self.assertEqual(
                ["--transition-request-id", report_basis_request_id],
                run_skill_mock.call_args_list[-1].kwargs["skill_args"],
            )
            self.assertEqual(
                [{"source": "approved-transition-request", "status": "adopted"}],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )

    def test_controller_restarts_completed_default_path_for_newer_approved_transition_request(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode

            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "report_basis_status": "frozen"},
                },
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            first_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                rationale="Approve first report_basis freeze request for stale-controller coverage.",
            )

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch(
                    "eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate",
                    return_value=gate_payload,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    return_value=report_basis_result,
                ) as first_run_skill_mock,
            ):
                first_payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                )

            time.sleep(1.1)
            second_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="freeze-report-basis",
                rationale="Approve second report_basis freeze request for stale-controller coverage.",
            )
            self.assertNotEqual(first_request_id, second_request_id)

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch(
                    "eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate",
                    return_value=gate_payload,
                ),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    return_value=report_basis_result,
                ) as second_run_skill_mock,
            ):
                second_payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                )

            self.assertEqual(first_request_id, first_payload["controller"]["adopted_transition_request_id"])
            self.assertEqual(second_request_id, second_payload["controller"]["adopted_transition_request_id"])
            self.assertEqual("restart-stale-transition", second_payload["controller"]["resume_status"])
            self.assertEqual(
                "newer-approved-transition-request",
                second_payload["controller"]["stale_controller"]["reason"],
            )
            self.assertEqual(
                second_request_id,
                second_payload["controller"]["stale_controller"]["latest_approved_transition_request_id"],
            )
            self.assertEqual(1, first_run_skill_mock.call_count)
            self.assertEqual(1, second_run_skill_mock.call_count)
            self.assertEqual(
                ["controller-freshness", "approved-transition-request"],
                [
                    item["source"]
                    for item in second_payload["controller"]["planning_attempts"]
                ],
            )

    def test_controller_completes_without_default_plan_when_no_transition_request_exists(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=AssertionError("default controller path should not execute skills without an approved transition request"),
                ) as run_skill_mock,
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                )

            run_skill_mock.assert_not_called()
            self.assertEqual("completed", payload["controller"]["controller_status"])
            self.assertEqual("transition-executor", payload["controller"]["planning_mode"])
            self.assertEqual(
                "transition-request-inspection",
                payload["controller"]["planning"]["plan_source"],
            )
            self.assertEqual([], payload["controller"]["completed_stage_names"])
            self.assertEqual("needs-more-data", payload["controller"]["readiness_status"])
            self.assertEqual("withheld", payload["controller"]["report_basis_status"])
            self.assertEqual([], payload["controller"]["planning_attempts"])

    def test_controller_uses_only_approved_transition_request_on_default_path(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode

            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "report_basis_status": "frozen"},
                },
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            report_basis_request_id = approve_report_basis_transition(run_dir)

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    return_value=report_basis_result,
                ) as run_skill_mock,
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                )

            self.assertEqual(
                ["freeze-report-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("approved-transition-request", payload["summary"]["plan_source"])
            self.assertEqual("transition-executor", payload["controller"]["planning_mode"])
            self.assertEqual("approved-transition-request", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(
                [{"source": "approved-transition-request", "status": "adopted"}],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )

    def test_controller_resume_skips_completed_stages_after_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode
            from eco_council_runtime.kernel.execution.executor import SkillExecutionError
            from eco_council_runtime.kernel.execution.runtime_planning_profile import runtime_planning_source

            planner_result = {
                "summary": {"skill_name": "plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            board_summary_result = {
                "summary": {"skill_name": "summarize-board-state", "event_id": "evt-summary", "receipt_id": "receipt-summary"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_summary.json")}},
            }
            board_brief_result = {
                "summary": {"skill_name": "materialize-board-brief", "event_id": "evt-brief", "receipt_id": "receipt-brief"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "board_brief.md")}},
            }
            next_actions_result = {
                "summary": {"skill_name": "propose-next-actions", "event_id": "evt-next", "receipt_id": "receipt-next"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "next_actions.json")}},
            }
            readiness_result = {
                "summary": {"skill_name": "summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "readiness.json"), "readiness_status": "ready"},
                },
            }
            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "basis.json"), "report_basis_status": "frozen"}},
            }
            planning = {
                "plan_id": "plan-resume-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "plan-round-orchestration",
                "probe_stage_included": False,
                "assigned_role_hints": ["moderator"],
                "execution_queue": [
                    {"stage_name": "board-summary", "skill_name": "summarize-board-state", "skill_args": [], "assigned_role_hint": "moderator", "reason": "refresh board"},
                    {"stage_name": "board-brief", "skill_name": "materialize-board-brief", "skill_args": [], "assigned_role_hint": "moderator", "reason": "refresh brief"},
                    {"stage_name": "next-actions", "skill_name": "propose-next-actions", "skill_args": [], "assigned_role_hint": "moderator", "reason": "rank next actions"},
                    {"stage_name": "round-readiness", "skill_name": "summarize-round-readiness", "skill_args": [], "assigned_role_hint": "moderator", "reason": "refresh readiness"},
                ],
                "post_gate_steps": [
                    {"stage_name": "report-basis-freeze", "skill_name": "freeze-report-basis", "skill_args": [], "assigned_role_hint": "moderator", "reason": "freeze-report-basis"}
                ],
                "stop_conditions": [],
                "fallback_path": [],
                "fallback_suggested_next_skills": ["post-board-note"],
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            board_brief_failure = SkillExecutionError(
                "board brief failed",
                {
                    "status": "failed",
                    "message": "board brief failed",
                    "summary": {"skill_name": "materialize-board-brief", "run_id": RUN_ID, "round_id": ROUND_ID},
                    "failure": {"error_code": "skill-exit-nonzero", "retryable": True},
                },
            )
            report_basis_request_id = approve_report_basis_transition(run_dir)
            runtime_only_sources = [
                runtime_planning_source(
                    "runtime-planner-only",
                    source_kind="planner-skill",
                    output_path_key="orchestration_plan_path",
                    planner_skill_name="plan-round-orchestration",
                    materialized_message="Use only the injected runtime planner path.",
                    failed_message="Injected runtime planner path failed.",
                )
            ]

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=[planner_result, board_summary_result, board_brief_failure],
                ),
            ):
                with self.assertRaises(SkillExecutionError):
                    run_governed_execution_round_with_contract_mode(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        contract_mode="warn",
                        gate_handlers=default_runtime_gate_handlers(),
                        posture_profile=default_runtime_posture_profile_config(),
                        planning_sources=runtime_only_sources,
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
            self.assertEqual("failed", state_payload["governed_execution"]["operator"]["controller_status"])
            self.assertEqual("board-brief", state_payload["governed_execution"]["operator"]["failed_stage"])
            self.assertIn("resume-governed-execution-round", state_payload["governed_execution"]["operator"]["resume_command"])
            (run_dir / "runtime" / f"round_controller_{ROUND_ID}.json").unlink()

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.controller.planning_bundle") as planning_bundle_mock,
                mock.patch("eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=[board_brief_result, next_actions_result, readiness_result, report_basis_result],
                ) as run_skill_mock,
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="warn",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                    planning_sources=runtime_only_sources,
                )

            planning_bundle_mock.assert_not_called()
            self.assertEqual(
                ["materialize-board-brief", "propose-next-actions", "summarize-round-readiness", "freeze-report-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual("completed", payload["controller"]["controller_status"])
            self.assertEqual("resumed", payload["controller"]["resume_status"])
            self.assertEqual("frozen", payload["controller"]["report_basis_status"])
            self.assertFalse(payload["controller"]["resume_recommended"])
            self.assertEqual(
                ["--transition-request-id", report_basis_request_id],
                run_skill_mock.call_args_list[-1].kwargs["skill_args"],
            )

    def test_controller_respects_injected_planning_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.controller import run_governed_execution_round_with_contract_mode
            from eco_council_runtime.kernel.execution.runtime_planning_profile import runtime_planning_source

            planner_result = {
                "summary": {"skill_name": "plan-round-orchestration", "event_id": "evt-plan", "receipt_id": "receipt-plan"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": []},
            }
            planning = {
                "plan_id": "plan-injected-runtime-only-001",
                "plan_path": str(root / "plan.json"),
                "planning_status": "ready-for-controller",
                "planning_mode": "planner-backed",
                "planner_skill_name": "plan-round-orchestration",
                "execution_queue": [
                    {
                        "stage_name": "round-readiness",
                        "skill_name": "summarize-round-readiness",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Injected planning sources force runtime planner.",
                        "expected_output_path": str(root / "readiness.json"),
                    }
                ],
                "post_gate_steps": [
                    {
                        "stage_name": "report-basis-freeze",
                        "skill_name": "freeze-report-basis",
                        "skill_args": [],
                        "assigned_role_hint": "moderator",
                        "reason": "Freeze the runtime-only planner result.",
                        "expected_output_path": str(root / "basis.json"),
                    }
                ],
                "fallback_path": [],
            }
            readiness_result = {
                "summary": {"skill_name": "summarize-round-readiness", "event_id": "evt-ready", "receipt_id": "receipt-ready"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "readiness.json"), "readiness_status": "ready"},
                },
            }
            report_basis_result = {
                "summary": {"skill_name": "freeze-report-basis", "event_id": "evt-promo", "receipt_id": "receipt-promo"},
                "event": {"status": "completed"},
                "skill_payload": {
                    "artifact_refs": [],
                    "canonical_ids": [],
                    "summary": {"output_path": str(root / "basis.json"), "report_basis_status": "frozen"},
                },
            }
            gate_payload = {
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "gate_status": "report-basis-freeze-allowed",
                "readiness_status": "ready",
                "report_basis_freeze_allowed": True,
                "output_path": str(root / "report_basis_gate.json"),
                "gate_reasons": [],
                "recommended_next_skills": [],
            }
            runtime_only_sources = [
                runtime_planning_source(
                    "runtime-planner-only",
                    source_kind="planner-skill",
                    output_path_key="orchestration_plan_path",
                    planner_skill_name="plan-round-orchestration",
                    materialized_message="Use only the injected runtime planner path.",
                    failed_message="Injected runtime planner path failed.",
                )
            ]
            report_basis_request_id = approve_report_basis_transition(run_dir)

            with (
                mock.patch("eco_council_runtime.kernel.execution.controller.write_registry"),
                mock.patch("eco_council_runtime.kernel.execution.controller.planning_bundle", return_value=planning),
                mock.patch("eco_council_runtime.kernel.execution.runtime_gate_handlers.apply_report_basis_gate", return_value=gate_payload),
                mock.patch(
                    "eco_council_runtime.kernel.execution.controller.run_skill",
                    side_effect=[planner_result, readiness_result, report_basis_result],
                ) as run_skill_mock,
            ):
                payload = run_governed_execution_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="strict",
                    gate_handlers=default_runtime_gate_handlers(),
                    posture_profile=default_runtime_posture_profile_config(),
                    planning_sources=runtime_only_sources,
                )

            self.assertEqual(
                ["plan-round-orchestration", "summarize-round-readiness", "freeze-report-basis"],
                [call.kwargs["skill_name"] for call in run_skill_mock.call_args_list],
            )
            self.assertEqual([], run_skill_mock.call_args_list[0].kwargs["skill_args"])
            self.assertEqual(
                ["--transition-request-id", report_basis_request_id],
                run_skill_mock.call_args_list[-1].kwargs["skill_args"],
            )
            self.assertEqual("runtime-planner", payload["controller"]["planning"]["plan_source"])
            self.assertEqual(
                [{"source": "runtime-planner-only", "status": "materialized"}],
                [
                    {"source": item["source"], "status": item["status"]}
                    for item in payload["controller"]["planning_attempts"]
                ],
            )

    def test_show_run_state_uses_deliberation_control_snapshots_when_governed_execution_json_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.planes.deliberation_plane import store_runtime_control_freeze_record

            controller_path = run_dir / "runtime" / f"round_controller_{ROUND_ID}.json"
            gate_path = run_dir / "runtime" / f"report_basis_gate_{ROUND_ID}.json"
            supervisor_path = run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json"
            controller_snapshot = {
                "schema_version": "runtime-controller-v3",
                "generated_at_utc": "2024-01-01T00:00:00Z",
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "controller_status": "completed",
                "planning_mode": "planner-backed",
                "readiness_status": "ready",
                "gate_status": "report-basis-freeze-allowed",
                "report_basis_status": "frozen",
                "resume_status": "fresh-run",
                "current_stage": "",
                "failed_stage": "",
                "completed_stage_names": ["orchestration-planner", "next-actions", "round-readiness", "report-basis-gate", "report-basis-freeze"],
                "pending_stage_names": [],
                "resume_recommended": False,
                "restart_recommended": False,
                "recovery": {"resume_from_stage": ""},
                "gate_reasons": [],
                "recommended_next_skills": ["materialize-reporting-handoff"],
                "planning": {"plan_path": str((run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve())},
                "steps": [],
                "artifacts": {
                    "controller_state_path": str(controller_path.resolve()),
                    "report_basis_gate_path": str(gate_path.resolve()),
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
                "report_basis_freeze_allowed": True,
                "gate_status": "report-basis-freeze-allowed",
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
                "supervisor_substatus": "report-basis-complete",
                "governed_execution_posture": "reporting-ready",
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
                "gate_status": "report-basis-freeze-allowed",
                "report_basis_status": "frozen",
                "planning_mode": "planner-backed",
                "report_basis_gate_path": str(gate_path.resolve()),
                "controller_path": str(controller_path.resolve()),
                "recommended_next_skills": ["materialize-reporting-handoff"],
                "round_transition": {},
                "operator_notes": ["Report basis freeze succeeded and the evidence basis is now ready for downstream reporting."],
                "inspection_paths": {
                    "controller_path": str(controller_path.resolve()),
                    "plan_path": str((run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve()),
                    "gate_path": str(gate_path.resolve()),
                    "report_basis_freeze_path": str((run_dir / "report_basis" / f"frozen_report_basis_{ROUND_ID}.json").resolve()),
                },
            }

            store_runtime_control_freeze_record(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                controller_snapshot=controller_snapshot,
                gate_snapshot=gate_snapshot,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths={
                    "controller_state_path": str(controller_path.resolve()),
                    "report_basis_gate_path": str(gate_path.resolve()),
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

            self.assertEqual("completed", state_payload["governed_execution"]["operator"]["controller_status"])
            self.assertEqual("reporting-ready", state_payload["governed_execution"]["operator"]["supervisor_status"])
            self.assertEqual("report-basis-freeze-allowed", state_payload["governed_execution"]["operator"]["gate_status"])
            self.assertEqual("frozen", state_payload["governed_execution"]["operator"]["report_basis_status"])
            self.assertTrue(state_payload["governed_execution"]["operator"]["reporting_ready"])
            self.assertEqual(
                "reporting-ready",
                state_payload["governed_execution"]["operator"]["reporting_handoff_status"],
            )
            self.assertIn(
                "show-reporting-state",
                state_payload["governed_execution"]["operator"]["show_reporting_state_command"],
            )
            self.assertIn(
                "query-control-objects",
                state_payload["governed_execution"]["operator"]["query_controller_state_command"],
            )
            self.assertIn(
                "--object-kind gate-state",
                state_payload["governed_execution"]["operator"]["query_gate_state_command"],
            )
            self.assertIn(
                "--object-kind supervisor-state",
                state_payload["governed_execution"]["operator"]["query_supervisor_state_command"],
            )
            self.assertIn(
                "--object-kind runtime-control-freeze",
                state_payload["governed_execution"]["operator"]["query_runtime_control_freeze_command"],
            )
            self.assertIn(
                "--object-kind report-basis-freeze",
                state_payload["governed_execution"]["operator"]["query_report_basis_freeze_command"],
            )
            self.assertIn(
                "--readiness-blocker-only",
                state_payload["governed_execution"]["operator"]["query_readiness_blockers_command"],
            )
            self.assertTrue(state_payload["reporting"]["surface"]["reporting_ready"])
            self.assertEqual(
                "supervisor",
                state_payload["reporting"]["surface"]["surface_source"],
            )
            self.assertEqual(str(controller_path.resolve()), state_payload["governed_execution"]["operator"]["inspection_paths"]["controller_path"])
            self.assertEqual(str(supervisor_path.resolve()), state_payload["governed_execution"]["operator"]["inspection_paths"]["supervisor_path"])

    def test_supervisor_forwards_execution_policy_and_records_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.supervisor import supervise_round_with_contract_mode

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
                    "gate_status": "report-basis-ready",
                    "report_basis_status": "frozen",
                    "recommended_next_skills": ["materialize-reporting-handoff"],
                    "gate_reasons": [],
                    "artifacts": {
                        "next_actions_path": "",
                        "orchestration_plan_path": str(root / "plan.json"),
                        "controller_state_path": str(root / "controller.json"),
                        "report_basis_gate_path": str(root / "gate.json"),
                        "report_basis_freeze_path": str(root / "basis.json"),
                    },
                }
            }

            with mock.patch(
                "eco_council_runtime.kernel.execution.supervisor.run_governed_execution_round_with_contract_mode",
                return_value=controller_result,
            ) as controller_mock:
                payload = supervise_round_with_contract_mode(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                    contract_mode="warn",
                    posture_profile=default_runtime_posture_profile_config(),
                    timeout_seconds=8.0,
                    retry_budget=1,
                    retry_backoff_ms=40,
                    allow_side_effects=["destructive-write"],
                )

            controller_mock.assert_called_once_with(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                actor_role="runtime-operator",
                contract_mode="warn",
                gate_handlers=None,
                posture_profile=mock.ANY,
                timeout_seconds=8.0,
                retry_budget=1,
                retry_backoff_ms=40,
                allow_side_effects=["destructive-write"],
            )
            self.assertEqual(8.0, payload["supervisor"]["execution_policy"]["timeout_seconds"])
            self.assertEqual(1, payload["supervisor"]["execution_policy"]["retry_budget"])
            self.assertEqual(["destructive-write"], payload["supervisor"]["execution_policy"]["allow_side_effects"])
            self.assertEqual("reporting-ready", payload["supervisor"]["governed_execution_posture"])
            self.assertEqual("handoff-reporting", payload["supervisor"]["operator_action"])
            self.assertIn("resume-governed-execution-round", payload["supervisor"]["resume_command"])

    def test_supervisor_respects_injected_posture_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.supervisor import supervise_round_with_contract_mode

            posture_profile = default_runtime_posture_profile_config()
            posture_profile["supervisor_classification_builder"] = (
                lambda controller: {
                    "supervisor_status": "custom-supervisor-status",
                    "supervisor_substatus": "custom-substatus",
                    "governed_execution_posture": "custom-posture",
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
                    "skill_name": "custom-round-transition",
                    "source_round_id": kwargs["round_id"],
                    "suggested_round_id": f"{kwargs['round_id']}-custom",
                    "command": "custom-round-transition --injected",
                }
            )
            posture_profile["supervisor_recommended_skills_builder"] = (
                lambda **kwargs: [
                    "custom-round-transition",
                    "custom-follow-up",
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
                    "gate_status": "report-basis-freeze-withheld",
                    "report_basis_status": "withheld",
                    "recommended_next_skills": ["kernel-default-follow-up"],
                    "gate_reasons": ["This should be ignored by the injected operator notes builder."],
                    "artifacts": {
                        "next_actions_path": "",
                        "orchestration_plan_path": str(root / "plan.json"),
                        "controller_state_path": str(root / "controller.json"),
                        "report_basis_gate_path": str(root / "gate.json"),
                        "report_basis_freeze_path": str(root / "basis.json"),
                    },
                }
            }

            with mock.patch(
                "eco_council_runtime.kernel.execution.supervisor.run_governed_execution_round_with_contract_mode",
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
                actor_role="runtime-operator",
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
                ["custom-round-transition", "custom-follow-up"],
                payload["supervisor"]["recommended_next_skills"],
            )
            self.assertEqual(
                "custom-round-transition",
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

            from eco_council_runtime.kernel.execution.executor import SkillExecutionError
            from eco_council_runtime.kernel.execution.supervisor import supervise_round_with_contract_mode

            controller_failure = SkillExecutionError(
                "governed-execution failed",
                {
                    "status": "failed",
                    "message": "governed-execution failed",
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
                        "report_basis_status": "not-evaluated",
                        "recommended_next_skills": ["materialize-board-brief"],
                        "artifacts": {
                            "orchestration_plan_path": str(root / "plan.json"),
                            "controller_state_path": str(root / "controller.json"),
                            "report_basis_gate_path": str(root / "gate.json"),
                        },
                    },
                },
            )

            with mock.patch(
                "eco_council_runtime.kernel.execution.supervisor.run_governed_execution_round_with_contract_mode",
                side_effect=controller_failure,
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    supervise_round_with_contract_mode(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        contract_mode="warn",
                        posture_profile=default_runtime_posture_profile_config(),
                    )

            supervisor_artifact = load_json(run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json")
            self.assertEqual("controller-failed", supervisor_artifact["supervisor_status"])
            self.assertEqual("board-brief", supervisor_artifact["failed_stage"])
            self.assertTrue(supervisor_artifact["resume_recommended"])
            self.assertIn("resume-governed-execution-round", supervisor_artifact["resume_command"])
            self.assertEqual("controller-failed", raised.exception.payload["supervisor"]["supervisor_status"])

    def test_close_round_blocks_on_archive_failure_by_default_and_persists_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.execution.executor import SkillExecutionError
            from eco_council_runtime.kernel.planes.deliberation_plane import (
                store_runtime_control_freeze_record,
            )
            from eco_council_runtime.kernel.archive.post_round import close_round_with_contract_mode

            runtime_dir = run_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            supervisor_path = runtime_dir / f"supervisor_state_{ROUND_ID}.json"
            supervisor_snapshot = {
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "supervisor_status": "reporting-ready",
                "readiness_status": "ready",
                "report_basis_status": "frozen",
                "reporting_ready": True,
                "reporting_blockers": [],
                "reporting_handoff_status": "reporting-ready",
                "supervisor_path": str(supervisor_path.resolve()),
            }
            supervisor_path.write_text(
                json.dumps(supervisor_snapshot, ensure_ascii=True),
                encoding="utf-8",
            )
            store_runtime_control_freeze_record(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                supervisor_snapshot=supervisor_snapshot,
                artifact_paths={
                    "supervisor_state_path": str(supervisor_path.resolve()),
                },
            )

            signal_archive_result = {
                "summary": {"skill_name": "archive-signal-corpus", "event_id": "evt-archive-signal", "receipt_id": "receipt-archive-signal"},
                "event": {"status": "completed"},
                "skill_payload": {"artifact_refs": [], "canonical_ids": [], "summary": {"output_path": str(root / "signal_archive.json")}},
            }
            case_archive_failure = SkillExecutionError(
                "case archive failed",
                {
                    "status": "failed",
                    "message": "case archive failed",
                    "summary": {"skill_name": "archive-case-library", "run_id": RUN_ID, "round_id": ROUND_ID},
                    "failure": {"error_code": "skill-exit-nonzero", "retryable": False},
                },
            )
            close_request_id = approve_close_round_transition(run_dir)

            with (
                mock.patch("eco_council_runtime.kernel.archive.post_round.close.write_registry"),
                mock.patch(
                    "eco_council_runtime.kernel.archive.post_round.close.run_skill",
                    side_effect=[signal_archive_result, case_archive_failure],
                ),
            ):
                with self.assertRaises(SkillExecutionError) as raised:
                    close_round_with_contract_mode(
                        run_dir,
                        run_id=RUN_ID,
                        round_id=ROUND_ID,
                        transition_request_id=close_request_id,
                        contract_mode="warn",
                    )

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
                mock.patch("eco_council_runtime.kernel.operator.cli_runtime_commands.init_run", return_value={"status": "completed"}),
                mock.patch(
                    "eco_council_runtime.kernel.operator.cli_runtime_commands.run_skill",
                    return_value={"status": "completed", "summary": {"skill_name": "post-board-note"}},
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
                        "post-board-note",
                        "--timeout-seconds",
                        "9",
                        "--retry-budget",
                        "2",
                        "--retry-backoff-ms",
                        "50",
                        "--actor-role",
                        "moderator",
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
            self.assertEqual("moderator", run_skill_mock.call_args.kwargs["actor_role"])
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
                        "--transition-request-id",
                        "transition-request-test",
                        "--actor-role",
                        "runtime-operator",
                        "--archive-failure-policy",
                        "warn",
                        "--timeout-seconds",
                        "6",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("runtime-operator", close_mock.call_args.kwargs["actor_role"])
            self.assertEqual(
                "transition-request-test",
                close_mock.call_args.kwargs["transition_request_id"],
            )
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
                        "--actor-role",
                        "runtime-operator",
                        "--retry-budget",
                        "2",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("runtime-operator", history_mock.call_args.kwargs["actor_role"])
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
                        "--actor-role",
                        "runtime-operator",
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
                        "--actor-role",
                        "runtime-operator",
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
                        "--actor-role",
                        "runtime-operator",
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
                        "--actor-role",
                        "runtime-operator",
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

    def test_cli_resume_and_restart_governed_execution_round_forward_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            ensure_runtime_src_on_path()

            from eco_council_runtime.kernel.cli import main

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.run_governed_execution_round_with_contract_mode",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as controller_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "resume-governed-execution-round",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--actor-role",
                        "runtime-operator",
                        "--timeout-seconds",
                        "7",
                        "--retry-budget",
                        "1",
                    ],
                    default_posture_profile=default_runtime_posture_profile_config(),
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("runtime-operator", controller_mock.call_args.kwargs["actor_role"])
            self.assertFalse(controller_mock.call_args.kwargs["force_restart"])
            self.assertEqual(7.0, controller_mock.call_args.kwargs["timeout_seconds"])
            self.assertEqual(1, controller_mock.call_args.kwargs["retry_budget"])
            self.assertEqual("completed", json.loads(stdout.getvalue())["status"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.cli.run_governed_execution_round_with_contract_mode",
                    return_value={"status": "completed", "summary": {"round_id": ROUND_ID}},
                ) as controller_mock,
                redirect_stdout(stdout),
            ):
                exit_code = main(
                    [
                        "restart-governed-execution-round",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--actor-role",
                        "runtime-operator",
                    ],
                    default_posture_profile=default_runtime_posture_profile_config(),
                )

            self.assertEqual(0, exit_code)
            self.assertEqual("runtime-operator", controller_mock.call_args.kwargs["actor_role"])
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
                    "eco_council_runtime.kernel.operator.cli_runtime_commands.materialize_admission_policy",
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
                        "--actor-role",
                        "runtime-operator",
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
                    "eco_council_runtime.kernel.operator.cli_runtime_commands.materialize_runtime_health",
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
                        "--actor-role",
                        "runtime-operator",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(ROUND_ID, health_mock.call_args.kwargs["round_id"])
            self.assertEqual("runtime-health-v1", json.loads(stdout.getvalue())["schema_version"])

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.operator.cli_runtime_commands.materialize_operator_runbook",
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
                        "--actor-role",
                        "runtime-operator",
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertEqual(ROUND_ID, runbook_mock.call_args.kwargs["round_id"])
            self.assertTrue(json.loads(stdout.getvalue())["operator_runbook_path"].endswith("operator_runbook.md"))

            stdout = io.StringIO()
            with (
                mock.patch(
                    "eco_council_runtime.kernel.operator.cli_runtime_commands.load_dead_letters",
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

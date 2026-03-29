from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from _workflow_support import kernel_script_path, load_json, run_kernel, run_kernel_process, run_script, runtime_src_path, script_path, seed_analysis_chain

RUN_ID = "run-kernel-001"
ROUND_ID = "round-kernel-001"


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
            runtime_src = runtime_src_path()
            if str(runtime_src) not in sys.path:
                sys.path.insert(0, str(runtime_src))

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


if __name__ == "__main__":
    unittest.main()
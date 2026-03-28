from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import kernel_script_path, load_json, run_kernel, run_script, script_path, seed_analysis_chain

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

            self.assertEqual("runtime-event-v2", event["schema_version"])
            self.assertEqual(["--author-role", "moderator", "--note-text", "Runtime metadata note."], event["skill_args"])
            self.assertEqual("eco-post-board-note", event["skill_registry_entry"]["skill_name"])
            self.assertIn(str((run_dir / "board" / f"investigation_board.json").resolve()), event["resolved_write_paths"])
            self.assertIn("argv", event["command_snapshot"])
            self.assertTrue(event["execution_input_hash"])
            self.assertTrue(event["payload_hash"])


if __name__ == "__main__":
    unittest.main()
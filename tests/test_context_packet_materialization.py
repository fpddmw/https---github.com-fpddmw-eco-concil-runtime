from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.governance.role_contracts import role_capabilities  # noqa: E402
from eco_council_runtime.kernel.governance.skill_registry import resolve_skill_policy  # noqa: E402

RUN_ID = "run-context-packet-materialization-001"
ROUND_ID = "round-context-packet-materialization-001"


class ContextPacketMaterializationTests(unittest.TestCase):
    def test_materialize_context_packet_writes_refs_only_delta_packet(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            plan = run_script(
                script_path("submit-investigation-plan"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--mission-ref",
                "mission://context-packet-test",
                "--rationale",
                "Frame the round without controlling source uptake.",
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            position = run_script(
                script_path("submit-agent-position"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "social-investigator",
                "--target-kind",
                "investigation-plan",
                "--target-id",
                plan["canonical_ids"][0],
                "--claim-summary",
                "A social evidence path is available for inspection.",
                "--rationale",
                "The agent records a provisional position, not a conclusion.",
                "--evidence-ref",
                "evidence://position/context-001",
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )

            first_packet = run_script(
                script_path("materialize-context-packet"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--object-id",
                "context-packet-001",
                "--packet-profile",
                "investigation",
                "--target-ref",
                "investigation-plan:" + plan["canonical_ids"][0],
                "--source-ref",
                "mission://context-packet-test",
                "--rationale",
                "Expose refs-only context for the next agent turn.",
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            first = first_packet["context_packet"]
            included_refs = set(first["included_object_refs"])
            delta_refs = set(first["delta_refs"])

            self.assertEqual("context-packet-001", first_packet["canonical_ids"][0])
            self.assertEqual("refs-only", first["raw_data_policy"])
            self.assertIn("raw-records:*", first["excluded_object_refs"])
            self.assertIn(
                "investigation-plan:" + plan["canonical_ids"][0],
                included_refs,
            )
            self.assertIn(
                "agent-position:" + position["canonical_ids"][0],
                included_refs,
            )
            self.assertIn("evidence://position/context-001", first["evidence_refs"])
            self.assertTrue(included_refs.issubset(delta_refs))
            self.assertFalse(
                any(ref.startswith("raw-record") for ref in first["included_object_refs"])
            )
            packet_text = json.dumps(first, ensure_ascii=True, sort_keys=True)
            self.assertNotIn('"score"', packet_text)
            self.assertNotIn('"weight"', packet_text)
            self.assertNotIn('"rank"', packet_text)

            request = run_script(
                script_path("submit-evidence-request"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "challenger",
                "--question",
                "Which sources would contradict the provisional position?",
                "--rationale",
                "Ask for contradiction evidence without ranking sources.",
                "--target-kind",
                "agent-position",
                "--target-id",
                position["canonical_ids"][0],
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            second_packet = run_script(
                script_path("materialize-context-packet"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--object-id",
                "context-packet-002",
                "--packet-profile",
                "supplemental",
                "--target-ref",
                "agent-position:" + position["canonical_ids"][0],
                "--rationale",
                "Expose only refs and deltas for supplemental investigation.",
            )
            second = second_packet["context_packet"]

            self.assertIn(
                "evidence-request:" + request["canonical_ids"][0],
                second["delta_refs"],
            )
            self.assertNotIn(
                "investigation-plan:" + plan["canonical_ids"][0],
                second["delta_refs"],
            )
            self.assertIn(
                "no salience ranking",
                second["ordering_semantics"],
            )

    def test_materialize_context_packet_rejects_raw_included_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script_path("materialize-context-packet")),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--include-object-ref",
                    "raw-records:provider-row-001",
                    "--rationale",
                    "This should fail because context packets are refs-only.",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            payload = json.loads(completed.stdout)

            self.assertNotEqual(0, completed.returncode)
            self.assertEqual("failed", payload["status"])
            self.assertIn("raw records", payload["message"])

    def test_materialize_context_packet_policy_matches_role_capabilities(self) -> None:
        policy = resolve_skill_policy("materialize-context-packet")
        required = set(policy["required_capabilities"])

        self.assertEqual("deliberation-write", policy["skill_layer"])
        self.assertEqual(["moderator"], policy["allowed_roles"])
        self.assertEqual(["context-packet"], policy["output_object_kinds"])
        for role in policy["allowed_roles"]:
            self.assertTrue(required.issubset(role_capabilities(role)))
        self.assertEqual({}, policy["skill_boundary"])


if __name__ == "__main__":
    unittest.main()

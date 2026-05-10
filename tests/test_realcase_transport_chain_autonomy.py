from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import (
    request_and_approve_transition,
    run_kernel,
    run_script,
    script_path,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "openclaw-realcase-nyc-smoke-transport-chain-20230607"
ROUND_ID = "round-001"
RUN_DIR = REPO_ROOT / "runs" / RUN_ID


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def all_keys(value: Any) -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            keys.add(str(key))
            keys.update(all_keys(child))
    elif isinstance(value, list):
        for child in value:
            keys.update(all_keys(child))
    return keys


class RealcaseTransportChainAutonomyTests(unittest.TestCase):
    def setUp(self) -> None:
        if not RUN_DIR.exists():
            self.skipTest(f"realcase run fixture is not present: {RUN_DIR}")

    def test_mission_does_not_seed_source_region_or_runtime_source_choice(self) -> None:
        mission = load_json(RUN_DIR / "input" / "mission.json")
        mission_text = json.dumps(mission, ensure_ascii=True).lower()

        self.assertNotIn("canada", mission_text)
        self.assertNotIn("quebec", mission_text)
        self.assertEqual([], mission.get("source_requests", []))
        self.assertEqual({}, mission.get("source_selections", {}))

    def test_source_surface_stays_capability_based_when_fetch_plan_is_empty(self) -> None:
        surface = run_kernel(
            "show-source-surfaces",
            "--run-dir",
            str(RUN_DIR),
            "--run-id",
            RUN_ID,
            "--round-id",
            ROUND_ID,
            "--limit",
            "200",
        )

        self.assertEqual("source-surfaces", surface["surface"])
        self.assertEqual(0, surface["fetch_plan"]["step_count"])
        catalog = {entry["source_skill"]: entry for entry in surface["catalog"]}
        self.assertIn("fetch-gdelt-doc-search", catalog)
        self.assertIn("fetch-nasa-firms-fire", catalog)
        self.assertIn("fetch-open-meteo-historical", catalog)
        self.assertTrue(catalog["fetch-nasa-firms-fire"]["provider_modes"])
        self.assertTrue(catalog["fetch-nasa-firms-fire"]["fetch_argument_templates"])
        self.assertFalse({"score", "rank", "weight", "priority"} & all_keys(surface))

    def test_liveness_drives_continuation_without_challenge_only_gate(self) -> None:
        status = run_kernel(
            "show-council-status",
            "--run-dir",
            str(RUN_DIR),
            "--run-id",
            RUN_ID,
            "--round-id",
            ROUND_ID,
        )
        liveness = status["round_liveness"]
        counts = liveness["counts"]

        self.assertEqual("unresolved-refs-present", status["summary"]["liveness_status"])
        self.assertEqual("unresolved-refs-present", liveness["liveness_status"])
        self.assertEqual("unresolved-refs-present", liveness["continuation"]["status"])
        self.assertGreaterEqual(counts["open_evidence_requests_count"], 1)
        self.assertGreaterEqual(counts["unbundled_findings_count"], 1)
        self.assertGreaterEqual(counts["open_challenges_count"], 1)
        self.assertIn("request-phase-transition", liveness["continuation"]["request_open_round_command_template"])
        self.assertIn("open-investigation-round", liveness["continuation"]["request_open_round_command_template"])
        self.assertIn("primary_focus_refs", liveness["continuation"]["request_open_round_command_template"])
        self.assertFalse({"score", "rank", "weight", "priority"} & all_keys(status))

    def test_archive_status_exposes_checkpoint_gap_and_commands(self) -> None:
        archive = run_kernel(
            "show-archive-status",
            "--run-dir",
            str(RUN_DIR),
            "--run-id",
            RUN_ID,
            "--round-id",
            ROUND_ID,
        )

        self.assertEqual("archive-status", archive["surface"])
        self.assertTrue(archive["gap_hints"])
        self.assertIn("archive-signal-corpus", archive["commands"]["archive_signal_corpus_checkpoint"])
        self.assertIn("archive-case-library", archive["commands"]["archive_case_library_checkpoint"])
        self.assertIn("materialize-history-context", archive["commands"]["materialize_history_context"])

    def test_realcase_round_001_can_open_round_002_with_unresolved_refs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            working_run_dir = Path(tmpdir) / RUN_ID
            shutil.copytree(RUN_DIR, working_run_dir)

            target_round_id = "round-002"
            focus_refs = [
                "finding:finding-38401351d52e",
                "challenge:challenge-38c1d2bb31df",
                "evidence-request:evidence-request-dfe196ae4634",
            ]
            transition_request_id = request_and_approve_transition(
                working_run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="open-investigation-round",
                target_round_id=target_round_id,
                source_round_id=ROUND_ID,
                rationale="Open continuation round from realcase unresolved refs.",
                request_payload={
                    "round_mode": "continuation",
                    "primary_focus_refs": focus_refs,
                    "continuation_basis": "moderator-selected unresolved refs",
                },
            )

            payload = run_script(
                script_path("open-investigation-round"),
                "--run-dir",
                str(working_run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                target_round_id,
                "--source-round-id",
                ROUND_ID,
                "--transition-request-id",
                transition_request_id,
                "--round-mode",
                "continuation",
                "--primary-focus-ref",
                focus_refs[0],
                "--primary-focus-ref",
                focus_refs[1],
            )

            transition_artifact = load_json(working_run_dir / "runtime" / f"round_transition_{target_round_id}.json")
            round2_tasks = json.loads(
                (working_run_dir / "investigation" / f"round_tasks_{target_round_id}.json").read_text(
                    encoding="utf-8"
                )
            )
            task_context = round2_tasks[0]["inputs"]["round_coordination_context"]

            self.assertEqual("completed", payload["status"])
            self.assertEqual(target_round_id, payload["summary"]["round_id"])
            self.assertEqual("continuation", transition_artifact["round_mode"])
            self.assertEqual(ROUND_ID, transition_artifact["source_round_id"])
            self.assertEqual(focus_refs, transition_artifact["primary_focus_refs"])
            self.assertEqual(focus_refs, task_context["primary_focus_refs"])
            self.assertIn("does not restrict", task_context["semantics"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_script, runtime_path, script_path, seed_analysis_chain

RUN_ID = "run-planner-001"
ROUND_ID = "round-planner-001"


def prepare_ready_board_state(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    run_script(script_path("eco-derive-claim-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-derive-observation-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_payload = run_script(script_path("eco-score-evidence-coverage"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
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
        "--category",
        "analysis",
        "--note-text",
        "Round is organized enough for planner-backed promotion.",
        "--linked-artifact-ref",
        coverage_ref,
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
        "Smoke over NYC was materially significant",
        "--statement",
        "Public smoke reports are backed by elevated PM2.5 observations.",
        "--status",
        "active",
        "--owner-role",
        "environmentalist",
        "--linked-claim-id",
        outputs["cluster_claims"]["canonical_ids"][0],
        "--confidence",
        "0.91",
    )


def prepare_hold_board_state(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    run_script(script_path("eco-derive-claim-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-derive-observation-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_payload = run_script(script_path("eco-score-evidence-coverage"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
    hypothesis_payload = run_script(
        script_path("eco-update-hypothesis-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        "Smoke over NYC may be overstated",
        "--statement",
        "Public reports may overstate severity relative to observed PM2.5 coverage.",
        "--status",
        "active",
        "--owner-role",
        "moderator",
        "--linked-claim-id",
        outputs["cluster_claims"]["canonical_ids"][0],
        "--confidence",
        "0.52",
    )
    run_script(
        script_path("eco-open-challenge-ticket"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        "Check whether smoke narrative is overstated",
        "--challenge-statement",
        "Re-test whether the strongest narrative exceeds evidence coverage.",
        "--target-claim-id",
        outputs["cluster_claims"]["canonical_ids"][0],
        "--target-hypothesis-id",
        hypothesis_payload["canonical_ids"][0],
        "--priority",
        "high",
        "--owner-role",
        "challenger",
        "--linked-artifact-ref",
        coverage_ref,
    )


class OrchestrationPlannerWorkflowTests(unittest.TestCase):
    def test_agent_advisory_mode_marks_plan_as_advisory(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_board_state(run_dir, root)

            payload = run_script(
                script_path("eco-plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--planner-mode",
                "agent-advisory",
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))

            self.assertEqual("agent-advisory", payload["summary"]["planning_mode"])
            self.assertEqual("agent-advisory", plan["planning_mode"])
            self.assertEqual("advisory-only", plan["controller_authority"])
            self.assertIn("recommended_skill_sequence", plan["agent_turn_hints"])

    def test_ready_round_planner_skips_probe_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_board_state(run_dir, root)

            payload = run_script(script_path("eco-plan-round-orchestration"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]
            derived_export_stage_names = [item["stage_name"] for item in plan["derived_exports"]]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("planner-backed-phase2", plan["planning_mode"])
            self.assertFalse(plan["probe_stage_included"])
            self.assertEqual("promote-candidate", plan["downstream_posture"])
            self.assertEqual(["next-actions", "round-readiness"], stage_names)
            self.assertEqual(["board-summary", "board-brief"], derived_export_stage_names)
            self.assertTrue(plan["observed_state"]["board_exports_are_derived"])
            self.assertEqual(2, payload["summary"]["derived_export_count"])
            self.assertEqual("eco-promote-evidence-basis", plan["post_gate_steps"][0]["skill_name"])

    def test_hold_round_planner_keeps_probe_stage_and_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_board_state(run_dir, root)

            payload = run_script(script_path("eco-plan-round-orchestration"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]
            fallback_skill_sets = [item.get("suggested_next_skills", []) for item in plan["fallback_path"] if isinstance(item, dict)]

            self.assertEqual("completed", payload["status"])
            self.assertTrue(plan["probe_stage_included"])
            self.assertEqual("hold-investigation-open", plan["downstream_posture"])
            self.assertFalse(plan["observed_state"]["board_summary_present"])
            self.assertEqual("deliberation-plane", plan["observed_state"]["board_state_source"])
            self.assertTrue(plan["observed_state"]["board_exports_are_derived"])
            self.assertEqual("deliberation-plane", payload["summary"]["board_state_source"])
            self.assertEqual("completed", payload["deliberation_sync"]["status"])
            self.assertIn("falsification-probes", stage_names)
            self.assertNotIn("board-summary", stage_names)
            self.assertNotIn("board-brief", stage_names)
            self.assertTrue(any("eco-open-falsification-probe" in skill_set for skill_set in fallback_skill_sets))

    def test_planner_reads_db_backed_actions_and_probes_when_exports_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_board_state(run_dir, root)

            run_script(
                script_path("eco-propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            (run_dir / "investigation" / f"next_actions_{ROUND_ID}.json").unlink()
            (run_dir / "investigation" / f"falsification_probes_{ROUND_ID}.json").unlink()

            payload = run_script(
                script_path("eco-plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertTrue(plan["probe_stage_included"])
            self.assertTrue(plan["observed_state"]["next_actions_present"])
            self.assertTrue(plan["observed_state"]["probes_present"])
            self.assertEqual("deliberation-plane-actions", plan["observed_state"]["next_actions_source"])
            self.assertEqual("deliberation-plane-probes", plan["observed_state"]["probes_source"])


if __name__ == "__main__":
    unittest.main()

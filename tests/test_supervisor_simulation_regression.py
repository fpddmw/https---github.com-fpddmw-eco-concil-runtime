from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    primary_research_issue_id,
    primary_wp4_evidence_ref,
    promotion_path,
    request_and_approve_transition,
    run_kernel,
    run_script,
    runtime_path,
    script_path,
    seed_analysis_chain,
    submit_ready_council_support,
)

RUN_ID = "run-phase2-001"
ROUND_ID = "round-phase2-001"


def approve_promotion_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="promote-evidence-basis",
        rationale="Approve promotion for supervisor regression coverage.",
    )


class SupervisorSimulationRegressionTests(unittest.TestCase):
    def test_phase2_round_controller_promotes_ready_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            coverage_ref = primary_wp4_evidence_ref(outputs)
            issue_id = primary_research_issue_id(outputs)
            submit_ready_council_support(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                issue_id=issue_id,
                evidence_ref=coverage_ref,
            )

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
                "--category",
                "analysis",
                "--note-text",
                "Round is organized enough for a controller-driven promotion pass.",
                "--linked-artifact-ref",
                coverage_ref,
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
                "Smoke over NYC was materially significant",
                "--statement",
                "Public smoke reports are backed by elevated PM2.5 observations.",
                "--status",
                "active",
                "--owner-role",
                "environmentalist",
                "--linked-claim-id",
                issue_id,
                "--confidence",
                "0.91",
            )

            approve_promotion_transition(run_dir)
            phase2_payload = run_kernel(
                "run-phase2-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--tail",
                "20",
            )

            gate_artifact = load_json(runtime_path(run_dir, f"promotion_gate_{ROUND_ID}.json"))
            plan_path = runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json")
            controller_artifact = load_json(runtime_path(run_dir, f"round_controller_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertEqual("ready", phase2_payload["summary"]["readiness_status"])
            self.assertEqual("allow-promote", gate_artifact["gate_status"])
            self.assertTrue(gate_artifact["promote_allowed"])
            self.assertEqual("transition-executor", controller_artifact["planning_mode"])
            self.assertEqual("completed", controller_artifact["controller_status"])
            self.assertEqual("fresh-run", controller_artifact["resume_status"])
            self.assertFalse(plan_path.exists())
            self.assertEqual(str(plan_path.resolve()), controller_artifact["artifacts"]["orchestration_plan_path"])
            self.assertNotIn("falsification-probes", [item.get("stage") for item in controller_artifact["steps"]])
            self.assertNotIn("board-summary", [item.get("stage") for item in controller_artifact["steps"]])
            self.assertNotIn("board-brief", [item.get("stage") for item in controller_artifact["steps"]])
            self.assertEqual("promoted", controller_artifact["promotion_status"])
            self.assertEqual("promoted", promotion_artifact["promotion_status"])
            self.assertEqual("promoted", state_payload["phase2"]["controller"]["promotion_status"])
            event_types = [item.get("event_type") for item in state_payload["ledger_tail"]]
            self.assertIn("promotion-gate", event_types)
            self.assertIn("round-controller", event_types)

    def test_supervisor_entry_freezes_inflight_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            coverage_ref = primary_wp4_evidence_ref(outputs)
            issue_id = primary_research_issue_id(outputs)
            hypothesis_payload = run_script(
                script_path("update-hypothesis-status"),
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
                issue_id,
                "--confidence",
                "0.52",
            )
            run_script(
                script_path("open-challenge-ticket"),
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
                issue_id,
                "--target-hypothesis-id",
                hypothesis_payload["canonical_ids"][0],
                "--priority",
                "high",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                coverage_ref,
            )

            approve_promotion_transition(run_dir)
            supervisor_payload = run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--tail",
                "20",
            )

            gate_artifact = load_json(runtime_path(run_dir, f"promotion_gate_{ROUND_ID}.json"))
            plan_path = runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json")
            controller_artifact = load_json(runtime_path(run_dir, f"round_controller_{ROUND_ID}.json"))
            supervisor_artifact = load_json(runtime_path(run_dir, f"supervisor_state_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertEqual("hold-investigation-open", supervisor_payload["summary"]["supervisor_status"])
            self.assertEqual("freeze-withheld", gate_artifact["gate_status"])
            self.assertFalse(plan_path.exists())
            self.assertEqual("transition-executor", controller_artifact["planning_mode"])
            self.assertNotIn("falsification-probes", [item.get("stage") for item in controller_artifact["steps"]])
            self.assertNotIn("board-summary", [item.get("stage") for item in controller_artifact["steps"]])
            self.assertNotIn("board-brief", [item.get("stage") for item in controller_artifact["steps"]])
            self.assertEqual("hold-investigation-open", supervisor_artifact["supervisor_status"])
            self.assertEqual("investigation-hold", supervisor_artifact["phase2_posture"])
            self.assertEqual("continue-investigation", supervisor_artifact["operator_action"])
            self.assertEqual("withheld", supervisor_artifact["promotion_status"])
            self.assertEqual(str(plan_path.resolve()), supervisor_artifact["orchestration_plan_path"])
            self.assertEqual("withheld", promotion_artifact["promotion_status"])
            self.assertIn("open-investigation-round", supervisor_artifact["recommended_next_skills"])
            self.assertEqual("open-investigation-round", supervisor_artifact["round_transition"]["skill_name"])
            self.assertEqual("round-phase2-002", supervisor_artifact["round_transition"]["suggested_round_id"])
            self.assertEqual("hold-investigation-open", state_payload["phase2"]["supervisor"]["supervisor_status"])
            event_types = [item.get("event_type") for item in state_payload["ledger_tail"]]
            self.assertIn("supervisor", event_types)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    investigation_path,
    load_json,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    run_kernel,
    run_script,
    runtime_src_path,
    runtime_path,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
    store_readiness_opinion_records,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_moderator_action_records,
    store_moderator_action_snapshot,
)

RUN_ID = "run-planner-001"
ROUND_ID = "round-planner-001"


def prepare_ready_board_state(run_dir: Path, root: Path) -> None:
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
        "--category",
        "analysis",
        "--note-text",
        "Round is organized enough for planner-backed report_basis.",
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
        "Smoke over NYC was materially significant",
        "--statement",
        "Public smoke reports are backed by elevated PM2.5 observations.",
        "--status",
        "active",
        "--owner-role",
        "environmentalist",
        "--linked-claim-id",
        issue_id,
        "--linked-artifact-ref",
        evidence_ref,
        "--confidence",
        "0.91",
    )


def prepare_hold_board_state(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    evidence_ref = primary_successor_evidence_ref(outputs)
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
        "--linked-artifact-ref",
        evidence_ref,
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
        evidence_ref,
    )


class OrchestrationPlannerWorkflowTests(unittest.TestCase):
    def test_runtime_plan_is_queue_owned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_board_state(run_dir, root)

            payload = run_script(
                script_path("plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))

            self.assertEqual("planner-backed-phase2", payload["summary"]["planning_mode"])
            self.assertEqual("planner-backed-phase2", plan["planning_mode"])
            self.assertEqual("queue-owner", plan["controller_authority"])
            self.assertIn("recommended_skill_sequence", plan["agent_turn_hints"])

            runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json").unlink()
            plan_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "orchestration-plan",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--controller-authority",
                "queue-owner",
            )
            self.assertEqual(1, plan_query["summary"]["returned_object_count"])
            self.assertEqual(
                "queue-owner",
                plan_query["objects"][0]["controller_authority"],
            )

    def test_runtime_plan_can_skip_next_actions_from_council_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The active controversy has converged enough for report-basis review.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        },
                        {
                            "agent_role": "challenger",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "No remaining contradiction justifies another investigation round.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        },
                    ],
                },
            )

            payload = run_script(
                script_path("plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]
            gate_stage_names = [item["stage_name"] for item in plan["gate_steps"]]

            self.assertEqual("completed", payload["status"])
            self.assertEqual(["round-readiness"], stage_names)
            self.assertEqual(["report-basis-gate"], gate_stage_names)
            self.assertEqual("report-basis-candidate", plan["downstream_posture"])
            self.assertTrue(plan["observed_state"]["council_proposal_queue"])
            self.assertTrue(plan["observed_state"]["next_actions_stage_skipped"])
            self.assertEqual(2, plan["observed_state"]["council_readiness_opinion_count"])
            self.assertEqual("ready", plan["observed_state"]["council_readiness_status"])
            self.assertEqual(
                2,
                plan["phase_decision_basis"]["council_input_counts"]["readiness_ready_count"],
            )
            self.assertEqual(
                ["summarize-round-readiness"],
                plan["agent_turn_hints"]["recommended_skill_sequence"],
            )

    def test_runtime_plan_can_execute_probe_queue_from_council_proposals(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "open-probe",
                            "action_kind": "resolve-challenge",
                            "agent_role": "challenger",
                            "assigned_role": "challenger",
                            "objective": "Stress-test the contradiction around ticket-001 before any report-basis move.",
                            "rationale": "The council wants contradiction pressure applied before readiness is reconsidered.",
                            "target_kind": "challenge-ticket",
                            "target_id": "ticket-001",
                            "target_hypothesis_id": "hypothesis-001",
                            "target_claim_id": "claim-001",
                            "probe_candidate": True,
                            "controversy_gap": "unresolved-contestation",
                            "recommended_lane": "mixed-review",
                            "decision_source": "agent-council",
                            "confidence": 0.87,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://ticket-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]
            gate_stage_names = [item["stage_name"] for item in plan["gate_steps"]]

            self.assertEqual("completed", payload["status"])
            self.assertEqual(["falsification-probes", "round-readiness"], stage_names)
            self.assertEqual(["report-basis-gate"], gate_stage_names)
            self.assertTrue(plan["probe_stage_included"])
            self.assertEqual("hold-investigation-open", plan["downstream_posture"])
            self.assertTrue(plan["observed_state"]["council_proposal_queue"])
            self.assertTrue(plan["observed_state"]["next_actions_stage_skipped"])
            self.assertEqual(1, plan["observed_state"]["council_proposal_count"])
            self.assertEqual(1, plan["observed_state"]["council_proposal_action_count"])
            self.assertEqual(
                1,
                plan["phase_decision_basis"]["signal_counts"]["probe_candidate_actions"],
            )
            self.assertEqual("challenger", plan["agent_turn_hints"]["primary_role"])
            self.assertIn(
                "probe-candidate-actions",
                plan["phase_decision_basis"]["probe_stage_reason_codes"],
            )

    def test_runtime_phase2_plan_defaults_to_council_proposal_queue_when_proposals_exist(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "open-probe",
                            "action_kind": "resolve-challenge",
                            "agent_role": "challenger",
                            "assigned_role": "challenger",
                            "objective": "Stress-test the contradiction around ticket-runtime-001 before any report-basis move.",
                            "rationale": "Runtime planner should execute this proposal directly instead of recomputing fallback next actions.",
                            "target_kind": "challenge-ticket",
                            "target_id": "ticket-runtime-001",
                            "target_hypothesis_id": "hypothesis-runtime-001",
                            "target_claim_id": "claim-runtime-001",
                            "probe_candidate": True,
                            "controversy_gap": "unresolved-contestation",
                            "recommended_lane": "mixed-review",
                            "decision_source": "agent-council",
                            "confidence": 0.83,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": ["evidence://ticket-runtime-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = run_script(
                script_path("plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("planner-backed-phase2", plan["planning_mode"])
            self.assertTrue(plan["observed_state"]["council_proposal_queue"])
            self.assertTrue(plan["observed_state"]["next_actions_stage_skipped"])
            self.assertEqual(
                "proposal-authoritative",
                plan["council_execution_mode"],
            )
            self.assertEqual(
                "proposal-authoritative",
                plan["observed_state"]["council_execution_resolution"],
            )
            self.assertNotIn("next-actions", stage_names)
            self.assertEqual(
                ["falsification-probes", "round-readiness"],
                stage_names,
            )
            self.assertEqual(
                1,
                plan["phase_decision_basis"]["council_input_counts"]["selected_proposal_action_count"],
            )
            self.assertEqual(
                0,
                plan["phase_decision_basis"]["council_input_counts"]["selected_fallback_action_count"],
            )

    def test_ready_round_planner_skips_probe_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_board_state(run_dir, root)

            payload = run_script(script_path("plan-round-orchestration"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]
            derived_export_stage_names = [item["stage_name"] for item in plan["derived_exports"]]
            gate_stage_names = [item["stage_name"] for item in plan["gate_steps"]]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("planner-backed-phase2", plan["planning_mode"])
            self.assertFalse(plan["probe_stage_included"])
            self.assertEqual("report-basis-candidate", plan["downstream_posture"])
            self.assertEqual(["next-actions", "round-readiness"], stage_names)
            self.assertEqual(["report-basis-gate"], gate_stage_names)
            self.assertEqual(["board-summary", "board-brief"], derived_export_stage_names)
            self.assertTrue(plan["observed_state"]["board_exports_are_derived"])
            self.assertEqual(2, payload["summary"]["derived_export_count"])
            self.assertEqual("freeze-report-basis", plan["post_gate_steps"][0]["skill_name"])

    def test_planner_uses_agenda_diffusion_signal_to_keep_probe_stage(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_board_state(run_dir, root)

            next_actions = store_moderator_action_records(
                run_dir,
                action_snapshot={
                    "schema_version": "d1.1",
                    "skill": "propose-next-actions",
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "agenda_source": "controversy-agenda-materialization",
                    "agenda_counts": {
                        "issue_cluster_count": 1,
                        "diffusion_focus_count": 1,
                    },
                    "controversy_gap_counts": {
                        "cross-platform-diffusion": 1,
                    },
                    "ranked_actions": [
                        {
                            "action_id": "action-diffusion-001",
                            "action_kind": "trace-cross-platform-diffusion",
                            "assigned_role": "sociologist",
                            "priority": "medium",
                            "objective": "Trace how the smoke issue is moving across platforms.",
                            "reason": "Cross-platform diffusion may be changing how the controversy is represented.",
                            "controversy_gap": "cross-platform-diffusion",
                            "probe_candidate": False,
                            "recommended_lane": "public-discourse-analysis",
                        }
                    ],
                },
            )
            store_moderator_action_snapshot(run_dir, action_snapshot=next_actions)

            payload = run_script(
                script_path("plan-round-orchestration"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"))
            stage_names = [item["stage_name"] for item in plan["execution_queue"]]

            self.assertEqual("completed", payload["status"])
            self.assertTrue(plan["probe_stage_included"])
            self.assertIn("falsification-probes", stage_names)
            self.assertIn(
                "agenda-diffusion-focus",
                plan["phase_decision_basis"]["probe_stage_reason_codes"],
            )
            self.assertEqual(
                1,
                plan["phase_decision_basis"]["signal_counts"]["diffusion_focus_count"],
            )

    def test_hold_round_planner_keeps_probe_stage_and_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_board_state(run_dir, root)

            payload = run_script(script_path("plan-round-orchestration"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
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
            self.assertTrue(any("open-falsification-probe" in skill_set for skill_set in fallback_skill_sets))

    def test_planner_reads_db_backed_actions_and_probes_when_exports_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_board_state(run_dir, root)

            run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("open-falsification-probe"),
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
                script_path("plan-round-orchestration"),
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

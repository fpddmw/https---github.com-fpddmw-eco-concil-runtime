from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    investigation_path,
    load_json,
    promotion_path,
    reporting_path,
    run_kernel,
    runtime_path,
    runtime_src_path,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel import investigation_planning, phase2_state_surfaces  # noqa: E402
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_falsification_probe_records,
    store_falsification_probe_snapshot,
    store_moderator_action_records,
    store_moderator_action_snapshot,
    store_orchestration_plan_record,
    store_promotion_basis_record,
    store_promotion_freeze_record,
    store_round_readiness_assessment,
)

WRAPPER_NAMES = (
    "load_orchestration_plan_wrapper",
    "load_next_actions_wrapper",
    "load_falsification_probe_wrapper",
    "load_round_readiness_wrapper",
    "load_promotion_basis_wrapper",
    "load_supervisor_state_wrapper",
    "load_reporting_handoff_wrapper",
    "load_council_decision_wrapper",
    "load_expert_report_wrapper",
    "load_final_publication_wrapper",
)

RUN_ID = "run-phase2-surface-001"
ROUND_ID = "round-phase2-surface-001"


def fetch_raw_json(
    db_path: Path,
    query: str,
    params: tuple[str, ...],
) -> dict[str, object]:
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(query, params).fetchone()
    finally:
        connection.close()
    assert row is not None
    payload = json.loads(row[0])
    assert isinstance(payload, dict)
    return payload


def execute_db(
    db_path: Path,
    query: str,
    params: tuple[str, ...],
) -> None:
    connection = sqlite3.connect(db_path)
    try:
        with connection:
            connection.execute(query, params)
    finally:
        connection.close()


def seed_phase2_surface_state(run_dir: Path) -> dict[str, dict[str, object]]:
    next_actions = store_moderator_action_records(
        run_dir,
        action_snapshot={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "generated_at_utc": "2024-01-01T00:00:00Z",
            "ranked_actions": [
                {
                    "action_kind": "advance-empirical-verification",
                    "priority": "high",
                    "assigned_role": "environmentalist",
                    "objective": "Advance smoke verification.",
                    "reason": "Coverage is still incomplete.",
                    "readiness_blocker": True,
                    "decision_source": "agent-council",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": ["artifact:coverage-001"],
                    "lineage": ["proposal-001"],
                    "source_ids": ["issue-001"],
                    "target": {"claim_id": "claim-001"},
                }
            ],
        },
    )
    store_moderator_action_snapshot(
        run_dir,
        action_snapshot=next_actions,
    )
    probes = store_falsification_probe_records(
        run_dir,
        probe_snapshot={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "generated_at_utc": "2024-01-01T00:05:00Z",
            "probes": [
                {
                    "probe_type": "contradiction-check",
                    "probe_status": "open",
                    "owner_role": "challenger",
                    "priority": "high",
                    "probe_goal": "Test the strongest smoke claim.",
                    "falsification_question": "Do observations contradict the public smoke narrative?",
                    "decision_source": "agent-council",
                    "provenance": {"source": "unit-test"},
                    "evidence_refs": ["artifact:coverage-001"],
                    "lineage": ["proposal-001"],
                }
            ],
        },
    )
    store_falsification_probe_snapshot(
        run_dir,
        probe_snapshot=probes,
    )
    readiness = store_round_readiness_assessment(
        run_dir,
        readiness_payload={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "generated_at_utc": "2024-01-01T00:10:00Z",
            "readiness_status": "needs-more-data",
            "sufficient_for_promotion": False,
            "decision_source": "agent-council",
            "provenance": {"source": "unit-test"},
            "evidence_refs": ["artifact:coverage-001"],
            "lineage": ["proposal-001"],
            "agenda_counts": {"issue_cluster_count": 1},
            "counts": {"open_challenges": 1},
            "controversy_gap_counts": {"representation-gap": 1},
            "gate_reasons": ["An open contradiction probe remains."],
            "readiness_source": "deliberation-plane-readiness",
        },
    )
    promotion = store_promotion_basis_record(
        run_dir,
        promotion_payload={
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "generated_at_utc": "2024-01-01T00:12:00Z",
            "promotion_status": "withheld",
            "readiness_status": "needs-more-data",
            "decision_source": "agent-council",
            "provenance": {"source": "unit-test"},
            "evidence_refs": ["artifact:coverage-001"],
            "lineage": ["proposal-001"],
            "selected_basis_object_ids": ["issue-001"],
            "selected_evidence_refs": ["artifact:coverage-001"],
            "frozen_basis": {
                "issue_clusters": [
                    {
                        "map_issue_id": "issue-001",
                        "issue_label": "smoke",
                        "evidence_refs": ["artifact:coverage-001"],
                    }
                ]
            },
            "promotion_source": "deliberation-plane-promotion-basis",
        },
    )
    plan_artifact_path = runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json").resolve()
    plan_payload = {
        "schema_version": "runtime-orchestration-plan-v1",
        "skill": "plan-round-orchestration",
        "generated_at_utc": "2024-01-01T00:02:00Z",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "plan_id": "orchestration-plan-surface-001",
        "planning_status": "ready-for-controller",
        "planning_mode": "planner-backed-phase2",
        "controller_authority": "queue-owner",
        "plan_source": "runtime-planner",
        "probe_stage_included": True,
        "downstream_posture": "hold-investigation-open",
        "assigned_role_hints": ["moderator", "challenger"],
        "phase_decision_basis": {
            "probe_stage_reason_codes": ["open-probe"],
            "posture_reason_codes": ["investigation-open"],
        },
        "agent_turn_hints": {
            "primary_role": "moderator",
            "support_roles": ["moderator", "challenger"],
            "recommended_skill_sequence": [
                "open-falsification-probe",
                "summarize-round-readiness",
            ],
        },
        "observed_state": {
            "direct_council_queue": False,
            "next_actions_stage_skipped": False,
        },
        "inputs": {
            "board_path": str((run_dir / "board" / "investigation_board.json").resolve()),
            "next_actions_path": str(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json").resolve()
            ),
            "probes_path": str(
                investigation_path(
                    run_dir, f"falsification_probes_{ROUND_ID}.json"
                ).resolve()
            ),
            "readiness_path": str(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json").resolve()
            ),
        },
        "execution_queue": [
            {
                "stage_name": "falsification-probes",
                "stage_kind": "skill",
                "phase_group": "investigation",
                "skill_name": "open-falsification-probe",
                "expected_skill_name": "open-falsification-probe",
                "assigned_role_hint": "challenger",
                "required_previous_stages": ["orchestration-planner"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Open contradiction probes before readiness review.",
                "reason": "An open contradiction probe remains.",
                "expected_output_path": str(
                    investigation_path(
                        run_dir, f"falsification_probes_{ROUND_ID}.json"
                    ).resolve()
                ),
            },
            {
                "stage_name": "round-readiness",
                "stage_kind": "skill",
                "phase_group": "readiness",
                "skill_name": "summarize-round-readiness",
                "expected_skill_name": "summarize-round-readiness",
                "assigned_role_hint": "moderator",
                "required_previous_stages": ["falsification-probes"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Summarize whether the round can promote.",
                "reason": "Readiness depends on the open contradiction probe.",
                "expected_output_path": str(
                    reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json").resolve()
                ),
            },
        ],
        "gate_steps": [
            {
                "stage_name": "promotion-gate",
                "stage_kind": "gate",
                "phase_group": "gate",
                "required_previous_stages": ["round-readiness"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Evaluate whether the current round can promote.",
                "reason": "Gate the round after readiness review.",
                "expected_output_path": str(
                    runtime_path(run_dir, f"promotion_gate_{ROUND_ID}.json").resolve()
                ),
                "gate_handler": "promotion-gate",
                "readiness_stage_name": "round-readiness",
            }
        ],
        "derived_exports": [
            {
                "stage_name": "board-summary",
                "stage_kind": "skill",
                "phase_group": "exports",
                "skill_name": "summarize-board-state",
                "expected_skill_name": "summarize-board-state",
                "assigned_role_hint": "moderator",
                "required_previous_stages": ["orchestration-planner"],
                "blocking": False,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Materialize a board summary only as a derived export.",
                "reason": "Board exports remain derived-only.",
                "expected_output_path": str(
                    (run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").resolve()
                ),
                "required_for_controller": False,
                "export_mode": "derived-only",
            }
        ],
        "post_gate_steps": [
            {
                "stage_name": "promotion-basis",
                "stage_kind": "skill",
                "phase_group": "promotion",
                "skill_name": "promote-evidence-basis",
                "expected_skill_name": "promote-evidence-basis",
                "assigned_role_hint": "moderator",
                "required_previous_stages": ["promotion-gate"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Freeze the promoted or withheld evidence basis.",
                "reason": "Persist the evidence basis after gate evaluation.",
                "expected_output_path": str(
                    promotion_path(
                        run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"
                    ).resolve()
                ),
            }
        ],
        "stop_conditions": [
            {
                "condition_id": "planned-skill-failure",
                "trigger": "Any planned skill returns blocked or failed.",
                "effect": "Abort controller execution.",
            }
        ],
        "fallback_path": [
            {
                "when": "Gate withholds promotion after readiness review.",
                "reason": "The contradiction probe remains open.",
                "suggested_next_skills": ["open-falsification-probe"],
            }
        ],
        "planning_notes": [
            "Planner output exists to keep the phase-2 queue auditable.",
        ],
        "deliberation_sync": {"status": "completed", "sync_mode": "unit-test"},
    }
    store_orchestration_plan_record(
        run_dir,
        plan_payload=plan_payload,
        artifact_path=str(plan_artifact_path),
    )
    supervisor_snapshot = {
        "schema_version": "runtime-supervisor-v3",
        "generated_at_utc": "2024-01-01T00:15:00Z",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "supervisor_status": "hold-investigation-open",
        "supervisor_substatus": "probe-outstanding",
        "phase2_posture": "hold-investigation-open",
        "terminal_state": "investigation-hold",
        "controller_status": "completed",
        "resume_status": "fresh-run",
        "current_stage": "",
        "failed_stage": "",
        "resume_recommended": False,
        "restart_recommended": False,
        "resume_from_stage": "",
        "readiness_status": "needs-more-data",
        "gate_status": "freeze-withheld",
        "promotion_status": "withheld",
        "reporting_ready": False,
        "reporting_blockers": ["An open contradiction probe remains."],
        "reporting_handoff_status": "investigation-open",
        "planning_mode": "planner-backed",
        "recommended_next_skills": ["open-falsification-probe"],
        "supervisor_path": str(
            runtime_path(run_dir, f"supervisor_state_{ROUND_ID}.json").resolve()
        ),
    }
    store_promotion_freeze_record(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        supervisor_snapshot=supervisor_snapshot,
        artifact_paths={
            "supervisor_state_path": str(
                runtime_path(run_dir, f"supervisor_state_{ROUND_ID}.json").resolve()
            )
        },
    )
    supervisor_export = dict(supervisor_snapshot)
    supervisor_export["handoff_status"] = supervisor_snapshot["reporting_handoff_status"]
    next_actions_export = dict(next_actions)
    next_actions_export["action_source"] = "deliberation-plane-actions"
    probes_export = dict(probes)
    probes_export["action_source"] = "deliberation-plane-probes"
    return {
        "plan": plan_payload,
        "next_actions": next_actions,
        "next_actions_export": next_actions_export,
        "probes": probes,
        "probes_export": probes_export,
        "readiness": readiness,
        "promotion": promotion,
        "supervisor_export": supervisor_export,
    }


class Phase2StateSurfaceTests(unittest.TestCase):
    def test_phase2_state_surfaces_exports_all_phase2_wrappers(self) -> None:
        exported = set(phase2_state_surfaces.__all__)
        for name in WRAPPER_NAMES:
            self.assertIn(name, exported)
        self.assertIn("build_reporting_surface", exported)

    def test_investigation_planning_reexports_phase2_surface_wrappers(self) -> None:
        for name in WRAPPER_NAMES:
            self.assertIs(
                getattr(investigation_planning, name),
                getattr(phase2_state_surfaces, name),
                name,
            )

    def test_phase2_wrappers_flag_orphaned_artifacts_instead_of_reusing_them(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            payloads = seed_phase2_surface_state(run_dir)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

            write_json(
                runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json"),
                payloads["plan"],
            )
            write_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"),
                payloads["next_actions"],
            )
            write_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json"),
                payloads["probes"],
            )
            write_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"),
                payloads["readiness"],
            )
            write_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"),
                payloads["promotion"],
            )
            write_json(
                runtime_path(run_dir, f"supervisor_state_{ROUND_ID}.json"),
                payloads["supervisor_export"],
            )

            for query in (
                "DELETE FROM orchestration_plan_steps WHERE run_id = ? AND round_id = ?",
                "DELETE FROM orchestration_plans WHERE run_id = ? AND round_id = ?",
                "DELETE FROM moderator_actions WHERE run_id = ? AND round_id = ?",
                "DELETE FROM moderator_action_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM falsification_probes WHERE run_id = ? AND round_id = ?",
                "DELETE FROM falsification_probe_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM round_readiness_assessments WHERE run_id = ? AND round_id = ?",
                "DELETE FROM promotion_basis_items WHERE run_id = ? AND round_id = ?",
                "DELETE FROM promotion_basis_records WHERE run_id = ? AND round_id = ?",
                "DELETE FROM controller_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM gate_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM supervisor_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM promotion_freezes WHERE run_id = ? AND round_id = ?",
            ):
                execute_db(db_path, query, (RUN_ID, ROUND_ID))

            contexts = {
                "plan": phase2_state_surfaces.load_orchestration_plan_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                ),
                "next_actions": phase2_state_surfaces.load_next_actions_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                ),
                "probes": phase2_state_surfaces.load_falsification_probe_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                ),
                "readiness": phase2_state_surfaces.load_round_readiness_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                ),
                "promotion": phase2_state_surfaces.load_promotion_basis_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                ),
                "supervisor": phase2_state_surfaces.load_supervisor_state_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                ),
            }

            self.assertIsNone(contexts["plan"]["payload"])
            self.assertTrue(contexts["plan"]["artifact_present"])
            self.assertFalse(contexts["plan"]["payload_present"])
            self.assertEqual(
                "orphaned-orchestration-plan-artifact",
                contexts["plan"]["source"],
            )
            self.assertIsNone(contexts["next_actions"]["payload"])
            self.assertTrue(contexts["next_actions"]["artifact_present"])
            self.assertFalse(contexts["next_actions"]["payload_present"])
            self.assertEqual(
                "orphaned-next-actions-artifact",
                contexts["next_actions"]["source"],
            )

            self.assertIsNone(contexts["probes"]["payload"])
            self.assertTrue(contexts["probes"]["artifact_present"])
            self.assertFalse(contexts["probes"]["payload_present"])
            self.assertEqual(
                "orphaned-falsification-probes-artifact",
                contexts["probes"]["source"],
            )

            self.assertIsNone(contexts["readiness"]["payload"])
            self.assertTrue(contexts["readiness"]["artifact_present"])
            self.assertFalse(contexts["readiness"]["payload_present"])
            self.assertEqual(
                "orphaned-round-readiness-artifact",
                contexts["readiness"]["source"],
            )

            self.assertIsNone(contexts["promotion"]["payload"])
            self.assertTrue(contexts["promotion"]["artifact_present"])
            self.assertFalse(contexts["promotion"]["payload_present"])
            self.assertEqual(
                "orphaned-promotion-basis-artifact",
                contexts["promotion"]["source"],
            )

            self.assertIsNone(contexts["supervisor"]["payload"])
            self.assertTrue(contexts["supervisor"]["artifact_present"])
            self.assertFalse(contexts["supervisor"]["payload_present"])
            self.assertEqual(
                "orphaned-supervisor-state-artifact",
                contexts["supervisor"]["source"],
            )

    def test_materialize_phase2_exports_rebuilds_phase2_files_from_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            expected = seed_phase2_surface_state(run_dir)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

            payload = run_kernel(
                "materialize-phase2-exports",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual(
                "phase2-export-materialization-v1",
                payload["schema_version"],
            )
            self.assertEqual(6, payload["summary"]["materialized_export_count"])
            self.assertEqual(0, payload["summary"]["missing_db_object_count"])
            self.assertEqual(0, payload["summary"]["orphaned_artifact_count"])
            self.assertEqual(6, payload["summary"]["target_export_count"])

            self.assertDictEqual(
                phase2_state_surfaces.load_orchestration_plan_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                )["payload"],
                load_json(runtime_path(run_dir, f"orchestration_plan_{ROUND_ID}.json")),
            )

            self.assertDictEqual(
                phase2_state_surfaces.load_next_actions_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                )["payload"],
                load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                phase2_state_surfaces.load_falsification_probe_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                )["payload"],
                load_json(
                    investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
                ),
            )
            self.assertDictEqual(
                phase2_state_surfaces.load_round_readiness_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                )["payload"],
                load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                phase2_state_surfaces.load_promotion_basis_wrapper(
                    run_dir,
                    run_id=RUN_ID,
                    round_id=ROUND_ID,
                )["payload"],
                load_json(
                    promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
                ),
            )

            supervisor_export = load_json(
                runtime_path(run_dir, f"supervisor_state_{ROUND_ID}.json")
            )
            self.assertEqual(
                expected["supervisor_export"]["supervisor_status"],
                supervisor_export["supervisor_status"],
            )
            self.assertEqual(
                expected["supervisor_export"]["promotion_status"],
                supervisor_export["promotion_status"],
            )
            self.assertEqual(
                expected["supervisor_export"]["reporting_handoff_status"],
                supervisor_export["reporting_handoff_status"],
            )
            self.assertEqual("investigation-open", supervisor_export["handoff_status"])

    def test_show_run_state_exposes_phase2_export_and_query_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel(
                "init-run",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
            )
            seed_phase2_surface_state(run_dir)

            payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
            )

            operator = payload["phase2"]["operator"]
            self.assertIn(
                "materialize-phase2-exports",
                operator["materialize_phase2_exports_command"],
            )
            self.assertIn(
                "query-public-signals",
                operator["query_public_signals_command"],
            )
            self.assertIn(
                "query-formal-signals",
                operator["query_formal_signals_command"],
            )
            self.assertIn(
                "query-environment-signals",
                operator["query_environment_signals_command"],
            )
            self.assertIn(
                "--object-kind orchestration-plan",
                operator["query_orchestration_plans_command"],
            )
            self.assertIn(
                "--object-kind orchestration-plan-step",
                operator["query_orchestration_plan_steps_command"],
            )
            self.assertIn(
                "--object-kind probe",
                operator["query_probes_command"],
            )
            self.assertIn(
                "--object-kind readiness-assessment",
                operator["query_readiness_assessments_command"],
            )
            self.assertIn(
                "--object-kind promotion-basis",
                operator["query_promotion_basis_command"],
            )


if __name__ == "__main__":
    unittest.main()

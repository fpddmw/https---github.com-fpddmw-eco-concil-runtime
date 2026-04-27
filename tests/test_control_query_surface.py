from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    connect_db,
    store_orchestration_plan_record,
    store_promotion_freeze_record,
)

RUN_ID = "run-control-query-001"
ROUND_ID = "round-control-query-001"


def seed_control_state(run_dir: Path) -> dict[str, str]:
    plan_path = (run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve()
    controller_path = (run_dir / "runtime" / f"controller_state_{ROUND_ID}.json").resolve()
    gate_path = (run_dir / "runtime" / f"promotion_gate_{ROUND_ID}.json").resolve()
    supervisor_path = (run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json").resolve()
    plan_payload = {
        "schema_version": "runtime-orchestration-plan-v1",
        "skill": "plan-round-orchestration",
        "generated_at_utc": "2024-01-01T00:00:00Z",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "plan_id": "orchestration-plan-control-001",
        "planning_status": "ready-for-controller",
        "planning_mode": "planner-backed-phase2",
        "controller_authority": "queue-owner",
        "plan_source": "runtime-planner",
        "probe_stage_included": False,
        "downstream_posture": "promote-candidate",
        "assigned_role_hints": ["moderator"],
        "phase_decision_basis": {
            "probe_stage_reason_codes": [],
            "posture_reason_codes": ["promotion-ready"],
        },
        "agent_turn_hints": {
            "primary_role": "moderator",
            "support_roles": ["moderator"],
            "recommended_skill_sequence": ["summarize-round-readiness"],
        },
        "observed_state": {
            "direct_council_queue": True,
            "next_actions_stage_skipped": True,
        },
        "inputs": {
            "readiness_path": str(
                (run_dir / "reporting" / f"round_readiness_{ROUND_ID}.json").resolve()
            ),
        },
        "execution_queue": [
            {
                "stage_name": "round-readiness",
                "stage_kind": "skill",
                "phase_group": "readiness",
                "skill_name": "summarize-round-readiness",
                "expected_skill_name": "summarize-round-readiness",
                "assigned_role_hint": "moderator",
                "required_previous_stages": ["orchestration-planner"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Summarize whether the round can promote.",
                "reason": "Readiness review is required before promotion gate.",
                "expected_output_path": str(
                    (run_dir / "reporting" / f"round_readiness_{ROUND_ID}.json").resolve()
                ),
            }
        ],
        "gate_steps": [
            {
                "stage_name": "promotion-gate",
                "stage_kind": "gate",
                "phase_group": "gate",
                "required_previous_stages": ["round-readiness"],
                "blocking": True,
                "resume_policy": "skip-if-completed",
                "operator_summary": "Evaluate whether the round can move into promotion.",
                "reason": "Gate the round after readiness review.",
                "expected_output_path": str(gate_path),
                "gate_handler": "promotion-gate",
                "readiness_stage_name": "round-readiness",
            }
        ],
        "derived_exports": [],
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
                "operator_summary": "Freeze the promoted evidence basis.",
                "reason": "Persist the final promotion basis.",
                "expected_output_path": str(
                    (run_dir / "promotion" / f"promoted_evidence_basis_{ROUND_ID}.json").resolve()
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
        "fallback_path": [],
        "planning_notes": ["Planner output remains DB-backed."],
        "deliberation_sync": {"status": "completed", "sync_mode": "unit-test"},
    }
    store_orchestration_plan_record(
        run_dir,
        plan_payload=plan_payload,
        artifact_path=str(plan_path),
    )
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
        "completed_stage_names": [
            "orchestration-planner",
            "next-actions",
            "round-readiness",
            "promotion-gate",
            "promotion-basis",
        ],
        "pending_stage_names": [],
        "resume_recommended": False,
        "restart_recommended": False,
        "recovery": {"resume_from_stage": ""},
        "gate_reasons": [],
        "recommended_next_skills": ["materialize-reporting-handoff"],
        "planning": {
            "plan_id": plan_payload["plan_id"],
            "plan_path": str(plan_path),
            "plan_source": plan_payload["plan_source"],
            "planning_status": plan_payload["planning_status"],
            "controller_authority": plan_payload["controller_authority"],
            "execution_queue": plan_payload["execution_queue"],
            "gate_steps": plan_payload["gate_steps"],
            "post_gate_steps": plan_payload["post_gate_steps"],
        },
        "steps": [],
        "artifacts": {
            "controller_state_path": str(controller_path),
            "promotion_gate_path": str(gate_path),
            "orchestration_plan_path": str(plan_path),
        },
    }
    gate_snapshot = {
        "schema_version": "runtime-gate-v1",
        "generated_at_utc": "2024-01-01T00:00:00Z",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "stage_name": "promotion-gate",
        "gate_handler": "promotion-gate",
        "readiness_path": str(
            (run_dir / "reporting" / f"round_readiness_{ROUND_ID}.json").resolve()
        ),
        "readiness_status": "ready",
        "promote_allowed": True,
        "gate_status": "allow-promote",
        "decision_source": "agent-council",
        "gate_reasons": [],
        "recommended_next_skills": [],
        "output_path": str(gate_path),
    }
    supervisor_snapshot = {
        "schema_version": "runtime-supervisor-v3",
        "generated_at_utc": "2024-01-01T00:05:00Z",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "supervisor_path": str(supervisor_path),
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
        "reporting_ready": True,
        "reporting_blockers": [],
        "reporting_handoff_status": "reporting-ready",
        "execution_policy": {},
        "planning_mode": "planner-backed",
        "promotion_gate_path": str(gate_path),
        "controller_path": str(controller_path),
        "recommended_next_skills": ["materialize-reporting-handoff"],
        "round_transition": {},
        "top_actions": [],
        "operator_notes": [
            "Round promotion succeeded and the evidence basis is now ready for downstream reporting."
        ],
        "inspection_paths": {
            "controller_path": str(controller_path),
            "plan_path": str(
                (run_dir / "runtime" / f"orchestration_plan_{ROUND_ID}.json").resolve()
            ),
            "gate_path": str(gate_path),
            "promotion_basis_path": str(
                (run_dir / "promotion" / f"promoted_evidence_basis_{ROUND_ID}.json").resolve()
            ),
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
            "controller_state_path": str(controller_path),
            "promotion_gate_path": str(gate_path),
            "supervisor_state_path": str(supervisor_path),
        },
    )
    return {
        "plan_path": str(plan_path),
        "controller_path": str(controller_path),
        "gate_path": str(gate_path),
        "supervisor_path": str(supervisor_path),
    }


class ControlQuerySurfaceTests(unittest.TestCase):
    def test_query_control_objects_prefers_db_columns_when_raw_json_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            paths = seed_control_state(run_dir)

            connection, _db_file = connect_db(run_dir)
            try:
                with connection:
                    controller_row = connection.execute(
                        """
                        SELECT snapshot_id, raw_json
                        FROM controller_snapshots
                        WHERE run_id = ? AND round_id = ?
                        """,
                        (RUN_ID, ROUND_ID),
                    ).fetchone()
                    self.assertIsNotNone(controller_row)
                    controller_payload = json.loads(controller_row["raw_json"])
                    controller_payload["controller_status"] = "running"
                    controller_payload["current_stage"] = "stale-stage"
                    controller_payload["promotion_status"] = "withheld"
                    controller_payload["artifacts"] = {}
                    connection.execute(
                        "UPDATE controller_snapshots SET raw_json = ? WHERE snapshot_id = ?",
                        (
                            json.dumps(
                                controller_payload,
                                ensure_ascii=True,
                                sort_keys=True,
                            ),
                            controller_row["snapshot_id"],
                        ),
                    )

                    gate_row = connection.execute(
                        """
                        SELECT snapshot_id, raw_json
                        FROM gate_snapshots
                        WHERE run_id = ? AND round_id = ?
                        """,
                        (RUN_ID, ROUND_ID),
                    ).fetchone()
                    self.assertIsNotNone(gate_row)
                    gate_payload = json.loads(gate_row["raw_json"])
                    gate_payload.pop("stage_name", None)
                    gate_payload.pop("gate_handler", None)
                    gate_payload["gate_status"] = "freeze-withheld"
                    gate_payload["output_path"] = ""
                    connection.execute(
                        "UPDATE gate_snapshots SET raw_json = ? WHERE snapshot_id = ?",
                        (
                            json.dumps(gate_payload, ensure_ascii=True, sort_keys=True),
                            gate_row["snapshot_id"],
                        ),
                    )

                    supervisor_row = connection.execute(
                        """
                        SELECT snapshot_id, raw_json
                        FROM supervisor_snapshots
                        WHERE run_id = ? AND round_id = ?
                        """,
                        (RUN_ID, ROUND_ID),
                    ).fetchone()
                    self.assertIsNotNone(supervisor_row)
                    supervisor_payload = json.loads(supervisor_row["raw_json"])
                    supervisor_payload["supervisor_status"] = "hold-investigation-open"
                    supervisor_payload["reporting_ready"] = False
                    supervisor_payload["reporting_handoff_status"] = "investigation-open"
                    supervisor_payload["supervisor_path"] = ""
                    connection.execute(
                        "UPDATE supervisor_snapshots SET raw_json = ? WHERE snapshot_id = ?",
                        (
                            json.dumps(
                                supervisor_payload,
                                ensure_ascii=True,
                                sort_keys=True,
                            ),
                            supervisor_row["snapshot_id"],
                        ),
                    )

                    freeze_row = connection.execute(
                        """
                        SELECT freeze_id, raw_json
                        FROM promotion_freezes
                        WHERE run_id = ? AND round_id = ?
                        """,
                        (RUN_ID, ROUND_ID),
                    ).fetchone()
                    self.assertIsNotNone(freeze_row)
                    freeze_payload = json.loads(freeze_row["raw_json"])
                    freeze_payload["promotion_status"] = "withheld"
                    freeze_payload["reporting_ready"] = False
                    freeze_payload["reporting_handoff_status"] = "investigation-open"
                    freeze_payload["reporting_blockers"] = ["stale-blocker"]
                    connection.execute(
                        "UPDATE promotion_freezes SET raw_json = ? WHERE freeze_id = ?",
                        (
                            json.dumps(
                                freeze_payload,
                                ensure_ascii=True,
                                sort_keys=True,
                            ),
                            freeze_row["freeze_id"],
                        ),
                    )

                    plan_row = connection.execute(
                        """
                        SELECT plan_id, raw_json
                        FROM orchestration_plans
                        WHERE run_id = ? AND round_id = ?
                        """,
                        (RUN_ID, ROUND_ID),
                    ).fetchone()
                    self.assertIsNotNone(plan_row)
                    plan_payload = json.loads(plan_row["raw_json"])
                    plan_payload["plan_source"] = "stale-plan-source"
                    plan_payload["planning_status"] = "stale-status"
                    plan_payload["execution_queue"] = []
                    connection.execute(
                        "UPDATE orchestration_plans SET raw_json = ? WHERE plan_id = ?",
                        (
                            json.dumps(
                                plan_payload,
                                ensure_ascii=True,
                                sort_keys=True,
                            ),
                            plan_row["plan_id"],
                        ),
                    )

                    plan_step_row = connection.execute(
                        """
                        SELECT step_id, raw_json
                        FROM orchestration_plan_steps
                        WHERE run_id = ? AND round_id = ? AND stage_name = ?
                        """,
                        (RUN_ID, ROUND_ID, "round-readiness"),
                    ).fetchone()
                    self.assertIsNotNone(plan_step_row)
                    plan_step_payload = json.loads(plan_step_row["raw_json"])
                    plan_step_payload["stage_name"] = "stale-stage"
                    plan_step_payload["skill_name"] = "stale-skill"
                    plan_step_payload["expected_output_path"] = ""
                    connection.execute(
                        "UPDATE orchestration_plan_steps SET raw_json = ? WHERE step_id = ?",
                        (
                            json.dumps(
                                plan_step_payload,
                                ensure_ascii=True,
                                sort_keys=True,
                            ),
                            plan_step_row["step_id"],
                        ),
                    )
            finally:
                connection.close()

            controller_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "controller-state",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual(
                1,
                controller_query["summary"]["returned_object_count"],
            )
            controller = controller_query["objects"][0]
            self.assertEqual("completed", controller["controller_status"])
            self.assertEqual("", controller["current_stage"])
            self.assertEqual("promoted", controller["promotion_status"])
            self.assertEqual(
                paths["controller_path"],
                controller["artifacts"]["controller_state_path"],
            )

            gate_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "gate-state",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--stage-name",
                "promotion-gate",
            )
            self.assertEqual(1, gate_query["summary"]["returned_object_count"])
            gate = gate_query["objects"][0]
            self.assertEqual("promotion-gate", gate["stage_name"])
            self.assertEqual("promotion-gate", gate["gate_handler"])
            self.assertEqual("allow-promote", gate["gate_status"])
            self.assertEqual(paths["gate_path"], gate["output_path"])

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
            )
            self.assertEqual(1, plan_query["summary"]["returned_object_count"])
            plan = plan_query["objects"][0]
            self.assertEqual("runtime-planner", plan["plan_source"])
            self.assertEqual("ready-for-controller", plan["planning_status"])
            self.assertEqual(1, len(plan["execution_queue"]))
            self.assertEqual(paths["plan_path"], plan["artifact_path"])

            plan_step_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "orchestration-plan-step",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--stage-name",
                "round-readiness",
            )
            self.assertEqual(1, plan_step_query["summary"]["returned_object_count"])
            plan_step = plan_step_query["objects"][0]
            self.assertEqual("round-readiness", plan_step["stage_name"])
            self.assertEqual(
                "summarize-round-readiness",
                plan_step["skill_name"],
            )
            self.assertTrue(plan_step["blocking"])

            supervisor_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "supervisor-state",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--reporting-ready-only",
            )
            self.assertEqual(1, supervisor_query["summary"]["returned_object_count"])
            supervisor = supervisor_query["objects"][0]
            self.assertEqual("reporting-ready", supervisor["supervisor_status"])
            self.assertTrue(supervisor["reporting_ready"])
            self.assertEqual(
                "reporting-ready",
                supervisor["reporting_handoff_status"],
            )
            self.assertEqual(paths["supervisor_path"], supervisor["supervisor_path"])

            freeze_query = run_kernel(
                "query-control-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "promotion-freeze",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--reporting-ready-only",
            )
            self.assertEqual(1, freeze_query["summary"]["returned_object_count"])
            freeze = freeze_query["objects"][0]
            self.assertEqual("promoted", freeze["promotion_status"])
            self.assertTrue(freeze["reporting_ready"])
            self.assertEqual("reporting-ready", freeze["reporting_handoff_status"])
            self.assertEqual([], freeze["reporting_blockers"])


if __name__ == "__main__":
    unittest.main()

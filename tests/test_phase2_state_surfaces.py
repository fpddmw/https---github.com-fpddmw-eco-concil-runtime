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
    store_promotion_basis_record,
    store_promotion_freeze_record,
    store_round_readiness_assessment,
)

WRAPPER_NAMES = (
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
        "recommended_next_skills": ["eco-open-falsification-probe"],
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
                "DELETE FROM moderator_actions WHERE run_id = ? AND round_id = ?",
                "DELETE FROM moderator_action_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM falsification_probes WHERE run_id = ? AND round_id = ?",
                "DELETE FROM falsification_probe_snapshots WHERE run_id = ? AND round_id = ?",
                "DELETE FROM round_readiness_assessments WHERE run_id = ? AND round_id = ?",
                "DELETE FROM promotion_basis_items WHERE run_id = ? AND round_id = ?",
                "DELETE FROM promotion_basis_records WHERE run_id = ? AND round_id = ?",
                "DELETE FROM promotion_freezes WHERE run_id = ? AND round_id = ?",
            ):
                execute_db(db_path, query, (RUN_ID, ROUND_ID))

            contexts = {
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
            self.assertEqual(5, payload["summary"]["materialized_export_count"])
            self.assertEqual(0, payload["summary"]["missing_db_object_count"])
            self.assertEqual(0, payload["summary"]["orphaned_artifact_count"])

            expected_next_actions = fetch_raw_json(
                db_path,
                """
                SELECT raw_json
                FROM moderator_action_snapshots
                WHERE run_id = ? AND round_id = ?
                """,
                (RUN_ID, ROUND_ID),
            )
            expected_next_actions["action_source"] = "deliberation-plane-actions"
            self.assertDictEqual(
                expected_next_actions,
                load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")),
            )
            expected_probes = fetch_raw_json(
                db_path,
                """
                SELECT raw_json
                FROM falsification_probe_snapshots
                WHERE run_id = ? AND round_id = ?
                """,
                (RUN_ID, ROUND_ID),
            )
            expected_probes["action_source"] = "deliberation-plane-probes"
            self.assertDictEqual(
                expected_probes,
                load_json(
                    investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
                ),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM round_readiness_assessments
                    WHERE run_id = ? AND round_id = ?
                    """,
                    (RUN_ID, ROUND_ID),
                ),
                load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM promotion_basis_records
                    WHERE run_id = ? AND round_id = ?
                    """,
                    (RUN_ID, ROUND_ID),
                ),
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

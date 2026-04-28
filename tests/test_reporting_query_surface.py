from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    primary_research_issue_id,
    primary_wp4_evidence_ref,
    request_and_approve_transition,
    reporting_path,
    run_kernel,
    run_script,
    runtime_src_path,
    script_path,
    seed_analysis_chain,
    submit_ready_council_support,
)

RUN_ID = "run-reporting-query-001"
ROUND_ID = "round-reporting-query-001"


def approve_promotion_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="promote-evidence-basis",
        rationale="Approve promotion for reporting query workflow coverage.",
    )

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_council_decision_wrapper,
    load_expert_report_wrapper,
    load_final_publication_wrapper,
    load_reporting_handoff_wrapper,
)


def prepare_ready_reporting_plane(run_dir: Path, root: Path) -> dict[str, str]:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    evidence_ref = primary_wp4_evidence_ref(outputs)
    issue_id = primary_research_issue_id(outputs)
    submit_ready_council_support(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        issue_id=issue_id,
        evidence_ref=evidence_ref,
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
        "Round is ready to move into role reports and final decision publish.",
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
        "--confidence",
        "0.93",
    )
    approve_promotion_transition(run_dir)
    run_kernel(
        "supervise-round",
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("materialize-reporting-handoff"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("draft-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("draft-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--role",
        "sociologist",
    )
    run_script(
        script_path("draft-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--role",
        "environmentalist",
    )
    run_script(
        script_path("publish-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--role",
        "sociologist",
    )
    run_script(
        script_path("publish-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--role",
        "environmentalist",
    )
    run_script(
        script_path("publish-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("materialize-final-publication"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    decision_payload = load_json(
        reporting_path(run_dir, f"council_decision_{ROUND_ID}.json")
    )
    handoff_payload = load_json(
        reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")
    )
    publication_payload = load_json(
        reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")
    )
    return {
        "decision_id": decision_payload["decision_id"],
        "handoff_id": handoff_payload["handoff_id"],
        "publication_id": publication_payload["publication_id"],
    }


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


class ReportingQuerySurfaceTests(unittest.TestCase):
    def test_kernel_lists_and_queries_reporting_plane_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seeded = prepare_ready_reporting_plane(run_dir, root)

            contracts_payload = run_kernel(
                "list-canonical-contracts",
                "--plane",
                "reporting",
            )
            self.assertEqual("reporting", contracts_payload["plane"])
            self.assertEqual("canonical-contract-list-v1", contracts_payload["schema_version"])
            self.assertSetEqual(
                {
                    "report-section-draft",
                    "reporting-handoff",
                    "council-decision",
                    "expert-report",
                    "final-publication",
                },
                {contract["object_kind"] for contract in contracts_payload["contracts"]},
            )

            reporting_state = run_kernel(
                "show-reporting-state",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertIn(
                "query-reporting-objects",
                reporting_state["operator"]["query_council_decisions_command"],
            )
            self.assertIn(
                "--stage canonical",
                reporting_state["operator"]["query_expert_reports_command"],
            )
            self.assertIn(
                "materialize-reporting-exports",
                reporting_state["operator"]["materialize_reporting_exports_command"],
            )
            self.assertIn(
                "query-reporting-objects",
                reporting_state["operator"]["query_report_section_drafts_command"],
            )

            handoff_payload = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "reporting-handoff",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
            )
            self.assertEqual("reporting-object-query-v1", handoff_payload["schema_version"])
            self.assertEqual(1, handoff_payload["summary"]["returned_object_count"])
            self.assertEqual("reporting-handoff", handoff_payload["contract"]["object_kind"])
            self.assertEqual(
                "reporting-handoff-v1",
                handoff_payload["objects"][0]["schema_version"],
            )
            self.assertIsInstance(handoff_payload["objects"][0]["provenance"], dict)

            decision_payload = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "council-decision",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--decision-id",
                seeded["decision_id"],
            )
            self.assertEqual(2, decision_payload["summary"]["matching_object_count"])
            self.assertEqual(2, decision_payload["summary"]["returned_object_count"])

            decision_draft_payload = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "council-decision",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--decision-id",
                seeded["decision_id"],
                "--stage",
                "draft",
            )
            self.assertEqual(1, decision_draft_payload["summary"]["returned_object_count"])
            self.assertEqual("draft", decision_draft_payload["objects"][0]["decision_stage"])

            expert_payload = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "expert-report",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "sociologist",
                "--stage",
                "canonical",
            )
            self.assertEqual(1, expert_payload["summary"]["returned_object_count"])
            self.assertEqual("canonical", expert_payload["objects"][0]["report_stage"])
            self.assertEqual("sociologist", expert_payload["objects"][0]["agent_role"])

            publication_payload = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "final-publication",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
            )
            self.assertEqual(1, publication_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "final-publication",
                publication_payload["contract"]["object_kind"],
            )
            self.assertTrue(publication_payload["objects"][0]["publication_status"])

            section_payload = run_kernel(
                "submit-report-section-draft",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "report-editor",
                "--agent-role",
                "report-editor",
                "--report-id",
                ROUND_ID,
                "--section-key",
                "executive-summary",
                "--section-title",
                "Executive Summary",
                "--section-text",
                "Smoke evidence supports the round-level conclusion.",
                "--basis-object-id",
                seeded["decision_id"],
                "--bundle-id",
                "bundle-001",
                "--finding-id",
                "finding-001",
                "--evidence-ref",
                "evidence://report-section-001",
                "--provenance-json",
                "{\"source\":\"unit-test\"}",
            )
            section_query = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "report-section-draft",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--include-contract",
            )

            self.assertEqual("completed", section_payload["status"])
            self.assertEqual(1, section_query["summary"]["returned_object_count"])
            self.assertEqual(
                "report-section-draft",
                section_query["contract"]["object_kind"],
            )
            self.assertEqual(
                "executive-summary",
                section_query["objects"][0]["section_key"],
            )
            self.assertEqual(
                "Executive Summary",
                section_query["objects"][0]["section_title"],
            )

    def test_reporting_records_are_persisted_as_canonical_db_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_reporting_plane(run_dir, root)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

            handoff_payload = fetch_raw_json(
                db_path,
                "SELECT raw_json FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                (RUN_ID, ROUND_ID),
            )
            decision_draft_payload = fetch_raw_json(
                db_path,
                """
                SELECT raw_json
                FROM council_decision_records
                WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                """,
                (RUN_ID, ROUND_ID, "draft"),
            )
            decision_payload = fetch_raw_json(
                db_path,
                """
                SELECT raw_json
                FROM council_decision_records
                WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                """,
                (RUN_ID, ROUND_ID, "canonical"),
            )
            expert_draft_payload = fetch_raw_json(
                db_path,
                """
                SELECT raw_json
                FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (RUN_ID, ROUND_ID, "draft", "sociologist"),
            )
            expert_payload = fetch_raw_json(
                db_path,
                """
                SELECT raw_json
                FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (RUN_ID, ROUND_ID, "canonical", "sociologist"),
            )
            publication_payload = fetch_raw_json(
                db_path,
                "SELECT raw_json FROM final_publications WHERE run_id = ? AND round_id = ?",
                (RUN_ID, ROUND_ID),
            )

            self.assertEqual("reporting-handoff-v1", handoff_payload["schema_version"])
            self.assertIsInstance(handoff_payload["evidence_refs"], list)
            self.assertIsInstance(handoff_payload["lineage"], list)
            self.assertIsInstance(handoff_payload["provenance"], dict)

            self.assertEqual("council-decision-v1", decision_draft_payload["schema_version"])
            self.assertEqual("draft", decision_draft_payload["decision_stage"])
            self.assertIsInstance(decision_draft_payload["decision_trace_ids"], list)
            self.assertIsInstance(decision_draft_payload["published_report_refs"], list)
            self.assertIsInstance(decision_draft_payload["provenance"], dict)
            self.assertEqual("council-decision-v1", decision_payload["schema_version"])
            self.assertEqual("canonical", decision_payload["decision_stage"])
            self.assertIsInstance(decision_payload["published_report_refs"], list)
            self.assertIsInstance(decision_payload["provenance"], dict)

            self.assertEqual("expert-report-v1", expert_draft_payload["schema_version"])
            self.assertEqual("draft", expert_draft_payload["report_stage"])
            self.assertEqual("sociologist", expert_draft_payload["agent_role"])
            self.assertIsInstance(expert_draft_payload["evidence_refs"], list)
            self.assertIsInstance(expert_draft_payload["lineage"], list)
            self.assertIsInstance(expert_draft_payload["provenance"], dict)
            self.assertEqual("expert-report-v1", expert_payload["schema_version"])
            self.assertEqual("canonical", expert_payload["report_stage"])
            self.assertEqual("sociologist", expert_payload["agent_role"])
            self.assertIsInstance(expert_payload["evidence_refs"], list)
            self.assertIsInstance(expert_payload["lineage"], list)
            self.assertIsInstance(expert_payload["provenance"], dict)

            self.assertEqual("final-publication-v1", publication_payload["schema_version"])
            self.assertIsInstance(publication_payload["decision"], dict)
            self.assertIsInstance(publication_payload["evidence_refs"], list)
            self.assertIsInstance(publication_payload["lineage"], list)
            self.assertIsInstance(publication_payload["provenance"], dict)

            self.assertDictEqual(
                handoff_payload,
                load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                decision_draft_payload,
                load_json(
                    reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
                ),
            )
            self.assertDictEqual(
                decision_payload,
                load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                expert_draft_payload,
                load_json(
                    reporting_path(
                        run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json"
                    )
                ),
            )
            self.assertDictEqual(
                expert_payload,
                load_json(
                    reporting_path(run_dir, f"expert_report_sociologist_{ROUND_ID}.json")
                ),
            )
            self.assertDictEqual(
                publication_payload,
                load_json(reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")),
            )

    def test_reporting_wrappers_keep_db_fields_and_flag_orphaned_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_reporting_plane(run_dir, root)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

            decision_context = load_council_decision_wrapper(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                decision_stage="draft",
            )
            expert_context = load_expert_report_wrapper(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_role="sociologist",
                report_stage="canonical",
            )
            self.assertIn("record_id", decision_context["payload"])
            self.assertEqual("draft", decision_context["payload"]["decision_stage"])
            self.assertIn("record_id", expert_context["payload"])
            self.assertEqual("canonical", expert_context["payload"]["report_stage"])

            execute_db(
                db_path,
                "DELETE FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                (RUN_ID, ROUND_ID),
            )
            execute_db(
                db_path,
                """
                DELETE FROM council_decision_records
                WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                """,
                (RUN_ID, ROUND_ID, "draft"),
            )
            execute_db(
                db_path,
                """
                DELETE FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (RUN_ID, ROUND_ID, "canonical", "sociologist"),
            )
            execute_db(
                db_path,
                "DELETE FROM final_publications WHERE run_id = ? AND round_id = ?",
                (RUN_ID, ROUND_ID),
            )

            handoff_context = load_reporting_handoff_wrapper(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )
            decision_context = load_council_decision_wrapper(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                decision_stage="draft",
            )
            expert_context = load_expert_report_wrapper(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_role="sociologist",
                report_stage="canonical",
            )
            publication_context = load_final_publication_wrapper(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )

            self.assertIsNone(handoff_context["payload"])
            self.assertTrue(handoff_context["artifact_present"])
            self.assertFalse(handoff_context["payload_present"])
            self.assertEqual(
                "orphaned-reporting-handoff-artifact",
                handoff_context["source"],
            )

            self.assertIsNone(decision_context["payload"])
            self.assertTrue(decision_context["artifact_present"])
            self.assertFalse(decision_context["payload_present"])
            self.assertEqual(
                "orphaned-council-decision-draft-artifact",
                decision_context["source"],
            )

            self.assertIsNone(expert_context["payload"])
            self.assertTrue(expert_context["artifact_present"])
            self.assertFalse(expert_context["payload_present"])
            self.assertEqual(
                "orphaned-expert-report-artifact",
                expert_context["source"],
            )

            self.assertIsNone(publication_context["payload"])
            self.assertTrue(publication_context["artifact_present"])
            self.assertFalse(publication_context["payload_present"])
            self.assertEqual(
                "orphaned-final-publication-artifact",
                publication_context["source"],
            )

    def test_materialize_reporting_exports_rebuilds_reporting_files_from_db(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_reporting_plane(run_dir, root)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

            for name in (
                f"reporting_handoff_{ROUND_ID}.json",
                f"council_decision_draft_{ROUND_ID}.json",
                f"council_decision_{ROUND_ID}.json",
                f"expert_report_draft_sociologist_{ROUND_ID}.json",
                f"expert_report_draft_environmentalist_{ROUND_ID}.json",
                f"expert_report_sociologist_{ROUND_ID}.json",
                f"expert_report_environmentalist_{ROUND_ID}.json",
                f"final_publication_{ROUND_ID}.json",
            ):
                reporting_path(run_dir, name).unlink()

            payload = run_kernel(
                "materialize-reporting-exports",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("reporting-export-materialization-v1", payload["schema_version"])
            self.assertEqual(8, payload["summary"]["materialized_export_count"])
            self.assertEqual(0, payload["summary"]["missing_db_object_count"])
            self.assertEqual(0, payload["summary"]["orphaned_artifact_count"])

            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    "SELECT raw_json FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ),
                load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM council_decision_records
                    WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                    """,
                    (RUN_ID, ROUND_ID, "draft"),
                ),
                load_json(
                    reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
                ),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM council_decision_records
                    WHERE run_id = ? AND round_id = ? AND decision_stage = ?
                    """,
                    (RUN_ID, ROUND_ID, "canonical"),
                ),
                load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json")),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM expert_report_records
                    WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                    """,
                    (RUN_ID, ROUND_ID, "draft", "sociologist"),
                ),
                load_json(
                    reporting_path(
                        run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json"
                    )
                ),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM expert_report_records
                    WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                    """,
                    (RUN_ID, ROUND_ID, "draft", "environmentalist"),
                ),
                load_json(
                    reporting_path(
                        run_dir,
                        f"expert_report_draft_environmentalist_{ROUND_ID}.json",
                    )
                ),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM expert_report_records
                    WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                    """,
                    (RUN_ID, ROUND_ID, "canonical", "sociologist"),
                ),
                load_json(
                    reporting_path(run_dir, f"expert_report_sociologist_{ROUND_ID}.json")
                ),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    """
                    SELECT raw_json
                    FROM expert_report_records
                    WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                    """,
                    (RUN_ID, ROUND_ID, "canonical", "environmentalist"),
                ),
                load_json(
                    reporting_path(
                        run_dir, f"expert_report_environmentalist_{ROUND_ID}.json"
                    )
                ),
            )
            self.assertDictEqual(
                fetch_raw_json(
                    db_path,
                    "SELECT raw_json FROM final_publications WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ),
                load_json(reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")),
            )


if __name__ == "__main__":
    unittest.main()

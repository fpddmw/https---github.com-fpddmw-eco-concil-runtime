from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    reporting_path,
    run_kernel,
    run_script,
    script_path,
    seed_analysis_chain,
)

RUN_ID = "run-reporting-query-001"
ROUND_ID = "round-reporting-query-001"


def prepare_ready_reporting_plane(run_dir: Path, root: Path) -> dict[str, str]:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    run_script(
        script_path("eco-derive-claim-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("eco-derive-observation-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    coverage_payload = run_script(
        script_path("eco-score-evidence-coverage"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
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
        "Round is ready to move into role reports and final decision publish.",
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
        "0.93",
    )
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
        script_path("eco-materialize-reporting-handoff"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("eco-draft-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("eco-draft-expert-report"),
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
        script_path("eco-draft-expert-report"),
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
        script_path("eco-publish-expert-report"),
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
        script_path("eco-publish-expert-report"),
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
        script_path("eco-publish-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("eco-materialize-final-publication"),
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
                "--status",
                "finalize",
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
                "--status",
                "ready-to-publish",
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
                "--status",
                "ready-for-release",
                "--include-contract",
            )
            self.assertEqual(1, publication_payload["summary"]["returned_object_count"])
            self.assertEqual(
                "final-publication",
                publication_payload["contract"]["object_kind"],
            )
            self.assertEqual(
                "ready-for-release",
                publication_payload["objects"][0]["publication_status"],
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


if __name__ == "__main__":
    unittest.main()

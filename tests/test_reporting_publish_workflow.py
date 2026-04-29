from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    report_basis_path,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    reporting_path,
    request_and_approve_transition,
    run_kernel,
    run_script,
    script_path,
    seed_analysis_chain,
    submit_ready_council_support,
    write_json,
)

RUN_ID = "run-reporting-publish-001"
ROUND_ID = "round-reporting-publish-001"


def approve_report_basis_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="freeze-report-basis",
        rationale="Approve report_basis for reporting publish workflow coverage.",
    )


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


def prepare_ready_round(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    evidence_ref = primary_successor_evidence_ref(outputs)
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
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--author-role", "moderator", "--category", "analysis",
        "--note-text", "Round is ready to move into role reports and final decision publish.",
        "--linked-artifact-ref", evidence_ref,
    )
    run_script(
        script_path("update-hypothesis-status"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--title", "Smoke over NYC was materially significant",
        "--statement", "Public smoke reports are backed by elevated PM2.5 observations.",
        "--status", "active", "--owner-role", "environmentalist",
        "--linked-claim-id", issue_id,
        "--linked-artifact-ref", evidence_ref,
        "--confidence", "0.93",
    )
    run_script(
        script_path("summarize-board-state"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    approve_report_basis_transition(run_dir)
    run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)


def prepare_hold_round(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    evidence_ref = primary_successor_evidence_ref(outputs)
    issue_id = primary_research_issue_id(outputs)
    hypothesis_payload = run_script(
        script_path("update-hypothesis-status"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--title", "Smoke over NYC may be overstated",
        "--statement", "Public reports may overstate severity relative to observed PM2.5 coverage.",
        "--status", "active", "--owner-role", "moderator",
        "--linked-claim-id", issue_id,
        "--linked-artifact-ref", evidence_ref,
        "--confidence", "0.52",
    )
    run_script(
        script_path("open-challenge-ticket"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--title", "Check whether smoke narrative is overstated",
        "--challenge-statement", "Re-test whether the strongest narrative exceeds evidence coverage.",
        "--target-claim-id", issue_id,
        "--target-hypothesis-id", hypothesis_payload["canonical_ids"][0],
        "--priority", "high", "--owner-role", "challenger", "--linked-artifact-ref", evidence_ref,
    )
    approve_report_basis_transition(run_dir)
    run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)


class ReportingPublishWorkflowTests(unittest.TestCase):
    def test_show_reporting_state_recovers_db_first_reporting_surface(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json").unlink()

            payload = run_kernel(
                "show-reporting-state",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("completed", payload["status"])
            self.assertTrue(payload["summary"]["reporting_ready"])
            self.assertEqual(
                "council-decision-draft",
                payload["summary"]["surface_source"],
            )
            self.assertTrue(payload["surface"]["handoff_present"])
            self.assertTrue(payload["surface"]["decision_draft_present"])
            self.assertEqual([], payload["surface"]["reporting_blockers"])
            self.assertTrue(payload["operator"]["reporting_ready"])
            self.assertIn(
                "materialize-reporting-handoff",
                payload["operator"]["materialize_reporting_handoff_command"],
            )

    def test_role_reports_and_decision_publish_ready_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            sociologist_draft = run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            environmentalist_draft = run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            soc_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            env_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            decision_publish = run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            soc_report = load_json(reporting_path(run_dir, f"expert_report_sociologist_{ROUND_ID}.json"))
            env_report = load_json(reporting_path(run_dir, f"expert_report_environmentalist_{ROUND_ID}.json"))
            soc_draft = load_json(reporting_path(run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json"))
            decision = load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json"))

            self.assertEqual("ready-to-publish", sociologist_draft["summary"]["report_status"])
            self.assertEqual("ready-to-publish", environmentalist_draft["summary"]["report_status"])
            self.assertEqual("published", soc_publish["summary"]["operation"])
            self.assertEqual("published", env_publish["summary"]["operation"])
            self.assertEqual("published", decision_publish["summary"]["operation"])
            self.assertEqual("sociologist", soc_report["agent_role"])
            self.assertEqual("environmentalist", env_report["agent_role"])
            self.assertEqual("expert-report", soc_report["canonical_artifact"])
            self.assertEqual(
                "deliberation-plane-expert-report-draft",
                soc_report["expert_report_draft_source"],
            )
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                soc_report["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-council-decision-draft",
                soc_report["decision_source"],
            )
            self.assertTrue(
                soc_report["observed_inputs"][
                    "expert_report_draft_artifact_present"
                ]
            )
            self.assertTrue(
                soc_report["observed_inputs"]["expert_report_draft_present"]
            )
            self.assertEqual("deliberation-plane-reporting-handoff", soc_draft["reporting_handoff_source"])
            self.assertEqual("deliberation-plane-council-decision-draft", soc_draft["decision_source"])
            self.assertEqual("missing-board-brief", soc_draft["board_brief_source"])
            self.assertEqual("missing-board", soc_draft["board_state_source"])
            self.assertEqual("missing-coverage", soc_draft["coverage_source"])
            self.assertTrue(
                soc_draft["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertTrue(soc_draft["observed_inputs"]["reporting_handoff_present"])
            self.assertTrue(soc_draft["observed_inputs"]["decision_artifact_present"])
            self.assertTrue(soc_draft["observed_inputs"]["decision_present"])
            self.assertEqual("ready", decision["publication_readiness"])
            self.assertEqual("council-decision", decision["canonical_artifact"])
            self.assertEqual(
                "deliberation-plane-council-decision-draft",
                decision["decision_source"],
            )
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                decision["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                decision["report_basis_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                decision["sociologist_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                decision["environmentalist_report_source"],
            )
            self.assertTrue(decision["observed_inputs"]["decision_artifact_present"])
            self.assertTrue(decision["observed_inputs"]["decision_present"])
            self.assertTrue(
                decision["observed_inputs"]["sociologist_report_artifact_present"]
            )
            self.assertTrue(
                decision["observed_inputs"]["sociologist_report_present"]
            )
            self.assertTrue(
                decision["observed_inputs"][
                    "environmentalist_report_artifact_present"
                ]
            )
            self.assertTrue(
                decision["observed_inputs"]["environmentalist_report_present"]
            )
            self.assertEqual(2, len(decision["published_report_refs"]))

    def test_publish_council_decision_blocks_ready_round_without_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            payload = run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            self.assertEqual("blocked", payload["status"])
            self.assertEqual("blocked", payload["summary"]["operation"])
            self.assertTrue(any(item["code"] == "missing-canonical-report" for item in payload["warnings"]))

    def test_expert_report_draft_recovers_from_db_when_handoff_and_decision_artifacts_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json").unlink()

            draft_payload = run_script(
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
            draft_artifact = load_json(
                reporting_path(run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json")
            )

            self.assertEqual("ready-to-publish", draft_payload["summary"]["report_status"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                draft_artifact["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-council-decision-draft",
                draft_artifact["decision_source"],
            )
            self.assertFalse(
                draft_artifact["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertTrue(
                draft_artifact["observed_inputs"]["reporting_handoff_present"]
            )
            self.assertFalse(
                draft_artifact["observed_inputs"]["decision_artifact_present"]
            )
            self.assertTrue(draft_artifact["observed_inputs"]["decision_present"])

    def test_publish_expert_report_recovers_from_db_when_draft_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

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
            reporting_path(run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json").unlink()

            publish_payload = run_script(
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
            report_artifact = load_json(
                reporting_path(run_dir, f"expert_report_sociologist_{ROUND_ID}.json")
            )

            self.assertEqual("published", publish_payload["summary"]["operation"])
            self.assertEqual(
                "deliberation-plane-expert-report-draft",
                report_artifact["expert_report_draft_source"],
            )
            self.assertFalse(
                report_artifact["observed_inputs"][
                    "expert_report_draft_artifact_present"
                ]
            )
            self.assertTrue(
                report_artifact["observed_inputs"]["expert_report_draft_present"]
            )

    def test_publish_expert_report_blocks_on_orphaned_draft_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)
            db_path = analytics_path(run_dir, "signal_plane.sqlite")

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
            execute_db(
                db_path,
                """
                DELETE FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (RUN_ID, ROUND_ID, "draft", "sociologist"),
            )

            publish_payload = run_script(
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

            self.assertEqual("blocked", publish_payload["status"])
            self.assertEqual("blocked", publish_payload["summary"]["operation"])
            self.assertTrue(
                any(item["code"] == "missing-report-draft" for item in publish_payload["warnings"])
            )
            self.assertTrue(
                any("orphaned from the reporting plane" in item["message"] for item in publish_payload["warnings"])
            )

    def test_hold_decision_can_publish_without_reports_and_draft_artifact_drift_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_round(run_dir, root)

            draft_payload = run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            first_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            decision_publish = run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            draft_path = reporting_path(run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json")
            modified = load_json(draft_path)
            modified["summary"] = modified["summary"] + " Changed after first publish."
            write_json(draft_path, modified)
            second_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            decision = load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json"))

            self.assertEqual("needs-more-evidence", draft_payload["summary"]["report_status"])
            self.assertEqual("published", first_publish["summary"]["operation"])
            self.assertEqual("completed", decision_publish["status"])
            self.assertEqual("hold", decision_publish["summary"]["publication_readiness"])
            self.assertIn(
                "submit-council-proposal",
                draft_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "submit-readiness-opinion",
                draft_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "post-board-note",
                draft_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "submit-council-proposal",
                decision_publish["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "submit-readiness-opinion",
                decision_publish["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "post-board-note",
                decision_publish["board_handoff"]["suggested_next_skills"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                decision["sociologist_report_source"],
            )
            self.assertEqual(
                "missing-environmentalist-report",
                decision["environmentalist_report_source"],
            )
            self.assertTrue(
                decision["observed_inputs"]["sociologist_report_artifact_present"]
            )
            self.assertTrue(
                decision["observed_inputs"]["sociologist_report_present"]
            )
            self.assertFalse(
                decision["observed_inputs"][
                    "environmentalist_report_artifact_present"
                ]
            )
            self.assertFalse(
                decision["observed_inputs"]["environmentalist_report_present"]
            )
            self.assertEqual("completed", second_publish["status"])
            self.assertEqual("noop", second_publish["summary"]["operation"])

    def test_final_publication_ready_round_collects_reports_and_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            publication_payload = run_script(script_path("materialize-final-publication"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            publication_noop = run_script(script_path("materialize-final-publication"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            publication = load_json(reporting_path(run_dir, f"final_publication_{ROUND_ID}.json"))

            self.assertEqual("completed", publication_payload["status"])
            self.assertEqual("ready-for-release", publication_payload["summary"]["publication_status"])
            self.assertEqual("noop", publication_noop["summary"]["operation"])
            self.assertEqual("release", publication["publication_posture"])
            self.assertEqual(
                "decision-maker-environmental-policy-report",
                publication["decision_maker_report"]["report_type"],
            )
            self.assertGreaterEqual(len(publication["decision_maker_report"]["sections"]), 8)
            self.assertEqual(
                publication["decision_maker_report"]["evidence_index"],
                publication["evidence_index"],
            )
            self.assertIn("citation-index", publication["published_sections"])
            self.assertIn("uncertainty-register", publication["published_sections"])
            self.assertIn("remaining-disputes", publication["published_sections"])
            self.assertEqual("missing-board", publication["board_state_source"])
            self.assertEqual("missing-coverage", publication["coverage_source"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                publication["reporting_handoff_source"],
            )
            self.assertEqual("deliberation-plane-council-decision", publication["decision_source"])
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                publication["report_basis_source"],
            )
            self.assertEqual(
                "deliberation-plane-supervisor",
                publication["supervisor_state_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["sociologist_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["environmentalist_report_source"],
            )
            self.assertTrue(
                publication["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertTrue(publication["observed_inputs"]["decision_artifact_present"])
            self.assertTrue(
                publication["observed_inputs"]["sociologist_report_artifact_present"]
            )
            self.assertTrue(
                publication["observed_inputs"][
                    "environmentalist_report_artifact_present"
                ]
            )
            self.assertEqual(2, len(publication["role_reports"]))
            self.assertIn("role-reports", publication["published_sections"])
            self.assertEqual(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json").resolve().as_posix(), Path(publication["audit_refs"]["decision_path"]).resolve().as_posix())

    def test_final_publication_recovers_from_db_when_report_basis_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json").unlink()

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            publication = load_json(reporting_path(run_dir, f"final_publication_{ROUND_ID}.json"))

            self.assertEqual("completed", publication_payload["status"])
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                publication["report_basis_source"],
            )
            self.assertFalse(
                publication["observed_inputs"]["report_basis_artifact_present"]
            )
            self.assertTrue(publication["observed_inputs"]["report_basis_present"])

    def test_final_publication_recovers_supervisor_state_from_db_when_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            (run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json").unlink()

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            publication = load_json(reporting_path(run_dir, f"final_publication_{ROUND_ID}.json"))

            self.assertEqual("completed", publication_payload["status"])
            self.assertEqual(
                "deliberation-plane-supervisor",
                publication["supervisor_state_source"],
            )
            self.assertFalse(
                publication["observed_inputs"]["supervisor_state_artifact_present"]
            )
            self.assertTrue(publication["observed_inputs"]["supervisor_state_present"])

    def test_final_publication_recovers_from_db_when_reporting_artifacts_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"council_decision_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"expert_report_sociologist_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"expert_report_environmentalist_{ROUND_ID}.json").unlink()

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            publication = load_json(reporting_path(run_dir, f"final_publication_{ROUND_ID}.json"))

            self.assertEqual("completed", publication_payload["status"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                publication["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-council-decision",
                publication["decision_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["sociologist_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["environmentalist_report_source"],
            )
            self.assertFalse(
                publication["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertFalse(publication["observed_inputs"]["decision_artifact_present"])
            self.assertFalse(
                publication["observed_inputs"]["sociologist_report_artifact_present"]
            )
            self.assertFalse(
                publication["observed_inputs"][
                    "environmentalist_report_artifact_present"
                ]
            )
            self.assertTrue(publication["observed_inputs"]["reporting_handoff_present"])
            self.assertTrue(publication["observed_inputs"]["decision_present"])
            self.assertTrue(publication["observed_inputs"]["sociologist_report_present"])
            self.assertTrue(
                publication["observed_inputs"]["environmentalist_report_present"]
            )

    def test_final_publication_hold_round_materializes_hold_artifact_and_guards_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_round(run_dir, root)

            run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            first_publication = run_script(script_path("materialize-final-publication"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            publication_path = reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")
            modified = load_json(publication_path)
            modified["publication_summary"] = modified["publication_summary"] + " Changed after first publish."
            write_json(publication_path, modified)
            second_publication = run_script(script_path("materialize-final-publication"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
            first_payload = load_json(publication_path)

            self.assertEqual("hold-release", first_publication["summary"]["publication_status"])
            self.assertEqual("withhold", first_payload["publication_posture"])
            self.assertEqual(
                "missing-sociologist-report",
                first_payload["sociologist_report_source"],
            )
            self.assertEqual(
                "missing-environmentalist-report",
                first_payload["environmentalist_report_source"],
            )
            self.assertFalse(
                first_payload["observed_inputs"]["sociologist_report_artifact_present"]
            )
            self.assertFalse(
                first_payload["observed_inputs"][
                    "environmentalist_report_artifact_present"
                ]
            )
            self.assertEqual("blocked", second_publication["status"])
            self.assertTrue(any(item["code"] == "overwrite-blocked" for item in second_publication["warnings"]))


if __name__ == "__main__":
    unittest.main()

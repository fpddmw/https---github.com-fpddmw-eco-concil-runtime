from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    report_basis_path,
    reporting_path,
    request_and_approve_skill_approval,
    request_and_approve_transition,
    run_kernel,
    run_script,
    script_path,
    seed_analysis_chain,
    submit_ready_council_support,
)

RUN_ID = "run-reporting-001"
ROUND_ID = "round-reporting-001"


def approve_report_basis_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="freeze-report-basis",
        rationale="Approve report_basis for reporting workflow coverage.",
    )


def prepare_optional_analysis_for_supervision(run_dir: Path) -> None:
    next_actions_request_id = request_and_approve_skill_approval(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        skill_name="propose-next-actions",
        requested_actor_role="moderator",
        rationale="Approve optional-analysis next-actions execution for reporting workflow coverage.",
    )
    run_kernel(
        "run-skill",
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--skill-name",
        "propose-next-actions",
        "--skill-approval-request-id",
        next_actions_request_id,
    )

    readiness_request_id = request_and_approve_skill_approval(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        skill_name="summarize-round-readiness",
        requested_actor_role="moderator",
        rationale="Approve optional-analysis readiness execution for reporting workflow coverage.",
    )
    run_kernel(
        "run-skill",
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--skill-name",
        "summarize-round-readiness",
        "--skill-approval-request-id",
        readiness_request_id,
    )


def seed_ready_reporting_context(
    run_dir: Path,
    root: Path,
    *,
    note_text: str,
    hypothesis_title: str = "Smoke over NYC was materially significant",
    hypothesis_statement: str = "Public smoke reports are backed by elevated PM2.5 observations.",
    hypothesis_status: str = "active",
    owner_role: str = "environmentalist",
    confidence: str = "0.93",
) -> dict[str, object]:
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
        note_text,
        "--linked-artifact-ref",
        evidence_ref,
    )
    hypothesis_payload = run_script(
        script_path("update-hypothesis-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        hypothesis_title,
        "--statement",
        hypothesis_statement,
        "--status",
        hypothesis_status,
        "--owner-role",
        owner_role,
        "--linked-claim-id",
        issue_id,
        "--linked-artifact-ref",
        evidence_ref,
        "--confidence",
        confidence,
    )
    return {
        "outputs": outputs,
        "evidence_ref": evidence_ref,
        "issue_id": issue_id,
        "hypothesis_payload": hypothesis_payload,
    }


class ReportingWorkflowTests(unittest.TestCase):
    def test_reporting_handoff_and_decision_finalize_frozen_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_reporting_context(
                run_dir,
                root,
                note_text="Round is ready to move into reporting and decision drafting.",
            )

            prepare_optional_analysis_for_supervision(run_dir)
            approve_report_basis_transition(run_dir)
            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_payload = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"))
            decision_artifact = load_json(reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json"))
            report_basis_artifact = load_json(report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json"))

            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertEqual("reporting-ready", handoff_artifact["handoff_status"])
            self.assertTrue(handoff_payload["summary"]["reporting_ready"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual([], handoff_artifact["reporting_blockers"])
            self.assertEqual("frozen", handoff_artifact["report_basis_status"])
            self.assertEqual([], handoff_artifact["key_findings"])
            self.assertEqual(
                "decision-maker-report-evidence-packet",
                handoff_artifact["evidence_packet"]["packet_kind"],
            )
            self.assertEqual(
                "moderator-decision-memo-packet",
                handoff_artifact["decision_packet"]["packet_kind"],
            )
            self.assertEqual(
                "decision-maker-policy-report-packet",
                handoff_artifact["report_packet"]["packet_kind"],
            )
            self.assertEqual(
                handoff_artifact["evidence_packet"]["evidence_index"],
                handoff_artifact["evidence_index"],
            )
            self.assertGreaterEqual(len(handoff_artifact["evidence_index"]), 1)
            self.assertIn(
                "citation-index",
                [
                    item["section_key"]
                    for item in handoff_artifact["report_packet"]["recommended_sections"]
                ],
            )
            self.assertEqual("deliberation-plane", report_basis_artifact["board_state_source"])
            self.assertEqual("missing-coverage", report_basis_artifact["coverage_source"])
            self.assertEqual(
                "deliberation-plane-readiness",
                report_basis_artifact["readiness_source"],
            )
            self.assertEqual(
                "missing-board-brief",
                report_basis_artifact["board_brief_source"],
            )
            self.assertFalse(
                report_basis_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertFalse(
                report_basis_artifact["observed_inputs"]["board_brief_present"]
            )
            self.assertTrue(
                report_basis_artifact["observed_inputs"]["readiness_artifact_present"]
            )
            self.assertTrue(report_basis_artifact["observed_inputs"]["readiness_present"])
            self.assertTrue(
                report_basis_artifact["observed_inputs"]["next_actions_artifact_present"]
            )
            self.assertTrue(
                report_basis_artifact["observed_inputs"]["next_actions_present"]
            )
            self.assertEqual("deliberation-plane", handoff_artifact["board_state_source"])
            self.assertEqual("missing-coverage", handoff_artifact["coverage_source"])
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                handoff_artifact["report_basis_source"],
            )
            self.assertEqual(
                "deliberation-plane-readiness",
                handoff_artifact["readiness_source"],
            )
            self.assertEqual(
                "deliberation-plane-supervisor",
                handoff_artifact["supervisor_state_source"],
            )
            self.assertEqual("missing-board-brief", handoff_artifact["board_brief_source"])
            self.assertFalse(
                handoff_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertEqual("completed", handoff_payload["deliberation_sync"]["status"])
            self.assertIn(
                handoff_payload["analysis_sync"]["status"],
                {"completed", "existing-result-set", "missing-coverage"},
            )
            self.assertTrue(
                handoff_artifact["observed_inputs"]["report_basis_artifact_present"]
            )
            self.assertTrue(handoff_artifact["observed_inputs"]["report_basis_present"])
            self.assertTrue(
                handoff_artifact["observed_inputs"][
                    "supervisor_state_artifact_present"
                ]
            )
            self.assertTrue(
                handoff_artifact["observed_inputs"]["supervisor_state_present"]
            )
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual("ready", decision_artifact["publication_readiness"])
            self.assertFalse(decision_artifact["next_round_required"])
            self.assertEqual(
                "moderator-decision-memo-packet",
                decision_artifact["decision_packet"]["packet_kind"],
            )
            self.assertGreaterEqual(len(decision_artifact["memo_sections"]), 4)
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                decision_artifact["reporting_handoff_source"],
            )
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                decision_artifact["report_basis_source"],
            )
            self.assertEqual("deliberation-plane", decision_artifact["board_state_source"])
            self.assertEqual("missing-coverage", decision_artifact["coverage_source"])
            self.assertTrue(
                decision_artifact["observed_inputs"][
                    "reporting_handoff_artifact_present"
                ]
            )
            self.assertTrue(
                decision_artifact["observed_inputs"]["reporting_handoff_present"]
            )
            self.assertEqual(report_basis_artifact["basis_id"], handoff_artifact["report_basis_id"])

    def test_reporting_handoff_and_decision_recover_from_db_when_report_basis_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_reporting_context(
                run_dir,
                root,
                note_text="Round is ready to move into reporting even if report-basis export is removed.",
            )

            prepare_optional_analysis_for_supervision(run_dir)
            approve_report_basis_transition(run_dir)
            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                basis_count = connection.execute(
                    "SELECT COUNT(*) FROM report_basis_freeze_records WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
                item_count = connection.execute(
                    "SELECT COUNT(*) FROM report_basis_freeze_items WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            handoff_payload = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(
                reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")
            )
            decision_artifact = load_json(
                reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
            )

            self.assertGreater(basis_count, 0)
            self.assertEqual(0, item_count)
            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                handoff_artifact["report_basis_source"],
            )
            self.assertFalse(
                handoff_artifact["observed_inputs"]["report_basis_artifact_present"]
            )
            self.assertTrue(handoff_artifact["observed_inputs"]["report_basis_present"])
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual(
                "deliberation-plane-report-basis-freeze",
                decision_artifact["report_basis_source"],
            )

    def test_reporting_handoff_recovers_supervisor_state_from_db_when_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_reporting_context(
                run_dir,
                root,
                note_text="Reporting handoff should recover supervisor state directly from the deliberation DB.",
            )

            prepare_optional_analysis_for_supervision(run_dir)
            approve_report_basis_transition(run_dir)
            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            supervisor_path = (run_dir / "runtime" / f"supervisor_state_{ROUND_ID}.json")
            supervisor_path.unlink()

            handoff_payload = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(
                reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")
            )
            decision_artifact = load_json(
                reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
            )

            self.assertEqual("reporting-ready", handoff_payload["summary"]["handoff_status"])
            self.assertTrue(handoff_artifact["reporting_ready"])
            self.assertEqual(
                "deliberation-plane-supervisor",
                handoff_artifact["supervisor_state_source"],
            )
            self.assertFalse(
                handoff_artifact["observed_inputs"][
                    "supervisor_state_artifact_present"
                ]
            )
            self.assertTrue(
                handoff_artifact["observed_inputs"]["supervisor_state_present"]
            )
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertTrue(decision_artifact["reporting_ready"])

    def test_decision_draft_recovers_from_db_when_handoff_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_reporting_context(
                run_dir,
                root,
                note_text="Decision drafting should recover from the deliberation-plane handoff record.",
            )

            prepare_optional_analysis_for_supervision(run_dir)
            approve_report_basis_transition(run_dir)
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
            reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                handoff_count = connection.execute(
                    "SELECT COUNT(*) FROM reporting_handoffs WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            decision_payload = run_script(
                script_path("draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_artifact = load_json(
                reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json")
            )

            self.assertGreater(handoff_count, 0)
            self.assertEqual("finalize", decision_payload["summary"]["moderator_status"])
            self.assertEqual(
                "deliberation-plane-reporting-handoff",
                decision_artifact["reporting_handoff_source"],
            )
            self.assertFalse(
                decision_artifact["observed_inputs"][
                    "reporting_handoff_artifact_present"
                ]
            )
            self.assertTrue(
                decision_artifact["observed_inputs"]["reporting_handoff_present"]
            )

    def test_reporting_handoff_and_decision_hold_withheld_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            coverage_ref = primary_successor_evidence_ref(outputs)
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
                "Round has DB-backed successor evidence but an open challenge should hold reporting.",
                "--linked-artifact-ref",
                coverage_ref,
            )
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
                coverage_ref,
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

            prepare_optional_analysis_for_supervision(run_dir)
            approve_report_basis_transition(run_dir)
            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_payload = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision_payload = run_script(
                script_path("draft-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            handoff_artifact = load_json(reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"))
            decision_artifact = load_json(reporting_path(run_dir, f"council_decision_draft_{ROUND_ID}.json"))

            self.assertEqual("investigation-open", handoff_payload["summary"]["handoff_status"])
            self.assertFalse(handoff_artifact["reporting_ready"])
            self.assertIn("report-basis-withheld", handoff_artifact["reporting_blockers"])
            self.assertEqual("withheld", handoff_artifact["report_basis_status"])
            self.assertGreaterEqual(len(handoff_artifact["open_risks"]), 1)
            self.assertGreaterEqual(len(handoff_artifact["recommended_next_actions"]), 1)
            self.assertEqual("missing-board-brief", handoff_artifact["board_brief_source"])
            self.assertFalse(
                handoff_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertFalse(handoff_artifact["observed_inputs"]["board_brief_present"])
            self.assertIn(
                "submit-council-proposal",
                handoff_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "submit-readiness-opinion",
                handoff_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "post-board-note",
                handoff_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertEqual("continue", decision_payload["summary"]["moderator_status"])
            self.assertEqual("hold", decision_artifact["publication_readiness"])
            self.assertTrue(decision_artifact["next_round_required"])
            self.assertEqual("missing-board-brief", decision_artifact["board_brief_source"])
            self.assertFalse(
                decision_artifact["observed_inputs"]["board_brief_artifact_present"]
            )
            self.assertIn("report-basis-withheld", decision_artifact["decision_gating"]["reason_codes"])
            self.assertIn(
                "submit-council-proposal",
                decision_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "submit-readiness-opinion",
                decision_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "post-board-note",
                decision_payload["board_handoff"]["suggested_next_skills"],
            )


if __name__ == "__main__":
    unittest.main()

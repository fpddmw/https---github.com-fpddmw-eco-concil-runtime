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
        "--status", "active", "--owner-role", "environmental-investigator",
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
    run_script(
        script_path("summarize-round-readiness"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
    )
    approve_report_basis_transition(run_dir)
    run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)


def minimal_narrative_draft() -> dict[str, object]:
    sections = []
    for index, section_id in enumerate(
        (
            "executive-summary",
            "key-points",
            "what-happened",
            "evidence-basis",
            "council-reasoning",
            "limitations",
            "decision-implications",
            "audit-trail",
        ),
        start=1,
    ):
        sections.append(
            {
                "section_id": section_id,
                "title": section_id.replace("-", " ").title(),
                "status": "limitations-visible"
                if section_id == "limitations"
                else "traceability-index"
                if section_id == "audit-trail"
                else "draft",
                "paragraphs": [f"Reader-facing paragraph {index} with bounded report language."],
                "evidence_refs": ["signal:test-report-quality-001"],
            }
        )
    return {
        "schema_version": "narrative-report-draft-v1",
        "template_version": "test-template",
        "draft_id": "narrative-report-draft-quality-test",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "basis_round_id": ROUND_ID,
        "title": "Bounded Test Narrative Report",
        "status": "draft",
        "claim_boundary": {
            "summary": "Claims are limited to recorded council/reporting artifacts and cited refs.",
            "forbidden_claims": ["new factual claim not present in council/reporting basis"],
        },
        "sections": sections,
        "reader_guidance": {"primary_audience": "human reviewer"},
        "evidence_refs": ["signal:test-report-quality-001"],
        "audit_refs": ["signal:test-report-quality-001"],
        "source_material": {
            "reporting_artifacts": [],
            "council_object_counts": {},
            "public_discourse_summary": {},
        },
    }


def write_narrative_draft(run_dir: Path, draft: dict[str, object]) -> Path:
    draft_path = reporting_path(run_dir, f"narrative_report_draft_{ROUND_ID}.json")
    write_json(draft_path, draft)
    return draft_path


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

            social_investigator_draft = run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            environmental_investigator_draft = run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            soc_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            env_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            decision_publish = run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            soc_report = load_json(reporting_path(run_dir, f"expert_report_social_investigator_{ROUND_ID}.json"))
            env_report = load_json(reporting_path(run_dir, f"expert_report_environmental_investigator_{ROUND_ID}.json"))
            soc_draft = load_json(reporting_path(run_dir, f"expert_report_draft_social_investigator_{ROUND_ID}.json"))
            decision = load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json"))

            self.assertEqual("ready-to-publish", social_investigator_draft["summary"]["report_status"])
            self.assertEqual("ready-to-publish", environmental_investigator_draft["summary"]["report_status"])
            self.assertEqual("published", soc_publish["summary"]["operation"])
            self.assertEqual("published", env_publish["summary"]["operation"])
            self.assertEqual("published", decision_publish["summary"]["operation"])
            self.assertEqual("ready-to-publish", soc_publish["summary"]["source_report_status"])
            self.assertEqual("canonical-published", soc_publish["summary"]["canonical_report_status"])
            self.assertEqual("social-investigator", soc_report["agent_role"])
            self.assertEqual("environmental-investigator", env_report["agent_role"])
            self.assertEqual("canonical-published", soc_report["status"])
            self.assertEqual("ready-to-publish", soc_report["source_report_status"])
            self.assertEqual("canonical-published", env_report["status"])
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
                decision["social_investigator_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                decision["environmental_investigator_report_source"],
            )
            self.assertTrue(decision["observed_inputs"]["decision_artifact_present"])
            self.assertTrue(decision["observed_inputs"]["decision_present"])
            self.assertTrue(
                decision["observed_inputs"]["social_investigator_report_artifact_present"]
            )
            self.assertTrue(
                decision["observed_inputs"]["social_investigator_report_present"]
            )
            self.assertTrue(
                decision["observed_inputs"][
                    "environmental_investigator_report_artifact_present"
                ]
            )
            self.assertTrue(
                decision["observed_inputs"]["environmental_investigator_report_present"]
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
                "social-investigator",
            )
            draft_artifact = load_json(
                reporting_path(run_dir, f"expert_report_draft_social_investigator_{ROUND_ID}.json")
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
                "social-investigator",
            )
            reporting_path(run_dir, f"expert_report_draft_social_investigator_{ROUND_ID}.json").unlink()

            publish_payload = run_script(
                script_path("publish-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--role",
                "social-investigator",
            )
            report_artifact = load_json(
                reporting_path(run_dir, f"expert_report_social_investigator_{ROUND_ID}.json")
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
                "social-investigator",
            )
            execute_db(
                db_path,
                """
                DELETE FROM expert_report_records
                WHERE run_id = ? AND round_id = ? AND report_stage = ? AND agent_role = ?
                """,
                (RUN_ID, ROUND_ID, "draft", "social-investigator"),
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
                "social-investigator",
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

            draft_payload = run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            first_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            decision_publish = run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            draft_path = reporting_path(run_dir, f"expert_report_draft_social_investigator_{ROUND_ID}.json")
            modified = load_json(draft_path)
            modified["summary"] = modified["summary"] + " Changed after first publish."
            write_json(draft_path, modified)
            second_publish = run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            decision = load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json"))

            self.assertEqual("needs-more-evidence", draft_payload["summary"]["report_status"])
            self.assertEqual("published", first_publish["summary"]["operation"])
            self.assertEqual("canonical-needs-more-evidence", first_publish["summary"]["canonical_report_status"])
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
                decision["social_investigator_report_source"],
            )
            self.assertEqual(
                "missing-environmental-investigator-report",
                decision["environmental_investigator_report_source"],
            )
            self.assertTrue(
                decision["observed_inputs"]["social_investigator_report_artifact_present"]
            )
            self.assertTrue(
                decision["observed_inputs"]["social_investigator_report_present"]
            )
            self.assertFalse(
                decision["observed_inputs"][
                    "environmental_investigator_report_artifact_present"
                ]
            )
            self.assertFalse(
                decision["observed_inputs"]["environmental_investigator_report_present"]
            )
            self.assertEqual("completed", second_publish["status"])
            self.assertEqual("noop", second_publish["summary"]["operation"])

    def test_final_publication_ready_round_collects_reports_and_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
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
            recommendation_section = next(
                section
                for section in publication["decision_maker_report"]["sections"]
                if section["section_key"] == "recommendations"
            )
            self.assertEqual("not-in-scope", recommendation_section["status"])
            self.assertEqual([], publication["decision_maker_report"]["policy_recommendations"])
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
                publication["social_investigator_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["environmental_investigator_report_source"],
            )
            self.assertTrue(
                publication["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertTrue(publication["observed_inputs"]["decision_artifact_present"])
            self.assertTrue(
                publication["observed_inputs"]["social_investigator_report_artifact_present"]
            )
            self.assertTrue(
                publication["observed_inputs"][
                    "environmental_investigator_report_artifact_present"
                ]
            )
            self.assertEqual(2, len(publication["role_reports"]))
            self.assertEqual(
                {"canonical-published"},
                {row["status"] for row in publication["role_reports"]},
            )
            self.assertIn("role-reports", publication["published_sections"])
            self.assertEqual(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json").resolve().as_posix(), Path(publication["audit_refs"]["decision_path"]).resolve().as_posix())

    def test_final_publication_recovers_from_db_when_report_basis_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
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

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
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

            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "social-investigator")
            run_script(script_path("publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmental-investigator")
            run_script(script_path("publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"council_decision_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"expert_report_social_investigator_{ROUND_ID}.json").unlink()
            reporting_path(run_dir, f"expert_report_environmental_investigator_{ROUND_ID}.json").unlink()

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
                publication["social_investigator_report_source"],
            )
            self.assertEqual(
                "deliberation-plane-expert-report",
                publication["environmental_investigator_report_source"],
            )
            self.assertFalse(
                publication["observed_inputs"]["reporting_handoff_artifact_present"]
            )
            self.assertFalse(publication["observed_inputs"]["decision_artifact_present"])
            self.assertFalse(
                publication["observed_inputs"]["social_investigator_report_artifact_present"]
            )
            self.assertFalse(
                publication["observed_inputs"][
                    "environmental_investigator_report_artifact_present"
                ]
            )
            self.assertTrue(publication["observed_inputs"]["reporting_handoff_present"])
            self.assertTrue(publication["observed_inputs"]["decision_present"])
            self.assertTrue(publication["observed_inputs"]["social_investigator_report_present"])
            self.assertTrue(
                publication["observed_inputs"]["environmental_investigator_report_present"]
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
                "missing-social-investigator-report",
                first_payload["social_investigator_report_source"],
            )
            self.assertEqual(
                "missing-environmental-investigator-report",
                first_payload["environmental_investigator_report_source"],
            )
            self.assertFalse(
                first_payload["observed_inputs"]["social_investigator_report_artifact_present"]
            )
            self.assertFalse(
                first_payload["observed_inputs"][
                    "environmental_investigator_report_artifact_present"
                ]
            )
            self.assertEqual("blocked", second_publication["status"])
            self.assertTrue(any(item["code"] == "overwrite-blocked" for item in second_publication["warnings"]))

    def test_narrative_validator_blocks_representative_public_opinion_upgrade(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][0]["paragraphs"] = [
                "Overall public opinion shows that affected residents were mostly angry about the event."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertEqual("invalid", validation["status"])
            self.assertIn(
                "unsupported-public-opinion-claim",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_sample_percentages_without_public_discourse_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "Health-risk labels appear in 60% of the YouTube comment public discourse."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "sample-distribution-without-public-discourse-basis",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_allows_public_opinion_language_with_representative_design_mission(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            write_json(
                run_dir / "mission.json",
                {
                    "objective": "Analyze public opinion using a representative sampling design.",
                    "sampling_design": "representative sampling design with survey weights",
                },
            )
            draft = minimal_narrative_draft()
            draft["sections"][0]["paragraphs"] = [
                "Overall public opinion is reported only because the mission records a representative sampling design."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertEqual("valid", validation["status"])
            self.assertNotIn(
                "unsupported-public-opinion-claim",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_gdelt_tone_as_public_sentiment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "GDELT V2Tone proves public sentiment was negative across the issue."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "gdelt-tone-public-sentiment",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_gdelt_tone_public_emotion_paraphrase(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The GDELT tone average indicates public mood became negative."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "gdelt-tone-public-sentiment",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_public_percentage_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The report states that 60% of the public opposed the water release policy."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "unsupported-public-opinion-claim",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_platform_sample_generalization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The YouTube comments show residents think the policy failed."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "platform-or-docket-sample-generalized",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_formal_comments_as_public_opinion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "Formal comment distribution shows public opinion: 60% of the public opposed the policy."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "formal-comment-distribution-as-public-opinion",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_formal_stance_distribution_without_annotation_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "Formal comment stance distribution shows most comments oppose the policy."
            ]
            draft["source_material"]["formal_comment_basis"] = {
                "comment_listing_count": 125,
                "comment_detail_count": 1,
                "readable_formal_signal_count": 1,
                "annotation_count": 0,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))
            issue_codes = [item["code"] for item in validation["issues"]]

            self.assertEqual("blocked", payload["status"])
            self.assertIn("formal-comment-structure-without-annotation-basis", issue_codes)
            self.assertIn("formal-comment-stance-distribution-insufficient-basis", issue_codes)

    def test_narrative_validator_requires_formal_candidate_audit_for_issue_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "Formal participation main issues include health risk and implementation burden in this sample."
            ]
            draft["source_material"]["formal_comment_basis"] = {
                "readable_formal_signal_count": 3,
                "annotation_count": 3,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "formal-comment-structure-without-candidate-audit",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_accepts_bounded_formal_annotation_language(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                (
                    "In this sample-local formal participation sample (n=2), non-exclusive "
                    "formal_stance_hints identify support and oppose cues; these annotations "
                    "are not representative public opinion."
                )
            ]
            draft["source_material"]["formal_comment_basis"] = {
                "candidate_audit_count": 1,
                "candidate_audit_ref": "analytics/formal_comment_candidate_audit_test.json",
                "readable_formal_signal_count": 2,
                "annotation_count": 2,
                "attachment_text_signal_count": 1,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))
            issue_codes = [item["code"] for item in validation["issues"]]

            self.assertEqual("completed", payload["status"])
            self.assertNotIn("formal-comment-structure-without-annotation-basis", issue_codes)
            self.assertNotIn("formal-comment-stance-distribution-insufficient-basis", issue_codes)

    def test_narrative_validator_warns_when_attachment_limit_not_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The formal comment attachment text supports the agency response."
            ]
            draft["source_material"]["formal_comment_basis"] = {
                "readable_formal_signal_count": 2,
                "annotation_count": 2,
                "limitations": ["requires-attachment-text"],
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertIn(
                "formal-attachment-text-limitation-missing",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_sample_fraction_totalization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "Sample-local concern labels appear in 50% and anger labels appear in 50%, so they sum to 100% opinion composition."
            ]
            draft["source_material"]["public_discourse_summary"] = {
                "path": "analytics/public_discourse_sample_summary_complete.json",
                "summary_id": "public-summary-complete",
                "status": "completed",
                "advisory_only": True,
            }
            write_json(
                analytics_path(run_dir, "public_discourse_sample_summary_complete.json"),
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "status": "completed",
                    "summary_id": "public-summary-complete",
                    "sample_definition": {"run_id": RUN_ID, "round_id": ROUND_ID},
                    "source_family_counts": [],
                    "discourse_lane_counts": [],
                    "warnings": [],
                    "evidence_refs": ["signal:test-public-summary-001"],
                    "observed_inputs": {
                        "corpus_path": "analytics/public_discourse_corpus_complete.json",
                        "coverage_audit_path": "analytics/public_discourse_coverage_audit_complete.json",
                        "aggregation_path": "analytics/public_discourse_annotation_aggregation_complete.json",
                    },
                    "distribution_denominators": {
                        "label_family_denominators": {"social_affect_labels": 2},
                    },
                    "distribution_use_policy": {
                        "label_sets_are_non_exclusive": True,
                        "sample_fractions_are_sample_local": True,
                        "do_not_sum_to_population_opinion": True,
                        "requires_council_uptake_before_reporting": True,
                    },
                    "social_affect_distribution": [
                        {"label": "concern", "annotated_signal_count": 1, "sample_fraction": 0.5}
                    ],
                },
            )
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "sample-fractions-totalized-as-opinion-composition",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_warns_for_small_mixed_public_sample_without_denominators(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            write_json(
                analytics_path(run_dir, "public_discourse_sample_summary_mixed_small.json"),
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "status": "completed",
                    "summary_id": "public-summary-mixed-small",
                    "sample_definition": {"run_id": RUN_ID, "round_id": ROUND_ID},
                    "sample_count": 4,
                    "source_family_counts": [
                        {"source_family": "youtube-public-discourse", "signal_count": 2},
                        {"source_family": "regulationsgov-formal-comments", "signal_count": 2},
                    ],
                    "discourse_lane_counts": [
                        {"discourse_lane": "social_sample_affect", "signal_count": 2},
                        {"discourse_lane": "formal_public_comment_sample", "signal_count": 2},
                    ],
                    "warnings": [],
                    "evidence_refs": ["signal:test-public-summary-001"],
                    "distribution_use_policy": {
                        "label_sets_are_non_exclusive": True,
                        "sample_fractions_are_sample_local": True,
                        "do_not_sum_to_population_opinion": True,
                        "requires_council_uptake_before_reporting": True,
                    },
                    "social_affect_distribution": [
                        {"label": "concern", "annotated_signal_count": 2, "sample_fraction": 0.5}
                    ],
                },
            )
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "Concern labels appear in about 50% of public discourse records."
            ]
            draft["source_material"]["public_discourse_summary"] = {
                "path": "analytics/public_discourse_sample_summary_mixed_small.json",
                "summary_id": "public-summary-mixed-small",
                "status": "completed",
                "advisory_only": True,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))
            issue_codes = [item["code"] for item in validation["issues"]]

            self.assertEqual("blocked", payload["status"])
            self.assertIn("small-public-discourse-sample-boundary-missing", issue_codes)
            self.assertIn("mixed-source-family-denominator-missing", issue_codes)
            self.assertIn("public-discourse-denominator-missing", issue_codes)
            self.assertIn("public-discourse-corpus-basis-missing", issue_codes)
            self.assertIn("public-discourse-coverage-audit-basis-missing", issue_codes)
            self.assertIn("public-discourse-aggregation-basis-missing", issue_codes)

    def test_narrative_validator_blocks_source_narrative_as_physical_attribution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The source narrative proves physical source attribution to a specific fire."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "source-narrative-as-physical-attribution",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_warns_when_source_narrative_boundary_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The report discusses source narrative labels from the sampled public records."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertIn(
                "source-narrative-boundary-missing",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_strong_attribution_without_model_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The environmental evidence proves source attribution to a specific fire."
            ]
            draft["source_material"]["council_object_counts"] = {
                "finding": 1,
                "review-comment": 1,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "strong-attribution-without-attribution-model",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_blocks_environment_trend_without_aggregation_or_item_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "USBR RISE water level trend peaked in 2023 and then recovered by late 2024."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("blocked", payload["status"])
            self.assertIn(
                "environment-state-claim-without-aggregation",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_allows_environment_item_example_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "As an item-level example, one USBR RISE water level peak row is cited; this is not a trend."
            ]
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertNotIn(
                "environment-state-claim-without-aggregation",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_warns_when_environment_helper_is_not_carried(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][3]["paragraphs"] = [
                "The aggregate-environment-evidence helper is cited as descriptive environment coverage."
            ]
            draft["sections"][3]["evidence_refs"] = [
                "analytics/environment_evidence_aggregation_round-reporting-publish-001.json:$.aggregation"
            ]
            draft["source_material"]["reporting_artifacts"] = []
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertEqual("valid", validation["status"])
            self.assertIn(
                "optional-analysis-helper-not-carried",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_warns_when_public_helper_is_not_carried(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"].append(
                {
                    "section_id": "public-discourse-deepening",
                    "title": "Public Discourse Addendum",
                    "status": "advisory-addendum",
                    "paragraphs": [
                        "The supplied public discourse summary is used only as sample-local addendum material."
                    ],
                    "evidence_refs": ["signal:test-public-summary-001"],
                }
            )
            draft["source_material"] = {
                "reporting_artifacts": [],
                "council_object_counts": {
                    "finding": 0,
                    "evidence-bundle": 0,
                    "proposal": 0,
                    "agent-position": 0,
                    "readiness-opinion": 0,
                    "round-synthesis": 0,
                },
                "public_discourse_summary": {
                    "path": "analytics/public_discourse_sample_summary_test.json",
                    "summary_id": "public-summary-not-carried",
                    "status": "completed",
                    "advisory_only": True,
                },
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertEqual("valid", validation["status"])
            self.assertIn(
                "optional-analysis-not-carried",
                [item["code"] for item in validation["issues"]],
            )

    def test_narrative_validator_warns_when_public_summary_contract_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            summary_path = analytics_path(run_dir, "public_discourse_sample_summary_incomplete.json")
            write_json(
                summary_path,
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "status": "completed",
                    "summary_id": "public-summary-incomplete",
                    "sample_count": 2,
                    "social_affect_distribution": [
                        {"label": "concern", "annotated_signal_count": 1, "sample_fraction": 0.5}
                    ],
                },
            )
            draft = minimal_narrative_draft()
            draft["sections"].append(
                {
                    "section_id": "public-discourse-deepening",
                    "title": "Public Discourse Addendum",
                    "status": "advisory-addendum",
                    "paragraphs": [
                        "Sample-local concern labels appear in about 50% of the annotated sample."
                    ],
                    "evidence_refs": ["signal:test-public-summary-001"],
                }
            )
            draft["source_material"]["public_discourse_summary"] = {
                "path": "analytics/public_discourse_sample_summary_incomplete.json",
                "summary_id": "public-summary-incomplete",
                "status": "completed",
                "advisory_only": True,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))
            issue_codes = [item["code"] for item in validation["issues"]]

            self.assertEqual("blocked", payload["status"])
            self.assertEqual("invalid", validation["status"])
            self.assertIn("public-summary-contract-incomplete", issue_codes)
            self.assertIn("public-summary-policy-boundary-missing", issue_codes)
            self.assertIn("public-discourse-corpus-basis-missing", issue_codes)
            self.assertIn("public-discourse-coverage-audit-basis-missing", issue_codes)
            self.assertIn("public-discourse-aggregation-basis-missing", issue_codes)

    def test_narrative_validator_accepts_complete_public_summary_contract(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            summary_path = analytics_path(run_dir, "public_discourse_sample_summary_complete.json")
            write_json(
                summary_path,
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "status": "completed",
                    "summary_id": "public-summary-complete",
                    "sample_definition": {
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "round_scope": "current",
                        "sample_boundary": "DB-visible normalized public/formal text sample only",
                    },
                    "sample_count": 2,
                    "source_family_counts": [
                        {"source_family": "youtube-public-discourse", "signal_count": 2}
                    ],
                    "discourse_lane_counts": [
                        {"discourse_lane": "social_sample_affect", "signal_count": 2}
                    ],
                    "social_affect_distribution": [
                        {"label": "concern", "annotated_signal_count": 1, "sample_fraction": 0.5}
                    ],
                    "distribution_use_policy": {
                        "schema_version": "public-discourse-distribution-use-policy-v1",
                        "label_sets_are_non_exclusive": True,
                        "sample_fractions_are_sample_local": True,
                        "do_not_sum_to_population_opinion": True,
                        "requires_council_uptake_before_reporting": True,
                        "gdelt_tone_boundary": "media_or_document_tone_not_public_sentiment",
                        "source_narrative_boundary": "public_source_narrative_cue_not_physical_source_attribution",
                    },
                    "observed_inputs": {
                        "corpus_path": "analytics/public_discourse_corpus_complete.json",
                        "coverage_audit_path": "analytics/public_discourse_coverage_audit_complete.json",
                        "aggregation_path": "analytics/public_discourse_annotation_aggregation_complete.json",
                    },
                    "distribution_denominators": {
                        "label_family_denominators": {"social_affect_labels": 2},
                    },
                    "warnings": [],
                    "evidence_refs": ["signal:test-public-summary-001"],
                },
            )
            draft = minimal_narrative_draft()
            draft["sections"].append(
                {
                    "section_id": "public-discourse-deepening",
                    "title": "Public Discourse Addendum",
                    "status": "advisory-addendum",
                    "paragraphs": [
                        "Sample-local concern labels appear in about 50% of the annotated sample."
                    ],
                    "evidence_refs": ["signal:test-public-summary-001"],
                }
            )
            draft["source_material"]["public_discourse_summary"] = {
                "path": "analytics/public_discourse_sample_summary_complete.json",
                "summary_id": "public-summary-complete",
                "status": "completed",
                "advisory_only": True,
            }
            write_narrative_draft(run_dir, draft)

            payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            validation = load_json(reporting_path(run_dir, f"narrative_report_validation_{ROUND_ID}.json"))
            issue_codes = [item["code"] for item in validation["issues"]]

            self.assertEqual("completed", payload["status"])
            self.assertEqual("valid", validation["status"])
            self.assertNotIn("public-summary-contract-incomplete", issue_codes)
            self.assertNotIn("public-summary-policy-boundary-missing", issue_codes)
            self.assertIn("public-discourse-label-nonexclusive-boundary-missing", issue_codes)


if __name__ == "__main__":
    unittest.main()

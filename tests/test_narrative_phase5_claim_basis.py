from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, reporting_path, run_script, script_path, write_json

RUN_ID = "run-narrative-phase5-001"
ROUND_ID = "round-narrative-phase5-001"


def minimal_narrative_draft() -> dict[str, object]:
    sections: list[dict[str, object]] = []
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
                "paragraphs": [f"Reader-facing bounded paragraph {index}."],
                "evidence_refs": ["signal:phase5-test-001"],
            }
        )
    return {
        "schema_version": "narrative-report-draft-v1",
        "template_version": "test-template",
        "draft_id": "narrative-report-draft-phase5-test",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "basis_round_id": ROUND_ID,
        "title": "Phase 5 Claim Basis Test Report",
        "status": "draft",
        "claim_boundary": {
            "summary": "Claims are limited to recorded council/reporting artifacts and cited refs.",
            "forbidden_claims": ["new factual claim not present in council/reporting basis"],
        },
        "sections": sections,
        "reader_guidance": {"primary_audience": "human reviewer"},
        "evidence_refs": ["signal:phase5-test-001"],
        "audit_refs": ["signal:phase5-test-001"],
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


def validate_draft(run_dir: Path, draft: dict[str, object]) -> dict[str, object]:
    draft_path = write_narrative_draft(run_dir, draft)
    payload = run_script(
        script_path("validate-narrative-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--draft-path",
        str(draft_path),
    )
    return load_json(Path(payload["summary"]["output_path"]))


def issue_codes(validation: dict[str, object]) -> set[str]:
    issues = validation.get("issues")
    if not isinstance(issues, list):
        return set()
    return {str(issue.get("code")) for issue in issues if isinstance(issue, dict)}


class NarrativePhase5ClaimBasisTests(unittest.TestCase):
    def test_draft_uses_reporting_handoff_section_brief_as_interaction_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            timeline_path = run_dir / "analytics" / f"fact_policy_public_interaction_timeline_{ROUND_ID}.json"
            write_json(
                reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json"),
                {
                    "schema_version": "reporting-handoff-v1",
                    "handoff_id": "reporting-handoff-phase5-test",
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "report_packet": {
                        "section_briefs": [
                            {
                                "brief_id": "section-brief-fpp-test",
                                "section_key": "fact-policy-public-interaction-timeline",
                                "claim_strength": "bounded-descriptive-context-only",
                                "denominator": {
                                    "interaction_node_count": 1,
                                    "parallel_timeline_node_count": 0,
                                    "environment_signal_count": 1,
                                    "formal_signal_count": 1,
                                    "public_signal_count": 1,
                                },
                                "limitations": ["Same-date visibility does not prove causality."],
                                "candidate_section_claims": [
                                    "Fact/policy-side records and public/media records were visible in the same timeline window."
                                ],
                                "refs": [
                                    {
                                        "artifact_path": str(timeline_path),
                                        "record_locator": "$.interaction_nodes[0]",
                                        "artifact_ref": f"{timeline_path}:$.interaction_nodes[0]",
                                    }
                                ],
                                "source_artifact_path": str(timeline_path),
                            }
                        ],
                        "interaction_timeline_nodes": [
                            {
                                "node_id": "fact-policy-public-interaction-node-test",
                                "node_kind": "interaction",
                                "date": "2023-06-07",
                                "fact_policy_refs": ["signal:fact-policy-001"],
                                "public_media_refs": ["signal:public-media-001"],
                            }
                        ],
                    },
                },
            )

            payload = run_script(
                script_path("draft-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--language",
                "en",
                "--title",
                "Phase 5 Interaction Draft",
            )
            draft = load_json(Path(payload["summary"]["output_path"]))
            sections = draft["sections"]
            self.assertIsInstance(sections, list)
            interaction_sections = [
                section
                for section in sections
                if isinstance(section, dict)
                and section.get("section_id") == "fact-policy-public-interaction"
            ]
            self.assertEqual(1, len(interaction_sections))
            source_material = draft["source_material"]
            self.assertIsInstance(source_material, dict)
            self.assertTrue(source_material["section_briefs"])
            interaction_meta = source_material["interaction_timeline"]
            self.assertIsInstance(interaction_meta, dict)
            self.assertEqual(1, interaction_meta["section_brief_count"])
            self.assertEqual(1, interaction_meta["interaction_node_count"])
            prose = "\n".join(interaction_sections[0]["paragraphs"])
            self.assertIn("bounded-descriptive-context-only", prose)
            self.assertIn("not causality", prose)
            self.assertIn("public-response attribution", prose)

    def test_validator_blocks_interaction_claim_without_timeline_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][0]["paragraphs"] = [
                "The official action drove public response to the policy on the same day."
            ]
            validation = validate_draft(run_dir, draft)
            self.assertEqual("invalid", validation["status"])
            self.assertIn("interaction-claim-without-timeline-basis", issue_codes(validation))

    def test_validator_blocks_policy_evaluation_without_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][0]["paragraphs"] = [
                "The policy was effective and solved the air quality problem."
            ]
            validation = validate_draft(run_dir, draft)
            self.assertEqual("invalid", validation["status"])
            self.assertIn("policy-evaluation-claim-without-basis", issue_codes(validation))

    def test_validator_blocks_responsibility_claim_without_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_narrative_draft()
            draft["sections"][0]["paragraphs"] = [
                "The agency is responsible for the health outcome."
            ]
            validation = validate_draft(run_dir, draft)
            self.assertEqual("invalid", validation["status"])
            self.assertIn("responsibility-claim-without-basis", issue_codes(validation))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    request_and_approve_transition,
    run_kernel,
    run_script,
    runtime_path,
    runtime_src_path,
    script_path,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-report-writing-round-001"
ROUND_ID = "round-report-writing-source"
REPORT_ROUND_ID = "round-report-writing-output"


def seed_mission(root: Path) -> Path:
    mission_path = root / "mission.json"
    write_json(
        mission_path,
        {
            "schema_version": "1.0.0",
            "run_id": RUN_ID,
            "topic": "Narrative report workflow",
            "objective": "Prepare a bounded narrative report from council evidence.",
            "window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-08T00:00:00Z"},
            "region": {"label": "New York City"},
        },
    )
    return mission_path


class ReportWritingRoundWorkflowTests(unittest.TestCase):
    def test_report_writing_round_registers_only_report_editor_and_publishes_narrative(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            mission_path = seed_mission(root)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )
            run_kernel(
                "submit-finding-record",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "environmental-investigator",
                "--agent-role",
                "environmental-investigator",
                "--title",
                "PM2.5 observations were elevated",
                "--summary",
                "Recorded air-quality observations support a bounded smoke-impact summary.",
                "--rationale",
                "This is a test finding used as narrative report basis.",
                "--confidence",
                "0.70",
                "--target-kind",
                "round",
                "--target-id",
                ROUND_ID,
                "--evidence-ref",
                "signal:test-pm25-001",
            )

            request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="open-report-writing-round",
                target_round_id=REPORT_ROUND_ID,
                source_round_id=ROUND_ID,
                rationale="Open report-editor-only narrative reporting round.",
                request_payload={
                    "round_mode": "report-writing",
                    "basis_round_id": ROUND_ID,
                    "reporting_basis_refs": ["finding:PM2.5 observations were elevated"],
                },
            )
            open_payload = run_script(
                script_path("open-report-writing-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                REPORT_ROUND_ID,
                "--source-round-id",
                ROUND_ID,
                "--transition-request-id",
                request_id,
            )

            gate_payload = run_kernel(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                REPORT_ROUND_ID,
                "--pretty",
            )
            roles = [
                item.get("role")
                for item in gate_payload["agent_entry"]["role_entry_points"]
                if isinstance(item, dict)
            ]
            report_editor_entry = gate_payload["agent_entry"]["role_entry_points"][0]
            write_surface = "\n".join(report_editor_entry["write_commands"])

            self.assertEqual("completed", open_payload["status"])
            self.assertEqual(["report-editor"], roles)
            self.assertEqual("report-writing", report_editor_entry["round_mode"])
            self.assertIn("draft-narrative-report", write_surface)
            self.assertIn("validate-narrative-report", write_surface)
            self.assertIn("publish-narrative-report", write_surface)
            self.assertNotIn("fetch-gdelt-doc-search", "\n".join(report_editor_entry["fetch_commands"]))

            public_summary_path = run_dir / "analytics" / "public_discourse_sample_summary_test.json"
            write_json(
                public_summary_path,
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "status": "completed",
                    "summary_id": "public-summary-test",
                    "sample_count": 3,
                    "source_skill_counts": [
                        {"source_skill": "fetch-gdelt-doc-search", "signal_count": 1},
                        {"source_skill": "fetch-gdelt-gkg", "signal_count": 1},
                        {"source_skill": "fetch-youtube-comments", "signal_count": 1},
                    ],
                    "discourse_lane_counts": [
                        {"discourse_lane": "gdelt_doc_recon", "signal_count": 1},
                        {"discourse_lane": "gdelt_media_tone", "signal_count": 1},
                        {"discourse_lane": "social_sample_affect", "signal_count": 1},
                    ],
                    "social_affect_distribution": [
                        {"label": "concern-or-alarm", "annotated_signal_count": 1}
                    ],
                    "issue_distribution": [
                        {"label": "health-risk-or-air-safety", "annotated_signal_count": 1}
                    ],
                    "source_narrative_distribution": [
                        {"label": "regional-wildfire-smoke", "annotated_signal_count": 1}
                    ],
                    "gdelt_media_tone_summary": [
                        {"metric": "v2_tone", "average_value": -0.2}
                    ],
                    "evidence_refs": ["signal:test-public-discourse-001"],
                },
            )
            draft_payload = run_script(
                script_path("draft-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                REPORT_ROUND_ID,
                "--basis-round-id",
                ROUND_ID,
                "--public-discourse-summary-path",
                "analytics/public_discourse_sample_summary_test.json",
            )
            validation_payload = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                REPORT_ROUND_ID,
            )
            publish_payload = run_script(
                script_path("publish-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                REPORT_ROUND_ID,
            )

            transition = load_json(runtime_path(run_dir, f"round_transition_{REPORT_ROUND_ID}.json"))
            draft = load_json(run_dir / "reporting" / f"narrative_report_draft_{REPORT_ROUND_ID}.json")
            published = load_json(run_dir / "reporting" / f"narrative_report_{REPORT_ROUND_ID}.json")

            self.assertEqual("report-writing", transition["round_mode"])
            self.assertEqual("completed", draft_payload["status"])
            self.assertEqual("completed", validation_payload["status"])
            self.assertEqual("completed", publish_payload["status"])
            self.assertEqual("narrative-report-draft-v1", draft["schema_version"])
            self.assertEqual("narrative-report-v1", published["schema_version"])
            self.assertEqual("canonical-published", published["status"])
            self.assertIn("signal:test-pm25-001", draft["evidence_refs"])
            public_sections = [
                section
                for section in draft["sections"]
                if section.get("section_id") == "public-discourse-deepening"
            ]
            self.assertEqual(1, len(public_sections))
            self.assertEqual("advisory-addendum", public_sections[0]["status"])
            self.assertIn("signal:test-public-discourse-001", public_sections[0]["evidence_refs"])


if __name__ == "__main__":
    unittest.main()

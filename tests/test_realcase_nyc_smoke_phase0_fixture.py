from __future__ import annotations

import json
import unittest
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "openclaw-realcase-nyc-smoke-phase0.json"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    assert isinstance(payload, dict)
    return payload


class RealcaseNycSmokePhase0FixtureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = load_json(FIXTURE_PATH)

    def source_path(self, key: str) -> Path:
        value = self.fixture["source_paths"][key]
        self.assertIsInstance(value, str)
        return REPO_ROOT / value

    def test_fixture_is_offline_and_points_to_local_run_artifacts(self) -> None:
        self.assertEqual("openclaw-realcase-diagnostic-fixture-v1", self.fixture["schema_version"])
        self.assertTrue(self.fixture["network_free"])
        self.assertEqual("openclaw-realcase-nyc-smoke-20230607", self.fixture["source_run_id"])
        self.assertEqual("round-001", self.fixture["source_round_id"])
        for key in self.fixture["source_paths"]:
            with self.subTest(source_path=key):
                self.assertTrue(self.source_path(key).exists())

    def test_fixture_preserves_original_issue_signatures(self) -> None:
        assertions = self.fixture["diagnostic_assertions"]
        for key in (
            "missing_fire_origin_selection",
            "missing_transport_or_plume_source_selection",
            "open_dead_letter_after_successful_import",
            "positive_gate_reasons_carried_as_open_risks",
            "expert_reports_ready_despite_blocked_readiness_status",
            "final_publication_released_with_missing_coverage",
        ):
            with self.subTest(diagnostic=key):
                self.assertTrue(assertions[key])

    def test_source_selection_snapshot_matches_original_run(self) -> None:
        mission = load_json(self.source_path("mission"))
        snapshot = self.fixture["mission_snapshot"]

        self.assertEqual(snapshot["objective"], mission["objective"])
        self.assertEqual(snapshot["policy_profile"], mission["policy_profile"])
        self.assertEqual(snapshot["source_governance"], mission["source_governance"])
        self.assertEqual(
            snapshot["source_request_skills"],
            [request["source_skill"] for request in mission["source_requests"]],
        )
        selected_by_role = {
            role: details["selected_sources"]
            for role, details in mission["source_selections"].items()
        }
        self.assertEqual(snapshot["selected_sources_by_role"], selected_by_role)

        selected_sources = {
            source
            for sources in selected_by_role.values()
            for source in sources
        }
        self.assertNotIn("fetch-nasa-firms-fire", selected_sources)
        self.assertFalse(any("hms" in source or "plume" in source for source in selected_sources))
        self.assertEqual(snapshot["selected_source_count"], len(selected_sources))

    def test_successful_import_kept_open_dead_letter_signature(self) -> None:
        execution = load_json(self.source_path("import_execution"))
        dead_letter = load_json(self.source_path("dead_letter"))
        snapshot = self.fixture["execution_snapshot"]

        self.assertEqual(snapshot["completed_count"], execution["completed_count"])
        self.assertEqual(snapshot["failed_count"], execution["failed_count"])
        self.assertEqual(3, execution["completed_count"])
        self.assertEqual(0, execution["failed_count"])
        self.assertEqual("open", dead_letter["resolution_status"])
        self.assertEqual("normalize-fetch-execution", dead_letter["source_name"])
        self.assertEqual(snapshot["dead_letters"][0]["error_code"], dead_letter["failure"]["error_code"])

    def test_reporting_snapshot_preserves_original_gate_and_status_holes(self) -> None:
        handoff = load_json(self.source_path("reporting_handoff"))
        final_publication = load_json(self.source_path("final_publication"))
        environmentalist_report = load_json(self.source_path("expert_report_environmentalist"))
        sociologist_report = load_json(self.source_path("expert_report_sociologist"))
        snapshot = self.fixture["reporting_snapshot"]

        self.assertEqual(snapshot["handoff_status"], handoff["handoff_status"])
        self.assertEqual(snapshot["final_publication_status"], final_publication["publication_status"])
        self.assertEqual(snapshot["final_publication_posture"], final_publication["publication_posture"])
        self.assertEqual("missing-coverage", final_publication["coverage_source"])

        open_risk_summaries = [risk["summary"] for risk in handoff["decision_packet"]["open_risks"]]
        self.assertEqual(snapshot["handoff_open_risk_summaries"], open_risk_summaries)
        self.assertIn("The round is now explicitly ready for downstream reporting handoff.", open_risk_summaries)
        self.assertIn("Council submitted 1 readiness opinions and all support report-basis freeze.", open_risk_summaries)

        reports_by_role = {
            environmentalist_report["agent_role"]: environmentalist_report,
            sociologist_report["agent_role"]: sociologist_report,
        }
        for report_snapshot in snapshot["expert_reports"]:
            with self.subTest(role=report_snapshot["role"]):
                report = reports_by_role[report_snapshot["role"]]
                self.assertEqual(report_snapshot["status"], report["status"])
                self.assertEqual(report_snapshot["readiness_status"], report["readiness_status"])
                self.assertEqual(report_snapshot["supervisor_status"], report["supervisor_status"])
                self.assertEqual(report_snapshot["coverage_source"], report["coverage_source"])


if __name__ == "__main__":
    unittest.main()

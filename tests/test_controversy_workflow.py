from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import analytics_path, load_json, run_script, script_path, seed_analysis_chain

RUN_ID = "run-controversy-001"
ROUND_ID = "round-controversy-001"


class ControversyWorkflowTests(unittest.TestCase):
    def test_research_issue_surface_views_and_map_replace_controversy_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            map_payload = run_script(
                script_path("export-research-issue-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--issue-surface-path",
                outputs["research_issue_surface"]["summary"]["output_path"],
                "--issue-views-path",
                outputs["research_issue_views"]["summary"]["output_path"],
            )

            issue_artifact = load_json(analytics_path(run_dir, f"research_issue_surface_{ROUND_ID}.json"))
            view_artifact = load_json(analytics_path(run_dir, f"research_issue_views_{ROUND_ID}.json"))
            map_artifact = load_json(analytics_path(run_dir, f"research_issue_map_{ROUND_ID}.json"))

            self.assertEqual("completed", outputs["research_issue_surface"]["status"])
            self.assertEqual("completed", outputs["research_issue_views"]["status"])
            self.assertEqual("completed", map_payload["status"])
            self.assertEqual("approved-helper-view", map_payload["summary"]["decision_source"])
            self.assertGreaterEqual(len(issue_artifact["research_issues"]), 1)
            self.assertGreaterEqual(len(view_artifact["issue_views"]), 1)
            self.assertGreaterEqual(map_payload["summary"]["node_count"], 1)
            self.assertGreaterEqual(map_payload["summary"]["edge_count"], 1)

            first_issue = issue_artifact["research_issues"][0]
            self.assertEqual("candidate-for-human-review", first_issue["issue_surface_status"])
            self.assertIn("evidence_refs", first_issue)
            self.assertIn("lineage", first_issue)
            self.assertNotIn("recommended_lane", first_issue)
            self.assertNotIn("controversy_posture", first_issue)

            first_view = view_artifact["issue_views"][0]
            self.assertEqual("candidate-for-human-review", first_view["projection_status"])
            self.assertIn("typed_cues", first_view)
            self.assertNotIn("dominant_stance", first_view)
            self.assertNotIn("support", first_view)

            issue_map = map_artifact["research_issue_map"]
            self.assertEqual("navigation-export", issue_map["map_status"])
            self.assertTrue(all(edge["relationship_kind"] == "traceability-cue" for edge in issue_map["edges"]))
            self.assertNotIn("actionable_gaps", issue_map)
            self.assertNotIn("readiness", issue_map)

    def test_research_issue_map_reports_missing_inputs_without_inline_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            analytics_path(run_dir, f"research_issue_surface_{ROUND_ID}.json").unlink()
            analytics_path(run_dir, f"research_issue_views_{ROUND_ID}.json").unlink()

            payload = run_script(
                script_path("export-research-issue-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(analytics_path(run_dir, f"research_issue_map_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertEqual(0, payload["summary"]["node_count"])
            self.assertEqual(0, payload["summary"]["edge_count"])
            self.assertEqual([], artifact["research_issue_map"]["nodes"])
            self.assertEqual([], artifact["research_issue_map"]["edges"])
            self.assertTrue(any(warning["code"] == "no-map-inputs" for warning in payload["warnings"]))


if __name__ == "__main__":
    unittest.main()

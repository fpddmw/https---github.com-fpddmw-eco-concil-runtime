from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    primary_research_issue_id,
    run_script,
    script_path,
    seed_analysis_chain,
)

RUN_ID = "run-analysis-001"
ROUND_ID = "round-analysis-001"


class AnalysisWorkflowTests(unittest.TestCase):
    def test_successor_analysis_chain_materializes_db_backed_surfaces(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            self.assertSetEqual(
                {
                    "discourse_issues",
                    "environment_aggregation",
                    "evidence_lanes",
                    "research_issue_surface",
                    "research_issue_views",
                },
                set(outputs),
            )
            for payload in outputs.values():
                self.assertEqual("completed", payload["status"])
                self.assertEqual("approved-helper-view", payload["summary"]["decision_source"])
                self.assertTrue(payload["summary"]["rule_id"].startswith("HEUR-"))
                self.assertEqual([], payload.get("board_handoff", {}).get("suggested_next_skills", []))

            discourse_artifact = load_json(analytics_path(run_dir, f"discourse_issue_discovery_{ROUND_ID}.json"))
            environment_artifact = load_json(analytics_path(run_dir, f"environment_evidence_aggregation_{ROUND_ID}.json"))
            issue_artifact = load_json(analytics_path(run_dir, f"research_issue_surface_{ROUND_ID}.json"))
            view_artifact = load_json(analytics_path(run_dir, f"research_issue_views_{ROUND_ID}.json"))

            self.assertGreaterEqual(len(discourse_artifact["discourse_issue_hints"]), 1)
            self.assertGreaterEqual(environment_artifact["aggregation"]["statistics_summary"]["signal_count"], 1)
            self.assertGreaterEqual(len(issue_artifact["research_issues"]), 1)
            self.assertGreaterEqual(len(view_artifact["issue_views"]), 1)
            self.assertIn("coverage_limitations", environment_artifact["aggregation"])
            self.assertIn("mentioned_scope_metadata", discourse_artifact["discourse_issue_hints"][0])
            self.assertEqual(
                "appendix-or-audit-only-until-db-basis-cites-it",
                issue_artifact["research_issues"][0]["report_usage"],
            )

            with sqlite3.connect(analytics_path(run_dir, "signal_plane.sqlite")) as connection:
                public_count = connection.execute(
                    "SELECT COUNT(*) FROM normalized_signals WHERE run_id = ? AND round_id = ? AND plane = ?",
                    (RUN_ID, ROUND_ID, "public"),
                ).fetchone()[0]
                environment_count = connection.execute(
                    "SELECT COUNT(*) FROM normalized_signals WHERE run_id = ? AND round_id = ? AND plane = ?",
                    (RUN_ID, ROUND_ID, "environment"),
                ).fetchone()[0]
            self.assertGreaterEqual(public_count, 1)
            self.assertGreaterEqual(environment_count, 1)

    def test_successor_analysis_outputs_support_custom_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=False)

            discourse_path = root / "custom_discourse_issues.json"
            environment_path = root / "custom_environment_aggregation.json"
            issue_path = root / "custom_research_issue_surface.json"
            views_path = root / "custom_research_issue_views.json"
            map_path = root / "custom_research_issue_map.json"

            discourse_payload = run_script(
                script_path("discover-discourse-issues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(discourse_path),
            )
            environment_payload = run_script(
                script_path("aggregate-environment-evidence"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(environment_path),
            )
            issue_payload = run_script(
                script_path("materialize-research-issue-surface"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--input-path",
                str(discourse_path),
                "--output-path",
                str(issue_path),
            )
            views_payload = run_script(
                script_path("project-research-issue-views"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--input-path",
                str(issue_path),
                "--output-path",
                str(views_path),
            )
            map_payload = run_script(
                script_path("export-research-issue-map"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--issue-surface-path",
                str(issue_path),
                "--issue-views-path",
                str(views_path),
                "--output-path",
                str(map_path),
            )

            self.assertEqual(str(discourse_path.resolve()), discourse_payload["summary"]["output_path"])
            self.assertEqual(str(environment_path.resolve()), environment_payload["summary"]["output_path"])
            self.assertEqual(str(issue_path.resolve()), issue_payload["summary"]["output_path"])
            self.assertEqual(str(views_path.resolve()), views_payload["summary"]["output_path"])
            self.assertEqual(str(map_path.resolve()), map_payload["summary"]["output_path"])
            for path in (discourse_path, environment_path, issue_path, views_path, map_path):
                self.assertTrue(path.exists())

    def test_evidence_sufficiency_review_remains_non_gate_helper(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            issue_id = primary_research_issue_id(outputs)

            review_payload = run_script(
                script_path("review-evidence-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "research-issue",
                "--target-id",
                issue_id,
            )
            review_artifact = load_json(analytics_path(run_dir, f"evidence_sufficiency_review_{ROUND_ID}.json"))

            self.assertEqual("approved-helper-view", review_payload["summary"]["decision_source"])
            self.assertEqual("review-evidence-sufficiency", review_payload["summary"]["skill"])
            self.assertNotIn("readiness_score", review_payload["review"])
            self.assertNotIn("promote_allowed", review_payload["review"])
            self.assertEqual("approval-gated-helper-view", review_artifact["review"]["wp4_helper_metadata"]["helper_status"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_script, script_path


REPO_ROOT = Path(__file__).resolve().parents[1]
REALCASE_RUN_ID = "openclaw-realcase-nyc-smoke-skillguidance-validation-20260512"
REALCASE_RUN_DIR = REPO_ROOT / "runs" / REALCASE_RUN_ID
SOURCE_SIGNAL_DB = REALCASE_RUN_DIR / "analytics" / "signal_plane.sqlite"
REGRESSION_ROUND_ID = "round-003"
BASE_REALCASE_SAMPLE_COUNT = 76
BASE_REALCASE_ROUND_IDS = {"round-001", "round-002"}
GDELT_ROW_LAYER_SKILLS = {
    "fetch-gdelt-events",
    "fetch-gdelt-mentions",
    "fetch-gdelt-gkg",
}
SOCIAL_SAMPLE_AFFECT_SKILLS = {
    "fetch-youtube-comments",
    "fetch-bluesky-cascade",
}
REALCASE_CORPUS_LIMIT = "800"


def count_by(items: list[dict[str, object]], key: str) -> dict[str, int]:
    return {
        str(item.get(key)): int(item.get("signal_count") or 0)
        for item in items
        if isinstance(item, dict)
    }


class RealcasePublicDiscourseDeepeningTests(unittest.TestCase):
    def setUp(self) -> None:
        if not SOURCE_SIGNAL_DB.exists():
            self.skipTest(f"realcase signal DB fixture is not present: {SOURCE_SIGNAL_DB}")

    def test_realcase_run_scope_preflight_surfaces_missing_deepening_layers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            target_db = run_dir / "analytics" / "signal_plane.sqlite"
            target_db.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(SOURCE_SIGNAL_DB, target_db)

            corpus_payload = run_script(
                script_path("materialize-public-discourse-corpus"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REALCASE_RUN_ID,
                "--round-id",
                REGRESSION_ROUND_ID,
                "--round-scope",
                "run",
                "--limit",
                REALCASE_CORPUS_LIMIT,
            )
            corpus_artifact = load_json(Path(corpus_payload["summary"]["output_path"]))

            self.assertEqual("completed", corpus_payload["status"])
            self.assertEqual("run", corpus_artifact["sample_definition"]["round_scope"])
            self.assertEqual(corpus_payload["sample_count"], corpus_artifact["sample_count"])
            self.assertEqual(
                corpus_payload["sample_count"],
                corpus_artifact["observed_inputs"]["matched_signal_count"],
            )
            source_skill_counts = count_by(corpus_artifact["source_skill_counts"], "source_skill")
            youtube_comment_count = source_skill_counts.get("fetch-youtube-comments", 0)
            has_social_sample_affect = any(
                source_skill_counts.get(skill_name, 0)
                for skill_name in SOCIAL_SAMPLE_AFFECT_SKILLS
            )
            has_gdelt_row_layer = any(
                source_skill_counts.get(skill_name, 0)
                for skill_name in GDELT_ROW_LAYER_SKILLS
            )
            gdelt_row_layer_count = sum(
                source_skill_counts.get(skill_name, 0)
                for skill_name in GDELT_ROW_LAYER_SKILLS
            )

            self.assertGreaterEqual(
                corpus_payload["sample_count"],
                BASE_REALCASE_SAMPLE_COUNT + youtube_comment_count + gdelt_row_layer_count,
            )
            observed_round_ids = {
                item["round_id"] for item in corpus_artifact["corpus_items"]
            }
            self.assertTrue(BASE_REALCASE_ROUND_IDS.issubset(observed_round_ids))
            if youtube_comment_count:
                self.assertIn(REGRESSION_ROUND_ID, observed_round_ids)
            self.assertEqual(51, source_skill_counts["fetch-gdelt-doc-search"])
            self.assertEqual(25, source_skill_counts["fetch-youtube-video-search"])
            lane_counts = count_by(corpus_artifact["discourse_lane_counts"], "discourse_lane")
            self.assertEqual(51 + gdelt_row_layer_count, lane_counts["gdelt_media_tone"])
            self.assertEqual(25, lane_counts["public_visibility"])
            if youtube_comment_count:
                self.assertEqual(youtube_comment_count, lane_counts["social_sample_affect"])
            else:
                self.assertNotIn("social_sample_affect", lane_counts)

            audit_payload = run_script(
                script_path("audit-public-discourse-sample-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REALCASE_RUN_ID,
                "--round-id",
                REGRESSION_ROUND_ID,
                "--round-scope",
                "run",
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
                "--limit",
                REALCASE_CORPUS_LIMIT,
            )
            audit_artifact = load_json(Path(audit_payload["summary"]["output_path"]))
            audit_warning_codes = {warning["code"] for warning in audit_artifact["warnings"]}

            self.assertEqual("completed", audit_payload["status"])
            self.assertEqual("run", audit_artifact["query_parameters"]["round_scope"])
            handoff = audit_artifact["source_acquisition_handoff"]
            missing_layer_ids = {
                layer["layer_id"]
                for layer in handoff["missing_layers"]
                if isinstance(layer, dict)
            }
            expected_missing_layer_ids = set()
            if youtube_comment_count:
                self.assertNotIn("youtube-comments-not-materialized", audit_warning_codes)
            else:
                expected_missing_layer_ids.add("youtube-comments")
                self.assertIn("youtube-comments-not-materialized", audit_warning_codes)
            if has_gdelt_row_layer:
                self.assertNotIn("gdelt-row-layer-not-materialized", audit_warning_codes)
            else:
                expected_missing_layer_ids.add("gdelt-events-mentions-gkg")
                self.assertIn("gdelt-row-layer-not-materialized", audit_warning_codes)
            if has_social_sample_affect:
                self.assertNotIn("no-social-sample-affect-basis", audit_warning_codes)
            else:
                expected_missing_layer_ids.add("social-sample-affect-basis")
                self.assertIn("no-social-sample-affect-basis", audit_warning_codes)

            self.assertEqual(expected_missing_layer_ids, missing_layer_ids)
            self.assertEqual(len(expected_missing_layer_ids), handoff["missing_layer_count"])
            if "youtube-comments" in expected_missing_layer_ids:
                self.assertIn("fetch-youtube-comments", handoff["candidate_fetch_skills"])
                self.assertIn(
                    "normalize-youtube-comments-public-signals",
                    handoff["candidate_normalize_skills"],
                )
            if "gdelt-events-mentions-gkg" in expected_missing_layer_ids:
                self.assertIn("fetch-gdelt-gkg", handoff["candidate_fetch_skills"])
            if expected_missing_layer_ids:
                self.assertIn(
                    "submit-source-acquisition-proposal",
                    audit_payload["board_handoff"]["suggested_next_skills"],
                )
            else:
                self.assertEqual([], audit_payload["board_handoff"]["suggested_next_skills"])

            summary_payload = run_script(
                script_path("summarize-public-discourse-sample"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                REALCASE_RUN_ID,
                "--round-id",
                REGRESSION_ROUND_ID,
                "--round-scope",
                "run",
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
                "--coverage-audit-path",
                audit_payload["summary"]["output_path"],
                "--limit",
                REALCASE_CORPUS_LIMIT,
            )
            summary_artifact = load_json(Path(summary_payload["summary"]["output_path"]))
            summary_warning_codes = {warning["code"] for warning in summary_artifact["warnings"]}

            self.assertEqual("completed", summary_payload["status"])
            self.assertEqual(corpus_payload["sample_count"], summary_artifact["sample_count"])
            self.assertEqual("run", summary_artifact["sample_definition"]["round_scope"])
            self.assertEqual([], summary_artifact["social_affect_distribution"])
            self.assertEqual(
                len(expected_missing_layer_ids),
                summary_artifact["source_acquisition_handoff"]["missing_layer_count"],
            )
            if youtube_comment_count:
                self.assertNotIn("youtube-comments-not-materialized", summary_warning_codes)
            else:
                self.assertIn("youtube-comments-not-materialized", summary_warning_codes)
            if has_gdelt_row_layer:
                self.assertNotIn("gdelt-row-layer-not-materialized", summary_warning_codes)
            else:
                self.assertIn("gdelt-row-layer-not-materialized", summary_warning_codes)
            if has_social_sample_affect:
                self.assertNotIn("no-social-sample-affect-basis", summary_warning_codes)
            else:
                self.assertIn("no-social-sample-affect-basis", summary_warning_codes)
            self.assertNotIn("recommended_conclusion", summary_artifact)
            self.assertNotIn("readiness_score", summary_artifact)


if __name__ == "__main__":
    unittest.main()

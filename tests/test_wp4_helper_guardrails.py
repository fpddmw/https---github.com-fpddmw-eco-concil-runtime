from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-wp4-helper-guardrails"
ROUND_ID = "round-wp4-helper-guardrails"


class WP4HelperGuardrailTests(unittest.TestCase):
    def test_optional_analysis_registry_entries_have_wp4_freeze_metadata(self) -> None:
        from eco_council_runtime.kernel.skill_registry import (
            SKILL_LAYER_OPTIONAL_ANALYSIS,
            WP4_ALLOWED_HELPER_DECISION_SOURCES,
            skill_registry_snapshot,
        )

        snapshot = skill_registry_snapshot()
        optional_skills = [
            skill
            for skill in snapshot["skills"]
            if skill["skill_layer"] == SKILL_LAYER_OPTIONAL_ANALYSIS
        ]

        self.assertGreaterEqual(len(optional_skills), 20)
        for skill in optional_skills:
            with self.subTest(skill=skill["skill_name"]):
                self.assertTrue(skill["requires_operator_approval"])
                metadata = skill.get("wp4_helper_metadata", {})
                self.assertTrue(metadata.get("rule_id"))
                self.assertEqual(
                    "wp4-freeze-line-2026-04-28",
                    metadata.get("rule_version"),
                )
                self.assertIn(
                    metadata.get("decision_source"),
                    WP4_ALLOWED_HELPER_DECISION_SOURCES,
                )
                self.assertIn("approval-required", metadata.get("audit_status", ""))
                self.assertTrue(metadata.get("wp4_destination"))

    def test_legacy_claim_observation_link_alias_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            payload = run_script(
                script_path("link-claims-to-observations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("deprecated-blocked", payload["status"])
            self.assertEqual([], payload["canonical_ids"])
            self.assertEqual([], payload["board_handoff"]["suggested_next_skills"])
            self.assertEqual("skipped", payload["analysis_sync"]["status"])
            self.assertEqual(
                "review-fact-check-evidence-scope",
                payload["summary"]["replacement_skill"],
            )

            artifact = load_json(
                run_dir / "analytics" / f"claim_observation_links_{ROUND_ID}.json"
            )
            self.assertEqual("deprecated-blocked", artifact["status"])
            self.assertEqual([], artifact.get("links", []))
            artifact_text = json.dumps(artifact, ensure_ascii=True, sort_keys=True)
            self.assertNotIn('"relation"', artifact_text)
            self.assertNotIn('"support"', artifact_text)
            self.assertNotIn('"contradiction"', artifact_text)

    def test_legacy_evidence_coverage_alias_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            payload = run_script(
                script_path("score-evidence-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("deprecated-blocked", payload["status"])
            self.assertEqual([], payload["canonical_ids"])
            self.assertEqual([], payload["board_handoff"]["suggested_next_skills"])
            self.assertEqual("skipped", payload["analysis_sync"]["status"])
            self.assertEqual(
                "review-evidence-sufficiency",
                payload["summary"]["replacement_skill"],
            )

            artifact = load_json(
                run_dir / "analytics" / f"evidence_coverage_{ROUND_ID}.json"
            )
            self.assertEqual("deprecated-blocked", artifact["status"])
            self.assertEqual([], artifact.get("coverages", []))
            artifact_text = json.dumps(artifact, ensure_ascii=True, sort_keys=True)
            self.assertNotIn('"coverage_score"', artifact_text)
            self.assertNotIn('"readiness"', artifact_text)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    run_script,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUN_ID = "run-diffusion-001"
ROUND_ID = "round-diffusion-001"


def seed_regulationsgov_smoke_comment(run_dir: Path, root: Path) -> None:
    regulations_path = root / "regulationsgov_smoke_comments.json"
    write_json(
        regulations_path,
        {
            "records": [
                {
                    "id": "rg-smoke-001",
                    "attributes": {
                        "title": "Wildfire smoke and air quality impacts",
                        "comment": "Wildfire smoke is reducing air quality and visibility across the city.",
                        "postedDate": "2023-06-08T12:00:00Z",
                        "agencyId": "EPA",
                        "docketId": "EPA-2023-001",
                    },
                }
            ]
        },
    )
    run_script(
        script_path("normalize-regulationsgov-comments-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(regulations_path),
    )


class DiffusionWorkflowTests(unittest.TestCase):
    def test_temporal_cooccurrence_cues_are_descriptive_not_diffusion_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            seed_regulationsgov_smoke_comment(run_dir, root)

            cue_payload = run_script(
                script_path("detect-temporal-cooccurrence-cues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            cue_artifact = load_json(analytics_path(run_dir, f"temporal_cooccurrence_cues_{ROUND_ID}.json"))

            self.assertEqual("completed", cue_payload["status"])
            self.assertEqual("approved-helper-view", cue_payload["summary"]["decision_source"])
            self.assertGreaterEqual(cue_payload["summary"]["cue_count"], 1)
            first_cue = cue_artifact["temporal_cooccurrence_cues"][0]
            self.assertEqual("descriptive-cooccurrence-only", first_cue["interpretation_limit"])
            self.assertIn("cooccurring_planes", first_cue)
            self.assertIn("evidence_refs", first_cue)
            self.assertNotIn("edge_type", first_cue)
            self.assertNotIn("source_platform", first_cue)
            self.assertNotIn("target_platform", first_cue)
            self.assertNotIn("influence", first_cue)
            self.assertNotIn("spillover", first_cue)

    def test_temporal_cooccurrence_custom_output_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=False)
            output_path = root / "custom_temporal_cues.json"

            payload = run_script(
                script_path("detect-temporal-cooccurrence-cues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(output_path),
            )

            self.assertEqual(str(output_path.resolve()), payload["summary"]["output_path"])
            self.assertTrue(output_path.exists())


if __name__ == "__main__":
    unittest.main()

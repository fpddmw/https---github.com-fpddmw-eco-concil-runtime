from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    run_script,
    runtime_src_path,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import query_analysis_result_items

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
    def test_cross_platform_diffusion_materializes_public_and_formal_spillover_edges(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("classify-claim-verifiability"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("route-verification-lane"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            seed_regulationsgov_smoke_comment(run_dir, root)

            run_script(
                script_path("link-formal-comments-to-public-discourse"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            diffusion_payload = run_script(
                script_path("detect-cross-platform-diffusion"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            diffusion_artifact = load_json(
                analytics_path(run_dir, f"diffusion_edges_{ROUND_ID}.json")
            )

            self.assertEqual("completed", diffusion_payload["status"])
            self.assertEqual("completed", diffusion_payload["analysis_sync"]["status"])
            self.assertGreaterEqual(diffusion_artifact["edge_count"], 2)

            smoke_edges = [
                edge
                for edge in diffusion_artifact["edges"]
                if edge["issue_label"] == "air-quality-smoke"
            ]
            self.assertGreaterEqual(len(smoke_edges), 2)
            edge_types = {edge["edge_type"] for edge in smoke_edges}
            self.assertIn("cross-public-diffusion", edge_types)
            self.assertIn("public-to-formal-spillover", edge_types)
            self.assertTrue(
                any(
                    edge["source_platform"] == "bluesky"
                    and edge["target_platform"] == "youtube"
                    for edge in smoke_edges
                )
            )
            self.assertTrue(
                any(
                    edge["target_platform"] == "regulationsgov"
                    and edge["edge_type"] == "public-to-formal-spillover"
                    for edge in smoke_edges
                )
            )
            spillover_edge = next(
                edge
                for edge in smoke_edges
                if edge["target_platform"] == "regulationsgov"
                and edge["edge_type"] == "public-to-formal-spillover"
            )
            self.assertEqual("heuristic-fallback", spillover_edge["decision_source"])
            self.assertTrue(spillover_edge["rationale"])
            self.assertGreaterEqual(len(spillover_edge["linkage_ids"]), 1)
            self.assertGreaterEqual(len(spillover_edge["route_ids"]), 1)
            self.assertGreaterEqual(len(spillover_edge["lineage"]), 1)
            self.assertIn("source_skill", spillover_edge["provenance"])

            analytics_path(run_dir, f"diffusion_edges_{ROUND_ID}.json").unlink()
            query_payload = query_analysis_result_items(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                analysis_kind="diffusion-edge",
                subject_id="air-quality-smoke",
                latest_only=True,
            )
            self.assertGreaterEqual(query_payload["summary"]["returned_item_count"], 2)
            self.assertTrue(
                any(
                    item["item"]["edge_type"] == "public-to-formal-spillover"
                    and not item["artifact_present"]
                    for item in query_payload["items"]
                )
            )


if __name__ == "__main__":
    unittest.main()

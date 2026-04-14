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

RUN_ID = "run-formal-public-001"
ROUND_ID = "round-formal-public-001"


def seed_regulationsgov_comments(run_dir: Path, root: Path) -> None:
    regulations_path = root / "regulationsgov_comments.json"
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
                },
                {
                    "id": "rg-permit-001",
                    "attributes": {
                        "title": "Extend the permit hearing and comment period",
                        "comment": "The agency should reopen the permit hearing and extend the public comment period for this docket.",
                        "postedDate": "2023-06-08T13:00:00Z",
                        "agencyId": "EPA",
                        "docketId": "EPA-2023-002",
                    },
                },
            ]
        },
    )
    run_script(
        script_path("eco-normalize-regulationsgov-comments-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(regulations_path),
    )


def seed_public_only_trust_signal(run_dir: Path, root: Path) -> None:
    youtube_path = root / "youtube_public_only_trust.json"
    write_json(
        youtube_path,
        [
            {
                "query": "community voice ignored environment",
                "video_id": "vid-trust-001",
                "video": {
                    "id": "vid-trust-001",
                    "title": "Residents say their community voice was ignored",
                    "description": (
                        "Local residents say their community voice was ignored and public trust collapsed during the environmental decision."
                    ),
                    "channel_title": "Neighborhood Watch",
                    "published_at": "2023-06-08T15:00:00Z",
                    "default_language": "en",
                    "statistics": {"view_count": 320},
                },
            }
        ],
    )
    run_script(
        script_path("eco-normalize-youtube-video-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(youtube_path),
    )


class FormalPublicWorkflowTests(unittest.TestCase):
    def test_formal_public_linkage_and_representation_gaps_materialize_issue_statuses(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            run_script(
                script_path("eco-derive-claim-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-classify-claim-verifiability"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("eco-route-verification-lane"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            seed_regulationsgov_comments(run_dir, root)
            seed_public_only_trust_signal(run_dir, root)

            link_payload = run_script(
                script_path("eco-link-formal-comments-to-public-discourse"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            gap_payload = run_script(
                script_path("eco-identify-representation-gaps"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            link_artifact = load_json(
                analytics_path(run_dir, f"formal_public_links_{ROUND_ID}.json")
            )
            gap_artifact = load_json(
                analytics_path(run_dir, f"representation_gaps_{ROUND_ID}.json")
            )

            self.assertEqual("completed", link_payload["status"])
            self.assertEqual("completed", gap_payload["status"])
            self.assertEqual("completed", link_payload["analysis_sync"]["status"])
            self.assertEqual("completed", gap_payload["analysis_sync"]["status"])
            self.assertGreaterEqual(link_artifact["link_count"], 3)
            self.assertGreaterEqual(gap_artifact["gap_count"], 2)

            links_by_issue = {
                link["issue_label"]: link for link in link_artifact["links"]
            }
            self.assertIn("air-quality-smoke", links_by_issue)
            self.assertIn("permit-process", links_by_issue)
            self.assertIn("representation-trust", links_by_issue)

            smoke_link = links_by_issue["air-quality-smoke"]
            permit_link = links_by_issue["permit-process"]
            trust_link = links_by_issue["representation-trust"]

            self.assertEqual("aligned", smoke_link["link_status"])
            self.assertGreaterEqual(smoke_link["formal_signal_count"], 1)
            self.assertGreaterEqual(smoke_link["public_signal_count"], 1)
            self.assertEqual("formal-only", permit_link["link_status"])
            self.assertGreaterEqual(permit_link["formal_signal_count"], 1)
            self.assertEqual(0, permit_link["public_signal_count"])
            self.assertEqual("public-only", trust_link["link_status"])
            self.assertEqual(0, trust_link["formal_signal_count"])
            self.assertGreaterEqual(trust_link["public_signal_count"], 1)

            gap_pairs = {
                (gap["issue_label"], gap["gap_type"]) for gap in gap_artifact["gaps"]
            }
            self.assertIn(
                ("permit-process", "public-underrepresentation"),
                gap_pairs,
            )
            self.assertIn(
                ("representation-trust", "formal-underrepresentation"),
                gap_pairs,
            )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import sqlite3
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

            with sqlite3.connect(analytics_path(run_dir, "signal_plane.sqlite")) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    """
                    SELECT source_skill, plane, canonical_object_kind
                    FROM normalized_signals
                    WHERE run_id = ? AND round_id = ?
                    ORDER BY source_skill, signal_id
                    """,
                    (RUN_ID, ROUND_ID),
                ).fetchall()
            regulations_rows = [
                row for row in rows if str(row["source_skill"]).startswith("regulationsgov-")
            ]
            youtube_rows = [
                row for row in rows if str(row["source_skill"]) == "youtube-video-search"
            ]
            self.assertGreaterEqual(len(regulations_rows), 2)
            self.assertEqual({"formal"}, {str(row["plane"]) for row in regulations_rows})
            self.assertEqual(
                {"formal-comment-signal"},
                {str(row["canonical_object_kind"]) for row in regulations_rows},
            )
            self.assertEqual({"public"}, {str(row["plane"]) for row in youtube_rows})
            self.assertEqual(
                {"public-discourse-signal"},
                {str(row["canonical_object_kind"]) for row in youtube_rows},
            )

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
            self.assertEqual("heuristic-fallback", smoke_link["decision_source"])
            self.assertTrue(smoke_link["rationale"])
            self.assertGreaterEqual(len(smoke_link["route_ids"]), 1)
            self.assertGreaterEqual(len(smoke_link["lineage"]), 1)
            self.assertIn("source_skill", smoke_link["provenance"])
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
            permit_gap = next(
                gap
                for gap in gap_artifact["gaps"]
                if gap["issue_label"] == "permit-process"
                and gap["gap_type"] == "public-underrepresentation"
            )
            self.assertEqual("heuristic-fallback", permit_gap["decision_source"])
            self.assertTrue(permit_gap["rationale"])
            self.assertIsInstance(permit_gap["route_ids"], list)
            self.assertGreaterEqual(len(permit_gap["lineage"]), 1)
            self.assertIn("source_skill", permit_gap["provenance"])

            analytics_path(run_dir, f"formal_public_links_{ROUND_ID}.json").unlink()
            analytics_path(run_dir, f"representation_gaps_{ROUND_ID}.json").unlink()

            link_query = query_analysis_result_items(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                analysis_kind="formal-public-link",
                subject_id="air-quality-smoke",
                latest_only=True,
                include_result_sets=True,
                include_contract=True,
            )
            self.assertEqual(1, link_query["summary"]["returned_item_count"])
            self.assertFalse(link_query["items"][0]["artifact_present"])
            self.assertEqual("heuristic-fallback", link_query["items"][0]["decision_source"])
            self.assertGreaterEqual(len(link_query["items"][0]["item"]["route_ids"]), 1)
            self.assertIn("source_skill", link_query["items"][0]["provenance"])

            gap_query = query_analysis_result_items(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                analysis_kind="representation-gap",
                subject_id="permit-process",
                latest_only=True,
            )
            self.assertGreaterEqual(gap_query["summary"]["returned_item_count"], 1)
            self.assertTrue(
                any(
                    item["item"]["gap_type"] == "public-underrepresentation"
                    and not item["artifact_present"]
                    for item in gap_query["items"]
                )
            )

    def test_claim_extractor_ignores_formal_signal_rows_after_formal_plane_split(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_regulationsgov_comments(run_dir, root)
            seed_public_only_trust_signal(run_dir, root)

            payload = run_script(
                script_path("eco-extract-claim-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(analytics_path(run_dir, f"claim_candidates_{ROUND_ID}.json"))

            self.assertEqual("completed", payload["status"])
            self.assertEqual(1, artifact["candidate_count"])
            self.assertEqual("representation-trust", artifact["candidates"][0]["issue_hint"])
            self.assertEqual(1, len(artifact["candidates"][0]["source_signal_ids"]))


if __name__ == "__main__":
    unittest.main()

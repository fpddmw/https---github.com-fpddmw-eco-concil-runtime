from __future__ import annotations

import sqlite3
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
                    "description": "Local residents say their community voice was ignored and public trust collapsed during the environmental decision.",
                    "channel_title": "Neighborhood Watch",
                    "published_at": "2023-06-08T15:00:00Z",
                    "default_language": "en",
                    "statistics": {"view_count": 320},
                },
            }
        ],
    )
    run_script(
        script_path("normalize-youtube-video-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(youtube_path),
    )


def write_approved_taxonomy(root: Path) -> Path:
    taxonomy_path = root / "approved_formal_public_taxonomy.json"
    write_json(
        taxonomy_path,
        {
            "version": "test-taxonomy-v1",
            "labels": [
                {"label": "air-quality-smoke", "terms": ["smoke", "air quality", "visibility"]},
                {"label": "permit-process", "terms": ["permit", "hearing", "comment period"]},
                {"label": "representation-trust", "terms": ["community voice", "public trust"]},
            ],
        },
    )
    return taxonomy_path


class FormalPublicWorkflowTests(unittest.TestCase):
    def test_formal_public_successors_materialize_footprints_and_audit_cues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            seed_regulationsgov_comments(run_dir, root)
            seed_public_only_trust_signal(run_dir, root)
            taxonomy_path = write_approved_taxonomy(root)

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
            regulations_rows = [row for row in rows if str(row["source_skill"]).startswith("fetch-regulationsgov-")]
            youtube_rows = [row for row in rows if str(row["source_skill"]) == "fetch-youtube-video-search"]
            self.assertGreaterEqual(len(regulations_rows), 2)
            self.assertEqual({"formal"}, {str(row["plane"]) for row in regulations_rows})
            self.assertEqual({"formal-comment-signal"}, {str(row["canonical_object_kind"]) for row in regulations_rows})
            self.assertEqual({"public"}, {str(row["plane"]) for row in youtube_rows})

            taxonomy_payload = run_script(
                script_path("apply-approved-formal-public-taxonomy"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--taxonomy-path",
                str(taxonomy_path),
                "--approval-ref",
                "approval://taxonomy/test-taxonomy-v1",
            )
            footprint_payload = run_script(
                script_path("compare-formal-public-footprints"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--taxonomy-labels-path",
                taxonomy_payload["summary"]["output_path"],
            )
            cue_payload = run_script(
                script_path("identify-representation-audit-cues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            taxonomy_artifact = load_json(analytics_path(run_dir, f"formal_public_taxonomy_labels_{ROUND_ID}.json"))
            footprint_artifact = load_json(analytics_path(run_dir, f"formal_public_footprints_{ROUND_ID}.json"))
            cue_artifact = load_json(analytics_path(run_dir, f"representation_audit_cues_{ROUND_ID}.json"))

            self.assertEqual("completed", taxonomy_payload["status"])
            self.assertEqual("completed", footprint_payload["status"])
            self.assertEqual("completed", cue_payload["status"])
            self.assertGreaterEqual(taxonomy_payload["summary"]["label_cue_count"], 3)
            self.assertGreaterEqual(footprint_payload["summary"]["formal_signal_count"], 2)
            self.assertGreaterEqual(footprint_payload["summary"]["public_signal_count"], 1)
            self.assertGreaterEqual(cue_payload["summary"]["cue_count"], 1)

            first_label = taxonomy_artifact["taxonomy_labels"][0]
            self.assertEqual("candidate-for-human-review", first_label["audit_status"])
            self.assertEqual("approval://taxonomy/test-taxonomy-v1", first_label["taxonomy_approval_ref"])
            self.assertEqual("approved-helper-view", first_label["helper_governance"]["decision_source"])

            footprints = footprint_artifact["formal_public_footprints"]
            self.assertIn("formal_record_summary", footprints)
            self.assertIn("public_discourse_summary", footprints)
            self.assertIn("overlap_terms", footprints)
            self.assertNotIn("link_status", footprints)
            self.assertNotIn("aligned", footprints)

            first_cue = cue_artifact["representation_audit_cues"][0]
            self.assertEqual("requires-human-review", first_cue["audit_status"])
            self.assertIn("review_prompt", first_cue)
            self.assertNotIn("gap_type", first_cue)
            self.assertNotIn("severity", first_cue)

    def test_taxonomy_helper_requires_explicit_approved_taxonomy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_regulationsgov_comments(run_dir, root)
            seed_public_only_trust_signal(run_dir, root)

            payload = run_script(
                script_path("apply-approved-formal-public-taxonomy"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(analytics_path(run_dir, f"formal_public_taxonomy_labels_{ROUND_ID}.json"))

            self.assertEqual("taxonomy-required", payload["status"])
            self.assertEqual(0, payload["summary"]["label_cue_count"])
            self.assertEqual([], artifact["taxonomy_labels"])
            self.assertTrue(any(warning["code"] == "taxonomy-required" for warning in payload["warnings"]))

    def test_taxonomy_helper_rejects_taxonomy_file_without_approval_ref(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_regulationsgov_comments(run_dir, root)
            seed_public_only_trust_signal(run_dir, root)
            taxonomy_path = write_approved_taxonomy(root)

            payload = run_script(
                script_path("apply-approved-formal-public-taxonomy"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--taxonomy-path",
                str(taxonomy_path),
            )
            artifact = load_json(analytics_path(run_dir, f"formal_public_taxonomy_labels_{ROUND_ID}.json"))

            self.assertEqual("taxonomy-approval-required", payload["status"])
            self.assertEqual(0, payload["summary"]["label_cue_count"])
            self.assertEqual([], artifact["taxonomy_labels"])
            self.assertTrue(any(warning["code"] == "taxonomy-approval-required" for warning in payload["warnings"]))


if __name__ == "__main__":
    unittest.main()

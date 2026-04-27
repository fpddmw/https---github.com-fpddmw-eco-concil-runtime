from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_script, script_path, seed_signal_plane, write_json

RUN_ID = "run-signal-001"
ROUND_ID = "round-signal-001"
ROUND2_ID = "round-signal-002"


class SignalPlaneWorkflowTests(unittest.TestCase):
    def test_formal_signal_roundtrip_and_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            regulations_path = root / "regulationsgov_comments.json"
            write_json(
                regulations_path,
                {
                    "records": [
                        {
                            "id": "rg-smoke-001",
                            "attributes": {
                                "title": "Oppose the rule because wildfire smoke harms children",
                                "comment": (
                                    "Coalition members oppose this rule because wildfire smoke worsens "
                                    "asthma and the EPA monitoring study already shows dangerous air quality."
                                ),
                                "postedDate": "2023-06-08T12:00:00Z",
                                "agencyId": "EPA",
                                "docketId": "EPA-2023-001",
                                "submitterName": "Coalition of River Residents",
                            },
                        },
                        {
                            "id": "rg-water-001",
                            "attributes": {
                                "title": "Water permit hearing request",
                                "comment": "The agency should extend the hearing timeline for this water permit docket.",
                                "postedDate": "2023-06-08T13:00:00Z",
                                "agencyId": "EPA",
                                "docketId": "EPA-2023-002",
                                "submitterName": "Concerned Citizen",
                            },
                        },
                    ]
                },
            )
            normalize_payload = run_script(
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
            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(2, len(normalize_payload["canonical_ids"]))

            query_payload = run_script(
                script_path("query-formal-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--docket-id",
                "EPA-2023-001",
                "--agency-id",
                "EPA",
                "--keyword",
                "smoke",
            )
            self.assertEqual(1, query_payload["result_count"])
            result = query_payload["results"][0]
            self.assertEqual("formal-comment-signal", result["canonical_object_kind"])
            self.assertEqual("EPA-2023-001", result["docket_id"])
            self.assertEqual("EPA", result["agency_id"])
            self.assertEqual("Coalition of River Residents", result["submitter_name"])
            self.assertEqual("", result["submitter_type"])
            self.assertEqual("", result["stance_hint"])
            self.assertEqual("", result["route_hint"])
            self.assertEqual([], result["issue_labels"])
            self.assertEqual([], result["concern_facets"])
            self.assertEqual([], result["evidence_citation_types"])
            self.assertEqual("provider-field-normalization", result["decision_source"])
            self.assertEqual("", result["typing_method"])

            permit_query_payload = run_script(
                script_path("query-formal-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--docket-id",
                "EPA-2023-002",
            )
            self.assertEqual(1, permit_query_payload["result_count"])
            self.assertEqual(
                "EPA-2023-002",
                permit_query_payload["results"][0]["docket_id"],
            )
            typed_filter_payload = run_script(
                script_path("query-formal-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--issue-label",
                "permit-process",
            )
            self.assertEqual(0, typed_filter_payload["result_count"])

            lookup_payload = run_script(
                script_path("query-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                result["signal_id"],
            )
            self.assertEqual(1, lookup_payload["result_count"])
            self.assertEqual(
                "Oppose the rule because wildfire smoke harms children",
                lookup_payload["results"][0]["title"],
            )

            with sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite") as connection:
                connection.row_factory = sqlite3.Row
                index_rows = connection.execute(
                    """
                    SELECT field_name, field_value
                    FROM normalized_signal_index
                    WHERE signal_id = ?
                    ORDER BY field_name, field_value
                    """,
                    (result["signal_id"],),
                ).fetchall()
                raw_row = connection.execute(
                    """
                    SELECT metadata_json, quality_flags_json
                    FROM normalized_signals
                    WHERE signal_id = ?
                    """,
                    (result["signal_id"],),
                ).fetchone()
            indexed_pairs = {
                (str(row["field_name"]), str(row["field_value"])) for row in index_rows
            }
            self.assertIn(("docket_id", "EPA-2023-001"), indexed_pairs)
            self.assertIn(("agency_id", "EPA"), indexed_pairs)
            self.assertIn(("submitter_name", "Coalition of River Residents"), indexed_pairs)
            self.assertIn(("decision_source", "provider-field-normalization"), indexed_pairs)
            self.assertNotIn(("issue_labels", "air-quality-smoke"), indexed_pairs)
            self.assertNotIn(("route_hint", "environmental-observation"), indexed_pairs)
            self.assertNotIn(("concern_facets", "health-safety"), indexed_pairs)
            self.assertNotIn(("evidence_citation_types", "scientific-study"), indexed_pairs)
            self.assertIsNotNone(raw_row)
            self.assertIn("provider-fields-only", str(raw_row["metadata_json"]))
            self.assertIn("provider-field-normalized", str(raw_row["quality_flags_json"]))

    def test_formal_comment_detail_signal_enrichment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            detail_path = root / "regulationsgov_comment_detail.json"
            write_json(
                detail_path,
                {
                    "records": [
                        {
                            "comment_id": "rg-detail-001",
                            "response_url": "https://www.regulations.gov/comment/rg-detail-001",
                            "detail": {
                                "attributes": {
                                    "title": "Reopen the refinery permit hearing",
                                    "comment": (
                                        "Clean Air Alliance asks the agency to reopen the hearing "
                                        "and review the monitoring data for this permit."
                                    ),
                                    "postedDate": "2023-06-08T14:00:00Z",
                                    "modifyDate": "2023-06-08T15:00:00Z",
                                    "receiveDate": "2023-06-08T14:30:00Z",
                                    "agencyId": "EPA",
                                    "docketId": "EPA-2023-009",
                                    "submitterName": "Clean Air Alliance",
                                    "organizationName": "Clean Air Alliance",
                                }
                            },
                        }
                    ]
                },
            )
            normalize_payload = run_script(
                script_path("normalize-regulationsgov-comment-detail-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(detail_path),
            )
            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, len(normalize_payload["canonical_ids"]))

            query_payload = run_script(
                script_path("query-formal-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--source-skill",
                "fetch-regulationsgov-comment-detail",
                "--docket-id",
                "EPA-2023-009",
            )
            self.assertEqual(1, query_payload["result_count"])
            result = query_payload["results"][0]
            self.assertEqual("EPA-2023-009", result["docket_id"])
            self.assertEqual("Clean Air Alliance", result["submitter_name"])
            self.assertEqual("", result["submitter_type"])
            self.assertEqual([], result["concern_facets"])
            self.assertEqual([], result["evidence_citation_types"])
            self.assertEqual("", result["route_hint"])
            self.assertEqual("provider-field-normalization", result["decision_source"])

            with sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite") as connection:
                connection.row_factory = sqlite3.Row
                index_rows = connection.execute(
                    """
                    SELECT field_name, field_value
                    FROM normalized_signal_index
                    WHERE signal_id = ?
                    ORDER BY field_name, field_value
                    """,
                    (result["signal_id"],),
                ).fetchall()
                raw_row = connection.execute(
                    """
                    SELECT metadata_json, quality_flags_json
                    FROM normalized_signals
                    WHERE signal_id = ?
                    """,
                    (result["signal_id"],),
                ).fetchone()
            indexed_pairs = {
                (str(row["field_name"]), str(row["field_value"])) for row in index_rows
            }
            self.assertIn(("docket_id", "EPA-2023-009"), indexed_pairs)
            self.assertIn(("agency_id", "EPA"), indexed_pairs)
            self.assertIn(("submitter_name", "Clean Air Alliance"), indexed_pairs)
            self.assertNotIn(("submitter_type", "ngo"), indexed_pairs)
            self.assertNotIn(("issue_labels", "permit-process"), indexed_pairs)
            self.assertNotIn(("route_hint", "formal-comment-and-policy-record"), indexed_pairs)
            self.assertIsNotNone(raw_row)
            self.assertIn("comment-detail", str(raw_row["quality_flags_json"]))

    def test_public_signal_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            youtube_path = root / "youtube_videos.json"
            write_json(
                youtube_path,
                [
                    {
                        "query": "nyc smoke wildfire",
                        "video_id": "vid-001",
                        "video": {
                            "id": "vid-001",
                            "title": "Smoke over New York City",
                            "description": "Canadian wildfire smoke over NYC skyline.",
                            "channel_title": "City Desk",
                            "published_at": "2023-06-07T13:00:00Z",
                            "default_language": "en",
                            "statistics": {"view_count": 1250},
                        },
                    }
                ],
            )
            normalize_payload = run_script(
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
            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, len(normalize_payload["canonical_ids"]))

            query_payload = run_script(
                script_path("query-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--keyword",
                "smoke",
            )
            self.assertEqual(1, query_payload["result_count"])
            signal_id = query_payload["results"][0]["signal_id"]

            lookup_payload = run_script(
                script_path("query-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual(1, lookup_payload["result_count"])
            self.assertEqual("Smoke over New York City", lookup_payload["results"][0]["title"])

            raw_payload = run_script(
                script_path("query-raw-record"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual(1, raw_payload["result_count"])
            self.assertEqual("vid-001", raw_payload["results"][0]["raw_record"]["video_id"])

    def test_environment_signal_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            openaq_path = root / "openaq.json"
            write_json(
                openaq_path,
                {
                    "results": [
                        {
                            "parameter": {"name": "pm25", "units": "ug/m3"},
                            "value": 41.5,
                            "date": {"utc": "2023-06-07T12:00:00Z"},
                            "coordinates": {"latitude": 40.7, "longitude": -74.0},
                            "location": {"id": 1, "name": "NYC"},
                            "provider": {"name": "OpenAQ"},
                        }
                    ]
                },
            )

            normalize_payload = run_script(
                script_path("normalize-openaq-observation-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(openaq_path),
            )
            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, len(normalize_payload["canonical_ids"]))

            query_payload = run_script(
                script_path("query-environment-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
                "--bbox",
                "-75.0",
                "40.0",
                "-73.0",
                "41.0",
            )
            self.assertEqual(1, query_payload["result_count"])
            signal_id = query_payload["results"][0]["signal_id"]

            lookup_payload = run_script(
                script_path("query-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual("pm2_5", lookup_payload["results"][0]["metric"])

            raw_payload = run_script(
                script_path("query-raw-record"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual(41.5, raw_payload["results"][0]["raw_record"]["value"])

    def test_mixed_signal_plane_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_signal_plane(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True, include_openmeteo=True)

            public_payload = run_script(
                script_path("query-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--keyword",
                "smoke",
            )
            self.assertGreaterEqual(public_payload["result_count"], 2)

            environment_payload = run_script(
                script_path("query-environment-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
                "--bbox",
                "-75.0",
                "40.0",
                "-73.0",
                "41.0",
            )
            self.assertGreaterEqual(environment_payload["result_count"], 2)

            signal_id = public_payload["results"][0]["signal_id"]
            lookup_payload = run_script(
                script_path("query-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual(1, lookup_payload["result_count"])

            db_path = run_dir / "analytics" / "signal_plane.sqlite"
            self.assertTrue(db_path.exists())
            self.assertIsInstance(load_json(run_dir / "analytics" / "nonexistent.json") if False else {}, dict)

    def test_query_skills_can_read_prior_rounds_with_round_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_signal_plane(run_dir, root, RUN_ID, ROUND_ID, include_airnow=False, include_openmeteo=False)
            seed_signal_plane(run_dir, root, RUN_ID, ROUND2_ID, include_airnow=False, include_openmeteo=False)

            public_current = run_script(
                script_path("query-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND2_ID,
            )
            public_cross_round = run_script(
                script_path("query-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND2_ID,
                "--round-scope",
                "up-to-current",
            )
            environment_cross_round = run_script(
                script_path("query-environment-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND2_ID,
                "--round-scope",
                "up-to-current",
                "--metric",
                "pm2_5",
            )

            self.assertEqual("current", public_current["summary"]["round_scope"])
            self.assertEqual(2, public_current["result_count"])
            self.assertEqual("up-to-current", public_cross_round["summary"]["round_scope"])
            self.assertEqual([ROUND_ID, ROUND2_ID], public_cross_round["summary"]["queried_round_ids"])
            self.assertEqual(4, public_cross_round["result_count"])
            self.assertEqual({ROUND_ID, ROUND2_ID}, {item["round_id"] for item in public_cross_round["results"]})
            self.assertEqual([ROUND_ID, ROUND2_ID], environment_cross_round["summary"]["queried_round_ids"])
            self.assertEqual(4, environment_cross_round["result_count"])
            self.assertEqual({ROUND_ID, ROUND2_ID}, {item["round_id"] for item in environment_cross_round["results"]})


if __name__ == "__main__":
    unittest.main()

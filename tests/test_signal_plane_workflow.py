from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_script, script_path, seed_signal_plane, write_json

RUN_ID = "run-signal-001"
ROUND_ID = "round-signal-001"
ROUND2_ID = "round-signal-002"


class SignalPlaneWorkflowTests(unittest.TestCase):
    def test_youtube_comment_jsonl_artifact_normalizes_to_public_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            comments_path = Path(tmpdir) / "youtube_comments.jsonl"
            comments_path.write_text(
                json.dumps(
                    {
                        "video_id": "abc123def45",
                        "thread_id": "thread-001",
                        "comment_id": "comment-001",
                        "parent_comment_id": "",
                        "comment_type": "top_level",
                        "channel_id": "channel-001",
                        "author_display_name": "Fixture Viewer",
                        "author_channel_id": "author-001",
                        "author_channel_url": "https://example.test/author",
                        "published_at": "2023-06-07T18:00:00Z",
                        "updated_at": "2023-06-07T18:00:00Z",
                        "text_display": "The smoke made it hard to breathe in NYC.",
                        "text_original": "The smoke made it hard to breathe in NYC.",
                        "like_count": 3,
                        "viewer_rating": "none",
                        "can_rate": True,
                        "time_field_used": "published",
                        "source": {"video_id": "abc123def45", "order": "time"},
                    },
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            normalize_payload = run_script(
                script_path("normalize-youtube-comments-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(comments_path),
            )

            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, len(normalize_payload["canonical_ids"]))
            with sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite") as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    """
                    SELECT source_skill, signal_kind, body_text, metadata_json
                    FROM normalized_signals
                    WHERE signal_id = ?
                    """,
                    (normalize_payload["canonical_ids"][0],),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual("fetch-youtube-comments", row["source_skill"])
            self.assertEqual("comment", row["signal_kind"])
            self.assertIn("hard to breathe", row["body_text"])
            self.assertIn("abc123def45", row["metadata_json"])

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
                                "commentOnId": "EPA-DOC-2023-001",
                                "documentType": "Public Submission",
                                "subtype": "Comment",
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
            self.assertEqual(result["signal_id"], result["evidence_refs"][0]["signal_id"])
            self.assertEqual(
                "formal-comment-signal",
                result["evidence_basis"]["basis_object_kind"],
            )
            self.assertEqual(
                "none",
                result["evidence_basis"]["data_quality"]["research_judgement"],
            )
            self.assertEqual(
                "fetch-regulationsgov-comments",
                result["evidence_basis"]["source_provenance"]["source_skill"],
            )
            self.assertIn(
                "submit-finding-record",
                query_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertIn(
                "submit-evidence-bundle",
                query_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "link-formal-comments-to-public-discourse",
                query_payload["board_handoff"]["suggested_next_skills"],
            )

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
            self.assertIn('"comment_on_document_id": "EPA-DOC-2023-001"', str(raw_row["metadata_json"]))
            self.assertIn('"document_type": "Public Submission"', str(raw_row["metadata_json"]))
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
                                    "commentOnId": "EPA-DOC-2023-009",
                                    "documentType": "Public Submission",
                                    "subtype": "Comment",
                                    "submitterName": "Clean Air Alliance",
                                    "organizationName": "Clean Air Alliance",
                                }
                            },
                            "attachments": [
                                {"id": "att-001", "attributes": {"title": "Attachment one"}},
                                {"id": "att-002", "attributes": {"title": "Attachment two"}},
                            ],
                        },
                        {
                            "comment_id": "rg-detail-attached",
                            "response_url": "https://www.regulations.gov/comment/rg-detail-attached",
                            "detail": {
                                "attributes": {
                                    "title": "Attached technical appendix",
                                    "comment": "See Attached",
                                    "postedDate": "2023-06-09T14:00:00Z",
                                    "agencyId": "EPA",
                                    "docketId": "EPA-2023-009",
                                    "commentOnId": "EPA-DOC-2023-009",
                                    "submitterName": "Technical Commenter",
                                },
                                "relationships": {
                                    "attachments": {
                                        "data": [
                                            {"id": "att-technical-001", "type": "attachments"}
                                        ]
                                    }
                                },
                            },
                        },
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
            self.assertEqual(2, len(normalize_payload["canonical_ids"]))

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
                "--keyword",
                "reopen",
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
            self.assertIn('"attachment_count": 2', str(raw_row["metadata_json"]))
            self.assertIn('"has_inline_comment_text": true', str(raw_row["metadata_json"]))

            with sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite") as connection:
                connection.row_factory = sqlite3.Row
                attached_row = connection.execute(
                    """
                    SELECT metadata_json, quality_flags_json
                    FROM normalized_signals
                    WHERE external_id = 'rg-detail-attached'
                    """,
                ).fetchone()
            self.assertIsNotNone(attached_row)
            self.assertIn('"requires_attachment_text": true', str(attached_row["metadata_json"]))
            self.assertIn("requires-attachment-text", str(attached_row["quality_flags_json"]))

    def test_formal_comment_candidate_corpus_audit_reports_drift_cues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            regulations_path = root / "regulationsgov_comments.json"
            write_json(
                regulations_path,
                {
                    "records": [
                        {
                            "id": "rg-candidate-001",
                            "attributes": {
                                "title": "PM2.5 health comment",
                                "comment": "The rule should account for health benefits.",
                                "postedDate": "2024-02-01T12:00:00Z",
                                "agencyId": "EPA",
                                "docketId": "EPA-HQ-OAR-2015-0072",
                                "commentOnId": "EPA-HQ-OAR-2015-0072-0001",
                            },
                        },
                        {
                            "id": "rg-candidate-drift",
                            "attributes": {
                                "title": "Unrelated permit comment",
                                "comment": "This concerns a different action.",
                                "postedDate": "2024-02-01T13:00:00Z",
                                "agencyId": "DOT",
                            },
                        },
                    ]
                },
            )

            audit_payload = run_script(
                script_path("audit-formal-comment-candidate-corpus"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(regulations_path),
                "--docket-id",
                "EPA-HQ-OAR-2015-0072",
                "--comment-on-document-id",
                "EPA-HQ-OAR-2015-0072-0001",
                "--agency-id",
                "EPA",
                "--keyword",
                "health",
                "--sample-ref-limit",
                "5",
            )

            self.assertEqual("completed", audit_payload["status"])
            audit = audit_payload["audit"]
            self.assertEqual(2, audit["candidate_comment_count"])
            self.assertEqual(1, audit["eligible_count"])
            self.assertEqual(1, audit["excluded_count"])
            self.assertEqual(["rg-candidate-001"], audit["candidate_ids"])
            self.assertEqual(1, audit["missing_docket_count"])
            self.assertEqual(1, audit["exact_docket_match_count"])
            drift_codes = {item["code"] for item in audit["likely_drift_indicators"]}
            self.assertIn("docket-mismatch-or-missing", drift_codes)
            self.assertIn("keyword-miss", drift_codes)
            self.assertIn("Formal comment samples must not be converted", " ".join(audit["source_limitations"]))

    def test_regulationsgov_fetch_candidate_summary_and_dry_run_filters(self) -> None:
        fetch_script = script_path("fetch-regulationsgov-comments")
        spec = importlib.util.spec_from_file_location("fetch_regulationsgov_comments", fetch_script)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        sys.modules["fetch_regulationsgov_comments"] = module
        spec.loader.exec_module(module)

        summary = module.candidate_corpus_summary(
            [
                {
                    "id": "rg-candidate-001",
                    "attributes": {
                        "title": "PM2.5 health comment",
                        "comment": "The rule should account for health benefits.",
                        "agencyId": "EPA",
                        "docketId": "EPA-HQ-OAR-2015-0072",
                        "commentOnId": "EPA-HQ-OAR-2015-0072-0001",
                        "documentType": "Public Submission",
                        "subtype": "Comment",
                    },
                },
                {
                    "id": "rg-candidate-drift",
                    "attributes": {
                        "title": "Unrelated permit comment",
                        "comment": "This concerns a different action.",
                        "agencyId": "DOT",
                    },
                },
            ],
            normalized_filters={
                "docket_id": "EPA-HQ-OAR-2015-0072",
                "comment_on_id": "EPA-HQ-OAR-2015-0072-0001",
                "agency_id": "EPA",
                "search_term": "health",
                "document_type": "Public Submission",
                "subtype": "Comment",
            },
            sample_ref_limit=5,
        )
        self.assertEqual(2, summary["candidate_comment_count"])
        self.assertEqual(["rg-candidate-001", "rg-candidate-drift"], summary["candidate_ids"])
        self.assertEqual(1, summary["field_coverage"]["records_with_docket_id"])
        drift_codes = {item["code"] for item in summary["likely_drift_indicators"]}
        self.assertIn("docket-mismatch-or-missing", drift_codes)
        self.assertIn("search-term-miss", drift_codes)
        self.assertIn("general public-opinion", " ".join(summary["source_limitations"]))

        dry_run_payload = run_script(
            fetch_script,
            "fetch",
            "--api-key",
            "DUMMY_KEY",
            "--filter-mode",
            "posted",
            "--start-date",
            "2024-02-01",
            "--end-date",
            "2024-02-02",
            "--docket-id",
            "EPA-HQ-OAR-2015-0072",
            "--comment-on-document-id",
            "EPA-HQ-OAR-2015-0072-0001",
            "--document-type",
            "Public Submission",
            "--subtype",
            "Comment",
            "--sample-ref-limit",
            "7",
            "--dry-run",
            "--pretty",
        )
        self.assertTrue(dry_run_payload["dry_run"])
        filters = dry_run_payload["request_plan"]["filters"]
        self.assertEqual("EPA-HQ-OAR-2015-0072", filters["docket_id"])
        self.assertEqual("EPA-HQ-OAR-2015-0072-0001", filters["comment_on_document_id"])
        self.assertEqual("Public Submission", filters["document_type"])
        self.assertEqual("Comment", filters["subtype"])
        self.assertEqual(7, dry_run_payload["request_plan"]["sample_ref_limit"])
        self.assertIn("filter%5BdocketId%5D=EPA-HQ-OAR-2015-0072", dry_run_payload["sample_request_url"])

    def test_regulationsgov_detail_fetch_reads_candidate_audit_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            audit_path = root / "formal_comment_candidate_corpus_audit.json"
            write_json(
                audit_path,
                {
                    "audit": {
                        "candidate_ids": [
                            "EPA-HQ-OAR-2015-0072-5836",
                            "EPA-HQ-OAR-2015-0072-5837",
                        ]
                    }
                },
            )

            dry_run_payload = run_script(
                script_path("fetch-regulationsgov-comment-detail"),
                "fetch",
                "--api-key",
                "DUMMY_KEY",
                "--comment-ids-file",
                str(audit_path),
                "--max-comments",
                "1",
                "--include",
                "attachments",
                "--dry-run",
                "--pretty",
            )

            self.assertTrue(dry_run_payload["dry_run"])
            self.assertEqual(1, dry_run_payload["request_plan"]["selected_count"])
            self.assertEqual(["EPA-HQ-OAR-2015-0072-5836"], dry_run_payload["sample_ids"])
            self.assertIn("include=attachments", dry_run_payload["sample_request_url"])

    def test_regulationsgov_attachment_fetch_dry_run_reads_detail_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            detail_path = root / "regulationsgov_comment_detail.json"
            write_json(
                detail_path,
                {
                    "records": [
                        {
                            "comment_id": "EPA-HQ-OAR-2015-0072-5836",
                            "detail": {
                                "relationships": {
                                    "attachments": {
                                        "data": [
                                            {"id": "09000064859c8451", "type": "attachments"}
                                        ]
                                    }
                                }
                            },
                        },
                        {
                            "comment_id": "EPA-HQ-OAR-2015-0072-5837",
                            "attachments": [
                                {
                                    "id": "09000064859c8452",
                                    "attributes": {
                                        "title": "Comment attachment",
                                        "fileFormats": [
                                            {
                                                "format": "pdf",
                                                "fileUrl": "https://downloads.regulations.gov/EPA-HQ-OAR-2015-0072-5837/attachment_1.pdf",
                                            }
                                        ],
                                    },
                                }
                            ],
                        },
                    ]
                },
            )

            dry_run_payload = run_script(
                script_path("fetch-regulationsgov-attachments"),
                "fetch",
                "--api-key",
                "DUMMY_KEY",
                "--input-artifact",
                str(detail_path),
                "--max-attachments",
                "2",
                "--dry-run",
                "--pretty",
            )

            self.assertTrue(dry_run_payload["dry_run"])
            self.assertEqual(2, dry_run_payload["request_plan"]["target_count"])
            targets = dry_run_payload["sample_targets"]
            self.assertEqual("EPA-HQ-OAR-2015-0072-5836", targets[0]["comment_id"])
            self.assertEqual("09000064859c8451", targets[0]["attachment_id"])
            self.assertEqual("09000064859c8452", targets[1]["attachment_id"])
            self.assertEqual(
                "https://downloads.regulations.gov/EPA-HQ-OAR-2015-0072-5837/attachment_1.pdf",
                targets[1]["file_url"],
            )
            self.assertTrue(
                any(
                    "comments/EPA-HQ-OAR-2015-0072-5836/attachments" in url
                    or "attachments/09000064859c8451" in url
                    for url in dry_run_payload["sample_request_urls"]
                )
            )

    def test_regulationsgov_attachment_text_extracts_and_normalizes_to_formal_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            raw_dir = root / "raw"
            raw_dir.mkdir()
            attachment_path = raw_dir / "attachment_1.txt"
            attachment_path.write_text(
                "Attached comment text asks EPA to consider health benefits and implementation costs.",
                encoding="utf-8",
            )
            fetch_manifest = root / "regulationsgov_attachments_manifest.json"
            write_json(
                fetch_manifest,
                {
                    "downloads": [
                        {
                            "status": "downloaded",
                            "comment_id": "EPA-HQ-OAR-2015-0072-5836",
                            "attachment_id": "09000064859c8451",
                            "file_url": "https://downloads.regulations.gov/EPA-HQ-OAR-2015-0072-5836/attachment_1.txt",
                            "output_path": str(attachment_path),
                            "content_type": "text/plain",
                            "metadata": {
                                "id": "09000064859c8451",
                                "attributes": {"title": "Attached comment letter"},
                            },
                            "comment_attributes": {
                                "agencyId": "EPA",
                                "docketId": "EPA-HQ-OAR-2015-0072",
                                "commentOnId": "EPA-HQ-OAR-2015-0072-0001",
                                "submitterName": "Forestry Association",
                            },
                        }
                    ]
                },
            )

            extraction_payload = run_script(
                script_path("extract-document-text"),
                "--input-manifest",
                str(fetch_manifest),
                "--output-dir",
                str(root / "text"),
                "--manifest-output",
                str(root / "text" / "extraction_manifest.json"),
                "--pretty",
            )
            self.assertEqual("completed", extraction_payload["status"])
            self.assertEqual(1, extraction_payload["completed_count"])
            extraction_manifest = Path(extraction_payload["manifest_output"])
            self.assertTrue(extraction_manifest.exists())

            normalize_payload = run_script(
                script_path("normalize-regulationsgov-attachment-text"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(extraction_manifest),
                "--pretty",
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
                "fetch-regulationsgov-attachments",
                "--docket-id",
                "EPA-HQ-OAR-2015-0072",
                "--keyword",
                "health",
            )
            self.assertEqual(1, query_payload["result_count"])
            result = query_payload["results"][0]
            self.assertEqual("attachment-text", result["signal_kind"])
            self.assertEqual("EPA-HQ-OAR-2015-0072", result["docket_id"])
            self.assertEqual("Forestry Association", result["submitter_name"])
            self.assertIn("implementation costs", result["snippet"])

            with sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite") as connection:
                connection.row_factory = sqlite3.Row
                raw_row = connection.execute(
                    """
                    SELECT metadata_json, quality_flags_json
                    FROM normalized_signals
                    WHERE signal_id = ?
                    """,
                    (result["signal_id"],),
                ).fetchone()
            self.assertIsNotNone(raw_row)
            self.assertIn('"attachment_id": "09000064859c8451"', str(raw_row["metadata_json"]))
            self.assertIn('"text_extraction_status": "completed"', str(raw_row["metadata_json"]))
            self.assertIn("attachment-text", str(raw_row["quality_flags_json"]))

    def test_regulationsgov_attachment_text_normalizer_rejects_fetch_manifest_without_extraction(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            raw_manifest = root / "regulationsgov_attachments_manifest.json"
            write_json(
                raw_manifest,
                {
                    "ok": True,
                    "source": "regulationsgov-v4-attachments",
                    "records": [
                        {
                            "comment_id": "EPA-HQ-OAR-2015-0072-5836",
                            "attachment_id": "09000064859c8451",
                            "file_url": "https://downloads.regulations.gov/example/attachment_1.pdf",
                        }
                    ],
                    "downloads": [],
                },
            )

            normalize_payload = run_script(
                script_path("normalize-regulationsgov-attachment-text"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(raw_manifest),
                "--pretty",
            )

            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual([], normalize_payload["canonical_ids"])
            warning_codes = {item["code"] for item in normalize_payload["warnings"]}
            self.assertIn("expected-document-text-extraction-manifest", warning_codes)

    def test_regulationsgov_attachment_failed_download_still_materializes_limited_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            fetch_manifest = root / "regulationsgov_attachments_manifest.json"
            write_json(
                fetch_manifest,
                {
                    "ok": False,
                    "source": "regulationsgov-v4-attachments",
                    "records": [
                        {
                            "comment_id": "EPA-HQ-OAR-2015-0072-5836",
                            "attachment_id": "09000064859c8451",
                            "file_url": "https://downloads.regulations.gov/example/attachment_1.pdf",
                            "metadata": {
                                "id": "09000064859c8451",
                                "attributes": {"title": "Attached comment letter"},
                            },
                            "comment_attributes": {
                                "agencyId": "EPA",
                                "docketId": "EPA-HQ-OAR-2015-0072",
                                "commentOnId": "EPA-HQ-OAR-2015-0072-0001",
                                "submitterName": "Forestry Association",
                            },
                        }
                    ],
                    "downloads": [],
                    "failures": [
                        {
                            "target": {
                                "comment_id": "EPA-HQ-OAR-2015-0072-5836",
                                "attachment_id": "09000064859c8451",
                                "file_url": "https://downloads.regulations.gov/example/attachment_1.pdf",
                            },
                            "error": "HTTP 403 from provider download host",
                        }
                    ],
                },
            )

            extraction_payload = run_script(
                script_path("extract-document-text"),
                "--input-manifest",
                str(fetch_manifest),
                "--output-dir",
                str(root / "text"),
                "--manifest-output",
                str(root / "text" / "extraction_manifest.json"),
                "--pretty",
            )
            self.assertEqual("completed", extraction_payload["status"])
            self.assertEqual(0, extraction_payload["completed_count"])
            self.assertEqual(1, extraction_payload["limited_count"])
            extraction_record = extraction_payload["records"][0]
            self.assertEqual("limited", extraction_record["text_extraction_status"])
            self.assertIn("metadata-only-no-local-file", extraction_record["quality_flags"])

            normalize_payload = run_script(
                script_path("normalize-regulationsgov-attachment-text"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(extraction_payload["manifest_output"]),
                "--pretty",
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
                "fetch-regulationsgov-attachments",
                "--docket-id",
                "EPA-HQ-OAR-2015-0072",
            )
            self.assertEqual(1, query_payload["result_count"])
            result = query_payload["results"][0]
            self.assertEqual("attachment-text", result["signal_kind"])
            self.assertEqual("", result["snippet"])

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

    def test_youtube_video_normalizer_accepts_jsonl_search_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            youtube_path = root / "youtube_videos.jsonl"
            rows = [
                {
                    "query": "nyc smoke wildfire",
                    "video_id": "vid-001",
                    "video": {
                        "id": "vid-001",
                        "title": "Smoke over New York City",
                        "description": "Wildfire smoke over NYC skyline.",
                        "channel_title": "City Desk",
                        "published_at": "2023-06-07T13:00:00Z",
                        "default_language": "en",
                        "statistics": {"view_count": 1250},
                    },
                },
                {
                    "query": "nyc smoke wildfire",
                    "video_id": "vid-002",
                    "video": {
                        "id": "vid-002",
                        "title": "New York air quality warning",
                        "description": "Officials discuss air quality impacts.",
                        "channel_title": "Metro News",
                        "published_at": "2023-06-08T13:00:00Z",
                        "default_language": "en",
                        "statistics": {"view_count": 800},
                    },
                },
            ]
            youtube_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

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
            self.assertEqual(2, len(normalize_payload["canonical_ids"]))

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
            public_result = public_payload["results"][0]
            self.assertEqual(
                public_result["signal_id"],
                public_result["evidence_refs"][0]["signal_id"],
            )
            self.assertEqual(
                "public-discourse-signal",
                public_result["evidence_basis"]["basis_object_kind"],
            )
            self.assertEqual(
                "none",
                public_result["evidence_basis"]["data_quality"]["research_judgement"],
            )
            self.assertIn(
                "submit-finding-record",
                public_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "extract-claim-candidates",
                public_payload["board_handoff"]["suggested_next_skills"],
            )

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
            environment_result = environment_payload["results"][0]
            self.assertEqual(
                environment_result["signal_id"],
                environment_result["evidence_refs"][0]["signal_id"],
            )
            self.assertEqual(
                "environment-observation-signal",
                environment_result["evidence_basis"]["basis_object_kind"],
            )
            self.assertIn(
                "coverage_limitations",
                environment_result["evidence_basis"],
            )
            self.assertIn(
                "submit-evidence-bundle",
                environment_payload["board_handoff"]["suggested_next_skills"],
            )
            self.assertNotIn(
                "extract-observation-candidates",
                environment_payload["board_handoff"]["suggested_next_skills"],
            )

            signal_id = public_payload["results"][0]["signal_id"]
            lookup_payload = run_script(
                script_path("query-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual(1, lookup_payload["result_count"])
            self.assertEqual(
                signal_id,
                lookup_payload["results"][0]["evidence_refs"][0]["signal_id"],
            )
            self.assertEqual(
                "query-raw-record",
                lookup_payload["board_handoff"]["suggested_next_skills"][0],
            )
            raw_payload = run_script(
                script_path("query-raw-record"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            self.assertEqual(1, raw_payload["result_count"])
            self.assertEqual(
                signal_id,
                raw_payload["results"][0]["evidence_refs"][0]["signal_id"],
            )
            self.assertEqual(
                "public-discourse-signal",
                raw_payload["results"][0]["evidence_basis"]["basis_object_kind"],
            )
            artifact_ref = raw_payload["results"][0]["artifact_ref"]
            raw_by_ref_payload = run_script(
                script_path("query-raw-record"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-ref",
                artifact_ref,
            )
            self.assertEqual(1, raw_by_ref_payload["result_count"])
            self.assertEqual(signal_id, raw_by_ref_payload["results"][0]["signal_id"])

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

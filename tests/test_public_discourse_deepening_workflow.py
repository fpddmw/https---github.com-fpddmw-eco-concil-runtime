from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import (
    analytics_path,
    load_json,
    run_kernel,
    run_script,
    runtime_src_path,
    script_path,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-public-discourse-deepening"
ROUND_ID = "round-public-discourse-deepening"


def insert_signal(
    run_dir: Path,
    *,
    signal_id: str,
    plane: str,
    source_skill: str,
    signal_kind: str,
    title: str,
    body_text: str,
    metric: str = "",
    numeric_value: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    from eco_council_runtime.kernel.planes.signal import (
        INSERT_SQL,
        ensure_signal_plane_schema,
        resolved_canonical_object_kind,
    )

    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path = str(run_dir / "raw" / f"{signal_id}.json")
    row = {
        "signal_id": signal_id,
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "plane": plane,
        "batch_id": f"batch-{signal_id}",
        "source_skill": source_skill,
        "signal_kind": signal_kind,
        "canonical_object_kind": resolved_canonical_object_kind(
            plane=plane,
            source_skill=source_skill,
            signal_kind=signal_kind,
        ),
        "external_id": signal_id,
        "dedupe_key": signal_id,
        "title": title,
        "body_text": body_text,
        "url": f"https://example.test/{signal_id}",
        "author_name": "Fixture Author",
        "channel_name": "Fixture Channel",
        "language": "en",
        "query_text": "nyc wildfire smoke",
        "metric": metric,
        "numeric_value": numeric_value,
        "unit": "score" if metric else "",
        "published_at_utc": "2023-06-07T15:00:00Z",
        "observed_at_utc": "",
        "window_start_utc": "",
        "window_end_utc": "",
        "captured_at_utc": "2026-05-13T00:00:00Z",
        "latitude": None,
        "longitude": None,
        "bbox_json": "{}",
        "quality_flags_json": "[]",
        "engagement_json": "{}",
        "metadata_json": json.dumps(metadata or {}, ensure_ascii=True, sort_keys=True),
        "raw_json": "{}",
        "artifact_path": artifact_path,
        "record_locator": "$",
        "artifact_sha256": "",
    }
    with sqlite3.connect(db_path) as connection:
        ensure_signal_plane_schema(connection)
        connection.execute(INSERT_SQL, row)
        connection.commit()


def seed_discourse_signals(run_dir: Path) -> None:
    insert_signal(
        run_dir,
        signal_id="sig-youtube-video",
        plane="public",
        source_skill="fetch-youtube-video-search",
        signal_kind="video",
        title="NYC wildfire smoke video",
        body_text="Public video about NYC wildfire smoke and orange skies.",
    )
    insert_signal(
        run_dir,
        signal_id="sig-youtube-comment",
        plane="public",
        source_skill="fetch-youtube-comments",
        signal_kind="comment",
        title="YouTube comment on NYC smoke",
        body_text="I am worried about health risks from the wildfire smoke and need mask guidance.",
    )
    insert_signal(
        run_dir,
        signal_id="sig-gdelt-gkg",
        plane="public",
        source_skill="fetch-gdelt-gkg",
        signal_kind="gkg-row",
        title="GDELT media tone row",
        body_text="themes=ENV_WILDFIRE; tone=-3.5; source narrative mentions Canada wildfire smoke.",
        metric="v2_tone",
        numeric_value=-3.5,
        metadata={
            "gdelt_tone_kind": "gdelt_media_tone",
            "tone_semantics": "media_or_document_tone_not_public_response_sentiment",
        },
    )
    insert_signal(
        run_dir,
        signal_id="sig-formal-comment",
        plane="formal",
        source_skill="fetch-regulationsgov-comments",
        signal_kind="comment",
        title="Formal comment about smoke health impacts",
        body_text="The agency should address smoke health impacts and public information needs.",
    )


def submit_source_attempt(
    run_dir: Path,
    *,
    source_skill: str,
    status: str,
    rationale: str,
) -> str:
    proposal = run_script(
        script_path("submit-source-acquisition-proposal"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--author-role",
        "social-investigator",
        "--source-skill",
        source_skill,
        "--query-parameters-json",
        "{\"query\":\"nyc wildfire smoke public reaction\",\"window\":\"2023-06-07\"}",
        "--target-kind",
        "round",
        "--target-id",
        ROUND_ID,
        "--rationale",
        rationale,
        "--provenance-json",
        "{\"source\":\"unit-test\"}",
    )
    proposal_id = str(proposal["summary"]["object_id"])
    run_script(
        script_path("update-source-acquisition-proposal-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--object-id",
        proposal_id,
        "--status",
        status,
        "--actor-role",
        "social-investigator",
        "--status-rationale",
        rationale,
        "--evidence-ref",
        f"receipt://{source_skill}/{status}",
        "--provenance-json",
        "{\"source\":\"unit-test\"}",
    )
    return proposal_id


class PublicDiscourseDeepeningWorkflowTests(unittest.TestCase):
    def test_annotation_worker_includes_water_governance_cues(self) -> None:
        from eco_council_runtime.optional_analysis.public_discourse_annotation_worker import (
            _candidate_labels_for_text,
        )

        labels = _candidate_labels_for_text(
            "Public comments oppose the Glen Canyon dam release plan because Lake Powell reservoir levels, "
            "water supply allocation, hydropower, and endangered fish habitat may be affected. "
            "Community voice was ignored, public trust collapsed, Reclamation should revise the policy, "
            "and policy effects remain uncertain.",
            max_labels_per_family=10,
        )
        labels_by_family = {(item["label_family"], item["label"]) for item in labels}

        self.assertIn(("issue_facets", "water-supply-risk"), labels_by_family)
        self.assertIn(("issue_facets", "reservoir-or-release-operations"), labels_by_family)
        self.assertIn(("issue_facets", "ecological-or-habitat-risk"), labels_by_family)
        self.assertIn(("issue_facets", "formal-governance-process"), labels_by_family)
        self.assertIn(("source_narrative_labels", "water-release-operations"), labels_by_family)
        self.assertIn(("source_narrative_labels", "reservoir-levels"), labels_by_family)
        self.assertIn(("source_narrative_labels", "basin-allocation-conflict"), labels_by_family)
        self.assertIn(("affect_labels", "opposition-or-criticism"), labels_by_family)
        self.assertIn(("policy_demand_labels", "policy-revision-demand"), labels_by_family)
        self.assertIn(("trust_confidence_labels", "distrust-in-agency"), labels_by_family)
        self.assertIn(("trust_confidence_labels", "ignored-community-voice"), labels_by_family)
        self.assertIn(("uncertainty_labels", "policy-effect-uncertainty"), labels_by_family)
        self.assertIn(("responsibility_attribution_labels", "agency-responsibility"), labels_by_family)

    def test_gdelt_doc_tone_aggregate_uses_distinct_lane_from_doc_recon(self) -> None:
        from eco_council_runtime.optional_analysis.public_discourse import public_discourse_lane

        self.assertEqual(
            "gdelt_doc_recon",
            public_discourse_lane(
                {
                    "source_skill": "fetch-gdelt-doc-search",
                    "metric": "",
                    "metadata": {"gdelt_doc_kind": "gdelt_doc_recon"},
                }
            ),
        )
        self.assertEqual(
            "gdelt_doc_tone_aggregate",
            public_discourse_lane(
                {
                    "source_skill": "fetch-gdelt-doc-search",
                    "metric": "doc_timeline_tone",
                    "metadata": {
                        "gdelt_doc_kind": "gdelt_doc_tone_aggregate",
                        "doc_mode": "timelinetone",
                    },
                }
            ),
        )

    def test_annotation_worker_classifies_sample_and_aggregation_consumes_basis_from_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            seed_discourse_signals(run_dir)

            corpus_payload = run_script(
                script_path("materialize-public-discourse-corpus"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            annotation_payload = run_script(
                script_path("classify-public-discourse-affect"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
            )
            annotation_artifact = load_json(
                analytics_path(run_dir, f"public_discourse_affect_annotations_{ROUND_ID}.json")
            )

            self.assertEqual("completed", annotation_payload["status"])
            self.assertGreater(annotation_payload["summary"]["annotation_count"], 0)
            self.assertEqual("public-discourse-annotation-worker", annotation_artifact["annotation_worker_role"])
            labels_by_signal = {
                (item["signal_id"], item["label_family"], item["label"])
                for item in annotation_artifact["annotations"]
            }
            self.assertIn(("sig-youtube-comment", "affect_labels", "concern"), labels_by_signal)
            self.assertIn(("sig-youtube-comment", "issue_facets", "health-risk"), labels_by_signal)
            self.assertTrue(
                all(item["label_semantics"]["source_family"] for item in annotation_artifact["annotations"])
            )
            self.assertTrue(
                all(item["denominator_boundary"] for item in annotation_artifact["annotations"])
            )

            aggregation_payload = run_script(
                script_path("aggregate-public-discourse-annotations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
                "--annotations-path",
                annotation_payload["summary"]["output_path"],
            )
            aggregation_artifact = load_json(
                analytics_path(run_dir, f"public_discourse_annotation_aggregation_{ROUND_ID}.json")
            )

            self.assertEqual("completed", aggregation_payload["status"])
            affect_labels = {item["label"] for item in aggregation_artifact["social_affect_distribution"]}
            self.assertIn("concern", affect_labels)
            concern_distribution = next(
                item for item in aggregation_artifact["social_affect_distribution"] if item["label"] == "concern"
            )
            self.assertIn(
                "public-discourse-annotation-worker",
                concern_distribution["provenance"]["annotation_sources"],
            )

    def test_formal_comment_issue_worker_labels_detail_and_attachment_text(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            insert_signal(
                run_dir,
                signal_id="sig-formal-detail-health",
                plane="formal",
                source_skill="fetch-regulationsgov-comment-detail",
                signal_kind="comment-detail",
                title="Comment supports stronger PM2.5 health protections",
                body_text=(
                    "We support stronger PM2.5 standards because public health benefits for children "
                    "and asthma prevention are needed."
                ),
                metadata={"docket_id": "EPA-HQ-OAR-2015-0072", "submitter_name": "Health Coalition"},
            )
            insert_signal(
                run_dir,
                signal_id="sig-formal-attachment-cost",
                plane="formal",
                source_skill="fetch-regulationsgov-attachments",
                signal_kind="attachment-text",
                title="Attached industry comment",
                body_text=(
                    "We oppose the deadline because compliance cost, economic burden, feasibility, "
                    "and state implementation discretion require further review."
                ),
                metadata={
                    "docket_id": "EPA-HQ-OAR-2015-0072",
                    "attachment_id": "09000064859c8451",
                    "submitter_name": "Industry Association",
                },
            )

            annotation_payload = run_script(
                script_path("classify-formal-comment-issues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-labels-per-family",
                "5",
            )
            annotation_artifact = load_json(
                analytics_path(run_dir, f"formal_comment_issue_annotations_{ROUND_ID}.json")
            )

            self.assertEqual("completed", annotation_payload["status"])
            self.assertEqual(2, annotation_payload["summary"]["sample_count"])
            self.assertGreaterEqual(annotation_payload["summary"]["annotation_count"], 6)
            self.assertEqual("formal-comment-issue-worker", annotation_artifact["annotation_worker_role"])
            self.assertEqual(
                "DB-visible formal comment text signals only",
                annotation_artifact["sample_definition"]["sample_boundary"],
            )
            labels_by_signal = {
                (item["signal_id"], item["label_family"], item["label"])
                for item in annotation_artifact["annotations"]
            }
            self.assertIn(("sig-formal-detail-health", "formal_issue_labels", "health-benefit"), labels_by_signal)
            self.assertIn(("sig-formal-detail-health", "formal_stance_hints", "support"), labels_by_signal)
            self.assertIn(("sig-formal-detail-health", "formal_concern_facets", "health"), labels_by_signal)
            self.assertIn(("sig-formal-attachment-cost", "formal_issue_labels", "compliance-cost"), labels_by_signal)
            self.assertIn(("sig-formal-attachment-cost", "formal_stance_hints", "oppose"), labels_by_signal)
            self.assertIn(("sig-formal-attachment-cost", "formal_concern_facets", "federalism"), labels_by_signal)
            self.assertTrue(
                all(item["label_method"] for item in annotation_artifact["annotations"])
            )
            self.assertIn(
                "Sample label counts are not general public-opinion estimates.",
                annotation_artifact["representativeness_limits"],
            )

            aggregation_payload = run_script(
                script_path("aggregate-public-discourse-annotations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--annotations-path",
                annotation_payload["summary"]["output_path"],
            )
            aggregation_artifact = load_json(
                analytics_path(run_dir, f"public_discourse_annotation_aggregation_{ROUND_ID}.json")
            )

            self.assertEqual("completed", aggregation_payload["status"])
            formal_distributions = {
                (item["label_family"], item["label"]): item
                for item in aggregation_artifact["annotation_distributions"]
            }
            self.assertIn(("formal_issue_labels", "health-benefit"), formal_distributions)
            self.assertIn(("formal_stance_hints", "oppose"), formal_distributions)
            self.assertEqual(
                2,
                aggregation_artifact["distribution_denominators"]["eligible_signal_count"],
            )
            self.assertTrue(
                aggregation_artifact["distribution_use_policy"]["sample_fractions_are_sample_local"]
            )

    def test_corpus_and_coverage_helpers_preserve_sample_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            seed_discourse_signals(run_dir)

            corpus_payload = run_script(
                script_path("materialize-public-discourse-corpus"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            corpus_artifact = load_json(analytics_path(run_dir, f"public_discourse_corpus_{ROUND_ID}.json"))

            self.assertEqual("completed", corpus_payload["status"])
            self.assertEqual(4, corpus_payload["sample_count"])
            self.assertEqual(4, corpus_artifact["sample_count"])
            self.assertEqual(4, corpus_artifact["eligible_count"])
            self.assertEqual(4, corpus_artifact["dedup_count"])
            self.assertEqual(
                "DB-visible normalized public/formal text sample only",
                corpus_artifact["sample_definition"]["sample_boundary"],
            )
            self.assertEqual(
                "normalized public/formal text-bearing signal",
                corpus_artifact["text_unit"],
            )
            self.assertEqual(
                "one corpus item per normalized signal_id after signal-plane dedupe",
                corpus_artifact["dedupe_policy"],
            )
            self.assertTrue(corpus_artifact["inclusion_filters"]["requires_text"])
            self.assertIn("signals without text", corpus_artifact["exclusion_filters"])
            self.assertTrue(corpus_artifact["query_variants"])
            self.assertTrue(
                corpus_artifact["denominator_policy"]["denominators_are_source_family_local"]
            )
            self.assertTrue(
                corpus_artifact["denominator_policy"][
                    "do_not_mix_gdelt_youtube_bluesky_formal_comments"
                ]
            )
            self.assertTrue(
                corpus_artifact["denominator_policy"][
                    "do_not_mix_gdelt_youtube_bluesky_formal_records_formal_comments"
                ]
            )
            denominator_by_family = {
                item["source_family"]: item
                for item in corpus_artifact["source_family_denominators"]
            }
            self.assertEqual(
                2,
                denominator_by_family["youtube-public-discourse"]["denominator"],
            )
            self.assertEqual(
                1,
                denominator_by_family["gdelt-public-record"]["denominator"],
            )
            self.assertIn(
                "regulationsgov-formal-comments",
                denominator_by_family["youtube-public-discourse"]["do_not_mix_with"],
            )
            self.assertIn("formal-record", denominator_by_family)
            family_audit = {
                item["source_family"]: item
                for item in corpus_artifact["source_family_audit"]
            }
            self.assertEqual(
                2,
                family_audit["youtube-public-discourse"]["eligible_count"],
            )
            self.assertEqual(
                2,
                family_audit["youtube-public-discourse"]["dedup_count"],
            )
            self.assertIn(
                "GDELT DOC/tone/table rows are media/document visibility or tone material, not public sentiment denominator.",
                family_audit["gdelt-public-record"]["failure_rationale"],
            )
            lane_counts = {
                item["discourse_lane"]: item["signal_count"]
                for item in corpus_artifact["discourse_lane_counts"]
            }
            self.assertEqual(1, lane_counts["social_sample_affect"])
            self.assertEqual(1, lane_counts["gdelt_media_tone"])
            sample_class_counts = {
                item["sample_class"]: item["signal_count"]
                for item in corpus_artifact["sample_class_counts"]
            }
            self.assertEqual(1, sample_class_counts["media_document_sample"])
            self.assertEqual(1, sample_class_counts["platform_visibility_sample"])
            self.assertEqual(1, sample_class_counts["platform_comment_sample"])
            self.assertEqual(1, sample_class_counts["formal_participation_sample"])
            self.assertTrue(
                any(warning["code"] == "gdelt-tone-boundary" for warning in corpus_artifact["warnings"])
            )
            first_item = corpus_artifact["corpus_items"][0]
            self.assertIn("sample_class", first_item)
            self.assertIn("text_unit", first_item)
            self.assertIn("evidence_refs", first_item)
            self.assertNotIn("recommended_conclusion", first_item)
            self.assertNotIn("rank", first_item)

            coverage_payload = run_script(
                script_path("audit-public-discourse-sample-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
            )
            coverage_artifact = load_json(analytics_path(run_dir, f"public_discourse_coverage_audit_{ROUND_ID}.json"))

            self.assertEqual("completed", coverage_payload["status"])
            self.assertEqual(5, coverage_payload["summary"]["coverage_cue_count"])
            self.assertEqual(5, len(coverage_artifact["coverage_cues"]))
            self.assertEqual(
                4,
                coverage_artifact["observed_inputs"]["corpus_item_count"],
            )
            self.assertFalse(
                any(warning["code"] == "no-social-sample-affect-basis" for warning in coverage_artifact["warnings"])
            )
            first_cue = coverage_artifact["coverage_cues"][0]
            self.assertEqual("requires-human-review", first_cue["audit_status"])
            self.assertIn("sample_definition", first_cue)
            self.assertIn("eligible_count", first_cue)
            self.assertIn("dedup_count", first_cue)
            self.assertIn("denominator_policy", first_cue)
            self.assertIn("coverage_layers", first_cue)
            self.assertNotIn("severity", first_cue)
            self.assertNotIn("coverage_score", first_cue)
            coverage_family_audit = {
                item["source_family"]: item
                for item in coverage_artifact["source_family_audit"]
            }
            self.assertTrue(
                coverage_artifact["denominator_policy"]["denominators_are_source_family_local"]
            )
            self.assertEqual(
                2,
                coverage_family_audit["youtube-public-discourse"]["dedup_count"],
            )
            self.assertIn(
                "GDELT DOC/tone/table rows are media/document visibility or tone material, not public sentiment denominator.",
                coverage_family_audit["gdelt-public-record"]["failure_rationale"],
            )

            annotations_path = Path(tmpdir) / "annotations.jsonl"
            annotations_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "signal_id": "sig-youtube-comment",
                                "label_family": "affect_labels",
                                "label": "concern",
                            },
                            sort_keys=True,
                        ),
                        json.dumps(
                            {
                                "signal_id": "sig-youtube-comment",
                                "label_family": "issue_facets",
                                "label": "health-risk",
                            },
                            sort_keys=True,
                        ),
                        json.dumps(
                            {
                                "signal_id": "sig-gdelt-gkg",
                                "label_family": "source_narrative_labels",
                                "label": "canada-wildfires",
                            },
                            sort_keys=True,
                        ),
                        json.dumps(
                            {
                                "signal_id": "sig-formal-comment",
                                "label_family": "affect_labels",
                                "label": "concern",
                            },
                            sort_keys=True,
                        ),
                        json.dumps(
                            {
                                "signal_id": "sig-formal-comment",
                                "label_family": "formal_policy_semantic_labels",
                                "label": "public-participation-mechanism",
                            },
                            sort_keys=True,
                        ),
                        json.dumps(
                            {
                                "signal_id": "sig-youtube-comment",
                                "label_family": "trust_confidence_labels",
                                "label": "transparency-concern",
                            },
                            sort_keys=True,
                        ),
                        json.dumps(
                            {
                                "signal_id": "sig-youtube-comment",
                                "label_family": "uncertainty_labels",
                                "label": "health-risk-uncertainty",
                            },
                            sort_keys=True,
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            aggregation_payload = run_script(
                script_path("aggregate-public-discourse-annotations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
                "--annotations-path",
                str(annotations_path),
                "--annotation-basis-ref",
                "annotation-basis://fixture/public-discourse-v1",
            )
            aggregation_artifact = load_json(
                analytics_path(run_dir, f"public_discourse_annotation_aggregation_{ROUND_ID}.json")
            )

            self.assertEqual("completed", aggregation_payload["status"])
            self.assertEqual(7, aggregation_payload["summary"]["annotation_count"])
            self.assertEqual(1, len(aggregation_artifact["social_affect_distribution"]))
            self.assertEqual("concern", aggregation_artifact["social_affect_distribution"][0]["label"])
            self.assertEqual(1.0, aggregation_artifact["social_affect_distribution"][0]["sample_fraction"])
            self.assertEqual(
                "youtube-public-discourse",
                aggregation_artifact["social_affect_distribution"][0]["semantic_scope"]["source_family"],
            )
            self.assertEqual(
                "social_sample_affect",
                aggregation_artifact["social_affect_distribution"][0]["semantic_scope"]["discourse_lane"],
            )
            self.assertEqual(
                1,
                len(aggregation_artifact["formal_participation_affect_distribution"]),
            )
            self.assertEqual(
                "regulationsgov-formal-comments",
                aggregation_artifact["formal_participation_affect_distribution"][0]["semantic_scope"]["source_family"],
            )
            self.assertEqual(
                1,
                len(aggregation_artifact["formal_policy_semantic_distribution"]),
            )
            self.assertEqual(
                "public-participation-mechanism",
                aggregation_artifact["formal_policy_semantic_distribution"][0]["label"],
            )
            self.assertEqual(
                "transparency-concern",
                aggregation_artifact["trust_confidence_distribution"][0]["label"],
            )
            self.assertEqual(
                "health-risk-uncertainty",
                aggregation_artifact["uncertainty_distribution"][0]["label"],
            )
            self.assertTrue(aggregation_artifact["semantic_distributions"])
            self.assertEqual(
                4,
                aggregation_artifact["distribution_denominators"]["eligible_signal_count"],
            )
            self.assertEqual(
                1,
                aggregation_artifact["social_affect_distribution"][0]["label_family_denominator"],
            )
            self.assertTrue(
                aggregation_artifact["social_affect_distribution"][0]["labels_are_not_mutually_exclusive"]
            )
            self.assertTrue(
                aggregation_artifact["social_affect_distribution"][0]["fractions_do_not_sum_to_100_percent"]
            )
            self.assertEqual(
                "sample_fraction is label-family-local and must not be treated as public opinion share",
                aggregation_artifact["distribution_denominators"]["denominator_policy"],
            )
            self.assertIn(
                "source-family and discourse-lane local",
                aggregation_artifact["distribution_denominators"]["semantic_scope_policy"],
            )
            for distribution in aggregation_artifact["semantic_distributions"]:
                if "sample_fraction" in distribution:
                    self.assertIn("sample_definition", distribution)
                    self.assertIn("label_family_denominator", distribution)
                    self.assertTrue(
                        distribution["denominator_scope"]["do_not_mix_source_families"]
                    )
            self.assertEqual(
                "annotation-basis://fixture/public-discourse-v1",
                aggregation_artifact["social_affect_distribution"][0]["provenance"]["annotation_basis_refs"][0],
            )
            self.assertTrue(
                aggregation_artifact["distribution_use_policy"]["label_sets_are_non_exclusive"]
            )
            self.assertTrue(
                aggregation_artifact["distribution_use_policy"]["sample_fractions_are_sample_local"]
            )
            self.assertEqual(
                "public_source_narrative_cue_not_physical_source_attribution",
                aggregation_artifact["distribution_use_policy"]["source_narrative_boundary"],
            )
            self.assertNotIn("recommended_conclusion", aggregation_artifact)
            self.assertNotIn("ranked_items", aggregation_artifact)

            comparison_payload = run_script(
                script_path("compare-public-media-narratives"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
                "--aggregation-path",
                aggregation_payload["summary"]["output_path"],
            )
            comparison_artifact = load_json(
                analytics_path(run_dir, f"public_media_narrative_comparison_{ROUND_ID}.json")
            )

            self.assertEqual("completed", comparison_payload["status"])
            self.assertEqual(
                "sample_affect_not_public_opinion",
                comparison_artifact["social_sample_affect_summary"]["boundary"],
            )
            self.assertEqual(
                [{"average_value": -3.5, "max_value": -3.5, "metric": "v2_tone", "min_value": -3.5, "numeric_count": 1, "tone_boundary": "gdelt_media_or_doc_tone_not_public_response_sentiment"}],
                comparison_artifact["gdelt_media_tone_summary"],
            )
            self.assertTrue(
                any(warning["code"] == "gdelt-tone-boundary" for warning in comparison_artifact["warnings"])
            )
            self.assertIn("cannot_support", comparison_artifact["cross_source_comparison"])
            self.assertNotIn("alignment_score", comparison_artifact)
            self.assertNotIn("source_attribution", comparison_artifact)

            summary_payload = run_script(
                script_path("summarize-public-discourse-sample"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--corpus-path",
                corpus_payload["summary"]["output_path"],
                "--coverage-audit-path",
                coverage_payload["summary"]["output_path"],
                "--aggregation-path",
                aggregation_payload["summary"]["output_path"],
                "--comparison-path",
                comparison_payload["summary"]["output_path"],
            )
            summary_artifact = load_json(
                analytics_path(run_dir, f"public_discourse_sample_summary_{ROUND_ID}.json")
            )

            self.assertEqual("completed", summary_payload["status"])
            self.assertEqual(4, summary_payload["summary"]["sample_count"])
            self.assertEqual(4, summary_artifact["sample_count"])
            self.assertEqual("health-risk", summary_artifact["issue_distribution"][0]["label"])
            self.assertEqual("concern", summary_artifact["social_affect_distribution"][0]["label"])
            self.assertIn("sample_class_counts", summary_artifact)
            self.assertIn("text_unit_counts", summary_artifact)
            self.assertIn("sample_internal_distribution", summary_artifact)
            self.assertEqual(
                "sample_fraction is label-family-local and must not be treated as public opinion share",
                summary_artifact["sample_internal_distribution"]["distribution_denominators"]["denominator_policy"],
            )
            self.assertIn(
                "general public opinion estimates",
                summary_artifact["what_this_sample_cannot_support"]["en"],
            )
            self.assertIn(
                "Formal comments are described as institutional participation records, not general public opinion.",
                summary_artifact["recommended_report_language"]["en"],
            )
            self.assertIn(
                "Public source narratives prove the physical source ...",
                summary_artifact["forbidden_report_language"]["en"],
            )
            self.assertEqual(
                comparison_artifact["gdelt_media_tone_summary"],
                summary_artifact["gdelt_media_tone_summary"],
            )
            self.assertEqual(
                "advisory comparison of sample lanes only",
                summary_artifact["cross_source_comparison"]["comparison_scope"],
            )
            self.assertTrue(summary_artifact["example_refs"])
            self.assertTrue(
                summary_artifact["distribution_use_policy"]["requires_council_uptake_before_reporting"]
            )
            self.assertEqual(
                "media_or_document_tone_not_public_sentiment",
                summary_artifact["distribution_use_policy"]["gdelt_tone_boundary"],
            )
            self.assertIn("board_handoff", summary_artifact)
            self.assertIn("submit-agent-position", summary_artifact["board_handoff"]["suggested_next_skills"])
            self.assertNotIn("recommended_conclusion", summary_artifact)
            self.assertNotIn("readiness_score", summary_artifact)

            summary_id = summary_payload["summary_id"]
            summary_ref = summary_payload["artifact_refs"][0]["artifact_ref"]

            finding_payload = run_kernel(
                "submit-finding-record",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "social-investigator",
                "--agent-role",
                "social-investigator",
                "--finding-kind",
                "public-discourse-sample-finding",
                "--title",
                "Public discourse sample records health concern labels",
                "--summary",
                "The approved public discourse summary records health-risk and concern labels inside the fixture sample.",
                "--rationale",
                "The finding cites the optional-analysis summary and preserves its sample-only boundary.",
                "--confidence",
                "0.72",
                "--target-kind",
                "optional-analysis",
                "--target-id",
                summary_id,
                "--basis-object-id",
                summary_id,
                "--evidence-ref",
                summary_ref,
                "--provenance-json",
                "{\"source\":\"summarize-public-discourse-sample\"}",
            )
            finding_id = finding_payload["canonical_ids"][0]

            bundle_payload = run_kernel(
                "submit-evidence-bundle",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "social-investigator",
                "--agent-role",
                "social-investigator",
                "--bundle-kind",
                "public-discourse-sample-evidence-bundle",
                "--title",
                "Public discourse sample summary bundle",
                "--summary",
                "Bundle carries the approved summary artifact and the social-investigator finding.",
                "--rationale",
                "The bundle keeps advisory optional-analysis material attached to a council finding.",
                "--confidence",
                "0.74",
                "--target-kind",
                "finding",
                "--target-id",
                finding_id,
                "--basis-object-id",
                summary_id,
                "--finding-id",
                finding_id,
                "--evidence-ref",
                summary_ref,
                "--provenance-json",
                "{\"source\":\"public-discourse-summary-finding\"}",
            )
            bundle_id = bundle_payload["canonical_ids"][0]

            challenge_payload = run_script(
                script_path("open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Check sample-only wording for public discourse finding",
                "--challenge-statement",
                "The finding must not be used as a public-opinion estimate or physical source attribution.",
                "--priority",
                "medium",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                summary_ref,
                "--evidence-bundle-id",
                bundle_id,
            )
            challenge_id = challenge_payload["canonical_ids"][0]

            readiness_payload = run_script(
                script_path("submit-readiness-opinion"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "challenger",
                "--readiness-status",
                "blocked",
                "--rationale",
                "The sample summary can support bounded sample wording but is not sufficient for report basis without challenge resolution.",
                "--basis-object-id",
                finding_id,
                "--basis-object-id",
                bundle_id,
                "--basis-object-id",
                challenge_id,
                "--evidence-ref",
                summary_ref,
                "--provenance-json",
                "{\"source\":\"public-discourse-boundary-review\"}",
            )
            readiness_id = readiness_payload["canonical_ids"][0]

            synthesis_payload = run_script(
                script_path("submit-round-synthesis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--synthesis-text",
                "Public discourse sample material is council-visible only as a bounded finding, bundle, and challenger boundary.",
                "--stage-conclusion",
                "Continue public-discourse deepening only if the challenge is resolved or wording remains explicitly sample-bound.",
                "--rationale",
                "Record that optional-analysis material is advisory until carried by council objects.",
                "--covered-object-ref",
                finding_id,
                "--covered-object-ref",
                bundle_id,
                "--unresolved-object-ref",
                challenge_id,
                "--next-round-candidate-ref",
                challenge_id,
                "--known-fact",
                "No public-opinion estimate or physical source attribution has been accepted.",
                "--evidence-ref",
                summary_ref,
                "--lineage-id",
                readiness_id,
                "--provenance-json",
                "{\"source\":\"public-discourse-council-uptake\"}",
            )
            synthesis_id = synthesis_payload["canonical_ids"][0]

            finding_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "finding",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "optional-analysis",
                "--target-id",
                summary_id,
            )
            bundle_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "evidence-bundle",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "finding",
                "--target-id",
                finding_id,
            )
            challenge_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "challenge",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            readiness_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "readiness-opinion",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--status",
                "blocked",
            )
            synthesis_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "round-synthesis",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual(finding_id, finding_query["objects"][0]["finding_id"])
            self.assertEqual(bundle_id, bundle_query["objects"][0]["bundle_id"])
            self.assertEqual(challenge_id, challenge_query["objects"][0]["ticket_id"])
            self.assertEqual(readiness_id, readiness_query["objects"][0]["opinion_id"])
            self.assertEqual(synthesis_id, synthesis_query["objects"][0]["object_id"])
            self.assertIn(summary_id, finding_query["objects"][0]["basis_object_ids"])
            self.assertIn(summary_ref, finding_query["objects"][0]["evidence_refs"])
            self.assertIn(finding_id, bundle_query["objects"][0]["finding_ids"])
            self.assertIn(summary_ref, bundle_query["objects"][0]["evidence_refs"])
            self.assertIn(summary_ref, challenge_query["objects"][0]["evidence_refs"])
            self.assertIn(bundle_id, challenge_query["objects"][0]["evidence_bundle_ids"])
            self.assertIn(challenge_id, readiness_query["objects"][0]["basis_object_ids"])
            self.assertIn(summary_ref, readiness_query["objects"][0]["evidence_refs"])
            self.assertIn(challenge_id, synthesis_query["objects"][0]["unresolved_object_refs"])
            self.assertIn(summary_ref, synthesis_query["objects"][0]["evidence_refs"])

    def test_coverage_audit_records_false_zero_attempt_as_source_limit_not_absence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            submit_source_attempt(
                run_dir,
                source_skill="fetch-bluesky-cascade",
                status="executed",
                rationale="Bluesky route returned no normalized rows for this query.",
            )

            coverage_payload = run_script(
                script_path("audit-public-discourse-sample-coverage"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            coverage_artifact = load_json(
                analytics_path(run_dir, f"public_discourse_coverage_audit_{ROUND_ID}.json")
            )

            self.assertEqual("completed", coverage_payload["status"])
            attempts = coverage_artifact["source_acquisition_attempt_audit"]
            self.assertEqual(1, len(attempts))
            self.assertEqual("fetch-bluesky-cascade", attempts[0]["source_skill"])
            self.assertEqual("zero-result", attempts[0]["attempt_kind"])
            self.assertIn("not proof", attempts[0]["interpretation_boundary"])
            source_limit_text = " ".join(
                item["rationale"]
                for item in coverage_artifact["source_limit_records"]
            )
            self.assertIn("zero normalized rows", source_limit_text)
            self.assertIn("not evidence absence", source_limit_text)
            bluesky_audit = {
                item["source_family"]: item
                for item in coverage_artifact["source_family_audit"]
            }["bluesky-public-discourse"]
            self.assertEqual(
                "zero-result",
                bluesky_audit["acquisition_attempts"][0]["attempt_kind"],
            )


if __name__ == "__main__":
    unittest.main()

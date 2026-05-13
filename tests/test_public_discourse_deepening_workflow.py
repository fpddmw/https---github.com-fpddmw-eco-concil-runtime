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


class PublicDiscourseDeepeningWorkflowTests(unittest.TestCase):
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
            self.assertEqual(
                "DB-visible normalized public/formal text sample only",
                corpus_artifact["sample_definition"]["sample_boundary"],
            )
            lane_counts = {
                item["discourse_lane"]: item["signal_count"]
                for item in corpus_artifact["discourse_lane_counts"]
            }
            self.assertEqual(1, lane_counts["social_sample_affect"])
            self.assertEqual(1, lane_counts["gdelt_media_tone"])
            self.assertTrue(
                any(warning["code"] == "gdelt-tone-boundary" for warning in corpus_artifact["warnings"])
            )
            first_item = corpus_artifact["corpus_items"][0]
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
            self.assertEqual(4, coverage_payload["summary"]["coverage_cue_count"])
            self.assertEqual(4, len(coverage_artifact["coverage_cues"]))
            self.assertEqual(
                4,
                coverage_artifact["observed_inputs"]["corpus_item_count"],
            )
            self.assertFalse(
                any(warning["code"] == "no-social-sample-affect-basis" for warning in coverage_artifact["warnings"])
            )
            first_cue = coverage_artifact["coverage_cues"][0]
            self.assertEqual("requires-human-review", first_cue["audit_status"])
            self.assertNotIn("severity", first_cue)
            self.assertNotIn("coverage_score", first_cue)

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
            self.assertEqual(3, aggregation_payload["summary"]["annotation_count"])
            self.assertEqual(1, len(aggregation_artifact["social_affect_distribution"]))
            self.assertEqual("concern", aggregation_artifact["social_affect_distribution"][0]["label"])
            self.assertEqual(1.0, aggregation_artifact["social_affect_distribution"][0]["sample_fraction"])
            self.assertEqual(
                "annotation-basis://fixture/public-discourse-v1",
                aggregation_artifact["social_affect_distribution"][0]["provenance"]["annotation_basis_refs"][0],
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
                [{"average_value": -3.5, "max_value": -3.5, "metric": "v2_tone", "min_value": -3.5, "numeric_count": 1, "tone_boundary": "gdelt_media_tone_not_public_response_sentiment"}],
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
            self.assertEqual(
                comparison_artifact["gdelt_media_tone_summary"],
                summary_artifact["gdelt_media_tone_summary"],
            )
            self.assertEqual(
                "advisory comparison of sample lanes only",
                summary_artifact["cross_source_comparison"]["comparison_scope"],
            )
            self.assertTrue(summary_artifact["example_refs"])
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


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import load_json, reporting_path, run_script, runtime_src_path, script_path, write_json

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-report-chain-upgrade"
ROUND_ID = "round-report-chain-upgrade"


def insert_signal(
    run_dir: Path,
    *,
    signal_id: str,
    plane: str,
    source_skill: str,
    timestamp: str = "2023-06-07T12:00:00Z",
) -> None:
    from eco_council_runtime.kernel.planes.signal import INSERT_SQL, ensure_signal_plane_schema

    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "signal_id": signal_id,
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "plane": plane,
        "batch_id": f"batch-{signal_id}",
        "source_skill": source_skill,
        "signal_kind": f"{plane}-signal",
        "canonical_object_kind": f"{plane}-signal",
        "external_id": signal_id,
        "dedupe_key": signal_id,
        "title": f"{plane} fixture",
        "body_text": "Fixture text for report-chain upgrade tests.",
        "url": f"https://example.test/{signal_id}",
        "author_name": "",
        "channel_name": "",
        "language": "en",
        "query_text": "smoke policy concern",
        "metric": "",
        "numeric_value": None,
        "unit": "",
        "published_at_utc": timestamp if plane in {"public", "formal"} else "",
        "observed_at_utc": timestamp if plane == "environment" else "",
        "window_start_utc": "",
        "window_end_utc": "",
        "captured_at_utc": "",
        "latitude": None,
        "longitude": None,
        "bbox_json": "{}",
        "quality_flags_json": "[]",
        "engagement_json": "{}",
        "metadata_json": "{}",
        "raw_json": "{}",
        "artifact_path": str(run_dir / "raw" / f"{signal_id}.json"),
        "record_locator": "$",
        "artifact_sha256": "",
    }
    with sqlite3.connect(db_path) as connection:
        ensure_signal_plane_schema(connection)
        connection.execute(INSERT_SQL, row)
        connection.commit()


def materialize_blueprint(run_dir: Path) -> dict[str, Any]:
    payload = run_script(
        script_path("materialize-report-blueprint"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--mission-text",
        "Analyze NYC wildfire smoke air quality, official advisories, public discourse, and policy response boundaries.",
    )
    return load_json(Path(payload["summary"]["output_path"]))


def minimal_draft() -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    for section_id in (
        "executive-summary",
        "key-points",
        "what-happened",
        "evidence-basis",
        "council-reasoning",
        "limitations",
        "decision-implications",
        "audit-trail",
    ):
        sections.append(
            {
                "section_id": section_id,
                "title": section_id.replace("-", " ").title(),
                "status": "draft",
                "paragraphs": ["Bounded fixture paragraph."],
                "evidence_refs": ["signal:fixture"],
            }
        )
    return {
        "schema_version": "narrative-report-draft-v1",
        "draft_id": "draft-report-chain-upgrade",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "basis_round_id": ROUND_ID,
        "title": "Report Chain Upgrade Fixture",
        "claim_boundary": {
            "summary": "Claims stay within section briefs, sufficiency review, and frozen basis.",
            "forbidden_claims": ["unsupported policy effectiveness"],
        },
        "sections": sections,
        "reader_guidance": {"primary_audience": "test"},
        "evidence_refs": ["signal:fixture"],
        "audit_refs": ["signal:fixture"],
        "source_material": {"reporting_artifacts": [], "council_object_counts": {}},
    }


def validate_draft(run_dir: Path, draft: dict[str, Any]) -> set[str]:
    draft_path = reporting_path(run_dir, f"narrative_report_draft_{ROUND_ID}.json")
    write_json(draft_path, draft)
    payload = run_script(
        script_path("validate-narrative-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--draft-path",
        str(draft_path),
    )
    validation = load_json(Path(payload["summary"]["output_path"]))
    return {
        str(item.get("code"))
        for item in validation.get("issues", [])
        if isinstance(item, dict)
    }


class ReportChainUpgradeTests(unittest.TestCase):
    def test_report_blueprint_frames_claim_slots_without_source_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            artifact = materialize_blueprint(run_dir)

            claim_slots = artifact["claim_slots"]
            themes = artifact["investigation_themes"]
            self.assertGreaterEqual(len(claim_slots), 4)
            self.assertTrue(all(slot["status"] == "open-question-not-conclusion" for slot in claim_slots))
            self.assertFalse(any("source_skill" in json.dumps(slot) for slot in claim_slots))
            self.assertFalse(any(theme["theme_id"] == "policy_evaluation_basis" for theme in themes))
            self.assertTrue(all("source_selection_policy" in theme for theme in themes))
            self.assertTrue(artifact["synthesis_targets"][0]["not_acquisition_theme"])

    def test_acquisition_checkpoints_only_emit_when_claim_impact_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            insert_signal(run_dir, signal_id="sig-env", plane="environment", source_skill="fetch-airnow-observations")
            insert_signal(run_dir, signal_id="sig-formal", plane="formal", source_skill="fetch-federal-register-documents")
            insert_signal(run_dir, signal_id="sig-public", plane="public", source_skill="fetch-youtube-comments")
            write_json(
                run_dir / "analytics" / f"fact_policy_public_interaction_timeline_{ROUND_ID}.json",
                {
                    "interaction_nodes": [
                        {
                            "node_id": "node-supported",
                            "fact_or_policy_evidence_refs": ["signal:sig-formal"],
                            "public_or_media_evidence_refs": ["signal:sig-public"],
                        }
                    ],
                    "lane_episode_cards": [],
                },
            )

            complete = run_script(
                script_path("materialize-acquisition-checkpoints"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            complete_artifact = load_json(Path(complete["summary"]["output_path"]))
            self.assertEqual(0, complete_artifact["checkpoint_count"])

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            impacted = run_script(
                script_path("materialize-acquisition-checkpoints"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            impacted_artifact = load_json(Path(impacted["summary"]["output_path"]))
            self.assertGreater(impacted_artifact["checkpoint_count"], 0)
            self.assertTrue(impacted_artifact["checkpoint_policy"]["not_per_tool_call_form"])

    def test_sufficiency_review_agent_brief_and_handoff_are_connected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            review = run_script(
                script_path("review-theme-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            review_artifact = load_json(Path(review["summary"]["output_path"]))
            self.assertTrue(review_artifact["unsupported_claim_slots"])
            self.assertTrue(review_artifact["required_downgrades"])

            brief = run_script(
                script_path("draft-agent-section-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "social-investigator",
                "--sufficiency-review-path",
                str(review["summary"]["output_path"]),
            )
            self.assertEqual("insufficient-basis-downgrade-required", brief["summary"]["claim_strength"])

            handoff = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_artifact = load_json(Path(handoff["summary"]["output_path"]))
            self.assertEqual(1, handoff_artifact["agent_section_brief_count"])
            self.assertEqual(4, handoff_artifact["theme_sufficiency_review_count"])
            self.assertEqual(
                "social-investigator",
                handoff_artifact["report_packet"]["section_briefs"][0]["agent_role"],
            )

    def test_sufficiency_review_does_not_support_interaction_from_fact_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            insert_signal(run_dir, signal_id="sig-env-only", plane="environment", source_skill="fetch-airnow-observations")
            review = run_script(
                script_path("review-theme-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            review_artifact = load_json(Path(review["summary"]["output_path"]))
            interaction_review = next(
                item
                for item in review_artifact["theme_sufficiency_reviews"]
                if item["theme_id"] == "theme-interaction-timeline"
            )
            self.assertFalse(interaction_review["supported_claim_slots"])
            self.assertIn("claim-slot-interaction-timeline", interaction_review["unsupported_claim_slots"])

    def test_theme_acquisition_plan_requires_investigator_authorship(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            common_args = (
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--rationale",
                "Investigator adopts a bounded public semantic acquisition route.",
                "--theme-id",
                "theme-public-semantic-perception",
                "--authoring-mode",
                "agent-authored",
                "--sample-unit",
                "public-posts",
                "--downgrade-boundary",
                "examples-only",
                "--claim-slot-supported",
                "claim-slot-public-semantics",
                "--evidence-obligation",
                "Materialize enough item-level text evidence to support only sample-local public semantic claims.",
                "--success-criterion",
                "The investigator can cite item-level refs, sample boundary, and annotation/aggregation basis.",
                "--denominator-obligation",
                "source-family-count",
                "--failure-recovery-plan",
                "downgrade-to-example-only",
                "--forbidden-precommitment",
                "Do not preselect sources, source skills, query variants, or route rankings in the plan.",
                "--payload-json",
                '{"time_window":{"start":"2023-06-01","end":"2023-06-10"}}',
            )
            with self.assertRaises(AssertionError):
                run_script(
                    script_path("submit-theme-acquisition-plan"),
                    *common_args,
                    "--author-role",
                    "moderator",
                )
            payload = run_script(
                script_path("submit-theme-acquisition-plan"),
                *common_args,
                "--author-role",
                "social-investigator",
            )
            self.assertEqual("completed", payload["status"])
            with self.assertRaises(AssertionError):
                run_script(
                    script_path("submit-theme-acquisition-plan"),
                    *common_args,
                    "--author-role",
                    "social-investigator",
                    "--source-family-candidate",
                    "youtube-public-discourse",
                )

    def test_validator_blocks_policy_lane_misuse_and_missing_interaction_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = ["The policy was effective and improved the smoke response."]
            draft["source_material"] = {
                "theme_sufficiency_reviews": [{"review_id": "rev-policy"}],
                "policy_lane": {"official_action_or_governance_record_basis_visible": False},
                "theme_acquisition_plans": [
                    {
                        "object_kind": "theme-acquisition-plan",
                        "theme_id": "policy_evaluation_basis",
                        "source_family_candidates": ["policy_evaluation_basis"],
                    }
                ],
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("policy-evaluation-basis-as-acquisition-lane", codes)
            self.assertIn("policy-lane-absence-claim-downgrade-required", codes)

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "The same-day interaction is reported as co-visible descriptive chronology, not causality."
            ]
            draft["source_material"] = {
                "section_briefs": [
                    {
                        "brief_id": "section-brief-fpp",
                        "section_key": "fact-policy-public-interaction-timeline",
                        "denominator": {"interaction_node_count": 1, "lane_episode_card_count": 2},
                    }
                ],
                "interaction_timeline": {
                    "section_brief_count": 1,
                    "interaction_node_count": 1,
                    "lane_episode_card_count": 2,
                },
                "interaction_timeline_nodes": [
                    {
                        "node_id": "node-missing-summary",
                        "fact_or_policy_evidence_refs": ["signal:formal"],
                        "public_or_media_evidence_refs": ["signal:public"],
                    }
                ],
                "lane_episode_cards": [{"episode_id": "fact"}, {"episode_id": "public"}],
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("interaction-node-summary-missing", codes)

    def test_validator_requires_public_semantic_source_family_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            summary_path = run_dir / "analytics" / f"public_discourse_sample_summary_{ROUND_ID}.json"
            write_json(
                summary_path,
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "corpus_id": "corpus-fixture",
                    "aggregation_id": "aggregation-fixture",
                    "coverage_audit_summary": {"coverage_audit_id": "coverage-fixture"},
                    "sample_definition": {"sample_count": 2},
                    "source_family_counts": [],
                    "discourse_lane_counts": [{"discourse_lane": "public", "signal_count": 2}],
                    "warnings": [],
                    "evidence_refs": ["signal:public-1"],
                    "distribution_use_policy": {
                        "label_sets_are_non_exclusive": True,
                        "sample_fractions_are_sample_local": True,
                        "do_not_sum_to_population_opinion": True,
                        "requires_council_uptake_before_reporting": True,
                    },
                    "issue_distribution": [
                        {
                            "label": "health concern",
                            "sample_fraction": 0.5,
                            "label_family_denominator": 2,
                            "annotated_signal_count": 2,
                        }
                    ],
                },
            )
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "Within the sample, the public discourse issue distribution was 50% health concern and not representative."
            ]
            draft["source_material"] = {
                "theme_sufficiency_reviews": [{"review_id": "rev-public"}],
                "public_discourse_summary": {"path": str(summary_path), "summary_id": "summary-fixture", "status": "completed"},
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("public-semantic-source-family-denominator-missing", codes)


if __name__ == "__main__":
    unittest.main()

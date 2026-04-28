from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-wp4-helper-guardrails"
ROUND_ID = "round-wp4-helper-guardrails"
REMOVED_LEGACY_SKILLS = [
    "extract-observation-candidates",
    "merge-observation-candidates",
    "derive-observation-scope",
    "extract-claim-candidates",
    "cluster-claim-candidates",
    "derive-claim-scope",
    "classify-claim-verifiability",
    "route-verification-lane",
    "extract-issue-candidates",
    "cluster-issue-candidates",
    "extract-stance-candidates",
    "extract-concern-facets",
    "extract-actor-profiles",
    "extract-evidence-citation-types",
    "materialize-controversy-map",
    "link-formal-comments-to-public-discourse",
    "identify-representation-gaps",
    "detect-cross-platform-diffusion",
    "link-claims-to-observations",
    "score-evidence-coverage",
]


def insert_signal(
    run_dir: Path,
    *,
    signal_id: str,
    run_id: str = RUN_ID,
    round_id: str = ROUND_ID,
    plane: str,
    source_skill: str,
    title: str,
    body_text: str,
    author_name: str = "",
    metric: str = "",
    numeric_value: float | None = None,
    unit: str = "",
    published_at_utc: str = "",
    observed_at_utc: str = "",
    latitude: float | None = None,
    longitude: float | None = None,
    metadata: dict[str, object] | None = None,
) -> None:
    from eco_council_runtime.kernel.signal_plane_normalizer import (
        INSERT_SQL,
        ensure_signal_plane_schema,
    )

    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path = str(run_dir / "raw" / f"{signal_id}.json")
    row = {
        "signal_id": signal_id,
        "run_id": run_id,
        "round_id": round_id,
        "plane": plane,
        "batch_id": f"batch-{signal_id}",
        "source_skill": source_skill,
        "signal_kind": f"{plane}-signal",
        "canonical_object_kind": f"{plane}-signal",
        "external_id": signal_id,
        "dedupe_key": signal_id,
        "title": title,
        "body_text": body_text,
        "url": "",
        "author_name": author_name,
        "channel_name": "",
        "language": "en",
        "query_text": "",
        "metric": metric,
        "numeric_value": numeric_value,
        "unit": unit,
        "published_at_utc": published_at_utc,
        "observed_at_utc": observed_at_utc,
        "window_start_utc": "",
        "window_end_utc": "",
        "captured_at_utc": "",
        "latitude": latitude,
        "longitude": longitude,
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


class WP4HelperGuardrailTests(unittest.TestCase):
    def test_optional_analysis_registry_entries_have_wp4_freeze_metadata(self) -> None:
        from eco_council_runtime.kernel.skill_registry import (
            SKILL_LAYER_OPTIONAL_ANALYSIS,
            WP4_ALLOWED_HELPER_DECISION_SOURCES,
            skill_registry_snapshot,
        )

        snapshot = skill_registry_snapshot()
        optional_skills = [
            skill
            for skill in snapshot["skills"]
            if skill["skill_layer"] == SKILL_LAYER_OPTIONAL_ANALYSIS
        ]

        self.assertGreaterEqual(len(optional_skills), 17)
        for skill in optional_skills:
            with self.subTest(skill=skill["skill_name"]):
                self.assertTrue(skill["requires_operator_approval"])
                metadata = skill.get("wp4_helper_metadata", {})
                self.assertTrue(metadata.get("rule_id"))
                self.assertEqual(
                    "wp4-freeze-line-2026-04-28",
                    metadata.get("rule_version"),
                )
                self.assertIn(
                    metadata.get("decision_source"),
                    WP4_ALLOWED_HELPER_DECISION_SOURCES,
                )
                self.assertIn("approval-required", metadata.get("audit_status", ""))
                self.assertTrue(metadata.get("wp4_destination"))

    def test_removed_skill_entries_are_not_registered_or_executable(self) -> None:
        from eco_council_runtime.kernel.skill_registry import skill_registry_snapshot

        snapshot = skill_registry_snapshot()
        registered = {skill["skill_name"] for skill in snapshot["skills"]}
        for skill_name in REMOVED_LEGACY_SKILLS:
            with self.subTest(skill=skill_name):
                self.assertNotIn(skill_name, registered)
                self.assertFalse((script_path(skill_name)).exists())

    def test_review_evidence_sufficiency_reads_db_objects_without_gate_scores(self) -> None:
        from eco_council_runtime.council_objects import (
            append_evidence_bundle_record,
            append_finding_record,
            append_review_comment_record,
        )
        from eco_council_runtime.reporting_objects import store_report_section_draft_record

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            append_finding_record(
                run_dir,
                finding_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "finding_kind": "environmental-evidence",
                    "agent_role": "environmental-investigator",
                    "title": "Observed smoke exposure evidence",
                    "summary": "Observed evidence exists, but spatial uncertainty remains limited.",
                    "rationale": "The source record is relevant to the study period.",
                    "target_kind": "round",
                    "target_id": ROUND_ID,
                    "evidence_refs": ["evidence://finding-smoke-001"],
                    "source_signal_ids": ["signal-smoke-001"],
                    "provenance": {"source": "unit-test"},
                },
            )
            append_evidence_bundle_record(
                run_dir,
                bundle_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "bundle_kind": "environmental-evidence-bundle",
                    "agent_role": "environmental-investigator",
                    "title": "Smoke evidence bundle",
                    "summary": "Collected the DB-backed smoke evidence basis.",
                    "rationale": "Bundle connects finding and source signal.",
                    "target_kind": "round",
                    "target_id": ROUND_ID,
                    "finding_ids": ["finding-smoke-001"],
                    "evidence_refs": ["evidence://bundle-smoke-001"],
                    "provenance": {"source": "unit-test"},
                },
            )
            append_review_comment_record(
                run_dir,
                comment_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "author_role": "challenger",
                    "review_kind": "sufficiency-review",
                    "status": "open",
                    "comment_text": "Missing source coverage may affect report usage.",
                    "target_kind": "round",
                    "target_id": ROUND_ID,
                    "evidence_refs": ["evidence://challenge-001"],
                    "provenance": {"source": "unit-test"},
                },
            )
            store_report_section_draft_record(
                run_dir,
                section_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "report_id": "report-001",
                    "agent_role": "report-editor",
                    "status": "draft",
                    "section_key": "evidence",
                    "section_title": "Evidence Basis",
                    "section_text": "Draft cites the finding and bundle basis.",
                    "basis_object_ids": ["finding-smoke-001"],
                    "bundle_ids": ["bundle-smoke-001"],
                    "finding_ids": ["finding-smoke-001"],
                    "evidence_refs": ["evidence://report-section-001"],
                    "provenance": {"source": "unit-test"},
                },
            )

            payload = run_script(
                script_path("review-evidence-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("approved-helper-view", payload["summary"]["decision_source"])
            self.assertEqual("HEUR-SUFFICIENCY-REVIEW-001", payload["summary"]["rule_id"])
            self.assertEqual([], payload["board_handoff"]["suggested_next_skills"])
            self.assertEqual(
                "contested-review",
                payload["summary"]["review_posture"],
            )
            review = payload["review"]
            self.assertFalse(review["rubric"]["numeric_scores"])
            self.assertFalse(review["rubric"]["phase_gate"])
            self.assertEqual(5, len(review["notes"]))
            self.assertTrue(
                all(note.get("evidence_refs") is not None for note in review["notes"])
            )
            artifact = load_json(
                run_dir / "analytics" / f"evidence_sufficiency_review_{ROUND_ID}.json"
            )
            artifact_text = json.dumps(artifact, ensure_ascii=True, sort_keys=True)
            self.assertNotIn('"coverage_score"', artifact_text)
            self.assertNotIn('"readiness"', artifact_text)
            self.assertIn("report_usage_constraints", artifact_text)

    def test_aggregate_environment_evidence_is_descriptive_not_matching(self) -> None:
        from _workflow_support import seed_signal_plane

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            root = Path(tmpdir) / "fixtures"
            seed_signal_plane(
                run_dir,
                root,
                RUN_ID,
                ROUND_ID,
                include_airnow=True,
                include_openmeteo=True,
            )

            payload = run_script(
                script_path("aggregate-environment-evidence"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual([], payload["board_handoff"]["suggested_next_skills"])
            aggregation = payload["aggregation"]
            self.assertGreater(aggregation["statistics_summary"]["signal_count"], 0)
            self.assertTrue(aggregation["evidence_refs"])
            artifact = load_json(
                run_dir / "analytics" / f"environment_evidence_aggregation_{ROUND_ID}.json"
            )
            artifact_text = json.dumps(artifact, ensure_ascii=True, sort_keys=True)
            self.assertNotIn("usable_for_matching", artifact_text)
            self.assertNotIn("link-claims-to-observations", artifact_text)
            self.assertNotIn("score-evidence-coverage", artifact_text)

    def test_formal_public_footprints_do_not_emit_alignment_scores(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            insert_signal(
                run_dir,
                signal_id="public-signal-001",
                plane="public",
                source_skill="normalize-youtube-video-public-signals",
                title="Community concern about flood rule",
                body_text="Residents discuss flood risk and agency permitting.",
                author_name="Community Channel",
                published_at_utc="2024-01-02T10:00:00Z",
            )
            insert_signal(
                run_dir,
                signal_id="formal-signal-001",
                plane="formal",
                source_skill="normalize-regulationsgov-comments-public-signals",
                title="Agency docket comment on flood rule",
                body_text="Formal comment discusses flood risk and permit requirements.",
                author_name="Formal Submitter",
                published_at_utc="2024-01-02T11:00:00Z",
                metadata={"submitter_name": "Formal Submitter", "issue_terms": ["flood", "permit"]},
            )

            payload = run_script(
                script_path("compare-formal-public-footprints"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("completed", payload["status"])
            footprints = payload["formal_public_footprints"]
            self.assertIn("overlap_terms", footprints)
            artifact = load_json(run_dir / "analytics" / f"formal_public_footprints_{ROUND_ID}.json")
            artifact_text = json.dumps(artifact, ensure_ascii=True, sort_keys=True)
            self.assertNotIn('"aligned"', artifact_text)
            self.assertNotIn('"alignment_score"', artifact_text)
            self.assertNotIn('"formal_public_links"', artifact_text)

    def test_temporal_cooccurrence_requires_real_timestamps_without_1970_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            insert_signal(
                run_dir,
                signal_id="public-no-time-001",
                plane="public",
                source_skill="normalize-youtube-video-public-signals",
                title="Public comment without timestamp",
                body_text="Public discussion lacks a usable timestamp.",
            )
            insert_signal(
                run_dir,
                signal_id="formal-no-time-001",
                plane="formal",
                source_skill="normalize-regulationsgov-comments-public-signals",
                title="Formal comment without timestamp",
                body_text="Formal record lacks a usable timestamp.",
            )

            payload = run_script(
                script_path("detect-temporal-cooccurrence-cues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            self.assertEqual("insufficient-temporal-basis", payload["status"])
            self.assertEqual(2, payload["summary"]["missing_timestamp_count"])
            self.assertEqual([], payload["temporal_cooccurrence_cues"])
            self.assertEqual([], payload["board_handoff"]["suggested_next_skills"])
            artifact = load_json(run_dir / "analytics" / f"temporal_cooccurrence_cues_{ROUND_ID}.json")
            artifact_text = json.dumps(artifact, ensure_ascii=True, sort_keys=True)
            self.assertIn("insufficient-temporal-basis", artifact_text)
            self.assertNotIn("1970", artifact_text)


if __name__ == "__main__":
    unittest.main()

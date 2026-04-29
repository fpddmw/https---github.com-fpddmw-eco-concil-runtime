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

RUN_ID = "run-optional-analysis-guardrails"
ROUND_ID = "round-optional-analysis-guardrails"
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


class OptionalAnalysisGuardrailTests(unittest.TestCase):
    def test_optional_analysis_registry_entries_have_freeze_metadata(self) -> None:
        from eco_council_runtime.kernel.skill_registry import (
            SKILL_LAYER_OPTIONAL_ANALYSIS,
            OPTIONAL_HELPER_ALLOWED_DECISION_SOURCES,
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
                metadata = skill.get("helper_governance", {})
                self.assertTrue(metadata.get("rule_id"))
                self.assertEqual(
                    "optional-analysis-freeze-line-2026-04-28",
                    metadata.get("rule_version"),
                )
                self.assertIn(
                    metadata.get("decision_source"),
                    OPTIONAL_HELPER_ALLOWED_DECISION_SOURCES,
                )
                self.assertIn("approval-required", metadata.get("audit_status", ""))
                self.assertTrue(metadata.get("helper_destination"))

    def test_analysis_kind_governance_freezes_legacy_report_basis_paths(self) -> None:
        from eco_council_runtime.kernel.analysis_plane import (
            ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
            analysis_kind_governance,
            query_analysis_result_sets,
        )

        legacy_kinds = {
            "evidence-coverage": "review-evidence-sufficiency",
            "claim-observation-link": "review-fact-check-evidence-scope",
            "observation-candidate": "aggregate-environment-evidence",
            "merged-observation": "aggregate-environment-evidence",
            "formal-public-link": "compare-formal-public-footprints",
            "representation-gap": "identify-representation-audit-cues",
            "diffusion-edge": "detect-temporal-cooccurrence-cues",
        }
        for analysis_kind, successor_skill in legacy_kinds.items():
            with self.subTest(analysis_kind=analysis_kind):
                governance = analysis_kind_governance(analysis_kind)
                self.assertEqual(
                    ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
                    governance["governance_status"],
                )
                self.assertEqual(successor_skill, governance["successor_skill"])
                self.assertFalse(governance["default_chain_eligible"])
                self.assertFalse(governance["phase_gate_eligible"])
                self.assertFalse(governance["report_basis_eligible"])
                self.assertTrue(governance["requires_explicit_approval"])
                self.assertIn("finding-record", governance["report_use_requires"])

        with tempfile.TemporaryDirectory() as tmpdir:
            payload = query_analysis_result_sets(
                Path(tmpdir) / "run",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                analysis_kind="evidence-coverage",
                latest_only=True,
            )
            governance = payload["analysis_kind_governance"]
            self.assertEqual(
                ANALYSIS_GOVERNANCE_LEGACY_FROZEN,
                governance["governance_status"],
            )
            self.assertFalse(governance["report_basis_eligible"])
            self.assertEqual(0, payload["summary"]["matching_result_set_count"])

    def test_formal_signal_taxonomy_records_are_frozen_and_candidate_only(self) -> None:
        from eco_council_runtime.formal_signal_semantics import (
            FORMAL_PUBLIC_TAXONOMY_AUDIT_STATUS,
            FORMAL_PUBLIC_TAXONOMY_VERSION,
            build_formal_signal_semantics,
            formal_signal_semantics_taxonomy_metadata,
        )
        from eco_council_runtime.kernel.skill_registry import resolve_skill_policy

        metadata = formal_signal_semantics_taxonomy_metadata()
        self.assertEqual(FORMAL_PUBLIC_TAXONOMY_VERSION, metadata["taxonomy_version"])
        self.assertEqual(FORMAL_PUBLIC_TAXONOMY_AUDIT_STATUS, metadata["audit_status"])
        self.assertFalse(metadata["default_chain_eligible"])
        self.assertFalse(metadata["phase_gate_eligible"])
        self.assertFalse(metadata["report_basis_eligible"])
        family_ids = {
            family["taxonomy_family_id"]
            for family in metadata["families"]
        }
        self.assertIn("formal-public-issue-labels", family_ids)
        self.assertIn("formal-public-route-hints", family_ids)
        self.assertTrue(
            all(
                family["audit_status"] == FORMAL_PUBLIC_TAXONOMY_AUDIT_STATUS
                for family in metadata["families"]
            )
        )

        semantics = build_formal_signal_semantics(
            title="Permit comment requests health study",
            body_text="The agency should extend the comment period and review asthma evidence.",
            author_name="Fixture Community Coalition",
            attributes={"submitterType": "community"},
        )
        self.assertEqual("heuristic-fallback", semantics["decision_source"])
        self.assertEqual("candidate-labels-only", semantics["typing_status"])
        self.assertEqual(FORMAL_PUBLIC_TAXONOMY_VERSION, semantics["taxonomy_version"])
        self.assertEqual(FORMAL_PUBLIC_TAXONOMY_AUDIT_STATUS, semantics["taxonomy_status"])
        self.assertFalse(semantics["report_basis_eligible"])
        self.assertFalse(semantics["phase_gate_eligible"])
        self.assertGreaterEqual(len(semantics["taxonomy_family_records"]), 6)

        taxonomy_policy = resolve_skill_policy("apply-approved-formal-public-taxonomy")
        helper_metadata = taxonomy_policy["helper_governance"]
        self.assertEqual(
            FORMAL_PUBLIC_TAXONOMY_VERSION,
            helper_metadata["taxonomy_version"],
        )
        self.assertIn("approval-required", helper_metadata["audit_status"])

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

    def test_research_issue_helpers_ignore_unapproved_input_artifacts_without_db_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            raw_input = root / "unapproved_issue_hints.json"
            raw_input.write_text(
                json.dumps(
                    {
                        "discourse_issue_hints": [
                            {
                                "hint_id": "raw-hint-001",
                                "hint_label": "Raw artifact issue",
                                "text_evidence_snippets": ["This should not become a research issue."],
                                "evidence_refs": ["artifact://raw"],
                            }
                        ]
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            surface_payload = run_script(
                script_path("materialize-research-issue-surface"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--input-path",
                str(raw_input),
            )
            views_payload = run_script(
                script_path("project-research-issue-views"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--input-path",
                str(raw_input),
            )

            surface_artifact = load_json(
                run_dir / "analytics" / f"research_issue_surface_{ROUND_ID}.json"
            )
            views_artifact = load_json(
                run_dir / "analytics" / f"research_issue_views_{ROUND_ID}.json"
            )

            self.assertEqual("completed", surface_payload["status"])
            self.assertEqual(0, surface_payload["summary"]["issue_count"])
            self.assertEqual([], surface_artifact["research_issues"])
            self.assertTrue(
                any(warning["code"] == "unapproved-input-artifact" for warning in surface_payload["warnings"])
            )
            self.assertEqual("completed", views_payload["status"])
            self.assertEqual(0, views_payload["summary"]["view_count"])
            self.assertEqual([], views_artifact["issue_views"])
            self.assertTrue(
                any(warning["code"] == "unapproved-input-artifact" for warning in views_payload["warnings"])
            )


if __name__ == "__main__":
    unittest.main()

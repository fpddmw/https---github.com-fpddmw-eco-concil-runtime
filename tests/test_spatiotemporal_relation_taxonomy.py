from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    run_kernel,
    run_script,
    runtime_src_path,
    script_path,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.analysis_objects import (  # noqa: E402
    normalize_spatiotemporal_relation_cue_payload,
)
from eco_council_runtime.canonical_contracts import (  # noqa: E402
    ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
    PLANE_ANALYSIS,
    canonical_contract,
    canonical_contract_kinds,
    validate_canonical_payload,
)
from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    sync_spatiotemporal_relation_cue_result_set,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_moderator_action_records,
)
from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    base_signal,
    connect_db,
    insert_signals,
)
from eco_council_runtime.kernel.skill_registry import resolve_skill_policy  # noqa: E402

RUN_ID = "run-spatiotemporal-taxonomy-001"
ROUND_ID = "round-spatiotemporal-taxonomy-001"


def relation_payload() -> dict[str, object]:
    return {
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "relation_id": "relation-cue-001",
        "decision_source": "approved-helper-view",
        "relation_type": "lag-window-candidate",
        "relation_status": "candidate",
        "source_signal_id": "signal-source-001",
        "target_signal_id": "signal-target-001",
        "context_signal_ids": ["signal-context-001"],
        "source_role": "source-event",
        "target_role": "receptor-observation",
        "temporal_rule": {"rule": "lag-window"},
        "spatial_rule": {"rule": "max-distance-km"},
        "lag_window": {"min_hours": 1, "max_hours": 24},
        "time_delta": {"hours": 6},
        "distance": {"kilometers": 42},
        "spatial_basis": {"method": "fixture"},
        "temporal_basis": {"method": "fixture"},
        "rejection_reasons": [],
        "caveats": ["candidate relation cue only"],
        "evidence_refs": [
            {
                "signal_id": "signal-source-001",
                "artifact_path": "/tmp/source.json",
                "record_locator": "$",
                "artifact_ref": "/tmp/source.json:$",
            }
        ],
        "lineage": ["signal-source-001", "signal-target-001"],
        "provenance": {"source": "unit-test"},
        "helper_governance": {
            "skill": "detect-temporal-cooccurrence-cues",
            "rule_id": "HEUR-SPATIOTEMPORAL-RELATION-001",
            "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
            "approval_ref": "approval://fixture",
            "audit_status": "default-frozen; approval-required; audit-pending",
        },
    }


def seed_structured_relation_signals(
    run_dir: Path,
    *,
    target_longitude: float = -74.5,
) -> None:
    artifact_dir = run_dir / "raw"
    signals = [
        base_signal(
            signal_id="signal-fire-001",
            run_id=RUN_ID,
            round_id=ROUND_ID,
            plane="environment",
            source_skill="fetch-nasa-firms-fire",
            signal_kind="fire-detection",
            external_id="fire-001",
            dedupe_key="fire-001",
            title="Fire detection",
            body_text="",
            url="",
            author_name="NASA FIRMS",
            channel_name="",
            language="",
            query_text="",
            metric="fire_detection_count",
            numeric_value=1.0,
            unit="count",
            published_at_utc="",
            observed_at_utc="2026-05-01T00:00:00Z",
            window_start_utc="",
            window_end_utc="",
            captured_at_utc="2026-05-01T00:05:00Z",
            latitude=40.0,
            longitude=-75.0,
            quality_flags=[],
            engagement={},
            metadata={},
            raw_record={},
            artifact_path=artifact_dir / "fire.json",
            record_locator="$.records[0]",
            artifact_sha256="",
        ),
        base_signal(
            signal_id="signal-pm25-001",
            run_id=RUN_ID,
            round_id=ROUND_ID,
            plane="environment",
            source_skill="fetch-openaq",
            signal_kind="hourly-observation",
            external_id="pm25-001",
            dedupe_key="pm25-001",
            title="PM2.5 observation",
            body_text="",
            url="",
            author_name="OpenAQ",
            channel_name="",
            language="",
            query_text="",
            metric="pm25",
            numeric_value=35.0,
            unit="ug/m3",
            published_at_utc="",
            observed_at_utc="2026-05-01T06:00:00Z",
            window_start_utc="",
            window_end_utc="",
            captured_at_utc="2026-05-01T06:05:00Z",
            latitude=40.2,
            longitude=target_longitude,
            quality_flags=[],
            engagement={},
            metadata={},
            raw_record={},
            artifact_path=artifact_dir / "pm25.json",
            record_locator="$.results[0]",
            artifact_sha256="",
        ),
        base_signal(
            signal_id="signal-wind-001",
            run_id=RUN_ID,
            round_id=ROUND_ID,
            plane="environment",
            source_skill="fetch-open-meteo-historical",
            signal_kind="hourly-observation",
            external_id="wind-001",
            dedupe_key="wind-001",
            title="Wind observation",
            body_text="",
            url="",
            author_name="Open-Meteo",
            channel_name="",
            language="",
            query_text="",
            metric="wind_speed_10m",
            numeric_value=4.0,
            unit="m/s",
            published_at_utc="",
            observed_at_utc="2026-05-01T03:00:00Z",
            window_start_utc="",
            window_end_utc="",
            captured_at_utc="2026-05-01T03:05:00Z",
            latitude=40.1,
            longitude=-74.8,
            quality_flags=[],
            engagement={},
            metadata={},
            raw_record={},
            artifact_path=artifact_dir / "wind.json",
            record_locator="$.records[0]",
            artifact_sha256="",
        ),
    ]
    connection, _ = connect_db(run_dir, "")
    try:
        with connection:
            insert_signals(connection, signals)
    finally:
        connection.close()


def run_structured_relation_detection(
    run_dir: Path,
    *,
    target_longitude: float = -74.5,
    max_distance_km: str = "200",
) -> dict[str, object]:
    seed_structured_relation_signals(run_dir, target_longitude=target_longitude)
    return run_script(
        script_path("detect-temporal-cooccurrence-cues"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--source-role",
        "source-event",
        "--target-role",
        "receptor-observation",
        "--source-class",
        "fire-detection",
        "--target-class",
        "air-quality",
        "--lag-min-hours",
        "1",
        "--lag-max-hours",
        "12",
        "--max-distance-km",
        max_distance_km,
        "--spatial-rule",
        "max-distance-km",
    )


class SpatiotemporalRelationTaxonomyTests(unittest.TestCase):
    def test_relation_cue_contract_is_analysis_plane_and_requires_evidence(self) -> None:
        self.assertIn(
            "spatiotemporal-relation-cue",
            canonical_contract_kinds(plane=PLANE_ANALYSIS),
        )
        contract = canonical_contract("spatiotemporal-relation-cue")
        self.assertEqual("spatiotemporal-relation-cue-v1", contract.schema_version)
        payload = validate_canonical_payload(
            "spatiotemporal-relation-cue",
            relation_payload(),
        )
        self.assertEqual("relation-cue-001", payload["relation_id"])

        missing_evidence = dict(relation_payload())
        missing_evidence["evidence_refs"] = []
        with self.assertRaises(ValueError):
            validate_canonical_payload(
                "spatiotemporal-relation-cue",
                missing_evidence,
            )

    def test_relation_normalizer_rejects_non_taxonomy_status(self) -> None:
        payload = dict(relation_payload())
        payload["relation_status"] = "proven-causal"
        with self.assertRaises(ValueError):
            normalize_spatiotemporal_relation_cue_payload(
                payload,
                source_skill="detect-temporal-cooccurrence-cues",
            )

    def test_relation_cue_artifact_can_sync_to_analysis_plane(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            relation = normalize_spatiotemporal_relation_cue_payload(
                relation_payload(),
                source_skill="detect-temporal-cooccurrence-cues",
                artifact_path=str(
                    run_dir
                    / "analytics"
                    / f"spatiotemporal_relation_cues_{ROUND_ID}.json"
                ),
            )
            artifact_path = (
                run_dir
                / "analytics"
                / f"spatiotemporal_relation_cues_{ROUND_ID}.json"
            )
            write_json(
                artifact_path,
                {
                    "schema_version": "optional-analysis-spatiotemporal-relation-cues-v1",
                    "skill": "detect-temporal-cooccurrence-cues",
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "generated_at_utc": "2026-05-02T00:00:00Z",
                    "spatiotemporal_relation_cues": [relation],
                    "relation_cue_count": 1,
                    "taxonomy_version": ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
                },
            )

            sync = sync_spatiotemporal_relation_cue_result_set(
                run_dir,
                expected_run_id=RUN_ID,
                round_id=ROUND_ID,
                relation_cues_path=artifact_path,
            )

            self.assertEqual("completed", sync["status"])
            self.assertEqual(1, sync["item_count"])
            self.assertEqual(
                "spatiotemporal-relation-cue",
                sync["analysis_kind"],
            )
            self.assertEqual(
                "detect-temporal-cooccurrence-cues",
                sync["analysis_kind_governance"]["successor_skill"],
            )

    def test_environment_role_and_class_metadata_are_indexed(self) -> None:
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
            signal_id = normalize_payload["canonical_ids"][0]

            with sqlite3.connect(run_dir / "analytics" / "signal_plane.sqlite") as connection:
                connection.row_factory = sqlite3.Row
                signal_row = connection.execute(
                    "SELECT metadata_json FROM normalized_signals WHERE signal_id = ?",
                    (signal_id,),
                ).fetchone()
                index_rows = connection.execute(
                    """
                    SELECT field_name, field_value
                    FROM normalized_signal_index
                    WHERE signal_id = ?
                    ORDER BY field_name, field_value
                    """,
                    (signal_id,),
                ).fetchall()

            self.assertIsNotNone(signal_row)
            metadata = json.loads(signal_row["metadata_json"])
            self.assertEqual("receptor-observation", metadata["signal_role"])
            self.assertEqual("air-quality", metadata["environment_signal_class"])
            self.assertEqual(
                ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
                metadata["environment_signal_taxonomy"]["taxonomy_version"],
            )
            indexed_pairs = {
                (str(row["field_name"]), str(row["field_value"])) for row in index_rows
            }
            self.assertIn(("signal_role", "receptor-observation"), indexed_pairs)
            self.assertIn(("environment_signal_class", "air-quality"), indexed_pairs)

    def test_temporal_helper_freeze_line_carries_relation_taxonomy_version(self) -> None:
        policy = resolve_skill_policy("detect-temporal-cooccurrence-cues")
        helper_governance = policy["helper_governance"]
        self.assertEqual(
            "HEUR-SPATIOTEMPORAL-RELATION-001",
            helper_governance["rule_id"],
        )
        self.assertEqual(
            ENVIRONMENT_SIGNAL_TAXONOMY_VERSION,
            helper_governance["taxonomy_version"],
        )

    def test_temporal_helper_structured_relation_mode_writes_analysis_result_set(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            seed_structured_relation_signals(run_dir)

            payload = run_script(
                script_path("detect-temporal-cooccurrence-cues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--source-role",
                "source-event",
                "--target-role",
                "receptor-observation",
                "--source-class",
                "fire-detection",
                "--target-class",
                "air-quality",
                "--lag-min-hours",
                "1",
                "--lag-max-hours",
                "12",
                "--max-distance-km",
                "200",
                "--spatial-rule",
                "max-distance-km",
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual(1, payload["summary"]["relation_cue_count"])
            relation = payload["spatiotemporal_relation_cues"][0]
            self.assertEqual("spatiotemporal-window-candidate", relation["relation_type"])
            self.assertEqual("candidate", relation["relation_status"])
            self.assertEqual(["signal-wind-001"], relation["context_signal_ids"])
            self.assertEqual(
                "completed",
                payload["analysis_sync"]["status"],
            )
            self.assertEqual(1, payload["analysis_sync"]["item_count"])
            relation_artifact = load_json(
                run_dir
                / "analytics"
                / f"spatiotemporal_relation_cues_{ROUND_ID}.json"
            )
            self.assertEqual(1, relation_artifact["relation_cue_count"])

    def test_temporal_helper_structured_relation_mode_records_spatial_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            seed_structured_relation_signals(run_dir, target_longitude=-70.0)

            payload = run_script(
                script_path("detect-temporal-cooccurrence-cues"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--source-role",
                "source-event",
                "--target-role",
                "receptor-observation",
                "--lag-min-hours",
                "1",
                "--lag-max-hours",
                "12",
                "--max-distance-km",
                "20",
                "--spatial-rule",
                "max-distance-km",
            )

            relation = payload["spatiotemporal_relation_cues"][0]
            self.assertEqual("rejected-by-spatial-rule", relation["relation_type"])
            self.assertEqual("rejected-by-rule", relation["relation_status"])
            self.assertIn("spatial-scope-overbroad", relation["rejection_reasons"])

    def test_relation_query_surface_filters_relation_cues(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])

            query_payload = run_script(
                script_path("query-spatiotemporal-relations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--relation-id",
                relation_id,
                "--relation-status",
                "candidate",
                "--latest-only",
                "--include-result-sets",
            )

            self.assertEqual(
                "spatiotemporal-relation-query-v1",
                query_payload["schema_version"],
            )
            self.assertEqual(1, query_payload["summary"]["matching_relation_count"])
            self.assertEqual(1, query_payload["summary"]["returned_relation_count"])
            self.assertEqual(relation_id, query_payload["relations"][0]["relation_id"])
            self.assertEqual("candidate", query_payload["relations"][0]["relation_status"])
            self.assertEqual(
                "detect-temporal-cooccurrence-cues",
                query_payload["analysis_kind_governance"]["successor_skill"],
            )
            self.assertTrue(query_payload["artifact_refs"])

            kernel_query = run_kernel(
                "query-spatiotemporal-relations",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--relation-id",
                relation_id,
                "--source-role",
                "source-event",
                "--target-role",
                "receptor-observation",
            )
            self.assertEqual(1, kernel_query["summary"]["matching_relation_count"])
            self.assertEqual(relation_id, kernel_query["relations"][0]["relation_id"])

    def test_relation_alternatives_challenge_and_review_comment_carry_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])

            alternatives_payload = run_script(
                script_path("review-spatiotemporal-relation-alternatives"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--relation-id",
                relation_id,
            )
            self.assertEqual("completed", alternatives_payload["status"])
            candidate = next(
                item
                for item in alternatives_payload["objection_candidates"]
                if item["objection_code"] == "report-overclaim-risk"
            )
            evidence_ref = candidate["evidence_refs"][0]["artifact_ref"]

            challenge_payload = run_script(
                script_path("open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Review spatiotemporal relation overclaim risk",
                "--challenge-statement",
                "Candidate relation cue must not be used as causality or source attribution.",
                "--priority",
                "high",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                evidence_ref,
                "--relation-id",
                relation_id,
                "--objection-code",
                candidate["objection_code"],
                "--challenged-rule",
                candidate["challenged_rule"],
                "--alternative-explanation",
                candidate["alternative_explanation"],
                "--required-followup-evidence",
                candidate["required_followup_evidence"][0],
                "--report-risk",
                candidate["report_risk"],
            )
            self.assertEqual("completed", challenge_payload["status"])

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
                "--status",
                "open",
            )
            self.assertEqual(1, challenge_query["summary"]["matching_object_count"])
            challenge = challenge_query["objects"][0]
            self.assertEqual(relation_id, challenge["relation_id"])
            self.assertEqual(candidate["objection_code"], challenge["objection_code"])
            self.assertEqual(
                "spatiotemporal-relation-cue",
                challenge["target"]["object_kind"],
            )
            self.assertEqual(relation_id, challenge["target"]["object_id"])

            review_payload = run_kernel(
                "post-review-comment",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "challenger",
                "--review-kind",
                "spatiotemporal-relation-objection",
                "--comment-text",
                "Carry relation objection before report use.",
                "--evidence-ref",
                evidence_ref,
                "--relation-id",
                relation_id,
                "--objection-code",
                candidate["objection_code"],
                "--challenged-rule",
                candidate["challenged_rule"],
                "--alternative-explanation",
                candidate["alternative_explanation"],
                "--required-followup-evidence",
                candidate["required_followup_evidence"][0],
                "--report-risk",
                candidate["report_risk"],
            )
            self.assertEqual("completed", review_payload["status"])
            review_comment = review_payload["record"]["comment"]
            self.assertEqual(relation_id, review_comment["relation_id"])
            self.assertEqual(candidate["objection_code"], review_comment["objection_code"])
            self.assertEqual("spatiotemporal-relation-cue", review_comment["target_kind"])
            self.assertEqual(relation_id, review_comment["target_id"])

            review_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "review-comment",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "spatiotemporal-relation-cue",
                "--target-id",
                relation_id,
            )
            self.assertEqual(1, review_query["summary"]["matching_object_count"])
            self.assertEqual(
                candidate["report_risk"],
                review_query["objects"][0]["report_risk"],
            )

    def test_relation_falsification_probe_carries_objection_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])
            evidence_ref = relation["evidence_refs"][0]["artifact_ref"]
            next_actions_path = run_dir / "investigation" / f"next_actions_{ROUND_ID}.json"
            next_actions_payload = {
                "schema_version": "moderator-next-actions-v1",
                "skill": "summarize-round-readiness",
                "run_id": RUN_ID,
                "round_id": ROUND_ID,
                "generated_at_utc": "2026-05-02T00:00:00Z",
                "action_source": "unit-test",
                "ranked_actions": [
                    {
                        "action_id": "action-review-relation-001",
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "action_kind": "review-spatiotemporal-relation",
                        "assigned_role": "challenger",
                        "priority": "high",
                        "objective": "Review candidate relation before report use.",
                        "reason": "Candidate relation cue is not report-ready support.",
                        "probe_candidate": True,
                        "relation_id": relation_id,
                        "objection_code": "report-overclaim-risk",
                        "challenged_rule": "spatiotemporal-window-candidate",
                        "alternative_explanation": "Candidate relation may be coincidental.",
                        "required_followup_evidence": [
                            "Confirm report language stays at candidate relation level."
                        ],
                        "report_risk": "overclaim-if-used-as-causality",
                        "target": {
                            "object_kind": "spatiotemporal-relation-cue",
                            "object_id": relation_id,
                        },
                        "evidence_refs": [evidence_ref],
                        "source_ids": [relation_id],
                        "lineage": [relation_id],
                        "decision_source": "unit-test",
                    }
                ],
            }
            write_json(next_actions_path, next_actions_payload)
            store_moderator_action_records(
                run_dir,
                action_snapshot=next_actions_payload,
                artifact_path=str(next_actions_path),
            )

            probe_payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--next-actions-path",
                str(next_actions_path),
                "--max-probes",
                "1",
            )
            self.assertEqual("completed", probe_payload["status"])
            probe_snapshot = load_json(Path(probe_payload["summary"]["output_path"]))
            probe = probe_snapshot["probes"][0]
            self.assertEqual("spatiotemporal-relation-probe", probe["probe_type"])
            self.assertEqual(relation_id, probe["relation_id"])
            self.assertEqual("report-overclaim-risk", probe["objection_code"])
            self.assertEqual(
                "spatiotemporal-relation-cue",
                probe["target"]["object_kind"],
            )
            self.assertEqual(relation_id, probe["target"]["object_id"])
            self.assertIn("query-spatiotemporal-relations", probe["requested_skills"])
            self.assertIn(
                "review-spatiotemporal-relation-alternatives",
                probe["requested_skills"],
            )

            probe_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "probe",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "spatiotemporal-relation-cue",
                "--target-id",
                relation_id,
            )
            self.assertEqual(1, probe_query["summary"]["matching_object_count"])
            self.assertEqual(
                "report-overclaim-risk",
                probe_query["objects"][0]["objection_code"],
            )

    def test_relation_evidence_packet_defaults_to_artifact_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])

            policy = resolve_skill_policy(
                "materialize-spatiotemporal-relation-evidence-packet"
            )
            self.assertEqual("reporting", policy["skill_layer"])
            self.assertTrue(policy["requires_operator_approval"])
            self.assertIn("reporting", policy["db_write_planes"])

            packet_payload = run_script(
                script_path("materialize-spatiotemporal-relation-evidence-packet"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--relation-id",
                relation_id,
            )
            self.assertEqual("completed", packet_payload["status"])
            self.assertEqual(0, packet_payload["summary"]["basis_object_write_count"])
            packet = load_json(Path(packet_payload["summary"]["output_path"]))
            self.assertEqual(
                "spatiotemporal-relation-evidence-packet-v1",
                packet["schema_version"],
            )
            self.assertEqual([relation_id], packet["accepted_relation_cue_ids"])
            self.assertFalse(packet["basis_handoff"]["write_basis_objects"])
            self.assertEqual({}, packet["basis_handoff"]["written_records"])
            self.assertIn(
                "not as causal or attribution findings",
                packet["relation_cues_summary"]["accepted_semantics"],
            )

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
                "spatiotemporal-relation-evidence-packet",
                "--target-id",
                packet["packet_id"],
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
                "spatiotemporal-relation-evidence-packet",
                "--target-id",
                packet["packet_id"],
            )
            section_query = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "report-section-draft",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual(0, finding_query["summary"]["matching_object_count"])
            self.assertEqual(0, bundle_query["summary"]["matching_object_count"])
            self.assertEqual(0, section_query["summary"]["matching_object_count"])

    def test_relation_evidence_packet_writes_mediated_basis_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            detection_payload = run_structured_relation_detection(run_dir)
            relation = detection_payload["spatiotemporal_relation_cues"][0]
            relation_id = str(relation["relation_id"])

            alternatives_payload = run_script(
                script_path("review-spatiotemporal-relation-alternatives"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--relation-id",
                relation_id,
            )
            candidate = next(
                item
                for item in alternatives_payload["objection_candidates"]
                if item["objection_code"] == "report-overclaim-risk"
            )
            evidence_ref = candidate["evidence_refs"][0]["artifact_ref"]
            challenge_payload = run_script(
                script_path("open-challenge-ticket"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--title",
                "Review spatiotemporal relation overclaim risk",
                "--challenge-statement",
                "Candidate relation cue must not be used as causality or source attribution.",
                "--priority",
                "high",
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                evidence_ref,
                "--relation-id",
                relation_id,
                "--objection-code",
                candidate["objection_code"],
                "--challenged-rule",
                candidate["challenged_rule"],
                "--alternative-explanation",
                candidate["alternative_explanation"],
                "--required-followup-evidence",
                candidate["required_followup_evidence"][0],
                "--report-risk",
                candidate["report_risk"],
            )
            self.assertEqual("completed", challenge_payload["status"])

            packet_payload = run_script(
                script_path("materialize-spatiotemporal-relation-evidence-packet"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--relation-id",
                relation_id,
                "--write-basis-objects",
            )
            self.assertEqual("completed", packet_payload["status"])
            self.assertEqual(3, packet_payload["summary"]["basis_object_write_count"])
            packet = load_json(Path(packet_payload["summary"]["output_path"]))
            packet_id = packet["packet_id"]
            packet_ref = packet["basis_handoff"]["packet_artifact_ref"]["artifact_ref"]
            self.assertEqual([relation_id], packet["accepted_relation_cue_ids"])
            self.assertIn(
                "report-overclaim-risk",
                [
                    item["objection_code"]
                    for item in packet["challenger_objections"]
                ],
            )
            self.assertIn(
                "challenger-objection",
                [item["uncertainty_kind"] for item in packet["uncertainty_register"]],
            )
            self.assertIn(
                "proves transport",
                packet["report_use_constraints"]["prohibited_claims"],
            )
            self.assertIn(
                "report-section-draft",
                packet["report_use_constraints"]["required_mediation"],
            )
            self.assertIn(
                candidate["required_followup_evidence"][0],
                packet["board_handoff"]["gap_hints"],
            )
            self.assertIn(
                candidate["report_risk"],
                packet_payload["board_handoff"]["gap_hints"],
            )
            self.assertIn(
                "report-overclaim-risk",
                packet_payload["board_handoff"]["challenge_hints"],
            )

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
                "spatiotemporal-relation-evidence-packet",
                "--target-id",
                packet_id,
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
                "spatiotemporal-relation-evidence-packet",
                "--target-id",
                packet_id,
            )
            section_query = run_kernel(
                "query-reporting-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "report-section-draft",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--status",
                "draft",
            )
            self.assertEqual(1, finding_query["summary"]["matching_object_count"])
            self.assertEqual(1, bundle_query["summary"]["matching_object_count"])
            self.assertEqual(1, section_query["summary"]["matching_object_count"])
            finding = finding_query["objects"][0]
            bundle = bundle_query["objects"][0]
            section = section_query["objects"][0]
            self.assertEqual(packet_id, finding["target_id"])
            self.assertEqual(packet_id, bundle["target_id"])
            self.assertIn(packet_id, finding["basis_object_ids"])
            self.assertIn(packet_id, bundle["basis_object_ids"])
            self.assertTrue(
                any(
                    isinstance(ref, dict) and ref.get("artifact_ref") == packet_ref
                    for ref in finding["evidence_refs"]
                )
            )
            self.assertIn(
                "do not establish causality",
                section["section_text"],
            )
            self.assertNotIn("proves transport", section["section_text"])
            self.assertIn(packet_id, section["basis_object_ids"])
            self.assertFalse((run_dir / "report_basis").exists())


if __name__ == "__main__":
    unittest.main()

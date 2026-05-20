from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import load_json, run_kernel, run_script, runtime_src_path, script_path, write_json

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-interaction-timeline"
ROUND_ID = "round-interaction-timeline"


def insert_signal(
    run_dir: Path,
    *,
    signal_id: str,
    plane: str,
    source_skill: str,
    title: str,
    body_text: str,
    timestamp: str,
    metric: str = "",
    metadata: dict[str, object] | None = None,
) -> None:
    from eco_council_runtime.kernel.planes.signal import (
        INSERT_SQL,
        ensure_signal_plane_schema,
    )

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
        "title": title,
        "body_text": body_text,
        "url": f"https://example.test/{signal_id}",
        "author_name": "",
        "channel_name": "",
        "language": "en",
        "query_text": "smoke policy",
        "metric": metric,
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
        "metadata_json": json.dumps(metadata or {}, ensure_ascii=True, sort_keys=True),
        "raw_json": "{}",
        "artifact_path": str(run_dir / "raw" / f"{signal_id}.json"),
        "record_locator": "$",
        "artifact_sha256": "",
    }
    with sqlite3.connect(db_path) as connection:
        ensure_signal_plane_schema(connection)
        connection.execute(INSERT_SQL, row)
        connection.commit()


def write_semantic_helper_artifact(run_dir: Path) -> None:
    write_json(
        run_dir
        / "analytics"
        / f"public_discourse_annotation_aggregation_{ROUND_ID}.json",
        {
            "schema_version": "fixture",
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "semantic_distributions": [
                {
                    "label_family": "trust_confidence_labels",
                    "source_family": "youtube",
                    "discourse_lane": "social_sample_affect",
                    "semantic_scope": "source-family-local",
                    "sample_definition": {"source_family": "youtube"},
                    "denominator_scope": {"denominator": 1},
                    "distribution": [{"label": "low_trust", "item_count": 1}],
                }
            ],
        },
    )


def forbidden_helper_fields(payload: Any) -> list[str]:
    forbidden = {
        "priority",
        "priority_order",
        "rank",
        "ranking",
        "score",
        "weight",
        "recommended_source_rank",
        "recommended_conclusion",
    }
    paths: list[str] = []

    def visit(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}"
                if key in forbidden:
                    paths.append(child_path)
                visit(child, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{path}[{index}]")

    visit(payload, "$")
    return paths


class InteractionTimelineAndSectionBriefTests(unittest.TestCase):
    def test_timeline_nodes_carry_both_sides_and_handoff_section_brief(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            write_semantic_helper_artifact(run_dir)
            insert_signal(
                run_dir,
                signal_id="signal-env-001",
                plane="environment",
                source_skill="fetch-openaq",
                title="PM2.5 observation",
                body_text="PM2.5 was elevated in the city.",
                timestamp="2023-06-07T12:00:00Z",
                metric="pm25",
            )
            insert_signal(
                run_dir,
                signal_id="signal-formal-001",
                plane="formal",
                source_skill="fetch-federal-register-documents",
                title="Official smoke response notice",
                body_text="Agency notice described smoke response coordination.",
                timestamp="2023-06-07T10:00:00Z",
            )
            insert_signal(
                run_dir,
                signal_id="signal-public-001",
                plane="public",
                source_skill="fetch-youtube-comments",
                title="Public comment",
                body_text="Residents asked why smoke guidance was unclear.",
                timestamp="2023-06-07T14:00:00Z",
            )

            result = run_script(
                script_path("build-fact-policy-public-interaction-timeline"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                run_dir
                / "analytics"
                / f"fact_policy_public_interaction_timeline_{ROUND_ID}.json"
            )
            nodes = artifact["interaction_nodes"]

            self.assertEqual("completed", result["status"])
            self.assertEqual(1, len(nodes))
            self.assertIn("semantic_shift_events", artifact)
            self.assertEqual([], forbidden_helper_fields(artifact))
            self.assertEqual([], result["board_handoff"]["suggested_next_skills"])
            self.assertIn(
                "fact-policy-public-interaction-node",
                result["analysis_sync"]["analysis_kind"],
            )
            for node in nodes:
                self.assertTrue(node["fact_or_policy_evidence_refs"])
                self.assertTrue(node["public_or_media_evidence_refs"])
                self.assertIn(
                    "causality",
                    node["claim_boundary"]["excluded_inferences"],
                )
                self.assertIn("semantic_cues", node)

            handoff = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_artifact = load_json(
                run_dir / "reporting" / f"reporting_handoff_{ROUND_ID}.json"
            )
            section_briefs = handoff_artifact["report_packet"]["section_briefs"]

            self.assertEqual("completed", handoff["status"])
            self.assertEqual(1, handoff_artifact["interaction_timeline_node_count"])
            self.assertEqual(1, handoff_artifact["section_brief_count"])
            self.assertEqual(section_briefs, handoff_artifact["section_briefs"])
            self.assertTrue(section_briefs[0]["refs"])
            self.assertEqual(
                "bounded-descriptive-context-only",
                section_briefs[0]["claim_strength"],
            )
            self.assertIn("denominator", section_briefs[0])
            self.assertTrue(section_briefs[0]["limitations"])
            self.assertEqual(
                nodes,
                handoff_artifact["report_packet"]["interaction_timeline_nodes"],
            )

    def test_one_sided_timeline_is_limitation_not_absence_claim(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            insert_signal(
                run_dir,
                signal_id="signal-env-only-001",
                plane="environment",
                source_skill="fetch-openaq",
                title="PM2.5 observation",
                body_text="PM2.5 was elevated in the city.",
                timestamp="2023-06-07T12:00:00Z",
                metric="pm25",
            )

            result = run_script(
                script_path("build-fact-policy-public-interaction-timeline"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                run_dir
                / "analytics"
                / f"fact_policy_public_interaction_timeline_{ROUND_ID}.json"
            )
            boundary_text = json.dumps(
                artifact["parallel_timeline_nodes"],
                sort_keys=True,
            ).lower()

            self.assertEqual("insufficient-interaction-basis", result["status"])
            self.assertEqual([], artifact["interaction_nodes"])
            self.assertGreaterEqual(len(artifact["parallel_timeline_nodes"]), 1)
            self.assertIn("do not write an interaction claim", boundary_text)
            self.assertIn("evidence absence", boundary_text)
            self.assertNotIn("proves absence", boundary_text)


if __name__ == "__main__":
    unittest.main()

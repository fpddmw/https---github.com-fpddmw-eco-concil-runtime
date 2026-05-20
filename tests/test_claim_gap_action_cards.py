from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import (
    load_json,
    run_kernel,
    run_script,
    runtime_path,
    runtime_src_path,
    script_path,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-claim-gap-cards"
ROUND_ID = "round-claim-gap-cards"


def write_public_semantic_mission(run_dir: Path) -> None:
    write_json(
        run_dir / "mission.json",
        {
            "schema_version": "1.0.0",
            "run_id": RUN_ID,
            "topic": "Public policy situation analysis",
            "objective": "Analyze public sentiment and policy semantic interaction for an environmental event.",
            "request_text": "Assess public sentiment, public concerns, media tone, and policy communication boundaries.",
        },
    )


def insert_signal(
    run_dir: Path,
    *,
    signal_id: str,
    plane: str,
    source_skill: str,
    title: str = "Fixture signal",
    body_text: str = "Fixture text",
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
        "url": "",
        "author_name": "",
        "channel_name": "",
        "language": "en",
        "query_text": "",
        "metric": metric,
        "numeric_value": None,
        "unit": "",
        "published_at_utc": "",
        "observed_at_utc": "",
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


def submit_public_request(run_dir: Path) -> str:
    payload = run_script(
        script_path("submit-evidence-request"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--author-role",
        "social-investigator",
        "--question",
        "What public sentiment and public concern sample can support report wording?",
        "--desired-evidence-type",
        "public semantic sample with denominator",
        "--rationale",
        "The report wants public semantic claims but basis is not visible yet.",
        "--target-kind",
        "round",
        "--target-id",
        ROUND_ID,
        "--provenance-json",
        "{\"source\":\"unit-test\"}",
    )
    return str(payload["summary"]["object_id"])


def submit_attempt(
    run_dir: Path,
    *,
    request_id: str,
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
        "{\"query\":\"public smoke concern\"}",
        "--target-kind",
        "evidence-request",
        "--target-id",
        request_id,
        "--target-evidence-request-id",
        request_id,
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


def forbidden_card_fields(payload: Any) -> list[str]:
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


class ClaimGapActionCardTests(unittest.TestCase):
    def test_action_cards_are_advisory_and_recover_all_attempt_outcomes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            write_public_semantic_mission(run_dir)
            request_id = submit_public_request(run_dir)
            insert_signal(
                run_dir,
                signal_id="signal-bluesky-low-volume-001",
                plane="public",
                source_skill="fetch-bluesky-cascade",
                body_text="Smoke communication felt confusing.",
            )
            for source_skill, status in (
                ("fetch-gdelt-doc-search", "failed"),
                ("fetch-youtube-video-search", "receipt-only"),
                ("fetch-youtube-comments", "fetched"),
                ("fetch-bluesky-cascade", "normalized"),
            ):
                submit_attempt(
                    run_dir,
                    request_id=request_id,
                    source_skill=source_skill,
                    status=status,
                    rationale=f"Fixture {status} route outcome.",
                )

            payload = run_script(
                script_path("materialize-claim-gap-action-cards"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(run_dir / "analytics" / f"claim_gap_action_cards_{ROUND_ID}.json")
            cards = artifact["action_cards"]
            text = json.dumps(cards, sort_keys=True).lower()

            self.assertEqual("completed", payload["status"])
            self.assertIn("discovery order", artifact["advisory_semantics"])
            self.assertEqual([], forbidden_card_fields(artifact))
            self.assertEqual([], payload["board_handoff"]["suggested_next_skills"])
            self.assertIn("failed acquisition attempt", text)
            self.assertIn("receipt-only acquisition attempt", text)
            self.assertIn("zero-result acquisition attempt", text)
            self.assertIn("low-volume acquisition attempt", text)
            self.assertIn("must not treat the attempt result as evidence", text)
            self.assertIn("does not prove source absence", text)
            self.assertIn("claim-gap-action-card", payload["analysis_sync"]["analysis_kind"])

    def test_public_semantic_claim_gap_requires_corpus_coverage_and_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            write_public_semantic_mission(run_dir)

            run_script(
                script_path("materialize-claim-gap-action-cards"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(run_dir / "analytics" / f"claim_gap_action_cards_{ROUND_ID}.json")
            public_gap = next(
                card
                for card in artifact["action_cards"]
                if card["claim_gap"].startswith("Public semantic claim lacks")
            )

            self.assertIn("materialize-public-discourse-corpus", public_gap["candidate_skills"])
            self.assertIn("audit-public-discourse-sample-coverage", public_gap["candidate_skills"])
            self.assertIn("aggregate-public-discourse-annotations", public_gap["candidate_skills"])
            self.assertIn("explicit denominator", public_gap["required_inputs"])
            self.assertIn("do not write public sentiment", public_gap["if_not_done_report_boundary"])

    def test_gdelt_tone_is_not_recommended_as_public_sentiment_basis(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            write_public_semantic_mission(run_dir)
            insert_signal(
                run_dir,
                signal_id="signal-gdelt-tone-001",
                plane="public",
                source_skill="fetch-gdelt-doc-search",
                title="GDELT tone fixture",
                body_text="Article text fixture",
                metric="doc_timeline_tone",
                metadata={"gdelt_doc_kind": "gdelt_doc_tone_aggregate"},
            )

            run_script(
                script_path("materialize-claim-gap-action-cards"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(run_dir / "analytics" / f"claim_gap_action_cards_{ROUND_ID}.json")
            gdelt_card = next(
                card
                for card in artifact["action_cards"]
                if card["claim_gap"] == "GDELT tone is visible but cannot serve as public sentiment basis."
            )
            public_sentiment_cards = [
                card
                for card in artifact["action_cards"]
                if "public sentiment" in json.dumps(card, sort_keys=True).lower()
            ]

            self.assertIn("media/document tone", gdelt_card["if_not_done_report_boundary"])
            for card in public_sentiment_cards:
                self.assertFalse(
                    any(
                        skill.startswith("fetch-gdelt")
                        or skill.startswith("normalize-gdelt")
                        for skill in card["candidate_skills"]
                    ),
                    card,
                )

    def test_action_cards_expose_to_agent_entry_and_reporting_handoff_without_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            write_public_semantic_mission(run_dir)
            run_script(
                script_path("materialize-claim-gap-action-cards"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            gate = run_kernel(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            social_entry = next(
                entry
                for entry in gate["agent_entry"]["role_entry_points"]
                if entry.get("role") == "social-investigator"
            )
            contracts = social_entry["skill_contracts_by_layer"]["optional-analysis"]
            action_contract = next(
                item
                for item in contracts
                if item["skill_name"] == "materialize-claim-gap-action-cards"
            )
            self.assertTrue(social_entry["claim_gap_action_card_surface"]["present"])
            self.assertIn("failure_recovery", action_contract)
            self.assertTrue(action_contract["followups"])
            self.assertTrue(action_contract["claim_limits"])
            self.assertIn("materialize-claim-gap-action-cards", social_entry["claim_gap_action_card_command"])

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

            self.assertGreaterEqual(handoff_artifact["claim_gap_action_card_count"], 1)
            self.assertEqual(
                handoff_artifact["claim_gap_action_cards"],
                handoff_artifact["report_packet"]["claim_gap_action_cards"],
            )
            self.assertNotIn("recommended_next_skills", handoff_artifact["claim_gap_action_cards"][0])
            self.assertEqual("completed", handoff["status"])


if __name__ == "__main__":
    unittest.main()

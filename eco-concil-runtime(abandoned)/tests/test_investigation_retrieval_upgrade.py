from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.case_library import command_search_cases, connect_db, init_db  # noqa: E402
from eco_council_runtime.controller.paths import (  # noqa: E402
    history_context_path,
    history_retrieval_path,
    investigation_plan_path,
)
from eco_council_runtime.controller.state_config import write_history_context_file  # noqa: E402
from eco_council_runtime.investigation import build_investigation_plan, causal_focus_for_role  # noqa: E402


def insert_case(
    conn,
    *,
    case_id: str,
    topic: str,
    objective: str,
    region_label: str,
    final_missing_evidence_types: list[str],
    claim_types: list[str],
    observations: list[tuple[str, str]],
    evidence_gaps: list[list[str]],
    mission: dict[str, object],
) -> None:
    conn.execute(
        """
        INSERT INTO cases (
            case_id, run_dir, topic, objective, region_label, region_geometry_json,
            window_start_utc, window_end_utc, max_rounds, max_claims_per_round, max_tasks_per_round,
            source_governance_json, current_round_id, current_stage, round_count, latest_decision_round_id,
            final_moderator_status, final_evidence_sufficiency, final_decision_summary, final_brief,
            final_missing_evidence_types_json, latest_claim_count, latest_observation_count, latest_evidence_count,
            imported_at_utc, mission_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            case_id,
            f"/tmp/{case_id}",
            topic,
            objective,
            region_label,
            "{}",
            "2026-03-01T00:00:00Z",
            "2026-03-02T00:00:00Z",
            None,
            None,
            None,
            "{}",
            "round-01",
            "completed",
            1,
            "round-01",
            "supported",
            "partial",
            f"{case_id} decision summary",
            "",
            json.dumps(final_missing_evidence_types),
            len(claim_types),
            len(observations),
            len(evidence_gaps),
            "2026-03-27T00:00:00Z",
            json.dumps(mission),
        ),
    )
    for index, claim_type in enumerate(claim_types, start=1):
        conn.execute(
            """
            INSERT INTO case_claims (
                case_id, round_id, claim_id, claim_type, priority, status,
                needs_physical_validation, summary, statement, public_source_skills_json, claim_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                "round-01",
                f"{case_id}-claim-{index}",
                claim_type,
                1,
                "reviewed",
                1,
                f"{claim_type} summary",
                f"{claim_type} statement",
                json.dumps(["gdelt-doc-search"]),
                "{}",
            ),
        )
    for index, (metric, source_skill) in enumerate(observations, start=1):
        conn.execute(
            """
            INSERT INTO case_observations (
                case_id, round_id, observation_id, source_skill, metric, aggregation,
                value, unit, quality_flags_json, time_window_json, place_scope_json, observation_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                "round-01",
                f"{case_id}-obs-{index}",
                source_skill,
                metric,
                "daily_mean",
                1.0,
                "unit",
                "[]",
                "{}",
                "{}",
                "{}",
            ),
        )
    for index, gaps in enumerate(evidence_gaps, start=1):
        conn.execute(
            """
            INSERT INTO case_evidence (
                case_id, round_id, evidence_id, claim_id, verdict, confidence,
                summary, gaps_json, observation_ids_json, evidence_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id,
                "round-01",
                f"{case_id}-evidence-{index}",
                f"{case_id}-claim-1",
                "supports",
                "medium",
                "evidence summary",
                json.dumps(gaps),
                json.dumps([f"{case_id}-obs-1"]),
                "{}",
            ),
        )


class InvestigationRetrievalUpgradeTests(unittest.TestCase):
    def test_plan_emits_fetch_intents_alternatives_and_history_query(self) -> None:
        mission = {
            "run_id": "run-001",
            "topic": "New York smoke episode",
            "objective": "Assess whether Canadian wildfire smoke drove the New York PM2.5 spike.",
            "region": {"label": "New York, USA"},
            "window": {"start_utc": "2026-03-25T00:00:00Z", "end_utc": "2026-03-26T00:00:00Z"},
            "hypotheses": ["Canadian wildfire smoke caused the New York AQI surge."],
        }

        plan = build_investigation_plan(mission=mission, round_id="round-01")
        environmentalist_focus = causal_focus_for_role(plan, "environmentalist")

        self.assertEqual("smoke-transport", plan["profile_id"])
        self.assertTrue(plan["fetch_intents"])
        intent_by_leg = {item["leg_id"]: item for item in plan["fetch_intents"] if item["hypothesis_id"] == "hypothesis-001"}
        self.assertEqual(["fire-detection"], intent_by_leg["source"]["gap_types"])
        self.assertEqual(["station-air-quality"], intent_by_leg["impact"]["gap_types"])
        self.assertTrue(plan["hypotheses"][0]["alternative_hypotheses"])
        self.assertEqual("smoke-transport", plan["history_query"]["profile_id"])
        self.assertIn("station-air-quality", plan["history_query"]["gap_types"])
        self.assertIn("fire-detection", plan["history_query"]["gap_types"])
        self.assertTrue(plan["open_questions"])
        self.assertIn("station-air-quality", environmentalist_focus["priority_gap_types"])
        self.assertTrue(environmentalist_focus["hypotheses"][0]["alternative_hypotheses"])

    def test_case_library_search_matches_structured_overlap_without_query_terms(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "case-library.sqlite"
            init_db(db_path)
            smoke_mission = {
                "run_id": "case-smoke",
                "topic": "Cross-border plume investigation",
                "objective": "Attribute the plume through source, transport, and impact evidence.",
                "hypotheses": ["Canadian wildfire smoke drove a New York air-quality event."],
            }
            flood_mission = {
                "run_id": "case-flood",
                "topic": "River basin overflow review",
                "objective": "Review flood propagation evidence.",
                "hypotheses": ["Upstream rainfall caused downstream flooding."],
            }
            with connect_db(db_path) as conn:
                insert_case(
                    conn,
                    case_id="case-smoke",
                    topic="Cross-border plume investigation",
                    objective="Attribute the plume through source, transport, and impact evidence.",
                    region_label="New York, USA",
                    final_missing_evidence_types=["station-air-quality", "fire-detection"],
                    claim_types=["smoke", "wildfire"],
                    observations=[
                        ("pm2_5", "openaq-data-fetch"),
                        ("fire_detection_count", "nasa-firms-fire-fetch"),
                        ("wind_speed_10m", "open-meteo-historical-fetch"),
                    ],
                    evidence_gaps=[["station-air-quality", "fire-detection"]],
                    mission=smoke_mission,
                )
                insert_case(
                    conn,
                    case_id="case-flood",
                    topic="River basin overflow review",
                    objective="Review flood propagation evidence.",
                    region_label="Mississippi Basin, USA",
                    final_missing_evidence_types=["precipitation-hydrology"],
                    claim_types=["flood"],
                    observations=[
                        ("river_discharge", "open-meteo-flood-fetch"),
                        ("precipitation_sum", "open-meteo-historical-fetch"),
                    ],
                    evidence_gaps=[["precipitation-hydrology"]],
                    mission=flood_mission,
                )
                conn.commit()

            payload = command_search_cases(
                argparse.Namespace(
                    db=str(db_path),
                    query="",
                    region_label="",
                    moderator_status="",
                    evidence_sufficiency="",
                    exclude_case_id="",
                    profile_id="smoke-transport",
                    claim_types=["smoke", "wildfire"],
                    metric_families=["air-quality", "fire-detection", "meteorology"],
                    gap_types=["station-air-quality", "fire-detection"],
                    source_skills=["openaq-data-fetch", "nasa-firms-fire-fetch"],
                    limit=10,
                    pretty=False,
                )
            )

            self.assertEqual(1, payload["count"])
            self.assertEqual("case-smoke", payload["cases"][0]["case_id"])
            self.assertIn("profile:smoke-transport", payload["cases"][0]["match_reasons"])
            self.assertIn("station-air-quality", payload["cases"][0]["matched_gap_types"])
            self.assertIn("air-quality", payload["cases"][0]["matched_metric_families"])
            self.assertEqual("structured-strong", payload["cases"][0]["score_components"]["match_tier"])

    def test_case_library_search_prefers_strong_structured_match_over_lexical_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "case-library.sqlite"
            init_db(db_path)
            structured_mission = {
                "run_id": "case-structured",
                "topic": "Transport attribution archive",
                "objective": "Resolve the smoke transport chain with direct evidence.",
                "hypotheses": ["Canadian wildfire smoke drove a New York air-quality event."],
            }
            lexical_only_mission = {
                "run_id": "case-lexical-only",
                "topic": "Mississippi flood archive",
                "objective": "Narrative archive about downstream flooding without aligned physical support.",
                "hypotheses": ["Downstream flooding remained the main issue."],
            }
            with connect_db(db_path) as conn:
                insert_case(
                    conn,
                    case_id="case-structured",
                    topic="Transport attribution archive",
                    objective="Resolve the smoke transport chain with direct evidence.",
                    region_label="New York, USA",
                    final_missing_evidence_types=["station-air-quality", "fire-detection"],
                    claim_types=["smoke", "wildfire"],
                    observations=[
                        ("pm2_5", "openaq-data-fetch"),
                        ("fire_detection_count", "nasa-firms-fire-fetch"),
                        ("wind_speed_10m", "open-meteo-historical-fetch"),
                    ],
                    evidence_gaps=[["station-air-quality", "fire-detection"]],
                    mission=structured_mission,
                )
                insert_case(
                    conn,
                    case_id="case-lexical-only",
                    topic="New York smoke episode and Canadian wildfire discussion archive",
                    objective="Narrative archive about smoke without aligned physical support.",
                    region_label="New York, USA",
                    final_missing_evidence_types=["precipitation-hydrology"],
                    claim_types=["flood"],
                    observations=[
                        ("river_discharge", "usgs-water-fetch"),
                    ],
                    evidence_gaps=[["precipitation-hydrology"]],
                    mission=lexical_only_mission,
                )
                conn.commit()

            payload = command_search_cases(
                argparse.Namespace(
                    db=str(db_path),
                    query="New York smoke episode",
                    region_label="New York, USA",
                    moderator_status="",
                    evidence_sufficiency="",
                    exclude_case_id="",
                    profile_id="smoke-transport",
                    claim_types=["smoke", "wildfire"],
                    metric_families=["air-quality", "fire-detection", "meteorology"],
                    gap_types=["station-air-quality", "fire-detection"],
                    source_skills=["openaq-data-fetch", "nasa-firms-fire-fetch"],
                    limit=10,
                    pretty=False,
                )
            )

            self.assertEqual(2, payload["count"])
            self.assertEqual("case-structured", payload["cases"][0]["case_id"])
            self.assertEqual("structured-strong", payload["cases"][0]["score_components"]["match_tier"])
            self.assertEqual("region", payload["cases"][1]["score_components"]["match_tier"])

    def test_write_history_context_file_uses_plan_driven_structured_search(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "run-001"
            run_dir.mkdir(parents=True, exist_ok=True)
            mission = {
                "run_id": "run-001",
                "topic": "New York smoke episode",
                "objective": "Assess whether Canadian wildfire smoke drove the New York PM2.5 spike.",
                "region": {"label": "New York, USA"},
                "window": {"start_utc": "2026-03-25T00:00:00Z", "end_utc": "2026-03-26T00:00:00Z"},
                "hypotheses": ["Canadian wildfire smoke caused the New York AQI surge."],
            }
            (run_dir / "mission.json").write_text(json.dumps(mission), encoding="utf-8")
            plan = build_investigation_plan(mission=mission, round_id="round-01")
            plan_path = investigation_plan_path(run_dir, "round-01")
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(json.dumps(plan), encoding="utf-8")
            db_path = Path(temp_dir) / "history.sqlite"
            init_db(db_path)
            with connect_db(db_path) as conn:
                insert_case(
                    conn,
                    case_id="case-smoke",
                    topic="Historic smoke case",
                    objective="Historic objective",
                    region_label="New York, USA",
                    final_missing_evidence_types=["station-air-quality", "fire-detection"],
                    claim_types=["smoke", "wildfire"],
                    observations=[
                        ("pm2_5", "openaq-data-fetch"),
                        ("fire_detection_count", "nasa-firms-fire-fetch"),
                        ("wind_speed_10m", "open-meteo-historical-fetch"),
                    ],
                    evidence_gaps=[["station-air-quality", "fire-detection"], ["meteorology-background"]],
                    mission={
                        "run_id": "case-smoke",
                        "topic": "Historic smoke case",
                        "objective": "Historic objective",
                        "hypotheses": ["Canadian wildfire smoke caused a New York AQI surge."],
                    },
                )
                insert_case(
                    conn,
                    case_id="case-flood",
                    topic="Historic flood case",
                    objective="Historic flood objective",
                    region_label="Mississippi Basin, USA",
                    final_missing_evidence_types=["precipitation-hydrology"],
                    claim_types=["flood"],
                    observations=[
                        ("river_discharge", "open-meteo-flood-fetch"),
                        ("precipitation_sum", "open-meteo-historical-fetch"),
                    ],
                    evidence_gaps=[["precipitation-hydrology"]],
                    mission={
                        "run_id": "case-flood",
                        "topic": "Historic flood case",
                        "objective": "Historic flood objective",
                        "hypotheses": ["Upstream rainfall caused downstream flooding."],
                    },
                )
                conn.commit()

            target = write_history_context_file(
                run_dir,
                {"history_context": {"db": str(db_path), "top_k": 5}},
                "round-01",
            )

            self.assertIsNotNone(target)
            self.assertEqual(history_context_path(run_dir, "round-01"), target)
            content = target.read_text(encoding="utf-8")
            snapshot = json.loads(history_retrieval_path(run_dir, "round-01").read_text(encoding="utf-8"))

            self.assertIn("Current investigation profile: smoke-transport", content)
            self.assertIn("Retrieval focus:", content)
            self.assertIn("Open planning questions:", content)
            self.assertIn("alternatives=", content)
            self.assertIn("case_id=case-smoke", content)
            self.assertIn("excerpt_1=", content)
            self.assertEqual(3, snapshot["budget"]["max_cases"])
            self.assertEqual(2, snapshot["budget"]["max_excerpts_per_case"])
            self.assertGreaterEqual(snapshot["budget"]["selected_case_count"], 1)
            self.assertGreaterEqual(snapshot["budget"]["candidate_case_count"], 1)
            self.assertGreater(snapshot["budget"]["estimated_token_cost"], 0)
            self.assertEqual("case-smoke", snapshot["cases"][0]["case_id"])
            self.assertLessEqual(len(snapshot["cases"][0]["excerpts"]), 2)
            self.assertTrue(snapshot["cases"][0]["excerpt_budget"]["truncated_by_cap"])
            self.assertIn(
                snapshot["cases"][0]["excerpts"][0]["artifact_kind"],
                {"decision-summary", "evidence-card", "curated-summary", "round-summary", "report-summary"},
            )

            second_target = write_history_context_file(
                run_dir,
                {"history_context": {"db": str(db_path), "top_k": 5}},
                "round-01",
            )
            self.assertEqual(target, second_target)
            second_snapshot = json.loads(history_retrieval_path(run_dir, "round-01").read_text(encoding="utf-8"))
            self.assertEqual(snapshot["budget"], second_snapshot["budget"])
            self.assertEqual(snapshot["cases"], second_snapshot["cases"])


if __name__ == "__main__":
    unittest.main()

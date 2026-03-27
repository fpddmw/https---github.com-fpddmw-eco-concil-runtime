from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.reporting_drafts import (  # noqa: E402
    build_data_readiness_draft,
    build_decision_draft_from_state,
    build_expert_report_draft_from_state,
    build_investigation_review_draft_from_state,
)

ROUND_ID = "round-001"
NEXT_ROUND_ID = "round-002"


def example_mission(*, run_id: str) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "topic": "Chiang Mai smoke verification",
        "objective": "Determine whether public smoke claims are supported by physical evidence.",
        "policy_profile": "standard",
        "constraints": {
            "max_rounds": 3,
            "max_claims_per_round": 4,
            "max_tasks_per_round": 4,
        },
        "window": {
            "start_utc": "2026-03-18T00:00:00Z",
            "end_utc": "2026-03-19T23:59:59Z",
        },
        "region": {
            "label": "Chiang Mai, Thailand",
            "geometry": {
                "type": "Point",
                "latitude": 18.7883,
                "longitude": 98.9853,
            },
        },
        "hypotheses": [
            "Smoke discussion is driven by real fire activity upwind of Chiang Mai.",
        ],
    }


class ReportingDraftsTests(unittest.TestCase):
    def test_build_data_readiness_draft_flags_missing_station_air_quality(self) -> None:
        mission = example_mission(run_id="drafts-readiness-run")
        state = {
            "claims": [
                {
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "summary": "Smoke plume reached Chiang Mai.",
                    "needs_physical_validation": True,
                }
            ],
            "observation_submissions_current": [
                {
                    "submission_id": "obssub-001",
                    "observation_id": "obs-001",
                    "metric": "wind_speed_10m",
                    "source_skill": "open-meteo-archive-fetch",
                    "meaning": "Wind observations indicate transport conditions.",
                    "statistics": {"sample_count": 3, "mean": 7.2},
                }
            ],
        }

        payload = build_data_readiness_draft(
            mission=mission,
            round_id=ROUND_ID,
            role="environmentalist",
            state=state,
            max_findings=3,
        )

        self.assertEqual("needs-more-data", payload["readiness_status"])
        self.assertFalse(payload["sufficient_for_matching"])
        self.assertIn("station-air-quality", " ".join(payload["open_questions"]))

    def test_build_expert_report_draft_from_state_uses_matching_artifacts(self) -> None:
        mission = example_mission(run_id="drafts-report-run")
        state = {
            "mission": mission,
            "round_id": ROUND_ID,
            "claims": [
                {
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "summary": "Smoke plume reached Chiang Mai.",
                }
            ],
            "observations": [
                {
                    "observation_id": "obs-001",
                    "source_skill": "openaq-data-fetch",
                    "metric": "pm2_5",
                    "value": 55.0,
                    "unit": "ug/m3",
                }
            ],
            "cards_active": [
                {
                    "evidence_id": "evidence-001",
                    "claim_id": "claim-001",
                    "verdict": "supports",
                    "confidence": "medium",
                    "summary": "PM2.5 observations support the smoke claim.",
                    "observation_ids": ["obs-001"],
                    "gaps": [],
                }
            ],
            "matching_authorization": {"authorization_status": "authorized"},
            "matching_adjudication": {"summary": "Matching identified one supported claim."},
            "investigation_review": {"summary": "The causal chain is adequately covered."},
            "evidence_adjudication": {"summary": "Evidence is sufficient.", "needs_additional_data": False},
            "remands_open": [],
            "readiness_reports": {
                "sociologist": {"readiness_status": "ready", "summary": "Public side is ready."}
            },
        }

        payload = build_expert_report_draft_from_state(
            state=state,
            role="sociologist",
            max_findings=3,
        )

        self.assertEqual("complete", payload["status"])
        self.assertIn("Matching/adjudication is available", payload["summary"])
        self.assertEqual(["claim-001"], payload["findings"][0]["claim_ids"])

    def test_build_investigation_review_draft_from_state_marks_partial_hypothesis(self) -> None:
        mission = example_mission(run_id="drafts-review-run")
        state = {
            "mission": mission,
            "round_id": ROUND_ID,
            "matching_authorization": {"authorization_id": "auth-001"},
            "matching_result": {
                "result_id": "matchres-001",
                "authorization_id": "auth-001",
                "summary": "One source-side observation was matched.",
            },
            "matching_adjudication": {},
            "evidence_adjudication": {
                "summary": "Source evidence is reasonable but transport remains unresolved.",
                "matching_reasonable": True,
                "needs_additional_data": False,
            },
            "claims": [
                {
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "summary": "Smoke plume reached Chiang Mai.",
                    "hypothesis_id": "hypothesis-001",
                }
            ],
            "observations": [
                {
                    "observation_id": "obs-source",
                    "source_skill": "nasa-firms-fire-fetch",
                    "metric": "fire_detection_count",
                    "hypothesis_id": "hypothesis-001",
                    "leg_id": "source",
                }
            ],
            "cards_active": [
                {
                    "evidence_id": "evidence-001",
                    "claim_id": "claim-001",
                    "verdict": "supports",
                    "confidence": "medium",
                    "summary": "Upwind fire detections support the source leg.",
                    "observation_ids": ["obs-source"],
                }
            ],
            "isolated_active": [],
            "remands_open": [],
            "investigation_plan": {
                "profile_id": "smoke-transport",
                "hypotheses": [
                    {
                        "hypothesis_id": "hypothesis-001",
                        "statement": "Smoke was transported from upwind fires.",
                        "chain_legs": [
                            {"leg_id": "source", "label": "Source fires", "required": True},
                            {"leg_id": "mechanism", "label": "Transport winds", "required": True},
                        ],
                    }
                ],
            },
        }

        payload = build_investigation_review_draft_from_state(state)

        self.assertEqual("partial", payload["review_status"])
        self.assertEqual("partial", payload["hypothesis_reviews"][0]["overall_status"])
        self.assertIn("evidence-001", payload["matched_card_ids"])

    def test_build_decision_draft_from_state_blocks_empty_round(self) -> None:
        mission = example_mission(run_id="drafts-decision-run")
        state = {
            "mission": mission,
            "round_id": ROUND_ID,
            "claims": [],
            "observations": [],
            "readiness_reports": {},
            "matching_authorization": {},
            "remands_open": [],
        }

        payload, next_round_tasks, missing_types = build_decision_draft_from_state(
            run_dir=Path("/tmp/eco-reporting-drafts"),
            state=state,
            next_round_id=NEXT_ROUND_ID,
            reports={},
            report_sources={},
        )

        self.assertEqual("blocked", payload["moderator_status"])
        self.assertFalse(payload["next_round_required"])
        self.assertTrue(next_round_tasks)
        self.assertEqual("sociologist", next_round_tasks[0]["assigned_role"])
        self.assertIn("normalized-public-claims", missing_types)


if __name__ == "__main__":
    unittest.main()

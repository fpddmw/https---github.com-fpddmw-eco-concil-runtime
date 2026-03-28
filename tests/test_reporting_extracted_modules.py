from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.investigation.review import (  # noqa: E402
    build_investigation_review_draft_from_state,
)
from eco_council_runtime.application.reporting.council_decision import (  # noqa: E402
    build_decision_draft_from_state,
)
from eco_council_runtime.application.reporting.expert_reports import (  # noqa: E402
    build_expert_report_draft_from_state,
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


def example_investigation_actions(*actions: dict[str, object]) -> dict[str, object]:
    return {
        "ranked_actions": [dict(action) for action in actions],
    }


class ReportingExtractedModulesTests(unittest.TestCase):
    def test_expert_reports_module_prefers_investigation_actions_before_readiness_fallback(self) -> None:
        mission = example_mission(run_id="expert-module-run")
        state = {
            "mission": mission,
            "round_id": ROUND_ID,
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
            "readiness_reports": {
                "environmentalist": {
                    "readiness_status": "needs-more-data",
                    "summary": "Physical side is not ready yet.",
                    "recommended_next_actions": [
                        {
                            "assigned_role": "environmentalist",
                            "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                            "reason": "Readiness fallback prefers direct PM2.5 corroboration.",
                        }
                    ],
                }
            },
            "investigation_actions": example_investigation_actions(
                {
                    "assigned_role": "environmentalist",
                    "objective": "Add meteorology background such as wind, humidity, and precipitation for the same mission window.",
                    "reason": "The persisted action queue prioritizes the transport mechanism before generic readiness templates.",
                }
            ),
        }

        payload = build_expert_report_draft_from_state(
            state=state,
            role="environmentalist",
            max_findings=3,
        )

        self.assertEqual(
            "Add meteorology background such as wind, humidity, and precipitation for the same mission window.",
            payload["recommended_next_actions"][0]["objective"],
        )
        self.assertEqual(
            "The persisted action queue prioritizes the transport mechanism before generic readiness templates.",
            payload["recommended_next_actions"][0]["reason"],
        )

    def test_investigation_review_module_prefers_investigation_actions(self) -> None:
        mission = example_mission(run_id="review-module-run")
        state = {
            "mission": mission,
            "round_id": ROUND_ID,
            "matching_authorization": {"authorization_id": "auth-001"},
            "matching_result": {
                "result_id": "matchres-001",
                "authorization_id": "auth-001",
                "summary": "One source-side observation was matched.",
            },
            "matching_adjudication": {
                "recommended_next_actions": [
                    {
                        "assigned_role": "environmentalist",
                        "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                        "reason": "Older review fallback.",
                    }
                ]
            },
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
                    "gaps": ["Station-grade corroboration is missing."],
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
            "investigation_actions": example_investigation_actions(
                {
                    "assigned_role": "environmentalist",
                    "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                    "reason": "Top-ranked action says the impact leg should be resolved before broader review.",
                }
            ),
        }

        payload = build_investigation_review_draft_from_state(state)

        self.assertEqual(
            "Top-ranked action says the impact leg should be resolved before broader review.",
            payload["recommended_next_actions"][0]["reason"],
        )

    def test_council_decision_module_uses_investigation_actions_for_task_order(self) -> None:
        mission = example_mission(run_id="decision-module-run")
        state = {
            "mission": mission,
            "round_id": ROUND_ID,
            "claims": [
                {
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "summary": "Smoke plume reached Chiang Mai.",
                    "needs_physical_validation": True,
                }
            ],
            "claim_submissions_current": [
                {
                    "submission_id": "claimsub-001",
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "summary": "Smoke plume reached Chiang Mai.",
                    "source_signal_count": 2,
                    "meaning": "Public discussion indicates localized smoke impacts.",
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
            "readiness_reports": {
                "sociologist": {
                    "readiness_status": "ready",
                    "summary": "Public side is ready.",
                    "sufficient_for_matching": True,
                    "recommended_next_actions": [
                        {
                            "assigned_role": "sociologist",
                            "objective": "Collect more independent public-discussion evidence for the same mission window.",
                            "reason": "Readiness still wants broader public coverage.",
                        }
                    ],
                },
                "environmentalist": {
                    "readiness_status": "needs-more-data",
                    "summary": "Physical side still needs corroboration.",
                    "sufficient_for_matching": False,
                    "recommended_next_actions": [
                        {
                            "assigned_role": "environmentalist",
                            "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                            "reason": "Readiness wants direct PM2.5 corroboration.",
                        }
                    ],
                },
            },
            "matching_authorization": {
                "authorization_status": "deferred",
                "authorization_basis": "needs-more-evidence",
            },
            "remands_open": [],
            "investigation_actions": example_investigation_actions(
                {
                    "assigned_role": "environmentalist",
                    "objective": "Add meteorology background such as wind, humidity, and precipitation for the same mission window.",
                    "reason": "The mechanism leg remains unresolved and is ranked first.",
                },
                {
                    "assigned_role": "sociologist",
                    "objective": "Collect more independent public-discussion evidence for the same mission window.",
                    "reason": "Alternative attribution still needs broader public coverage.",
                },
            ),
        }

        payload, next_round_tasks, missing_types = build_decision_draft_from_state(
            run_dir=Path("/tmp/eco-reporting-drafts"),
            state=state,
            next_round_id=NEXT_ROUND_ID,
            reports={},
            report_sources={},
        )

        self.assertEqual("continue", payload["moderator_status"])
        self.assertEqual(
            "Add meteorology background such as wind, humidity, and precipitation for the same mission window.",
            next_round_tasks[0]["objective"],
        )
        self.assertEqual("environmentalist", next_round_tasks[0]["assigned_role"])
        self.assertEqual(
            "Collect more independent public-discussion evidence for the same mission window.",
            next_round_tasks[1]["objective"],
        )
        self.assertIn("station-air-quality", missing_types)


if __name__ == "__main__":
    unittest.main()

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


def competing_hypothesis_state(*, run_id: str) -> dict[str, object]:
    mission = example_mission(run_id=run_id)
    return {
        "mission": mission,
        "round_id": ROUND_ID,
        "matching_authorization": {
            "authorization_id": "auth-001",
            "authorization_status": "authorized",
        },
        "matching_result": {
            "result_id": "matchres-001",
            "authorization_id": "auth-001",
            "summary": "Source support exists, but impact evidence is contradictory.",
        },
        "matching_adjudication": {},
        "evidence_adjudication": {
            "summary": "Transport source evidence is present, but the impact leg remains contradictory.",
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
            },
            {
                "observation_id": "obs-impact",
                "source_skill": "openaq-data-fetch",
                "metric": "pm2_5",
                "hypothesis_id": "hypothesis-001",
                "leg_id": "impact",
            },
        ],
        "cards_active": [
            {
                "evidence_id": "evidence-001",
                "claim_id": "claim-001",
                "verdict": "supports",
                "confidence": "medium",
                "summary": "Upwind fire detections support the source leg.",
                "observation_ids": ["obs-source"],
                "gaps": [],
            },
            {
                "evidence_id": "evidence-002",
                "claim_id": "claim-001",
                "verdict": "contradicts",
                "confidence": "medium",
                "summary": "Direct impact corroboration still points away from the transported-smoke framing.",
                "observation_ids": ["obs-impact"],
                "gaps": ["Station signal weak."],
            },
        ],
        "isolated_active": [],
        "remands_open": [],
        "investigation_plan": {
            "profile_id": "smoke-transport",
            "hypotheses": [
                {
                    "hypothesis_id": "hypothesis-001",
                    "statement": "Smoke was transported from upwind fires into Chiang Mai.",
                    "chain_legs": [
                        {"leg_id": "source", "label": "Source fires", "required": True},
                        {"leg_id": "mechanism", "label": "Transport winds", "required": True},
                        {"leg_id": "impact", "label": "Local impact", "required": True},
                    ],
                    "alternative_hypotheses": [
                        {
                            "alternative_id": "alt-001",
                            "summary": "Local pollution explains the AQ spike.",
                            "statement": "Local pollution or weather trapping, not transported smoke, drove the AQ spike.",
                            "priority": "high",
                            "gap_types": ["station-air-quality", "meteorology-background"],
                        }
                    ],
                }
            ],
        },
        "investigation_actions": example_investigation_actions(
            {
                "assigned_role": "environmentalist",
                "objective": "Fetch station-based air-quality corroboration for the same mission window and geometry.",
                "reason": "Impact contradiction and the local-pollution alternative both remain active.",
            }
        ),
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

    def test_investigation_review_module_surfaces_governed_probe_requests(self) -> None:
        state = competing_hypothesis_state(run_id="review-module-probe-run")
        state["investigation_actions"] = {
            "ranked_actions": [
                {
                    "assigned_role": "environmentalist",
                    "objective": "Draft a bounded governed discovery probe before selecting the next source family.",
                    "reason": "Current evidence is too thin for a confident template-only follow-up.",
                    "candidate_kind": "governed-discovery-probe",
                    "probe_request": {
                        "probe_id": "probe-round-001-hypothesis-001",
                        "mode": "governance-aware-discovery",
                        "assigned_role": "environmentalist",
                        "question": "Which bounded governed probe would add the most new auditable evidence with minimal token cost?",
                        "reason_codes": ["governed-discovery-probe", "low-evidence-density"],
                    },
                }
            ],
            "probe_requests": [
                {
                    "probe_id": "probe-round-001-hypothesis-001",
                    "mode": "governance-aware-discovery",
                    "assigned_role": "environmentalist",
                    "question": "Which bounded governed probe would add the most new auditable evidence with minimal token cost?",
                    "reason_codes": ["governed-discovery-probe", "low-evidence-density"],
                }
            ],
        }

        payload = build_investigation_review_draft_from_state(state)

        self.assertEqual(1, len(payload["probe_requests"]))
        self.assertIn("governed-discovery-needed", payload["decision_gating"]["reason_codes"])
        self.assertIn("Which bounded governed probe", " ".join(payload["open_questions"]))

    def test_council_decision_module_carries_probe_requests_from_review(self) -> None:
        state = competing_hypothesis_state(run_id="decision-module-probe-run")
        state["investigation_actions"] = {
            "ranked_actions": [
                {
                    "assigned_role": "environmentalist",
                    "objective": "Draft a bounded governed discovery probe before selecting the next source family.",
                    "reason": "Current evidence is too thin for a confident template-only follow-up.",
                    "candidate_kind": "governed-discovery-probe",
                    "probe_request": {
                        "probe_id": "probe-round-001-hypothesis-001",
                        "mode": "governance-aware-discovery",
                        "assigned_role": "environmentalist",
                        "question": "Which bounded governed probe would add the most new auditable evidence with minimal token cost?",
                        "reason_codes": ["governed-discovery-probe", "low-evidence-density"],
                    },
                }
            ],
            "probe_requests": [
                {
                    "probe_id": "probe-round-001-hypothesis-001",
                    "mode": "governance-aware-discovery",
                    "assigned_role": "environmentalist",
                    "question": "Which bounded governed probe would add the most new auditable evidence with minimal token cost?",
                    "reason_codes": ["governed-discovery-probe", "low-evidence-density"],
                }
            ],
        }
        state["investigation_review"] = build_investigation_review_draft_from_state(state)

        payload, next_round_tasks, _missing_types = build_decision_draft_from_state(
            run_dir=Path("/tmp/eco-reporting-drafts"),
            state=state,
            next_round_id=NEXT_ROUND_ID,
            reports={},
            report_sources={},
        )

        self.assertEqual("continue", payload["moderator_status"])
        self.assertEqual(1, len(payload["probe_requests"]))
        self.assertEqual(1, payload["decision_gating"]["discovery_probe_count"])
        self.assertTrue(next_round_tasks)

    def test_investigation_review_module_surfaces_competing_hypothesis_gating(self) -> None:
        payload = build_investigation_review_draft_from_state(
            competing_hypothesis_state(run_id="review-module-gating-run")
        )

        self.assertTrue(payload["another_round_required"])
        self.assertIn("required-leg-unresolved", payload["decision_gating"]["reason_codes"])
        self.assertIn("contradiction-active", payload["decision_gating"]["reason_codes"])
        self.assertIn("alternative-still-active", payload["decision_gating"]["reason_codes"])
        self.assertEqual("impact", payload["contradiction_paths"][0]["leg_id"])
        hypothesis_review = payload["hypothesis_reviews"][0]
        self.assertEqual("alternative-pressure", hypothesis_review["comparison_outcome"])
        self.assertEqual("active", hypothesis_review["alternative_reviews"][0]["status"])
        self.assertIn("mechanism", hypothesis_review["unresolved_required_leg_ids"])

    def test_council_decision_module_uses_review_gating_without_remands(self) -> None:
        state = competing_hypothesis_state(run_id="decision-module-gating-run")
        state["investigation_review"] = build_investigation_review_draft_from_state(state)

        payload, next_round_tasks, missing_types = build_decision_draft_from_state(
            run_dir=Path("/tmp/eco-reporting-drafts"),
            state=state,
            next_round_id=NEXT_ROUND_ID,
            reports={},
            report_sources={},
        )

        self.assertEqual("continue", payload["moderator_status"])
        self.assertTrue(payload["next_round_required"])
        self.assertEqual("partial", payload["evidence_sufficiency"])
        self.assertEqual([], missing_types)
        self.assertEqual("environmentalist", next_round_tasks[0]["assigned_role"])
        self.assertIn("contradiction", payload["decision_summary"].lower())
        self.assertIn("alternative", payload["decision_summary"].lower())
        self.assertIn("contradiction-active", payload["decision_gating"]["reason_codes"])


if __name__ == "__main__":
    unittest.main()

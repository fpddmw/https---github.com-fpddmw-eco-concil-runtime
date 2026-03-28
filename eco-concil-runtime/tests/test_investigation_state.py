from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.investigation.state import build_investigation_state_from_round_state  # noqa: E402


def smoke_round_state() -> dict[str, object]:
    claim = {
        "claim_id": "claim-001",
        "claim_type": "smoke",
        "summary": "Residents attributed the haze to transported wildfire smoke.",
        "hypothesis_id": "hypothesis-001",
        "leg_id": "public_interpretation",
    }
    source_observation = {
        "observation_id": "obs-001",
        "metric": "fire_detection_count",
        "source_skill": "nasa-firms-fire-fetch",
        "hypothesis_id": "hypothesis-001",
        "leg_id": "source",
    }
    mechanism_observation = {
        "observation_id": "obs-002",
        "metric": "wind_speed_10m",
        "source_skill": "open-meteo-historical-fetch",
        "hypothesis_id": "hypothesis-001",
        "leg_id": "mechanism",
    }
    impact_observation = {
        "observation_id": "obs-003",
        "metric": "pm2_5",
        "source_skill": "openaq-data-fetch",
        "hypothesis_id": "hypothesis-001",
        "leg_id": "impact",
    }
    return {
        "mission": {
            "schema_version": "1.0.0",
            "run_id": "investigation-state-run",
            "objective": "Determine whether transported smoke drove the Chiang Mai impact.",
        },
        "round_id": "round-001",
        "phase_state": {"matching_authorization_status": "authorized"},
        "investigation_plan": {
            "schema_version": "1.0.0",
            "profile_id": "smoke-transport",
            "profile_summary": "Cross-region smoke attribution from source through transport into impact.",
            "open_questions": ["Which explanation currently has the stronger support?"],
            "hypotheses": [
                {
                    "hypothesis_id": "hypothesis-001",
                    "statement": "Transported wildfire smoke caused the mission-region air-quality degradation.",
                    "summary": "Transported smoke caused the impact.",
                    "chain_legs": [
                        {
                            "leg_id": "source",
                            "label": "Source-region fire activity",
                            "required": True,
                            "gap_types": ["fire-detection"],
                        },
                        {
                            "leg_id": "mechanism",
                            "label": "Transport mechanism",
                            "required": True,
                            "gap_types": ["meteorology-background"],
                        },
                        {
                            "leg_id": "impact",
                            "label": "Receptor impact",
                            "required": True,
                            "gap_types": ["station-air-quality"],
                        },
                        {
                            "leg_id": "public_interpretation",
                            "label": "Public interpretation",
                            "required": False,
                            "gap_types": ["public-discussion-coverage"],
                            "claim_types": ["smoke"],
                        },
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
        "claims": [claim],
        "observations": [source_observation, mechanism_observation, impact_observation],
        "claim_submissions_current": [claim],
        "observation_submissions_current": [source_observation, mechanism_observation, impact_observation],
        "claim_submissions_auditable": [claim],
        "observation_submissions_auditable": [source_observation, mechanism_observation, impact_observation],
        "cards_active": [
            {
                "evidence_id": "evidence-001",
                "claim_id": "claim-001",
                "observation_ids": ["obs-001"],
                "verdict": "supports",
                "hypothesis_id": "hypothesis-001",
                "leg_id": "source",
                "gaps": [],
            },
            {
                "evidence_id": "evidence-002",
                "claim_id": "claim-001",
                "observation_ids": ["obs-002"],
                "verdict": "mixed",
                "hypothesis_id": "hypothesis-001",
                "leg_id": "mechanism",
                "gaps": ["Need clearer transport linkage."],
            },
            {
                "evidence_id": "evidence-003",
                "claim_id": "claim-001",
                "observation_ids": ["obs-003"],
                "verdict": "contradicts",
                "hypothesis_id": "hypothesis-001",
                "leg_id": "impact",
                "gaps": ["Station signal weak."],
            },
        ],
        "isolated_active": [
            {
                "isolated_id": "isolated-001",
                "entity_kind": "claim",
                "entity_id": "claim-001",
                "reason": "Public narrative still needs a stronger cross-domain link.",
            }
        ],
        "remands_open": [
            {
                "remand_id": "remand-001",
                "entity_kind": "observation",
                "entity_id": "obs-002",
                "reasons": ["Need better wind corridor alignment."],
            }
        ],
        "matching_result": {"result_id": "matchres-001"},
        "evidence_adjudication": {
            "adjudication_id": "adj-001",
            "open_questions": ["Can the mechanism leg be resolved more cleanly?"],
        },
        "investigation_review": {
            "review_id": "review-001",
            "review_status": "partial",
            "open_questions": ["Should the local-pollution alternative remain active?"],
            "hypothesis_reviews": [
                {
                    "hypothesis_id": "hypothesis-001",
                    "overall_status": "partial",
                    "notes": ["Moderator review sees unresolved contradiction on impact."],
                    "leg_reviews": [
                        {
                            "leg_id": "impact",
                            "status": "contradicted",
                            "summary": "Impact evidence currently points away from the main transport framing.",
                            "evidence_refs": ["card:evidence-003"],
                            "notes": ["Impact contradiction remains unresolved."],
                        }
                    ],
                }
            ],
        },
    }


class InvestigationStateTests(unittest.TestCase):
    def test_build_investigation_state_tracks_legs_gaps_and_alternatives(self) -> None:
        payload = build_investigation_state_from_round_state(smoke_round_state())

        self.assertEqual("investigation-state-round-001", payload["state_id"])
        self.assertEqual("partial", payload["overall_status"])
        self.assertEqual("investigation-review", payload["last_update_stage"])
        self.assertEqual(3, payload["artifact_presence"]["matched_card_count"])
        self.assertEqual(1, payload["artifact_presence"]["isolated_count"])
        self.assertEqual(1, payload["artifact_presence"]["remand_count"])
        self.assertEqual(1, payload["summary"]["hypothesis_count"])
        self.assertEqual(1, payload["summary"]["partial_hypothesis_count"])
        self.assertEqual(1, payload["summary"]["alternative_count"])
        self.assertIn("station-air-quality", payload["remaining_gaps"])
        self.assertIn("Should the local-pollution alternative remain active?", payload["open_questions"])

        hypothesis = payload["hypotheses"][0]
        self.assertEqual("partial", hypothesis["overall_status"])
        self.assertEqual(2, hypothesis["support"]["count"])
        self.assertEqual(2, hypothesis["contradiction"]["count"])
        self.assertEqual(3, hypothesis["coverage"]["required_leg_count"])
        self.assertEqual(2, hypothesis["coverage"]["supported_required_leg_count"])
        self.assertEqual(1, hypothesis["coverage"]["unresolved_leg_count"])
        self.assertIn("card:evidence-001", hypothesis["latest_evidence_refs"])
        self.assertIn("card:evidence-003", hypothesis["latest_evidence_refs"])

        impact_leg = next(item for item in hypothesis["legs"] if item["leg_id"] == "impact")
        self.assertEqual("contradicted", impact_leg["status"])
        self.assertEqual(1, impact_leg["contradiction"]["count"])
        self.assertIn("station-air-quality", impact_leg["remaining_gaps"])
        self.assertIn("Station signal weak.", impact_leg["remaining_gaps"])
        self.assertEqual("high", impact_leg["uncertainty"]["level"])

        public_leg = next(item for item in hypothesis["legs"] if item["leg_id"] == "public_interpretation")
        self.assertEqual("supported", public_leg["status"])
        self.assertEqual(1, public_leg["coverage"]["direct_ref_count"])
        self.assertIn("claim:claim-001", public_leg["latest_evidence_refs"])

        alternative = hypothesis["alternative_hypotheses"][0]
        self.assertEqual("alt-001", alternative["alternative_id"])
        self.assertEqual("seeded", alternative["coverage"]["status"])
        self.assertEqual("high", alternative["uncertainty"]["level"])


if __name__ == "__main__":
    unittest.main()

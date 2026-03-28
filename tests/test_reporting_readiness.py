from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.reporting import (  # noqa: E402
    build_data_readiness_draft,
    build_pre_match_report_findings,
    readiness_missing_types,
)

ROUND_ID = "round-001"


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


class ReportingReadinessTests(unittest.TestCase):
    def test_package_home_build_data_readiness_draft_flags_missing_station_air_quality(self) -> None:
        mission = example_mission(run_id="reporting-readiness-run")
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

    def test_package_home_build_pre_match_report_findings_prefers_persisted_readiness_findings(self) -> None:
        state = {
            "readiness_reports": {
                "environmentalist": {
                    "findings": [
                        {
                            "finding_id": "finding-001",
                            "title": "Persisted readiness finding",
                            "summary": "Already materialized in the readiness report.",
                            "confidence": "medium",
                            "claim_ids": [],
                            "observation_ids": ["obs-001"],
                            "evidence_ids": [],
                        }
                    ]
                }
            }
        }

        findings = build_pre_match_report_findings(state, "environmentalist", 3)

        self.assertEqual(1, len(findings))
        self.assertEqual("finding-001", findings[0]["finding_id"])

    def test_package_home_readiness_missing_types_tracks_public_coverage(self) -> None:
        state = {
            "claim_submissions_current": [
                {
                    "submission_id": "claimsub-001",
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "summary": "Smoke plume reached Chiang Mai.",
                    "meaning": "Public discussion indicates smoke impacts.",
                    "source_signal_count": 2,
                    "public_refs": [
                        {
                            "source_skill": "gdelt-fetch",
                            "channel": "news",
                        }
                    ],
                }
            ]
        }

        missing_types = readiness_missing_types(state, "sociologist")

        self.assertIn("public-discussion-coverage", missing_types)


if __name__ == "__main__":
    unittest.main()

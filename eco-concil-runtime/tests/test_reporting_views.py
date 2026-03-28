from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.reporting_views import (  # noqa: E402
    build_claim_candidate_pool_summary,
    build_fallback_context_from_state,
    build_observation_candidate_pool_summary,
    candidate_claim_entry_from_candidate,
    candidate_observation_entry_from_candidate,
    select_environment_submissions,
    select_public_submissions,
)
from eco_council_runtime.contract import scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.paths import claim_candidates_path  # noqa: E402

ROUND_ID = "round-001"


def example_mission(*, run_id: str) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "topic": "Chiang Mai smoke verification",
        "objective": "Determine whether public smoke claims are supported by physical evidence.",
        "policy_profile": "standard",
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


def scaffold_temp_run(root: Path, *, run_id: str = "reporting-views-run-001") -> Path:
    run_dir = root / run_id
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=example_mission(run_id=run_id),
        tasks=None,
        pretty=True,
    )
    return run_dir


class ReportingViewsTests(unittest.TestCase):
    def test_select_public_submissions_preserves_channel_diversity(self) -> None:
        submissions = [
            {
                "submission_id": "claimsub-gdelt",
                "claim_type": "smoke",
                "source_signal_count": 5,
                "public_refs": [{"source_skill": "gdelt-doc-search"}],
            },
            {
                "submission_id": "claimsub-youtube",
                "claim_type": "smoke",
                "source_signal_count": 4,
                "public_refs": [{"source_skill": "youtube-video-search"}],
            },
            {
                "submission_id": "claimsub-rulemaking",
                "claim_type": "policy-reaction",
                "source_signal_count": 2,
                "public_refs": [{"source_skill": "regulationsgov-comments-fetch"}],
            },
            {
                "submission_id": "claimsub-gdelt-2",
                "claim_type": "smoke",
                "source_signal_count": 3,
                "public_refs": [{"source_skill": "gdelt-gkg-fetch"}],
            },
        ]

        selected = select_public_submissions(submissions, 3)

        self.assertEqual(
            ["claimsub-gdelt", "claimsub-youtube", "claimsub-rulemaking"],
            [item["submission_id"] for item in selected],
        )

    def test_select_environment_submissions_prefers_claim_relevant_families(self) -> None:
        claims = [
            {
                "claim_id": "claim-001",
                "claim_type": "flood",
                "needs_physical_validation": True,
            }
        ]
        submissions = [
            {
                "submission_id": "obs-air",
                "metric": "pm2_5",
                "source_skill": "openaq-data-fetch",
                "value": 22,
            },
            {
                "submission_id": "obs-hydro",
                "metric": "river_discharge_mean",
                "source_skill": "usgs-water-services-fetch",
                "value": 180,
            },
            {
                "submission_id": "obs-meteo",
                "metric": "precipitation_sum",
                "source_skill": "open-meteo-archive-fetch",
                "value": 42,
            },
        ]

        selected = select_environment_submissions(submissions, claims, 2)

        self.assertEqual(["obs-hydro", "obs-meteo"], [item["submission_id"] for item in selected])

    def test_build_claim_candidate_pool_summary_tracks_channels_and_days(self) -> None:
        summary = build_claim_candidate_pool_summary(
            [
                {
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "needs_physical_validation": True,
                    "source_signal_count": 3,
                    "time_window": {"start_utc": "2026-03-18T01:00:00Z"},
                    "public_refs": [{"source_skill": "gdelt-doc-search"}],
                },
                {
                    "claim_id": "claim-002",
                    "claim_type": "policy-reaction",
                    "needs_physical_validation": False,
                    "source_signal_count": 1,
                    "time_window": {"start_utc": "2026-03-19T02:00:00Z"},
                    "public_refs": [{"source_skill": "regulationsgov-comments-fetch"}],
                },
            ]
        )

        self.assertEqual(4, summary["total_source_signal_count"])
        self.assertEqual(1, summary["needs_physical_validation_count"])
        self.assertEqual(
            [{"value": "gdelt", "count": 1}, {"value": "rulemaking", "count": 1}],
            summary["channel_counts"],
        )
        self.assertEqual(
            [{"value": "2026-03-18", "count": 1}, {"value": "2026-03-19", "count": 1}],
            summary["day_bucket_counts"],
        )

    def test_build_observation_candidate_pool_summary_uses_distribution_counts(self) -> None:
        summary = build_observation_candidate_pool_summary(
            [
                {
                    "observation_id": "obs-001",
                    "metric": "pm2_5",
                    "source_skill": "openaq-data-fetch",
                    "quality_flags": ["night"],
                    "distribution_summary": {
                        "signal_count": 6,
                        "time_bucket_counts": [{"value": "2026-03-18", "count": 6}],
                        "source_skill_counts": [{"value": "openaq-data-fetch", "count": 6}],
                        "metric_counts": [{"value": "pm2_5", "count": 6}],
                        "point_bucket_counts": [{"value": "18.788,98.985", "count": 6}],
                    },
                },
                {
                    "observation_id": "obs-002",
                    "metric": "temperature_2m",
                    "source_skill": "open-meteo-archive-fetch",
                    "quality_flags": ["day"],
                    "statistics": {"sample_count": 2, "mean": 30.0},
                    "time_window": {"start_utc": "2026-03-19T00:00:00Z"},
                    "place_scope": {
                        "geometry": {
                            "type": "Point",
                            "latitude": 18.9,
                            "longitude": 99.0,
                        }
                    },
                },
            ]
        )

        coverage = summary["distribution_coverage"]
        self.assertEqual(8, coverage["total_signal_count"])
        self.assertEqual(1, coverage["with_distribution_summary_count"])
        self.assertEqual(1, coverage["with_statistics_count"])
        self.assertEqual(2, coverage["multi_signal_candidate_count"])
        self.assertEqual(
            [{"value": "2026-03-18", "count": 6}, {"value": "2026-03-19", "count": 2}],
            coverage["time_bucket_signal_counts"],
        )

    def test_candidate_entry_builders_preserve_legging_and_evidence_role(self) -> None:
        claim_candidate = {
            "claim_id": "claim-001",
            "claim_type": "flood",
            "summary": "River flooding worsened downstream.",
            "statement": "Flooding intensified after upstream rainfall.",
            "priority": 4,
            "needs_physical_validation": True,
            "hypothesis_id": "hypothesis-001",
            "leg_id": "impact",
            "claim_scope": {
                "time_source": "claim",
                "place_source": "claim",
                "usable_for_matching": True,
                "time_window": {"start_utc": "2026-03-18T00:00:00Z", "end_utc": "2026-03-18T06:00:00Z"},
            },
        }
        observation_candidate = {
            "observation_id": "obs-001",
            "metric": "precipitation_sum",
            "aggregation": "sum",
            "value": 42.0,
            "unit": "mm",
            "source_skill": "open-meteo-archive-fetch",
            "time_window": {"start_utc": "2026-03-18T00:00:00Z", "end_utc": "2026-03-18T06:00:00Z"},
            "place_scope": {"label": "Chiang Mai basin"},
            "statistics": {"sample_count": 1, "mean": 42.0},
            "distribution_summary": {"signal_count": 1},
            "quality_flags": ["storm"],
            "hypothesis_id": "hypothesis-001",
            "leg_id": "source",
            "provenance": {"artifact_path": "runs/example/meteo.json"},
        }

        claim_entry = candidate_claim_entry_from_candidate(claim_candidate)
        observation_entry = candidate_observation_entry_from_candidate(
            observation_candidate,
            [claim_candidate],
        )

        self.assertEqual("hypothesis-001", claim_entry["hypothesis_id"])
        self.assertEqual("impact", claim_entry["leg_id"])
        self.assertEqual("flood", claim_entry["claim_type"])
        self.assertEqual("primary", observation_entry["evidence_role"])
        self.assertEqual("source", observation_entry["leg_id"])
        self.assertEqual([{"artifact_path": "runs/example/meteo.json"}], observation_entry["provenance_refs"])

    def test_build_fallback_context_from_state_compacts_library_views(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir))
            state = {
                "mission": example_mission(run_id=run_dir.name),
                "round_id": ROUND_ID,
                "tasks": [
                    {"task_id": "task-soc", "assigned_role": "sociologist"},
                    {"task_id": "task-env", "assigned_role": "environmentalist"},
                ],
                "claims": [
                    {
                        "claim_id": "claim-001",
                        "claim_type": "smoke",
                        "summary": "Smoke plume reached Chiang Mai.",
                        "needs_physical_validation": True,
                    }
                ],
                "observations": [
                    {
                        "observation_id": "obs-001",
                        "source_skill": "openaq-data-fetch",
                        "metric": "pm2_5",
                        "aggregation": "mean",
                        "value": 55.0,
                        "unit": "ug/m3",
                        "time_window": {"start_utc": "2026-03-18T00:00:00Z", "end_utc": "2026-03-18T06:00:00Z"},
                        "statistics": {"sample_count": 4, "mean": 55.0},
                    }
                ],
                "cards_active": [
                    {
                        "evidence_id": "evidence-001",
                        "claim_id": "claim-001",
                        "verdict": "supports",
                        "confidence": "medium",
                        "summary": "Observed PM2.5 elevation aligns with the smoke claim.",
                        "observation_ids": ["obs-001"],
                        "gaps": [],
                    }
                ],
                "claim_submissions_current": [
                    {
                        "submission_id": "claimsub-001",
                        "claim_id": "claim-001",
                        "claim_type": "smoke",
                        "summary": "Smoke plume reached Chiang Mai.",
                        "meaning": "Public reports describe visible smoke conditions.",
                        "worth_storing": True,
                        "source_signal_count": 2,
                        "public_refs": [{"source_skill": "gdelt-doc-search"}],
                    }
                ],
                "observation_submissions_current": [
                    {
                        "submission_id": "obssub-001",
                        "observation_id": "obs-001",
                        "metric": "pm2_5",
                        "source_skill": "openaq-data-fetch",
                        "aggregation": "mean",
                        "value": 55.0,
                        "unit": "ug/m3",
                        "statistics": {"sample_count": 4, "mean": 55.0},
                        "meaning": "Station observations captured elevated PM2.5.",
                        "worth_storing": True,
                    }
                ],
                "claim_candidates_current": [
                    {
                        "claim_id": "claim-001",
                        "claim_type": "smoke",
                        "summary": "Smoke plume reached Chiang Mai.",
                        "statement": "Residents reported smoke over the city.",
                        "needs_physical_validation": True,
                    }
                ],
                "observation_candidates_current": [
                    {
                        "observation_id": "obs-001",
                        "source_skill": "openaq-data-fetch",
                        "metric": "pm2_5",
                        "aggregation": "mean",
                        "value": 55.0,
                        "unit": "ug/m3",
                    }
                ],
                "claim_curation": {"status": "pending", "curated_claims": []},
                "observation_curation": {"status": "pending", "curated_observations": []},
                "isolated_active": [],
                "remands_open": [],
                "phase_state": {"matching_executed": False},
                "investigation_plan": {},
            }

            context = build_fallback_context_from_state(run_dir=run_dir, state=state, role="environmentalist")

        self.assertEqual("reporting-fallback-v2", context["context_layer"])
        self.assertEqual(1, context["dataset"]["claim_submission_count"])
        self.assertEqual(1, context["dataset"]["observation_submission_count"])
        self.assertEqual("obs-001", context["observations"][0]["observation_id"])
        self.assertEqual(
            "claim-001",
            context["evidence_library"]["claim_candidates_current"][0]["claim_id"],
        )
        self.assertEqual(
            "obs-001",
            context["evidence_library"]["observation_candidates_current"][0]["observation_id"],
        )
        self.assertEqual(
            str(claim_candidates_path(run_dir, ROUND_ID)),
            context["canonical_paths"]["claim_candidates"],
        )


if __name__ == "__main__":
    unittest.main()

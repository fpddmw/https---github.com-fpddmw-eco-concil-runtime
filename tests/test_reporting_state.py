from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.reporting_state import (  # noqa: E402
    augment_context_with_matching_state,
    collect_round_state,
    hydrate_observation_submissions_with_observations,
    matching_executed_for_state,
    shared_observation_id,
)
from eco_council_runtime.contract import scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.paths import (  # noqa: E402
    claims_active_path,
    claim_submissions_path,
    matching_adjudication_path,
    matching_authorization_path,
    matching_result_path,
    observation_submissions_path,
    observations_active_path,
    shared_claims_path,
    shared_observations_path,
)

ROUND_ID = "round-001"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


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
        "source_governance": {
            "approved_layers": [
                {
                    "family_id": "gdelt",
                    "layer_id": "bulk",
                    "approved_by": "human",
                    "reason": "This run may use one anchored GDELT bulk layer after article recon.",
                }
            ]
        },
    }


def scaffold_temp_run(root: Path, *, run_id: str = "reporting-state-run-001") -> Path:
    run_dir = root / run_id
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=example_mission(run_id=run_id),
        tasks=None,
        pretty=True,
    )
    return run_dir


def observation_record() -> dict[str, object]:
    return {
        "source_skill": "nasa-firms-fire-fetch",
        "metric": "fire_detection_count",
        "aggregation": "event-count",
        "value": 3.0,
        "unit": "count",
        "statistics": {"sample_count": 3, "mean": 1.0},
        "distribution_summary": {"signal_count": 3, "metric_counts": [{"value": "fire_detection_count", "count": 3}]},
        "time_window": {
            "start_utc": "2026-03-18T01:00:00Z",
            "end_utc": "2026-03-18T05:00:00Z",
        },
        "place_scope": {
            "label": "Upwind fire cluster",
            "geometry": {
                "type": "Point",
                "latitude": 19.2,
                "longitude": 99.1,
            },
        },
        "quality_flags": ["night"],
        "hypothesis_id": "hypothesis-001",
        "leg_id": "source",
        "provenance": {
            "source_skill": "nasa-firms-fire-fetch",
            "artifact_path": "runs/example/firms.json",
            "record_locator": "$[0]",
        },
    }


class ReportingStateTests(unittest.TestCase):
    def test_hydrate_observation_submissions_backfills_canonical_ids_and_tags(self) -> None:
        observation = dict(observation_record())
        observation["observation_id"] = shared_observation_id(observation)
        submission = {
            "submission_id": "obssub-legacy",
            "observation_id": "obs-legacy",
            "source_skill": observation["source_skill"],
            "metric": observation["metric"],
            "aggregation": observation["aggregation"],
            "value": observation["value"],
            "unit": observation["unit"],
            "place_scope": observation["place_scope"],
            "time_window": observation["time_window"],
            "provenance": observation["provenance"],
            "quality_flags": observation["quality_flags"],
        }

        hydrated = hydrate_observation_submissions_with_observations([submission], [observation])

        self.assertEqual(1, len(hydrated))
        item = hydrated[0]
        self.assertEqual(observation["observation_id"], item["observation_id"])
        self.assertEqual(f"obssub-{observation['observation_id']}", item["submission_id"])
        self.assertEqual("count", item["unit"])
        self.assertEqual("hypothesis-001", item["hypothesis_id"])
        self.assertEqual("source", item["leg_id"])
        self.assertEqual(observation["statistics"], item["statistics"])

    def test_collect_round_state_hydrates_auditable_observations_and_phase_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir))
            observation = dict(observation_record())
            canonical_observation_id = shared_observation_id(observation)
            claim = {"claim_id": "claim-001", "summary": "Smoke claim"}
            submission = {
                "submission_id": "obssub-legacy",
                "observation_id": "obs-legacy",
                "source_skill": observation["source_skill"],
                "metric": observation["metric"],
                "aggregation": observation["aggregation"],
                "value": observation["value"],
                "unit": observation["unit"],
                "place_scope": observation["place_scope"],
                "time_window": observation["time_window"],
                "provenance": observation["provenance"],
                "quality_flags": observation["quality_flags"],
            }
            write_json(shared_claims_path(run_dir, ROUND_ID), [claim])
            write_json(shared_observations_path(run_dir, ROUND_ID), [observation])
            write_json(claim_submissions_path(run_dir, ROUND_ID), [claim])
            write_json(observation_submissions_path(run_dir, ROUND_ID), [submission])
            write_json(claims_active_path(run_dir, ROUND_ID), [claim])
            write_json(observations_active_path(run_dir, ROUND_ID), [submission])
            write_json(
                matching_authorization_path(run_dir, ROUND_ID),
                {
                    "authorization_id": "auth-001",
                    "authorization_status": "deferred",
                    "authorization_basis": "need-more-data",
                },
            )

            state = collect_round_state(run_dir, ROUND_ID)

        self.assertEqual(["claim-001"], [item["claim_id"] for item in state["claims"]])
        self.assertEqual(canonical_observation_id, state["observations"][0]["observation_id"])
        self.assertEqual(canonical_observation_id, state["observation_submissions_current"][0]["observation_id"])
        self.assertEqual(canonical_observation_id, state["observation_submissions_auditable"][0]["observation_id"])
        self.assertEqual("source", state["observation_submissions_auditable"][0]["leg_id"])
        self.assertEqual("deferred", state["phase_state"]["matching_authorization_status"])
        self.assertFalse(state["phase_state"]["matching_executed"])

    def test_augment_context_with_matching_state_adds_phase_and_matching_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            state = {
                "round_id": ROUND_ID,
                "matching_authorization": {
                    "authorization_id": "auth-001",
                    "authorization_status": "authorized",
                    "authorization_basis": "moderator-approved",
                    "claim_ids": ["claim-001"],
                    "observation_ids": ["obs-001"],
                },
                "matching_adjudication": {
                    "adjudication_id": "matchadj-001",
                    "candidate_set_id": "candset-001",
                    "recommended_next_actions": [
                        {"assigned_role": "moderator", "objective": "Review", "reason": "Need confirmation"}
                    ],
                },
                "matching_result": {
                    "result_id": "matchres-001",
                    "result_status": "matched",
                    "matched_pairs": [{"claim_id": "claim-001"}],
                    "matched_claim_ids": ["claim-001"],
                    "matched_observation_ids": ["obs-001"],
                },
                "evidence_adjudication": {
                    "adjudication_id": "evidence-001",
                    "adjudication_status": "complete",
                },
                "investigation_review": {
                    "review_id": "review-001",
                    "review_status": "complete",
                    "hypothesis_reviews": [{"hypothesis_id": "hypothesis-001"}],
                },
                "readiness_reports": {},
                "claim_curation": {},
                "observation_curation": {},
                "cards_active": [{"evidence_id": "evidence-001"}],
                "isolated_active": [],
                "remands_open": [],
            }

            context = augment_context_with_matching_state(run_dir=run_dir, state=state, context={"phase_state": {}})

        self.assertTrue(matching_executed_for_state(state))
        self.assertEqual("authorized", context["matching"]["authorization"]["authorization_status"])
        self.assertEqual("matched", context["matching"]["result"]["result_status"])
        self.assertEqual("complete", context["matching"]["investigation_review"]["review_status"])
        self.assertTrue(context["phase_state"]["matching_executed"])
        self.assertIn("matching_result", context["canonical_paths"])


if __name__ == "__main__":
    unittest.main()

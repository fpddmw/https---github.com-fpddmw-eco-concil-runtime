from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.normalize_library import (  # noqa: E402
    claim_submission_from_claim,
    library_state,
    materialize_curated_claims,
    materialize_observation_submission_from_curated_entry,
    observation_submission_from_observation,
    shared_observation_id,
)
from eco_council_runtime.controller.paths import (  # noqa: E402
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    claims_active_path,
    evidence_library_ledger_path,
    observation_submissions_path,
    observations_active_path,
    shared_claims_path,
    shared_observations_path,
)


def write_json_artifact(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def read_json_artifact(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def point_scope(label: str, latitude: float, longitude: float) -> dict[str, object]:
    return {
        "label": label,
        "geometry": {
            "type": "Point",
            "latitude": latitude,
            "longitude": longitude,
        },
    }


class NormalizeLibraryTests(unittest.TestCase):
    def test_library_state_hydrates_current_submissions_and_inherits_prior_active_lists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            prior_observation = {
                "run_id": "run-001",
                "round_id": "round-001",
                "source_skill": "openaq-data-fetch",
                "metric": "pm2_5",
                "aggregation": "point",
                "value": 31.0,
                "unit": "ug/m3",
                "time_window": {"start_utc": "2023-06-06T00:00:00Z", "end_utc": "2023-06-06T01:00:00Z"},
                "place_scope": point_scope("Lower Manhattan", 40.7128, -74.0060),
                "statistics": {"sample_count": 1, "mean": 31.0},
                "distribution_summary": {"signal_count": 1},
                "hypothesis_id": "hyp-prior",
                "leg_id": "impact",
                "component_roles": [{"role": "primary", "candidate_observation_id": "cand-prior"}],
                "provenance": {
                    "source_skill": "openaq-data-fetch",
                    "record_locator": "$[0]",
                    "external_id": "prior-obs",
                    "sha256": "a" * 64,
                },
            }
            current_observation = {
                "run_id": "run-001",
                "round_id": "round-002",
                "source_skill": "open-meteo-historical-fetch",
                "metric": "wind_speed",
                "aggregation": "point",
                "value": 6.0,
                "unit": "m/s",
                "time_window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-07T01:00:00Z"},
                "place_scope": point_scope("Brooklyn", 40.6782, -73.9442),
                "statistics": {"sample_count": 3, "mean": 6.0},
                "distribution_summary": {"signal_count": 3},
                "hypothesis_id": "hyp-current",
                "leg_id": "mechanism",
                "component_roles": [{"role": "primary", "candidate_observation_id": "cand-current"}],
                "provenance": {
                    "source_skill": "open-meteo-historical-fetch",
                    "record_locator": "$[0]",
                    "external_id": "current-obs",
                    "sha256": "b" * 64,
                },
            }

            write_json_artifact(shared_observations_path(run_dir, "round-001"), [prior_observation])
            write_json_artifact(
                observations_active_path(run_dir, "round-001"),
                [observation_submission_from_observation(prior_observation)],
            )
            write_json_artifact(shared_observations_path(run_dir, "round-002"), [current_observation])
            write_json_artifact(
                observation_submissions_path(run_dir, "round-002"),
                [
                    {
                        "run_id": "run-001",
                        "round_id": "round-002",
                        "agent_role": "environmentalist",
                        "source_skill": "open-meteo-historical-fetch",
                        "metric": "wind_speed",
                        "aggregation": "point",
                        "value": 6.0,
                        "unit": "m/s",
                        "time_window": current_observation["time_window"],
                        "place_scope": current_observation["place_scope"],
                        "worth_storing": True,
                        "provenance": current_observation["provenance"],
                    }
                ],
            )

            state = library_state(run_dir, "round-002")

            self.assertEqual(2, len(state["shared_observations"]))
            hydrated_current = state["observation_submissions_current"][0]
            self.assertEqual(shared_observation_id(current_observation), hydrated_current["observation_id"])
            self.assertEqual("hyp-current", hydrated_current["hypothesis_id"])
            self.assertEqual("mechanism", hydrated_current["leg_id"])
            self.assertEqual({"sample_count": 3, "mean": 6.0}, hydrated_current["statistics"])
            self.assertEqual({"signal_count": 3}, hydrated_current["distribution_summary"])
            self.assertEqual(
                [{"role": "primary", "candidate_observation_id": "cand-current"}],
                hydrated_current["component_roles"],
            )
            self.assertEqual(1, len(state["observations_active"]))
            self.assertEqual(
                shared_observation_id(prior_observation),
                state["observations_active"][0]["observation_id"],
            )

    def test_materialize_observation_submission_from_curated_entry_marks_mixed_composites(self) -> None:
        entry = {
            "observation_id": "obs-curated-001",
            "candidate_observation_ids": ["cand-001", "cand-002"],
            "meaning": "Composite observation curated across physical evidence sources.",
            "worth_storing": True,
            "component_roles": [
                {"candidate_observation_id": "cand-001", "role": "primary"},
                {"candidate_observation_id": "cand-002", "role": "context"},
            ],
        }
        candidate_lookup = {
            "cand-001": {
                "observation_id": "cand-001",
                "source_skill": "openaq-data-fetch",
                "metric": "pm2_5",
                "aggregation": "point",
                "value": 41.5,
                "unit": "ug/m3",
                "time_window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-07T01:00:00Z"},
                "place_scope": point_scope("Brooklyn", 40.6782, -73.9442),
                "quality_flags": ["station-observation"],
                "hypothesis_id": "hyp-smoke",
                "leg_id": "impact",
            },
            "cand-002": {
                "observation_id": "cand-002",
                "source_skill": "open-meteo-historical-fetch",
                "metric": "wind_speed",
                "aggregation": "point",
                "value": 5.8,
                "unit": "m/s",
                "time_window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-07T01:00:00Z"},
                "place_scope": point_scope("Brooklyn", 40.6782, -73.9442),
                "quality_flags": ["modeled-background"],
                "hypothesis_id": "hyp-smoke",
                "leg_id": "impact",
            },
        }

        submission = materialize_observation_submission_from_curated_entry(
            entry=entry,
            candidate_lookup=candidate_lookup,
            run_id="run-001",
            round_id="round-001",
            mission_scope=point_scope("Mission region", 40.6782, -73.9442),
            mission_time_window={"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-08T00:00:00Z"},
        )

        self.assertEqual("composite-curation", submission["source_skill"])
        self.assertEqual(["openaq-data-fetch", "open-meteo-historical-fetch"], submission["source_skills"])
        self.assertEqual("mixed", submission["unit"])
        self.assertIn("mixed-metric-composite", submission["quality_flags"])
        self.assertIn("mixed-unit-composite", submission["quality_flags"])
        self.assertIn("statistics-omitted-noncomparable-components", submission["quality_flags"])
        self.assertEqual("round_001/environmentalist/observation_curation.json", submission["provenance"]["artifact_path"])
        self.assertEqual("hyp-smoke", submission["hypothesis_id"])
        self.assertEqual("impact", submission["leg_id"])
        self.assertEqual("hyp-smoke", submission["component_roles"][0]["hypothesis_id"])
        self.assertEqual("impact", submission["component_roles"][1]["leg_id"])

    def test_materialize_curated_claims_persists_current_outputs_and_merges_prior_active_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            mission_scope = point_scope("Mission region", 40.7128, -74.0060)
            mission_window = {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-08T00:00:00Z"}
            prior_claim = {
                "claim_id": "claim-prior",
                "run_id": "run-001",
                "round_id": "round-001",
                "claim_type": "smoke",
                "summary": "Prior smoke claim",
                "statement": "Prior smoke affected the city.",
                "priority": 1,
                "needs_physical_validation": True,
                "time_window": mission_window,
                "place_scope": mission_scope,
                "claim_scope": {
                    "time_window": mission_window,
                    "place_scope": mission_scope,
                    "time_source": "candidate-merged",
                    "place_source": "candidate-merged",
                    "usable_for_matching": True,
                    "notes": [],
                },
                "public_refs": [{"source_skill": "youtube-video-search", "external_id": "vid-prior"}],
                "hypothesis_id": "hyp-smoke",
                "leg_id": "impact",
            }
            write_json_artifact(
                claims_active_path(run_dir, "round-001"),
                [claim_submission_from_claim(prior_claim)],
            )
            write_json_artifact(
                claim_candidates_path(run_dir, "round-002"),
                [
                    {
                        "claim_id": "cand-001",
                        "claim_type": "smoke",
                        "summary": "Candidate smoke claim",
                        "statement": "Smoke from wildfires affected New York City.",
                        "source_signal_count": 2,
                        "place_scope": mission_scope,
                        "claim_scope": {
                            "time_window": mission_window,
                            "place_scope": mission_scope,
                            "time_source": "candidate-merged",
                            "place_source": "candidate-merged",
                            "usable_for_matching": True,
                            "notes": [],
                        },
                        "public_refs": [
                            {"source_skill": "youtube-video-search", "external_id": "vid-001"},
                            {"source_skill": "youtube-video-search", "external_id": "vid-002"},
                        ],
                        "hypothesis_id": "hyp-smoke",
                        "leg_id": "impact",
                    }
                ],
            )
            write_json_artifact(
                claim_curation_path(run_dir, "round-002"),
                {
                    "curated_claims": [
                        {
                            "claim_id": "claim-curated-001",
                            "candidate_claim_ids": ["cand-001"],
                            "claim_type": "smoke",
                            "summary": "Curated smoke claim",
                            "statement": "Canadian wildfire smoke affected New York City.",
                            "meaning": "Curated public-side claim for the mission.",
                            "priority": 2,
                            "needs_physical_validation": True,
                            "worth_storing": True,
                            "selection_reason": "Most representative public narrative.",
                        }
                    ]
                },
            )

            result = materialize_curated_claims(
                run_dir=run_dir,
                round_id="round-002",
                run_id="run-001",
                mission_scope=mission_scope,
                mission_time_window=mission_window,
                pretty=False,
            )

            self.assertEqual(1, result["candidate_count"])
            self.assertEqual(1, result["curated_count"])
            self.assertEqual(1, result["claim_submission_count"])
            self.assertEqual(2, result["claims_active_count"])
            self.assertEqual(2, result["shared_claims_count"])

            current_submissions = read_json_artifact(claim_submissions_path(run_dir, "round-002"))
            self.assertEqual("claim-curated-001", current_submissions[0]["claim_id"])
            self.assertEqual("hyp-smoke", current_submissions[0]["hypothesis_id"])
            self.assertEqual("impact", current_submissions[0]["leg_id"])
            self.assertEqual(
                "Most representative public narrative.",
                current_submissions[0]["selection_reason"],
            )

            shared_claims = read_json_artifact(shared_claims_path(run_dir, "round-002"))
            self.assertEqual(
                {"claim-prior", "claim-curated-001"},
                {item["claim_id"] for item in shared_claims},
            )

            ledger_lines = [
                json.loads(line)
                for line in evidence_library_ledger_path(run_dir, "round-002").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(1, len(ledger_lines))
            self.assertEqual("claim-submission", ledger_lines[0]["object_kind"])
            self.assertEqual("claim-curated-001", ledger_lines[0]["payload"]["claim_id"])


if __name__ == "__main__":
    unittest.main()

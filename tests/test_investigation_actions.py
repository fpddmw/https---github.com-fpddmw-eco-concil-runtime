from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.investigation.actions import (  # noqa: E402
    build_investigation_actions_from_round_state,
    materialize_investigation_actions,
    recommendations_from_investigation_actions,
)
from eco_council_runtime.contract import scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.paths import investigation_actions_path, investigation_state_path  # noqa: E402

ROUND_ID = "round-001"


def example_mission(*, run_id: str) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "topic": "Chiang Mai smoke verification",
        "objective": "Determine whether transported smoke caused the local air-quality degradation.",
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
                    "reason": "Bulk GDELT may be used after a narrower recon step.",
                }
            ]
        },
    }


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def investigation_state_payload() -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "state_id": "investigation-state-round-001",
        "run_id": "investigation-actions-run",
        "round_id": ROUND_ID,
        "overall_status": "partial",
        "last_update_stage": "match",
        "last_update_round_id": ROUND_ID,
        "summary": {
            "hypothesis_count": 3,
            "alternative_count": 2,
        },
        "hypotheses": [
            {
                "hypothesis_id": "hypothesis-001",
                "overall_status": "partial",
                "contradiction": {"count": 2, "evidence_refs": ["card:evidence-003", "card:evidence-004"]},
                "remaining_gaps": ["station-air-quality", "meteorology-background"],
                "latest_evidence_refs": ["card:evidence-003", "remand:remand-001"],
                "legs": [
                    {
                        "leg_id": "source",
                        "required": True,
                        "status": "supported",
                        "remaining_gaps": [],
                        "contradiction": {"count": 0, "evidence_refs": []},
                        "coverage": {"pending_ref_count": 0, "direct_ref_count": 0},
                        "latest_evidence_refs": ["card:evidence-001"],
                        "uncertainty": {"level": "low"},
                    },
                    {
                        "leg_id": "mechanism",
                        "required": True,
                        "status": "unresolved",
                        "remaining_gaps": ["meteorology-background"],
                        "contradiction": {"count": 0, "evidence_refs": []},
                        "coverage": {"pending_ref_count": 1, "direct_ref_count": 0},
                        "latest_evidence_refs": ["remand:remand-001"],
                        "uncertainty": {"level": "high"},
                    },
                    {
                        "leg_id": "impact",
                        "required": True,
                        "status": "contradicted",
                        "remaining_gaps": ["station-air-quality"],
                        "contradiction": {"count": 1, "evidence_refs": ["card:evidence-003"]},
                        "coverage": {"pending_ref_count": 0, "direct_ref_count": 0},
                        "latest_evidence_refs": ["card:evidence-003"],
                        "uncertainty": {"level": "high"},
                    },
                ],
                "alternative_hypotheses": [
                    {
                        "alternative_id": "alt-001",
                        "summary": "Local pollution accumulation remains plausible.",
                        "priority": "high",
                        "remaining_gaps": ["public-discussion-coverage"],
                        "coverage": {"status": "seeded"},
                        "uncertainty": {"level": "high"},
                    }
                ],
            },
            {
                "hypothesis_id": "hypothesis-002",
                "overall_status": "unresolved",
                "contradiction": {"count": 0, "evidence_refs": []},
                "remaining_gaps": ["fire-detection", "public-discussion-coverage"],
                "latest_evidence_refs": ["claim:claim-002"],
                "legs": [
                    {
                        "leg_id": "source",
                        "required": True,
                        "status": "unresolved",
                        "remaining_gaps": ["fire-detection"],
                        "contradiction": {"count": 0, "evidence_refs": []},
                        "coverage": {"pending_ref_count": 0, "direct_ref_count": 1},
                        "latest_evidence_refs": ["claim:claim-002"],
                        "uncertainty": {"level": "medium"},
                    },
                    {
                        "leg_id": "impact",
                        "required": True,
                        "status": "unresolved",
                        "remaining_gaps": ["station-air-quality"],
                        "contradiction": {"count": 0, "evidence_refs": []},
                        "coverage": {"pending_ref_count": 0, "direct_ref_count": 0},
                        "latest_evidence_refs": [],
                        "uncertainty": {"level": "high"},
                    },
                ],
                "alternative_hypotheses": [
                    {
                        "alternative_id": "alt-002",
                        "summary": "Public attribution may be misaligned with the physical event.",
                        "priority": "medium",
                        "remaining_gaps": ["public-discussion-coverage"],
                        "coverage": {"status": "planned"},
                        "uncertainty": {"level": "high"},
                    }
                ],
            },
            {
                "hypothesis_id": "hypothesis-003",
                "overall_status": "unresolved",
                "contradiction": {"count": 0, "evidence_refs": []},
                "remaining_gaps": ["precipitation-hydrology"],
                "latest_evidence_refs": [],
                "legs": [
                    {
                        "leg_id": "impact",
                        "required": True,
                        "status": "unresolved",
                        "remaining_gaps": ["precipitation-hydrology"],
                        "contradiction": {"count": 0, "evidence_refs": []},
                        "coverage": {"pending_ref_count": 0, "direct_ref_count": 0},
                        "latest_evidence_refs": [],
                        "uncertainty": {"level": "high"},
                    }
                ],
                "alternative_hypotheses": [],
            },
        ],
    }


class InvestigationActionsTests(unittest.TestCase):
    def test_build_investigation_actions_ranks_and_truncates_deterministically(self) -> None:
        state = {
            "mission": example_mission(run_id="investigation-actions-run"),
            "round_id": ROUND_ID,
            "investigation_state": investigation_state_payload(),
        }

        payload = build_investigation_actions_from_round_state(state)

        self.assertEqual("investigation-actions-round-001", payload["actions_id"])
        self.assertEqual("investigation-state-round-001", payload["investigation_state_id"])
        self.assertEqual(9, payload["budget"]["candidate_count"])
        self.assertEqual(6, payload["budget"]["returned_count"])
        self.assertTrue(payload["budget"]["truncated_by_cap"])
        self.assertEqual(2, payload["budget"]["max_discovery_probes"])
        self.assertEqual(3, payload["summary"]["primary_hypothesis_count"])
        self.assertEqual(2, payload["summary"]["alternative_hypothesis_count"])
        self.assertEqual(5, payload["summary"]["required_leg_gap_count"])
        self.assertEqual(1, payload["summary"]["contradictory_leg_count"])
        self.assertGreaterEqual(payload["summary"]["discovery_probe_count"], 1)

        ranked_actions = payload["ranked_actions"]
        self.assertEqual(6, len(ranked_actions))
        self.assertEqual(list(range(1, 7)), [item["rank"] for item in ranked_actions])
        self.assertEqual(
            ["investigation-action-round-001-01", "investigation-action-round-001-02"],
            [ranked_actions[0]["action_id"], ranked_actions[1]["action_id"]],
        )
        self.assertEqual("environmentalist", ranked_actions[0]["assigned_role"])
        self.assertEqual("resolve-contradiction", ranked_actions[0]["candidate_kind"])
        self.assertEqual("hypothesis-001", ranked_actions[0]["target"]["hypothesis_id"])
        self.assertEqual("impact", ranked_actions[0]["target"]["leg_id"])
        self.assertEqual("station-air-quality", ranked_actions[0]["target"]["gap_types"][0])
        self.assertGreater(ranked_actions[0]["score"]["total"], ranked_actions[-1]["score"]["total"])
        self.assertGreaterEqual(
            ranked_actions[0]["score"]["components"]["contradiction_resolution_value"],
            1.0,
        )
        self.assertTrue(ranked_actions[0]["governed_source_options"])
        self.assertIn(
            ranked_actions[0]["governed_source_options"][0]["approval_state"],
            {"auto-selectable", "approved-layer"},
        )
        self.assertIn("round-001:card:evidence-003", ranked_actions[0]["anchor_refs"])

        action_kinds = {item["candidate_kind"] for item in ranked_actions}
        self.assertIn("governed-discovery-probe", action_kinds)
        self.assertIn("test-alternative-hypothesis", action_kinds)
        self.assertIn("resolve-required-leg", action_kinds)
        self.assertGreaterEqual(len(payload["probe_requests"]), 1)

        recommendations = recommendations_from_investigation_actions(payload, limit=3)
        self.assertEqual(3, len(recommendations))
        self.assertEqual(ranked_actions[0]["objective"], recommendations[0]["objective"])

    def test_build_investigation_actions_emits_governed_probe_for_atypical_gaps(self) -> None:
        state = {
            "mission": example_mission(run_id="investigation-actions-run"),
            "round_id": ROUND_ID,
            "investigation_state": {
                **investigation_state_payload(),
                "hypotheses": [
                    {
                        "hypothesis_id": "hypothesis-probe",
                        "overall_status": "unresolved",
                        "contradiction": {"count": 0, "evidence_refs": []},
                        "remaining_gaps": ["cross-border-attribution", "station-air-quality"],
                        "latest_evidence_refs": [],
                        "legs": [
                            {
                                "leg_id": "impact",
                                "required": True,
                                "status": "unresolved",
                                "remaining_gaps": ["cross-border-attribution"],
                                "contradiction": {"count": 0, "evidence_refs": []},
                                "coverage": {"pending_ref_count": 0, "direct_ref_count": 0},
                                "latest_evidence_refs": [],
                                "uncertainty": {"level": "high"},
                            }
                        ],
                        "alternative_hypotheses": [],
                    }
                ],
            },
        }

        payload = build_investigation_actions_from_round_state(state)

        self.assertEqual(1, payload["budget"]["discovery_probe_count"])
        self.assertEqual(1, len(payload["probe_requests"]))
        probe_request = payload["probe_requests"][0]
        self.assertEqual("governance-aware-discovery", probe_request["mode"])
        self.assertIn("atypical-gap-types", probe_request["reason_codes"])
        probe_action = next(
            item for item in payload["ranked_actions"] if item["candidate_kind"] == "governed-discovery-probe"
        )
        self.assertIn("cross-border-attribution", probe_action["target"]["atypical_gap_types"])
        self.assertTrue(probe_action["governed_source_options"])
        self.assertEqual(probe_request["question"], probe_action["probe_request"]["question"])

    def test_governed_probe_budget_envelope_stays_bounded(self) -> None:
        hypotheses = []
        for index in range(1, 5):
            hypotheses.append(
                {
                    "hypothesis_id": f"hypothesis-probe-{index:03d}",
                    "overall_status": "unresolved",
                    "contradiction": {"count": 0, "evidence_refs": []},
                    "remaining_gaps": ["cross-border-attribution", "station-air-quality"],
                    "latest_evidence_refs": [],
                    "legs": [
                        {
                            "leg_id": "impact",
                            "required": True,
                            "status": "unresolved",
                            "remaining_gaps": ["cross-border-attribution", "station-air-quality"],
                            "contradiction": {"count": 0, "evidence_refs": []},
                            "coverage": {"pending_ref_count": 0, "direct_ref_count": 0},
                            "latest_evidence_refs": [],
                            "uncertainty": {"level": "high"},
                        }
                    ],
                    "alternative_hypotheses": [],
                }
            )
        state = {
            "mission": example_mission(run_id="investigation-actions-run"),
            "round_id": ROUND_ID,
            "investigation_state": {
                **investigation_state_payload(),
                "hypotheses": hypotheses,
            },
        }

        payload = build_investigation_actions_from_round_state(state)

        self.assertEqual(2, payload["budget"]["max_discovery_probes"])
        self.assertEqual(2, payload["budget"]["discovery_probe_count"])
        self.assertLessEqual(len(payload["probe_requests"]), 2)
        self.assertLessEqual(len(payload["ranked_actions"]), 6)
        for probe_request in payload["probe_requests"]:
            self.assertLessEqual(probe_request["governance_envelope"]["source_option_count"], 3)
            self.assertEqual(3, probe_request["budget"]["max_source_options"])
        for action in payload["ranked_actions"]:
            if action["candidate_kind"] != "governed-discovery-probe":
                continue
            self.assertLessEqual(len(action["governed_source_options"]), 3)

    def test_build_investigation_actions_is_deterministic_for_same_state(self) -> None:
        state = {
            "mission": example_mission(run_id="investigation-actions-run"),
            "round_id": ROUND_ID,
            "investigation_state": investigation_state_payload(),
        }

        payload_one = build_investigation_actions_from_round_state(state)
        payload_two = build_investigation_actions_from_round_state(state)

        self.assertEqual(payload_one, payload_two)

    def test_materialize_investigation_actions_writes_canonical_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "investigation-actions-run"
            scaffold_run_from_mission(
                run_dir=run_dir,
                mission=example_mission(run_id="investigation-actions-run"),
                tasks=None,
                pretty=True,
            )
            write_json(investigation_state_path(run_dir, ROUND_ID), investigation_state_payload())

            result = materialize_investigation_actions(run_dir, ROUND_ID, pretty=True)

            self.assertEqual(6, result["ranked_action_count"])
            self.assertTrue(investigation_actions_path(run_dir, ROUND_ID).exists())
            payload = json.loads(investigation_actions_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            self.assertEqual("investigation-actions-round-001", payload["actions_id"])
            self.assertEqual(6, len(payload["ranked_actions"]))


if __name__ == "__main__":
    unittest.main()

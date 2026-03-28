from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.contract_runtime import (  # noqa: E402
    command_init_db,
    parse_bbox,
    parse_point,
    scaffold_run_from_mission,
    validate_bundle,
)
from eco_council_runtime.controller.paths import investigation_actions_path, investigation_state_path  # noqa: E402


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


class ContractRuntimeTests(unittest.TestCase):
    def test_geometry_parsers_handle_valid_and_invalid_inputs(self) -> None:
        self.assertEqual(
            {"type": "Point", "latitude": 18.7, "longitude": 98.9},
            parse_point("18.7,98.9"),
        )
        self.assertEqual(
            {"type": "BBox", "west": 98.0, "south": 18.0, "east": 99.0, "north": 19.0},
            parse_bbox("98.0,18.0,99.0,19.0"),
        )
        with self.assertRaisesRegex(ValueError, "east must be greater than west"):
            parse_bbox("99.0,18.0,98.0,19.0")

    def test_scaffold_run_from_mission_and_validate_bundle_work_directly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "contract-runtime-run"
            result = scaffold_run_from_mission(
                run_dir=run_dir,
                mission=example_mission(run_id="contract-runtime-run"),
                tasks=None,
                pretty=True,
            )

            self.assertEqual("round-001", result["round_id"])
            self.assertTrue((run_dir / "mission.json").exists())
            self.assertTrue((run_dir / "round_001" / "moderator" / "tasks.json").exists())
            state_path = investigation_state_path(run_dir, "round-001")
            actions_path = investigation_actions_path(run_dir, "round-001")
            self.assertTrue(state_path.exists())
            self.assertTrue(actions_path.exists())
            investigation_state = json.loads(state_path.read_text(encoding="utf-8"))
            investigation_actions = json.loads(actions_path.read_text(encoding="utf-8"))
            self.assertEqual("round-001", investigation_state["round_id"])
            self.assertGreaterEqual(investigation_state["summary"]["hypothesis_count"], 1)
            self.assertEqual("round-001", investigation_actions["round_id"])
            self.assertIn("ranked_actions", investigation_actions)

            bundle = validate_bundle(run_dir)
            self.assertTrue(bundle["ok"])
            self.assertEqual(["round-001"], bundle["round_ids"])

    def test_command_init_db_creates_sqlite_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "contracts.sqlite"
            result = command_init_db(argparse.Namespace(db=str(db_path)))

            self.assertTrue(db_path.exists())
            self.assertEqual(str(db_path), result["db"])
            self.assertIn("initialized_at", result)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
sys.path.insert(0, str(RUNTIME_SRC))


def load_registry_snapshot():
    registry_module = importlib.import_module("eco_council_runtime.kernel.registry")

    return registry_module.registry_snapshot


class RuntimeSourceQueueProfileTests(unittest.TestCase):
    def test_registry_assigns_source_queue_profiles_to_every_skill(self) -> None:
        registry = load_registry_snapshot()(WORKSPACE_ROOT)

        self.assertEqual(registry["skill_count"], registry["source_queue_profile_summary"]["skill_count"])
        self.assertEqual(registry["skill_count"], registry["source_queue_profile_summary"]["source_queue_ready_count"])
        self.assertGreater(registry["source_queue_profile_summary"]["core_queue_default_count"], 0)

        for entry in registry["skills"]:
            profile = entry["source_queue_profile"]
            self.assertTrue(profile["source_queue_ready"])
            self.assertTrue(profile["queue_status"])
            self.assertTrue(profile["stage"])
            self.assertTrue(profile["queue_role"])
            self.assertTrue(profile["default_invocation"])
            self.assertIn("notes", profile)

    def test_registry_profiles_classify_key_skill_roles(self) -> None:
        registry = load_registry_snapshot()(WORKSPACE_ROOT)
        profiles = {entry["skill_name"]: entry["source_queue_profile"] for entry in registry["skills"]}

        self.assertEqual("bridge", profiles["eco-prepare-round"]["queue_status"])
        self.assertEqual("source-selection", profiles["eco-prepare-round"]["stage"])
        self.assertEqual("queue-planner", profiles["eco-prepare-round"]["queue_role"])

        self.assertEqual("direct", profiles["eco-normalize-airnow-observation-signals"]["queue_status"])
        self.assertEqual("normalize", profiles["eco-normalize-airnow-observation-signals"]["stage"])

        self.assertEqual("advisory", profiles["eco-query-public-signals"]["queue_status"])
        self.assertEqual("context", profiles["eco-query-public-signals"]["stage"])

        self.assertEqual("direct", profiles["eco-summarize-round-readiness"]["queue_status"])
        self.assertEqual("investigation", profiles["eco-summarize-round-readiness"]["stage"])

        self.assertEqual("direct", profiles["eco-materialize-final-publication"]["queue_status"])
        self.assertEqual("reporting", profiles["eco-materialize-final-publication"]["stage"])


if __name__ == "__main__":
    unittest.main()
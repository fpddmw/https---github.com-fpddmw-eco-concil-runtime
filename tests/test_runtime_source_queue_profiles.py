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
    def test_registry_includes_skill_access_policy_for_every_skill(self) -> None:
        registry = load_registry_snapshot()(WORKSPACE_ROOT)

        self.assertEqual("runtime-registry-v3", registry["schema_version"])
        self.assertEqual(registry["skill_count"], registry["skill_access_summary"]["skill_count"])
        self.assertGreater(registry["skill_access_summary"]["operator_approval_required_count"], 0)

        for entry in registry["skills"]:
            access = entry["skill_access"]
            self.assertEqual(entry["skill_name"], access["skill_name"])
            self.assertTrue(access["skill_layer"])
            self.assertIn("allowed_roles", access)
            self.assertIn("required_capabilities", access)

    def test_registry_assigns_source_queue_profiles_to_every_skill(self) -> None:
        registry = load_registry_snapshot()(WORKSPACE_ROOT)

        self.assertEqual(registry["skill_count"], registry["source_queue_profile_summary"]["skill_count"])
        self.assertEqual(registry["skill_count"], registry["source_queue_profile_summary"]["source_queue_ready_count"])
        self.assertIn(
            "phase2_behavior_counts",
            registry["source_queue_profile_summary"],
        )

        for entry in registry["skills"]:
            profile = entry["source_queue_profile"]
            self.assertTrue(profile["source_queue_ready"])
            self.assertTrue(profile["queue_status"])
            self.assertTrue(profile["stage"])
            self.assertTrue(profile["queue_role"])
            self.assertTrue(profile["default_invocation"])
            self.assertTrue(profile["phase2_behavior"])
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

        self.assertEqual("advisory", profiles["eco-open-investigation-round"]["queue_status"])
        self.assertEqual("round-transition", profiles["eco-open-investigation-round"]["queue_role"])

        self.assertEqual("direct", profiles["eco-summarize-round-readiness"]["queue_status"])
        self.assertEqual("investigation", profiles["eco-summarize-round-readiness"]["stage"])

        self.assertEqual("direct", profiles["eco-materialize-final-publication"]["queue_status"])
        self.assertEqual("reporting", profiles["eco-materialize-final-publication"]["stage"])

        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-extract-observation-candidates"]["phase2_behavior"],
        )
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-merge-observation-candidates"]["phase2_behavior"],
        )
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-link-claims-to-observations"]["phase2_behavior"],
        )
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-derive-observation-scope"]["phase2_behavior"],
        )
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-score-evidence-coverage"]["phase2_behavior"],
        )

        self.assertEqual("direct", profiles["eco-classify-claim-verifiability"]["queue_status"])
        self.assertEqual(
            "non-owning-runtime-surface",
            profiles["eco-classify-claim-verifiability"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-route-verification-lane"]["queue_status"])
        self.assertEqual(
            "non-owning-runtime-surface",
            profiles["eco-route-verification-lane"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-extract-issue-candidates"]["queue_status"])
        self.assertEqual(
            "non-owning-runtime-surface",
            profiles["eco-extract-issue-candidates"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-cluster-issue-candidates"]["queue_status"])
        self.assertEqual(
            "non-owning-runtime-surface",
            profiles["eco-cluster-issue-candidates"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-extract-stance-candidates"]["queue_status"])
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-extract-stance-candidates"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-extract-concern-facets"]["queue_status"])
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-extract-concern-facets"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-extract-actor-profiles"]["queue_status"])
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-extract-actor-profiles"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-extract-evidence-citation-types"]["queue_status"])
        self.assertEqual(
            "optional-runtime-surface",
            profiles["eco-extract-evidence-citation-types"]["phase2_behavior"],
        )
        self.assertEqual("direct", profiles["eco-materialize-controversy-map"]["queue_status"])
        self.assertEqual(
            "non-owning-runtime-surface",
            profiles["eco-materialize-controversy-map"]["phase2_behavior"],
        )


if __name__ == "__main__":
    unittest.main()

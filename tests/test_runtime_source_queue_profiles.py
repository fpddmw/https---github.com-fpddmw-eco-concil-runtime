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
            self.assertEqual([], profile["downstream_hints"])
            self.assertFalse(profile["default_chain_eligible"])
            self.assertNotEqual("planned-step", profile["default_invocation"])

    def test_skill_names_use_layer_prefix_without_project_prefix(self) -> None:
        registry = load_registry_snapshot()(WORKSPACE_ROOT)

        for entry in registry["skills"]:
            skill_name = entry["skill_name"]
            script_path = Path(entry["script_path"])
            profile = entry["source_queue_profile"]

            self.assertFalse(skill_name.startswith("eco-"), skill_name)
            self.assertEqual(skill_name, script_path.parents[1].name)
            self.assertEqual(f"{skill_name.replace('-', '_')}.py", script_path.name)

            if profile["stage"] == "fetch":
                self.assertTrue(skill_name.startswith("fetch-"), skill_name)
            if profile["stage"] == "normalize":
                self.assertTrue(skill_name.startswith("normalize-"), skill_name)
            if profile["stage"] == "query":
                self.assertTrue(skill_name.startswith("query-"), skill_name)

    def test_registry_profiles_classify_key_skill_roles(self) -> None:
        registry = load_registry_snapshot()(WORKSPACE_ROOT)
        profiles = {entry["skill_name"]: entry["source_queue_profile"] for entry in registry["skills"]}

        self.assertEqual("bridge", profiles["prepare-round"]["queue_status"])
        self.assertEqual("source-selection", profiles["prepare-round"]["stage"])
        self.assertEqual("capability-check", profiles["prepare-round"]["queue_role"])

        self.assertEqual("capability", profiles["normalize-airnow-observation-signals"]["queue_status"])
        self.assertEqual("normalize", profiles["normalize-airnow-observation-signals"]["stage"])

        self.assertEqual("advisory", profiles["query-public-signals"]["queue_status"])
        self.assertEqual("query", profiles["query-public-signals"]["stage"])

        self.assertEqual("transition", profiles["open-investigation-round"]["queue_status"])
        self.assertEqual("round-transition-request-consumer", profiles["open-investigation-round"]["queue_role"])

        self.assertEqual("advisory", profiles["summarize-round-readiness"]["queue_status"])
        self.assertEqual("optional-analysis", profiles["summarize-round-readiness"]["stage"])
        self.assertTrue(profiles["summarize-round-readiness"]["requires_explicit_approval"])

        self.assertEqual("capability", profiles["materialize-final-publication"]["queue_status"])
        self.assertEqual("reporting", profiles["materialize-final-publication"]["stage"])
        self.assertTrue(profiles["materialize-final-publication"]["requires_explicit_approval"])

        for skill_name in [
            "aggregate-environment-evidence",
            "review-fact-check-evidence-scope",
            "review-evidence-sufficiency",
            "discover-discourse-issues",
            "suggest-evidence-lanes",
            "materialize-research-issue-surface",
            "project-research-issue-views",
            "export-research-issue-map",
            "apply-approved-formal-public-taxonomy",
            "compare-formal-public-footprints",
            "identify-representation-audit-cues",
            "detect-temporal-cooccurrence-cues",
            "plan-round-orchestration",
            "propose-next-actions",
            "open-falsification-probe",
            "summarize-round-readiness",
        ]:
            self.assertEqual("advisory", profiles[skill_name]["queue_status"])
            self.assertEqual("operator-approved-on-demand", profiles[skill_name]["default_invocation"])
            self.assertEqual("approval-gated-runtime-surface", profiles[skill_name]["phase2_behavior"])
            self.assertTrue(profiles[skill_name]["requires_explicit_approval"])
            self.assertFalse(profiles[skill_name]["default_chain_eligible"])

        for removed_skill in [
            "extract-observation-candidates",
            "merge-observation-candidates",
            "derive-observation-scope",
            "extract-claim-candidates",
            "cluster-claim-candidates",
            "derive-claim-scope",
            "classify-claim-verifiability",
            "route-verification-lane",
            "extract-issue-candidates",
            "cluster-issue-candidates",
            "extract-stance-candidates",
            "extract-concern-facets",
            "extract-actor-profiles",
            "extract-evidence-citation-types",
            "materialize-controversy-map",
            "link-formal-comments-to-public-discourse",
            "identify-representation-gaps",
            "detect-cross-platform-diffusion",
            "link-claims-to-observations",
            "score-evidence-coverage",
        ]:
            self.assertNotIn(removed_skill, profiles)


if __name__ == "__main__":
    unittest.main()

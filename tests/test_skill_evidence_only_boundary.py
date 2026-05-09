from __future__ import annotations

import sys
import unittest
from pathlib import Path

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.governance.skill_registry import (  # noqa: E402
    EVIDENCE_ONLY_FORBIDDEN_OUTPUT_FIELDS,
    FETCH_SKILLS,
    NORMALIZE_SKILLS,
    OPTIONAL_ANALYSIS_SKILLS,
    QUERY_SKILLS,
    SKILL_LAYER_FETCH,
    SKILL_LAYER_NORMALIZE,
    SKILL_LAYER_OPTIONAL_ANALYSIS,
    SKILL_LAYER_QUERY,
    evidence_only_skill_names,
    resolve_skill_policy,
    skill_boundary_violations,
    skill_registry_snapshot,
    validate_skill_output_boundary,
)


class SkillEvidenceOnlyBoundaryTests(unittest.TestCase):
    def test_evidence_only_skills_expose_machine_readable_boundaries(self) -> None:
        snapshot = skill_registry_snapshot()
        registered_names = {
            skill["skill_name"]
            for skill in snapshot["skills"]
            if isinstance(skill, dict)
        }

        for skill_name in evidence_only_skill_names():
            with self.subTest(skill=skill_name):
                self.assertIn(skill_name, registered_names)
                policy = resolve_skill_policy(skill_name)
                boundary = policy["skill_boundary"]

                self.assertEqual(
                    "evidence-only-skill-boundary-v1",
                    boundary["boundary_version"],
                )
                self.assertTrue(boundary["requires_agent_uptake"])
                self.assertTrue(boundary["does_not_assign_evidence_weight"])
                self.assertTrue(boundary["does_not_rank_sources"])
                self.assertTrue(boundary["does_not_recommend_conclusions"])
                self.assertIn("provenance", boundary["required_output_semantics"])
                self.assertTrue(
                    {
                        "score",
                        "weight",
                        "rank",
                        "recommended_conclusion",
                    }.issubset(set(boundary["forbidden_output_fields"]))
                )

    def test_boundary_profiles_match_skill_layers(self) -> None:
        expected_layers = {
            **{name: SKILL_LAYER_FETCH for name in FETCH_SKILLS},
            **{name: SKILL_LAYER_NORMALIZE for name in NORMALIZE_SKILLS},
            "normalize-fetch-execution": SKILL_LAYER_NORMALIZE,
            **{name: SKILL_LAYER_QUERY for name in QUERY_SKILLS},
            **{
                name: SKILL_LAYER_OPTIONAL_ANALYSIS
                for name in OPTIONAL_ANALYSIS_SKILLS
            },
        }

        for skill_name, expected_layer in expected_layers.items():
            with self.subTest(skill=skill_name):
                policy = resolve_skill_policy(skill_name)
                self.assertEqual(expected_layer, policy["skill_layer"])
                self.assertTrue(policy["skill_boundary"])

    def test_non_evidence_write_skills_do_not_claim_evidence_only_boundary(self) -> None:
        evidence_only_names = set(evidence_only_skill_names())
        snapshot = skill_registry_snapshot()

        for skill in snapshot["skills"]:
            if not isinstance(skill, dict):
                continue
            skill_name = skill["skill_name"]
            if skill_name in evidence_only_names:
                continue
            with self.subTest(skill=skill_name):
                self.assertEqual({}, skill.get("skill_boundary", {}))

    def test_evidence_only_scripts_do_not_accept_scoring_or_ranking_flags(self) -> None:
        skills_root = Path(__file__).resolve().parents[1] / "skills"
        forbidden_flags = {
            "--" + field_name.replace("_", "-")
            for field_name in EVIDENCE_ONLY_FORBIDDEN_OUTPUT_FIELDS
        }

        for skill_name in evidence_only_skill_names():
            script_dir = skills_root / skill_name / "scripts"
            scripts = sorted(script_dir.glob("*.py"))
            self.assertTrue(scripts, skill_name)
            for script_path in scripts:
                script_text = script_path.read_text(encoding="utf-8")
                for forbidden_flag in forbidden_flags:
                    with self.subTest(skill=skill_name, flag=forbidden_flag):
                        self.assertNotIn(forbidden_flag, script_text)

    def test_boundary_validator_finds_forbidden_output_fields(self) -> None:
        payload = {
            "status": "completed",
            "matches": [
                {
                    "signal_id": "signal-001",
                    "score": 0.88,
                    "recommended_conclusion": "adopt this claim",
                }
            ],
        }

        violations = skill_boundary_violations("query-public-signals", payload)

        self.assertEqual(
            ["$.matches[0].score", "$.matches[0].recommended_conclusion"],
            [violation["path"] for violation in violations],
        )
        with self.assertRaisesRegex(ValueError, "query-public-signals"):
            validate_skill_output_boundary("query-public-signals", payload)

    def test_boundary_validator_ignores_non_evidence_write_skills(self) -> None:
        payload = {"proposal": {"confidence_score": 0.9, "rank": 1}}

        self.assertEqual(
            [],
            skill_boundary_violations("submit-council-proposal", payload),
        )
        validate_skill_output_boundary("submit-council-proposal", payload)


if __name__ == "__main__":
    unittest.main()

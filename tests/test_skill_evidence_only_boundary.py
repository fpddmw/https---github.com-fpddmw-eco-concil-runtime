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
from eco_council_runtime.kernel.source_queue.source_queue_contract import (  # noqa: E402
    source_capability_hints,
)


SKILLS_ROOT = Path(__file__).resolve().parents[1] / "skills"

P3_HISTORY_HELPER_AND_REPORTING_SKILLS = [
    "archive-case-library",
    "archive-signal-corpus",
    "materialize-history-context",
    "plan-round-orchestration",
    "propose-next-actions",
    "open-falsification-probe",
    "summarize-round-readiness",
    "materialize-reporting-handoff",
    "materialize-spatiotemporal-relation-evidence-packet",
    "draft-council-decision",
    "draft-expert-report",
    "publish-expert-report",
    "publish-council-decision",
    "materialize-final-publication",
]


def skill_doc_text(skill_name: str) -> str:
    return (SKILLS_ROOT / skill_name / "SKILL.md").read_text(encoding="utf-8")


def agent_metadata_text(skill_name: str) -> str:
    return (SKILLS_ROOT / skill_name / "agents" / "openai.yaml").read_text(
        encoding="utf-8"
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

    def test_fetch_normalize_query_and_helper_docs_have_reasoning_guides(self) -> None:
        skill_names = {
            *evidence_only_skill_names(),
            *P3_HISTORY_HELPER_AND_REPORTING_SKILLS,
        }

        for skill_name in sorted(skill_names):
            with self.subTest(skill=skill_name):
                self.assertIn(
                    "\n## Agent Reasoning Guide\n",
                    "\n" + skill_doc_text(skill_name),
                )

    def test_fetch_skill_use_cards_keep_zero_result_discipline(self) -> None:
        for skill_name in FETCH_SKILLS:
            with self.subTest(skill=skill_name):
                card = source_capability_hints(skill_name)["skill_use_card"]
                self.assertEqual("skill-use-card-v1", card["card_version"])

                zero_text = " ".join(card["zero_or_failed_result_discipline"]).lower()
                self.assertIn("not proof", zero_text)
                self.assertIn("revise", zero_text)
                self.assertIn("source-limit rationale", zero_text)

                autonomy_text = str(card["autonomy_boundary"]).lower()
                self.assertIn("not source ranking", autonomy_text)
                self.assertIn("fixed agenda", autonomy_text)

    def test_normalize_docs_explain_no_row_is_not_absence(self) -> None:
        skill_names = [*NORMALIZE_SKILLS, "normalize-fetch-execution"]

        for skill_name in skill_names:
            text = skill_doc_text(skill_name).lower()
            with self.subTest(skill=skill_name):
                self.assertIn("## agent reasoning guide", text)
                self.assertTrue(
                    "not proof" in text or "does not mean" in text,
                    text,
                )
                self.assertTrue("artifact" in text or "receipt" in text, text)
                self.assertTrue(
                    "normalizer" in text or "normalization" in text,
                    text,
                )

                prompt = agent_metadata_text(skill_name).lower()
                self.assertTrue(
                    "no-row" in prompt
                    or "receipt-only" in prompt
                    or "not absence" in prompt,
                    prompt,
                )

    def test_query_docs_explain_empty_result_visibility_limits(self) -> None:
        for skill_name in QUERY_SKILLS:
            raw_text = skill_doc_text(skill_name).lower()
            text = " ".join(raw_text.split())
            with self.subTest(skill=skill_name):
                self.assertIn("## agent reasoning guide", raw_text)
                self.assertIn("empty", text)
                self.assertTrue(
                    "does not prove" in text
                    or "do not prove" in text
                    or "does not mean" in text,
                    text,
                )
                self.assertTrue(
                    "filter" in text
                    or "round_scope" in text
                    or "archive" in text
                    or "provenance" in text,
                    text,
                )

                prompt = agent_metadata_text(skill_name).lower()
                self.assertTrue(
                    "empty" in prompt
                    or "no matches" in prompt
                    or "visibility" in prompt,
                    prompt,
                )

    def test_optional_analysis_docs_require_advisory_uptake(self) -> None:
        for skill_name in OPTIONAL_ANALYSIS_SKILLS:
            raw_text = skill_doc_text(skill_name).lower()
            text = " ".join(raw_text.split())
            with self.subTest(skill=skill_name):
                self.assertIn("## agent reasoning guide", raw_text)
                self.assertTrue(
                    "advisory" in text
                    or "approval-scoped" in text
                    or "navigation material" in text,
                    text,
                )
                self.assertTrue(
                    "before downstream use" in text
                    or "explicitly cites" in text
                    or "explicitly cited" in text
                    or "before it affects" in text,
                    text,
                )
                self.assertTrue(
                    "do not" in text
                    or "does not" in text
                    or "not " in text
                    or "cannot" in text,
                    text,
                )

    def test_history_and_reporting_docs_preserve_non_conclusion_boundaries(self) -> None:
        expectations = {
            "archive-case-library": ["historical context", "not a conclusion"],
            "archive-signal-corpus": [
                "historical traces",
                "not a claim that no source evidence existed",
            ],
            "materialize-history-context": ["not as a current-run conclusion"],
            "open-falsification-probe": ["not as proof"],
            "summarize-round-readiness": ["cannot move a", "advisory"],
            "materialize-reporting-handoff": ["does not reopen", "helper cues"],
            "materialize-spatiotemporal-relation-evidence-packet": [
                "not transport proof",
                "causality proof",
            ],
            "draft-council-decision": ["not a new investigation"],
            "draft-expert-report": ["must not add new findings"],
            "publish-expert-report": ["does not advance investigation"],
            "publish-council-decision": ["does not reopen investigation"],
            "materialize-final-publication": [
                "does not create new investigation conclusions"
            ],
        }

        for skill_name, required_phrases in expectations.items():
            raw_text = skill_doc_text(skill_name).lower()
            text = " ".join(raw_text.split())
            with self.subTest(skill=skill_name):
                self.assertIn("## agent reasoning guide", raw_text)
                for phrase in required_phrases:
                    self.assertIn(phrase, text)


if __name__ == "__main__":
    unittest.main()

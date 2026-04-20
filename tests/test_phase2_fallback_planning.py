from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime import phase2_fallback_common  # noqa: E402
from eco_council_runtime import phase2_fallback_context  # noqa: E402
from eco_council_runtime import phase2_fallback_agenda  # noqa: E402
from eco_council_runtime import phase2_fallback_agenda_profile  # noqa: E402
from eco_council_runtime import phase2_fallback_contracts  # noqa: E402
from eco_council_runtime import phase2_fallback_policy  # noqa: E402
from eco_council_runtime import phase2_fallback_planning  # noqa: E402
from eco_council_runtime.kernel import investigation_planning  # noqa: E402

COMMON_EXPORT_NAMES = (
    "maybe_text",
    "resolve_path",
    "unique_texts",
)

CONTRACT_EXPORT_NAMES = (
    "d1_contract_fields",
    "d1_contract_fields_from_payload",
    "normalize_d1_observed_inputs",
)

CONTEXT_EXPORT_NAMES = (
    "load_d1_shared_context",
    "load_ranked_actions_context",
)

POLICY_EXPORT_NAMES = (
    "DEFAULT_FALLBACK_POLICY_PROFILE",
    "fallback_policy_annotation",
)

AGENDA_PROFILE_EXPORT_NAMES = (
    "agenda_action",
    "build_fallback_agenda_context",
    "default_fallback_agenda_profile",
    "fallback_agenda_source",
)

STATE_WRAPPER_NAMES = (
    "load_next_actions_wrapper",
    "load_falsification_probe_wrapper",
    "load_round_readiness_wrapper",
)


class Phase2FallbackPlanningTests(unittest.TestCase):
    def test_split_modules_own_phase2_fallback_exports(self) -> None:
        for name in COMMON_EXPORT_NAMES:
            self.assertTrue(hasattr(phase2_fallback_common, name), name)
        for name in CONTRACT_EXPORT_NAMES:
            self.assertTrue(hasattr(phase2_fallback_contracts, name), name)
        for name in CONTEXT_EXPORT_NAMES:
            self.assertTrue(hasattr(phase2_fallback_context, name), name)
        for name in POLICY_EXPORT_NAMES:
            self.assertTrue(hasattr(phase2_fallback_policy, name), name)
        for name in AGENDA_PROFILE_EXPORT_NAMES:
            self.assertTrue(hasattr(phase2_fallback_agenda_profile, name), name)

    def test_compatibility_facade_reexports_split_modules(self) -> None:
        export_names = (
            COMMON_EXPORT_NAMES
            + CONTRACT_EXPORT_NAMES
            + CONTEXT_EXPORT_NAMES
            + POLICY_EXPORT_NAMES
            + AGENDA_PROFILE_EXPORT_NAMES
        )
        for name in export_names:
            self.assertIs(
                getattr(phase2_fallback_planning, name),
                getattr(
                    phase2_fallback_common
                    if name in COMMON_EXPORT_NAMES
                    else phase2_fallback_contracts
                    if name in CONTRACT_EXPORT_NAMES
                    else phase2_fallback_context
                    if name in CONTEXT_EXPORT_NAMES
                    else phase2_fallback_policy,
                    name,
                )
                if name not in AGENDA_PROFILE_EXPORT_NAMES
                else getattr(
                    phase2_fallback_agenda_profile,
                    name,
                ),
                name,
            )

    def test_investigation_planning_reexports_split_phase2_helpers(self) -> None:
        export_names = (
            COMMON_EXPORT_NAMES
            + CONTRACT_EXPORT_NAMES
            + CONTEXT_EXPORT_NAMES
            + POLICY_EXPORT_NAMES
            + AGENDA_PROFILE_EXPORT_NAMES
        )
        for name in export_names:
            self.assertIs(getattr(investigation_planning, name), getattr(phase2_fallback_planning, name), name)

    def test_fallback_module_does_not_own_state_surface_wrappers(self) -> None:
        for name in STATE_WRAPPER_NAMES:
            self.assertFalse(hasattr(phase2_fallback_planning, name), name)

    def test_agenda_engine_reexports_action_mappers_from_profile_module(self) -> None:
        self.assertEqual(
            "eco_council_runtime.phase2_fallback_agenda_profile",
            phase2_fallback_agenda.action_from_issue_cluster.__module__,
        )
        self.assertEqual(
            "eco_council_runtime.phase2_fallback_agenda_profile",
            phase2_fallback_agenda.default_fallback_agenda_profile.__module__,
        )

    def test_build_actions_accepts_injected_agenda_profile(self) -> None:
        custom_profile = {
            "source_specs": [
                phase2_fallback_agenda_profile.fallback_agenda_source(
                    "custom-issues",
                    rows=lambda context: context.get("issue_clusters", []),
                    build_action=lambda issue, context: phase2_fallback_agenda_profile.agenda_action(
                        action_id="custom-action-001",
                        action_kind="custom-route-review",
                        priority="high",
                        assigned_role="moderator",
                        objective=f"Custom review for {issue['map_issue_id']}.",
                        reason="Injected agenda profile should own issue-to-action mapping.",
                        source_ids=[issue["map_issue_id"]],
                        target={"map_issue_id": issue["map_issue_id"]},
                        controversy_gap="custom-gap",
                        recommended_lane="custom-lane",
                        expected_outcome="Custom profile decides the next step.",
                        evidence_refs=[],
                        probe_candidate=False,
                        agenda_source="custom-profile",
                    ),
                )
            ],
            "empty_action_builder": lambda context: [],
        }

        actions = phase2_fallback_agenda.build_actions(
            {"open_challenges": [], "open_tasks": [], "active_hypotheses": []},
            [],
            [
                {
                    "map_issue_id": "issue-001",
                    "claim_ids": ["claim-001"],
                    "recommended_lane": "mixed-review",
                }
            ],
            [],
            [],
            [],
            [],
            [],
            "Custom brief",
            agenda_profile=custom_profile,
        )

        self.assertEqual(1, len(actions))
        self.assertEqual("custom-route-review", actions[0]["action_kind"])
        self.assertEqual("custom-profile", actions[0]["agenda_source"])

    def test_default_empty_agenda_action_is_non_blocking_readiness_review(self) -> None:
        actions = phase2_fallback_agenda_profile.default_empty_agenda_actions(
            {
                "coverages": [
                    {
                        "coverage_id": "coverage-001",
                        "claim_id": "claim-001",
                        "readiness": "strong",
                        "coverage_score": 0.91,
                        "evidence_refs": ["artifact:coverage-001"],
                    }
                ]
            }
        )

        self.assertEqual(1, len(actions))
        self.assertEqual("open-council-readiness-review", actions[0]["action_kind"])
        self.assertFalse(actions[0]["readiness_blocker"])


if __name__ == "__main__":
    unittest.main()

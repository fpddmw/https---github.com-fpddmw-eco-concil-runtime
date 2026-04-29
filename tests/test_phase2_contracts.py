from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime import phase2_stage_profile  # noqa: E402
from eco_council_runtime.kernel.phase2_contract import validate_stage_blueprints, validate_stage_sequence  # noqa: E402


class Phase2ContractTests(unittest.TestCase):
    def test_kernel_phase2_contract_is_compatibility_facade(self) -> None:
        self.assertIs(validate_stage_sequence, phase2_stage_profile.validate_stage_sequence)
        self.assertIs(validate_stage_blueprints, phase2_stage_profile.validate_stage_blueprints)

    def test_contract_allows_agent_selected_readiness_without_next_actions(self) -> None:
        validate_stage_sequence(
            [
                "orchestration-planner",
                "round-readiness",
                "report-basis-gate",
                "report-basis-freeze",
            ]
        )

    def test_contract_still_requires_gate_after_readiness(self) -> None:
        with self.assertRaises(ValueError):
            validate_stage_sequence(
                [
                    "orchestration-planner",
                    "report-basis-gate",
                    "round-readiness",
                    "report-basis-freeze",
                ]
            )

    def test_explicit_stage_dependencies_can_override_known_stage_defaults(self) -> None:
        validate_stage_blueprints(
            [
                {"stage": "orchestration-planner"},
                {
                    "stage": "custom-readiness-review",
                    "required_previous_stages": ["orchestration-planner"],
                },
                {
                    "stage": "report-basis-gate",
                    "required_previous_stages": ["custom-readiness-review"],
                },
                {
                    "stage": "report-basis-freeze",
                    "required_previous_stages": ["report-basis-gate"],
                },
            ]
        )


if __name__ == "__main__":
    unittest.main()

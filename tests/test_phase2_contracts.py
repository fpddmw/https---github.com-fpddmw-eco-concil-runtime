from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.phase2_contract import validate_stage_sequence  # noqa: E402


class Phase2ContractTests(unittest.TestCase):
    def test_contract_allows_agent_selected_readiness_without_next_actions(self) -> None:
        validate_stage_sequence(
            [
                "orchestration-planner",
                "round-readiness",
                "promotion-gate",
                "promotion-basis",
            ]
        )

    def test_contract_still_requires_gate_after_readiness(self) -> None:
        with self.assertRaises(ValueError):
            validate_stage_sequence(
                [
                    "orchestration-planner",
                    "promotion-gate",
                    "round-readiness",
                    "promotion-basis",
                ]
            )


if __name__ == "__main__":
    unittest.main()

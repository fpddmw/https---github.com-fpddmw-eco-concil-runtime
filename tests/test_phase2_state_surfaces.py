from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel import investigation_planning, phase2_state_surfaces  # noqa: E402

WRAPPER_NAMES = (
    "load_next_actions_wrapper",
    "load_falsification_probe_wrapper",
    "load_round_readiness_wrapper",
    "load_promotion_basis_wrapper",
    "load_supervisor_state_wrapper",
    "load_reporting_handoff_wrapper",
    "load_council_decision_wrapper",
    "load_expert_report_wrapper",
    "load_final_publication_wrapper",
)


class Phase2StateSurfaceTests(unittest.TestCase):
    def test_phase2_state_surfaces_exports_all_phase2_wrappers(self) -> None:
        exported = set(phase2_state_surfaces.__all__)
        for name in WRAPPER_NAMES:
            self.assertIn(name, exported)
        self.assertIn("build_reporting_surface", exported)

    def test_investigation_planning_reexports_phase2_surface_wrappers(self) -> None:
        for name in WRAPPER_NAMES:
            self.assertIs(
                getattr(investigation_planning, name),
                getattr(phase2_state_surfaces, name),
                name,
            )


if __name__ == "__main__":
    unittest.main()

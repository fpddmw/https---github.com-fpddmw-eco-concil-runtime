from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime import phase2_gate_handlers  # noqa: E402
from eco_council_runtime.phase2_gate_profile import phase2_gate_handler_registry  # noqa: E402
from eco_council_runtime.kernel.gate import gate_handler_registry  # noqa: E402


class Phase2GateHandlerTests(unittest.TestCase):
    def test_kernel_gate_registry_has_no_builtin_domain_handlers(self) -> None:
        self.assertEqual({}, gate_handler_registry())

    def test_phase2_profile_owns_default_promotion_gate_handler(self) -> None:
        registry = phase2_gate_handler_registry()
        self.assertIs(registry["promotion-gate"], phase2_gate_handlers.apply_promotion_gate)

    def test_handler_module_no_longer_owns_phase2_default_registry(self) -> None:
        self.assertFalse(hasattr(phase2_gate_handlers, "phase2_gate_handler_registry"))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.execution import runtime_gate_handlers  # noqa: E402
from eco_council_runtime.kernel.execution.runtime_gate_profile import runtime_gate_handler_registry  # noqa: E402
from eco_council_runtime.kernel.execution.gate import gate_handler_registry  # noqa: E402


class GovernedExecutionGateHandlerTests(unittest.TestCase):
    def test_kernel_gate_registry_has_no_builtin_domain_handlers(self) -> None:
        self.assertEqual({}, gate_handler_registry())

    def test_governed_execution_profile_owns_default_report_basis_gate_handler(self) -> None:
        registry = runtime_gate_handler_registry()
        self.assertIs(registry["report-basis-gate"], runtime_gate_handlers.apply_report_basis_gate)
        self.assertIs(registry["report-basis-gate"], runtime_gate_handlers.apply_report_basis_gate)

    def test_handler_module_no_longer_owns_governed_execution_default_registry(self) -> None:
        self.assertFalse(hasattr(runtime_gate_handlers, "runtime_gate_handler_registry"))


if __name__ == "__main__":
    unittest.main()

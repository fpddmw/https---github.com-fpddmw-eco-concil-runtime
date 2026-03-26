from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.cli_invocation import (  # noqa: E402
    runtime_module_argv,
    runtime_module_command,
    runtime_module_name,
)


class CliInvocationTests(unittest.TestCase):
    def test_runtime_module_name_uses_package_entry(self) -> None:
        self.assertEqual("eco_council_runtime.reporting", runtime_module_name("reporting"))

    def test_runtime_module_argv_uses_current_python(self) -> None:
        argv = runtime_module_argv("contract", "validate", "--kind", "expert-report")
        self.assertEqual(sys.executable, argv[0])
        self.assertEqual(["-m", "eco_council_runtime.contract"], argv[1:3])
        self.assertEqual(["validate", "--kind", "expert-report"], argv[3:])

    def test_runtime_module_command_renders_shell_safe_text(self) -> None:
        command = runtime_module_command(
            "reporting",
            "promote-decision-draft",
            "--run-dir",
            Path("/tmp/eco council"),
        )
        self.assertIn("eco_council_runtime.reporting", command)
        self.assertIn("'/tmp/eco council'", command)


if __name__ == "__main__":
    unittest.main()

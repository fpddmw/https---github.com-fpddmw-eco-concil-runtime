from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class CliModuleImportTests(unittest.TestCase):
    def test_top_level_cli_modules_import_cleanly(self) -> None:
        module_names = [
            "eco_council_runtime.case_library",
            "eco_council_runtime.contract",
            "eco_council_runtime.eval",
            "eco_council_runtime.normalize",
            "eco_council_runtime.orchestrate",
            "eco_council_runtime.reporting",
            "eco_council_runtime.signal_corpus",
            "eco_council_runtime.simulate",
            "eco_council_runtime.supervisor",
        ]
        for module_name in module_names:
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)
                self.assertTrue(hasattr(module, "main"))


if __name__ == "__main__":
    unittest.main()

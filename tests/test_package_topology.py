from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class PackageTopologyTests(unittest.TestCase):
    def test_target_layer_packages_import_cleanly(self) -> None:
        module_names = [
            "eco_council_runtime.domain",
            "eco_council_runtime.application",
            "eco_council_runtime.adapters",
            "eco_council_runtime.cli",
        ]
        for module_name in module_names:
            with self.subTest(module=module_name):
                module = importlib.import_module(module_name)
                module_path = Path(module.__file__).resolve()
                self.assertEqual("__init__.py", module_path.name)
                self.assertEqual(module_name.rsplit(".", 1)[-1], module_path.parent.name)

    def test_controller_package_is_marked_as_transitional(self) -> None:
        module = importlib.import_module("eco_council_runtime.controller")
        self.assertIn("transitional", (module.__doc__ or "").lower())
        self.assertIn("domain", (module.__doc__ or "").lower())


if __name__ == "__main__":
    unittest.main()

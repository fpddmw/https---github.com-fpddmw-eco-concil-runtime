from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "src"))


def read_repo_text(relative_path: str) -> str:
    return (ROOT_DIR / relative_path).read_text(encoding="utf-8")


class ImportBoundaryTests(unittest.TestCase):
    def test_boundary_support_modules_import_cleanly(self) -> None:
        archive_state = importlib.import_module("eco_council_runtime.application.archive.runtime_state")
        contract_support = importlib.import_module("eco_council_runtime.application.contract.runtime_support")
        self.assertTrue(hasattr(archive_state, "collect_run_snapshot"))
        self.assertTrue(hasattr(contract_support, "validate_payload"))

    def test_root_runtime_facades_remain_thin_wrapper_files(self) -> None:
        expected_owners = {
            "src/eco_council_runtime/reporting.py": "eco_council_runtime.application.reporting.runtime_cli",
            "src/eco_council_runtime/orchestrate.py": "eco_council_runtime.application.orchestration.runtime_cli",
            "src/eco_council_runtime/supervisor.py": "eco_council_runtime.application.supervisor.runtime_cli",
        }
        for relative_path, owner_module in expected_owners.items():
            with self.subTest(path=relative_path):
                text = read_repo_text(relative_path)
                nonempty_lines = [line for line in text.splitlines() if line.strip()]
                self.assertLessEqual(len(nonempty_lines), 10)
                self.assertIn(f"from {owner_module} import *", text)
                self.assertIn(f"from {owner_module} import main", text)

    def test_root_runtime_facades_preserve_owner_main_identity(self) -> None:
        module_pairs = [
            ("eco_council_runtime.reporting", "eco_council_runtime.application.reporting.runtime_cli"),
            ("eco_council_runtime.orchestrate", "eco_council_runtime.application.orchestration.runtime_cli"),
            ("eco_council_runtime.supervisor", "eco_council_runtime.application.supervisor.runtime_cli"),
        ]
        for public_module_name, owner_module_name in module_pairs:
            with self.subTest(module=public_module_name):
                public_module = importlib.import_module(public_module_name)
                owner_module = importlib.import_module(owner_module_name)
                self.assertIs(public_module.main, owner_module.main)

    def test_cycle_prone_package_surfaces_stay_lazy(self) -> None:
        package_paths = {
            "src/eco_council_runtime/application/archive/__init__.py": [
                "from .importers import",
                "from .runtime_state import",
            ],
            "src/eco_council_runtime/application/reporting/__init__.py": [],
            "src/eco_council_runtime/application/supervisor/__init__.py": [
                "from eco_council_runtime.application.supervisor.runtime_cli import",
            ],
        }
        for relative_path, banned_imports in package_paths.items():
            with self.subTest(path=relative_path):
                text = read_repo_text(relative_path)
                self.assertIn("import_module", text)
                self.assertIn("__getattr__", text)
                for banned_import in banned_imports:
                    self.assertNotIn(banned_import, text)

    def test_contract_runtime_avoids_direct_root_contract_import(self) -> None:
        text = read_repo_text("src/eco_council_runtime/application/contract_runtime.py")
        self.assertIn("application.contract.runtime_support", text)
        self.assertNotIn("from eco_council_runtime import contract as contract_module", text)

    def test_archive_entrypoints_avoid_root_supervisor_import(self) -> None:
        for relative_path in (
            "src/eco_council_runtime/signal_corpus.py",
            "src/eco_council_runtime/case_library.py",
        ):
            with self.subTest(path=relative_path):
                text = read_repo_text(relative_path)
                self.assertIn("application.archive.runtime_state", text)
                self.assertNotIn("load_supervisor_module", text)
                self.assertNotIn("from eco_council_runtime import supervisor", text)

    def test_docs_stop_describing_old_cli_boundary_as_final(self) -> None:
        readme = read_repo_text("README.md")
        controller_cli = read_repo_text("src/eco_council_runtime/controller/cli.py")
        self.assertNotIn("cli.py`: thin supervisor CLI assembly", readme)
        self.assertIn("compatibility wrapper", readme)
        self.assertNotIn("during T06 migration", controller_cli)


if __name__ == "__main__":
    unittest.main()

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

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.archive import (  # noqa: E402
    case_library_argv,
    maybe_auto_import_case_library,
    maybe_auto_import_signal_corpus,
    signal_corpus_argv,
)


class ArchiveExtractedModuleTests(unittest.TestCase):
    def test_archive_import_helpers_build_runtime_module_argv(self) -> None:
        self.assertEqual(["-m", "eco_council_runtime.case_library", "import-run"], case_library_argv("import-run")[-3:])
        self.assertEqual(["-m", "eco_council_runtime.signal_corpus", "import-run"], signal_corpus_argv("import-run")[-3:])

    def test_archive_signal_corpus_import_updates_state(self) -> None:
        state = {
            "signal_corpus": {
                "db": "/tmp/signal-corpus.db",
                "auto_import": True,
            }
        }

        with patch(
            "eco_council_runtime.application.archive.importers.run_json_command",
            return_value={"payload": {"imported_runs": 1}},
        ):
            result = maybe_auto_import_signal_corpus(Path("/tmp/eco-run"), state, "round-002")

        self.assertTrue(result["ok"])
        self.assertEqual("round-002", state["signal_corpus"]["last_imported_round_id"])
        self.assertEqual(1, state["signal_corpus"]["last_import"]["import_result"]["imported_runs"])

    def test_archive_case_library_import_updates_state(self) -> None:
        state = {
            "case_library_archive": {
                "db": "/tmp/case-library.db",
                "auto_import": True,
            }
        }

        with patch(
            "eco_council_runtime.application.archive.importers.run_json_command",
            return_value={"payload": {"imported_cases": 3}},
        ):
            result = maybe_auto_import_case_library(Path("/tmp/eco-run"), state, "round-005")

        self.assertTrue(result["ok"])
        self.assertEqual("round-005", state["case_library_archive"]["last_imported_round_id"])
        self.assertEqual(3, state["case_library_archive"]["last_import"]["import_result"]["imported_cases"])


if __name__ == "__main__":
    unittest.main()
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.cli.supervisor_cli import build_supervisor_parser  # noqa: E402
from eco_council_runtime.controller.cli import build_supervisor_parser as build_controller_supervisor_parser  # noqa: E402


class SupervisorCliTests(unittest.TestCase):
    def test_cli_supervisor_parser_parses_init_run_arguments(self) -> None:
        parser = build_supervisor_parser()
        args = parser.parse_args(
            [
                "init-run",
                "--run-dir",
                "/tmp/run",
                "--mission-input",
                "/tmp/mission.json",
                "--history-top-k",
                "5",
            ]
        )

        self.assertEqual("init-run", args.command)
        self.assertEqual("/tmp/run", args.run_dir)
        self.assertEqual(5, args.history_top_k)

    def test_controller_cli_remains_compatibility_wrapper(self) -> None:
        parser = build_controller_supervisor_parser()
        args = parser.parse_args(
            [
                "import-fetch-execution",
                "--run-dir",
                "/tmp/run",
            ]
        )

        self.assertEqual("import-fetch-execution", args.command)
        self.assertEqual("", args.input)


if __name__ == "__main__":
    unittest.main()

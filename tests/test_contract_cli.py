from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.cli.contract_cli import build_contract_parser, run_contract_cli  # noqa: E402


class ContractCliTests(unittest.TestCase):
    def test_build_contract_parser_parses_scaffold_run_from_mission_options(self) -> None:
        parser = build_contract_parser()
        args = parser.parse_args(
            [
                "scaffold-run-from-mission",
                "--run-dir",
                "/tmp/run",
                "--mission-input",
                "/tmp/mission.json",
            ]
        )

        self.assertEqual("scaffold-run-from-mission", args.command)
        self.assertEqual("", args.tasks_input)

    def test_run_contract_cli_dispatches_write_example_command(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "mission.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = run_contract_cli(
                    [
                        "write-example",
                        "--kind",
                        "mission",
                        "--output",
                        str(output_path),
                    ]
                )

            self.assertEqual(0, exit_code)
            self.assertTrue(output_path.exists())
            payload = json.loads(stdout.getvalue())
            self.assertTrue(payload["ok"])
            self.assertEqual("write-example", payload["command"])


if __name__ == "__main__":
    unittest.main()

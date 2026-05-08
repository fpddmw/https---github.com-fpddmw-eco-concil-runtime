from __future__ import annotations

import json
import subprocess
import sys
import unittest

from _workflow_support import script_path


class NasaFirmsFetchSkillTests(unittest.TestCase):
    def test_fetch_accepts_negative_leading_bbox_token(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(script_path("fetch-nasa-firms-fire")),
                "fetch",
                "--source",
                "MODIS_NRT",
                "--bbox",
                "-90,40,-60,55",
                "--start-date",
                "2023-06-01",
                "--end-date",
                "2023-06-01",
                "--dry-run",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["dry_run"])
        self.assertEqual(-90.0, payload["request"]["bbox"]["west"])
        self.assertEqual(-60.0, payload["request"]["bbox"]["east"])


if __name__ == "__main__":
    unittest.main()

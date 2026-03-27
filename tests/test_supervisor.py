from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.controller.constants import STAGE_AWAITING_REPORTS  # noqa: E402
from eco_council_runtime.supervisor import continue_run_matching_adjudication  # noqa: E402


class SupervisorFlowTests(unittest.TestCase):
    def test_continue_run_matching_adjudication_moves_directly_to_reports(self) -> None:
        state = {
            "current_round_id": "round-02",
            "stage": "ready-to-run-matching-adjudication",
            "imports": {},
        }

        with (
            patch("eco_council_runtime.supervisor.run_json_command", return_value={"matching": "ok"}) as run_json_command,
            patch("eco_council_runtime.supervisor.save_state") as save_state,
            patch(
                "eco_council_runtime.supervisor.build_status_payload",
                side_effect=lambda _run_dir, current_state: {"stage": current_state["stage"]},
            ),
        ):
            result = continue_run_matching_adjudication(Path("/tmp/eco-council-runtime"), state)

        self.assertEqual(STAGE_AWAITING_REPORTS, state["stage"])
        self.assertTrue(state["imports"]["matching_authorization_received"])
        self.assertTrue(state["imports"]["matching_adjudication_received"])
        self.assertTrue(state["imports"]["investigation_review_received"])
        self.assertEqual([], state["imports"]["report_roles_received"])
        self.assertEqual(STAGE_AWAITING_REPORTS, result["state"]["stage"])
        self.assertEqual("run-matching-adjudication", result["action"])
        run_json_command.assert_called_once()
        save_state.assert_called_once()


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.supervisor_lifecycle import (  # noqa: E402
    continue_promote,
    continue_recover_or_run_matching_adjudication,
    maybe_auto_import_signal_corpus,
    save_state,
)
from eco_council_runtime.controller.constants import STAGE_AWAITING_REPORTS, STAGE_READY_ADVANCE  # noqa: E402
from eco_council_runtime.controller.paths import decision_target_path, matching_execution_path, supervisor_state_path  # noqa: E402


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class SupervisorLifecycleTests(unittest.TestCase):
    def test_save_state_writes_updated_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            state = {
                "current_round_id": "round-001",
                "stage": "awaiting-task-review",
            }

            with patch(
                "eco_council_runtime.application.supervisor_lifecycle.refresh_supervisor_files",
                return_value=None,
            ):
                save_state(run_dir, state)

            payload = json.loads(supervisor_state_path(run_dir).read_text(encoding="utf-8"))
            self.assertIn("updated_at_utc", payload)
            self.assertEqual("round-001", payload["current_round_id"])

    def test_maybe_auto_import_signal_corpus_records_success(self) -> None:
        state = {
            "signal_corpus": {
                "db": "/tmp/signal-corpus.db",
                "auto_import": True,
            }
        }

        with patch(
            "eco_council_runtime.application.supervisor_lifecycle.run_json_command",
            return_value={"payload": {"imported_runs": 1}},
        ):
            result = maybe_auto_import_signal_corpus(Path("/tmp/eco-run"), state, "round-002")

        self.assertTrue(result["ok"])
        self.assertEqual("round-002", state["signal_corpus"]["last_imported_round_id"])
        self.assertEqual(1, state["signal_corpus"]["last_import"]["import_result"]["imported_runs"])

    def test_continue_recover_or_run_matching_adjudication_reuses_existing_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            state = {
                "current_round_id": "round-003",
                "stage": "ready-to-run-matching-adjudication",
                "imports": {},
            }
            payload = {
                "round_id": "round-003",
                "statuses": [],
            }
            write_json(matching_execution_path(run_dir, "round-003"), payload)

            with (
                patch(
                    "eco_council_runtime.application.supervisor_lifecycle.ensure_matching_execution_matches",
                    return_value=None,
                ),
                patch("eco_council_runtime.application.supervisor_lifecycle.save_state") as save_state_mock,
                patch(
                    "eco_council_runtime.application.supervisor_lifecycle.build_status_payload",
                    side_effect=lambda _run_dir, current_state: {"stage": current_state["stage"]},
                ),
            ):
                result = continue_recover_or_run_matching_adjudication(run_dir, state)

        self.assertEqual(STAGE_AWAITING_REPORTS, state["stage"])
        self.assertTrue(result["reused_existing_execution"])
        self.assertEqual("reuse-matching-adjudication-execution", result["action"])
        save_state_mock.assert_called_once()

    def test_continue_promote_moves_to_ready_advance_when_decision_requires_next_round(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            state = {
                "current_round_id": "round-004",
                "stage": "ready-to-promote",
                "imports": {},
            }
            write_json(
                decision_target_path(run_dir, "round-004"),
                {
                    "next_round_required": True,
                },
            )

            with (
                patch(
                    "eco_council_runtime.application.supervisor_lifecycle.run_json_command",
                    return_value={"payload": {"promoted": True}},
                ),
                patch(
                    "eco_council_runtime.application.supervisor_lifecycle.maybe_auto_import_case_library",
                    return_value={"attempted": False},
                ),
                patch("eco_council_runtime.application.supervisor_lifecycle.save_state") as save_state_mock,
                patch(
                    "eco_council_runtime.application.supervisor_lifecycle.build_status_payload",
                    side_effect=lambda _run_dir, current_state: {"stage": current_state["stage"]},
                ),
            ):
                result = continue_promote(run_dir, state)

        self.assertEqual(STAGE_READY_ADVANCE, state["stage"])
        self.assertEqual("promote-all", result["action"])
        self.assertEqual({"attempted": False}, result["payload"]["case_library_import"])
        self.assertEqual(2, save_state_mock.call_count)


if __name__ == "__main__":
    unittest.main()

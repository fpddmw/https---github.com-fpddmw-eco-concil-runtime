from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.investigation_planning import (  # noqa: E402
    d1_contract_fields_from_payload,
    normalize_d1_observed_inputs,
)


class InvestigationContractTests(unittest.TestCase):
    def test_normalize_d1_observed_inputs_preserves_explicit_artifact_flags(self) -> None:
        normalized = normalize_d1_observed_inputs(
            {
                "board_summary_artifact_present": True,
                "board_summary_present": False,
                "board_brief_present": True,
                "custom_flag": "kept",
            }
        )

        self.assertTrue(normalized["board_summary_artifact_present"])
        self.assertFalse(normalized["board_summary_present"])
        self.assertTrue(normalized["board_brief_artifact_present"])
        self.assertTrue(normalized["board_brief_present"])
        self.assertEqual("kept", normalized["custom_flag"])
        self.assertFalse(normalized["next_actions_artifact_present"])
        self.assertFalse(normalized["probes_artifact_present"])

    def test_contract_fields_from_payload_backfills_new_flags_from_legacy_payload(self) -> None:
        fields = d1_contract_fields_from_payload(
            {
                "board_state_source": "deliberation-plane",
                "coverage_source": "analysis-plane",
                "db_path": "/tmp/signal_plane.sqlite",
                "deliberation_sync": {"status": "completed"},
                "analysis_sync": {"status": "completed"},
                "observed_inputs": {
                    "board_summary_present": False,
                    "coverage_present": True,
                    "next_actions_present": True,
                },
            },
            observed_inputs_overrides={
                "next_actions_artifact_present": True,
                "next_actions_present": False,
            },
        )

        self.assertEqual("deliberation-plane", fields["board_state_source"])
        self.assertEqual("analysis-plane", fields["coverage_source"])
        self.assertEqual("/tmp/signal_plane.sqlite", fields["db_path"])
        self.assertEqual("completed", fields["deliberation_sync"]["status"])
        self.assertEqual("completed", fields["analysis_sync"]["status"])
        self.assertFalse(fields["observed_inputs"]["board_summary_artifact_present"])
        self.assertFalse(fields["observed_inputs"]["board_summary_present"])
        self.assertTrue(fields["observed_inputs"]["coverage_present"])
        self.assertTrue(fields["observed_inputs"]["next_actions_artifact_present"])
        self.assertFalse(fields["observed_inputs"]["next_actions_present"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    normalize_reporting_observed_inputs,
    reporting_contract_fields_from_payload,
)


class ReportingContractTests(unittest.TestCase):
    def test_normalize_reporting_observed_inputs_preserves_explicit_artifact_flags(self) -> None:
        normalized = normalize_reporting_observed_inputs(
            {
                "readiness_artifact_present": True,
                "readiness_present": False,
                "promotion_present": True,
                "expert_report_draft_present": True,
                "custom_flag": "kept",
            }
        )

        self.assertTrue(normalized["readiness_artifact_present"])
        self.assertFalse(normalized["readiness_present"])
        self.assertTrue(normalized["promotion_artifact_present"])
        self.assertTrue(normalized["promotion_present"])
        self.assertTrue(normalized["expert_report_draft_artifact_present"])
        self.assertTrue(normalized["expert_report_draft_present"])
        self.assertEqual("kept", normalized["custom_flag"])
        self.assertFalse(normalized["board_summary_artifact_present"])
        self.assertNotIn("decision_artifact_present", normalized)

    def test_contract_fields_from_payload_merges_fallback_contract_and_overrides(self) -> None:
        fields = reporting_contract_fields_from_payload(
            {
                "promotion_source": "promotion-artifact",
                "observed_inputs": {
                    "promotion_present": True,
                },
            },
            fallback_payload={
                "board_state_source": "deliberation-plane",
                "coverage_source": "analysis-plane",
                "db_path": "/tmp/signal_plane.sqlite",
                "deliberation_sync": {"status": "completed"},
                "analysis_sync": {"status": "completed"},
                "readiness_source": "round-readiness-artifact",
                "observed_inputs": {
                    "coverage_present": True,
                    "readiness_present": True,
                },
            },
            observed_inputs_overrides={
                "reporting_handoff_artifact_present": True,
                "reporting_handoff_present": False,
            },
            field_overrides={
                "reporting_handoff_source": "reporting-handoff-artifact",
            },
        )

        self.assertEqual("deliberation-plane", fields["board_state_source"])
        self.assertEqual("analysis-plane", fields["coverage_source"])
        self.assertEqual("/tmp/signal_plane.sqlite", fields["db_path"])
        self.assertEqual("completed", fields["deliberation_sync"]["status"])
        self.assertEqual("completed", fields["analysis_sync"]["status"])
        self.assertEqual("promotion-artifact", fields["promotion_source"])
        self.assertEqual("round-readiness-artifact", fields["readiness_source"])
        self.assertEqual(
            "reporting-handoff-artifact",
            fields["reporting_handoff_source"],
        )
        self.assertTrue(fields["observed_inputs"]["promotion_artifact_present"])
        self.assertTrue(fields["observed_inputs"]["promotion_present"])
        self.assertTrue(fields["observed_inputs"]["coverage_present"])
        self.assertTrue(fields["observed_inputs"]["readiness_artifact_present"])
        self.assertTrue(fields["observed_inputs"]["readiness_present"])
        self.assertTrue(
            fields["observed_inputs"]["reporting_handoff_artifact_present"]
        )
        self.assertFalse(fields["observed_inputs"]["reporting_handoff_present"])


if __name__ == "__main__":
    unittest.main()

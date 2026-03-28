from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.controller.paths import (  # noqa: E402
    claim_candidates_path,
    claim_curation_draft_path,
    claim_submissions_path,
    default_context_dir,
    evidence_library_dir,
    observation_curation_draft_path,
    observation_submissions_path,
    role_context_path,
    round_dir,
    round_dir_name,
    shared_evidence_cards_path,
    shared_evidence_path,
)


class ControllerPathsTests(unittest.TestCase):
    def test_round_dir_name_normalizes_standard_round_ids(self) -> None:
        self.assertEqual("round_007", round_dir_name("round-007"))
        self.assertEqual("round_007", round_dir_name("round_007"))

    def test_round_dir_name_preserves_legacy_fallback_behavior(self) -> None:
        self.assertEqual("custom_round", round_dir_name("custom-round"))

    def test_round_dir_uses_normalized_directory_name(self) -> None:
        self.assertEqual(
            Path("/tmp/run/round_003"),
            round_dir(Path("/tmp/run"), "round-003"),
        )

    def test_reporting_and_normalize_specific_paths_share_same_base_layer(self) -> None:
        run_dir = Path("/tmp/run")
        round_id = "round-004"

        self.assertEqual(
            Path("/tmp/run/round_004/sociologist/normalized/claim_candidates.json"),
            claim_candidates_path(run_dir, round_id),
        )
        self.assertEqual(
            Path("/tmp/run/round_004/sociologist/derived"),
            default_context_dir(run_dir, round_id, "sociologist"),
        )
        self.assertEqual(
            Path("/tmp/run/round_004/sociologist/derived/context_sociologist.json"),
            role_context_path(run_dir, round_id, "sociologist"),
        )
        self.assertEqual(
            Path("/tmp/run/round_004/shared/evidence-library"),
            evidence_library_dir(run_dir, round_id),
        )

    def test_shared_evidence_alias_matches_controller_name(self) -> None:
        run_dir = Path("/tmp/run")
        round_id = "round-005"
        self.assertEqual(
            shared_evidence_cards_path(run_dir, round_id),
            shared_evidence_path(run_dir, round_id),
        )

    def test_curation_draft_paths_exist_in_shared_controller_layer(self) -> None:
        run_dir = Path("/tmp/run")
        round_id = "round-006"

        self.assertEqual(
            Path("/tmp/run/round_006/sociologist/derived/claim_curation_draft.json"),
            claim_curation_draft_path(run_dir, round_id),
        )
        self.assertEqual(
            Path("/tmp/run/round_006/environmentalist/derived/observation_curation_draft.json"),
            observation_curation_draft_path(run_dir, round_id),
        )

    def test_submission_paths_exist_in_shared_controller_layer(self) -> None:
        run_dir = Path("/tmp/run")
        round_id = "round-007"

        self.assertEqual(
            Path("/tmp/run/round_007/sociologist/claim_submissions.json"),
            claim_submissions_path(run_dir, round_id),
        )
        self.assertEqual(
            Path("/tmp/run/round_007/environmentalist/observation_submissions.json"),
            observation_submissions_path(run_dir, round_id),
        )


if __name__ == "__main__":
    unittest.main()

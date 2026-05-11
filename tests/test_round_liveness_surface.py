from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    run_kernel,
    run_script,
    runtime_src_path,
    script_path,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.governance.round_liveness import (  # noqa: E402
    build_round_liveness_surface,
    compact_object,
)

RUN_ID = "run-round-liveness-001"
ROUND_ID = "round-001"


def all_keys(value: object) -> list[str]:
    if isinstance(value, dict):
        keys: list[str] = []
        for key, nested in value.items():
            keys.append(str(key))
            keys.extend(all_keys(nested))
        return keys
    if isinstance(value, list):
        keys = []
        for item in value:
            keys.extend(all_keys(item))
        return keys
    return []


def submit_open_evidence_request(run_dir: Path) -> str:
    payload = run_script(
        script_path("submit-evidence-request"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--author-role",
        "moderator",
        "--question",
        "Which source can verify the receptor smoke timing?",
        "--desired-evidence-type",
        "environment-observation",
        "--rationale",
        "Keep the request visible for follow-up acquisition.",
        "--target-kind",
        "round",
        "--target-id",
        ROUND_ID,
    )
    return str(payload["summary"]["object_id"])


def submit_finding(run_dir: Path, request_id: str) -> str:
    payload = run_kernel(
        "submit-finding-record",
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--actor-role",
        "environmental-investigator",
        "--agent-role",
        "environmental-investigator",
        "--finding-kind",
        "finding",
        "--title",
        "Receptor smoke timing needs transport verification",
        "--summary",
        "Receipt-level receptor evidence exists, but source transport remains unresolved.",
        "--rationale",
        "The finding is useful for the next round but not yet bundled.",
        "--confidence",
        "0.7",
        "--target-kind",
        "evidence-request",
        "--target-id",
        request_id,
        "--basis-object-id",
        request_id,
        "--evidence-ref",
        "receipt://airnow/test-001",
        "--pretty",
    )
    return str(payload["summary"]["object_id"])


class RoundLivenessSurfaceTests(unittest.TestCase):
    def test_compact_object_exposes_object_local_handoffs_without_choice_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            proposal = compact_object(
                "source-acquisition-proposal",
                {
                    "proposal_id": "proposal-001",
                    "status": "proposed",
                    "author_role": "social-investigator",
                    "source_skill": "query-public-signals",
                },
                run_dir=run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )
            hypothesis = compact_object(
                "hypothesis",
                {
                    "hypothesis_id": "hypothesis-001",
                    "status": "active",
                    "owner_role": "environmental-investigator",
                },
                run_dir=run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )

            self.assertIn(
                "update-source-acquisition-proposal-status",
                proposal["handoff_commands"][
                    "update_source_acquisition_proposal_status_command_template"
                ],
            )
            self.assertIn(
                "source-acquisition-proposal:proposal-001",
                proposal["handoff_commands"]["carry_to_next_round_command_template"],
            )
            self.assertIn(
                "open-challenge-ticket",
                hypothesis["handoff_commands"][
                    "open_challenge_on_hypothesis_command_template"
                ],
            )
            self.assertIn(
                "hypothesis:hypothesis-001",
                hypothesis["handoff_commands"]["carry_to_next_round_command_template"],
            )
            self.assertFalse(
                {"score", "rank", "weight", "priority"}
                & set(all_keys({"proposal": proposal, "hypothesis": hypothesis}))
            )

    def test_liveness_surface_lists_unresolved_refs_without_scoring_or_ranking(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            request_id = submit_open_evidence_request(run_dir)
            finding_id = submit_finding(run_dir, request_id)

            payload = run_kernel(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            state = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            council_status = run_kernel(
                "show-council-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            unbundled_status = run_kernel(
                "show-unbundled-findings",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            source_surfaces = run_kernel(
                "show-source-surfaces",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            source_surfaces_alias = run_kernel(
                "show-source-surfaces",
                "--run_dir",
                str(run_dir),
                "--run_id",
                RUN_ID,
                "--round_id",
                ROUND_ID,
                "--pretty",
            )
            archive_status = run_kernel(
                "show-archive-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            board_summary = run_script(
                script_path("summarize-board-state"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            board_summary_artifact = load_json(
                Path(board_summary["summary"]["summary_path"])
            )

            liveness = payload["agent_entry"]["round_liveness_surface"]
            unresolved_sets = liveness["unresolved_sets"]
            self.assertEqual("round-liveness-surface-v1", liveness["schema_version"])
            self.assertEqual("unresolved-refs-present", liveness["liveness_status"])
            self.assertIn("does not rank, score", liveness["semantics"])
            self.assertIn("object-local templates", liveness["handoff_semantics"])
            self.assertIn(
                f"evidence-request:{request_id}",
                liveness["unresolved_refs"],
            )
            self.assertIn(
                f"finding:{finding_id}",
                liveness["unresolved_refs"],
            )
            self.assertEqual(
                1,
                liveness["counts"]["open_evidence_requests_count"],
            )
            self.assertEqual(
                1,
                liveness["counts"]["unbundled_findings_count"],
            )
            self.assertNotIn("ranked_refs", liveness)
            self.assertFalse(
                {"score", "rank", "weight", "priority"} & set(all_keys(liveness))
            )
            self.assertIn(
                "request-phase-transition",
                liveness["continuation"]["request_open_round_command_template"],
            )
            self.assertIn(
                "--request-payload-json",
                liveness["continuation"]["request_open_round_command_template"],
            )
            self.assertIn(
                f"finding:{finding_id}",
                [
                    item["object_ref"]
                    for item in unresolved_sets["unbundled_findings"]
                    if isinstance(item, dict)
                ],
            )
            finding_item = next(
                item
                for item in unresolved_sets["unbundled_findings"]
                if isinstance(item, dict) and item.get("object_id") == finding_id
            )
            self.assertIn("receipt://airnow/test-001", finding_item["evidence_refs"])
            finding_handoff = finding_item["handoff_commands"]
            self.assertIn(
                "submit-evidence-bundle",
                finding_handoff["submit_evidence_bundle_from_finding_command_template"],
            )
            self.assertIn(
                f"--finding-id {finding_id}",
                finding_handoff["submit_evidence_bundle_from_finding_command_template"],
            )
            self.assertIn(
                "update-hypothesis-status",
                finding_handoff["update_hypothesis_from_finding_command_template"],
            )
            self.assertIn(
                f"finding:{finding_id}",
                finding_handoff["update_hypothesis_from_finding_command_template"],
            )
            self.assertIn(
                f"finding:{finding_id}",
                finding_handoff["carry_to_next_round_command_template"],
            )
            evidence_request_item = next(
                item
                for item in unresolved_sets["open_evidence_requests"]
                if isinstance(item, dict) and item.get("object_id") == request_id
            )
            self.assertIn(
                "submit-source-acquisition-proposal",
                evidence_request_item["handoff_commands"][
                    "submit_source_acquisition_proposal_for_request_command_template"
                ],
            )
            self.assertIn(
                f"--target-evidence-request-id {request_id}",
                evidence_request_item["handoff_commands"][
                    "submit_source_acquisition_proposal_for_request_command_template"
                ],
            )
            self.assertEqual(
                "unresolved-refs-present",
                state["round_liveness"]["continuation"]["status"],
            )
            self.assertEqual(
                liveness["unresolved_ref_count"],
                state["agent_entry"]["operator"]["round_unresolved_ref_count"],
            )
            self.assertIn("round_liveness", board_summary_artifact)
            self.assertIn(
                f"finding:{finding_id}",
                board_summary_artifact["round_liveness"]["unresolved_refs"],
            )
            self.assertEqual("council-status", council_status["surface"])
            self.assertEqual(
                liveness["unresolved_ref_count"],
                council_status["summary"]["round_unresolved_ref_count"],
            )
            self.assertEqual(
                "unresolved-refs-present",
                council_status["summary"]["liveness_status"],
            )
            self.assertEqual("unbundled_findings", unbundled_status["surface"])
            self.assertIn(
                f"finding:{finding_id}",
                [item["object_ref"] for item in unbundled_status["items"]],
            )
            self.assertEqual("source-surfaces", source_surfaces["surface"])
            self.assertTrue(source_surfaces["catalog"])
            self.assertEqual("source-surfaces", source_surfaces_alias["surface"])
            catalog_entry = source_surfaces["catalog"][0]
            self.assertIn("provider_modes", catalog_entry)
            self.assertIn("fetch_argument_templates", catalog_entry)
            self.assertTrue(catalog_entry["provider_modes"])
            self.assertTrue(catalog_entry["fetch_argument_templates"])
            self.assertEqual("archive-status", archive_status["surface"])
            self.assertTrue(archive_status["gap_hints"])
            for payload_part in (council_status, unbundled_status, source_surfaces):
                self.assertFalse(
                    {"score", "rank", "weight", "priority"} & set(all_keys(payload_part))
                )

    def test_finding_leaves_unbundled_set_after_explicit_bundle_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)
            request_id = submit_open_evidence_request(run_dir)
            finding_id = submit_finding(run_dir, request_id)

            run_kernel(
                "submit-evidence-bundle",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "environmental-investigator",
                "--agent-role",
                "environmental-investigator",
                "--bundle-kind",
                "evidence-bundle",
                "--title",
                "Receipt-level receptor evidence bundle",
                "--summary",
                "Explicitly bundles the receptor smoke timing finding.",
                "--rationale",
                "The agent chose to carry this finding forward as a bundle.",
                "--confidence",
                "0.7",
                "--target-kind",
                "evidence-request",
                "--target-id",
                request_id,
                "--finding-id",
                finding_id,
                "--basis-object-id",
                request_id,
                "--evidence-ref",
                "receipt://airnow/test-001",
                "--pretty",
            )

            liveness = build_round_liveness_surface(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
            )
            unbundled_refs = [
                item["object_ref"]
                for item in liveness["unresolved_sets"]["unbundled_findings"]
                if isinstance(item, dict)
            ]
            self.assertNotIn(f"finding:{finding_id}", unbundled_refs)
            self.assertEqual(0, liveness["counts"]["unbundled_findings_count"])
            self.assertIn(f"evidence-request:{request_id}", liveness["unresolved_refs"])


if __name__ == "__main__":
    unittest.main()

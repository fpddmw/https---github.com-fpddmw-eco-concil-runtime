from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, reporting_path, run_kernel, run_script, script_path, seed_analysis_chain, write_json

RUN_ID = "run-reporting-publish-001"
ROUND_ID = "round-reporting-publish-001"


def prepare_ready_round(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    run_script(script_path("eco-derive-claim-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-derive-observation-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_payload = run_script(script_path("eco-score-evidence-coverage"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
    run_script(
        script_path("eco-post-board-note"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--author-role", "moderator", "--category", "analysis",
        "--note-text", "Round is ready to move into role reports and final decision publish.",
        "--linked-artifact-ref", coverage_ref,
    )
    run_script(
        script_path("eco-update-hypothesis-status"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--title", "Smoke over NYC was materially significant",
        "--statement", "Public smoke reports are backed by elevated PM2.5 observations.",
        "--status", "active", "--owner-role", "environmentalist",
        "--linked-claim-id", outputs["cluster_claims"]["canonical_ids"][0],
        "--confidence", "0.93",
    )
    run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)


def prepare_hold_round(run_dir: Path, root: Path) -> None:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    run_script(script_path("eco-derive-claim-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-derive-observation-scope"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_payload = run_script(script_path("eco-score-evidence-coverage"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
    hypothesis_payload = run_script(
        script_path("eco-update-hypothesis-status"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--title", "Smoke over NYC may be overstated",
        "--statement", "Public reports may overstate severity relative to observed PM2.5 coverage.",
        "--status", "active", "--owner-role", "moderator",
        "--linked-claim-id", outputs["cluster_claims"]["canonical_ids"][0],
        "--confidence", "0.52",
    )
    run_script(
        script_path("eco-open-challenge-ticket"),
        "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID,
        "--title", "Check whether smoke narrative is overstated",
        "--challenge-statement", "Re-test whether the strongest narrative exceeds evidence coverage.",
        "--target-claim-id", outputs["cluster_claims"]["canonical_ids"][0],
        "--target-hypothesis-id", hypothesis_payload["canonical_ids"][0],
        "--priority", "high", "--owner-role", "challenger", "--linked-artifact-ref", coverage_ref,
    )
    run_kernel("supervise-round", "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-materialize-reporting-handoff"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)
    run_script(script_path("eco-draft-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)


class ReportingPublishWorkflowTests(unittest.TestCase):
    def test_role_reports_and_decision_publish_ready_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            sociologist_draft = run_script(script_path("eco-draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            environmentalist_draft = run_script(script_path("eco-draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            soc_publish = run_script(script_path("eco-publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            env_publish = run_script(script_path("eco-publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "environmentalist")
            decision_publish = run_script(script_path("eco-publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            soc_report = load_json(reporting_path(run_dir, f"expert_report_sociologist_{ROUND_ID}.json"))
            env_report = load_json(reporting_path(run_dir, f"expert_report_environmentalist_{ROUND_ID}.json"))
            decision = load_json(reporting_path(run_dir, f"council_decision_{ROUND_ID}.json"))

            self.assertEqual("ready-to-publish", sociologist_draft["summary"]["report_status"])
            self.assertEqual("ready-to-publish", environmentalist_draft["summary"]["report_status"])
            self.assertEqual("published", soc_publish["summary"]["operation"])
            self.assertEqual("published", env_publish["summary"]["operation"])
            self.assertEqual("published", decision_publish["summary"]["operation"])
            self.assertEqual("sociologist", soc_report["agent_role"])
            self.assertEqual("environmentalist", env_report["agent_role"])
            self.assertEqual("ready", decision["publication_readiness"])
            self.assertEqual(2, len(decision["published_report_refs"]))

    def test_publish_council_decision_blocks_ready_round_without_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_ready_round(run_dir, root)

            payload = run_script(script_path("eco-publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            self.assertEqual("blocked", payload["status"])
            self.assertEqual("blocked", payload["summary"]["operation"])
            self.assertTrue(any(item["code"] == "missing-canonical-report" for item in payload["warnings"]))

    def test_hold_decision_can_publish_without_reports_and_report_overwrite_is_guarded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            prepare_hold_round(run_dir, root)

            draft_payload = run_script(script_path("eco-draft-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            first_publish = run_script(script_path("eco-publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")
            decision_publish = run_script(script_path("eco-publish-council-decision"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID)

            draft_path = reporting_path(run_dir, f"expert_report_draft_sociologist_{ROUND_ID}.json")
            modified = load_json(draft_path)
            modified["summary"] = modified["summary"] + " Changed after first publish."
            write_json(draft_path, modified)
            second_publish = run_script(script_path("eco-publish-expert-report"), "--run-dir", str(run_dir), "--run-id", RUN_ID, "--round-id", ROUND_ID, "--role", "sociologist")

            self.assertEqual("needs-more-evidence", draft_payload["summary"]["report_status"])
            self.assertEqual("published", first_publish["summary"]["operation"])
            self.assertEqual("completed", decision_publish["status"])
            self.assertEqual("hold", decision_publish["summary"]["publication_readiness"])
            self.assertEqual("blocked", second_publish["status"])
            self.assertTrue(any(item["code"] == "overwrite-blocked" for item in second_publish["warnings"]))


if __name__ == "__main__":
    unittest.main()
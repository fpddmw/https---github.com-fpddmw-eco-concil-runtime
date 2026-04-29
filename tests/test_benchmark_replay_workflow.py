from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    primary_research_issue_id,
    primary_wp4_evidence_ref,
    request_and_approve_transition,
    run_kernel,
    run_script,
    script_path,
    seed_analysis_chain,
    submit_ready_council_support,
    write_json,
)

RUN_ID = "run-benchmark-001"
ROUND_ID = "round-benchmark-001"


def approve_transition(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    transition_kind: str,
) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind=transition_kind,
        rationale=f"Approve `{transition_kind}` for benchmark workflow coverage.",
    )


def build_mission_file(root: Path, run_id: str, round_id: str) -> Path:
    mission_path = root / f"mission_{run_id}.json"
    write_json(
        mission_path,
        {
            "schema_version": "1.0.0",
            "run_id": run_id,
            "topic": "NYC smoke verification",
            "objective": "Determine whether public smoke reports are supported by physical evidence.",
            "policy_profile": "standard",
            "window": {
                "start_utc": "2023-06-07T00:00:00Z",
                "end_utc": "2023-06-07T23:59:59Z",
            },
            "region": {
                "label": "New York City, USA",
                "geometry": {
                    "type": "Point",
                    "latitude": 40.7128,
                    "longitude": -74.0060,
                },
            },
            "hypotheses": [
                {
                    "title": "Smoke over NYC was materially significant",
                    "statement": "Public smoke reports are backed by elevated PM2.5 observations.",
                    "confidence": 0.55,
                }
            ],
            "artifact_imports": [],
            "seed_round_id": round_id,
        },
    )
    return mission_path


def prepare_benchmark_ready_round(run_dir: Path, fixture_root: Path, run_id: str, round_id: str) -> None:
    mission_path = build_mission_file(fixture_root, run_id, round_id)
    run_script(
        script_path("scaffold-mission-run"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--mission-path",
        str(mission_path),
    )
    outputs = seed_analysis_chain(run_dir, fixture_root, run_id, round_id, include_airnow=True)
    evidence_ref = primary_wp4_evidence_ref(outputs)
    issue_id = primary_research_issue_id(outputs)
    submit_ready_council_support(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        issue_id=issue_id,
        evidence_ref=evidence_ref,
    )
    run_script(
        script_path("post-board-note"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--author-role",
        "moderator",
        "--category",
        "analysis",
        "--note-text",
        "Round is ready for benchmark and replay validation.",
        "--linked-artifact-ref",
        evidence_ref,
    )
    run_script(
        script_path("update-hypothesis-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--title",
        "Smoke over NYC was materially significant",
        "--statement",
        "Public smoke reports are backed by elevated PM2.5 observations.",
        "--status",
        "active",
        "--owner-role",
        "environmentalist",
        "--linked-claim-id",
        issue_id,
        "--linked-artifact-ref",
        evidence_ref,
        "--confidence",
        "0.93",
    )
    approve_transition(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind="promote-evidence-basis",
    )
    run_kernel(
        "supervise-round",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("materialize-reporting-handoff"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("draft-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    for role in ("sociologist", "environmentalist"):
        run_script(
            script_path("draft-expert-report"),
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--role",
            role,
        )
        run_script(
            script_path("publish-expert-report"),
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--role",
            role,
        )
    run_script(
        script_path("publish-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("materialize-final-publication"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    close_request_id = approve_transition(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind="close-round",
    )
    run_kernel(
        "close-round",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--transition-request-id",
        close_request_id,
        "--contract-mode",
        "strict",
    )
    run_kernel(
        "bootstrap-history-context",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--contract-mode",
        "strict",
    )


class BenchmarkReplayWorkflowTests(unittest.TestCase):
    def test_runtime_benchmark_fixture_detects_missing_orchestration_plan_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline_run_dir = root / "baseline-run"
            candidate_run_dir = root / "candidate-run"

            prepare_benchmark_ready_round(baseline_run_dir, root / "baseline-fixtures", RUN_ID, ROUND_ID)

            manifest_payload = run_kernel(
                "materialize-benchmark-manifest",
                "--run-dir",
                str(baseline_run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            fixture_payload = run_kernel(
                "materialize-scenario-fixture",
                "--run-dir",
                str(baseline_run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--scenario-id",
                "scenario-nyc-smoke-benchmark",
            )

            shutil.copytree(baseline_run_dir, candidate_run_dir)
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(candidate_run_dir),
                "--round-id",
                ROUND_ID,
                "--tail",
                "20",
            )
            replay_payload = run_kernel(
                "replay-runtime-scenario",
                "--run-dir",
                str(candidate_run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--fixture-path",
                str(baseline_run_dir / "runtime" / f"scenario_fixture_{ROUND_ID}.json"),
            )

            candidate_manifest = load_json(candidate_run_dir / "runtime" / f"benchmark_manifest_{ROUND_ID}.json")
            compare_artifact = load_json(candidate_run_dir / "runtime" / f"benchmark_compare_{ROUND_ID}.json")
            replay_report = load_json(candidate_run_dir / "runtime" / f"replay_report_{ROUND_ID}.json")

            self.assertEqual("completed", manifest_payload["status"])
            self.assertEqual("scenario-nyc-smoke-benchmark", fixture_payload["scenario_fixture"]["scenario_id"])
            self.assertTrue(state_payload["benchmark"]["operator"]["fixture_materialized"])
            self.assertTrue(state_payload["benchmark"]["operator"]["benchmark_materialized"])
            self.assertIn("replay-runtime-scenario", state_payload["benchmark"]["operator"]["replay_command"])
            self.assertEqual(
                "regression-detected",
                replay_payload["replay_report"]["replay_verdict"],
            )
            self.assertEqual("regression", compare_artifact["verdict"])
            self.assertEqual("regression-detected", replay_report["replay_verdict"])
            self.assertGreaterEqual(replay_report["artifact_drift_count"], 1)
            self.assertTrue(
                any(
                    item.get("key") == "orchestration_plan"
                    for item in compare_artifact["artifact_drift"]
                    if isinstance(item, dict)
                )
            )
            self.assertEqual(0, candidate_manifest["summary"]["failed_event_count"])
            self.assertEqual(
                candidate_manifest["phase2_summary"]["reporting_ready"],
                state_payload["benchmark"]["operator"]["reporting_ready"],
            )
            self.assertTrue(any(item["skill_name"] == "archive-case-library" for item in candidate_manifest["skill_timing_summary"]))
            self.assertTrue(any(item["event_type"] == "round-close" for item in candidate_manifest["round_event_summary"]))

    def test_runtime_replay_detects_output_regression(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            baseline_run_dir = root / "baseline-run"
            candidate_run_dir = root / "candidate-run"

            prepare_benchmark_ready_round(baseline_run_dir, root / "baseline-fixtures", RUN_ID, ROUND_ID)
            run_kernel(
                "materialize-scenario-fixture",
                "--run-dir",
                str(baseline_run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            shutil.copytree(baseline_run_dir, candidate_run_dir)
            final_publication_path = candidate_run_dir / "reporting" / f"final_publication_{ROUND_ID}.json"
            final_publication = load_json(final_publication_path)
            final_publication["publication_posture"] = "release"
            final_publication["publication_status"] = "ready-for-release"
            final_publication["publication_summary"] = "Tampered publication summary for replay regression testing."
            write_json(final_publication_path, final_publication)

            replay_payload = run_kernel(
                "replay-runtime-scenario",
                "--run-dir",
                str(candidate_run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--fixture-path",
                str(baseline_run_dir / "runtime" / f"scenario_fixture_{ROUND_ID}.json"),
            )
            compare_artifact = load_json(candidate_run_dir / "runtime" / f"benchmark_compare_{ROUND_ID}.json")

            self.assertEqual("regression-detected", replay_payload["replay_report"]["replay_verdict"])
            self.assertEqual("regression", compare_artifact["verdict"])
            self.assertFalse(compare_artifact["output_match"])
            self.assertTrue(any(item["key"] == "final_publication" for item in compare_artifact["artifact_drift"]))


if __name__ == "__main__":
    unittest.main()

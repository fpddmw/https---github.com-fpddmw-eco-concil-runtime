from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import load_json, run_kernel, run_script, script_path, seed_analysis_chain, write_json

HISTORICAL_RUN_ID = "run-history-archive-001"
HISTORICAL_ROUND_ID = "round-history-archive-001"
CURRENT_RUN_ID = "run-history-current-001"
CURRENT_ROUND_ID = "round-history-current-001"
SEARCH_RUN_ID = "run-history-search-001"
SEARCH_ROUND_ID = "round-history-search-001"


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


def prepare_ready_round(run_dir: Path, fixture_root: Path, run_id: str, round_id: str, *, publish: bool) -> None:
    mission_path = build_mission_file(fixture_root, run_id, round_id)
    run_script(
        script_path("eco-scaffold-mission-run"),
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

    run_script(
        script_path("eco-derive-claim-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("eco-derive-observation-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    coverage_payload = run_script(
        script_path("eco-score-evidence-coverage"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
    run_script(
        script_path("eco-post-board-note"),
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
        "Round is ready to move into archive and history validation.",
        "--linked-artifact-ref",
        coverage_ref,
    )
    run_script(
        script_path("eco-update-hypothesis-status"),
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
        outputs["cluster_claims"]["canonical_ids"][0],
        "--confidence",
        "0.93",
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
        script_path("eco-materialize-reporting-handoff"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("eco-draft-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )

    if not publish:
        return

    run_script(
        script_path("eco-draft-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--role",
        "sociologist",
    )
    run_script(
        script_path("eco-draft-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--role",
        "environmentalist",
    )
    run_script(
        script_path("eco-publish-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--role",
        "sociologist",
    )
    run_script(
        script_path("eco-publish-expert-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--role",
        "environmentalist",
    )
    run_script(
        script_path("eco-publish-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("eco-materialize-final-publication"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )


class ArchiveHistoryWorkflowTests(unittest.TestCase):
    def test_archive_skills_support_strict_runtime_and_structured_query(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            historical_run_dir = root / "historical-run"
            search_run_dir = root / "search-run"

            prepare_ready_round(
                historical_run_dir,
                root / "historical-fixtures",
                HISTORICAL_RUN_ID,
                HISTORICAL_ROUND_ID,
                publish=True,
            )

            signal_archive = run_kernel(
                "run-skill",
                "--run-dir",
                str(historical_run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
                "--skill-name",
                "eco-archive-signal-corpus",
                "--contract-mode",
                "strict",
            )
            case_archive = run_kernel(
                "run-skill",
                "--run-dir",
                str(historical_run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
                "--skill-name",
                "eco-archive-case-library",
                "--contract-mode",
                "strict",
            )

            case_query = run_script(
                script_path("eco-query-case-library"),
                "--run-dir",
                str(search_run_dir),
                "--run-id",
                SEARCH_RUN_ID,
                "--round-id",
                SEARCH_ROUND_ID,
                "--query-text",
                "nyc smoke verification",
                "--region-label",
                "New York City, USA",
                "--profile-id",
                "smoke-transport",
                "--claim-type",
                "smoke",
                "--metric-family",
                "air-quality",
                "--gap-type",
                "station-air-quality",
            )
            signal_query = run_script(
                script_path("eco-query-signal-corpus"),
                "--run-dir",
                str(search_run_dir),
                "--run-id",
                SEARCH_RUN_ID,
                "--round-id",
                SEARCH_ROUND_ID,
                "--query-text",
                "nyc smoke pm2_5",
                "--region-label",
                "New York City, USA",
                "--metric-family",
                "air-quality",
            )

            case_query_artifact = load_json(search_run_dir / "archive" / f"case_library_query_{SEARCH_ROUND_ID}.json")
            signal_query_artifact = load_json(search_run_dir / "archive" / f"signal_corpus_query_{SEARCH_ROUND_ID}.json")
            case_db_path = (root / "archives" / "eco_case_library.sqlite").resolve().as_posix()
            signal_db_path = (root / "archives" / "eco_signal_corpus.sqlite").resolve().as_posix()

            self.assertEqual("completed", signal_archive["status"])
            self.assertEqual("completed", case_archive["status"])
            self.assertEqual("strict", signal_archive["summary"]["contract_mode"])
            self.assertEqual("strict", case_archive["summary"]["contract_mode"])
            self.assertIn(signal_db_path, signal_archive["event"]["resolved_write_paths"])
            self.assertIn(case_db_path, case_archive["event"]["resolved_write_paths"])
            self.assertEqual(HISTORICAL_RUN_ID, case_archive["skill_payload"]["summary"]["case_id"])

            self.assertEqual("completed", case_query["status"])
            self.assertEqual("completed", signal_query["status"])
            self.assertGreaterEqual(case_query["summary"]["result_count"], 1)
            self.assertGreaterEqual(signal_query["summary"]["result_count"], 1)

            first_case = case_query_artifact["cases"][0]
            first_signal = signal_query_artifact["results"][0]
            self.assertEqual(HISTORICAL_RUN_ID, first_case["case_id"])
            self.assertEqual("structured-strong", first_case["score_components"]["match_tier"])
            self.assertIn("air-quality", first_case["matched_metric_families"])
            self.assertEqual(HISTORICAL_RUN_ID, first_signal["run_id"])
            self.assertEqual("air-quality", first_signal["metric_family"])

    def test_history_context_materialization_reuses_archived_cases_and_signals(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            historical_run_dir = root / "historical-run"
            current_run_dir = root / "current-run"

            prepare_ready_round(
                historical_run_dir,
                root / "historical-fixtures",
                HISTORICAL_RUN_ID,
                HISTORICAL_ROUND_ID,
                publish=True,
            )
            run_script(
                script_path("eco-archive-signal-corpus"),
                "--run-dir",
                str(historical_run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
            )
            run_script(
                script_path("eco-archive-case-library"),
                "--run-dir",
                str(historical_run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
            )

            prepare_ready_round(
                current_run_dir,
                root / "current-fixtures",
                CURRENT_RUN_ID,
                CURRENT_ROUND_ID,
                publish=False,
            )

            history_payload = run_kernel(
                "run-skill",
                "--run-dir",
                str(current_run_dir),
                "--run-id",
                CURRENT_RUN_ID,
                "--round-id",
                CURRENT_ROUND_ID,
                "--skill-name",
                "eco-materialize-history-context",
                "--contract-mode",
                "strict",
            )

            retrieval_artifact = load_json(current_run_dir / "investigation" / f"history_retrieval_{CURRENT_ROUND_ID}.json")
            context_text = (current_run_dir / "investigation" / f"history_context_{CURRENT_ROUND_ID}.md").read_text(encoding="utf-8")

            self.assertEqual("completed", history_payload["status"])
            self.assertEqual("strict", history_payload["summary"]["contract_mode"])
            self.assertGreaterEqual(retrieval_artifact["budget"]["selected_case_count"], 1)
            self.assertGreaterEqual(retrieval_artifact["budget"]["selected_signal_count"], 1)
            self.assertEqual("smoke-transport", retrieval_artifact["history_query"]["profile_id"])
            self.assertTrue(any(case["case_id"] == HISTORICAL_RUN_ID for case in retrieval_artifact["cases"]))
            self.assertIn(HISTORICAL_RUN_ID, context_text)
            self.assertIn("Historical Signal Hints", context_text)


if __name__ == "__main__":
    unittest.main()
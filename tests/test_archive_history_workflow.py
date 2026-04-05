from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    investigation_path,
    load_json,
    run_kernel,
    run_script,
    script_path,
    seed_analysis_chain,
    write_json,
)

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


def seed_moderator_actions_and_probes(
    run_dir: Path,
    run_id: str,
    round_id: str,
) -> tuple[dict[str, object], dict[str, object]]:
    run_script(
        script_path("eco-summarize-board-state"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("eco-materialize-board-brief"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("eco-propose-next-actions"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    run_script(
        script_path("eco-open-falsification-probe"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    return (
        load_json(investigation_path(run_dir, f"next_actions_{round_id}.json")),
        load_json(
            investigation_path(run_dir, f"falsification_probes_{round_id}.json")
        ),
    )


class ArchiveHistoryWorkflowTests(unittest.TestCase):
    def test_close_round_runtime_command_archives_terminal_round_and_exposes_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "historical-run"

            prepare_ready_round(
                run_dir,
                root / "historical-fixtures",
                HISTORICAL_RUN_ID,
                HISTORICAL_ROUND_ID,
                publish=True,
            )

            close_payload = run_kernel(
                "close-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
                "--contract-mode",
                "strict",
            )
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                HISTORICAL_ROUND_ID,
                "--tail",
                "20",
            )

            close_artifact = load_json(run_dir / "runtime" / f"round_close_{HISTORICAL_ROUND_ID}.json")
            final_publication = load_json(run_dir / "reporting" / f"final_publication_{HISTORICAL_ROUND_ID}.json")
            expected_close_posture = "published-release" if final_publication["publication_posture"] == "release" else "published-withhold"

            self.assertEqual("completed", close_payload["status"])
            self.assertEqual("completed", close_payload["round_close"]["close_status"])
            self.assertEqual("completed", close_artifact["archive_status"])
            self.assertEqual(expected_close_posture, close_artifact["close_posture"])
            self.assertIn("eco-materialize-history-context", close_artifact["recommended_next_skills"])
            self.assertEqual(
                ["archive-signal-corpus", "archive-case-library"],
                [item["stage"] for item in close_artifact["steps"]],
            )
            self.assertEqual("completed", state_payload["post_round"]["round_close"]["close_status"])
            self.assertEqual("completed", state_payload["post_round"]["operator"]["round_close_status"])
            self.assertTrue(Path(close_artifact["artifacts"]["signal_archive_db_path"]).exists())
            self.assertTrue(Path(close_artifact["artifacts"]["case_archive_db_path"]).exists())

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
            analytics_path(
                historical_run_dir, f"claim_scope_proposals_{HISTORICAL_ROUND_ID}.json"
            ).unlink()
            analytics_path(
                historical_run_dir,
                f"observation_scope_proposals_{HISTORICAL_ROUND_ID}.json",
            ).unlink()
            analytics_path(
                historical_run_dir, f"evidence_coverage_{HISTORICAL_ROUND_ID}.json"
            ).unlink()

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
            case_import_artifact = load_json(
                historical_run_dir
                / "archive"
                / f"case_library_import_{HISTORICAL_ROUND_ID}.json"
            )
            case_db_path = (root / "archives" / "eco_case_library.sqlite").resolve().as_posix()
            signal_db_path = (root / "archives" / "eco_signal_corpus.sqlite").resolve().as_posix()

            self.assertEqual("completed", signal_archive["status"])
            self.assertEqual("completed", case_archive["status"])
            self.assertEqual("strict", signal_archive["summary"]["contract_mode"])
            self.assertEqual("strict", case_archive["summary"]["contract_mode"])
            self.assertIn(signal_db_path, signal_archive["event"]["resolved_write_paths"])
            self.assertIn(case_db_path, case_archive["event"]["resolved_write_paths"])
            self.assertEqual(HISTORICAL_RUN_ID, case_archive["skill_payload"]["summary"]["case_id"])
            self.assertEqual(
                "analysis-plane",
                case_archive["skill_payload"]["summary"]["claim_scope_source"],
            )
            self.assertEqual(
                "analysis-plane",
                case_archive["skill_payload"]["summary"]["observation_scope_source"],
            )
            self.assertEqual(
                "analysis-plane",
                case_archive["skill_payload"]["summary"]["coverage_source"],
            )

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
            self.assertEqual("analysis-plane", case_import_artifact["claim_scope_source"])
            self.assertEqual(
                "analysis-plane",
                case_import_artifact["observation_scope_source"],
            )
            self.assertEqual("analysis-plane", case_import_artifact["coverage_source"])
            self.assertFalse(
                case_import_artifact["observed_inputs"]["claim_scope_artifact_present"]
            )
            self.assertFalse(
                case_import_artifact["observed_inputs"][
                    "observation_scope_artifact_present"
                ]
            )
            self.assertFalse(
                case_import_artifact["observed_inputs"]["coverage_artifact_present"]
            )
            self.assertTrue(case_import_artifact["observed_inputs"]["claim_scope_present"])
            self.assertTrue(
                case_import_artifact["observed_inputs"]["observation_scope_present"]
            )
            self.assertTrue(case_import_artifact["observed_inputs"]["coverage_present"])

    def test_runtime_history_bootstrap_materializes_archive_backed_context(self) -> None:
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
            run_kernel(
                "close-round",
                "--run-dir",
                str(historical_run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
                "--contract-mode",
                "strict",
            )

            prepare_ready_round(
                current_run_dir,
                root / "current-fixtures",
                CURRENT_RUN_ID,
                CURRENT_ROUND_ID,
                publish=False,
            )

            history_payload = run_kernel(
                "bootstrap-history-context",
                "--run-dir",
                str(current_run_dir),
                "--run-id",
                CURRENT_RUN_ID,
                "--round-id",
                CURRENT_ROUND_ID,
                "--contract-mode",
                "strict",
            )
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(current_run_dir),
                "--round-id",
                CURRENT_ROUND_ID,
                "--tail",
                "20",
            )

            bootstrap_artifact = load_json(current_run_dir / "runtime" / f"history_bootstrap_{CURRENT_ROUND_ID}.json")
            retrieval_artifact = load_json(current_run_dir / "investigation" / f"history_retrieval_{CURRENT_ROUND_ID}.json")

            self.assertEqual("completed", history_payload["status"])
            self.assertEqual("completed", bootstrap_artifact["bootstrap_status"])
            self.assertGreaterEqual(bootstrap_artifact["selected_case_count"], 1)
            self.assertGreaterEqual(bootstrap_artifact["selected_signal_count"], 1)
            self.assertEqual("completed", state_payload["post_round"]["history_bootstrap"]["bootstrap_status"])
            self.assertEqual("completed", state_payload["post_round"]["operator"]["history_bootstrap_status"])
            self.assertEqual(bootstrap_artifact["selected_case_count"], retrieval_artifact["budget"]["selected_case_count"])
            self.assertEqual("smoke-transport", retrieval_artifact["history_query"]["profile_id"])

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
            analytics_path(
                current_run_dir, f"claim_scope_proposals_{CURRENT_ROUND_ID}.json"
            ).unlink()
            analytics_path(
                current_run_dir,
                f"observation_scope_proposals_{CURRENT_ROUND_ID}.json",
            ).unlink()

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
            self.assertEqual(
                "analysis-plane",
                history_payload["skill_payload"]["summary"]["claim_scope_source"],
            )
            self.assertEqual(
                "analysis-plane",
                history_payload["skill_payload"]["summary"][
                    "observation_scope_source"
                ],
            )
            self.assertGreaterEqual(retrieval_artifact["budget"]["selected_case_count"], 1)
            self.assertGreaterEqual(retrieval_artifact["budget"]["selected_signal_count"], 1)
            self.assertEqual("smoke-transport", retrieval_artifact["history_query"]["profile_id"])
            self.assertEqual("analysis-plane", retrieval_artifact["claim_scope_source"])
            self.assertEqual(
                "analysis-plane",
                retrieval_artifact["observation_scope_source"],
            )
            self.assertFalse(
                retrieval_artifact["observed_inputs"]["claim_scope_artifact_present"]
            )
            self.assertFalse(
                retrieval_artifact["observed_inputs"][
                    "observation_scope_artifact_present"
                ]
            )
            self.assertTrue(retrieval_artifact["observed_inputs"]["claim_scope_present"])
            self.assertTrue(
                retrieval_artifact["observed_inputs"]["observation_scope_present"]
            )
            self.assertTrue(any(case["case_id"] == HISTORICAL_RUN_ID for case in retrieval_artifact["cases"]))
            self.assertIn(HISTORICAL_RUN_ID, context_text)
            self.assertIn("Historical Signal Hints", context_text)

    def test_archive_case_library_reads_db_backed_actions_and_probes_when_exports_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            historical_run_dir = root / "historical-run"

            prepare_ready_round(
                historical_run_dir,
                root / "historical-fixtures",
                HISTORICAL_RUN_ID,
                HISTORICAL_ROUND_ID,
                publish=True,
            )
            next_actions_artifact, probes_artifact = seed_moderator_actions_and_probes(
                historical_run_dir,
                HISTORICAL_RUN_ID,
                HISTORICAL_ROUND_ID,
            )
            expected_questions = [
                next_actions_artifact["ranked_actions"][0]["objective"],
                probes_artifact["probes"][0]["falsification_question"],
            ]

            investigation_path(
                historical_run_dir, f"next_actions_{HISTORICAL_ROUND_ID}.json"
            ).unlink()
            investigation_path(
                historical_run_dir,
                f"falsification_probes_{HISTORICAL_ROUND_ID}.json",
            ).unlink()

            archive_payload = run_script(
                script_path("eco-archive-case-library"),
                "--run-dir",
                str(historical_run_dir),
                "--run-id",
                HISTORICAL_RUN_ID,
                "--round-id",
                HISTORICAL_ROUND_ID,
            )

            archive_artifact = load_json(
                historical_run_dir
                / "archive"
                / f"case_library_import_{HISTORICAL_ROUND_ID}.json"
            )
            connection = sqlite3.connect(archive_payload["summary"]["db_path"])
            try:
                row = connection.execute(
                    "SELECT open_questions_json FROM cases WHERE case_id = ?",
                    (HISTORICAL_RUN_ID,),
                ).fetchone()
            finally:
                connection.close()

            self.assertEqual(
                "deliberation-plane-actions",
                archive_payload["summary"]["next_actions_source"],
            )
            self.assertEqual(
                "deliberation-plane-probes",
                archive_payload["summary"]["probes_source"],
            )
            self.assertEqual(
                "deliberation-plane-actions",
                archive_artifact["next_actions_source"],
            )
            self.assertEqual(
                "deliberation-plane-probes",
                archive_artifact["probes_source"],
            )
            self.assertFalse(
                archive_artifact["observed_inputs"]["next_actions_artifact_present"]
            )
            self.assertFalse(
                archive_artifact["observed_inputs"]["probes_artifact_present"]
            )
            self.assertTrue(archive_artifact["observed_inputs"]["next_actions_present"])
            self.assertTrue(archive_artifact["observed_inputs"]["probes_present"])
            self.assertIsNotNone(row)
            assert row is not None
            open_questions = json.loads(row[0])
            self.assertTrue(
                all(question in open_questions for question in expected_questions)
            )

    def test_history_context_reads_db_backed_actions_and_probes_when_exports_are_missing(self) -> None:
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
            next_actions_artifact, probes_artifact = seed_moderator_actions_and_probes(
                current_run_dir,
                CURRENT_RUN_ID,
                CURRENT_ROUND_ID,
            )
            expected_questions = [
                next_actions_artifact["ranked_actions"][0]["objective"],
                probes_artifact["probes"][0]["falsification_question"],
            ]

            investigation_path(
                current_run_dir, f"next_actions_{CURRENT_ROUND_ID}.json"
            ).unlink()
            investigation_path(
                current_run_dir, f"falsification_probes_{CURRENT_ROUND_ID}.json"
            ).unlink()

            history_payload = run_script(
                script_path("eco-materialize-history-context"),
                "--run-dir",
                str(current_run_dir),
                "--run-id",
                CURRENT_RUN_ID,
                "--round-id",
                CURRENT_ROUND_ID,
            )

            retrieval_artifact = load_json(
                current_run_dir / "investigation" / f"history_retrieval_{CURRENT_ROUND_ID}.json"
            )

            self.assertEqual(
                "deliberation-plane-actions",
                history_payload["summary"]["next_actions_source"],
            )
            self.assertEqual(
                "deliberation-plane-probes",
                history_payload["summary"]["probes_source"],
            )
            self.assertEqual(
                "deliberation-plane-actions",
                retrieval_artifact["next_actions_source"],
            )
            self.assertEqual(
                "deliberation-plane-probes",
                retrieval_artifact["probes_source"],
            )
            self.assertFalse(
                retrieval_artifact["observed_inputs"][
                    "next_actions_artifact_present"
                ]
            )
            self.assertFalse(
                retrieval_artifact["observed_inputs"]["probes_artifact_present"]
            )
            self.assertTrue(retrieval_artifact["observed_inputs"]["next_actions_present"])
            self.assertTrue(retrieval_artifact["observed_inputs"]["probes_present"])
            self.assertTrue(
                all(
                    question in retrieval_artifact["current_context"]["open_questions"]
                    for question in expected_questions
                )
            )
            self.assertGreaterEqual(
                retrieval_artifact["budget"]["selected_case_count"], 1
            )
            self.assertGreaterEqual(
                retrieval_artifact["budget"]["selected_signal_count"], 1
            )


if __name__ == "__main__":
    unittest.main()

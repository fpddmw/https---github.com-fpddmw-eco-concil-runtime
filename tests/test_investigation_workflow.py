from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    investigation_path,
    load_json,
    primary_research_issue_id,
    primary_wp4_evidence_ref,
    promotion_path,
    request_and_approve_transition,
    reporting_path,
    run_script,
    script_path,
    seed_analysis_chain,
    submit_ready_council_support,
)

RUN_ID = "run-investigation-001"
ROUND_ID = "round-investigation-001"


def approve_promotion_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="promote-evidence-basis",
        rationale="Approve promotion for investigation workflow coverage.",
    )


def seed_wp4_issue_context(run_dir: Path, root: Path) -> dict[str, str]:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    return {
        "evidence_ref": primary_wp4_evidence_ref(outputs),
        "issue_id": primary_research_issue_id(outputs),
    }


def seed_open_challenge_context(
    run_dir: Path,
    root: Path,
    *,
    title: str = "Smoke over NYC may be overstated",
) -> dict[str, object]:
    context = seed_wp4_issue_context(run_dir, root)
    evidence_ref = context["evidence_ref"]
    issue_id = context["issue_id"]
    hypothesis_payload = run_script(
        script_path("update-hypothesis-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        title,
        "--statement",
        "Public reports may overstate severity relative to observed PM2.5 coverage.",
        "--status",
        "active",
        "--owner-role",
        "moderator",
        "--linked-claim-id",
        issue_id,
        "--linked-artifact-ref",
        evidence_ref,
        "--confidence",
        "0.52",
    )
    run_script(
        script_path("open-challenge-ticket"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--title",
        "Check whether smoke narrative is overstated",
        "--challenge-statement",
        "Re-test whether the strongest narrative exceeds evidence coverage.",
        "--target-claim-id",
        issue_id,
        "--target-hypothesis-id",
        hypothesis_payload["canonical_ids"][0],
        "--priority",
        "high",
        "--owner-role",
        "challenger",
        "--linked-artifact-ref",
        evidence_ref,
    )
    return {**context, "hypothesis_payload": hypothesis_payload}


def seed_ready_investigation_context(run_dir: Path, root: Path) -> dict[str, str]:
    context = seed_wp4_issue_context(run_dir, root)
    evidence_ref = context["evidence_ref"]
    issue_id = context["issue_id"]
    submit_ready_council_support(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        issue_id=issue_id,
        evidence_ref=evidence_ref,
    )
    run_script(
        script_path("post-board-note"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--author-role",
        "moderator",
        "--category",
        "analysis",
        "--note-text",
        "Board is organized and successor evidence is available for promotion review.",
        "--linked-artifact-ref",
        evidence_ref,
    )
    run_script(
        script_path("update-hypothesis-status"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
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
        "0.91",
    )
    return context


class InvestigationWorkflowTests(unittest.TestCase):
    def test_d1_builds_actions_and_probes_from_open_challenge(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            context = seed_open_challenge_context(run_dir, root)
            hypothesis_payload = context["hypothesis_payload"]
            run_script(
                script_path("summarize-board-state"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("materialize-board-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            actions_payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            probes_payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            actions_artifact = load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"))
            probes_artifact = load_json(investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json"))

            self.assertGreaterEqual(actions_payload["summary"]["action_count"], 1)
            self.assertEqual("deliberation-plane", actions_payload["summary"]["board_state_source"])
            self.assertEqual("completed", actions_payload["deliberation_sync"]["status"])
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)
            self.assertEqual("deliberation-plane-actions", probes_payload["summary"]["action_source"])
            actions = actions_artifact["ranked_actions"]
            self.assertEqual("deliberation-plane", actions_artifact["board_state_source"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_summary_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertTrue(actions_artifact["observed_inputs"]["board_brief_present"])
            self.assertTrue(any(action["action_kind"] == "resolve-challenge" for action in actions))
            self.assertTrue(any(bool(action["probe_candidate"]) for action in actions))
            self.assertTrue(any("controversy_gap" in action for action in actions))
            self.assertTrue(
                any(action.get("policy_source") == "runtime-fallback-policy" for action in actions)
            )
            self.assertEqual(
                len(actions),
                sum(actions_artifact["policy_source_counts"].values()),
            )
            probes = probes_artifact["probes"]
            self.assertEqual("deliberation-plane-actions", probes_artifact["action_source"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertTrue(any(probe["target_hypothesis_id"] == hypothesis_payload["canonical_ids"][0] for probe in probes))
            self.assertTrue(any("close-challenge-ticket" in probe["requested_skills"] for probe in probes))
            self.assertTrue(any("probe_type" in probe for probe in probes))

    def test_d1_next_actions_and_probes_work_without_board_summary_or_brief(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_open_challenge_context(
                run_dir,
                root,
                title="Smoke over NYC may still be overstated",
            )

            actions_payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            probes_payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            actions_artifact = load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"))
            probes_artifact = load_json(investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json"))

            self.assertFalse((run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").exists())
            self.assertFalse((run_dir / "board" / f"board_brief_{ROUND_ID}.md").exists())
            self.assertEqual("deliberation-plane", actions_payload["summary"]["board_state_source"])
            self.assertEqual("completed", actions_payload["deliberation_sync"]["status"])
            self.assertEqual("deliberation-plane", actions_artifact["board_state_source"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_summary_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["board_brief_present"])
            self.assertTrue(any(action["action_kind"] == "resolve-challenge" for action in actions_artifact["ranked_actions"]))
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertGreaterEqual(len(probes_artifact["probes"]), 1)

    def test_d1_probe_skill_can_rebuild_candidates_without_next_actions_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_open_challenge_context(
                run_dir,
                root,
                title="Direct probe fallback hypothesis",
            )
            analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").unlink(missing_ok=True)

            probes_payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            probes_artifact = load_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
            )

            self.assertFalse((run_dir / "investigation" / f"next_actions_{ROUND_ID}.json").exists())
            self.assertFalse(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").exists())
            self.assertEqual("derived-from-deliberation", probes_payload["summary"]["action_source"])
            self.assertEqual("deliberation-plane", probes_payload["summary"]["board_state_source"])
            self.assertEqual("missing-coverage", probes_payload["summary"]["coverage_source"])
            self.assertEqual("completed", probes_payload["deliberation_sync"]["status"])
            self.assertEqual("derived-from-deliberation", probes_artifact["action_source"])
            self.assertEqual("missing-coverage", probes_artifact["coverage_source"])
            self.assertFalse(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertFalse(probes_artifact["observed_inputs"]["coverage_present"])
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)
            self.assertGreaterEqual(len(probes_artifact["probes"]), 1)

    def test_d1_probe_skill_reads_db_backed_actions_when_next_actions_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_open_challenge_context(
                run_dir,
                root,
                title="DB-backed next actions survive artifact deletion",
            )
            run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            investigation_path(run_dir, f"next_actions_{ROUND_ID}.json").unlink()

            probes_payload = run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            probes_artifact = load_json(
                investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json")
            )

            self.assertFalse((run_dir / "investigation" / f"next_actions_{ROUND_ID}.json").exists())
            self.assertEqual("deliberation-plane-actions", probes_payload["summary"]["action_source"])
            self.assertEqual("deliberation-plane-actions", probes_artifact["action_source"])
            self.assertFalse(probes_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(probes_artifact["observed_inputs"]["next_actions_present"])
            self.assertGreaterEqual(probes_payload["summary"]["probe_count"], 1)

    def test_d2_reads_db_backed_actions_and_probes_when_artifacts_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_open_challenge_context(
                run_dir,
                root,
                title="Readiness should honor DB-backed action and probe state",
            )
            run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("open-falsification-probe"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            investigation_path(run_dir, f"next_actions_{ROUND_ID}.json").unlink()
            investigation_path(run_dir, f"falsification_probes_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                action_count = connection.execute(
                    "SELECT COUNT(*) FROM moderator_actions WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
                probe_count = connection.execute(
                    "SELECT COUNT(*) FROM falsification_probes WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_request_id = approve_promotion_transition(run_dir)
            promotion_payload = run_script(
                script_path("promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                promotion_request_id,
            )

            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertEqual("needs-more-data", readiness_payload["summary"]["readiness_status"])
            self.assertFalse(readiness_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(readiness_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["probes_artifact_present"])
            self.assertTrue(readiness_artifact["observed_inputs"]["probes_present"])
            self.assertGreater(action_count, 0)
            self.assertGreater(probe_count, 0)
            self.assertGreater(int(readiness_artifact["counts"]["open_probes"]), 0)
            self.assertIn("controversy_gap_counts", readiness_artifact)
            self.assertIn("probe_type_counts", readiness_artifact)
            self.assertEqual("withheld", promotion_payload["summary"]["promotion_status"])
            self.assertFalse(promotion_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertTrue(promotion_artifact["observed_inputs"]["next_actions_present"])
            self.assertEqual("deliberation-plane-actions", promotion_artifact["next_actions_source"])

    def test_d2_promotion_reads_db_backed_readiness_when_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_investigation_context(run_dir, root)

            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json").unlink()
            connection = sqlite3.connect(
                (run_dir / "analytics" / "signal_plane.sqlite").resolve()
            )
            try:
                readiness_count = connection.execute(
                    "SELECT COUNT(*) FROM round_readiness_assessments WHERE run_id = ? AND round_id = ?",
                    (RUN_ID, ROUND_ID),
                ).fetchone()[0]
            finally:
                connection.close()

            promotion_request_id = approve_promotion_transition(run_dir)
            promotion_payload = run_script(
                script_path("promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                promotion_request_id,
            )
            promotion_artifact = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )

            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertGreater(readiness_count, 0)
            self.assertEqual("promoted", promotion_payload["summary"]["promotion_status"])
            self.assertEqual(
                "deliberation-plane-readiness",
                promotion_artifact["readiness_source"],
            )
            self.assertFalse(
                promotion_artifact["observed_inputs"]["readiness_artifact_present"]
            )
            self.assertTrue(promotion_artifact["observed_inputs"]["readiness_present"])

    def test_d2_marks_ready_and_promotes_basis_when_board_is_clean(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_investigation_context(run_dir, root)
            run_script(
                script_path("summarize-board-state"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("materialize-board-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            actions_payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_request_id = approve_promotion_transition(run_dir)
            promotion_payload = run_script(
                script_path("promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                promotion_request_id,
            )

            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))
            self.assertEqual(0, actions_payload["summary"]["action_count"])
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertTrue(readiness_artifact["sufficient_for_promotion"])
            self.assertEqual("promoted", promotion_payload["summary"]["promotion_status"])
            self.assertEqual("promoted", promotion_artifact["promotion_status"])
            self.assertEqual(
                "council-judgement-freeze-v1",
                promotion_artifact["basis_selection_mode"],
            )
            self.assertEqual(0, promotion_artifact["basis_counts"]["coverage_count"])
            self.assertEqual([], promotion_artifact["selected_coverages"])
            self.assertGreaterEqual(len(promotion_artifact["supporting_proposal_ids"]), 1)
            self.assertGreaterEqual(len(promotion_artifact["supporting_opinion_ids"]), 1)

    def test_d2_reads_deliberation_plane_without_board_summary_or_brief(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_investigation_context(run_dir, root)

            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )

            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))

            self.assertFalse((run_dir / "board" / f"board_state_summary_{ROUND_ID}.json").exists())
            self.assertFalse((run_dir / "board" / f"board_brief_{ROUND_ID}.md").exists())
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertEqual("deliberation-plane", readiness_payload["summary"]["board_state_source"])
            self.assertEqual("completed", readiness_payload["deliberation_sync"]["status"])
            self.assertTrue(readiness_artifact["sufficient_for_promotion"])
            self.assertEqual("deliberation-plane", readiness_artifact["board_state_source"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_summary_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_summary_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_brief_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["board_brief_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["next_actions_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["next_actions_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["probes_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["probes_present"])

    def test_d1_and_d2_continue_from_council_basis_when_coverage_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_ready_investigation_context(run_dir, root)
            analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").unlink(missing_ok=True)

            actions_payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            readiness_payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_request_id = approve_promotion_transition(run_dir)
            promotion_payload = run_script(
                script_path("promote-evidence-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                promotion_request_id,
            )

            actions_artifact = load_json(investigation_path(run_dir, f"next_actions_{ROUND_ID}.json"))
            readiness_artifact = load_json(reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json"))
            promotion_artifact = load_json(promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json"))

            self.assertFalse(analytics_path(run_dir, f"evidence_coverage_{ROUND_ID}.json").exists())
            self.assertEqual("missing-coverage", actions_payload["summary"]["coverage_source"])
            self.assertEqual("missing-coverage", actions_artifact["coverage_source"])
            self.assertFalse(actions_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertFalse(actions_artifact["observed_inputs"]["coverage_present"])
            self.assertEqual(0, actions_payload["summary"]["action_count"])
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertEqual("missing-coverage", readiness_payload["summary"]["coverage_source"])
            self.assertFalse(readiness_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertFalse(readiness_artifact["observed_inputs"]["coverage_present"])
            self.assertTrue(readiness_artifact["sufficient_for_promotion"])
            self.assertEqual("promoted", promotion_payload["summary"]["promotion_status"])
            self.assertEqual("missing-coverage", promotion_payload["summary"]["coverage_source"])
            self.assertFalse(promotion_artifact["observed_inputs"]["coverage_artifact_present"])
            self.assertFalse(promotion_artifact["observed_inputs"]["coverage_present"])
            self.assertEqual(
                "council-judgement-freeze-v1",
                promotion_artifact["basis_selection_mode"],
            )
            self.assertEqual(0, promotion_artifact["basis_counts"]["coverage_count"])
            self.assertEqual([], promotion_artifact["selected_coverages"])
            self.assertGreaterEqual(len(promotion_artifact["supporting_proposal_ids"]), 1)
            self.assertGreaterEqual(len(promotion_artifact["supporting_opinion_ids"]), 1)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    investigation_path,
    load_json,
    promotion_path,
    request_and_approve_transition,
    reporting_path,
    run_script,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUN_ID = "run-deliberation-agenda-001"
ROUND_ID = "round-deliberation-agenda-001"


def approve_promotion_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="promote-evidence-basis",
        rationale="Approve promotion for deliberation agenda workflow coverage.",
    )


def seed_regulationsgov_comments(run_dir: Path, root: Path) -> None:
    regulations_path = root / "regulationsgov_comments.json"
    write_json(
        regulations_path,
        {
            "records": [
                {
                    "id": "rg-smoke-001",
                    "attributes": {
                        "title": "Wildfire smoke and air quality impacts",
                        "comment": "Wildfire smoke is reducing air quality and visibility across the city.",
                        "postedDate": "2023-06-08T12:00:00Z",
                        "agencyId": "EPA",
                        "docketId": "EPA-2023-001",
                    },
                },
                {
                    "id": "rg-permit-001",
                    "attributes": {
                        "title": "Extend the permit hearing and comment period",
                        "comment": "The agency should reopen the permit hearing and extend the public comment period for this docket.",
                        "postedDate": "2023-06-08T13:00:00Z",
                        "agencyId": "EPA",
                        "docketId": "EPA-2023-002",
                    },
                },
            ]
        },
    )
    run_script(
        script_path("normalize-regulationsgov-comments-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(regulations_path),
    )


def seed_public_only_trust_signal(run_dir: Path, root: Path) -> None:
    youtube_path = root / "youtube_public_only_trust.json"
    write_json(
        youtube_path,
        [
            {
                "query": "community voice ignored environment",
                "video_id": "vid-trust-001",
                "video": {
                    "id": "vid-trust-001",
                    "title": "Residents say their community voice was ignored",
                    "description": (
                        "Local residents say their community voice was ignored and public trust collapsed during the environmental decision."
                    ),
                    "channel_title": "Neighborhood Watch",
                    "published_at": "2023-06-08T15:00:00Z",
                    "default_language": "en",
                    "statistics": {"view_count": 320},
                },
            }
        ],
    )
    run_script(
        script_path("normalize-youtube-video-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(youtube_path),
    )


def seed_agenda_inputs(run_dir: Path, root: Path) -> None:
    seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    run_script(
        script_path("derive-claim-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("derive-observation-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("score-evidence-coverage"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("classify-claim-verifiability"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("route-verification-lane"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("materialize-controversy-map"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    seed_regulationsgov_comments(run_dir, root)
    seed_public_only_trust_signal(run_dir, root)
    run_script(
        script_path("link-formal-comments-to-public-discourse"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("identify-representation-gaps"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("detect-cross-platform-diffusion"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )


def seed_non_empirical_route_inputs(run_dir: Path, root: Path) -> None:
    seed_public_only_trust_signal(run_dir, root)
    run_script(
        script_path("extract-claim-candidates"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("cluster-claim-candidates"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("derive-claim-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("classify-claim-verifiability"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("route-verification-lane"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("materialize-controversy-map"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )


class DeliberationAgendaWorkflowTests(unittest.TestCase):
    def test_next_actions_materializes_representation_and_diffusion_agenda(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_agenda_inputs(run_dir, root)

            payload = run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-actions",
                "10",
            )
            artifact = load_json(
                investigation_path(run_dir, f"next_actions_{ROUND_ID}.json")
            )

            action_kinds = {
                action["action_kind"]
                for action in artifact["ranked_actions"]
                if isinstance(action, dict)
            }

            self.assertEqual("completed", payload["status"])
            self.assertEqual(
                "controversy-agenda-materialization",
                artifact["agenda_source"],
            )
            self.assertGreaterEqual(artifact["agenda_counts"]["issue_cluster_count"], 1)
            self.assertGreaterEqual(
                artifact["agenda_counts"]["representation_gap_count"],
                1,
            )
            self.assertGreaterEqual(
                artifact["agenda_counts"]["diffusion_focus_count"],
                1,
            )
            self.assertIn("address-representation-gap", action_kinds)
            self.assertIn("trace-cross-platform-diffusion", action_kinds)
            self.assertGreaterEqual(
                artifact["agenda_source_counts"].get("representation-gap", 0),
                1,
            )
            self.assertGreaterEqual(
                artifact["agenda_source_counts"].get("diffusion-edge", 0),
                1,
            )

    def test_readiness_uses_agenda_counts_as_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_agenda_inputs(run_dir, root)

            run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-actions",
                "10",
            )
            payload = run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("needs-more-data", payload["summary"]["readiness_status"])
            self.assertGreaterEqual(artifact["counts"]["issue_clusters"], 1)
            self.assertGreaterEqual(
                artifact["counts"]["representation_gap_actions"],
                1,
            )
            self.assertGreaterEqual(
                artifact["agenda_counts"]["formal_public_linkage_gap_count"],
                1,
            )
            self.assertTrue(
                any(
                    "representation-gap actions remain unresolved" in reason
                    for reason in artifact["gate_reasons"]
                )
            )

    def test_promotion_basis_freezes_controversy_objects_for_agenda_blocked_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_agenda_inputs(run_dir, root)

            run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-actions",
                "10",
            )
            run_script(
                script_path("summarize-round-readiness"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            promotion_request_id = approve_promotion_transition(run_dir)
            payload = run_script(
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
            artifact = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("withheld", artifact["promotion_status"])
            self.assertEqual(
                "freeze-controversy-basis-v1",
                artifact["basis_selection_mode"],
            )
            self.assertGreaterEqual(
                artifact["basis_counts"]["issue_cluster_count"],
                1,
            )
            self.assertGreaterEqual(
                artifact["basis_counts"]["formal_public_link_count"],
                1,
            )
            self.assertGreaterEqual(
                artifact["basis_counts"]["representation_gap_count"],
                1,
            )
            self.assertGreaterEqual(
                artifact["basis_counts"]["diffusion_edge_count"],
                1,
            )
            self.assertTrue(
                any(
                    row.get("object_type") == "representation-gap"
                    for row in artifact["frozen_basis"]["representation_gaps"]
                )
            )
            self.assertGreaterEqual(len(artifact["selected_basis_object_ids"]), 1)

    def test_non_empirical_round_can_promote_without_coverage_and_reporting_falls_back_to_structural_basis(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_non_empirical_route_inputs(run_dir, root)

            run_script(
                script_path("propose-next-actions"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--max-actions",
                "10",
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
            readiness_artifact = load_json(
                reporting_path(run_dir, f"round_readiness_{ROUND_ID}.json")
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
            promotion_artifact = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )
            run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_artifact = load_json(
                reporting_path(run_dir, f"reporting_handoff_{ROUND_ID}.json")
            )

            self.assertEqual("completed", readiness_payload["status"])
            self.assertEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertEqual(0, readiness_artifact["counts"]["strong_coverages"])
            self.assertEqual(0, readiness_artifact["counts"]["moderate_coverages"])
            self.assertEqual(0, readiness_artifact["agenda_counts"]["observation_lane_issue_count"])
            self.assertGreaterEqual(
                readiness_artifact["counts"]["public_discourse_issues"]
                + readiness_artifact["counts"]["stakeholder_deliberation_issues"]
                + readiness_artifact["counts"]["formal_record_issues"],
                1,
            )

            self.assertEqual("completed", promotion_payload["status"])
            self.assertEqual("promoted", promotion_artifact["promotion_status"])
            self.assertEqual(0, promotion_artifact["basis_counts"]["coverage_count"])
            self.assertEqual(0, len(promotion_artifact["selected_coverages"]))
            self.assertGreaterEqual(
                promotion_artifact["basis_counts"]["verification_route_count"],
                1,
            )
            self.assertTrue(
                any(
                    row.get("recommended_lane")
                    in {
                        "formal-comment-and-policy-record",
                        "public-discourse-analysis",
                        "stakeholder-deliberation-analysis",
                    }
                    for row in promotion_artifact["frozen_basis"]["verification_routes"]
                )
            )

            self.assertGreaterEqual(len(handoff_artifact["key_findings"]), 1)
            self.assertIn(
                handoff_artifact["key_findings"][0].get("object_type"),
                {
                    "issue-cluster",
                    "verification-route",
                    "formal-public-link",
                    "representation-gap",
                    "diffusion-edge",
                },
            )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    analytics_path,
    load_json,
    report_basis_path,
    request_and_approve_transition,
    reporting_path,
    run_script,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUN_ID = "run-deliberation-agenda-001"
ROUND_ID = "round-deliberation-agenda-001"


def approve_report_basis_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="freeze-report-basis",
        rationale="Approve report_basis for deliberation agenda workflow coverage.",
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
                    "description": "Local residents say their community voice was ignored and public trust collapsed during the environmental decision.",
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


def write_approved_taxonomy(root: Path) -> Path:
    taxonomy_path = root / "approved_formal_public_taxonomy.json"
    write_json(
        taxonomy_path,
        {
            "version": "test-taxonomy-v1",
            "labels": [
                {"label": "air-quality-smoke", "terms": ["smoke", "air quality", "visibility"]},
                {"label": "permit-process", "terms": ["permit", "hearing", "comment period"]},
                {"label": "representation-trust", "terms": ["community voice", "public trust"]},
            ],
        },
    )
    return taxonomy_path


def seed_successor_agenda_inputs(run_dir: Path, root: Path) -> dict[str, dict[str, object]]:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    seed_regulationsgov_comments(run_dir, root)
    seed_public_only_trust_signal(run_dir, root)
    taxonomy_path = write_approved_taxonomy(root)
    taxonomy = run_script(
        script_path("apply-approved-formal-public-taxonomy"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--taxonomy-path",
        str(taxonomy_path),
        "--approval-ref",
        "approval://taxonomy/test-taxonomy-v1",
    )
    footprints = run_script(
        script_path("compare-formal-public-footprints"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--taxonomy-labels-path",
        taxonomy["summary"]["output_path"],
    )
    representation_cues = run_script(
        script_path("identify-representation-audit-cues"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    temporal_cues = run_script(
        script_path("detect-temporal-cooccurrence-cues"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    issue_map = run_script(
        script_path("export-research-issue-map"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--issue-surface-path",
        outputs["research_issue_surface"]["summary"]["output_path"],
        "--issue-views-path",
        outputs["research_issue_views"]["summary"]["output_path"],
    )
    return {
        "analysis": outputs,
        "taxonomy": taxonomy,
        "footprints": footprints,
        "representation_cues": representation_cues,
        "temporal_cues": temporal_cues,
        "issue_map": issue_map,
    }


class DeliberationAgendaWorkflowTests(unittest.TestCase):
    def test_successor_helpers_materialize_audit_surfaces_not_agenda_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            outputs = seed_successor_agenda_inputs(run_dir, root)

            self.assertEqual("completed", outputs["taxonomy"]["status"])
            self.assertEqual("completed", outputs["footprints"]["status"])
            self.assertEqual("completed", outputs["representation_cues"]["status"])
            self.assertEqual("completed", outputs["temporal_cues"]["status"])
            self.assertEqual("completed", outputs["issue_map"]["status"])

            footprint_artifact = load_json(analytics_path(run_dir, f"formal_public_footprints_{ROUND_ID}.json"))
            representation_artifact = load_json(analytics_path(run_dir, f"representation_audit_cues_{ROUND_ID}.json"))
            temporal_artifact = load_json(analytics_path(run_dir, f"temporal_cooccurrence_cues_{ROUND_ID}.json"))
            map_artifact = load_json(analytics_path(run_dir, f"research_issue_map_{ROUND_ID}.json"))

            self.assertNotIn("link_status", footprint_artifact["formal_public_footprints"])
            self.assertNotIn("gap_type", representation_artifact["representation_audit_cues"][0])
            self.assertNotIn("edge_type", temporal_artifact["temporal_cooccurrence_cues"][0])
            self.assertEqual("navigation-export", map_artifact["research_issue_map"]["map_status"])

    def test_readiness_and_report_basis_do_not_use_helper_cues_as_default_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_successor_agenda_inputs(run_dir, root)

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
            report_basis_request_id = approve_report_basis_transition(run_dir)
            report_basis_payload = run_script(
                script_path("freeze-report-basis"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--transition-request-id",
                report_basis_request_id,
            )
            report_basis_artifact = load_json(report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json"))

            self.assertEqual("completed", readiness_payload["status"])
            self.assertNotEqual("ready", readiness_payload["summary"]["readiness_status"])
            self.assertFalse(readiness_artifact["sufficient_for_report_basis"])
            self.assertEqual("completed", report_basis_payload["status"])
            self.assertEqual("withheld", report_basis_artifact["report_basis_status"])
            self.assertEqual(0, report_basis_artifact["basis_counts"]["coverage_count"])
            self.assertEqual([], report_basis_artifact["selected_coverages"])


if __name__ == "__main__":
    unittest.main()

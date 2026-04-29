from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import (
    load_json,
    reporting_path,
    request_and_approve_skill_approval,
    request_and_approve_transition,
    run_kernel,
    run_script,
    runtime_path,
    script_path,
    submit_ready_council_support,
    submit_report_basis_records,
    write_json,
)


def public_fetch_script(root: Path, *, case_id: str, keyword: str, title: str, body: str) -> Path:
    script = root / f"emit_public_{case_id}.py"
    payload = [
        {
            "query": keyword,
            "video_id": f"vid-{case_id}-001",
            "video": {
                "id": f"vid-{case_id}-001",
                "title": title,
                "description": body,
                "channel_title": "Policy Research Local Fixture",
                "published_at": "2024-04-01T12:00:00Z",
                "default_language": "en",
                "statistics": {"view_count": 1200},
            },
        }
    ]
    script.write_text(
        "import json\n"
        f"payload = {json.dumps(payload, sort_keys=True)}\n"
        "print(json.dumps(payload))\n",
        encoding="utf-8",
    )
    return script


def regulationsgov_artifact(root: Path, *, case_id: str, keyword: str, title: str, body: str) -> Path:
    path = root / f"formal_{case_id}.json"
    write_json(
        path,
        {
            "records": [
                {
                    "id": f"REG-{case_id}-001",
                    "attributes": {
                        "title": title,
                        "comment": body,
                        "postedDate": "2024-04-01T13:00:00Z",
                        "lastModifiedDate": "2024-04-01T13:10:00Z",
                        "docketId": f"EPA-POLICY-{case_id}",
                        "agencyId": "EPA",
                        "submitterName": "Fixture Civic Association",
                        "commentOnDocumentTitle": keyword,
                    },
                }
            ]
        },
    )
    return path


def environment_artifact(root: Path, *, case_id: str, source_skill: str) -> Path:
    path = root / f"environment_{case_id}.json"
    if source_skill == "fetch-usgs-water-iv":
        write_json(
            path,
            {
                "payload": {
                    "generated_at_utc": "2024-04-01T14:00:00Z",
                    "records": [
                        {
                            "site_number": "01463500",
                            "site_name": "Fixture River Gauge",
                            "agency_code": "USGS",
                            "parameter_code": "00060",
                            "variable_name": "discharge",
                            "variable_description": "Streamflow discharge near the project area.",
                            "value": 420.0,
                            "unit": "ft3/s",
                            "observed_at_utc": "2024-04-01T12:00:00Z",
                            "latitude": 40.21,
                            "longitude": -75.01,
                            "source_query_url": "https://waterservices.usgs.gov/nwis/iv/",
                        }
                    ],
                }
            },
        )
        return path
    write_json(
        path,
        {
            "results": [
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 38.5,
                    "date": {"utc": "2024-04-01T12:00:00Z"},
                    "coordinates": {"latitude": 40.7, "longitude": -74.0},
                    "location": {"id": 1, "name": "Policy Research Fixture Station"},
                    "provider": {"name": "OpenAQ"},
                }
            ]
        },
    )
    return path


def mission_file(root: Path, *, run_id: str, round_id: str, case: dict[str, str]) -> Path:
    public_script = public_fetch_script(
        root,
        case_id=case["case_id"],
        keyword=case["public_keyword"],
        title=case["public_title"],
        body=case["public_body"],
    )
    formal_path = regulationsgov_artifact(
        root,
        case_id=case["case_id"],
        keyword=case["formal_keyword"],
        title=case["formal_title"],
        body=case["formal_body"],
    )
    environment_path = environment_artifact(
        root,
        case_id=case["case_id"],
        source_skill=case["environment_source"],
    )
    path = root / f"mission_{case['case_id']}.json"
    write_json(
        path,
        {
            "schema_version": "1.0.0",
            "run_id": run_id,
            "topic": case["topic"],
            "objective": case["objective"],
            "policy_profile": "policy-research-fixture",
            "window": {
                "start_utc": "2024-04-01T00:00:00Z",
                "end_utc": "2024-04-02T00:00:00Z",
            },
            "region": {
                "label": case["region"],
                "geometry": {
                    "type": "Point",
                    "latitude": 40.5,
                    "longitude": -74.5,
                },
            },
            "hypotheses": [
                {
                    "title": case["decision_question"],
                    "statement": case["decision_question"],
                    "owner_role": "moderator",
                    "confidence": 0.55,
                }
            ],
            "artifact_imports": [
                {
                    "source_skill": "fetch-regulationsgov-comments",
                    "artifact_path": str(formal_path),
                    "source_mode": "policy-research-fixture",
                },
                {
                    "source_skill": case["environment_source"],
                    "artifact_path": str(environment_path),
                    "source_mode": "policy-research-fixture",
                },
            ],
            "source_requests": [
                {
                    "source_skill": "fetch-youtube-video-search",
                    "query_text": case["public_keyword"],
                    "artifact_capture": "stdout-json",
                    "fetch_argv": [sys.executable, str(public_script)],
                    "declared_side_effects": ["reads-artifacts"],
                    "requested_side_effect_approvals": [],
                }
            ],
            "source_selections": {
                "sociologist": {
                    "status": "complete",
                    "selected_sources": [
                        "fetch-youtube-video-search",
                        "fetch-regulationsgov-comments",
                    ],
                },
                "environmentalist": {
                    "status": "complete",
                    "selected_sources": [case["environment_source"]],
                },
            },
        },
    )
    return path


def first_query_basis(payload: dict[str, Any]) -> tuple[str, str]:
    results = payload.get("results", [])
    assert isinstance(results, list) and results
    result = results[0]
    assert isinstance(result, dict)
    evidence_refs = result.get("evidence_refs", [])
    assert isinstance(evidence_refs, list) and evidence_refs
    ref = evidence_refs[0]
    assert isinstance(ref, dict)
    return str(result["signal_id"]), str(ref["artifact_ref"])


def submit_finding_from_query(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    case_label: str,
    role: str,
    finding_kind: str,
    signal_id: str,
    evidence_ref: str,
) -> str:
    payload = run_kernel(
        "submit-finding-record",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--actor-role",
        role,
        "--agent-role",
        role,
        "--finding-kind",
        finding_kind,
        "--title",
        f"{case_label} {finding_kind}",
        "--summary",
        f"{case_label} finding is grounded in one item-level query result.",
        "--rationale",
        "The finding references a normalized signal evidence ref returned by a query skill.",
        "--confidence",
        "0.83",
        "--target-kind",
        "normalized-signal",
        "--target-id",
        signal_id,
        "--basis-object-id",
        signal_id,
        "--source-signal-id",
        signal_id,
        "--evidence-ref",
        evidence_ref,
        "--provenance-json",
        json.dumps(
            {"source": "policy-research-case-query", "case_label": case_label},
            sort_keys=True,
        ),
    )
    return str(payload["canonical_ids"][0])


def run_policy_research_case(root: Path, *, case: dict[str, str]) -> dict[str, Any]:
    run_id = f"run-policy-research-{case['case_id']}"
    round_id = f"round-policy-research-{case['case_id']}"
    run_dir = root / f"run-{case['case_id']}"
    mission_path = mission_file(root, run_id=run_id, round_id=round_id, case=case)

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
        "--orchestration-mode",
        "openclaw-agent",
    )
    run_script(
        script_path("prepare-round"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    import_payload = run_script(
        script_path("normalize-fetch-execution"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    execution = load_json(runtime_path(run_dir, f"import_execution_{round_id}.json"))

    public_query = run_script(
        script_path("query-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--keyword",
        case["public_keyword"],
    )
    formal_query = run_script(
        script_path("query-formal-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--keyword",
        case["formal_keyword"],
    )
    environment_query = run_script(
        script_path("query-environment-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--source-skill",
        case["environment_source"],
    )

    public_signal_id, public_ref = first_query_basis(public_query)
    formal_signal_id, formal_ref = first_query_basis(formal_query)
    environment_signal_id, environment_ref = first_query_basis(environment_query)

    finding_ids = [
        submit_finding_from_query(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            case_label=case["case_label"],
            role="public-discourse-investigator",
            finding_kind="public-discourse-finding",
            signal_id=public_signal_id,
            evidence_ref=public_ref,
        ),
        submit_finding_from_query(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            case_label=case["case_label"],
            role="formal-record-investigator",
            finding_kind="formal-record-finding",
            signal_id=formal_signal_id,
            evidence_ref=formal_ref,
        ),
        submit_finding_from_query(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            case_label=case["case_label"],
            role="environmental-investigator",
            finding_kind="environmental-evidence-finding",
            signal_id=environment_signal_id,
            evidence_ref=environment_ref,
        ),
    ]

    bundle_payload = run_kernel(
        "submit-evidence-bundle",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--actor-role",
        "environmental-investigator",
        "--agent-role",
        "environmental-investigator",
        "--bundle-kind",
        "cross-plane-evidence-bundle",
        "--title",
        f"{case['case_label']} cross-plane evidence bundle",
        "--summary",
        "Bundle combines public, formal, and environmental findings without helper-derived conclusions.",
        "--rationale",
        "Each bundled finding is backed by an item-level query evidence ref.",
        "--confidence",
        "0.86",
        "--target-kind",
        "round",
        "--target-id",
        round_id,
        "--finding-id",
        finding_ids[0],
        "--finding-id",
        finding_ids[1],
        "--finding-id",
        finding_ids[2],
        "--evidence-ref",
        public_ref,
        "--evidence-ref",
        formal_ref,
        "--evidence-ref",
        environment_ref,
        "--provenance-json",
        json.dumps(
            {"source": "policy-research-case-bundle", "case_label": case["case_label"]},
            sort_keys=True,
        ),
    )
    bundle_id = str(bundle_payload["canonical_ids"][0])

    review_payload = run_kernel(
        "post-review-comment",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--actor-role",
        "challenger",
        "--author-role",
        "challenger",
        "--review-kind",
        "scope-and-source-review",
        "--comment-text",
        "Check source coverage and scope before treating this basis as policy guidance.",
        "--target-kind",
        "evidence-bundle",
        "--target-id",
        bundle_id,
        "--response-to-id",
        bundle_id,
        "--evidence-ref",
        environment_ref,
        "--provenance-json",
        json.dumps({"source": "policy-research-challenger-review"}, sort_keys=True),
    )
    review_id = str(review_payload["canonical_ids"][0])

    challenge_payload = run_script(
        script_path("open-challenge-ticket"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--title",
        f"Audit {case['case_label']} scope",
        "--challenge-statement",
        "Verify that source limitations and scope are carried into the report.",
        "--target-claim-id",
        bundle_id,
        "--priority",
        "medium",
        "--owner-role",
        "challenger",
        "--linked-artifact-ref",
        environment_ref,
        "--evidence-bundle-id",
        bundle_id,
    )
    challenge_id = str(challenge_payload["canonical_ids"][0])
    run_script(
        script_path("close-challenge-ticket"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--ticket-id",
        challenge_id,
        "--resolution",
        "resolved",
        "--resolution-note",
        "The final report must carry the scope caveat and citation index.",
        "--closing-role",
        "moderator",
        "--related-task-id",
        review_id,
    )

    report_basis = submit_report_basis_records(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        target_id=bundle_id,
        evidence_ref=environment_ref,
        case_label=case["case_label"],
        agent_role="environmental-investigator",
        section_key="key-findings",
    )
    submit_ready_council_support(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        issue_id=bundle_id,
        evidence_ref=environment_ref,
        agent_role="moderator",
        materialize_readiness_summary=False,
    )
    readiness_request_id = request_and_approve_skill_approval(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name="summarize-round-readiness",
        requested_actor_role="moderator",
        rationale="Approve optional readiness summary for policy research fixture.",
        evidence_refs=[environment_ref],
        basis_object_ids=[bundle_id, report_basis["finding_id"], report_basis["bundle_id"]],
    )
    run_kernel(
        "run-skill",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--skill-name",
        "summarize-round-readiness",
        "--skill-approval-request-id",
        readiness_request_id,
    )
    readiness_consumption = run_kernel(
        "query-control-objects",
        "--run-dir",
        str(run_dir),
        "--object-kind",
        "skill-approval-consumption",
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--request-id",
        readiness_request_id,
    )
    request_and_approve_transition(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind="promote-evidence-basis",
        rationale="Moderator requests report-basis freeze for policy research case fixture.",
        evidence_refs=[environment_ref],
        basis_object_ids=[bundle_id, report_basis["finding_id"], report_basis["bundle_id"]],
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
    handoff_payload = run_script(
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

    for artifact_name in (
        f"reporting_handoff_{round_id}.json",
        f"council_decision_{round_id}.json",
        f"expert_report_sociologist_{round_id}.json",
        f"expert_report_environmentalist_{round_id}.json",
    ):
        reporting_path(run_dir, artifact_name).unlink()
    (run_dir / "runtime" / f"supervisor_state_{round_id}.json").unlink()
    (run_dir / "promotion" / f"promoted_evidence_basis_{round_id}.json").unlink()

    publication_payload = run_script(
        script_path("materialize-final-publication"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    publication = load_json(reporting_path(run_dir, f"final_publication_{round_id}.json"))
    return {
        "run_dir": run_dir,
        "run_id": run_id,
        "round_id": round_id,
        "import_payload": import_payload,
        "execution": execution,
        "handoff_payload": handoff_payload,
        "publication_payload": publication_payload,
        "publication": publication,
        "finding_ids": finding_ids,
        "bundle_id": bundle_id,
        "report_basis": report_basis,
        "readiness_request_id": readiness_request_id,
        "readiness_consumption": readiness_consumption,
    }


POLICY_RESEARCH_CASES = [
    {
        "case_id": "policy-dispute",
        "case_label": "Reservoir siting policy dispute",
        "topic": "Reservoir siting decision",
        "objective": "Evaluate a reservoir siting controversy for decision-maker reporting.",
        "decision_question": "Should the agency advance the reservoir option or compare alternatives first?",
        "region": "Fixture River Basin",
        "public_keyword": "reservoir",
        "public_title": "Residents question reservoir alternatives",
        "public_body": "Residents ask for habitat, relocation, and water-supply alternatives before a reservoir decision.",
        "formal_keyword": "alternatives",
        "formal_title": "Comment requests alternatives analysis",
        "formal_body": "The agency should compare reservoir alternatives, fish passage, and mitigation before selecting a project.",
        "environment_source": "fetch-usgs-water-iv",
    },
    {
        "case_id": "mixed-record",
        "case_label": "Port electrification public/formal mismatch",
        "topic": "Port electrification plan",
        "objective": "Compare formal permit records and public discourse about port emission controls.",
        "decision_question": "Which engagement and enforcement options should accompany the port electrification plan?",
        "region": "Fixture Harbor District",
        "public_keyword": "port",
        "public_title": "Community asks for port truck emission controls",
        "public_body": "Community members describe idling trucks and ask for electrification commitments near the port.",
        "formal_keyword": "electrification",
        "formal_title": "Permit comment on port electrification",
        "formal_body": "The permit record should explain electrification milestones and enforceable truck emission controls.",
        "environment_source": "fetch-openaq",
    },
    {
        "case_id": "empirical-event",
        "case_label": "Smoke episode response review",
        "topic": "Wildfire smoke response",
        "objective": "Assess a smoke episode response using public reports, formal notice, and observed air quality.",
        "decision_question": "What response actions should decision-makers consider after the smoke episode?",
        "region": "Fixture Metro Area",
        "public_keyword": "smoke",
        "public_title": "Residents report heavy wildfire smoke",
        "public_body": "Residents report smoke, visibility loss, and requests for clearer public-health guidance.",
        "formal_keyword": "health",
        "formal_title": "Comment requests clearer health guidance",
        "formal_body": "The response record should document health guidance, school closure criteria, and monitoring coverage.",
        "environment_source": "fetch-openaq",
    },
]


class PolicyResearchCaseFixtureTests(unittest.TestCase):
    def test_policy_research_cases_generate_db_backed_decision_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for case in POLICY_RESEARCH_CASES:
                with self.subTest(case=case["case_id"]):
                    result = run_policy_research_case(root, case=case)
                    execution = result["execution"]
                    publication = result["publication"]
                    report = publication["decision_maker_report"]

                    self.assertEqual(3, result["import_payload"]["summary"]["normalized_step_count"])
                    self.assertEqual(3, execution["completed_count"])
                    self.assertEqual({"import", "detached-fetch"}, {row["step_kind"] for row in execution["statuses"]})
                    self.assertEqual("reporting-ready", result["handoff_payload"]["summary"]["handoff_status"])
                    self.assertEqual(1, result["readiness_consumption"]["summary"]["returned_object_count"])
                    self.assertEqual(
                        result["readiness_request_id"],
                        result["readiness_consumption"]["objects"][0]["request_id"],
                    )
                    self.assertGreaterEqual(result["handoff_payload"]["summary"]["finding_count"], 1)
                    self.assertEqual("ready-for-release", result["publication_payload"]["summary"]["publication_status"])
                    self.assertEqual("release", publication["publication_posture"])
                    self.assertFalse(publication["observed_inputs"]["reporting_handoff_artifact_present"])
                    self.assertTrue(publication["observed_inputs"]["reporting_handoff_present"])
                    self.assertFalse(publication["observed_inputs"]["promotion_artifact_present"])
                    self.assertTrue(publication["observed_inputs"]["promotion_present"])
                    self.assertFalse(publication["observed_inputs"]["supervisor_state_artifact_present"])
                    self.assertTrue(publication["observed_inputs"]["supervisor_state_present"])
                    self.assertGreaterEqual(len(report["key_findings"]), 1)
                    self.assertGreaterEqual(len(report["evidence_index"]), 4)
                    self.assertIn("citation-index", publication["published_sections"])
                    self.assertIn("uncertainty-register", publication["published_sections"])
                    self.assertIn("remaining-disputes", publication["published_sections"])
                    self.assertTrue(
                        any(
                            item.get("object_kind") == "finding-record"
                            for item in publication["evidence_index"]
                        )
                    )
                    self.assertTrue(
                        any(
                            item.get("object_kind") == "evidence-bundle"
                            for item in publication["evidence_index"]
                        )
                    )


if __name__ == "__main__":
    unittest.main()

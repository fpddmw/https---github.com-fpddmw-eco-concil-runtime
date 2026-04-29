from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    primary_research_issue_id,
    primary_successor_evidence_ref,
    report_basis_path,
    request_and_approve_transition,
    reporting_path,
    run_kernel,
    run_script,
    runtime_path,
    runtime_src_path,
    script_path,
    seed_analysis_chain,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    store_council_proposal_records,
    store_readiness_opinion_records,
)

RUN_ID = "run-decision-trace-001"
ROUND_ID = "round-decision-trace-001"


def prepare_round_base(run_dir: Path, root: Path) -> dict[str, str]:
    outputs = seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
    coverage_ref = primary_successor_evidence_ref(outputs)
    claim_id = primary_research_issue_id(outputs)
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
        "Council inputs should drive reporting and trace publication for this round.",
        "--linked-artifact-ref",
        coverage_ref,
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
        claim_id,
        "--linked-artifact-ref",
        coverage_ref,
        "--confidence",
        "0.93",
    )
    return {
        "coverage_ref": coverage_ref,
        "claim_id": claim_id,
    }


def approve_report_basis_transition(run_dir: Path) -> str:
    return request_and_approve_transition(
        run_dir,
        run_id=RUN_ID,
        round_id=ROUND_ID,
        transition_kind="freeze-report-basis",
        rationale="Approve report_basis for decision-trace workflow coverage.",
    )


def prepare_reporting_chain(run_dir: Path) -> None:
    run_script(
        script_path("summarize-round-readiness"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    approve_report_basis_transition(run_dir)
    run_kernel(
        "supervise-round",
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
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
    run_script(
        script_path("draft-council-decision"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )


class DecisionTraceWorkflowTests(unittest.TestCase):
    def test_ready_round_persists_supporting_council_inputs_into_trace_and_publication(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seeded = prepare_round_base(run_dir, root)

            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "freeze-current-basis",
                            "agent_role": "moderator",
                            "target_kind": "round",
                            "target_id": ROUND_ID,
                            "publication_readiness": "ready",
                            "rationale": "Council wants the current evidence basis frozen and sent into reporting.",
                            "decision_source": "agent-council",
                            "confidence": 0.92,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]
            opinion_bundle = store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The controversy map is coherent enough to freeze the basis and publish.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        },
                        {
                            "agent_role": "challenger",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "No unresolved contradiction justifies another round.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        },
                    ],
                },
            )
            ready_opinion_ids = {
                opinion["opinion_id"] for opinion in opinion_bundle["opinions"]
            }

            prepare_reporting_chain(run_dir)
            report_basis = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("agent-council", report_basis["decision_source"])
            self.assertEqual(
                "council-judgement-freeze-v1",
                report_basis["basis_selection_mode"],
            )
            self.assertEqual(
                "gate-passed-with-council-support",
                report_basis["report_basis_resolution_mode"],
            )
            self.assertEqual([proposal_id], report_basis["supporting_proposal_ids"])
            self.assertEqual([], report_basis["rejected_proposal_ids"])
            self.assertEqual(ready_opinion_ids, set(report_basis["supporting_opinion_ids"]))
            self.assertEqual([], report_basis["rejected_opinion_ids"])
            self.assertEqual(1, report_basis["council_input_counts"]["proposal_count"])
            self.assertEqual(
                "explicit:publication_readiness",
                report_basis["proposal_resolution_records"][0]["resolution_mode"],
            )
            self.assertEqual(
                1,
                report_basis["council_input_counts"]["supporting_proposal_count"],
            )
            self.assertEqual(2, report_basis["council_input_counts"]["opinion_count"])
            self.assertEqual(
                2,
                report_basis["council_input_counts"]["supporting_opinion_count"],
            )

            run_script(
                script_path("draft-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--role",
                "sociologist",
            )
            run_script(
                script_path("draft-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--role",
                "environmentalist",
            )
            run_script(
                script_path("publish-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--role",
                "sociologist",
            )
            run_script(
                script_path("publish-expert-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--role",
                "environmentalist",
            )
            decision_publish = run_script(
                script_path("publish-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision = load_json(
                reporting_path(run_dir, f"council_decision_{ROUND_ID}.json")
            )

            self.assertEqual("ready", decision["publication_readiness"])
            self.assertEqual([proposal_id], decision["supporting_proposal_ids"])
            self.assertEqual([], decision["rejected_proposal_ids"])
            self.assertEqual(ready_opinion_ids, set(decision["supporting_opinion_ids"]))
            self.assertEqual([], decision["rejected_opinion_ids"])
            self.assertEqual(1, len(decision["decision_trace_ids"]))
            self.assertEqual(
                decision["decision_trace_ids"][0],
                decision_publish["summary"]["decision_trace_id"],
            )
            self.assertIn(proposal_id, decision["accepted_object_ids"])
            self.assertTrue(ready_opinion_ids.issubset(decision["accepted_object_ids"]))

            trace_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "decision-trace",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--decision-id",
                decision["decision_id"],
            )
            self.assertEqual(1, trace_query["summary"]["returned_object_count"])
            trace = trace_query["objects"][0]
            self.assertEqual(decision["decision_trace_ids"][0], trace["trace_id"])
            self.assertEqual("published", trace["status"])
            self.assertEqual("readiness-opinion", trace["selected_object_kind"])
            self.assertIn(trace["selected_object_id"], ready_opinion_ids)
            self.assertIn(proposal_id, trace["accepted_object_ids"])
            self.assertTrue(ready_opinion_ids.issubset(trace["accepted_object_ids"]))

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            publication = load_json(
                reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")
            )

            self.assertFalse(
                any(
                    warning["code"] == "missing-decision-trace"
                    for warning in publication_payload["warnings"]
                )
            )
            self.assertEqual(1, publication_payload["summary"]["decision_trace_count"])
            self.assertEqual(decision["decision_trace_ids"], publication["decision_trace_ids"])
            self.assertEqual(1, publication["decision_trace_count"])
            self.assertEqual(
                decision["decision_trace_ids"][0],
                publication["decision_traces"][0]["trace_id"],
            )
            self.assertEqual(
                "readiness-opinion",
                publication["decision_traces"][0]["selected_object_kind"],
            )
            self.assertIn(
                publication["decision_traces"][0]["selected_object_id"],
                ready_opinion_ids,
            )

    def test_legacy_named_report_basis_proposal_is_ignored_without_explicit_judgement(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seeded = prepare_round_base(run_dir, root)

            store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "ready-for-reporting",
                            "action_kind": "prepare-report-basis-freeze",
                            "agent_role": "moderator",
                            "target_kind": "round",
                            "target_id": ROUND_ID,
                            "rationale": "Legacy proposal naming should no longer imply report-basis support by itself.",
                            "decision_source": "agent-council",
                            "confidence": 0.61,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        }
                    ],
                },
            )
            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The round is ready, but report-basis support should come from explicit judgement fields.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        }
                    ],
                },
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
            report_basis_request_id = approve_report_basis_transition(run_dir)
            run_kernel(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
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
            report_basis = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("completed", report_basis_payload["status"])
            self.assertEqual("frozen", report_basis["report_basis_status"])
            self.assertEqual("readiness-opinion-gate", report_basis["report_basis_resolution_mode"])
            self.assertEqual([], report_basis["supporting_proposal_ids"])
            self.assertEqual([], report_basis["rejected_proposal_ids"])
            self.assertEqual(
                "ignored-implicit-report-basis-operation",
                report_basis["proposal_resolution_records"][0]["resolution_mode"],
            )
            self.assertEqual(
                "neutral",
                report_basis["proposal_resolution_records"][0]["disposition"],
            )
            self.assertIn(
                "implicit-report-basis-operation-without-explicit-signal",
                report_basis["proposal_resolution_records"][0]["relevance_reasons"],
            )
            self.assertEqual(
                1,
                report_basis["proposal_resolution_mode_counts"][
                    "ignored-implicit-report-basis-operation"
                ],
            )
            self.assertEqual(
                1,
                report_basis["council_input_counts"]["neutral_proposal_count"],
            )
            self.assertTrue(
                any(
                    warning["code"] == "ignored-implicit-report-basis-operation"
                    for warning in report_basis_payload["warnings"]
                )
            )

    def test_hold_round_persists_rejected_proposal_veto_into_gate_trace_and_publication(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seeded = prepare_round_base(run_dir, root)

            proposal_bundle = store_council_proposal_records(
                run_dir,
                proposal_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "proposals": [
                        {
                            "proposal_kind": "hold-current-round",
                            "agent_role": "challenger",
                            "target_kind": "round",
                            "target_id": ROUND_ID,
                            "report_basis_disposition": "hold",
                            "rationale": "A contradiction still needs explicit council review before publication can proceed.",
                            "decision_source": "agent-council",
                            "confidence": 0.88,
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        }
                    ],
                },
            )
            proposal_id = proposal_bundle["proposals"][0]["proposal_id"]
            opinion_bundle = store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The evidence basis would otherwise be ready for reporting.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        },
                        {
                            "agent_role": "environmentalist",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "No empirical blocker remains at the evidence layer.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        },
                    ],
                },
            )
            ready_opinion_ids = {
                opinion["opinion_id"] for opinion in opinion_bundle["opinions"]
            }

            prepare_reporting_chain(run_dir)
            gate = load_json(runtime_path(run_dir, f"report_basis_gate_{ROUND_ID}.json"))
            report_basis = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("report-basis-freeze-withheld", gate["gate_status"])
            self.assertEqual("council-veto", gate["report_basis_resolution_mode"])
            self.assertEqual([proposal_id], gate["rejected_proposal_ids"])
            self.assertEqual("agent-council", gate["decision_source"])
            self.assertEqual("withheld", report_basis["report_basis_status"])
            self.assertEqual("agent-council", report_basis["decision_source"])
            self.assertEqual([], report_basis["supporting_proposal_ids"])
            self.assertEqual([proposal_id], report_basis["rejected_proposal_ids"])
            self.assertEqual([], report_basis["supporting_opinion_ids"])
            self.assertEqual(ready_opinion_ids, set(report_basis["rejected_opinion_ids"]))
            self.assertEqual(
                1,
                report_basis["council_input_counts"]["rejected_proposal_count"],
            )

            decision_publish = run_script(
                script_path("publish-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision = load_json(
                reporting_path(run_dir, f"council_decision_{ROUND_ID}.json")
            )

            self.assertEqual("hold", decision["publication_readiness"])
            self.assertEqual([], decision["supporting_proposal_ids"])
            self.assertEqual([proposal_id], decision["rejected_proposal_ids"])
            self.assertEqual([], decision["supporting_opinion_ids"])
            self.assertEqual(ready_opinion_ids, set(decision["rejected_opinion_ids"]))
            self.assertIn(proposal_id, decision["rejected_object_ids"])
            self.assertEqual(
                decision["decision_trace_ids"][0],
                decision_publish["summary"]["decision_trace_id"],
            )

            trace_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "decision-trace",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--decision-id",
                decision["decision_id"],
            )
            self.assertEqual(1, trace_query["summary"]["returned_object_count"])
            trace = trace_query["objects"][0]
            self.assertEqual("withheld", trace["status"])
            self.assertEqual("proposal", trace["selected_object_kind"])
            self.assertEqual(proposal_id, trace["selected_object_id"])
            self.assertIn(proposal_id, trace["rejected_object_ids"])
            self.assertTrue(ready_opinion_ids.issubset(trace["rejected_object_ids"]))

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            publication = load_json(
                reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")
            )

            self.assertFalse(
                any(
                    warning["code"] == "missing-decision-trace"
                    for warning in publication_payload["warnings"]
                )
            )
            self.assertEqual("withhold", publication["publication_posture"])
            self.assertEqual("proposal", publication["decision_traces"][0]["selected_object_kind"])
            self.assertEqual(proposal_id, publication["decision_traces"][0]["selected_object_id"])

    def test_hold_round_persists_rejected_ready_opinion_into_trace_and_publication(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seeded = prepare_round_base(run_dir, root)

            opinion_bundle = store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The round could move forward from a release perspective.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        },
                        {
                            "agent_role": "challenger",
                            "readiness_status": "needs-more-data",
                            "sufficient_for_report_basis": False,
                            "rationale": "The round should stay open until the strongest contradiction is re-checked.",
                            "decision_source": "agent-council",
                            "basis_object_ids": [seeded["claim_id"]],
                            "provenance": {"source": "unit-test"},
                            "evidence_refs": [seeded["coverage_ref"]],
                            "lineage": [seeded["claim_id"]],
                        },
                    ],
                },
            )
            ready_opinion_ids = {
                opinion["opinion_id"]
                for opinion in opinion_bundle["opinions"]
                if opinion["readiness_status"] == "ready"
            }
            hold_opinion_ids = {
                opinion["opinion_id"]
                for opinion in opinion_bundle["opinions"]
                if opinion["readiness_status"] != "ready"
            }

            prepare_reporting_chain(run_dir)
            report_basis = load_json(
                report_basis_path(run_dir, f"frozen_report_basis_{ROUND_ID}.json")
            )

            self.assertEqual("withheld", report_basis["report_basis_status"])
            self.assertEqual("agent-council", report_basis["decision_source"])
            self.assertEqual([], report_basis["supporting_proposal_ids"])
            self.assertEqual(hold_opinion_ids, set(report_basis["supporting_opinion_ids"]))
            self.assertEqual(ready_opinion_ids, set(report_basis["rejected_opinion_ids"]))
            self.assertEqual(2, report_basis["council_input_counts"]["opinion_count"])
            self.assertEqual(
                1,
                report_basis["council_input_counts"]["supporting_opinion_count"],
            )
            self.assertEqual(
                1,
                report_basis["council_input_counts"]["rejected_opinion_count"],
            )

            decision_publish = run_script(
                script_path("publish-council-decision"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            decision = load_json(
                reporting_path(run_dir, f"council_decision_{ROUND_ID}.json")
            )

            self.assertEqual("hold", decision["publication_readiness"])
            self.assertEqual(hold_opinion_ids, set(decision["supporting_opinion_ids"]))
            self.assertEqual(ready_opinion_ids, set(decision["rejected_opinion_ids"]))
            self.assertEqual(1, len(decision["decision_trace_ids"]))
            self.assertEqual(
                decision["decision_trace_ids"][0],
                decision_publish["summary"]["decision_trace_id"],
            )
            self.assertTrue(hold_opinion_ids.issubset(decision["accepted_object_ids"]))
            self.assertEqual(ready_opinion_ids, set(decision["rejected_object_ids"]))

            trace_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "decision-trace",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--decision-id",
                decision["decision_id"],
            )
            self.assertEqual(1, trace_query["summary"]["returned_object_count"])
            trace = trace_query["objects"][0]
            self.assertEqual(decision["decision_trace_ids"][0], trace["trace_id"])
            self.assertEqual("withheld", trace["status"])
            self.assertEqual("readiness-opinion", trace["selected_object_kind"])
            self.assertIn(trace["selected_object_id"], ready_opinion_ids)
            self.assertEqual(ready_opinion_ids, set(trace["rejected_object_ids"]))
            self.assertTrue(hold_opinion_ids.issubset(trace["accepted_object_ids"]))

            publication_payload = run_script(
                script_path("materialize-final-publication"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            publication = load_json(
                reporting_path(run_dir, f"final_publication_{ROUND_ID}.json")
            )

            self.assertFalse(
                any(
                    warning["code"] == "missing-decision-trace"
                    for warning in publication_payload["warnings"]
                )
            )
            self.assertEqual(1, publication_payload["summary"]["decision_trace_count"])
            self.assertEqual("withhold", publication["publication_posture"])
            self.assertEqual(decision["decision_trace_ids"], publication["decision_trace_ids"])
            self.assertEqual(1, publication["decision_trace_count"])
            self.assertEqual(
                "readiness-opinion",
                publication["decision_traces"][0]["selected_object_kind"],
            )
            self.assertIn(
                publication["decision_traces"][0]["selected_object_id"],
                ready_opinion_ids,
            )


if __name__ == "__main__":
    unittest.main()

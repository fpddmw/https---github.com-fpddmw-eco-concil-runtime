from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    load_json,
    promotion_path,
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
    run_script(
        script_path("eco-derive-claim-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("eco-derive-observation-scope"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    coverage_payload = run_script(
        script_path("eco-score-evidence-coverage"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    coverage_ref = coverage_payload["artifact_refs"][0]["artifact_ref"]
    claim_id = outputs["cluster_claims"]["canonical_ids"][0]
    run_script(
        script_path("eco-post-board-note"),
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
        script_path("eco-update-hypothesis-status"),
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
        "--confidence",
        "0.93",
    )
    return {
        "coverage_ref": coverage_ref,
        "claim_id": claim_id,
    }


def prepare_reporting_chain(run_dir: Path) -> None:
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
        script_path("eco-materialize-reporting-handoff"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
    )
    run_script(
        script_path("eco-draft-council-decision"),
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
                            "sufficient_for_promotion": True,
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
                            "sufficient_for_promotion": True,
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
            promotion = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )

            self.assertEqual("agent-council", promotion["decision_source"])
            self.assertEqual(
                "council-judgement-freeze-v1",
                promotion["basis_selection_mode"],
            )
            self.assertEqual(
                "gate-passed-with-council-support",
                promotion["promotion_resolution_mode"],
            )
            self.assertEqual([proposal_id], promotion["supporting_proposal_ids"])
            self.assertEqual([], promotion["rejected_proposal_ids"])
            self.assertEqual(ready_opinion_ids, set(promotion["supporting_opinion_ids"]))
            self.assertEqual([], promotion["rejected_opinion_ids"])
            self.assertEqual(1, promotion["council_input_counts"]["proposal_count"])
            self.assertEqual(
                "explicit:publication_readiness",
                promotion["proposal_resolution_records"][0]["resolution_mode"],
            )
            self.assertEqual(
                1,
                promotion["council_input_counts"]["supporting_proposal_count"],
            )
            self.assertEqual(2, promotion["council_input_counts"]["opinion_count"])
            self.assertEqual(
                2,
                promotion["council_input_counts"]["supporting_opinion_count"],
            )

            run_script(
                script_path("eco-draft-expert-report"),
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
                script_path("eco-draft-expert-report"),
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
                script_path("eco-publish-expert-report"),
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
                script_path("eco-publish-expert-report"),
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
                script_path("eco-publish-council-decision"),
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
                script_path("eco-materialize-final-publication"),
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
                            "promotion_disposition": "hold",
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
                            "sufficient_for_promotion": True,
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
                            "sufficient_for_promotion": True,
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
            gate = load_json(runtime_path(run_dir, f"promotion_gate_{ROUND_ID}.json"))
            promotion = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )

            self.assertEqual("freeze-withheld", gate["gate_status"])
            self.assertEqual("council-veto", gate["promotion_resolution_mode"])
            self.assertEqual([proposal_id], gate["rejected_proposal_ids"])
            self.assertEqual("agent-council", gate["decision_source"])
            self.assertEqual("withheld", promotion["promotion_status"])
            self.assertEqual("agent-council", promotion["decision_source"])
            self.assertEqual([], promotion["supporting_proposal_ids"])
            self.assertEqual([proposal_id], promotion["rejected_proposal_ids"])
            self.assertEqual([], promotion["supporting_opinion_ids"])
            self.assertEqual(ready_opinion_ids, set(promotion["rejected_opinion_ids"]))
            self.assertEqual(
                1,
                promotion["council_input_counts"]["rejected_proposal_count"],
            )

            decision_publish = run_script(
                script_path("eco-publish-council-decision"),
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
                script_path("eco-materialize-final-publication"),
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
                            "sufficient_for_promotion": True,
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
                            "sufficient_for_promotion": False,
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
            promotion = load_json(
                promotion_path(run_dir, f"promoted_evidence_basis_{ROUND_ID}.json")
            )

            self.assertEqual("withheld", promotion["promotion_status"])
            self.assertEqual("agent-council", promotion["decision_source"])
            self.assertEqual([], promotion["supporting_proposal_ids"])
            self.assertEqual(hold_opinion_ids, set(promotion["supporting_opinion_ids"]))
            self.assertEqual(ready_opinion_ids, set(promotion["rejected_opinion_ids"]))
            self.assertEqual(2, promotion["council_input_counts"]["opinion_count"])
            self.assertEqual(
                1,
                promotion["council_input_counts"]["supporting_opinion_count"],
            )
            self.assertEqual(
                1,
                promotion["council_input_counts"]["rejected_opinion_count"],
            )

            decision_publish = run_script(
                script_path("eco-publish-council-decision"),
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
                script_path("eco-materialize-final-publication"),
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

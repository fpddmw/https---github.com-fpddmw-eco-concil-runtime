from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.contracts import canonical_contract  # noqa: E402
from eco_council_runtime.objects.council import (  # noqa: E402
    DYNAMIC_INVESTIGATION_OBJECT_KINDS,
    append_dynamic_investigation_object_record,
    query_council_objects,
)

RUN_ID = "run-dynamic-investigation-001"
ROUND_ID = "round-dynamic-investigation-001"


def minimal_council_program_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "object_kind": "council-investigation-program",
        "author_role": "moderator",
        "status": "proposed",
        "target_kind": "report-blueprint",
        "target_id": "report-blueprint-001",
        "rationale": "Synthesize council agenda questions without choosing acquisition routes.",
        "program_id": "council-program-unit-001",
        "mission_question": "What should the report answer?",
        "report_blueprint_ref": "report-blueprint:report-blueprint-001",
        "agent_position_refs": [
            "agent-position:env",
            "agent-position:social",
            "agent-position:challenger",
        ],
        "program_questions": ["What facts, governance records, and boundaries must be answered?"],
        "theme_threads": [
            {
                "theme_id": "theme-fact",
                "theme_question": "Which fact boundary is visible?",
                "claim_slots_supported": ["claim-slot-fact"],
            }
        ],
        "council_agenda_questions": ["Which fact boundary is visible?"],
        "agent_responsibility_boundaries": [
            "environmental-investigator: define fact-process claim basis and limitation boundary.",
            "challenger: review causal, denominator, and policy-evaluation overreach.",
        ],
        "round_sequence": [
            {
                "round_id": "round-002-fact",
                "round_title": "Fact issue council",
                "round_subtitle_question": "Which fact boundary is visible?",
                "round_mode": "issue-council",
                "round_category": "issue-deliberation",
                "active_theme_ids": ["theme-fact"],
                "agent_responsibility_boundaries": [
                    "environmental-investigator: define fact-process claim basis and limitation boundary.",
                    "challenger: review causal, denominator, and policy-evaluation overreach.",
                ],
                "round_internal_phases": [
                    "agenda-question",
                    "agent-acquisition-turns",
                    "agent-analysis-turns",
                    "progress-review",
                    "moderator-synthesis",
                ],
            }
        ],
        "round_internal_phase_model": [
            "round_internal_phases are descriptive organization hints only"
        ],
        "round_exit_criteria": [
            "Active theme status is recorded as supported, downgraded, scoped out, or carried forward."
        ],
        "downgrade_conditions": ["Missing denominator basis requires downgrade."],
        "supplemental_round_triggers": [
            "No reasonable in-round recovery remains for a named theme responsibility boundary."
        ],
        "source_autonomy_boundary": "Investigators choose acquisition routes in their work turns; this program does not choose them.",
        "policy_evaluation_boundary": "policy_evaluation_basis is a report synthesis boundary, not an acquisition lane.",
        "adoption_status": "proposed-for-council-use",
        "forbidden_scheduler_fields": [
            "source_family",
            "source_skill",
            "query",
            "query_parameters",
            "priority_score",
            "route_ranking",
            "source_priority",
            "scheduler_queue",
            "auto_execute",
        ],
        "evidence_refs": [],
        "provenance": {"source": "unit-test"},
    }
    payload.update(overrides)
    return payload


class DynamicInvestigationObjectTests(unittest.TestCase):
    def test_dynamic_investigation_contracts_are_thin_envelopes(self) -> None:
        for object_kind in DYNAMIC_INVESTIGATION_OBJECT_KINDS:
            contract = canonical_contract(object_kind)

            expected_id_field = {
                "report-blueprint": "blueprint_id",
                "report-outcome-contract": "contract_id",
                "investigation-theme": "theme_id",
                "council-investigation-program": "program_id",
                "dossier-program": "program_id",
                "theme-evidence-boundary-plan": "plan_id",
                "theme-progress-review": "review_id",
            }.get(object_kind, "object_id")
            self.assertEqual(expected_id_field, contract.id_field)
            self.assertEqual((), contract.required_number_fields)
            self.assertEqual((), contract.required_non_empty_list_fields)
            self.assertNotIn("confidence", contract.required_text_fields)
            if object_kind != "theme-progress-review":
                self.assertIn("object_kind", contract.required_text_fields)
            self.assertIn("evidence_refs", contract.required_list_fields)

    def test_dynamic_objects_store_and_query_without_evidence_scoring(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            plan_record = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="investigation-plan",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "investigation-plan",
                    "author_role": "moderator",
                    "status": "draft",
                    "target_kind": "mission",
                    "target_id": "mission-001",
                    "rationale": "Frame the next investigation pass without binding evidence uptake.",
                    "open_questions": ["What sources should agents inspect next?"],
                    "subissue_ids": ["subissue-air-quality"],
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )
            append_dynamic_investigation_object_record(
                run_dir,
                object_kind="evidence-request",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "evidence-request",
                    "author_role": "challenger",
                    "target_kind": "subissue",
                    "target_id": "subissue-air-quality",
                    "rationale": "Ask for contradicting records without ranking sources.",
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )

            query = query_council_objects(
                run_dir,
                object_kind="investigation-plan",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_role="moderator",
                status="draft",
                target_kind="mission",
                target_id="mission-001",
            )

            self.assertEqual(1, query["summary"]["returned_object_count"])
            plan = query["objects"][0]
            self.assertEqual(plan_record["object"]["object_id"], plan["object_id"])
            self.assertEqual("investigation-plan", plan["object_kind"])
            self.assertEqual([], plan["evidence_refs"])
            self.assertNotIn("confidence", plan)
            self.assertNotIn("priority", plan)

    def test_dynamic_objects_reject_heuristic_control_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            with self.assertRaisesRegex(ValueError, "heuristic/control field `priority`"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="round-brief",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "round-brief",
                        "author_role": "moderator",
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "Brief agents without imposing ordered agenda.",
                        "priority": "high",
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_council_program_contract_rejects_source_route_scheduler_fields(self) -> None:
        forbidden_fields = (
            "source_family",
            "source_skill",
            "query",
            "query_parameters",
            "route_ranking",
            "source_priority",
            "scheduler_queue",
            "auto_execute",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            stored = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="council-investigation-program",
                object_payload=minimal_council_program_payload(),
            )
            self.assertEqual(
                "council-investigation-program",
                stored["object"]["object_kind"],
            )
            self.assertEqual(
                "council-program-unit-001",
                stored["object"]["program_id"],
            )
            for field_name in forbidden_fields:
                with self.subTest(field_name=field_name):
                    payload = minimal_council_program_payload(
                        program_id=f"council-program-forbidden-{field_name}",
                        object_id=f"council-program-forbidden-{field_name}",
                    )
                    payload[field_name] = {"value": "precommitment"} if field_name == "query_parameters" else "precommitment"
                    with self.assertRaisesRegex(ValueError, "source/query/route/scheduler"):
                        append_dynamic_investigation_object_record(
                            run_dir,
                            object_kind="council-investigation-program",
                            object_payload=payload,
                        )

            payload = minimal_council_program_payload(
                program_id="council-program-forbidden-provenance",
                object_id="council-program-forbidden-provenance",
            )
            payload["provenance"] = {"source_skill": "hidden-precommitment"}
            with self.assertRaisesRegex(ValueError, "provenance.source_skill"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="council-investigation-program",
                    object_payload=payload,
                )

    def test_program_and_round_brief_reject_mechanical_responsibility_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            payload = minimal_council_program_payload(
                agent_responsibility_boundaries=[
                    "social-investigator must run skill fetch-youtube-comments then query parameters."
                ]
            )
            with self.assertRaisesRegex(ValueError, "cannot become a source/query/skill/task sequence"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="council-investigation-program",
                    object_payload=payload,
                )

            with self.assertRaisesRegex(ValueError, "cannot become a source/query/skill/task sequence"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="round-brief",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "round-brief",
                        "author_role": "moderator",
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "Brief must not become a task sequence.",
                        "program_id": "council-program-unit-001",
                        "round_subtitle_question": "Which boundary should this round resolve?",
                        "agent_responsibility_boundaries": [
                            "social-investigator must query a fixed provider sequence."
                        ],
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_round_brief_accepts_program_payload_but_rejects_phase_state_machine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            stored = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="round-brief",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "round-brief",
                    "author_role": "moderator",
                    "target_kind": "round",
                    "target_id": ROUND_ID,
                    "rationale": "Program-aware round brief for issue council.",
                    "program_id": "council-program-unit-001",
                    "round_title": "Fact issue council",
                    "round_subtitle_question": "Which fact boundary is visible?",
                    "round_mode": "issue-council",
                    "round_category": "issue-deliberation",
                    "active_theme_ids": ["theme-fact"],
                    "agent_responsibility_boundaries": [
                        "environmental-investigator: define fact-process claim basis and limitation boundary."
                    ],
                    "round_internal_phases": [
                        "agenda-question",
                        "agent-acquisition-turns",
                        "agent-analysis-turns",
                        "progress-review",
                        "moderator-synthesis",
                    ],
                    "expected_council_objects": [
                        "theme-evidence-boundary-plan",
                        "finding",
                        "theme-progress-review",
                        "round-synthesis",
                    ],
                    "round_exit_criteria": [
                        "Active theme status is recorded as support, downgrade, scope-out, or carry-forward."
                    ],
                    "in_round_feedback_triggers": [
                        "source owner records recovery status when claim strength would change."
                    ],
                    "supplemental_round_policy": "Supplement only after unresolved responsibility boundary or challenger concern survives in-round recovery.",
                    "forbidden_source_precommitments": [
                        "No preselected route, provider, query, or scheduler queue."
                    ],
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )
            self.assertEqual("council-program-unit-001", stored["object"]["program_id"])
            self.assertEqual(["theme-fact"], stored["object"]["active_theme_ids"])

            with self.assertRaisesRegex(ValueError, "not a runtime state machine"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="round-brief",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "round-brief",
                        "author_role": "moderator",
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "Invalid hard-gated phase brief.",
                        "round_subtitle_question": "Which boundary should this round resolve?",
                        "round_internal_phases": [{"phase_state": "must-complete"}],
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_supplemental_round_policy_rejects_ordinary_query_repair_trigger(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            with self.assertRaisesRegex(ValueError, "ordinary query repair"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="round-brief",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "round-brief",
                        "author_role": "moderator",
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "Invalid automatic supplemental trigger.",
                        "round_subtitle_question": "Which boundary should this round resolve?",
                        "supplemental_round_triggers": [
                            "Open supplemental round for zero result or query variant expansion."
                        ],
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_theme_boundary_plan_rejects_route_and_scheduler_precommitment_fields(self) -> None:
        base_payload = {
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "object_kind": "theme-evidence-boundary-plan",
            "author_role": "social-investigator",
            "target_kind": "investigation-theme",
            "target_id": "theme-public",
            "rationale": "Claim-basis boundary plan, not route plan.",
            "theme_id": "theme-public",
            "authoring_mode": "agent-authored",
            "sample_unit": "bounded sample",
            "downgrade_boundary": "examples only",
            "claim_slots_supported": ["claim-slot-public"],
            "evidence_obligations": ["record sample boundary and basis obligations"],
            "success_criteria": ["basis and denominator are visible"],
            "denominator_obligations": ["source-local denominator"],
            "failure_recovery_plan": ["downgrade or scope out after source-owner recovery note"],
            "forbidden_precommitments": ["no route precommitment"],
            "time_window": {},
            "evidence_refs": [],
            "provenance": {"source": "unit-test"},
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            for field_name in ("query", "query_parameters", "route_ranking", "scheduler_queue", "auto_execute"):
                with self.subTest(field_name=field_name):
                    payload = dict(base_payload)
                    payload["object_id"] = f"theme-plan-{field_name}"
                    payload[field_name] = {"value": "x"} if field_name == "query_parameters" else "x"
                    with self.assertRaisesRegex(ValueError, "source/query/route/scheduler"):
                        append_dynamic_investigation_object_record(
                            run_dir,
                            object_kind="theme-evidence-boundary-plan",
                            object_payload=payload,
                        )

    def test_theme_boundary_plan_derives_time_window_from_temporal_scope(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            record = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="theme-evidence-boundary-plan",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "theme-evidence-boundary-plan",
                    "author_role": "social-investigator",
                    "target_kind": "investigation-theme",
                    "target_id": "theme-public",
                    "rationale": "Claim-basis boundary plan, not route plan.",
                    "theme_id": "theme-public",
                    "authoring_mode": "agent-authored",
                    "sample_unit": "official/governance record",
                    "temporal_scope": "Acute June 2023 NYC smoke episode",
                    "downgrade_boundary": "examples only",
                    "claim_slots_supported": ["claim-slot-public"],
                    "evidence_obligations": ["record sample boundary and basis obligations"],
                    "success_criteria": ["basis and denominator are visible"],
                    "denominator_obligations": ["source-local denominator"],
                    "failure_recovery_plan": ["downgrade or scope out after source-owner recovery note"],
                    "forbidden_precommitments": ["no route precommitment"],
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )
        self.assertEqual(
            "Acute June 2023 NYC smoke episode",
            record["object"]["time_window"]["label"],
        )
        self.assertEqual("temporal_scope", record["object"]["time_window"]["source_field"])

    def test_source_acquisition_proposal_is_thin_and_queryable_by_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            record = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="source-acquisition-proposal",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "source-acquisition-proposal",
                    "author_role": "environmental-investigator",
                    "source_skill": "fetch-open-meteo-historical",
                    "query_parameters": {
                        "latitude": 40.7128,
                        "longitude": -74.0060,
                        "start_date": "2023-06-07",
                        "end_date": "2023-06-08",
                    },
                    "declared_side_effects": [
                        "network-external",
                        "writes-artifacts",
                    ],
                    "requested_side_effect_approvals": ["network-external"],
                    "target_kind": "evidence-request",
                    "target_id": "evidence-request-weather-context",
                    "rationale": "Record the agent-selected weather context source without ranking it.",
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )

            query = query_council_objects(
                run_dir,
                object_kind="source-acquisition-proposal",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                source_skill="fetch-open-meteo-historical",
                target_evidence_request_id="evidence-request-weather-context",
            )

            self.assertEqual(1, query["summary"]["returned_object_count"])
            proposal = query["objects"][0]
            self.assertEqual(record["object"]["object_id"], proposal["object_id"])
            self.assertEqual(record["object"]["proposal_id"], proposal["proposal_id"])
            self.assertEqual("source-acquisition-proposal", proposal["object_kind"])
            self.assertEqual("proposed", proposal["status"])
            self.assertEqual(
                "fetch-open-meteo-historical",
                proposal["source_skill"],
            )
            self.assertEqual([], proposal["evidence_refs"])
            self.assertNotIn("score", proposal)
            self.assertNotIn("rank", proposal)
            self.assertNotIn("priority", proposal)

            status_surface = run_kernel(
                "show-source-acquisition-intents",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "environmental-investigator",
                "--source-skill",
                "fetch-open-meteo-historical",
                "--status",
                "proposed",
                "--target-evidence-request-id",
                "evidence-request-weather-context",
            )
            submit_template = status_surface["commands"]["submit_source_acquisition_proposal_template"]
            self.assertEqual("environmental-investigator", status_surface["filters"]["author_role"])
            self.assertEqual("fetch-open-meteo-historical", status_surface["filters"]["source_skill"])
            self.assertEqual("proposed", status_surface["filters"]["status"])
            self.assertEqual(
                "evidence-request-weather-context",
                status_surface["filters"]["target_evidence_request_id"],
            )
            self.assertIn("--author-role '<agent_role>'", submit_template)
            self.assertIn("--query-parameters-json", submit_template)
            self.assertIn("--rationale", submit_template)
            self.assertNotIn("--proposal-text", submit_template)
            self.assertEqual(1, len(status_surface["objects"]))
            execution_surface = status_surface["objects"][0]["source_execution_surface"]
            self.assertEqual("fetch-open-meteo-historical", execution_surface["source_skill"])
            self.assertTrue(execution_surface["provider_modes"])
            self.assertTrue(execution_surface["fetch_command_templates"])
            self.assertIn("40.7128,-74.006", execution_surface["fetch_command_templates"][0])
            self.assertIn("--start-date 2023-06-07", execution_surface["fetch_command_templates"][0])
            self.assertIn("--end-date 2023-06-08", execution_surface["fetch_command_templates"][0])
            self.assertIn(
                "--allow-side-effect network-external",
                execution_surface["fetch_command_templates"][0],
            )
            self.assertNotIn(
                "--allow-side-effect writes-artifacts",
                execution_surface["fetch_command_templates"][0],
            )
            self.assertNotIn(
                "--allow-side-effect",
                execution_surface["preflight_fetch_command_templates"][0],
            )
            self.assertEqual(
                ["writes-artifacts"],
                execution_surface["missing_requested_side_effect_approvals"],
            )
            self.assertEqual(
                "2023-06-07",
                execution_surface["query_parameters"]["start_date"],
            )
            self.assertIn("update-source-acquisition-proposal-status", execution_surface["status_update_command_template"])
            self.assertIn(
                proposal["object_id"],
                execution_surface["status_update_command_template"],
            )
            self.assertIn(
                "link-source-acquisition-execution",
                execution_surface["link_execution_lineage_command_template"],
            )
            self.assertIn(
                proposal["object_id"],
                execution_surface["link_execution_lineage_command_template"],
            )
            self.assertIn("normalize-fetch-execution", execution_surface["normalize_fetch_execution_command"])

            status_update = run_script(
                script_path("update-source-acquisition-proposal-status"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--object-id",
                proposal["object_id"],
                "--status",
                "executed",
                "--actor-role",
                "environmental-investigator",
                "--status-rationale",
                "Fetch completed; agents still decide whether to use the evidence.",
                "--evidence-ref",
                "receipt://fetch/open-meteo/001",
                "--lineage-id",
                "fetch-execution-open-meteo-001",
                "--provenance-json",
                json.dumps({"source": "unit-test-status"}, ensure_ascii=True, sort_keys=True),
            )
            self.assertEqual("completed", status_update["status"])
            self.assertEqual("executed", status_update["summary"]["status"])
            self.assertEqual("proposed", status_update["summary"]["previous_status"])

            executed_query = query_council_objects(
                run_dir,
                object_kind="source-acquisition-proposal",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                status="executed",
                source_skill="fetch-open-meteo-historical",
            )
            self.assertEqual(1, executed_query["summary"]["returned_object_count"])
            executed = executed_query["objects"][0]
            self.assertEqual(proposal["object_id"], executed["object_id"])
            self.assertEqual("executed", executed["status"])
            self.assertEqual(
                "environmental-investigator",
                executed["status_updated_by_role"],
            )
            self.assertIn("receipt://fetch/open-meteo/001", executed["evidence_refs"])
            self.assertIn("fetch-execution-open-meteo-001", executed["lineage"])
            self.assertEqual("unit-test-status", executed["provenance"]["source"])
            self.assertEqual(1, len(executed["status_updates"]))
            self.assertNotIn("score", executed)
            self.assertNotIn("rank", executed)
            self.assertNotIn("priority", executed)

            lineage_link = run_script(
                script_path("link-source-acquisition-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--object-id",
                proposal["object_id"],
                "--actor-role",
                "environmental-investigator",
                "--status-rationale",
                "Link the fetch and normalized signal refs without deciding evidence acceptance.",
                "--fetch-receipt-ref",
                "receipt://fetch/open-meteo/001",
                "--normalization-receipt-ref",
                "receipt://normalize/open-meteo/001",
                "--normalized-signal-ref",
                "signal://open-meteo/weather/001",
                "--artifact-ref",
                "artifact://fetch/open-meteo/raw-001",
                "--provenance-json",
                json.dumps({"source": "unit-test-lineage"}, ensure_ascii=True, sort_keys=True),
            )
            self.assertEqual("completed", lineage_link["status"])
            self.assertEqual("normalized", lineage_link["summary"]["status"])
            self.assertEqual(1, lineage_link["summary"]["fetch_receipt_ref_count"])
            self.assertEqual(1, lineage_link["summary"]["normalization_receipt_ref_count"])
            self.assertEqual(1, lineage_link["summary"]["normalized_signal_ref_count"])

            linked_query = query_council_objects(
                run_dir,
                object_kind="source-acquisition-proposal",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                status="normalized",
                source_skill="fetch-open-meteo-historical",
            )
            linked = linked_query["objects"][0]
            self.assertEqual(1, len(linked["execution_links"]))
            self.assertIn("receipt://fetch/open-meteo/001", linked["fetch_receipt_refs"])
            self.assertIn(
                "receipt://normalize/open-meteo/001",
                linked["normalization_receipt_refs"],
            )
            self.assertIn("signal://open-meteo/weather/001", linked["normalized_signal_refs"])
            self.assertIn(
                "artifact://fetch/open-meteo/raw-001",
                linked["execution_artifact_refs"],
            )
            self.assertIn("signal://open-meteo/weather/001", linked["evidence_refs"])
            self.assertNotIn("score", linked)
            self.assertNotIn("rank", linked)
            self.assertNotIn("priority", linked)

            appended_lineage_link = run_script(
                script_path("link-source-acquisition-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--object-id",
                proposal["object_id"],
                "--actor-role",
                "environmental-investigator",
                "--status-rationale",
                "Append a second normalized signal ref without reusing the first link receipt identity.",
                "--fetch-receipt-ref",
                "receipt://fetch/open-meteo/001",
                "--normalization-receipt-ref",
                "receipt://normalize/open-meteo/001",
                "--normalized-signal-ref",
                "signal://open-meteo/weather/002",
                "--provenance-json",
                json.dumps({"source": "unit-test-lineage"}, ensure_ascii=True, sort_keys=True),
            )
            self.assertNotEqual(lineage_link["receipt_id"], appended_lineage_link["receipt_id"])
            self.assertEqual("normalized", appended_lineage_link["summary"]["status"])

            proposed_query = query_council_objects(
                run_dir,
                object_kind="source-acquisition-proposal",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                status="proposed",
            )
            self.assertEqual(0, proposed_query["summary"]["returned_object_count"])

            executed_surface = run_kernel(
                "show-source-acquisition-intents",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--status",
                "normalized",
            )
            self.assertIn("executed", executed_surface["supported_statuses"])
            self.assertIn("normalized", executed_surface["supported_statuses"])
            self.assertIn(
                "update-source-acquisition-proposal-status",
                executed_surface["commands"]["update_source_acquisition_proposal_status_template"],
            )
            self.assertIn(
                "link-source-acquisition-execution",
                executed_surface["commands"]["link_source_acquisition_execution_template"],
            )

    def test_source_acquisition_proposal_treats_false_approval_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            record = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="source-acquisition-proposal",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "source-acquisition-proposal",
                    "author_role": "environmental-investigator",
                    "source_skill": "fetch-open-meteo-historical",
                    "query_parameters": {"start_date": "2023-06-07"},
                    "declared_side_effects": ["network-external"],
                    "requested_side_effect_approvals": ["false"],
                    "rationale": "Non-executing proposal with no approval requested.",
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )
        self.assertEqual([], record["object"]["requested_side_effect_approvals"])

    def test_source_acquisition_proposal_rejects_wrong_role_for_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            with self.assertRaisesRegex(ValueError, "cannot execute fetch-open-meteo-historical"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="source-acquisition-proposal",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "source-acquisition-proposal",
                        "author_role": "social-investigator",
                        "source_skill": "fetch-open-meteo-historical",
                        "query_parameters": {},
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "This role should not acquire environmental weather data.",
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_kernel_submits_and_queries_dynamic_investigation_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            submit_payload = run_kernel(
                "submit-dynamic-investigation-object",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "social-investigator",
                "--object-kind",
                "agent-position",
                "--author-role",
                "social-investigator",
                "--target-kind",
                "subissue",
                "--target-id",
                "subissue-public-record",
                "--rationale",
                "State a provisional social-record position without scoring evidence.",
                "--evidence-ref",
                "evidence://public-record/001",
                "--payload-json",
                json.dumps(
                    {
                        "position_text": "Public records appear relevant, pending agent synthesis.",
                        "source_object_ids": ["formal-record-001"],
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            object_id = submit_payload["canonical_ids"][0]

            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "agent-position",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "social-investigator",
                "--target-kind",
                "subissue",
                "--target-id",
                "subissue-public-record",
            )

            self.assertEqual(1, query_payload["summary"]["returned_object_count"])
            self.assertEqual(object_id, query_payload["objects"][0]["object_id"])
            self.assertEqual("agent-position", query_payload["objects"][0]["object_kind"])
            self.assertEqual(
                ["evidence://public-record/001"],
                query_payload["objects"][0]["evidence_refs"],
            )


if __name__ == "__main__":
    unittest.main()

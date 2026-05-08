from __future__ import annotations

import contextlib
import importlib
import io
import sys
import unittest
from pathlib import Path

from _workflow_support import runtime_src_path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = runtime_src_path()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))


FACADE_PUBLIC_SYMBOLS = {
    "eco_council_runtime.kernel.planes.deliberation_plane": (
        "connect_db",
        "load_schema_status",
        "commit_board_mutation",
        "load_round_snapshot",
        "stable_hash",
        "store_reporting_handoff_record",
        "store_round_transition_record",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane": (
        "connect_db",
        "sync_analysis_result_set",
        "query_analysis_result_sets",
        "query_analysis_result_items",
        "query_spatiotemporal_relation_cues",
        "sync_spatiotemporal_relation_cue_result_set",
    ),
    "eco_council_runtime.optional_analysis": (
        "run_aggregate_environment_evidence",
        "run_review_fact_check_evidence_scope",
        "run_detect_temporal_cooccurrence_cues",
        "run_review_spatiotemporal_relation_alternatives",
    ),
    "eco_council_runtime.objects.council": (
        "connect_db",
        "append_finding_record",
        "append_evidence_bundle_record",
        "store_council_proposal_records",
        "query_council_objects",
    ),
    "eco_council_runtime.objects.analysis": (
        "canonical_evidence_refs",
        "build_heuristic_wrapper_provenance",
    ),
    "eco_council_runtime.contracts": (
        "canonical_contract",
        "canonical_contracts_for_plane",
        "contract_field_group",
        "contract_field_groups",
        "validate_canonical_payload",
    ),
    "eco_council_runtime.kernel.cli": (
        "build_parser",
        "init_run",
        "main",
        "show_run_state",
    ),
    "eco_council_runtime.kernel.operator.surfaces": (
        "build_reporting_surface",
        "load_controller_state_wrapper",
        "load_reporting_handoff_wrapper",
    ),
    "eco_council_runtime.kernel.planes.signal": (
        "connect_db",
        "finalize_normalization",
        "resolved_canonical_object_kind",
    ),
    "eco_council_runtime.kernel.execution.controller": (
        "controller_stage_skill_args",
        "run_governed_execution_round",
        "run_governed_execution_round_with_contract_mode",
    ),
}


SPLIT_MODULE_PUBLIC_SYMBOLS = {
    "eco_council_runtime.kernel.planes.deliberation_plane_schema": (
        "SCHEMA_SQL",
        "connect_db",
        "ensure_schema_migrations",
        "load_schema_status",
        "resolve_db_path",
    ),
    "eco_council_runtime.kernel.planes.deliberation_plane_rows": (
        "fetch_json_rows",
        "fetch_runtime_control_freeze",
        "latest_json_row",
        "payload_from_db_row",
        "json_text",
        "maybe_text",
        "write_board_event_row",
        "event_row_from_payload",
        "round_transition_row_from_payload",
    ),
    "eco_council_runtime.kernel.planes.deliberation_board_state": (
        "bootstrap_board_state",
        "commit_board_mutation",
        "fetch_round_events",
        "fetch_round_state",
        "iter_round_transition_rows",
        "load_round_snapshot",
        "store_round_transition_record",
        "sync_board_to_deliberation_plane",
    ),
    "eco_council_runtime.kernel.planes.deliberation_actions": (
        "load_moderator_action_records",
        "load_round_readiness_assessment",
        "store_falsification_probe_records",
        "store_moderator_action_records",
        "store_round_readiness_assessment",
    ),
    "eco_council_runtime.kernel.planes.deliberation_reporting_records": (
        "REPORT_AGENT_ROLES",
        "apply_reporting_contract_defaults",
        "ensure_dict_fields",
        "ensure_list_fields",
        "load_report_basis_freeze_record",
        "load_reporting_handoff_record",
        "store_council_decision_record",
        "store_expert_report_record",
        "store_final_publication_record",
        "store_report_basis_freeze_record",
        "store_reporting_handoff_record",
    ),
    "eco_council_runtime.kernel.planes.deliberation_runtime_control": (
        "load_controller_snapshot_record",
        "load_gate_snapshot_record",
        "load_governed_execution_control_state",
        "load_runtime_control_freeze_record",
        "load_supervisor_snapshot_record",
        "store_orchestration_plan_record",
        "store_round_task_snapshot",
        "store_runtime_control_freeze_record",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane_schema": (
        "SCHEMA_SQL",
        "connect_db",
        "ensure_analysis_plane_schema",
        "resolve_db_path",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane_contracts": (
        "ANALYSIS_KIND_SPATIOTEMPORAL_RELATION_CUE",
        "analysis_config",
        "analysis_kind_governance",
        "analysis_kind_names",
        "maybe_text",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane_support": (
        "build_result_contract",
        "empty_result_contract",
        "load_json_if_exists",
        "load_result_contract",
        "normalized_artifact_ref",
        "planned_item_rows",
        "resolve_artifact_path",
        "unique_artifact_refs",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane_results": (
        "load_analysis_result_context",
        "sync_analysis_result_set",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane_queries": (
        "query_analysis_result_items",
        "query_analysis_result_sets",
        "query_spatiotemporal_relation_cues",
    ),
    "eco_council_runtime.kernel.planes.analysis_plane_contexts": (
        "load_spatiotemporal_relation_cue_context",
        "sync_spatiotemporal_relation_cue_result_set",
    ),
    "eco_council_runtime.kernel.operator.run_state_view": (
        "benchmark_operator_view",
        "governed_execution_operator_view",
        "operations_state",
        "post_round_operator_view",
        "reporting_operator_view",
        "reporting_state_for_round",
        "show_run_state",
        "transition_request_state",
    ),
    "eco_council_runtime.kernel.operator.cli_parser": (
        "add_actor_role_arg",
        "add_admission_policy_args",
        "add_analysis_query_args",
        "add_control_query_args",
        "add_council_query_args",
        "add_execution_policy_args",
        "add_reporting_query_args",
        "build_parser",
    ),
    "eco_council_runtime.kernel.operator.cli_runtime_commands": (
        "command_access_failure",
        "handle_early_runtime_command",
        "handle_runtime_command",
        "init_run",
        "parse_json_object_arg",
        "pretty_json",
        "write_command_artifact",
    ),
    "eco_council_runtime.objects.analysis.common": (
        "build_heuristic_wrapper_provenance",
        "canonical_evidence_refs",
        "helper_governance_metadata",
        "maybe_text",
        "merged_lineage",
        "unique_artifact_refs",
    ),
    "eco_council_runtime.objects.analysis.signals": (
        "normalize_claim_candidate_payload",
        "normalize_claim_cluster_payload",
        "normalize_claim_scope_payload",
    ),
    "eco_council_runtime.objects.analysis.issues": (
        "normalize_actor_profile_payload",
        "normalize_concern_facet_payload",
        "normalize_controversy_map_payload",
        "normalize_evidence_citation_type_payload",
        "normalize_issue_cluster_payload",
        "normalize_stance_group_payload",
    ),
    "eco_council_runtime.objects.analysis.verification": (
        "normalize_verifiability_assessment_payload",
        "normalize_verification_route_payload",
    ),
    "eco_council_runtime.objects.analysis.relations": (
        "normalize_diffusion_edge_payload",
        "normalize_formal_public_link_payload",
        "normalize_representation_gap_payload",
        "normalize_spatiotemporal_relation_cue_payload",
    ),
    "eco_council_runtime.objects.council.schema": (
        "SCHEMA_SQL",
        "connect_db",
    ),
    "eco_council_runtime.objects.council.payloads": (
        "normalized_discussion_message_payload",
        "normalized_evidence_bundle_payload",
        "normalized_finding_payload",
        "normalized_proposal_payload",
        "normalized_readiness_opinion_payload",
    ),
    "eco_council_runtime.objects.council.rows": (
        "finding_row_from_payload",
        "proposal_row_from_payload",
        "write_council_proposal_row",
        "write_finding_row",
    ),
    "eco_council_runtime.objects.council.store": (
        "append_finding_record",
        "append_review_comment_record",
        "store_council_proposal_records",
        "store_readiness_opinion_records",
    ),
    "eco_council_runtime.objects.council.decision_traces": (
        "decision_trace_row_from_payload",
        "normalized_decision_trace_payload",
        "store_decision_trace_records",
    ),
    "eco_council_runtime.objects.council.query": (
        "QUERY_CONFIGS",
        "council_queryable_object_kinds",
        "query_council_objects",
    ),
    "eco_council_runtime.contracts.types": (
        "CanonicalContract",
        "ContractFieldGroup",
        "CONTRACT_FIELD_GROUPS",
        "PLANE_ANALYSIS",
        "PLANE_DELIBERATION",
        "PLANE_REPORTING",
        "PLANE_RUNTIME",
        "PLANE_SIGNAL",
        "contract_field_group",
        "contract_field_groups",
    ),
    "eco_council_runtime.contracts.signal": (
        "ENVIRONMENT_SIGNAL_TAXONOMY_VERSION",
        "SIGNAL_CONTRACTS",
        "environment_signal_taxonomy_metadata",
    ),
    "eco_council_runtime.contracts.analysis": (
        "ANALYSIS_CONTRACTS",
    ),
    "eco_council_runtime.contracts.deliberation": (
        "DELIBERATION_CONTRACTS",
    ),
    "eco_council_runtime.contracts.runtime": (
        "RUNTIME_CONTRACTS",
    ),
    "eco_council_runtime.contracts.reporting": (
        "REPORTING_CONTRACTS",
    ),
    "eco_council_runtime.contracts.registry": (
        "CANONICAL_CONTRACTS",
        "canonical_contract",
        "canonical_contracts_for_plane",
        "validate_canonical_payload",
    ),
    "eco_council_runtime.kernel.governance.transition_requests.common": (
        "TRANSITION_KIND_CLOSE_ROUND",
        "TRANSITION_KIND_FREEZE_REPORT_BASIS",
        "TRANSITION_KIND_OPEN_INVESTIGATION_ROUND",
        "normalize_transition_kind",
        "transition_kind_spec",
    ),
    "eco_council_runtime.kernel.governance.transition_requests.payloads": (
        "transition_approval_payload",
        "transition_rejection_payload",
        "transition_request_payload",
    ),
    "eco_council_runtime.kernel.governance.transition_requests.rows": (
        "transition_approval_row_from_payload",
        "transition_request_row_from_payload",
        "write_transition_request_row",
    ),
    "eco_council_runtime.kernel.governance.transition_requests.store": (
        "approve_transition_request",
        "load_transition_request",
        "mark_transition_request_committed",
        "resolve_transition_request_for_execution",
        "store_transition_request",
    ),
    "eco_council_runtime.kernel.governance.skill_approvals.common": (
        "OBJECT_KIND_SKILL_APPROVAL_REQUEST",
        "REQUEST_STATUS_APPROVED",
        "REQUEST_STATUS_CONSUMED",
        "REQUEST_STATUS_PENDING",
    ),
    "eco_council_runtime.kernel.governance.skill_approvals.payloads": (
        "skill_approval_consumption_payload",
        "skill_approval_payload",
        "skill_approval_rejection_payload",
        "skill_approval_request_payload",
    ),
    "eco_council_runtime.kernel.governance.skill_approvals.rows": (
        "skill_approval_request_row_from_payload",
        "skill_approval_row_from_payload",
        "write_skill_approval_request_row",
    ),
    "eco_council_runtime.kernel.governance.skill_approvals.store": (
        "approve_skill_approval_request",
        "load_skill_approval_request",
        "mark_skill_approval_consumed",
        "resolve_skill_approval_for_execution",
        "store_skill_approval_request",
    ),
    "eco_council_runtime.kernel.operator.admission_policy": (
        "admission_error_code",
        "evaluate_execution_admission",
        "load_admission_policy",
        "materialize_admission_policy",
    ),
    "eco_council_runtime.kernel.operator.dead_letters": (
        "classify_failure",
        "load_dead_letters",
        "materialize_dead_letter",
        "operator_resolution_steps",
    ),
    "eco_council_runtime.kernel.operator.runtime_health": (
        "materialize_runtime_health",
        "refresh_runtime_surfaces",
        "runtime_health_payload",
    ),
    "eco_council_runtime.kernel.operator.runbook": (
        "materialize_operator_runbook",
        "operator_runbook_markdown",
    ),
    "eco_council_runtime.kernel.execution.executor_common": (
        "SkillExecutionError",
        "json_hash",
        "new_runtime_event_id",
        "retryable_return_code",
    ),
    "eco_council_runtime.kernel.execution.executor_command_hints": (
        "skill_command_hint",
    ),
    "eco_council_runtime.kernel.execution.executor_failures": (
        "extract_dead_letter_id",
        "refresh_runtime_surfaces_safely",
        "structured_failure",
    ),
    "eco_council_runtime.kernel.operator.surfaces.common": (
        "maybe_text",
        "orphaned_artifact_wrapper",
        "resolve_path",
    ),
    "eco_council_runtime.kernel.operator.surfaces.reporting": (
        "build_reporting_surface",
        "enrich_reporting_record_payload",
        "enrich_supervisor_reporting_payload",
    ),
    "eco_council_runtime.kernel.operator.surfaces.investigation": (
        "load_falsification_probe_wrapper",
        "load_next_actions_wrapper",
        "load_report_basis_freeze_wrapper",
        "load_round_readiness_wrapper",
    ),
    "eco_council_runtime.kernel.operator.surfaces.execution": (
        "load_controller_state_wrapper",
        "load_orchestration_plan_wrapper",
        "load_report_basis_gate_wrapper",
        "load_supervisor_state_wrapper",
    ),
    "eco_council_runtime.kernel.operator.surfaces.publication": (
        "load_council_decision_wrapper",
        "load_expert_report_wrapper",
        "load_final_publication_wrapper",
        "load_reporting_handoff_wrapper",
    ),
    "eco_council_runtime.kernel.planes.signal.common": (
        "file_sha256",
        "json_text",
        "maybe_text",
        "stable_hash",
    ),
    "eco_council_runtime.kernel.planes.signal.schema": (
        "SCHEMA_SQL",
        "connect_db",
        "ensure_signal_plane_schema",
        "resolve_db_path",
    ),
    "eco_council_runtime.kernel.planes.signal.metadata": (
        "default_canonical_object_kind",
        "enrich_signal_metadata_fields",
        "resolved_canonical_object_kind",
    ),
    "eco_council_runtime.kernel.planes.signal.store": (
        "delete_existing_rows",
        "insert_signals",
        "replace_signal_index_rows",
    ),
    "eco_council_runtime.kernel.planes.signal.finalize": (
        "base_signal",
        "finalize_normalization",
        "finalize_normalization_streaming",
    ),
    "eco_council_runtime.kernel.execution.controller.artifacts": (
        "governed_execution_artifact_paths",
        "persist_controller_state",
    ),
    "eco_council_runtime.kernel.execution.controller.planning_adapters": (
        "agent_orchestration_requested",
        "execute_gate_step",
        "planning_bundle",
    ),
    "eco_council_runtime.kernel.execution.controller.transition_planning": (
        "approved_transition_request_planning",
        "controller_stage_skill_args",
        "inspection_only_planning",
    ),
    "eco_council_runtime.optional_analysis.support": (
        "helper_metadata",
        "lineage_from_signals",
        "query_signals",
        "refs_from_signals",
        "safe_board_handoff",
    ),
    "eco_council_runtime.optional_analysis.environment_evidence": (
        "run_aggregate_environment_evidence",
    ),
    "eco_council_runtime.optional_analysis.scope_review": (
        "STRUCTURED_VERIFICATION_SCOPE_FIELDS",
        "build_structured_verification_scope",
        "run_review_fact_check_evidence_scope",
    ),
    "eco_council_runtime.optional_analysis.research_issues": (
        "run_discover_discourse_issues",
        "run_export_research_issue_map",
        "run_materialize_research_issue_surface",
        "run_project_research_issue_views",
        "run_suggest_evidence_lanes",
    ),
    "eco_council_runtime.optional_analysis.formal_public": (
        "run_apply_approved_formal_public_taxonomy",
        "run_compare_formal_public_footprints",
        "run_identify_representation_audit_cues",
        "taxonomy_labels",
    ),
    "eco_council_runtime.optional_analysis.relations": (
        "build_spatiotemporal_relation_cues",
        "relation_objection_candidates",
        "run_detect_temporal_cooccurrence_cues",
        "run_review_spatiotemporal_relation_alternatives",
    ),
}


CLI_HELP_COMMANDS = (
    "run-skill",
    "preflight-skill",
    "show-run-state",
    "show-schema-status",
    "request-phase-transition",
    "approve-phase-transition",
    "request-skill-approval",
    "query-analysis-result-items",
    "query-spatiotemporal-relations",
    "query-council-objects",
    "query-reporting-objects",
    "query-control-objects",
)


class ModuleDecompositionContractTests(unittest.TestCase):
    def test_planned_facade_public_symbols_remain_importable(self) -> None:
        missing: list[str] = []
        for module_name, symbol_names in FACADE_PUBLIC_SYMBOLS.items():
            module = importlib.import_module(module_name)
            for symbol_name in symbol_names:
                if not hasattr(module, symbol_name):
                    missing.append(f"{module_name}.{symbol_name}")

        self.assertEqual([], missing)

    def test_deliberation_plane_all_exports_cover_critical_facade_symbols(self) -> None:
        module = importlib.import_module("eco_council_runtime.kernel.planes.deliberation_plane")
        exported = set(getattr(module, "__all__", []))
        required = set(FACADE_PUBLIC_SYMBOLS["eco_council_runtime.kernel.planes.deliberation_plane"])

        self.assertTrue(required.issubset(exported))

    def test_analysis_plane_all_exports_cover_critical_facade_symbols(self) -> None:
        module = importlib.import_module("eco_council_runtime.kernel.planes.analysis_plane")
        exported = set(getattr(module, "__all__", []))
        required = set(FACADE_PUBLIC_SYMBOLS["eco_council_runtime.kernel.planes.analysis_plane"])

        self.assertTrue(required.issubset(exported))

    def test_deliberation_plane_split_modules_export_planned_symbols(self) -> None:
        missing: list[str] = []
        for module_name, symbol_names in SPLIT_MODULE_PUBLIC_SYMBOLS.items():
            module = importlib.import_module(module_name)
            exported = set(getattr(module, "__all__", []))
            for symbol_name in symbol_names:
                if not hasattr(module, symbol_name) or symbol_name not in exported:
                    missing.append(f"{module_name}.{symbol_name}")

        self.assertEqual([], missing)

    def test_cli_help_for_split_candidate_commands_remains_available(self) -> None:
        from eco_council_runtime.kernel.cli import build_parser

        parser = build_parser()
        for command_name in CLI_HELP_COMMANDS:
            with self.subTest(command_name=command_name):
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    with self.assertRaises(SystemExit) as raised:
                        parser.parse_args([command_name, "--help"])
                self.assertEqual(0, raised.exception.code)
                self.assertIn(command_name, stdout.getvalue())

    def test_cli_parser_accepts_schema_status_command_shape(self) -> None:
        from eco_council_runtime.kernel.cli import build_parser

        args = build_parser().parse_args(
            [
                "show-schema-status",
                "--run-dir",
                "/tmp/openclaw-run",
                "--db-path",
                "analytics/signal_plane.sqlite",
            ]
        )

        self.assertEqual("show-schema-status", args.command)
        self.assertEqual("/tmp/openclaw-run", args.run_dir)
        self.assertEqual("analytics/signal_plane.sqlite", args.db_path)

    def test_module_size_report_tracks_decomposition_targets(self) -> None:
        from tools import module_size_report

        report = module_size_report.build_report(threshold=1500)
        target_paths = {
            item["path"]
            for item in report["decomposition_targets"]
            if isinstance(item, dict)
        }

        self.assertEqual("module-size-report-v1", report["schema_version"])
        self.assertIn(
            "eco-concil-runtime/src/eco_council_runtime/kernel/planes/deliberation_plane.py",
            target_paths,
        )
        self.assertGreaterEqual(report["summary"]["decomposition_target_count"], 10)
        self.assertGreaterEqual(report["summary"]["largest_target_line_count"], 1500)


if __name__ == "__main__":
    unittest.main()

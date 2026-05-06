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
    "eco_council_runtime.optional_analysis_helpers": (
        "run_aggregate_environment_evidence",
        "run_review_fact_check_evidence_scope",
        "run_detect_temporal_cooccurrence_cues",
        "run_review_spatiotemporal_relation_alternatives",
    ),
    "eco_council_runtime.council_objects": (
        "connect_db",
        "append_finding_record",
        "append_evidence_bundle_record",
        "store_council_proposal_records",
        "query_council_objects",
    ),
    "eco_council_runtime.analysis_objects": (
        "canonical_evidence_refs",
        "build_heuristic_wrapper_provenance",
    ),
    "eco_council_runtime.canonical_contracts": (
        "canonical_contract",
        "canonical_contracts_for_plane",
        "validate_canonical_payload",
    ),
    "eco_council_runtime.kernel.cli": (
        "build_parser",
        "main",
        "show_run_state",
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

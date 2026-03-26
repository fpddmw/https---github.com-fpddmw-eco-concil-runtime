"""Supervisor CLI assembly for the eco-council controller."""

from __future__ import annotations

import argparse

from eco_council_runtime.controller.constants import (
    DEFAULT_HISTORY_TOP_K,
    READINESS_ROLES,
    REPORT_ROLES,
    SOURCE_SELECTION_ROLES,
)


def build_supervisor_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an eco-council workflow with approval gates.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_run = sub.add_parser("init-run", help="Bootstrap a run, create supervisor state, and provision OpenClaw agents.")
    init_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    init_run.add_argument("--mission-input", required=True, help="Mission JSON file.")
    init_run.add_argument("--agent-prefix", default="", help="Optional OpenClaw agent id prefix.")
    init_run.add_argument("--workspace-root", default="", help="Optional workspace root for the three OpenClaw agents.")
    init_run.add_argument("--skills-root", default="", help="Optional detached skills repository root to project into OpenClaw.")
    init_run.add_argument("--no-provision-openclaw", action="store_true", help="Skip automatic OpenClaw agent provisioning during init-run.")
    init_run.add_argument("--yes", action="store_true", help="Skip interactive approval when provisioning agents.")
    init_run.add_argument("--history-db", default="", help="Optional case-library SQLite path for moderator historical context.")
    init_run.add_argument("--history-top-k", type=int, default=DEFAULT_HISTORY_TOP_K, help="Number of similar historical cases to inject into moderator turns.")
    init_run.add_argument("--case-library-db", default="", help="Optional case-library SQLite path for automatic run archiving. Defaults to runs/archives/eco_council_case_library.sqlite.")
    init_run.add_argument("--signal-corpus-db", default="", help="Optional signal-corpus SQLite path for automatic post-data-plane imports. Defaults to runs/archives/eco_council_signal_corpus.sqlite.")
    init_run.add_argument("--disable-auto-archive", action="store_true", help="Disable automatic imports into the case library and signal corpus for this run.")
    init_run.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    provision = sub.add_parser("provision-openclaw-agents", help="Create or reuse three isolated OpenClaw agents.")
    provision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    provision.add_argument("--workspace-root", default="", help="Optional workspace root for the three agents.")
    provision.add_argument("--skills-root", default="", help="Optional detached skills repository root to project into OpenClaw.")
    provision.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    provision.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    status = sub.add_parser("status", help="Show current supervisor state.")
    status.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    status.add_argument("--history-db", default="", help="Optional case-library SQLite path to attach for moderator historical context.")
    status.add_argument("--history-top-k", type=int, default=0, help="Optional override for moderator historical-case count.")
    status.add_argument("--disable-history-context", action="store_true", help="Disable moderator historical-case context for this run.")
    status.add_argument("--case-library-db", default="", help="Optional case-library SQLite path for automatic run archiving.")
    status.add_argument("--signal-corpus-db", default="", help="Optional signal-corpus SQLite path for automatic post-data-plane imports.")
    status.add_argument("--disable-auto-archive", action="store_true", help="Disable automatic imports into the case library and signal corpus for this run.")
    status.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    summarize = sub.add_parser("summarize-run", help="Render one human-readable run report from the run directory.")
    summarize.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    summarize.add_argument("--round-id", default="", help="Optional round id filter, for example round-001.")
    summarize.add_argument("--lang", default="zh", choices=("zh", "en"), help="Human-readable report language.")
    summarize.add_argument("--output", default="", help="Optional output markdown path.")
    summarize.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    continue_run = sub.add_parser("continue-run", help="Run the next approved local shell stage.")
    continue_run.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    continue_run.add_argument("--timeout-seconds", type=int, default=600, help="Timeout for execute-fetch-plan.")
    continue_run.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    continue_run.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    run_agent = sub.add_parser("run-agent-step", help="Send the current turn to OpenClaw, receive JSON, and import it.")
    run_agent.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    run_agent.add_argument("--role", default="", choices=("", "moderator", "sociologist", "environmentalist"), help="Optional role override for source-selection, curation, data-readiness, report, or moderator-gated stages.")
    run_agent.add_argument("--timeout-seconds", type=int, default=600, help="OpenClaw agent timeout.")
    run_agent.add_argument("--thinking", default="low", choices=("off", "minimal", "low", "medium", "high"), help="OpenClaw thinking level.")
    run_agent.add_argument("--yes", action="store_true", help="Skip interactive approval.")
    run_agent.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_task = sub.add_parser("import-task-review", help="Import moderator task-review JSON into tasks.json.")
    import_task.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_task.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_task.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_source_selection = sub.add_parser("import-source-selection", help="Import one source-selection JSON into the canonical role path.")
    import_source_selection.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_source_selection.add_argument("--role", required=True, choices=SOURCE_SELECTION_ROLES, help="Expert role.")
    import_source_selection.add_argument("--input", required=True, help="JSON file returned by the expert agent.")
    import_source_selection.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_claim_curation = sub.add_parser("import-claim-curation", help="Import one claim-curation JSON into the canonical sociologist path.")
    import_claim_curation.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_claim_curation.add_argument("--input", required=True, help="JSON file returned by the sociologist agent.")
    import_claim_curation.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_observation_curation = sub.add_parser("import-observation-curation", help="Import one observation-curation JSON into the canonical environmentalist path.")
    import_observation_curation.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_observation_curation.add_argument("--input", required=True, help="JSON file returned by the environmentalist agent.")
    import_observation_curation.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_data_readiness = sub.add_parser("import-data-readiness", help="Import one data-readiness-report JSON into the canonical role path.")
    import_data_readiness.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_data_readiness.add_argument("--role", required=True, choices=READINESS_ROLES, help="Expert role.")
    import_data_readiness.add_argument("--input", required=True, help="JSON file returned by the expert agent.")
    import_data_readiness.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_matching_authorization = sub.add_parser("import-matching-authorization", help="Import moderator matching-authorization JSON into the canonical path.")
    import_matching_authorization.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_matching_authorization.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_matching_authorization.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_matching_adjudication = sub.add_parser("import-matching-adjudication", help="Import moderator matching-adjudication JSON into the canonical path.")
    import_matching_adjudication.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_matching_adjudication.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_matching_adjudication.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_investigation_review = sub.add_parser("import-investigation-review", help="Import moderator investigation-review JSON into the canonical path.")
    import_investigation_review.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_investigation_review.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_investigation_review.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_report = sub.add_parser("import-report", help="Import one expert-report JSON into the draft path.")
    import_report.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_report.add_argument("--role", required=True, choices=REPORT_ROLES, help="Expert role.")
    import_report.add_argument("--input", required=True, help="JSON file returned by the expert agent.")
    import_report.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_decision = sub.add_parser("import-decision", help="Import moderator decision JSON into the draft path.")
    import_decision.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_decision.add_argument("--input", required=True, help="JSON file returned by the moderator.")
    import_decision.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    import_fetch_execution = sub.add_parser(
        "import-fetch-execution",
        help="Import canonical fetch_execution.json produced by an external fetch runner.",
    )
    import_fetch_execution.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    import_fetch_execution.add_argument(
        "--input",
        default="",
        help="Optional fetch execution JSON path. Defaults to the canonical round fetch_execution.json path.",
    )
    import_fetch_execution.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser

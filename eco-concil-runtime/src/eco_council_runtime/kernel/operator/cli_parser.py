from __future__ import annotations

import argparse

from eco_council_runtime.contracts import (
    PLANE_ANALYSIS,
    PLANE_DELIBERATION,
    PLANE_REPORTING,
    PLANE_RUNTIME,
    PLANE_SIGNAL,
)
from eco_council_runtime.control_objects import control_queryable_object_kinds
from eco_council_runtime.objects.council import council_queryable_object_kinds
from eco_council_runtime.kernel.archive.post_round import ARCHIVE_FAILURE_POLICIES
from eco_council_runtime.kernel.governance.runtime_governance import CONTRACT_MODES
from eco_council_runtime.kernel.governance.transition_requests import (
    TRANSITION_KIND_CLOSE_ROUND,
    TRANSITION_KIND_FREEZE_REPORT_BASIS,
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
)
from eco_council_runtime.kernel.operator.operations import PERMISSION_PROFILES
from eco_council_runtime.kernel.planes.analysis_plane import analysis_kind_names
from eco_council_runtime.reporting_objects import reporting_queryable_object_kinds


__all__ = (
    "add_actor_role_arg",
    "add_admission_policy_args",
    "add_analysis_query_args",
    "add_control_query_args",
    "add_council_query_args",
    "add_execution_policy_args",
    "add_reporting_query_args",
    "build_parser",
)


def add_execution_policy_args(command: argparse.ArgumentParser) -> None:
    command.add_argument("--timeout-seconds", type=float, default=None)
    command.add_argument("--retry-budget", type=int, default=None)
    command.add_argument("--retry-backoff-ms", type=int, default=None)
    command.add_argument("--allow-side-effect", action="append", default=[])


def add_admission_policy_args(command: argparse.ArgumentParser) -> None:
    command.add_argument("--permission-profile", default="standard", choices=PERMISSION_PROFILES)
    command.add_argument("--max-timeout-seconds", type=float, default=None)
    command.add_argument("--max-retry-budget", type=int, default=None)
    command.add_argument("--max-retry-backoff-ms", type=int, default=None)
    command.add_argument("--default-allow-side-effect", action="append", default=[])
    command.add_argument("--approval-required-side-effect", action="append", default=[])
    command.add_argument("--blocked-side-effect", action="append", default=[])
    command.add_argument("--allowed-read-root", action="append", default=[])
    command.add_argument("--allowed-write-root", action="append", default=[])
    command.add_argument("--allowed-cwd-root", action="append", default=[])


def add_actor_role_arg(command: argparse.ArgumentParser) -> None:
    command.add_argument("--actor-role", default="")


def add_analysis_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(analysis_kind_names())
    command.add_argument("--run-dir", required=True)
    command.add_argument("--result-set-id", default="")
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument(
        "--analysis-kind",
        default="",
        help=f"Optional analysis kind filter. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--source-skill", default="")
    command.add_argument("--artifact-path", default="")
    command.add_argument("--latest-only", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def add_council_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(council_queryable_object_kinds())
    command.add_argument("--run-dir", required=True)
    command.add_argument(
        "--object-kind",
        required=True,
        help=f"Canonical deliberation object kind. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument("--agent-role", default="")
    command.add_argument("--status", default="")
    command.add_argument("--decision-id", default="")
    command.add_argument("--target-kind", default="")
    command.add_argument("--target-id", default="")
    command.add_argument("--issue-label", default="")
    command.add_argument("--route-id", default="")
    command.add_argument("--actor-id", default="")
    command.add_argument("--assessment-id", default="")
    command.add_argument("--linkage-id", default="")
    command.add_argument("--gap-id", default="")
    command.add_argument("--proposal-id", default="")
    command.add_argument("--source-proposal-id", default="")
    command.add_argument("--readiness-blocker-only", action="store_true")
    command.add_argument("--include-contract", action="store_true")
    command.add_argument("--include-items", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def add_reporting_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(reporting_queryable_object_kinds())
    command.add_argument("--run-dir", required=True)
    command.add_argument(
        "--object-kind",
        required=True,
        help=f"Canonical reporting object kind. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument("--agent-role", default="")
    command.add_argument("--status", default="")
    command.add_argument("--decision-id", default="")
    command.add_argument("--stage", default="")
    command.add_argument("--include-contract", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def add_control_query_args(command: argparse.ArgumentParser) -> None:
    supported_kinds = ", ".join(control_queryable_object_kinds())
    command.add_argument("--run-dir", required=True)
    command.add_argument(
        "--object-kind",
        required=True,
        help=f"Canonical control object kind. Supported kinds: {supported_kinds}.",
    )
    command.add_argument("--run-id", default="")
    command.add_argument("--round-id", default="")
    command.add_argument("--status", default="")
    command.add_argument("--controller-status", default="")
    command.add_argument("--gate-status", default="")
    command.add_argument("--report-basis-status", default="")
    command.add_argument("--report_basis-status", default="")
    command.add_argument("--supervisor-status", default="")
    command.add_argument("--planning-mode", default="")
    command.add_argument("--controller-authority", default="")
    command.add_argument("--plan-source", default="")
    command.add_argument("--plan-id", default="")
    command.add_argument("--plan-step-group", default="")
    command.add_argument("--phase-group", default="")
    command.add_argument("--readiness-status", default="")
    command.add_argument("--current-stage", default="")
    command.add_argument("--failed-stage", default="")
    command.add_argument("--resume-status", default="")
    command.add_argument("--stage-name", default="")
    command.add_argument("--stage-kind", default="")
    command.add_argument("--skill-name", default="")
    command.add_argument("--assigned-role-hint", default="")
    command.add_argument("--gate-handler", default="")
    command.add_argument("--decision-source", default="")
    command.add_argument("--supervisor-substatus", default="")
    command.add_argument("--governed-execution-posture", default="")
    command.add_argument("--terminal-state", default="")
    command.add_argument("--reporting-handoff-status", default="")
    command.add_argument("--transition-kind", default="")
    command.add_argument("--requested-by-role", default="")
    command.add_argument("--requested-actor-role", default="")
    command.add_argument("--request-id", default="")
    command.add_argument("--target-round-id", default="")
    command.add_argument("--requested-command-name", default="")
    command.add_argument("--latest-decision-status", default="")
    command.add_argument("--latest-decision-by-role", default="")
    command.add_argument("--decision-by-role", default="")
    command.add_argument("--reporting-ready-only", action="store_true")
    command.add_argument("--include-contract", action="store_true")
    command.add_argument("--limit", type=int, default=20)
    command.add_argument("--offset", type=int, default=0)
    command.add_argument("--pretty", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal runtime kernel for skill-first investigation runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init-run", help="Initialize runtime manifest, cursor, and registry for a run.")
    init_cmd.add_argument("--run-dir", required=True)
    init_cmd.add_argument("--run-id", required=True)
    add_actor_role_arg(init_cmd)
    init_cmd.add_argument("--pretty", action="store_true")

    run_cmd = sub.add_parser("run-skill", help="Execute one skill through the runtime kernel and append a ledger event.")
    run_cmd.add_argument("--run-dir", required=True)
    run_cmd.add_argument("--run-id", required=True)
    run_cmd.add_argument("--round-id", required=True)
    run_cmd.add_argument("--skill-name", required=True)
    add_actor_role_arg(run_cmd)
    run_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    run_cmd.add_argument("--skill-approval-request-id", default="")
    run_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(run_cmd)
    run_cmd.add_argument("skill_args", nargs=argparse.REMAINDER)

    preflight_cmd = sub.add_parser("preflight-skill", help="Resolve one skill contract and report governance issues without executing the skill.")
    preflight_cmd.add_argument("--run-dir", required=True)
    preflight_cmd.add_argument("--run-id", required=True)
    preflight_cmd.add_argument("--round-id", required=True)
    preflight_cmd.add_argument("--skill-name", required=True)
    add_actor_role_arg(preflight_cmd)
    preflight_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    preflight_cmd.add_argument("--skill-approval-request-id", default="")
    preflight_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(preflight_cmd)
    preflight_cmd.add_argument("skill_args", nargs=argparse.REMAINDER)

    request_transition_cmd = sub.add_parser(
        "request-phase-transition",
        help="Persist one moderator-authored phase transition request for later operator approval.",
    )
    request_transition_cmd.add_argument("--run-dir", required=True)
    request_transition_cmd.add_argument("--run-id", required=True)
    request_transition_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(request_transition_cmd)
    request_transition_cmd.add_argument(
        "--transition-kind",
        required=True,
        choices=[
            TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
            TRANSITION_KIND_FREEZE_REPORT_BASIS,
            TRANSITION_KIND_CLOSE_ROUND,
        ],
    )
    request_transition_cmd.add_argument("--target-round-id", default="")
    request_transition_cmd.add_argument("--source-round-id", default="")
    request_transition_cmd.add_argument("--rationale", default="")
    request_transition_cmd.add_argument("--evidence-ref", action="append", default=[])
    request_transition_cmd.add_argument("--basis-object-id", action="append", default=[])
    request_transition_cmd.add_argument("--request-payload-json", default="")
    request_transition_cmd.add_argument("--pretty", action="store_true")

    approve_transition_cmd = sub.add_parser(
        "approve-phase-transition",
        help="Approve one pending phase transition request without committing the transition side effects.",
    )
    approve_transition_cmd.add_argument("--run-dir", required=True)
    approve_transition_cmd.add_argument("--request-id", required=True)
    add_actor_role_arg(approve_transition_cmd)
    approve_transition_cmd.add_argument("--approval-reason", default="")
    approve_transition_cmd.add_argument("--evidence-ref", action="append", default=[])
    approve_transition_cmd.add_argument("--basis-object-id", action="append", default=[])
    approve_transition_cmd.add_argument("--operator-note", action="append", default=[])
    approve_transition_cmd.add_argument("--pretty", action="store_true")

    reject_transition_cmd = sub.add_parser(
        "reject-phase-transition",
        help="Reject one pending phase transition request and persist the operator rationale.",
    )
    reject_transition_cmd.add_argument("--run-dir", required=True)
    reject_transition_cmd.add_argument("--request-id", required=True)
    add_actor_role_arg(reject_transition_cmd)
    reject_transition_cmd.add_argument("--rejection-reason", required=True)
    reject_transition_cmd.add_argument("--evidence-ref", action="append", default=[])
    reject_transition_cmd.add_argument("--basis-object-id", action="append", default=[])
    reject_transition_cmd.add_argument("--operator-note", action="append", default=[])
    reject_transition_cmd.add_argument("--pretty", action="store_true")

    request_skill_approval_cmd = sub.add_parser(
        "request-skill-approval",
        help="Persist one optional-analysis skill approval request for operator decision.",
    )
    request_skill_approval_cmd.add_argument("--run-dir", required=True)
    request_skill_approval_cmd.add_argument("--run-id", required=True)
    request_skill_approval_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(request_skill_approval_cmd)
    request_skill_approval_cmd.add_argument("--skill-name", required=True)
    request_skill_approval_cmd.add_argument("--requested-actor-role", default="")
    request_skill_approval_cmd.add_argument("--rationale", default="")
    request_skill_approval_cmd.add_argument("--requested-skill-arg", action="append", default=[])
    request_skill_approval_cmd.add_argument("--evidence-ref", action="append", default=[])
    request_skill_approval_cmd.add_argument("--basis-object-id", action="append", default=[])
    request_skill_approval_cmd.add_argument("--request-payload-json", default="")
    request_skill_approval_cmd.add_argument("--pretty", action="store_true")

    approve_skill_approval_cmd = sub.add_parser(
        "approve-skill-approval",
        help="Approve one pending optional-analysis skill approval request.",
    )
    approve_skill_approval_cmd.add_argument("--run-dir", required=True)
    approve_skill_approval_cmd.add_argument("--request-id", required=True)
    add_actor_role_arg(approve_skill_approval_cmd)
    approve_skill_approval_cmd.add_argument("--approval-reason", default="")
    approve_skill_approval_cmd.add_argument("--evidence-ref", action="append", default=[])
    approve_skill_approval_cmd.add_argument("--basis-object-id", action="append", default=[])
    approve_skill_approval_cmd.add_argument("--operator-note", action="append", default=[])
    approve_skill_approval_cmd.add_argument("--pretty", action="store_true")

    reject_skill_approval_cmd = sub.add_parser(
        "reject-skill-approval",
        help="Reject one pending optional-analysis skill approval request.",
    )
    reject_skill_approval_cmd.add_argument("--run-dir", required=True)
    reject_skill_approval_cmd.add_argument("--request-id", required=True)
    add_actor_role_arg(reject_skill_approval_cmd)
    reject_skill_approval_cmd.add_argument("--rejection-reason", required=True)
    reject_skill_approval_cmd.add_argument("--evidence-ref", action="append", default=[])
    reject_skill_approval_cmd.add_argument("--basis-object-id", action="append", default=[])
    reject_skill_approval_cmd.add_argument("--operator-note", action="append", default=[])
    reject_skill_approval_cmd.add_argument("--pretty", action="store_true")

    submit_finding_cmd = sub.add_parser(
        "submit-finding-record",
        help="Persist one DB-backed finding record for the selected round.",
    )
    submit_finding_cmd.add_argument("--run-dir", required=True)
    submit_finding_cmd.add_argument("--run-id", required=True)
    submit_finding_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(submit_finding_cmd)
    submit_finding_cmd.add_argument("--finding-kind", default="finding")
    submit_finding_cmd.add_argument("--agent-role", default="")
    submit_finding_cmd.add_argument("--title", required=True)
    submit_finding_cmd.add_argument("--summary", required=True)
    submit_finding_cmd.add_argument("--rationale", required=True)
    submit_finding_cmd.add_argument("--confidence", type=float, required=True)
    submit_finding_cmd.add_argument("--target-kind", default="round")
    submit_finding_cmd.add_argument("--target-id", default="")
    submit_finding_cmd.add_argument("--basis-object-id", action="append", default=[])
    submit_finding_cmd.add_argument("--source-signal-id", action="append", default=[])
    submit_finding_cmd.add_argument("--linked-bundle-id", action="append", default=[])
    submit_finding_cmd.add_argument("--response-to-id", action="append", default=[])
    submit_finding_cmd.add_argument("--evidence-ref", action="append", default=[])
    submit_finding_cmd.add_argument("--provenance-json", default="{}")
    submit_finding_cmd.add_argument("--pretty", action="store_true")

    post_discussion_cmd = sub.add_parser(
        "post-discussion-message",
        help="Persist one DB-backed discussion message for the selected round.",
    )
    post_discussion_cmd.add_argument("--run-dir", required=True)
    post_discussion_cmd.add_argument("--run-id", required=True)
    post_discussion_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(post_discussion_cmd)
    post_discussion_cmd.add_argument("--author-role", default="")
    post_discussion_cmd.add_argument("--message-kind", default="discussion")
    post_discussion_cmd.add_argument("--thread-id", default="")
    post_discussion_cmd.add_argument("--message-text", required=True)
    post_discussion_cmd.add_argument("--target-kind", default="round")
    post_discussion_cmd.add_argument("--target-id", default="")
    post_discussion_cmd.add_argument("--response-to-id", action="append", default=[])
    post_discussion_cmd.add_argument("--related-object-id", action="append", default=[])
    post_discussion_cmd.add_argument("--evidence-ref", action="append", default=[])
    post_discussion_cmd.add_argument("--provenance-json", default="{}")
    post_discussion_cmd.add_argument("--pretty", action="store_true")

    post_review_cmd = sub.add_parser(
        "post-review-comment",
        help="Persist one DB-backed challenger or moderator review comment for the selected round.",
    )
    post_review_cmd.add_argument("--run-dir", required=True)
    post_review_cmd.add_argument("--run-id", required=True)
    post_review_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(post_review_cmd)
    post_review_cmd.add_argument("--author-role", default="")
    post_review_cmd.add_argument("--review-kind", default="review")
    post_review_cmd.add_argument("--thread-id", default="")
    post_review_cmd.add_argument("--comment-text", required=True)
    post_review_cmd.add_argument("--target-kind", default="round")
    post_review_cmd.add_argument("--target-id", default="")
    post_review_cmd.add_argument("--response-to-id", action="append", default=[])
    post_review_cmd.add_argument("--evidence-ref", action="append", default=[])
    post_review_cmd.add_argument("--relation-id", default="")
    post_review_cmd.add_argument("--objection-code", default="")
    post_review_cmd.add_argument("--challenged-rule", default="")
    post_review_cmd.add_argument("--alternative-explanation", default="")
    post_review_cmd.add_argument("--required-followup-evidence", action="append", default=[])
    post_review_cmd.add_argument("--report-risk", default="")
    post_review_cmd.add_argument("--provenance-json", default="{}")
    post_review_cmd.add_argument("--pretty", action="store_true")

    submit_evidence_cmd = sub.add_parser(
        "submit-evidence-bundle",
        help="Persist one DB-backed evidence bundle for the selected round.",
    )
    submit_evidence_cmd.add_argument("--run-dir", required=True)
    submit_evidence_cmd.add_argument("--run-id", required=True)
    submit_evidence_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(submit_evidence_cmd)
    submit_evidence_cmd.add_argument("--bundle-kind", default="evidence-bundle")
    submit_evidence_cmd.add_argument("--agent-role", default="")
    submit_evidence_cmd.add_argument("--title", required=True)
    submit_evidence_cmd.add_argument("--summary", required=True)
    submit_evidence_cmd.add_argument("--rationale", required=True)
    submit_evidence_cmd.add_argument("--confidence", type=float, required=True)
    submit_evidence_cmd.add_argument("--target-kind", default="round")
    submit_evidence_cmd.add_argument("--target-id", default="")
    submit_evidence_cmd.add_argument("--basis-object-id", action="append", default=[])
    submit_evidence_cmd.add_argument("--source-signal-id", action="append", default=[])
    submit_evidence_cmd.add_argument("--finding-id", action="append", default=[])
    submit_evidence_cmd.add_argument("--evidence-ref", action="append", default=[])
    submit_evidence_cmd.add_argument("--provenance-json", default="{}")
    submit_evidence_cmd.add_argument("--pretty", action="store_true")

    submit_section_cmd = sub.add_parser(
        "submit-report-section-draft",
        help="Persist one DB-backed report section draft for the selected round.",
    )
    submit_section_cmd.add_argument("--run-dir", required=True)
    submit_section_cmd.add_argument("--run-id", required=True)
    submit_section_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(submit_section_cmd)
    submit_section_cmd.add_argument("--agent-role", default="")
    submit_section_cmd.add_argument("--report-id", default="")
    submit_section_cmd.add_argument("--section-key", required=True)
    submit_section_cmd.add_argument("--section-title", required=True)
    submit_section_cmd.add_argument("--section-text", required=True)
    submit_section_cmd.add_argument("--status", default="draft")
    submit_section_cmd.add_argument("--basis-object-id", action="append", default=[])
    submit_section_cmd.add_argument("--bundle-id", action="append", default=[])
    submit_section_cmd.add_argument("--finding-id", action="append", default=[])
    submit_section_cmd.add_argument("--evidence-ref", action="append", default=[])
    submit_section_cmd.add_argument("--provenance-json", default="{}")
    submit_section_cmd.add_argument("--pretty", action="store_true")

    gate_cmd = sub.add_parser("apply-report-basis-gate", help="Evaluate round readiness and write a report-basis gate artifact.")
    gate_cmd.add_argument("--run-dir", required=True)
    gate_cmd.add_argument("--run-id", required=True)
    gate_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(gate_cmd)
    gate_cmd.add_argument("--pretty", action="store_true")

    governed_execution_cmd = sub.add_parser("run-governed-execution-round", help="Run the approved governed-execution report-basis chain in one command.")
    governed_execution_cmd.add_argument("--run-dir", required=True)
    governed_execution_cmd.add_argument("--run-id", required=True)
    governed_execution_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(governed_execution_cmd)
    governed_execution_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    governed_execution_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(governed_execution_cmd)

    resume_governed_execution_cmd = sub.add_parser("resume-governed-execution-round", help="Resume one interrupted governed-execution round from the persisted controller state.")
    resume_governed_execution_cmd.add_argument("--run-dir", required=True)
    resume_governed_execution_cmd.add_argument("--run-id", required=True)
    resume_governed_execution_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(resume_governed_execution_cmd)
    resume_governed_execution_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    resume_governed_execution_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(resume_governed_execution_cmd)

    restart_governed_execution_cmd = sub.add_parser("restart-governed-execution-round", help="Force a fresh governed-execution controller run and overwrite any resumable state.")
    restart_governed_execution_cmd.add_argument("--run-dir", required=True)
    restart_governed_execution_cmd.add_argument("--run-id", required=True)
    restart_governed_execution_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(restart_governed_execution_cmd)
    restart_governed_execution_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    restart_governed_execution_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(restart_governed_execution_cmd)

    close_round_cmd = sub.add_parser("close-round", help="Run the standard post-round archive closeout for one terminal round.")
    close_round_cmd.add_argument("--run-dir", required=True)
    close_round_cmd.add_argument("--run-id", required=True)
    close_round_cmd.add_argument("--round-id", required=True)
    close_round_cmd.add_argument("--transition-request-id", required=True)
    add_actor_role_arg(close_round_cmd)
    close_round_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    close_round_cmd.add_argument("--archive-failure-policy", default="block", choices=ARCHIVE_FAILURE_POLICIES)
    close_round_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(close_round_cmd)

    bootstrap_history_cmd = sub.add_parser("bootstrap-history-context", help="Materialize one runtime-managed history context bundle for the selected round.")
    bootstrap_history_cmd.add_argument("--run-dir", required=True)
    bootstrap_history_cmd.add_argument("--run-id", required=True)
    bootstrap_history_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(bootstrap_history_cmd)
    bootstrap_history_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    bootstrap_history_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(bootstrap_history_cmd)

    scenario_fixture_cmd = sub.add_parser("materialize-scenario-fixture", help="Freeze one benchmarkable scenario contract for the selected round.")
    scenario_fixture_cmd.add_argument("--run-dir", required=True)
    scenario_fixture_cmd.add_argument("--run-id", required=True)
    scenario_fixture_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(scenario_fixture_cmd)
    scenario_fixture_cmd.add_argument("--scenario-id", default="")
    scenario_fixture_cmd.add_argument("--baseline-manifest-path", default="")
    scenario_fixture_cmd.add_argument("--pretty", action="store_true")

    benchmark_manifest_cmd = sub.add_parser("materialize-benchmark-manifest", help="Write one stable runtime benchmark manifest for the selected round.")
    benchmark_manifest_cmd.add_argument("--run-dir", required=True)
    benchmark_manifest_cmd.add_argument("--run-id", required=True)
    benchmark_manifest_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(benchmark_manifest_cmd)
    benchmark_manifest_cmd.add_argument("--pretty", action="store_true")

    compare_manifest_cmd = sub.add_parser("compare-benchmark-manifests", help="Compare two benchmark manifests and materialize one drift report.")
    compare_manifest_cmd.add_argument("--run-dir", required=True)
    compare_manifest_cmd.add_argument("--run-id", required=True)
    compare_manifest_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(compare_manifest_cmd)
    compare_manifest_cmd.add_argument("--left-manifest-path", required=True)
    compare_manifest_cmd.add_argument("--right-manifest-path", required=True)
    compare_manifest_cmd.add_argument("--pretty", action="store_true")

    replay_cmd = sub.add_parser("replay-runtime-scenario", help="Materialize a candidate benchmark manifest and compare it against one frozen scenario fixture.")
    replay_cmd.add_argument("--run-dir", required=True)
    replay_cmd.add_argument("--run-id", required=True)
    replay_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(replay_cmd)
    replay_cmd.add_argument("--fixture-path", default="")
    replay_cmd.add_argument("--baseline-manifest-path", default="")
    replay_cmd.add_argument("--pretty", action="store_true")

    supervisor_cmd = sub.add_parser("supervise-round", help="Run the governed-execution controller and materialize a compact supervisor state.")
    supervisor_cmd.add_argument("--run-dir", required=True)
    supervisor_cmd.add_argument("--run-id", required=True)
    supervisor_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(supervisor_cmd)
    supervisor_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    supervisor_cmd.add_argument("--pretty", action="store_true")
    add_execution_policy_args(supervisor_cmd)

    admission_policy_cmd = sub.add_parser("materialize-admission-policy", help="Write one runtime admission policy for permission and sandbox enforcement.")
    admission_policy_cmd.add_argument("--run-dir", required=True)
    admission_policy_cmd.add_argument("--run-id", required=True)
    add_actor_role_arg(admission_policy_cmd)
    admission_policy_cmd.add_argument("--pretty", action="store_true")
    add_admission_policy_args(admission_policy_cmd)

    runtime_health_cmd = sub.add_parser("materialize-runtime-health", help="Write one runtime health and alert snapshot.")
    runtime_health_cmd.add_argument("--run-dir", required=True)
    runtime_health_cmd.add_argument("--round-id", default="")
    add_actor_role_arg(runtime_health_cmd)
    runtime_health_cmd.add_argument("--pretty", action="store_true")

    operator_runbook_cmd = sub.add_parser("materialize-operator-runbook", help="Write one operator runbook markdown surface for the runtime.")
    operator_runbook_cmd.add_argument("--run-dir", required=True)
    operator_runbook_cmd.add_argument("--round-id", default="")
    add_actor_role_arg(operator_runbook_cmd)
    operator_runbook_cmd.add_argument("--pretty", action="store_true")

    agent_entry_cmd = sub.add_parser("materialize-agent-entry-gate", help="Write one operator-visible agent entry gate contract for the selected round.")
    agent_entry_cmd.add_argument("--run-dir", required=True)
    agent_entry_cmd.add_argument("--run-id", required=True)
    agent_entry_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(agent_entry_cmd)
    agent_entry_cmd.add_argument("--contract-mode", default="warn", choices=CONTRACT_MODES)
    agent_entry_cmd.add_argument("--pretty", action="store_true")

    dead_letters_cmd = sub.add_parser("show-dead-letters", help="Show open runtime dead letters for the selected run or round.")
    dead_letters_cmd.add_argument("--run-dir", required=True)
    dead_letters_cmd.add_argument("--round-id", default="")
    dead_letters_cmd.add_argument("--limit", type=int, default=20)
    dead_letters_cmd.add_argument("--pretty", action="store_true")

    resolve_dead_letter_cmd = sub.add_parser("resolve-dead-letter", help="Close one runtime dead letter after the operator has addressed it.")
    resolve_dead_letter_cmd.add_argument("--run-dir", required=True)
    resolve_dead_letter_cmd.add_argument("--dead-letter-id", required=True)
    resolve_dead_letter_cmd.add_argument("--resolution-reason", required=True)
    resolve_dead_letter_cmd.add_argument("--resolution-note", default="")
    add_actor_role_arg(resolve_dead_letter_cmd)
    resolve_dead_letter_cmd.add_argument("--pretty", action="store_true")

    list_analysis_cmd = sub.add_parser(
        "list-analysis-result-sets",
        help="List analysis-plane result sets from the shared SQLite query surface.",
    )
    add_analysis_query_args(list_analysis_cmd)
    list_analysis_cmd.add_argument("--include-contract", action="store_true")
    list_analysis_cmd.add_argument("--include-items", action="store_true")

    query_items_cmd = sub.add_parser(
        "query-analysis-result-items",
        help="Query analysis-plane result items from the shared SQLite query surface.",
    )
    add_analysis_query_args(query_items_cmd)
    query_items_cmd.add_argument("--subject-id", default="")
    query_items_cmd.add_argument("--readiness", default="")
    query_items_cmd.add_argument("--include-result-sets", action="store_true")
    query_items_cmd.add_argument("--include-contract", action="store_true")

    relation_query_cmd = sub.add_parser(
        "query-spatiotemporal-relations",
        help="Query DB-backed spatiotemporal relation cue items from the analysis plane.",
    )
    relation_query_cmd.add_argument("--run-dir", required=True)
    relation_query_cmd.add_argument("--result-set-id", default="")
    relation_query_cmd.add_argument("--run-id", default="")
    relation_query_cmd.add_argument("--round-id", default="")
    relation_query_cmd.add_argument("--relation-id", default="")
    relation_query_cmd.add_argument("--relation-type", default="")
    relation_query_cmd.add_argument("--relation-status", default="")
    relation_query_cmd.add_argument("--source-signal-id", default="")
    relation_query_cmd.add_argument("--target-signal-id", default="")
    relation_query_cmd.add_argument("--source-role", default="")
    relation_query_cmd.add_argument("--target-role", default="")
    relation_query_cmd.add_argument("--latest-only", action="store_true")
    relation_query_cmd.add_argument("--include-result-sets", action="store_true")
    relation_query_cmd.add_argument("--include-contract", action="store_true")
    relation_query_cmd.add_argument("--limit", type=int, default=20)
    relation_query_cmd.add_argument("--offset", type=int, default=0)
    relation_query_cmd.add_argument("--pretty", action="store_true")

    council_query_cmd = sub.add_parser(
        "query-council-objects",
        help="Query canonical deliberation objects from the shared SQLite query surface.",
    )
    add_council_query_args(council_query_cmd)

    reporting_query_cmd = sub.add_parser(
        "query-reporting-objects",
        help="Query canonical reporting-plane objects from the shared SQLite query surface.",
    )
    add_reporting_query_args(reporting_query_cmd)

    control_query_cmd = sub.add_parser(
        "query-control-objects",
        help="Query runtime control objects from the shared SQLite query surface.",
    )
    add_control_query_args(control_query_cmd)

    governed_execution_export_cmd = sub.add_parser(
        "materialize-governed-execution-exports",
        help="Rebuild governed-execution investigation/report_basis/runtime exports from canonical DB state.",
    )
    governed_execution_export_cmd.add_argument("--run-dir", required=True)
    governed_execution_export_cmd.add_argument("--run-id", required=True)
    governed_execution_export_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(governed_execution_export_cmd)
    governed_execution_export_cmd.add_argument("--pretty", action="store_true")

    reporting_export_cmd = sub.add_parser(
        "materialize-reporting-exports",
        help="Rebuild reporting/*.json exports from canonical reporting-plane DB records.",
    )
    reporting_export_cmd.add_argument("--run-dir", required=True)
    reporting_export_cmd.add_argument("--run-id", required=True)
    reporting_export_cmd.add_argument("--round-id", required=True)
    add_actor_role_arg(reporting_export_cmd)
    reporting_export_cmd.add_argument("--pretty", action="store_true")

    contract_list_cmd = sub.add_parser(
        "list-canonical-contracts",
        help="List target canonical contracts for the selected plane or all planes.",
    )
    contract_list_cmd.add_argument(
        "--plane",
        default="",
        choices=[
            "",
            PLANE_SIGNAL,
            PLANE_ANALYSIS,
            PLANE_DELIBERATION,
            PLANE_REPORTING,
            PLANE_RUNTIME,
        ],
    )
    contract_list_cmd.add_argument("--pretty", action="store_true")

    schema_status_cmd = sub.add_parser(
        "show-schema-status",
        help="Show SQLite schema version metadata and migration ledger status.",
    )
    schema_status_cmd.add_argument("--run-dir", required=True)
    schema_status_cmd.add_argument("--db-path", default="")
    schema_status_cmd.add_argument("--pretty", action="store_true")

    show_cmd = sub.add_parser("show-run-state", help="Show manifest, cursor, registry, and a tail of runtime ledger events.")
    show_cmd.add_argument("--run-dir", required=True)
    show_cmd.add_argument("--round-id", default="")
    show_cmd.add_argument("--tail", type=int, default=10)
    show_cmd.add_argument("--pretty", action="store_true")

    reporting_cmd = sub.add_parser(
        "show-reporting-state",
        help="Show the DB-first reporting surface for one round, including handoff, decision, and publication gate state.",
    )
    reporting_cmd.add_argument("--run-dir", required=True)
    reporting_cmd.add_argument("--run-id", default="")
    reporting_cmd.add_argument("--round-id", required=True)
    reporting_cmd.add_argument("--pretty", action="store_true")
    return parser

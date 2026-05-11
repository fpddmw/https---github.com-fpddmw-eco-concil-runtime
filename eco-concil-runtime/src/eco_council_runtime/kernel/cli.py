from __future__ import annotations

import json
from typing import Any

from eco_council_runtime.contracts import SPATIOTEMPORAL_OBJECTION_CODE_VALUES
from eco_council_runtime.control_objects import query_control_objects
from eco_council_runtime.objects.council import (
    append_discussion_message_record,
    append_dynamic_investigation_object_record,
    append_evidence_bundle_record,
    append_finding_record,
    append_review_comment_record,
    query_council_objects,
)
from eco_council_runtime.kernel.reporting.governed_execution_exports import materialize_governed_execution_exports
from ..reporting_objects import (
    query_reporting_objects,
    store_report_section_draft_record,
)
from ..reporting_exports import materialize_reporting_exports
from eco_council_runtime.kernel.governance.agent_entry.handoff import EntryChainBuilder, HardGateCommandBuilder
from eco_council_runtime.kernel.planes.analysis_plane import (
    query_analysis_result_items,
    query_analysis_result_sets,
    query_spatiotemporal_relation_cues,
)
from eco_council_runtime.kernel.governance.agent_entry import materialize_agent_entry_gate
from eco_council_runtime.kernel.governance.agent_entry.registration import (
    materialize_openclaw_agent_registration_plan,
)
from eco_council_runtime.kernel.governance.access_policy import (
    command_requires_explicit_actor_role,
    evaluate_kernel_command_access,
)
from eco_council_runtime.kernel.archive.benchmark import (
    compare_benchmark_manifests,
    materialize_benchmark_manifest,
    materialize_scenario_fixture,
    replay_runtime_scenario,
)
from eco_council_runtime.kernel.execution.controller import (
    run_governed_execution_round_with_contract_mode,
)
from eco_council_runtime.kernel.execution.executor import SkillExecutionError, maybe_text, new_runtime_event_id, run_skill
from eco_council_runtime.kernel.execution.gate import GateHandler
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.archive.post_round import (
    bootstrap_history_context_with_contract_mode,
    close_round_with_contract_mode,
)
from eco_council_runtime.kernel.core.paths import (
    agent_entry_gate_path,
    cursor_path,
    manifest_path,
    resolve_run_dir,
)
from eco_council_runtime.kernel.operator.cli_parser import build_parser
from eco_council_runtime.kernel.operator.cli_runtime_commands import (
    command_access_failure,
    handle_early_runtime_command,
    handle_runtime_command,
    init_run,
    parse_json_object_arg,
    pretty_json,
    write_command_artifact,
)
from eco_council_runtime.kernel.operator.council_status import (
    show_archive_status_surface,
    show_council_status_surface,
    show_open_challenges_surface,
    show_source_acquisition_intents_surface,
    show_source_surfaces_surface,
    show_unbundled_findings_surface,
)
from eco_council_runtime.kernel.operator.run_state_view import (
    benchmark_operator_view,
    governed_execution_operator_view,
    operations_state,
    post_round_operator_view,
    reporting_operator_view,
    reporting_state_for_round,
    show_run_state,
    transition_request_state,
)
from eco_council_runtime.kernel.governance.skill_approvals import (
    approve_skill_approval_request,
    reject_skill_approval_request,
    store_skill_approval_request,
)
from eco_council_runtime.kernel.execution.supervisor import supervise_round_with_contract_mode
from eco_council_runtime.kernel.governance.transition_requests import (
    approve_transition_request,
    reject_transition_request,
    store_transition_request,
)


def materialize_prompt_mission_input(run_dir: Any, *, run_id: str, prompt: str, topic: str) -> str:
    request_text = maybe_text(prompt)
    if not request_text:
        raise ValueError("--mission-prompt must be non-empty when used.")
    output_path = run_dir / "input" / "mission.json"
    write_json(
        output_path,
        {
            "schema_version": "1.0.0",
            "run_id": maybe_text(run_id),
            "topic": maybe_text(topic) or "User investigation request",
            "objective": request_text,
            "request_text": request_text,
            "artifact_imports": [],
            "source_requests": [],
        },
    )
    return str(output_path.resolve())


def main(
    argv: list[str] | None = None,
    *,
    default_gate_handlers: dict[str, GateHandler] | None = None,
    default_agent_entry_profile: dict[str, Any] | None = None,
    default_posture_profile: dict[str, Any] | None = None,
    hard_gate_command_builder: HardGateCommandBuilder | None = None,
    entry_chain_builder: EntryChainBuilder | None = None,
    default_planning_sources: list[dict[str, Any]] | None = None,
    default_stage_definitions: dict[str, dict[str, Any]] | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    gate_handlers = default_gate_handlers if isinstance(default_gate_handlers, dict) else None
    agent_entry_profile = (
        default_agent_entry_profile
        if isinstance(default_agent_entry_profile, dict)
        else None
    )
    posture_profile = (
        default_posture_profile
        if isinstance(default_posture_profile, dict)
        else None
    )
    planning_sources = (
        default_planning_sources
        if isinstance(default_planning_sources, list)
        else None
    )
    stage_definitions = (
        default_stage_definitions
        if isinstance(default_stage_definitions, dict)
        else None
    )

    early_runtime_status = handle_early_runtime_command(args)
    if early_runtime_status is not None:
        return early_runtime_status

    if command_requires_explicit_actor_role(args.command):
        access = evaluate_kernel_command_access(
            args.command,
            actor_role=getattr(args, "actor_role", ""),
        )
        if bool(access.get("block_execution")):
            payload = command_access_failure(
                command_name=args.command,
                actor_role=getattr(args, "actor_role", ""),
                access=access,
            )
            print(pretty_json(payload, getattr(args, "pretty", False)))
            return 1

    run_dir = resolve_run_dir(args.run_dir)

    runtime_status = handle_runtime_command(args, run_dir)
    if runtime_status is not None:
        return runtime_status

    if args.command == "start-council-run":
        init_run(run_dir, args.run_id)
        mission_input_mode = "path"
        mission_path = maybe_text(args.mission_path)
        if maybe_text(getattr(args, "mission_prompt", "")):
            try:
                mission_path = materialize_prompt_mission_input(
                    run_dir,
                    run_id=args.run_id,
                    prompt=args.mission_prompt,
                    topic=args.mission_topic,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "contract_mode": args.contract_mode,
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            mission_input_mode = "prompt"
        if (
            not isinstance(agent_entry_profile, dict)
            or not callable(hard_gate_command_builder)
            or not callable(entry_chain_builder)
        ):
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": "No agent entry profile or agent handoff profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            scaffold = run_skill(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                skill_name="scaffold-mission-run",
                actor_role="moderator",
                skill_args=[
                    "--mission-path",
                    mission_path,
                    "--orchestration-mode",
                    "openclaw-agent",
                ],
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
            prepare = run_skill(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                skill_name="prepare-round",
                actor_role="moderator",
                skill_args=[],
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
            entry_gate = materialize_agent_entry_gate(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                agent_entry_profile=agent_entry_profile,
                hard_gate_command_builder=hard_gate_command_builder,
                entry_chain_builder=entry_chain_builder,
                contract_mode=args.contract_mode,
            )
            registration = (
                materialize_openclaw_agent_registration_plan(
                    run_dir,
                    run_id=args.run_id,
                    round_id=args.round_id,
                    actor_role=args.actor_role,
                    agent_entry_gate=entry_gate.get("agent_entry", {}),
                    agent_name_prefix=args.agent_name_prefix,
                    workspace_root=args.agent_workspace_root,
                    create_workspaces=True,
                )
                if bool(args.materialize_agent_registration)
                else {}
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        except Exception as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        payload = {
            "status": "completed",
            "summary": {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "contract_mode": args.contract_mode,
                "orchestration_mode": "openclaw-agent",
                "mission_input_mode": mission_input_mode,
                "mission_path": mission_path,
                "entry_status": entry_gate.get("summary", {}).get("entry_status")
                if isinstance(entry_gate.get("summary"), dict)
                else "",
                "registration_count": registration.get("registration_count", 0)
                if isinstance(registration, dict)
                else 0,
            },
            "scaffold": scaffold,
            "prepare_round": prepare,
            "agent_entry_gate": entry_gate,
            "openclaw_agent_registration": registration,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "request-phase-transition":
        init_run(run_dir, args.run_id)
        request_payload_json: dict[str, Any] = {}
        if maybe_text(args.request_payload_json):
            try:
                decoded = json.loads(args.request_payload_json)
            except json.JSONDecodeError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "transition_kind": args.transition_kind,
                    },
                    "message": f"Invalid --request-payload-json: {exc}",
                }
                print(pretty_json(failure, args.pretty))
                return 1
            if not isinstance(decoded, dict):
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "transition_kind": args.transition_kind,
                    },
                    "message": "--request-payload-json must decode to a JSON object.",
                }
                print(pretty_json(failure, args.pretty))
                return 1
            request_payload_json = decoded
        try:
            request = store_transition_request(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                transition_kind=args.transition_kind,
                requested_by_role=args.actor_role,
                target_round_id=args.target_round_id,
                source_round_id=args.source_round_id,
                rationale=args.rationale,
                evidence_refs=args.evidence_ref,
                basis_object_ids=args.basis_object_id,
                request_payload=request_payload_json,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "transition_kind": args.transition_kind,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id(
                    "runtimeevt",
                    args.run_id,
                    args.round_id,
                    "transition-request",
                    request.get("created_at_utc"),
                ),
                "event_type": "transition-request",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "actor_role": args.actor_role,
                "status": "completed",
                "transition_kind": args.transition_kind,
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
            },
        )
        payload = {
            "status": "completed",
            "summary": {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "transition_kind": args.transition_kind,
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "db_path": request.get("db_path"),
            },
            "request": request,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "approve-phase-transition":
        try:
            result = approve_transition_request(
                run_dir,
                request_id=args.request_id,
                approved_by_role=args.actor_role,
                decision_reason=args.approval_reason,
                evidence_refs=args.evidence_ref,
                basis_object_ids=args.basis_object_id,
                operator_notes=args.operator_note,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {"request_id": args.request_id},
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        request = result.get("request", {}) if isinstance(result.get("request"), dict) else {}
        approval = result.get("approval", {}) if isinstance(result.get("approval"), dict) else {}
        if maybe_text(request.get("run_id")):
            init_run(run_dir, maybe_text(request.get("run_id")))
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id(
                    "runtimeevt",
                    maybe_text(request.get("run_id")),
                    maybe_text(request.get("round_id")),
                    "transition-approval",
                    approval.get("approved_at_utc"),
                ),
                "event_type": "transition-approval",
                "run_id": request.get("run_id"),
                "round_id": request.get("round_id"),
                "actor_role": args.actor_role,
                "status": "completed",
                "request_id": request.get("request_id"),
                "transition_kind": request.get("transition_kind"),
                "decision_status": approval.get("decision_status"),
            },
        )
        payload = {
            "status": "completed",
            "summary": {
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "transition_kind": request.get("transition_kind"),
                "decision_status": approval.get("decision_status"),
                "db_path": result.get("db_path"),
            },
            "request": request,
            "approval": approval,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "reject-phase-transition":
        try:
            result = reject_transition_request(
                run_dir,
                request_id=args.request_id,
                rejected_by_role=args.actor_role,
                decision_reason=args.rejection_reason,
                evidence_refs=args.evidence_ref,
                basis_object_ids=args.basis_object_id,
                operator_notes=args.operator_note,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {"request_id": args.request_id},
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        request = result.get("request", {}) if isinstance(result.get("request"), dict) else {}
        rejection = result.get("rejection", {}) if isinstance(result.get("rejection"), dict) else {}
        if maybe_text(request.get("run_id")):
            init_run(run_dir, maybe_text(request.get("run_id")))
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id(
                    "runtimeevt",
                    maybe_text(request.get("run_id")),
                    maybe_text(request.get("round_id")),
                    "transition-rejection",
                    rejection.get("rejected_at_utc"),
                ),
                "event_type": "transition-rejection",
                "run_id": request.get("run_id"),
                "round_id": request.get("round_id"),
                "actor_role": args.actor_role,
                "status": "completed",
                "request_id": request.get("request_id"),
                "transition_kind": request.get("transition_kind"),
                "decision_status": rejection.get("decision_status"),
            },
        )
        payload = {
            "status": "completed",
            "summary": {
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "transition_kind": request.get("transition_kind"),
                "decision_status": rejection.get("decision_status"),
                "db_path": result.get("db_path"),
            },
            "request": request,
            "rejection": rejection,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "request-skill-approval":
        init_run(run_dir, args.run_id)
        request_payload_json: dict[str, Any] = {}
        if maybe_text(args.request_payload_json):
            try:
                decoded = json.loads(args.request_payload_json)
            except json.JSONDecodeError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "skill_name": args.skill_name,
                    },
                    "message": f"Invalid --request-payload-json: {exc}",
                }
                print(pretty_json(failure, args.pretty))
                return 1
            if not isinstance(decoded, dict):
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "skill_name": args.skill_name,
                    },
                    "message": "--request-payload-json must decode to a JSON object.",
                }
                print(pretty_json(failure, args.pretty))
                return 1
            request_payload_json = decoded
        try:
            request = store_skill_approval_request(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                skill_name=args.skill_name,
                requested_by_role=args.actor_role,
                requested_actor_role=args.requested_actor_role,
                rationale=args.rationale,
                requested_skill_args=args.requested_skill_arg,
                evidence_refs=args.evidence_ref,
                basis_object_ids=args.basis_object_id,
                request_payload=request_payload_json,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "skill_name": args.skill_name,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id(
                    "runtimeevt",
                    args.run_id,
                    args.round_id,
                    "skill-approval-request",
                    request.get("created_at_utc"),
                ),
                "event_type": "skill-approval-request",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "actor_role": args.actor_role,
                "status": "completed",
                "skill_name": args.skill_name,
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "requested_actor_role": request.get("requested_actor_role"),
            },
        )
        payload = {
            "status": "completed",
            "summary": {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "skill_name": args.skill_name,
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "requested_actor_role": request.get("requested_actor_role"),
                "db_path": request.get("db_path"),
            },
            "request": request,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "approve-skill-approval":
        try:
            result = approve_skill_approval_request(
                run_dir,
                request_id=args.request_id,
                approved_by_role=args.actor_role,
                decision_reason=args.approval_reason,
                evidence_refs=args.evidence_ref,
                basis_object_ids=args.basis_object_id,
                operator_notes=args.operator_note,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {"request_id": args.request_id},
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        request = result.get("request", {}) if isinstance(result.get("request"), dict) else {}
        approval = result.get("approval", {}) if isinstance(result.get("approval"), dict) else {}
        if maybe_text(request.get("run_id")):
            init_run(run_dir, maybe_text(request.get("run_id")))
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id(
                    "runtimeevt",
                    maybe_text(request.get("run_id")),
                    maybe_text(request.get("round_id")),
                    "skill-approval",
                    approval.get("approved_at_utc"),
                ),
                "event_type": "skill-approval",
                "run_id": request.get("run_id"),
                "round_id": request.get("round_id"),
                "actor_role": args.actor_role,
                "status": "completed",
                "request_id": request.get("request_id"),
                "skill_name": request.get("skill_name"),
                "decision_status": approval.get("decision_status"),
            },
        )
        payload = {
            "status": "completed",
            "summary": {
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "skill_name": request.get("skill_name"),
                "decision_status": approval.get("decision_status"),
                "db_path": result.get("db_path"),
            },
            "request": request,
            "approval": approval,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "reject-skill-approval":
        try:
            result = reject_skill_approval_request(
                run_dir,
                request_id=args.request_id,
                rejected_by_role=args.actor_role,
                decision_reason=args.rejection_reason,
                evidence_refs=args.evidence_ref,
                basis_object_ids=args.basis_object_id,
                operator_notes=args.operator_note,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {"request_id": args.request_id},
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        request = result.get("request", {}) if isinstance(result.get("request"), dict) else {}
        rejection = result.get("rejection", {}) if isinstance(result.get("rejection"), dict) else {}
        if maybe_text(request.get("run_id")):
            init_run(run_dir, maybe_text(request.get("run_id")))
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id(
                    "runtimeevt",
                    maybe_text(request.get("run_id")),
                    maybe_text(request.get("round_id")),
                    "skill-approval-rejection",
                    rejection.get("rejected_at_utc"),
                ),
                "event_type": "skill-approval-rejection",
                "run_id": request.get("run_id"),
                "round_id": request.get("round_id"),
                "actor_role": args.actor_role,
                "status": "completed",
                "request_id": request.get("request_id"),
                "skill_name": request.get("skill_name"),
                "decision_status": rejection.get("decision_status"),
            },
        )
        payload = {
            "status": "completed",
            "summary": {
                "request_id": request.get("request_id"),
                "request_status": request.get("request_status"),
                "skill_name": request.get("skill_name"),
                "decision_status": rejection.get("decision_status"),
                "db_path": result.get("db_path"),
            },
            "request": request,
            "rejection": rejection,
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "submit-finding-record":
            init_run(run_dir, args.run_id)
            payload = {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "finding_kind": args.finding_kind,
                "agent_role": maybe_text(args.agent_role) or maybe_text(args.actor_role) or "environmental-investigator",
                "status": "submitted",
                "title": args.title,
                "summary": args.summary,
                "rationale": args.rationale,
                "confidence": args.confidence,
                "target_kind": args.target_kind,
                "target_id": args.target_id,
                "basis_object_ids": args.basis_object_id,
                "source_signal_ids": args.source_signal_id,
                "linked_bundle_ids": args.linked_bundle_id,
                "response_to_ids": args.response_to_id,
                "evidence_refs": args.evidence_ref,
                "provenance": parse_json_object_arg(args.provenance_json, field_name="provenance-json"),
            }
            try:
                record = append_finding_record(
                    run_dir,
                    finding_payload=payload,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": "finding",
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            finding = record.get("finding", {}) if isinstance(record, dict) else {}
            finding_id = maybe_text(finding.get("finding_id"))
            artifact_file = write_command_artifact(
                run_dir,
                f"deliberation/finding_record_{args.round_id}_{finding_id}.json",
                record,
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "finding-record", finding_id),
                    "event_type": "finding-record-submitted",
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "actor_role": args.actor_role,
                    "status": "completed",
                    "finding_id": finding_id,
                    "finding_kind": maybe_text(finding.get("finding_kind")),
                },
            )
            payload_out = {
                "status": "completed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": "finding",
                    "object_id": finding_id,
                    "db_path": record.get("db_path"),
                    "artifact_path": str(artifact_file),
                },
                "canonical_ids": [finding_id],
                "record": record,
            }
            print(pretty_json(payload_out, args.pretty))
            return 0

    if args.command == "post-discussion-message":
            init_run(run_dir, args.run_id)
            payload = {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "author_role": maybe_text(args.author_role) or maybe_text(args.actor_role) or "moderator",
                "message_kind": args.message_kind,
                "thread_id": args.thread_id,
                "message_text": args.message_text,
                "target_kind": args.target_kind,
                "target_id": args.target_id,
                "response_to_ids": args.response_to_id,
                "related_object_ids": args.related_object_id,
                "evidence_refs": args.evidence_ref,
                "provenance": parse_json_object_arg(args.provenance_json, field_name="provenance-json"),
            }
            try:
                record = append_discussion_message_record(
                    run_dir,
                    message_payload=payload,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": "discussion-message",
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            message = record.get("message", {}) if isinstance(record, dict) else {}
            message_id = maybe_text(message.get("message_id"))
            artifact_file = write_command_artifact(
                run_dir,
                f"discussion/discussion_message_{args.round_id}_{message_id}.json",
                record,
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "discussion-message", message_id),
                    "event_type": "discussion-message-posted",
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "actor_role": args.actor_role,
                    "status": "completed",
                    "message_id": message_id,
                    "message_kind": maybe_text(message.get("message_kind")),
                },
            )
            payload_out = {
                "status": "completed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": "discussion-message",
                    "object_id": message_id,
                    "db_path": record.get("db_path"),
                    "artifact_path": str(artifact_file),
                },
                "canonical_ids": [message_id],
                "record": record,
            }
            print(pretty_json(payload_out, args.pretty))
            return 0

    if args.command == "post-review-comment":
            init_run(run_dir, args.run_id)
            relation_id = maybe_text(args.relation_id)
            objection_code = maybe_text(args.objection_code)
            if objection_code and objection_code not in SPATIOTEMPORAL_OBJECTION_CODE_VALUES:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": "review-comment",
                        "relation_id": relation_id,
                    },
                    "message": f"Unsupported relation objection_code: {objection_code}.",
                }
                print(pretty_json(failure, args.pretty))
                return 1
            target_kind = maybe_text(args.target_kind)
            target_id = maybe_text(args.target_id)
            if relation_id and (not target_id or target_kind == "round"):
                target_kind = "spatiotemporal-relation-cue"
                target_id = relation_id
            payload = {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "author_role": maybe_text(args.author_role) or maybe_text(args.actor_role) or "challenger",
                "review_kind": args.review_kind,
                "status": maybe_text(args.status),
                "thread_id": args.thread_id,
                "comment_text": args.comment_text,
                "target_kind": target_kind,
                "target_id": target_id,
                "response_to_ids": args.response_to_id,
                "evidence_refs": args.evidence_ref,
                "relation_id": relation_id,
                "objection_code": objection_code,
                "challenged_rule": maybe_text(args.challenged_rule),
                "alternative_explanation": maybe_text(args.alternative_explanation),
                "required_followup_evidence": args.required_followup_evidence,
                "report_risk": maybe_text(args.report_risk),
                "constraint_disposition": maybe_text(args.constraint_disposition),
                "provenance": parse_json_object_arg(args.provenance_json, field_name="provenance-json"),
            }
            try:
                record = append_review_comment_record(
                    run_dir,
                    comment_payload=payload,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": "review-comment",
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            comment = record.get("comment", {}) if isinstance(record, dict) else {}
            comment_id = maybe_text(comment.get("comment_id"))
            artifact_file = write_command_artifact(
                run_dir,
                f"discussion/review_comment_{args.round_id}_{comment_id}.json",
                record,
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "review-comment", comment_id),
                    "event_type": "review-comment-posted",
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "actor_role": args.actor_role,
                    "status": "completed",
                    "comment_id": comment_id,
                    "review_kind": maybe_text(comment.get("review_kind")),
                },
            )
            payload_out = {
                "status": "completed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": "review-comment",
                    "object_id": comment_id,
                    "db_path": record.get("db_path"),
                    "artifact_path": str(artifact_file),
                },
                "canonical_ids": [comment_id],
                "record": record,
            }
            print(pretty_json(payload_out, args.pretty))
            return 0

    if args.command == "submit-evidence-bundle":
            init_run(run_dir, args.run_id)
            payload = {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "bundle_kind": args.bundle_kind,
                "agent_role": maybe_text(args.agent_role) or maybe_text(args.actor_role) or "moderator",
                "status": "submitted",
                "title": args.title,
                "summary": args.summary,
                "rationale": args.rationale,
                "confidence": args.confidence,
                "target_kind": args.target_kind,
                "target_id": args.target_id,
                "basis_object_ids": args.basis_object_id,
                "source_signal_ids": args.source_signal_id,
                "finding_ids": args.finding_id,
                "evidence_refs": args.evidence_ref,
                "provenance": parse_json_object_arg(args.provenance_json, field_name="provenance-json"),
            }
            try:
                record = append_evidence_bundle_record(
                    run_dir,
                    bundle_payload=payload,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": "evidence-bundle",
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            bundle = record.get("bundle", {}) if isinstance(record, dict) else {}
            bundle_id = maybe_text(bundle.get("bundle_id"))
            artifact_file = write_command_artifact(
                run_dir,
                f"evidence/evidence_bundle_{args.round_id}_{bundle_id}.json",
                record,
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "evidence-bundle", bundle_id),
                    "event_type": "evidence-bundle-submitted",
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "actor_role": args.actor_role,
                    "status": "completed",
                    "bundle_id": bundle_id,
                    "bundle_kind": maybe_text(bundle.get("bundle_kind")),
                },
            )
            payload_out = {
                "status": "completed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": "evidence-bundle",
                    "object_id": bundle_id,
                    "db_path": record.get("db_path"),
                    "artifact_path": str(artifact_file),
                },
                "canonical_ids": [bundle_id],
                "record": record,
            }
            print(pretty_json(payload_out, args.pretty))
            return 0

    if args.command == "submit-dynamic-investigation-object":
            init_run(run_dir, args.run_id)
            payload = parse_json_object_arg(args.payload_json, field_name="payload-json")
            payload_kind = maybe_text(payload.get("object_kind"))
            if payload_kind and payload_kind != maybe_text(args.object_kind):
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": args.object_kind,
                    },
                    "message": (
                        f"payload-json object_kind `{payload_kind}` does not match "
                        f"--object-kind `{args.object_kind}`."
                    ),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            provenance = payload.get("provenance") if isinstance(payload.get("provenance"), dict) else {}
            provenance.update(parse_json_object_arg(args.provenance_json, field_name="provenance-json"))
            existing_evidence_refs = (
                payload.get("evidence_refs") if isinstance(payload.get("evidence_refs"), list) else []
            )
            existing_lineage = (
                payload.get("lineage") if isinstance(payload.get("lineage"), list) else []
            )
            payload.update(
                {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": args.object_kind,
                    "author_role": maybe_text(args.author_role) or maybe_text(args.actor_role) or "moderator",
                    "target_kind": args.target_kind,
                    "target_id": args.target_id,
                    "rationale": args.rationale,
                    "evidence_refs": [*existing_evidence_refs, *args.evidence_ref],
                    "lineage": [*existing_lineage, *args.lineage_ref],
                    "provenance": provenance,
                }
            )
            if maybe_text(args.status):
                payload["status"] = maybe_text(args.status)
            try:
                record = append_dynamic_investigation_object_record(
                    run_dir,
                    object_payload=payload,
                    object_kind=args.object_kind,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": args.object_kind,
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            dynamic_object = record.get("object", {}) if isinstance(record, dict) else {}
            object_id = maybe_text(dynamic_object.get("object_id"))
            artifact_file = write_command_artifact(
                run_dir,
                f"deliberation/dynamic_investigation_object_{args.round_id}_{object_id}.json",
                record,
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": new_runtime_event_id(
                        "runtimeevt",
                        args.run_id,
                        args.round_id,
                        "dynamic-investigation-object",
                        object_id,
                    ),
                    "event_type": "dynamic-investigation-object-submitted",
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "actor_role": args.actor_role,
                    "status": "completed",
                    "object_kind": maybe_text(dynamic_object.get("object_kind")),
                    "object_id": object_id,
                },
            )
            payload_out = {
                "status": "completed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": maybe_text(dynamic_object.get("object_kind")),
                    "object_id": object_id,
                    "db_path": record.get("db_path"),
                    "artifact_path": str(artifact_file),
                },
                "canonical_ids": [object_id],
                "record": record,
            }
            print(pretty_json(payload_out, args.pretty))
            return 0

    if args.command == "submit-report-section-draft":
            init_run(run_dir, args.run_id)
            payload = {
                "run_id": args.run_id,
                "round_id": args.round_id,
                "report_id": args.report_id or args.round_id,
                "agent_role": maybe_text(args.agent_role) or maybe_text(args.actor_role) or "report-editor",
                "status": args.status,
                "section_key": args.section_key,
                "section_title": args.section_title,
                "section_text": args.section_text,
                "basis_object_ids": args.basis_object_id,
                "bundle_ids": args.bundle_id,
                "finding_ids": args.finding_id,
                "claim_id": maybe_text(args.claim_id),
                "claim_text": maybe_text(args.claim_text),
                "claim_constraint_ids": args.claim_constraint_id,
                "basis_use": maybe_text(args.basis_use),
                "lead_basis": bool(args.lead_basis),
                "evidence_refs": args.evidence_ref,
                "provenance": parse_json_object_arg(args.provenance_json, field_name="provenance-json"),
            }
            try:
                record = store_report_section_draft_record(
                    run_dir,
                    section_payload=payload,
                )
            except ValueError as exc:
                failure = {
                    "status": "failed",
                    "summary": {
                        "run_id": args.run_id,
                        "round_id": args.round_id,
                        "object_kind": "report-section-draft",
                    },
                    "message": str(exc),
                }
                print(pretty_json(failure, args.pretty))
                return 1
            section = record.get("section", {}) if isinstance(record, dict) else {}
            section_id = maybe_text(section.get("section_id"))
            artifact_file = write_command_artifact(
                run_dir,
                f"reporting/report_section_draft_{args.round_id}_{section_id}.json",
                record,
            )
            append_ledger_event(
                run_dir,
                {
                    "schema_version": "runtime-event-v3",
                    "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "report-section-draft", section_id),
                    "event_type": "report-section-draft-submitted",
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "actor_role": args.actor_role,
                    "status": "completed",
                    "section_id": section_id,
                    "report_id": maybe_text(section.get("report_id")),
                },
            )
            payload_out = {
                "status": "completed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "object_kind": "report-section-draft",
                    "object_id": section_id,
                    "db_path": record.get("db_path") if isinstance(record, dict) else "",
                    "artifact_path": str(artifact_file),
                },
                "canonical_ids": [section_id],
                "record": record,
            }
            print(pretty_json(payload_out, args.pretty))
            return 0

    if args.command == "apply-report-basis-gate":
        init_run(run_dir, args.run_id)
        gate_handler_name = "report-basis-gate"
        if not gate_handlers or gate_handler_name not in gate_handlers:
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id},
                "message": "No default governed-execution gate handler registry was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        payload = gate_handlers[gate_handler_name](
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v2",
                "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, "report-basis-gate", payload.get("generated_at_utc")),
                "event_type": "report-basis-gate",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "started_at_utc": payload.get("generated_at_utc"),
                "completed_at_utc": payload.get("generated_at_utc"),
                "status": "completed",
                "gate_status": payload.get("gate_status"),
                "report_basis_gate_status": payload.get("report_basis_gate_status"),
                "readiness_status": payload.get("readiness_status"),
                "report_basis_freeze_allowed": bool(payload.get("report_basis_freeze_allowed")),
                "gate_path": payload.get("output_path"),
            },
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "run-governed-execution-round":
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No governed-execution posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = run_governed_execution_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "resume-governed-execution-round":
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No governed-execution posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = run_governed_execution_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                force_restart=False,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "restart-governed-execution-round":
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No governed-execution posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = run_governed_execution_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                force_restart=True,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "close-round":
        try:
            payload = close_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                transition_request_id=args.transition_request_id,
                actor_role=args.actor_role,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                archive_failure_policy=args.archive_failure_policy,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "bootstrap-history-context":
        try:
            payload = bootstrap_history_context_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-scenario-fixture":
        try:
            payload = materialize_scenario_fixture(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                scenario_id=args.scenario_id,
                baseline_manifest_override=args.baseline_manifest_path,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-benchmark-manifest":
        try:
            payload = materialize_benchmark_manifest(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "compare-benchmark-manifests":
        try:
            payload = compare_benchmark_manifests(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                left_manifest_path=args.left_manifest_path,
                right_manifest_path=args.right_manifest_path,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "replay-runtime-scenario":
        try:
            payload = replay_runtime_scenario(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                fixture_path_override=args.fixture_path,
                baseline_manifest_override=args.baseline_manifest_path,
            )
        except Exception as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "supervise-round":
        if not isinstance(posture_profile, dict):
            failure = {
                "status": "failed",
                "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode},
                "message": "No governed-execution posture profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = supervise_round_with_contract_mode(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                contract_mode=args.contract_mode,
                gate_handlers=gate_handlers,
                posture_profile=posture_profile,
                planning_sources=planning_sources,
                stage_definitions=stage_definitions,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-agent-entry-gate":
        init_run(run_dir, args.run_id)
        if (
            not isinstance(agent_entry_profile, dict)
            or not callable(hard_gate_command_builder)
            or not callable(entry_chain_builder)
        ):
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": "No agent entry profile or agent handoff profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        try:
            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                agent_entry_profile=agent_entry_profile,
                hard_gate_command_builder=hard_gate_command_builder,
                entry_chain_builder=entry_chain_builder,
                contract_mode=args.contract_mode,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                    "contract_mode": args.contract_mode,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-openclaw-agent-registration":
        init_run(run_dir, args.run_id)
        gate_payload = load_json_if_exists(agent_entry_gate_path(run_dir, args.round_id)) or {}
        try:
            payload = materialize_openclaw_agent_registration_plan(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                actor_role=args.actor_role,
                agent_entry_gate=gate_payload,
                agent_name_prefix=args.agent_name_prefix,
                workspace_root=args.agent_workspace_root,
                create_workspaces=args.create_workspaces,
            )
        except Exception as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "list-analysis-result-sets":
        try:
            payload = query_analysis_result_sets(
                run_dir,
                result_set_id=args.result_set_id,
                run_id=args.run_id,
                round_id=args.round_id,
                analysis_kind=args.analysis_kind,
                source_skill=args.source_skill,
                artifact_path=args.artifact_path,
                latest_only=args.latest_only,
                include_contract=args.include_contract,
                include_items=args.include_items,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "analysis_kind": args.analysis_kind,
                    "result_set_id": args.result_set_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-analysis-result-items":
        try:
            payload = query_analysis_result_items(
                run_dir,
                result_set_id=args.result_set_id,
                run_id=args.run_id,
                round_id=args.round_id,
                analysis_kind=args.analysis_kind,
                source_skill=args.source_skill,
                artifact_path=args.artifact_path,
                subject_id=args.subject_id,
                readiness=args.readiness,
                latest_only=args.latest_only,
                include_result_sets=args.include_result_sets,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "analysis_kind": args.analysis_kind,
                    "result_set_id": args.result_set_id,
                    "subject_id": args.subject_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-spatiotemporal-relations":
        try:
            payload = query_spatiotemporal_relation_cues(
                run_dir,
                result_set_id=args.result_set_id,
                run_id=args.run_id,
                round_id=args.round_id,
                relation_id=args.relation_id,
                relation_type=args.relation_type,
                relation_status=args.relation_status,
                source_signal_id=args.source_signal_id,
                target_signal_id=args.target_signal_id,
                source_role=args.source_role,
                target_role=args.target_role,
                latest_only=args.latest_only,
                include_result_sets=args.include_result_sets,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "analysis_kind": "spatiotemporal-relation-cue",
                    "relation_id": args.relation_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-council-objects":
        try:
            payload = query_council_objects(
                run_dir,
                object_kind=args.object_kind,
                run_id=args.run_id,
                round_id=args.round_id,
                agent_role=args.agent_role,
                status=args.status,
                decision_id=args.decision_id,
                target_kind=args.target_kind,
                target_id=args.target_id,
                issue_label=args.issue_label,
                route_id=args.route_id,
                actor_id=args.actor_id,
                assessment_id=args.assessment_id,
                linkage_id=args.linkage_id,
                gap_id=args.gap_id,
                proposal_id=args.proposal_id,
                source_proposal_id=args.source_proposal_id,
                source_skill=args.source_skill,
                target_evidence_request_id=args.target_evidence_request_id,
                readiness_blocker_only=args.readiness_blocker_only,
                include_contract=args.include_contract,
                include_items=args.include_items,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "object_kind": args.object_kind,
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-reporting-objects":
        try:
            payload = query_reporting_objects(
                run_dir,
                object_kind=args.object_kind,
                run_id=args.run_id,
                round_id=args.round_id,
                agent_role=args.agent_role,
                status=args.status,
                decision_id=args.decision_id,
                stage=args.stage,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "object_kind": args.object_kind,
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "query-control-objects":
        try:
            payload = query_control_objects(
                run_dir,
                object_kind=args.object_kind,
                run_id=args.run_id,
                round_id=args.round_id,
                status=args.status,
                controller_status=args.controller_status,
                gate_status=args.gate_status,
                report_basis_status=args.report_basis_status,
                supervisor_status=args.supervisor_status,
                planning_mode=args.planning_mode,
                controller_authority=args.controller_authority,
                plan_source=args.plan_source,
                plan_id=args.plan_id,
                plan_step_group=args.plan_step_group,
                phase_group=args.phase_group,
                readiness_status=args.readiness_status,
                current_stage=args.current_stage,
                failed_stage=args.failed_stage,
                resume_status=args.resume_status,
                stage_name=args.stage_name,
                stage_kind=args.stage_kind,
                skill_name=args.skill_name,
                assigned_role_hint=args.assigned_role_hint,
                gate_handler=args.gate_handler,
                decision_source=args.decision_source,
                supervisor_substatus=args.supervisor_substatus,
                governed_execution_posture=args.governed_execution_posture,
                terminal_state=args.terminal_state,
                reporting_handoff_status=args.reporting_handoff_status,
                transition_kind=args.transition_kind,
                requested_by_role=args.requested_by_role,
                requested_actor_role=args.requested_actor_role,
                request_id=args.request_id,
                target_round_id=args.target_round_id,
                requested_command_name=args.requested_command_name,
                latest_decision_status=args.latest_decision_status,
                latest_decision_by_role=args.latest_decision_by_role,
                decision_by_role=args.decision_by_role,
                reporting_ready_only=args.reporting_ready_only,
                include_contract=args.include_contract,
                limit=args.limit,
                offset=args.offset,
            )
        except ValueError as exc:
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "object_kind": args.object_kind,
                    "run_id": args.run_id,
                    "round_id": args.round_id,
                },
                "message": str(exc),
            }
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-governed-execution-exports":
        payload = materialize_governed_execution_exports(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-reporting-exports":
        payload = materialize_reporting_exports(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-council-status":
        payload = show_council_status_surface(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            limit=args.limit,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-source-surfaces":
        payload = show_source_surfaces_surface(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            limit=args.limit,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-source-acquisition-intents":
        payload = show_source_acquisition_intents_surface(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            author_role=args.author_role,
            source_skill=args.source_skill,
            status=args.status,
            target_evidence_request_id=args.target_evidence_request_id,
            limit=args.limit,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-open-challenges":
        payload = show_open_challenges_surface(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            limit=args.limit,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-unbundled-findings":
        payload = show_unbundled_findings_surface(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            limit=args.limit,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-archive-status":
        payload = show_archive_status_surface(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-reporting-state":
        resolved_run_id = maybe_text(args.run_id)
        if not resolved_run_id:
            manifest = load_json_if_exists(manifest_path(run_dir)) or {}
            cursor = load_json_if_exists(cursor_path(run_dir)) or {}
            resolved_run_id = maybe_text(manifest.get("run_id")) or maybe_text(cursor.get("run_id"))
        payload = reporting_state_for_round(run_dir, resolved_run_id, args.round_id)
        output = {
            "status": "completed",
            "summary": {
                "run_dir": str(run_dir),
                "run_id": resolved_run_id,
                "round_id": args.round_id,
                "reporting_ready": bool(payload.get("surface", {}).get("reporting_ready"))
                if isinstance(payload.get("surface"), dict)
                else False,
                "reporting_blocker_count": len(payload.get("surface", {}).get("reporting_blockers", []))
                if isinstance(payload.get("surface", {}).get("reporting_blockers"), list)
                else 0,
                "surface_source": maybe_text(payload.get("surface", {}).get("surface_source"))
                if isinstance(payload.get("surface"), dict)
                else "",
                "publication_status": maybe_text(payload.get("surface", {}).get("publication_status"))
                if isinstance(payload.get("surface"), dict)
                else "",
            },
            **payload,
        }
        print(pretty_json(output, args.pretty))
        return 0

    if args.command == "show-run-state":
        if not isinstance(agent_entry_profile, dict):
            failure = {
                "status": "failed",
                "summary": {
                    "run_dir": str(run_dir),
                    "round_id": args.round_id,
                },
                "message": "No agent entry profile was injected into cli.main().",
            }
            print(pretty_json(failure, args.pretty))
            return 1
        payload = show_run_state(
            run_dir,
            args.tail,
            args.round_id,
            agent_entry_profile=agent_entry_profile,
            hard_gate_command_builder=hard_gate_command_builder,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    return 1

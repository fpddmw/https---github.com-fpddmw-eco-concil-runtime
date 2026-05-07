from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eco_council_runtime.contracts import canonical_contracts_for_plane
from eco_council_runtime.kernel.core.ledger import append_ledger_event
from eco_council_runtime.kernel.core.manifest import (
    init_round_cursor,
    init_run_manifest,
    write_json,
)
from eco_council_runtime.kernel.core.paths import (
    admission_policy_path,
    cursor_path,
    ensure_runtime_dirs,
    ledger_path,
    manifest_path,
    operator_runbook_path,
    registry_path,
    resolve_run_dir,
    runtime_health_path,
)
from eco_council_runtime.kernel.core.registry import write_registry
from eco_council_runtime.kernel.execution.executor import (
    SkillExecutionError,
    maybe_text,
    new_runtime_event_id,
    run_skill,
)
from eco_council_runtime.kernel.governance.runtime_governance import (
    preflight_skill_execution,
)
from eco_council_runtime.kernel.operator.operations import (
    load_dead_letters,
    materialize_admission_policy,
    materialize_operator_runbook,
    materialize_runtime_health,
)
from eco_council_runtime.kernel.planes.deliberation_plane import load_schema_status


__all__ = (
    "command_access_failure",
    "handle_early_runtime_command",
    "handle_runtime_command",
    "init_run",
    "parse_json_object_arg",
    "pretty_json",
    "write_command_artifact",
)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def parse_json_object_arg(value: Any, *, field_name: str) -> dict[str, Any]:
    if not maybe_text(value):
        return {}
    try:
        decoded = json.loads(maybe_text(value))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid --{field_name}: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ValueError(f"--{field_name} must decode to a JSON object.")
    return decoded


def write_command_artifact(run_dir: Path, relative_path: str, payload: dict[str, Any]) -> Path:
    output_file = (run_dir / relative_path).resolve()
    write_json(output_file, payload)
    return output_file


def command_access_failure(
    *,
    command_name: str,
    actor_role: str,
    access: dict[str, Any],
) -> dict[str, Any]:
    issues = access.get("issues", []) if isinstance(access.get("issues"), list) else []
    message = (
        maybe_text(issues[0].get("message"))
        if issues and isinstance(issues[0], dict)
        else f"Actor role validation blocked kernel command `{command_name}`."
    )
    return {
        "status": "failed",
        "summary": {
            "command_name": command_name,
            "actor_role": actor_role,
            "resolved_actor_role": access.get("resolved_actor_role", ""),
        },
        "message": message,
        "access_policy": access,
    }


def init_run(run_dir: Path, run_id: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    registry = write_registry(run_dir)
    manifest = init_run_manifest(run_dir, run_id)
    cursor = init_round_cursor(run_dir, run_id)
    if not admission_policy_path(run_dir).exists():
        materialize_admission_policy(run_dir, run_id=run_id)
    if not runtime_health_path(run_dir).exists():
        materialize_runtime_health(run_dir)
    if not operator_runbook_path(run_dir).exists():
        materialize_operator_runbook(run_dir)
    return {
        "status": "completed",
        "summary": {"run_id": run_id, "run_dir": str(run_dir), "skill_count": int(registry.get("skill_count") or 0)},
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
        "paths": {
            "manifest_path": str(manifest_path(run_dir)),
            "cursor_path": str(cursor_path(run_dir)),
            "ledger_path": str(ledger_path(run_dir)),
            "registry_path": str(registry_path(run_dir)),
            "admission_policy_path": str(admission_policy_path(run_dir)),
            "runtime_health_path": str(runtime_health_path(run_dir)),
            "operator_runbook_path": str(operator_runbook_path(run_dir)),
        },
    }


def handle_early_runtime_command(args: Any) -> int | None:
    if args.command == "list-canonical-contracts":
        contracts = canonical_contracts_for_plane(plane=args.plane)
        payload = {
            "schema_version": "canonical-contract-list-v1",
            "status": "completed",
            "plane": args.plane or "all",
            "contracts": contracts,
            "summary": {
                "plane": args.plane or "all",
                "contract_count": len(contracts),
            },
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-schema-status":
        run_dir = resolve_run_dir(args.run_dir)
        payload = load_schema_status(run_dir, db_path=args.db_path)
        print(pretty_json(payload, args.pretty))
        return 0
    return None


def handle_runtime_command(args: Any, run_dir: Path) -> int | None:
    if args.command == "init-run":
        payload = init_run(run_dir, args.run_id)
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "run-skill":
        init_run(run_dir, args.run_id)
        skill_args = list(args.skill_args or [])
        if skill_args and skill_args[0] == "--":
            skill_args = skill_args[1:]
        try:
            payload = run_skill(
                run_dir,
                run_id=args.run_id,
                round_id=args.round_id,
                skill_name=args.skill_name,
                actor_role=args.actor_role,
                skill_args=skill_args,
                contract_mode=args.contract_mode,
                timeout_seconds=args.timeout_seconds,
                retry_budget=args.retry_budget,
                retry_backoff_ms=args.retry_backoff_ms,
                allow_side_effects=args.allow_side_effect,
                skill_approval_request_id=args.skill_approval_request_id,
            )
        except SkillExecutionError as exc:
            failure = exc.payload or {"status": "failed", "summary": {"skill_name": args.skill_name, "run_id": args.run_id, "round_id": args.round_id, "contract_mode": args.contract_mode}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "preflight-skill":
        init_run(run_dir, args.run_id)
        skill_args = list(args.skill_args or [])
        if skill_args and skill_args[0] == "--":
            skill_args = skill_args[1:]
        preflight = preflight_skill_execution(
            run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            skill_name=args.skill_name,
            actor_role=args.actor_role,
            skill_args=skill_args,
            contract_mode=args.contract_mode,
            timeout_seconds=args.timeout_seconds,
            retry_budget=args.retry_budget,
            retry_backoff_ms=args.retry_backoff_ms,
            allow_side_effects=args.allow_side_effect,
            skill_approval_request_id=args.skill_approval_request_id,
        )
        payload = {
            "status": "blocked" if bool(preflight.get("block_execution")) else "completed",
            "summary": {
                "skill_name": args.skill_name,
                "run_id": args.run_id,
                "round_id": args.round_id,
                "contract_mode": args.contract_mode,
                "skill_approval_request_id": args.skill_approval_request_id,
                "issue_count": preflight.get("issue_count", 0),
                "blocking_issue_count": preflight.get("blocking_issue_count", 0),
                "timeout_seconds": preflight.get("execution_policy", {}).get("timeout_seconds"),
                "retry_budget": preflight.get("execution_policy", {}).get("retry_budget"),
            },
            "preflight": preflight,
        }
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v3",
                "event_id": new_runtime_event_id("runtimeevt", args.run_id, args.round_id, args.skill_name, "preflight-only", args.contract_mode),
                "event_type": "skill-preflight",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "skill_name": args.skill_name,
                "actor_role": args.actor_role,
                "status": payload["status"],
                "contract_mode": args.contract_mode,
                "skill_approval_request_id": args.skill_approval_request_id,
                "execution_policy": preflight.get("execution_policy", {}),
                "preflight": preflight,
            },
        )
        print(pretty_json(payload, args.pretty))
        return 0 if payload["status"] != "blocked" else 1

    if args.command == "materialize-admission-policy":
        payload = materialize_admission_policy(
            run_dir,
            run_id=args.run_id,
            permission_profile=args.permission_profile,
            max_timeout_seconds=args.max_timeout_seconds,
            max_retry_budget=args.max_retry_budget,
            max_retry_backoff_ms=args.max_retry_backoff_ms,
            default_allow_side_effects=args.default_allow_side_effect,
            approval_required_side_effects=args.approval_required_side_effect,
            blocked_side_effects=args.blocked_side_effect,
            allowed_read_roots=args.allowed_read_root,
            allowed_write_roots=args.allowed_write_root,
            allowed_cwd_roots=args.allowed_cwd_root,
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-runtime-health":
        payload = materialize_runtime_health(run_dir, round_id=args.round_id)
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "materialize-operator-runbook":
        payload = {
            "status": "completed",
            "summary": {
                "run_dir": str(run_dir),
                "round_id": args.round_id,
            },
            "operator_runbook_path": materialize_operator_runbook(run_dir, round_id=args.round_id),
        }
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-dead-letters":
        dead_letters = load_dead_letters(run_dir, round_id=args.round_id, limit=args.limit)
        payload = {
            "status": "completed",
            "summary": {
                "run_dir": str(run_dir),
                "round_id": args.round_id,
                "dead_letter_count": len(dead_letters),
            },
            "dead_letters": dead_letters,
        }
        print(pretty_json(payload, args.pretty))
        return 0
    return None

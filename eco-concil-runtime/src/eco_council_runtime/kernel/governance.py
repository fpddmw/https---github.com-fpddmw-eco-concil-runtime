from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .access_policy import evaluate_skill_access
from .registry import resolve_skill_entry, workspace_root
from .skill_approvals import resolve_skill_approval_for_execution
from .skill_registry import SKILL_LAYER_OPTIONAL_ANALYSIS

CONTRACT_MODES = ("off", "warn", "strict")
BUILTIN_REQUIRED_INPUTS = {"run_dir", "run_id", "round_id"}
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_RETRY_BUDGET = 0
DEFAULT_RETRY_BACKOFF_MS = 250
DEFAULT_ALLOWED_SIDE_EFFECTS = {
    "reads-artifacts",
    "writes-artifacts",
    "reads-shared-state",
    "writes-shared-state",
}
APPROVAL_REQUIRED_SIDE_EFFECTS = {"network-external", "destructive-write"}


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def option_scalar(value: Any) -> str:
    if isinstance(value, list):
        if not value:
            return ""
        return maybe_text(value[-1])
    return maybe_text(value)


def parse_skill_options(skill_args: list[str]) -> dict[str, Any]:
    options: dict[str, Any] = {}
    index = 0
    while index < len(skill_args):
        token = skill_args[index]
        if not token.startswith("--"):
            index += 1
            continue
        key = token[2:].replace("-", "_")
        value: Any = True
        if index + 1 < len(skill_args) and not skill_args[index + 1].startswith("--"):
            value = skill_args[index + 1]
            index += 1
        existing = options.get(key)
        if existing is None:
            options[key] = value
        elif isinstance(existing, list):
            existing.append(value)
        else:
            options[key] = [existing, value]
        index += 1
    return options


def resolve_contract_paths(patterns: list[Any], run_dir: Path, substitutions: dict[str, Any]) -> list[str]:
    results: list[str] = []
    for item in patterns:
        pattern = maybe_text(item)
        if not pattern:
            continue
        resolved = pattern
        if resolved == "run_dir":
            resolved = str(run_dir.resolve())
        elif resolved.startswith("run_dir/"):
            resolved = str((run_dir / resolved.removeprefix("run_dir/")).resolve())
        for key, value in substitutions.items():
            resolved = resolved.replace(f"<{key}>", option_scalar(value))
        results.append(resolved)
    return results


def resolve_user_path(run_dir: Path, raw_path: str) -> str:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return str(candidate.resolve())


def value_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, list):
        return not any(maybe_text(item) for item in value)
    if isinstance(value, bool):
        return value is False
    return not maybe_text(value)


def path_option_entries(skill_options: dict[str, Any], run_dir: Path) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for key, value in skill_options.items():
        if not key.endswith("_path"):
            continue
        values = value if isinstance(value, list) else [value]
        for raw_value in values:
            text = maybe_text(raw_value)
            if not text:
                continue
            results.append(
                {
                    "option_name": key,
                    "raw_value": text,
                    "resolved_path": resolve_user_path(run_dir, text),
                }
            )
    return results


def collect_summary_paths(summary: dict[str, Any], run_dir: Path) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for key, value in summary.items():
        if not key.endswith("_path"):
            continue
        text = maybe_text(value)
        if not text:
            continue
        results.append({"field_name": key, "raw_value": text, "resolved_path": resolve_user_path(run_dir, text)})
    return results


def issue(
    code: str,
    message: str,
    *,
    severity: str = "error",
    field: str = "",
    blocking: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "message": message,
        "blocking": blocking,
    }
    if field:
        payload["field"] = field
    return payload


def path_within_run_dir(run_dir: Path, candidate_path: str) -> bool:
    run_dir_text = str(run_dir.resolve())
    return candidate_path == run_dir_text or candidate_path.startswith(run_dir_text + "/")


def normalize_side_effects(run_dir: Path, declared_side_effects: list[Any], resolved_read_paths: list[str], resolved_write_paths: list[str]) -> list[str]:
    values = [maybe_text(item) for item in declared_side_effects if maybe_text(item)]
    if any(not path_within_run_dir(run_dir, maybe_text(path)) for path in resolved_read_paths if maybe_text(path)):
        values.append("reads-shared-state")
    if any(not path_within_run_dir(run_dir, maybe_text(path)) for path in resolved_write_paths if maybe_text(path)):
        values.append("writes-shared-state")
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def normalize_allowed_side_effects(values: list[str] | None) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for value in [*DEFAULT_ALLOWED_SIDE_EFFECTS, *(values or [])]:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def normalize_execution_policy(
    declared_policy: dict[str, Any],
    *,
    timeout_seconds: float | None,
    retry_budget: int | None,
    retry_backoff_ms: int | None,
) -> dict[str, Any]:
    resolved_timeout = timeout_seconds
    if resolved_timeout is None:
        declared_timeout = declared_policy.get("timeout_seconds")
        resolved_timeout = float(declared_timeout) if isinstance(declared_timeout, (int, float)) else DEFAULT_TIMEOUT_SECONDS
    resolved_retry_budget = retry_budget
    if resolved_retry_budget is None:
        declared_budget = declared_policy.get("retry_budget")
        resolved_retry_budget = int(declared_budget) if isinstance(declared_budget, (int, float)) else DEFAULT_RETRY_BUDGET
    resolved_retry_backoff_ms = retry_backoff_ms
    if resolved_retry_backoff_ms is None:
        declared_backoff = declared_policy.get("retry_backoff_ms")
        resolved_retry_backoff_ms = int(declared_backoff) if isinstance(declared_backoff, (int, float)) else DEFAULT_RETRY_BACKOFF_MS
    return {
        "timeout_seconds": max(0.0, float(resolved_timeout or 0.0)),
        "retry_budget": max(0, int(resolved_retry_budget or 0)),
        "retry_backoff_ms": max(0, int(resolved_retry_backoff_ms or 0)),
    }


def build_contract_context(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    actor_role: str = "",
    skill_args: list[str],
    workspace: Path | None = None,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
    skill_approval_request_id: str = "",
) -> dict[str, Any]:
    root = workspace or workspace_root()
    skill_entry = resolve_skill_entry(skill_name, root)
    skill_options = parse_skill_options(skill_args)
    declared_inputs = skill_entry.get("declared_inputs", {}) if isinstance(skill_entry.get("declared_inputs"), dict) else {}
    required_inputs = declared_inputs.get("required", []) if isinstance(declared_inputs.get("required"), list) else []
    optional_inputs = declared_inputs.get("optional", []) if isinstance(declared_inputs.get("optional"), list) else []
    declared_contract = skill_entry.get("declared_contract", {}) if isinstance(skill_entry.get("declared_contract"), dict) else {}
    declared_reads = declared_contract.get("reads", []) if isinstance(declared_contract.get("reads"), list) else []
    declared_writes = declared_contract.get("writes", []) if isinstance(declared_contract.get("writes"), list) else []
    substitutions = {"run_id": run_id, "round_id": round_id, "skill_name": skill_name, **skill_options}
    resolved_read_paths = resolve_contract_paths(declared_reads, run_dir, substitutions)
    resolved_write_paths = resolve_contract_paths(declared_writes, run_dir, substitutions)
    declared_side_effects = skill_entry.get("declared_side_effects", []) if isinstance(skill_entry.get("declared_side_effects"), list) else []
    execution_policy = skill_entry.get("execution_policy", {}) if isinstance(skill_entry.get("execution_policy"), dict) else {}
    return {
        "workspace_root": str(root),
        "skill_entry": skill_entry,
        "actor_role": maybe_text(actor_role),
        "skill_options": skill_options,
        "required_inputs": required_inputs,
        "optional_inputs": optional_inputs,
        "declared_reads": declared_reads,
        "declared_writes": declared_writes,
        "declared_side_effects": normalize_side_effects(run_dir, declared_side_effects, resolved_read_paths, resolved_write_paths),
        "execution_policy": normalize_execution_policy(
            execution_policy,
            timeout_seconds=timeout_seconds,
            retry_budget=retry_budget,
            retry_backoff_ms=retry_backoff_ms,
        ),
        "allowed_side_effects": normalize_allowed_side_effects(allow_side_effects),
        "skill_approval_request_id": maybe_text(skill_approval_request_id),
        "resolved_read_paths": resolved_read_paths,
        "resolved_write_paths": resolved_write_paths,
        "substitutions": substitutions,
        "path_options": path_option_entries(skill_options, run_dir),
    }


def resolve_skill_approval_context(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    contract_mode: str,
    actor_role: str,
    access_policy: dict[str, Any],
    skill_approval_request_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    skill_policy = (
        access_policy.get("skill_policy", {})
        if isinstance(access_policy.get("skill_policy"), dict)
        else {}
    )
    requires_operator_approval = bool(skill_policy.get("requires_operator_approval"))
    skill_layer = maybe_text(skill_policy.get("skill_layer"))
    normalized_request_id = maybe_text(skill_approval_request_id)
    if not requires_operator_approval or skill_layer != SKILL_LAYER_OPTIONAL_ANALYSIS:
        return [], {
            "required": False,
            "status": "not-required",
            "request_id": normalized_request_id,
        }
    resolved_actor_role = maybe_text(access_policy.get("resolved_actor_role")) or maybe_text(
        actor_role
    )
    if not normalized_request_id:
        return [
            issue(
                "missing-skill-approval-request-id",
                "Optional-analysis execution requires --skill-approval-request-id for an operator-approved request.",
                field="skill_approval_request_id",
                blocking=True,
            )
        ], {
            "required": True,
            "status": "missing-request-id",
            "request_id": "",
        }
    try:
        request = resolve_skill_approval_for_execution(
            run_dir,
            request_id=normalized_request_id,
            skill_name=skill_name,
            run_id=run_id,
            round_id=round_id,
            requested_actor_role=resolved_actor_role,
        )
    except ValueError as exc:
        return [
            issue(
                "invalid-skill-approval-request",
                str(exc),
                field="skill_approval_request_id",
                blocking=True,
            )
        ], {
            "required": True,
            "status": "invalid-request",
            "request_id": normalized_request_id,
            "message": str(exc),
        }
    return [], {
        "required": True,
        "status": "approved",
        "request_id": normalized_request_id,
        "request": request,
        "contract_mode": contract_mode,
    }


def preflight_skill_execution(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    actor_role: str = "",
    skill_args: list[str],
    contract_mode: str = "warn",
    workspace: Path | None = None,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
    skill_approval_request_id: str = "",
) -> dict[str, Any]:
    context = build_contract_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        actor_role=actor_role,
        skill_args=skill_args,
        workspace=workspace,
        timeout_seconds=timeout_seconds,
        retry_budget=retry_budget,
        retry_backoff_ms=retry_backoff_ms,
        allow_side_effects=allow_side_effects,
        skill_approval_request_id=skill_approval_request_id,
    )
    issues: list[dict[str, str]] = []

    for raw_name in context["required_inputs"]:
        normalized_name = maybe_text(raw_name).replace("-", "_")
        if normalized_name in BUILTIN_REQUIRED_INPUTS:
            continue
        if value_missing(context["skill_options"].get(normalized_name)):
            issues.append(
                issue(
                    "missing-required-input",
                    f"Required input `{normalized_name}` is missing for {skill_name}.",
                    field=normalized_name,
                )
            )

    unresolved_paths = [
        path
        for path in [*context["resolved_read_paths"], *context["resolved_write_paths"]]
        if re.search(r"<[^>]+>", path)
    ]
    for unresolved_path in unresolved_paths:
        issues.append(
            issue(
                "unresolved-contract-placeholder",
                f"Declared contract path still contains unresolved placeholders: {unresolved_path}",
            )
        )

    allowed_declared_paths = set(context["resolved_read_paths"]) | set(context["resolved_write_paths"])
    if allowed_declared_paths:
        optional_input_names = {maybe_text(item).replace("-", "_") for item in context["optional_inputs"]}
        for path_option in context["path_options"]:
            option_name = path_option["option_name"]
            if option_name not in optional_input_names:
                continue
            if option_name == "artifact_path":
                continue
            if path_option["resolved_path"] not in allowed_declared_paths:
                issues.append(
                    issue(
                        "undeclared-path-override",
                        f"Path override `{option_name}` resolves to {path_option['resolved_path']}, which is outside the declared contract for {skill_name}.",
                        field=option_name,
                    )
                )

    allowed_side_effects = set(context["allowed_side_effects"])
    for side_effect in context["declared_side_effects"]:
        if side_effect in APPROVAL_REQUIRED_SIDE_EFFECTS and side_effect not in allowed_side_effects:
            issues.append(
                issue(
                    "missing-side-effect-approval",
                    f"Side effect `{side_effect}` requires explicit approval before {skill_name} can run in strict mode.",
                    field=side_effect,
                )
            )

    access_policy = evaluate_skill_access(
        skill_name,
        actor_role=context["actor_role"],
        contract_mode=contract_mode,
    )
    issues.extend(access_policy.get("issues", []) if isinstance(access_policy.get("issues"), list) else [])
    approval_issues, skill_approval = resolve_skill_approval_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        contract_mode=contract_mode,
        actor_role=context["actor_role"],
        access_policy=access_policy,
        skill_approval_request_id=context["skill_approval_request_id"],
    )
    issues.extend(approval_issues)

    blocking_issue_count = len(
        [
            item
            for item in issues
            if bool(item.get("blocking"))
            or (contract_mode == "strict" and item.get("severity") == "error")
        ]
    )
    block_execution = blocking_issue_count > 0
    return {
        "schema_version": "runtime-preflight-v1",
        "status": "blocked" if block_execution else "completed",
        "contract_mode": contract_mode,
        "run_id": run_id,
        "round_id": round_id,
        "skill_name": skill_name,
        "actor_role": context["actor_role"],
        "resolved_actor_role": access_policy.get("resolved_actor_role", ""),
        "skill_args": skill_args,
        "skill_options": context["skill_options"],
        "required_inputs": context["required_inputs"],
        "optional_inputs": context["optional_inputs"],
        "declared_reads": context["declared_reads"],
        "declared_writes": context["declared_writes"],
        "declared_side_effects": context["declared_side_effects"],
        "allowed_side_effects": context["allowed_side_effects"],
        "skill_approval_request_id": context["skill_approval_request_id"],
        "execution_policy": context["execution_policy"],
        "resolved_read_paths": context["resolved_read_paths"],
        "resolved_write_paths": context["resolved_write_paths"],
        "path_options": context["path_options"],
        "issues": issues,
        "issue_count": len(issues),
        "blocking_issue_count": blocking_issue_count,
        "block_execution": block_execution,
        "access_policy": access_policy,
        "skill_approval": skill_approval,
        "skill_access": access_policy.get("skill_policy", {}),
        "skill_registry_entry": context["skill_entry"],
        "workspace_root": context["workspace_root"],
    }


def postflight_skill_execution(
    run_dir: Path,
    *,
    skill_name: str,
    payload: dict[str, Any],
    preflight: dict[str, Any],
    contract_mode: str,
) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    declared_read_paths = preflight.get("resolved_read_paths", []) if isinstance(preflight.get("resolved_read_paths"), list) else []
    declared_write_paths = preflight.get("resolved_write_paths", []) if isinstance(preflight.get("resolved_write_paths"), list) else []
    allowed_paths = set(str(path) for path in [*declared_read_paths, *declared_write_paths])

    artifact_refs = payload.get("artifact_refs", []) if isinstance(payload.get("artifact_refs"), list) else []
    observed_artifact_paths: list[str] = []
    matched_declared_write_paths: set[str] = set()
    for index, ref in enumerate(artifact_refs):
        if not isinstance(ref, dict):
            issues.append(issue("invalid-artifact-ref", f"artifact_refs[{index}] is not an object."))
            continue
        artifact_path = maybe_text(ref.get("artifact_path"))
        record_locator = maybe_text(ref.get("record_locator"))
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if artifact_path:
            artifact_path = resolve_user_path(run_dir, artifact_path)
            observed_artifact_paths.append(artifact_path)
        if artifact_path and artifact_ref:
            valid_artifact_ref = artifact_ref == artifact_path or (record_locator and artifact_ref == f"{artifact_path}:{record_locator}")
            if not valid_artifact_ref:
                issues.append(
                    issue(
                        "artifact-ref-mismatch",
                        f"artifact_refs[{index}] has inconsistent artifact_ref `{artifact_ref}` for artifact_path `{artifact_path}`.",
                    )
                )
        if artifact_path and artifact_path in declared_write_paths:
            matched_declared_write_paths.add(artifact_path)

    summary = payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {}
    summary_paths = collect_summary_paths(summary, run_dir)
    observed_summary_paths = [item["resolved_path"] for item in summary_paths]
    for item in summary_paths:
        if allowed_paths and item["resolved_path"] not in allowed_paths:
            issues.append(
                issue(
                    "undeclared-summary-path",
                    f"Summary field `{item['field_name']}` resolves to {item['resolved_path']}, which is outside the declared contract for {skill_name}.",
                    field=item["field_name"],
                )
            )
        if item["resolved_path"] in declared_write_paths:
            matched_declared_write_paths.add(item["resolved_path"])

    payload_status = maybe_text(payload.get("status")) or "completed"
    if declared_write_paths and payload_status not in {"blocked", "failed"} and not matched_declared_write_paths:
        issues.append(
            issue(
                "missing-declared-write-evidence",
                f"Skill {skill_name} completed but emitted no artifact_ref or summary path that matches its declared write contract.",
            )
        )

    blocking_issue_count = len([item for item in issues if item.get("severity") == "error"])
    block_execution = contract_mode == "strict" and blocking_issue_count > 0
    return {
        "schema_version": "runtime-postflight-v1",
        "status": "blocked" if block_execution else "completed",
        "contract_mode": contract_mode,
        "skill_name": skill_name,
        "issues": issues,
        "issue_count": len(issues),
        "blocking_issue_count": blocking_issue_count,
        "block_execution": block_execution,
        "observed_artifact_paths": observed_artifact_paths,
        "observed_summary_paths": observed_summary_paths,
        "matched_declared_write_paths": sorted(matched_declared_write_paths),
    }

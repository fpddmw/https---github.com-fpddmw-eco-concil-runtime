"""Run-summary rendering and aggregate reporting helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.cli_invocation import runtime_module_command
from eco_council_runtime.controller.common import first_nonempty, maybe_int
from eco_council_runtime.controller.constants import (
    READINESS_ROLES,
    REPORT_ROLES,
    ROLES,
    SOURCE_SELECTION_ROLES,
    STAGE_AWAITING_DATA_READINESS,
    STAGE_AWAITING_DECISION,
    STAGE_AWAITING_EVIDENCE_CURATION,
    STAGE_AWAITING_INVESTIGATION_REVIEW,
    STAGE_AWAITING_MATCHING_ADJUDICATION,
    STAGE_AWAITING_MATCHING_AUTHORIZATION,
    STAGE_AWAITING_REPORTS,
    STAGE_AWAITING_SOURCE_SELECTION,
    STAGE_AWAITING_TASK_REVIEW,
    STAGE_COMPLETED,
    STAGE_READY_ADVANCE,
    STAGE_READY_DATA_PLANE,
    STAGE_READY_FETCH,
    STAGE_READY_MATCHING_ADJUDICATION,
    STAGE_READY_PREPARE,
    STAGE_READY_PROMOTE,
)
from eco_council_runtime.controller.io import load_json_if_exists, maybe_text, utc_now_iso
from eco_council_runtime.controller.paths import (
    cards_active_path,
    claims_active_path,
    data_readiness_report_path,
    decision_target_path,
    environment_signals_path,
    evidence_adjudication_path,
    fetch_execution_path,
    investigation_review_path,
    isolated_active_path,
    matching_authorization_path,
    matching_result_path,
    observations_active_path,
    public_signals_path,
    remands_open_path,
    report_target_path,
    reports_dir,
    require_round_id,
    shared_claims_path,
    shared_evidence_cards_path,
    shared_observations_path,
    source_selection_path,
    supervisor_current_step_path,
    supervisor_state_path,
    tasks_path,
)
from eco_council_runtime.controller.policy import (
    allowed_sources_for_role,
    effective_constraints,
    load_override_requests,
    policy_profile_summary,
)


def count_json_list(path: Path) -> int:
    payload = load_json_if_exists(path)
    return len(payload) if isinstance(payload, list) else 0


def count_jsonl_records(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def role_label_zh(role: str) -> str:
    return {
        "moderator": "议长",
        "sociologist": "社会学家",
        "environmentalist": "环境数据学家",
    }.get(role, role)


def stage_label_zh(stage: str) -> str:
    return {
        STAGE_AWAITING_TASK_REVIEW: "等待议长复审任务",
        STAGE_AWAITING_SOURCE_SELECTION: "等待专家选择数据源",
        STAGE_READY_PREPARE: "等待生成本轮抓取计划",
        STAGE_READY_FETCH: "等待执行抓取计划",
        STAGE_READY_DATA_PLANE: "等待归一化与证据筛选包生成",
        STAGE_AWAITING_EVIDENCE_CURATION: "等待专家提交证据筛选结果",
        STAGE_AWAITING_DATA_READINESS: "等待专家提交数据准备审计",
        STAGE_AWAITING_MATCHING_AUTHORIZATION: "等待议长授权匹配",
        STAGE_AWAITING_MATCHING_ADJUDICATION: "等待议长提交匹配裁定",
        STAGE_AWAITING_INVESTIGATION_REVIEW: "旧路径：等待议长提交因果链审查",
        STAGE_READY_MATCHING_ADJUDICATION: "等待物化匹配裁定并生成后匹配审查/报告产物",
        STAGE_AWAITING_REPORTS: "等待专家报告",
        STAGE_AWAITING_DECISION: "等待议长作出决议",
        STAGE_READY_PROMOTE: "等待正式写入本轮产物",
        STAGE_READY_ADVANCE: "等待推进到下一轮",
        STAGE_COMPLETED: "流程已完成",
    }.get(stage, stage or "未知阶段")


def report_status_label_zh(report: dict[str, Any] | None) -> str:
    if not isinstance(report, dict):
        return "未生成"
    summary = maybe_text(report.get("summary")).lower()
    if summary.startswith("pending "):
        return "待执行"
    return {
        "needs-more-evidence": "需要更多证据",
        "supported": "已支持",
        "not-supported": "不支持",
        "blocked": "阻塞",
        "complete": "完成",
    }.get(maybe_text(report.get("status")), maybe_text(report.get("status")) or "未知")


def sufficiency_label_zh(value: str) -> str:
    return {
        "insufficient": "不足",
        "partial": "部分充分",
        "sufficient": "充分",
    }.get(value, value or "未知")


def bool_label_zh(value: Any) -> str:
    return "是" if bool(value) else "否"


def format_list_zh(values: list[Any]) -> str:
    items = [maybe_text(value) for value in values if maybe_text(value)]
    return "、".join(items) if items else "无"


def round_number(round_id: str) -> int:
    normalized = require_round_id(round_id)
    return int(normalized.split("-")[1])


def infer_fetch_role(status: dict[str, Any]) -> str:
    step_id = maybe_text(status.get("step_id"))
    artifact_path = maybe_text(status.get("artifact_path"))
    for role in REPORT_ROLES:
        if role in step_id or f"/{role}/" in artifact_path:
            return role
    return ""


def round_status_label_zh(
    *,
    round_id: str,
    current_round_id: str,
    current_stage: str,
    decision: dict[str, Any] | None,
    fetch_execution: dict[str, Any] | None,
) -> str:
    if round_id == current_round_id:
        return stage_label_zh(current_stage)
    if isinstance(decision, dict):
        return "已完成并形成议长决议"
    if isinstance(fetch_execution, dict):
        return "已抓取数据，但尚未形成决议"
    return "已创建，尚未开始"


def default_summary_output_path(run_dir: Path, round_id: str = "", lang: str = "zh") -> Path:
    suffix = "" if lang == "zh" else f".{lang}"
    filename = f"eco_council_record{suffix}.md"
    if round_id:
        filename = f"eco_council_record_{round_id}{suffix}.md"
    return reports_dir(run_dir) / filename


def recommended_commands_for_stage(run_dir: Path, state: dict[str, Any]) -> list[str]:
    stage = maybe_text(state.get("stage"))
    def supervisor_command(*args: object) -> str:
        return runtime_module_command("supervisor", *args, "--run-dir", run_dir, "--yes", "--pretty")

    if stage == STAGE_AWAITING_TASK_REVIEW:
        return [supervisor_command("run-agent-step", "--role", "moderator")]
    if stage == STAGE_AWAITING_SOURCE_SELECTION:
        return [
            supervisor_command("run-agent-step", "--role", "sociologist"),
            supervisor_command("run-agent-step", "--role", "environmentalist"),
        ]
    if stage == STAGE_AWAITING_EVIDENCE_CURATION:
        return [
            supervisor_command("run-agent-step", "--role", "sociologist"),
            supervisor_command("run-agent-step", "--role", "environmentalist"),
        ]
    if stage in {
        STAGE_READY_PREPARE,
        STAGE_READY_FETCH,
        STAGE_READY_DATA_PLANE,
        STAGE_READY_MATCHING_ADJUDICATION,
        STAGE_READY_PROMOTE,
        STAGE_READY_ADVANCE,
    }:
        return [supervisor_command("continue-run")]
    if stage == STAGE_AWAITING_DATA_READINESS:
        return [
            supervisor_command("run-agent-step", "--role", "sociologist"),
            supervisor_command("run-agent-step", "--role", "environmentalist"),
        ]
    if stage == STAGE_AWAITING_MATCHING_AUTHORIZATION:
        return [supervisor_command("run-agent-step", "--role", "moderator")]
    if stage == STAGE_AWAITING_MATCHING_ADJUDICATION:
        return [supervisor_command("run-agent-step", "--role", "moderator")]
    if stage == STAGE_AWAITING_INVESTIGATION_REVIEW:
        return [supervisor_command("run-agent-step", "--role", "moderator")]
    if stage == STAGE_AWAITING_REPORTS:
        return [
            supervisor_command("run-agent-step", "--role", "sociologist"),
            supervisor_command("run-agent-step", "--role", "environmentalist"),
        ]
    if stage == STAGE_AWAITING_DECISION:
        return [supervisor_command("run-agent-step", "--role", "moderator")]
    return []


def collect_round_summary(run_dir: Path, state: dict[str, Any], round_id: str) -> dict[str, Any]:
    current_round_id = maybe_text(state.get("current_round_id"))
    current_stage = maybe_text(state.get("stage"))

    tasks_payload = load_json_if_exists(tasks_path(run_dir, round_id))
    tasks = tasks_payload if isinstance(tasks_payload, list) else []
    fetch_payload = load_json_if_exists(fetch_execution_path(run_dir, round_id))
    fetch = fetch_payload if isinstance(fetch_payload, dict) else {}
    fetch_statuses = fetch.get("statuses") if isinstance(fetch.get("statuses"), list) else []
    decision_payload = load_json_if_exists(decision_target_path(run_dir, round_id))
    decision = decision_payload if isinstance(decision_payload, dict) else None
    source_selections: dict[str, dict[str, Any] | None] = {}
    for role in SOURCE_SELECTION_ROLES:
        selection_payload = load_json_if_exists(source_selection_path(run_dir, round_id, role))
        source_selections[role] = selection_payload if isinstance(selection_payload, dict) else None
    reports: dict[str, dict[str, Any] | None] = {}
    for role in REPORT_ROLES:
        report_payload = load_json_if_exists(report_target_path(run_dir, round_id, role))
        reports[role] = report_payload if isinstance(report_payload, dict) else None
    readiness_reports: dict[str, dict[str, Any] | None] = {}
    for role in READINESS_ROLES:
        readiness_payload = load_json_if_exists(data_readiness_report_path(run_dir, round_id, role))
        readiness_reports[role] = readiness_payload if isinstance(readiness_payload, dict) else None
    matching_authorization = load_json_if_exists(matching_authorization_path(run_dir, round_id))
    investigation_review = load_json_if_exists(investigation_review_path(run_dir, round_id))
    matching_result = load_json_if_exists(matching_result_path(run_dir, round_id))
    evidence_adjudication = load_json_if_exists(evidence_adjudication_path(run_dir, round_id))
    override_requests_by_role = {
        role: load_override_requests(run_dir, round_id, role)
        for role in ROLES
    }

    return {
        "round_id": round_id,
        "round_number": round_number(round_id),
        "is_current_round": round_id == current_round_id,
        "status_label": round_status_label_zh(
            round_id=round_id,
            current_round_id=current_round_id,
            current_stage=current_stage,
            decision=decision,
            fetch_execution=fetch,
        ),
        "tasks": tasks,
        "task_count": len(tasks),
        "fetch": {
            "step_count": maybe_int(fetch.get("step_count")) if fetch else 0,
            "completed_count": maybe_int(fetch.get("completed_count")) if fetch else 0,
            "failed_count": maybe_int(fetch.get("failed_count")) if fetch else 0,
            "statuses": [item for item in fetch_statuses if isinstance(item, dict)],
        },
        "shared": {
            "claim_count": count_json_list(shared_claims_path(run_dir, round_id)),
            "observation_count": count_json_list(shared_observations_path(run_dir, round_id)),
            "evidence_count": count_json_list(shared_evidence_cards_path(run_dir, round_id)),
            "claims_active_count": count_json_list(claims_active_path(run_dir, round_id)),
            "observations_active_count": count_json_list(observations_active_path(run_dir, round_id)),
            "cards_active_count": count_json_list(cards_active_path(run_dir, round_id)),
            "isolated_count": count_json_list(isolated_active_path(run_dir, round_id)),
            "remand_count": count_json_list(remands_open_path(run_dir, round_id)),
        },
        "normalized": {
            "public_signal_count": count_jsonl_records(public_signals_path(run_dir, round_id)),
            "environment_signal_count": count_jsonl_records(environment_signals_path(run_dir, round_id)),
        },
        "source_selections": source_selections,
        "override_requests": override_requests_by_role,
        "readiness_reports": readiness_reports,
        "matching_authorization": matching_authorization if isinstance(matching_authorization, dict) else None,
        "investigation_review": investigation_review if isinstance(investigation_review, dict) else None,
        "matching_result": matching_result if isinstance(matching_result, dict) else None,
        "evidence_adjudication": evidence_adjudication if isinstance(evidence_adjudication, dict) else None,
        "reports": reports,
        "decision": decision,
    }


def _source_selection_state(summary: dict[str, Any]) -> tuple[bool, int]:
    selections = summary.get("source_selections", {}) if isinstance(summary.get("source_selections"), dict) else {}
    all_complete = True
    selected_count = 0
    for role in SOURCE_SELECTION_ROLES:
        payload = selections.get(role)
        if not isinstance(payload, dict):
            all_complete = False
            continue
        status = maybe_text(payload.get("status"))
        if status not in {"complete", "blocked"}:
            all_complete = False
        selected = payload.get("selected_sources")
        if isinstance(selected, list):
            selected_count += len([item for item in selected if maybe_text(item)])
    return all_complete, selected_count


def _build_current_issues_zh(round_summaries: list[dict[str, Any]], state: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    current_round_id = maybe_text(state.get("current_round_id"))
    current_stage = maybe_text(state.get("stage"))
    latest_decision_round = first_nonempty(
        [summary["round_id"] for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)]
    )
    latest_decision = next(
        (summary.get("decision") for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
        None,
    )
    if isinstance(latest_decision, dict):
        missing = latest_decision.get("missing_evidence_types")
        if isinstance(missing, list) and missing:
            issues.append(
                f"最新已完成决议（{latest_decision_round}）认为仍缺少这些证据类型：{format_list_zh(missing)}。"
            )

    for summary in round_summaries:
        fetch = summary.get("fetch", {})
        shared = summary.get("shared", {})
        auth_status = maybe_text((summary.get("matching_authorization") or {}).get("authorization_status"))
        if (
            maybe_int(fetch.get("completed_count")) > 0
            and maybe_int(shared.get("claim_count")) == 0
            and maybe_int(shared.get("evidence_count")) == 0
            and auth_status == "authorized"
        ):
            issues.append(
                f"{summary['round_id']} 已完成 {maybe_int(fetch.get('completed_count'))} 个抓取步骤，但共享层仍是 claims=0、evidence_cards=0。"
            )

    current_summary = next((summary for summary in round_summaries if summary["round_id"] == current_round_id), None)
    if isinstance(current_summary, dict):
        override_requests = current_summary.get("override_requests", {}) if isinstance(current_summary.get("override_requests"), dict) else {}
        pending_count = sum(len(value) for value in override_requests.values() if isinstance(value, list))
        if pending_count > 0:
            issues.append(
                f"{current_round_id} 当前存在 {pending_count} 个待上游处理的 override request；在 mission 边界被明确修改之前，这些请求不会自动生效。"
            )
    current_stage_allows_fetch = current_stage in {
        STAGE_READY_FETCH,
        STAGE_READY_DATA_PLANE,
        STAGE_AWAITING_EVIDENCE_CURATION,
        STAGE_AWAITING_DATA_READINESS,
        STAGE_AWAITING_MATCHING_AUTHORIZATION,
        STAGE_AWAITING_MATCHING_ADJUDICATION,
        STAGE_AWAITING_INVESTIGATION_REVIEW,
        STAGE_READY_MATCHING_ADJUDICATION,
        STAGE_AWAITING_REPORTS,
        STAGE_AWAITING_DECISION,
        STAGE_READY_PROMOTE,
        STAGE_READY_ADVANCE,
        STAGE_COMPLETED,
    }
    if isinstance(current_summary, dict) and maybe_int(current_summary.get("fetch", {}).get("step_count")) == 0 and current_stage_allows_fetch:
        selections_complete, selected_count = _source_selection_state(current_summary)
        if not (selections_complete and selected_count == 0):
            issues.append(f"{current_round_id} 当前停在“{current_summary['status_label']}”，还没有开始本轮抓取。")

    if not issues:
        issues.append("当前未检测到结构性阻塞，但仍需按阶段继续执行。")
    return issues


def render_run_summary_markdown(
    *,
    run_dir: Path,
    state: dict[str, Any],
    mission: dict[str, Any],
    round_summaries: list[dict[str, Any]],
    lang: str,
) -> str:
    region = mission.get("region", {}) if isinstance(mission.get("region"), dict) else {}
    window = mission.get("window", {}) if isinstance(mission.get("window"), dict) else {}
    constraints = effective_constraints(mission)
    profile = policy_profile_summary(mission)
    allowed_sources_by_role = {
        "sociologist": allowed_sources_for_role(mission, "sociologist"),
        "environmentalist": allowed_sources_for_role(mission, "environmentalist"),
    }
    current_round_id = maybe_text(state.get("current_round_id"))
    current_stage = maybe_text(state.get("stage"))
    latest_decision_summary = next(
        (summary for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
        None,
    )
    latest_decision = latest_decision_summary.get("decision") if isinstance(latest_decision_summary, dict) else None
    if lang not in {"zh", "en"}:
        raise ValueError(f"Unsupported summary language: {lang}")

    def role_label(role: str) -> str:
        if lang == "en":
            return {
                "moderator": "Moderator",
                "sociologist": "Sociologist",
                "environmentalist": "Environmentalist",
            }.get(role, role)
        return role_label_zh(role)

    def stage_label(stage: str) -> str:
        if lang == "en":
            return {
                STAGE_AWAITING_TASK_REVIEW: "Waiting for moderator task review",
                STAGE_AWAITING_SOURCE_SELECTION: "Waiting for expert source selection",
                STAGE_READY_PREPARE: "Waiting to prepare the round fetch plan",
                STAGE_READY_FETCH: "Waiting to execute the fetch plan",
                STAGE_READY_DATA_PLANE: "Waiting to run normalization and evidence-curation packet generation",
                STAGE_AWAITING_EVIDENCE_CURATION: "Waiting for expert evidence curation",
                STAGE_AWAITING_DATA_READINESS: "Waiting for expert data-readiness reports",
                STAGE_AWAITING_MATCHING_AUTHORIZATION: "Waiting for moderator matching authorization",
                STAGE_AWAITING_MATCHING_ADJUDICATION: "Waiting for moderator matching adjudication",
                STAGE_AWAITING_INVESTIGATION_REVIEW: "Legacy path: waiting for moderator investigation review",
                STAGE_READY_MATCHING_ADJUDICATION: "Waiting to materialize the adjudication and generate post-match review/report artifacts",
                STAGE_AWAITING_REPORTS: "Waiting for expert reports",
                STAGE_AWAITING_DECISION: "Waiting for moderator decision",
                STAGE_READY_PROMOTE: "Waiting to promote canonical outputs",
                STAGE_READY_ADVANCE: "Waiting to advance to the next round",
                STAGE_COMPLETED: "Workflow completed",
            }.get(stage, stage or "Unknown stage")
        return stage_label_zh(stage)

    def report_status_label(report: dict[str, Any] | None) -> str:
        if lang == "en":
            if not isinstance(report, dict):
                return "Not generated"
            summary_text = maybe_text(report.get("summary")).lower()
            if summary_text.startswith("pending "):
                return "Pending"
            return {
                "needs-more-evidence": "Needs more evidence",
                "supported": "Supported",
                "not-supported": "Not supported",
                "blocked": "Blocked",
                "complete": "Complete",
            }.get(maybe_text(report.get("status")), maybe_text(report.get("status")) or "Unknown")
        return report_status_label_zh(report)

    def sufficiency_label(value: str) -> str:
        if lang == "en":
            return {
                "insufficient": "Insufficient",
                "partial": "Partially sufficient",
                "sufficient": "Sufficient",
            }.get(value, value or "Unknown")
        return sufficiency_label_zh(value)

    def bool_label(value: Any) -> str:
        if lang == "en":
            return "Yes" if bool(value) else "No"
        return bool_label_zh(value)

    def format_list(values: list[Any]) -> str:
        items = [maybe_text(value) for value in values if maybe_text(value)]
        if not items:
            return "None" if lang == "en" else "无"
        return ", ".join(items) if lang == "en" else "、".join(items)

    def round_status_label(summary: dict[str, Any]) -> str:
        if lang == "en":
            if summary.get("is_current_round"):
                return stage_label(current_stage)
            if isinstance(summary.get("decision"), dict):
                return "Completed with moderator decision"
            if maybe_int(summary.get("fetch", {}).get("step_count")) > 0:
                return "Fetched data, decision not completed"
            return "Scaffolded, not started"
        return summary["status_label"]

    def current_issues() -> list[str]:
        if lang == "zh":
            return _build_current_issues_zh(round_summaries, state)
        issues: list[str] = []
        latest_decision_round = first_nonempty(
            [summary["round_id"] for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)]
        )
        latest_decision_local = next(
            (summary.get("decision") for summary in reversed(round_summaries) if isinstance(summary.get("decision"), dict)),
            None,
        )
        if isinstance(latest_decision_local, dict):
            missing = latest_decision_local.get("missing_evidence_types")
            if isinstance(missing, list) and missing:
                issues.append(
                    f"The latest completed decision ({latest_decision_round}) still marks these evidence types as missing: {format_list(missing)}."
                )
        for summary in round_summaries:
            fetch = summary.get("fetch", {})
            shared = summary.get("shared", {})
            auth_status = maybe_text((summary.get("matching_authorization") or {}).get("authorization_status"))
            if (
                maybe_int(fetch.get("completed_count")) > 0
                and maybe_int(shared.get("claim_count")) == 0
                and maybe_int(shared.get("evidence_count")) == 0
                and auth_status == "authorized"
            ):
                issues.append(
                    f"{summary['round_id']} completed {maybe_int(fetch.get('completed_count'))} fetch steps, but the shared layer still has claims=0 and evidence_cards=0."
                )
        current_summary = next((summary for summary in round_summaries if summary["round_id"] == current_round_id), None)
        if isinstance(current_summary, dict):
            override_requests = current_summary.get("override_requests", {}) if isinstance(current_summary.get("override_requests"), dict) else {}
            pending_count = sum(len(value) for value in override_requests.values() if isinstance(value, list))
            if pending_count > 0:
                issues.append(
                    f"{current_round_id} currently has {pending_count} pending override requests. They remain advisory until an upstream human/bot edits the mission envelope."
                )
        current_stage_allows_fetch = current_stage in {
            STAGE_READY_FETCH,
            STAGE_READY_DATA_PLANE,
            STAGE_AWAITING_EVIDENCE_CURATION,
            STAGE_AWAITING_DATA_READINESS,
            STAGE_AWAITING_MATCHING_AUTHORIZATION,
            STAGE_AWAITING_MATCHING_ADJUDICATION,
            STAGE_AWAITING_INVESTIGATION_REVIEW,
            STAGE_READY_MATCHING_ADJUDICATION,
            STAGE_AWAITING_REPORTS,
            STAGE_AWAITING_DECISION,
            STAGE_READY_PROMOTE,
            STAGE_READY_ADVANCE,
            STAGE_COMPLETED,
        }
        if isinstance(current_summary, dict) and maybe_int(current_summary.get("fetch", {}).get("step_count")) == 0 and current_stage_allows_fetch:
            selections_complete, selected_count = _source_selection_state(current_summary)
            if not (selections_complete and selected_count == 0):
                issues.append(
                    f"{current_round_id} is currently at '{round_status_label(current_summary)}' and has not started round-level fetching yet."
                )
        if not issues:
            issues.append("No structural blocker is currently detected, but the workflow still needs to advance stage by stage.")
        return issues

    labels = {
        "title": "# Eco Council Meeting Record" if lang == "en" else "# 生态议会记录报告",
        "generated_at": "Generated at" if lang == "en" else "生成时间",
        "topic": "Topic" if lang == "en" else "主题",
        "objective": "Objective" if lang == "en" else "目标",
        "region": "Region" if lang == "en" else "区域",
        "window": "Time window" if lang == "en" else "时间窗口",
        "current_round": "Current round" if lang == "en" else "当前轮次",
        "current_stage": "Current stage" if lang == "en" else "当前阶段",
        "round_count": "Round count" if lang == "en" else "轮次数量",
        "state_file": "State file" if lang == "en" else "运行状态文件",
        "constraints": "## Constraints" if lang == "en" else "## 任务边界",
        "policy_profile": "Policy profile" if lang == "en" else "策略画像",
        "max_rounds": "Max rounds" if lang == "en" else "最多轮次",
        "max_tasks": "Max tasks per round" if lang == "en" else "每轮最多任务",
        "max_claims": "Max claims per round" if lang == "en" else "每轮最多 claims",
        "sociologist_sources": "Allowed sociologist sources" if lang == "en" else "社会学家允许源",
        "environmentalist_sources": "Allowed environmentalist sources" if lang == "en" else "环境数据学家允许源",
        "overall": "## Overall Assessment" if lang == "en" else "## 总体判断",
        "latest_decision_round": "Latest completed decision round" if lang == "en" else "最新完成决议轮次",
        "needs_next_round": "Requires next round" if lang == "en" else "是否要求下一轮",
        "evidence_sufficiency": "Evidence sufficiency" if lang == "en" else "证据充分性",
        "completion_score": "Completion score" if lang == "en" else "完成度评分",
        "decision_summary": "Decision summary" if lang == "en" else "决议摘要",
        "missing_evidence": "Missing evidence types" if lang == "en" else "缺失证据类型",
        "no_decision": "- No completed moderator decision is available yet." if lang == "en" else "- 当前尚无已完成的议长决议。",
        "round_records": "## Round Records" if lang == "en" else "## 各轮记录",
        "round_status": "Round status" if lang == "en" else "轮次状态",
        "is_current_round": "Is current round" if lang == "en" else "是否当前轮",
        "task_count": "Task count" if lang == "en" else "任务数量",
        "task_list": "#### Tasks" if lang == "en" else "#### 任务列表",
        "no_tasks": "- No tasks are available for this round yet." if lang == "en" else "- 本轮尚无任务清单。",
        "source_selection": "#### Source Selection" if lang == "en" else "#### 数据源选择",
        "selected_sources": "Selected sources" if lang == "en" else "已选源",
        "override_requests": "Override requests" if lang == "en" else "越界请求",
        "no_override_requests": "No override requests." if lang == "en" else "无越界请求。",
        "no_source_selection": "No source-selection generated." if lang == "en" else "未生成 source-selection。",
        "source": "Evidence requirements" if lang == "en" else "证据需求",
        "depends_on": "Depends on" if lang == "en" else "依赖",
        "fetch": "#### Fetch Execution" if lang == "en" else "#### 数据抓取",
        "fetch_summary": "Total steps" if lang == "en" else "总步骤",
        "completed": "completed" if lang == "en" else "完成",
        "failed": "failed" if lang == "en" else "失败",
        "unknown_role": "Unknown role" if lang == "en" else "未知角色",
        "no_fetch": "- No fetch execution record exists for this round yet." if lang == "en" else "- 本轮尚未生成抓取执行记录。",
        "normalized": "#### Normalization and Shared Layer" if lang == "en" else "#### 归一化与共享层",
        "shared_claims": "Shared claims" if lang == "en" else "共享 claims",
        "shared_observations": "Shared observations" if lang == "en" else "共享 observations",
        "shared_evidence": "Shared evidence cards" if lang == "en" else "共享 evidence cards",
        "public_signals": "Sociologist public signals" if lang == "en" else "社会学家 public signals",
        "environment_signals": "Environmentalist environment signals" if lang == "en" else "环境数据学家 environment signals",
        "reports": "#### Expert Reports" if lang == "en" else "#### 专家报告",
        "no_report": "No report generated." if lang == "en" else "未生成报告。",
        "finding": "Finding" if lang == "en" else "发现",
        "decision": "#### Moderator Decision" if lang == "en" else "#### 议长决议",
        "approved_next_round_tasks": "Approved next-round task count" if lang == "en" else "批准的下一轮任务数",
        "no_round_decision": "- No moderator decision has been finalized for this round yet." if lang == "en" else "- 本轮尚未形成议长决议。",
        "issues": "## Current Issues" if lang == "en" else "## 当前主要问题",
        "next_steps": "## Recommended Next Steps" if lang == "en" else "## 建议下一步",
        "current_action": "Current recommended action" if lang == "en" else "当前建议动作",
        "reference_file": "Reference file" if lang == "en" else "参考文件",
        "recommended_command": "Recommended command" if lang == "en" else "推荐命令",
        "no_command": "- No mandatory follow-up command is required right now." if lang == "en" else "- 当前没有必须执行的后续命令。",
    }

    lines = [
        labels["title"],
        "",
        f"- {labels['generated_at']}：{utc_now_iso()}",
        f"- Run ID：`{maybe_text(mission.get('run_id'))}`",
        f"- {labels['topic']}：{maybe_text(mission.get('topic'))}",
        f"- {labels['objective']}：{maybe_text(mission.get('objective'))}",
        f"- {labels['region']}：{maybe_text(region.get('label'))}",
        f"- {labels['window']}：{maybe_text(window.get('start_utc'))} -> {maybe_text(window.get('end_utc'))}",
        f"- {labels['current_round']}：`{current_round_id}`",
        f"- {labels['current_stage']}：{stage_label(current_stage)}（`{current_stage}`）",
        f"- {labels['round_count']}：{len(round_summaries)}",
        f"- {labels['state_file']}：`{supervisor_state_path(run_dir)}`",
        "",
        labels["constraints"],
        "",
        f"- {labels['policy_profile']}：{maybe_text(profile.get('profile_id')) or maybe_text(mission.get('policy_profile'))}",
        f"- {labels['max_rounds']}：{maybe_int(constraints.get('max_rounds'))}",
        f"- {labels['max_tasks']}：{maybe_int(constraints.get('max_tasks_per_round'))}",
        f"- {labels['max_claims']}：{maybe_int(constraints.get('max_claims_per_round'))}",
        f"- {labels['sociologist_sources']}：{format_list(allowed_sources_by_role['sociologist'])}",
        f"- {labels['environmentalist_sources']}：{format_list(allowed_sources_by_role['environmentalist'])}",
        "",
        labels["overall"],
        "",
    ]
    if isinstance(latest_decision, dict):
        lines.extend(
            [
                f"- {labels['latest_decision_round']}：`{latest_decision_summary['round_id']}`",
                f"- {labels['needs_next_round']}：{bool_label(latest_decision.get('next_round_required'))}",
                f"- {labels['evidence_sufficiency']}：{sufficiency_label(maybe_text(latest_decision.get('evidence_sufficiency')))}",
                f"- {labels['completion_score']}：{latest_decision.get('completion_score')}",
                f"- {labels['decision_summary']}：{maybe_text(latest_decision.get('decision_summary'))}",
                f"- {labels['missing_evidence']}：{format_list(latest_decision.get('missing_evidence_types') if isinstance(latest_decision.get('missing_evidence_types'), list) else [])}",
            ]
        )
    else:
        lines.append(labels["no_decision"])

    lines.extend(["", labels["round_records"], ""])
    for summary in round_summaries:
        round_heading = (
            f"### Round {summary['round_number']} (`{summary['round_id']}`)"
            if lang == "en"
            else f"### 第 {summary['round_number']} 轮（`{summary['round_id']}`）"
        )
        lines.extend(
            [
                round_heading,
                "",
                f"- {labels['round_status']}：{round_status_label(summary)}",
                f"- {labels['is_current_round']}：{bool_label(summary['is_current_round'])}",
                f"- {labels['task_count']}：{summary['task_count']}",
                "",
                labels["task_list"],
                "",
            ]
        )
        tasks = summary.get("tasks", [])
        if isinstance(tasks, list) and tasks:
            for task in tasks:
                if not isinstance(task, dict):
                    continue
                line = (
                    f"- [{role_label(maybe_text(task.get('assigned_role')))}] `{maybe_text(task.get('task_id'))}`: "
                    if lang == "en"
                    else f"- [{role_label(maybe_text(task.get('assigned_role')))}] `{maybe_text(task.get('task_id'))}`："
                )
                line += (
                    f"{maybe_text(task.get('objective'))} {labels['source']}: "
                    f"{format_list([maybe_text(item.get('requirement_type')) for item in task.get('inputs', {}).get('evidence_requirements', []) if isinstance(task.get('inputs'), dict) and isinstance(task.get('inputs', {}).get('evidence_requirements'), list) and isinstance(item, dict) and maybe_text(item.get('requirement_type'))])}; "
                    f"{labels['depends_on']}: {format_list(task.get('depends_on') if isinstance(task.get('depends_on'), list) else [])}."
                )
                lines.append(line)
        else:
            lines.append(labels["no_tasks"])

        lines.extend(["", labels["source_selection"], ""])
        source_selections = summary.get("source_selections", {}) if isinstance(summary.get("source_selections"), dict) else {}
        for role in SOURCE_SELECTION_ROLES:
            selection = source_selections.get(role)
            if not isinstance(selection, dict):
                lines.append(f"- [{role_label(role)}] {labels['no_source_selection']}")
                continue
            selected_sources = selection.get("selected_sources") if isinstance(selection.get("selected_sources"), list) else []
            line = (
                f"- [{role_label(role)}] {maybe_text(selection.get('status'))}: {maybe_text(selection.get('summary'))} "
                f"{labels['selected_sources']}: {format_list(selected_sources)}."
            )
            lines.append(line)

        override_requests = summary.get("override_requests", {}) if isinstance(summary.get("override_requests"), dict) else {}
        lines.extend(["", f"#### {labels['override_requests']}", ""])
        pending_override_lines: list[str] = []
        for role in ROLES:
            values = override_requests.get(role)
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                if lang == "en":
                    pending_override_lines.append(
                        f"- [{role_label(role)}] `{maybe_text(item.get('request_id'))}` -> `{maybe_text(item.get('target_path'))}`: {maybe_text(item.get('summary'))}"
                    )
                else:
                    pending_override_lines.append(
                        f"- [{role_label(role)}] `{maybe_text(item.get('request_id'))}` -> `{maybe_text(item.get('target_path'))}`：{maybe_text(item.get('summary'))}"
                    )
        if pending_override_lines:
            lines.extend(pending_override_lines)
        else:
            lines.append(f"- {labels['no_override_requests']}")

        lines.extend(["", labels["fetch"], ""])
        fetch = summary.get("fetch", {})
        statuses = fetch.get("statuses", []) if isinstance(fetch.get("statuses"), list) else []
        if maybe_int(fetch.get("step_count")) > 0:
            if lang == "en":
                lines.append(
                    f"- {labels['fetch_summary']}: {maybe_int(fetch.get('step_count'))}; {labels['completed']}: {maybe_int(fetch.get('completed_count'))}; {labels['failed']}: {maybe_int(fetch.get('failed_count'))}."
                )
            else:
                lines.append(
                    f"- {labels['fetch_summary']}：{maybe_int(fetch.get('step_count'))}；{labels['completed']}：{maybe_int(fetch.get('completed_count'))}；{labels['failed']}：{maybe_int(fetch.get('failed_count'))}。"
                )
            for status in statuses:
                if not isinstance(status, dict):
                    continue
                prefix = role_label(infer_fetch_role(status)) or labels["unknown_role"]
                if lang == "en":
                    lines.append(f"- [{prefix}] `{maybe_text(status.get('source_skill'))}`: {maybe_text(status.get('status'))}.")
                else:
                    lines.append(f"- [{prefix}] `{maybe_text(status.get('source_skill'))}`：{maybe_text(status.get('status'))}。")
        else:
            lines.append(labels["no_fetch"])

        shared = summary.get("shared", {})
        normalized = summary.get("normalized", {})
        lines.extend(
            [
                "",
                labels["normalized"],
                "",
                f"- {labels['shared_claims']}：{maybe_int(shared.get('claim_count'))}",
                f"- {labels['shared_observations']}：{maybe_int(shared.get('observation_count'))}",
                f"- {labels['shared_evidence']}：{maybe_int(shared.get('evidence_count'))}",
                f"- {labels['public_signals']}：{maybe_int(normalized.get('public_signal_count'))}",
                f"- {labels['environment_signals']}：{maybe_int(normalized.get('environment_signal_count'))}",
                "",
                labels["reports"],
                "",
            ]
        )
        for role in REPORT_ROLES:
            report = summary.get("reports", {}).get(role) if isinstance(summary.get("reports"), dict) else None
            if not isinstance(report, dict):
                lines.append(f"- [{role_label(role)}] {labels['no_report']}")
                continue
            if lang == "en":
                lines.append(f"- [{role_label(role)}] {report_status_label(report)}: {maybe_text(report.get('summary'))}")
            else:
                lines.append(f"- [{role_label(role)}] {report_status_label(report)}：{maybe_text(report.get('summary'))}")
            findings = report.get("findings") if isinstance(report.get("findings"), list) else []
            for finding in findings[:2]:
                if not isinstance(finding, dict):
                    continue
                title = maybe_text(finding.get("title"))
                summary_text = first_nonempty([title, maybe_text(finding.get("summary"))])
                if summary_text:
                    lines.append(f"- [{role_label(role)}/{labels['finding']}] {summary_text}")

        lines.extend(["", labels["decision"], ""])
        decision = summary.get("decision")
        if isinstance(decision, dict):
            lines.extend(
                [
                    f"- {labels['needs_next_round']}：{bool_label(decision.get('next_round_required'))}",
                    f"- {labels['evidence_sufficiency']}：{sufficiency_label(maybe_text(decision.get('evidence_sufficiency')))}",
                    f"- {labels['completion_score']}：{decision.get('completion_score')}",
                    f"- {labels['decision_summary']}：{maybe_text(decision.get('decision_summary'))}",
                    f"- {labels['missing_evidence']}：{format_list(decision.get('missing_evidence_types') if isinstance(decision.get('missing_evidence_types'), list) else [])}",
                    f"- {labels['approved_next_round_tasks']}：{len(decision.get('next_round_tasks', [])) if isinstance(decision.get('next_round_tasks'), list) else 0}",
                ]
            )
        else:
            lines.append(labels["no_round_decision"])
        lines.append("")

    lines.extend([labels["issues"], ""])
    for issue in current_issues():
        lines.append(f"- {issue}")

    lines.extend(["", labels["next_steps"], ""])
    lines.append(f"- {labels['current_action']}：{stage_label(current_stage)}。")
    lines.append(f"- {labels['reference_file']}：`{supervisor_current_step_path(run_dir)}`")
    commands = recommended_commands_for_stage(run_dir, state)
    if commands:
        for command in commands:
            lines.append(f"- {labels['recommended_command']}：`{command}`")
    else:
        lines.append(labels["no_command"])
    return "\n".join(lines).rstrip() + "\n"

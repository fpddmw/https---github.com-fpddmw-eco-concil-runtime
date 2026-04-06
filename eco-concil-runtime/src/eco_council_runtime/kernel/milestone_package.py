from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .progress_dashboard import (
    PROGRESS_ENTRY_RE,
    DashboardModel,
    DeliveryEntry,
    StageDefinition,
    format_stage_label,
    latest_delivery_for_route,
    load_dashboard_model,
    next_queue_item,
    parse_crosswalk,
    route_stage_groups,
)

SECTION_LINE_RE = re.compile(r"^([A-Za-z][A-Za-z0-9 /-]+):\s*(.*)$")
TOP_LEVEL_SECTION_NAMES = {
    "Status",
    "Objective",
    "Implementation",
    "Validation",
    "Tests added or extended",
    "Known limitations",
    "Next",
}


@dataclass(frozen=True)
class ProgressEntryDetail:
    date: str
    raw_stage_id: str
    normalized_stage_id: str
    title: str
    status: str
    sections: dict[str, list[str]]
    order_index: int


@dataclass(frozen=True)
class RenderedMilestonePackage:
    manifest: dict[str, Any]
    files: dict[str, str]


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def current_date_iso() -> str:
    return datetime.now().date().isoformat()


def json_text(value: Any, *, pretty: bool = False) -> str:
    if pretty:
        return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def parse_progress_entry_details(progress_text: str, crosswalk: dict[str, str]) -> list[ProgressEntryDetail]:
    matches = list(PROGRESS_ENTRY_RE.finditer(progress_text))
    results: list[ProgressEntryDetail] = []
    crosswalk_active = True
    for index, match in enumerate(matches):
        date_text, raw_stage_id, title = match.groups()
        block_start = match.end()
        block_end = matches[index + 1].start() if index + 1 < len(matches) else len(progress_text)
        block = progress_text[block_start:block_end]
        normalized_stage_id = (
            crosswalk.get(raw_stage_id, raw_stage_id) if crosswalk_active else raw_stage_id
        )
        sections: dict[str, list[str]] = {}
        current_section = ""
        for raw_line in block.splitlines():
            stripped = raw_line.rstrip()
            if not stripped:
                continue
            section_match = SECTION_LINE_RE.match(stripped)
            if section_match and section_match.group(1) in TOP_LEVEL_SECTION_NAMES:
                current_section = section_match.group(1)
                sections.setdefault(current_section, [])
                inline_value = maybe_text(section_match.group(2))
                if inline_value:
                    sections[current_section].append(inline_value)
                continue
            if current_section:
                sections.setdefault(current_section, []).append(stripped)
        status = maybe_text(sections.get("Status", [""])[0] if sections.get("Status") else "")
        results.append(
            ProgressEntryDetail(
                date=date_text,
                raw_stage_id=raw_stage_id,
                normalized_stage_id=normalized_stage_id,
                title=maybe_text(title),
                status=status,
                sections=sections,
                order_index=index,
            )
        )
        if raw_stage_id == "D2" and "Master Plan And Route Normalization" in maybe_text(title):
            crosswalk_active = False
    return results


def section_bullets(entry: ProgressEntryDetail, section_name: str) -> list[str]:
    lines = entry.sections.get(section_name, [])
    if not lines:
        return []
    results: list[str] = []
    current = ""
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- "):
            if current:
                results.append(current)
            current = maybe_text(stripped[2:])
            continue
        if current and (line.startswith("  ") or line.startswith("\t")):
            current = f"{current} {maybe_text(stripped.lstrip('-').strip())}".strip()
            continue
        if current:
            results.append(current)
            current = ""
        if stripped:
            results.append(maybe_text(stripped))
    if current:
        results.append(current)
    return [item for item in results if item]


def strip_wrapping_backticks(value: str) -> str:
    text = maybe_text(value).strip()
    while len(text) >= 2 and text.startswith("`") and text.endswith("`"):
        text = text[1:-1].strip()
    return text


def deduped_validation_commands(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    results: list[dict[str, str]] = []
    for item in values:
        route_code = maybe_text(item.get("route_code"))
        command = maybe_text(item.get("command"))
        key = (route_code, command)
        if not route_code or not command or key in seen:
            continue
        seen.add(key)
        results.append(item)
    return results


def latest_detail_by_route(
    route_code: str,
    *,
    deliveries: list[DeliveryEntry],
    detail_map: dict[int, ProgressEntryDetail],
    stage_map: dict[str, StageDefinition],
) -> ProgressEntryDetail | None:
    delivery = latest_delivery_for_route(route_code, deliveries, stage_map)
    if delivery is None:
        return None
    return detail_map.get(delivery.order_index)


def route_snapshot_rows(model: DashboardModel) -> list[dict[str, Any]]:
    stage_map = {stage.stage_id: stage for stage in model.stage_definitions}
    route_groups = route_stage_groups(model.stage_definitions)
    rows: list[dict[str, Any]] = []
    for route_code in ["A", "B", "C", "D"]:
        stages = route_groups.get(route_code, [])
        if not stages:
            continue
        completed = sum(1 for stage in stages if stage.status == "completed")
        next_stage = next(
            (stage for stage in stages if stage.status not in {"completed", "deferred"}),
            None,
        )
        latest_delivery = latest_delivery_for_route(route_code, model.deliveries, stage_map)
        rows.append(
            {
                "route_code": route_code,
                "route_name": stages[0].route_name,
                "completed_stage_count": completed,
                "total_stage_count": len(stages),
                "next_stage_id": maybe_text(next_stage.stage_id if next_stage else ""),
                "next_stage_title": maybe_text(next_stage.title if next_stage else ""),
                "latest_delivery_date": maybe_text(latest_delivery.date if latest_delivery else ""),
                "latest_delivery_stage_id": maybe_text(
                    latest_delivery.normalized_stage_id if latest_delivery else ""
                ),
                "latest_delivery_title": maybe_text(latest_delivery.title if latest_delivery else ""),
            }
        )
    return rows


def supplementary_report_dirs(output_dir: Path) -> list[str]:
    reports_root = output_dir.parent
    if not reports_root.exists():
        return []
    results: list[str] = []
    for candidate in sorted(reports_root.iterdir()):
        if not candidate.is_dir():
            continue
        if candidate.resolve() == output_dir.resolve():
            continue
        results.append(str(candidate))
    return results


def markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        padded = row + [""] * max(0, len(headers) - len(row))
        lines.append("| " + " | ".join(padded[: len(headers)]) + " |")
    return lines


def render_readme(manifest: dict[str, Any]) -> str:
    route_summary = ", ".join(
        f"{row['route_code']} {row['completed_stage_count']}/{row['total_stage_count']}"
        for row in manifest.get("route_snapshot", [])
    )
    supplementary_dirs = manifest.get("supplementary_report_dirs", [])
    lines = [
        "# OpenClaw DB-First Milestone Package",
        "",
        f"本目录是当前仓库控制状态的固定里程碑包，生成日期为 `{manifest.get('package_date')}`。",
        "",
        "当前快照：",
        f"- Latest delivered increment: {manifest.get('latest_delivery_label')}",
        f"- Next recommended stage: {manifest.get('next_stage_label')}",
        f"- Completed stage count: `{manifest.get('summary', {}).get('completed_stage_count')} / {manifest.get('summary', {}).get('stage_count')}`",
        f"- Route completion snapshot: {route_summary or 'none'}",
        "",
        "输入来源：",
        f"- `{manifest.get('source_paths', {}).get('master_plan_path')}`",
        f"- `{manifest.get('source_paths', {}).get('progress_log_path')}`",
        f"- `{manifest.get('source_paths', {}).get('dashboard_path')}`",
        "",
        "包内文件：",
        "1. `package_manifest.json`",
        "2. `01-executive-summary.md`",
        "3. `02-acceptance-and-demo.md`",
        "4. `03-risk-register.md`",
        "5. `04-next-steps.md`",
    ]
    if supplementary_dirs:
        lines.extend(
            [
                "",
                "仓内已发现的补充汇报目录：",
            ]
        )
        for item in supplementary_dirs:
            lines.append(f"- `{item}`")
    return "\n".join(lines) + "\n"


def render_executive_summary(manifest: dict[str, Any]) -> str:
    summary = manifest.get("summary", {})
    lines = [
        "# Executive Summary",
        "",
        "## Snapshot",
        "",
    ]
    lines.extend(
        markdown_table(
            ["Signal", "Value"],
            [
                ["Package date", f"`{manifest.get('package_date')}`"],
                ["Generated at", f"`{manifest.get('generated_at_utc')}`"],
                ["Latest delivered increment", manifest.get("latest_delivery_label", "none")],
                ["Next recommended stage", manifest.get("next_stage_label", "none")],
                ["Completed stage count", f"`{summary.get('completed_stage_count')} / {summary.get('stage_count')}`"],
                ["Planned stage count", f"`{summary.get('planned_stage_count')}`"],
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Route Snapshot",
            "",
        ]
    )
    route_rows = [
        [
            f"`{row['route_code']}` {row['route_name']}",
            f"`{row['completed_stage_count']} / {row['total_stage_count']}`",
            format_stage_label(row["next_stage_id"], row["next_stage_title"])
            if row["next_stage_id"]
            else "none",
            format_stage_label(row["latest_delivery_stage_id"], row["latest_delivery_title"])
            if row["latest_delivery_stage_id"]
            else "none",
        ]
        for row in manifest.get("route_snapshot", [])
    ]
    lines.extend(
        markdown_table(
            ["Route", "Completed", "Next Stage", "Latest Delivery"],
            route_rows,
        )
    )
    lines.extend(
        [
            "",
            "## Recent Deliveries",
            "",
        ]
    )
    recent_rows = [
        [
            maybe_text(item.get("date")),
            format_stage_label(
                maybe_text(item.get("stage_id")),
                maybe_text(item.get("title")),
            ),
            f"`{maybe_text(item.get('status'))}`",
        ]
        for item in manifest.get("recent_deliveries", [])
    ]
    lines.extend(markdown_table(["Date", "Stage", "Status"], recent_rows))
    return "\n".join(lines) + "\n"


def render_acceptance_and_demo(
    manifest: dict[str, Any],
    *,
    output_dir: Path,
) -> str:
    validation_commands = manifest.get("validation_commands", [])
    grouped_commands: dict[str, list[dict[str, str]]] = {}
    for item in validation_commands:
        grouped_commands.setdefault(maybe_text(item.get("route_code")), []).append(item)
    lines = [
        "# Acceptance And Demo",
        "",
        "## Package Generation",
        "",
        "```bash",
        f"python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir {output_dir}",
        "```",
        "",
        "## Recommended Validation Commands",
        "",
    ]
    if not validation_commands:
        lines.append("- none")
    else:
        for route_code in ["A", "B", "C", "D"]:
            items = grouped_commands.get(route_code, [])
            if not items:
                continue
            source_label = format_stage_label(
                maybe_text(items[0].get("stage_id")),
                maybe_text(items[0].get("stage_title")),
            )
            lines.append(f"### Route {route_code}")
            lines.append("")
            lines.append(f"Source delivery: {source_label}")
            lines.append("")
            for item in items:
                lines.append(f"- `{maybe_text(item.get('command'))}`")
            lines.append("")
    lines.extend(
        [
            "## Demo Walkthrough",
            "",
            "```bash",
            "python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty",
            f"python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir {output_dir} --pretty",
            "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py show-run-state --run-dir <run_dir> --round-id <round_id> --pretty",
            "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py list-analysis-result-sets --run-dir <run_dir> --run-id <run_id> --round-id <round_id> --analysis-kind claim-cluster --latest-only --include-contract --pretty",
            "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py query-analysis-result-items --run-dir <run_dir> --run-id <run_id> --round-id <round_id> --analysis-kind claim-cluster --latest-only --subject-id <cluster_id> --include-result-sets --include-contract --pretty",
            "```",
        ]
    )
    return "\n".join(lines) + "\n"


def render_risk_register(manifest: dict[str, Any]) -> str:
    rows = [
        [
            f"`{item['route_code']}` {item['route_name']}",
            format_stage_label(item["stage_id"], item["stage_title"]),
            maybe_text(item["risk"]),
        ]
        for item in manifest.get("risks", [])
    ]
    lines = [
        "# Risk Register",
        "",
        "当前风险来自各路线最近一次交付中仍然保留的 `Known limitations`，因此它们代表的是“当前残余限制”，而不是更早已被后续阶段解决的问题。",
        "",
    ]
    if not rows:
        lines.append("- none")
        lines.append("")
        return "\n".join(lines) + "\n"
    lines.extend(markdown_table(["Route", "Source Delivery", "Residual Risk"], rows))
    return "\n".join(lines) + "\n"


def render_next_steps(manifest: dict[str, Any]) -> str:
    queue_rows = [
        [
            maybe_text(item.get("order")),
            format_stage_label(
                maybe_text(item.get("stage_id")),
                maybe_text(item.get("stage_title")),
            ),
            f"`{maybe_text(item.get('status'))}`",
            maybe_text(item.get("reason")),
        ]
        for item in manifest.get("queue", [])
    ]
    lines = [
        "# Next Steps",
        "",
        f"Immediate recommended stage: {manifest.get('next_stage_label', 'none')}",
        "",
        "## Near-Term Queue",
        "",
    ]
    lines.extend(markdown_table(["Order", "Stage", "Status", "Why Now"], queue_rows))
    next_bullets = manifest.get("latest_delivery_next_steps", [])
    if next_bullets:
        lines.extend(
            [
                "",
                "## Follow-Up From Latest Delivery",
                "",
            ]
        )
        for item in next_bullets:
            lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def build_milestone_manifest(
    *,
    model: DashboardModel,
    detail_map: dict[int, ProgressEntryDetail],
    package_date: str,
    output_dir: Path,
    source_paths: dict[str, str],
) -> dict[str, Any]:
    stage_map = {stage.stage_id: stage for stage in model.stage_definitions}
    route_rows = route_snapshot_rows(model)
    latest_delivery = model.deliveries[-1] if model.deliveries else None
    next_stage = next_queue_item(model.queue, stage_map)
    completed_stage_count = len(
        [stage for stage in model.stage_definitions if stage.status == "completed"]
    )
    planned_stage_count = len(
        [stage for stage in model.stage_definitions if stage.status == "planned"]
    )
    latest_route_details: list[ProgressEntryDetail] = []
    for route_code in ["A", "B", "C", "D"]:
        detail = latest_detail_by_route(
            route_code,
            deliveries=model.deliveries,
            detail_map=detail_map,
            stage_map=stage_map,
        )
        if detail is not None:
            latest_route_details.append(detail)

    risk_items: list[dict[str, str]] = []
    validation_commands: list[dict[str, str]] = []
    for detail in latest_route_details:
        stage = stage_map.get(detail.normalized_stage_id)
        route_code = maybe_text(stage.route_code if stage else "")
        route_name = maybe_text(stage.route_name if stage else "")
        for risk in section_bullets(detail, "Known limitations"):
            risk_items.append(
                {
                    "route_code": route_code,
                    "route_name": route_name,
                    "stage_id": detail.normalized_stage_id,
                    "stage_title": detail.title,
                    "risk": risk,
                }
            )
        for command in section_bullets(detail, "Validation"):
            normalized_command = strip_wrapping_backticks(command)
            if normalized_command.startswith("python3 ") or normalized_command.startswith("pytest "):
                validation_commands.append(
                    {
                        "route_code": route_code,
                        "route_name": route_name,
                        "stage_id": detail.normalized_stage_id,
                        "stage_title": detail.title,
                        "command": normalized_command,
                    }
                )

    recent_deliveries = [
        {
            "date": entry.date,
            "stage_id": entry.normalized_stage_id,
            "title": entry.title,
            "status": entry.status,
        }
        for entry in reversed(model.deliveries[-8:])
    ]
    queue = []
    for item in model.queue:
        stage = stage_map.get(item.stage_id)
        queue.append(
            {
                "order": item.order,
                "stage_id": item.stage_id,
                "stage_title": maybe_text(stage.title if stage else ""),
                "route_code": item.route_code,
                "status": maybe_text(stage.status if stage else ""),
                "reason": item.reason,
                "expected_delivery": item.expected_delivery,
            }
        )
    latest_next_steps = []
    if latest_delivery is not None:
        latest_detail = detail_map.get(latest_delivery.order_index)
        if latest_detail is not None:
            latest_next_steps = section_bullets(latest_detail, "Next")

    return {
        "schema_version": "milestone-package-v1",
        "package_date": package_date,
        "generated_at_utc": utc_now_iso(),
        "summary": {
            "stage_count": len(model.stage_definitions),
            "completed_stage_count": completed_stage_count,
            "planned_stage_count": planned_stage_count,
            "delivery_count": len(model.deliveries),
            "route_count": len(route_rows),
        },
        "source_paths": source_paths,
        "output_dir": str(output_dir),
        "latest_delivery": {
            "date": maybe_text(latest_delivery.date if latest_delivery else ""),
            "stage_id": maybe_text(
                latest_delivery.normalized_stage_id if latest_delivery else ""
            ),
            "title": maybe_text(latest_delivery.title if latest_delivery else ""),
            "status": maybe_text(latest_delivery.status if latest_delivery else ""),
        },
        "latest_delivery_label": format_stage_label(
            maybe_text(latest_delivery.normalized_stage_id if latest_delivery else ""),
            maybe_text(latest_delivery.title if latest_delivery else ""),
        )
        if latest_delivery is not None
        else "none",
        "next_stage": {
            "stage_id": maybe_text(next_stage.stage_id if next_stage else ""),
            "title": maybe_text(
                stage_map[next_stage.stage_id].title
                if next_stage is not None and next_stage.stage_id in stage_map
                else ""
            ),
        },
        "next_stage_label": format_stage_label(
            maybe_text(next_stage.stage_id if next_stage else ""),
            maybe_text(
                stage_map[next_stage.stage_id].title
                if next_stage is not None and next_stage.stage_id in stage_map
                else ""
            ),
        )
        if next_stage is not None
        else "none",
        "route_snapshot": route_rows,
        "recent_deliveries": recent_deliveries,
        "queue": queue,
        "risks": risk_items,
        "validation_commands": deduped_validation_commands(validation_commands),
        "latest_delivery_next_steps": latest_next_steps,
        "supplementary_report_dirs": supplementary_report_dirs(output_dir),
    }


def render_milestone_package_from_paths(
    *,
    master_plan_path: Path,
    progress_log_path: Path,
    dashboard_path: Path,
    package_date: str,
    output_dir: Path,
) -> RenderedMilestonePackage:
    model = load_dashboard_model(
        master_plan_path=master_plan_path,
        progress_log_path=progress_log_path,
    )
    progress_text = progress_log_path.read_text(encoding="utf-8")
    crosswalk = parse_crosswalk(master_plan_path.read_text(encoding="utf-8"))
    details = parse_progress_entry_details(progress_text, crosswalk)
    detail_map = {item.order_index: item for item in details}
    manifest = build_milestone_manifest(
        model=model,
        detail_map=detail_map,
        package_date=package_date,
        output_dir=output_dir,
        source_paths={
            "master_plan_path": str(master_plan_path),
            "progress_log_path": str(progress_log_path),
            "dashboard_path": str(dashboard_path),
        },
    )
    files = {
        "package_manifest.json": json_text(manifest, pretty=True),
        "README.md": render_readme(manifest),
        "01-executive-summary.md": render_executive_summary(manifest),
        "02-acceptance-and-demo.md": render_acceptance_and_demo(
            manifest,
            output_dir=output_dir,
        ),
        "03-risk-register.md": render_risk_register(manifest),
        "04-next-steps.md": render_next_steps(manifest),
    }
    return RenderedMilestonePackage(manifest=manifest, files=files)


def materialize_milestone_package_from_paths(
    *,
    master_plan_path: Path,
    progress_log_path: Path,
    dashboard_path: Path,
    output_dir: Path,
    package_date: str = "",
) -> dict[str, Any]:
    resolved_output_dir = output_dir.expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_date = maybe_text(package_date) or current_date_iso()
    rendered = render_milestone_package_from_paths(
        master_plan_path=master_plan_path.expanduser().resolve(),
        progress_log_path=progress_log_path.expanduser().resolve(),
        dashboard_path=dashboard_path.expanduser().resolve(),
        package_date=resolved_date,
        output_dir=resolved_output_dir,
    )
    for file_name, content in rendered.files.items():
        (resolved_output_dir / file_name).write_text(content, encoding="utf-8")
    return {
        "status": "completed",
        "summary": {
            "output_dir": str(resolved_output_dir),
            "package_date": resolved_date,
            "generated_file_count": len(rendered.files),
            "completed_stage_count": rendered.manifest.get("summary", {}).get(
                "completed_stage_count", 0
            ),
            "stage_count": rendered.manifest.get("summary", {}).get("stage_count", 0),
            "next_stage": rendered.manifest.get("next_stage", {}).get("stage_id", ""),
            "latest_delivery_stage": rendered.manifest.get("latest_delivery", {}).get(
                "stage_id", ""
            ),
        },
        "output_files": [
            str((resolved_output_dir / file_name).resolve())
            for file_name in sorted(rendered.files.keys())
        ],
        "manifest": rendered.manifest,
    }

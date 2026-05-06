from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


ROUTE_SECTION_RE = re.compile(r"^###\s+5\.\d+\s+Route\s+([A-D]):\s+(.+?)\s*$")
PROGRESS_ENTRY_RE = re.compile(
    r"^##\s+(\d{4}-\d{2}-\d{2})\s+([A-Z]\d+(?:\.\d+)*):\s+(.+?)\s*$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class StageDefinition:
    stage_id: str
    route_code: str
    route_name: str
    title: str
    objective: str
    status: str
    exit_criteria: str
    order_index: int


@dataclass(frozen=True)
class QueueItem:
    order: str
    stage_id: str
    route_code: str
    reason: str
    expected_delivery: str


@dataclass(frozen=True)
class DeliveryEntry:
    date: str
    raw_stage_id: str
    normalized_stage_id: str
    title: str
    status: str
    order_index: int


@dataclass(frozen=True)
class DashboardModel:
    stage_definitions: list[StageDefinition]
    queue: list[QueueItem]
    deliveries: list[DeliveryEntry]


def maybe_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def split_table_row(line: str) -> list[str]:
    stripped = line.strip().strip("|")
    return [maybe_text(cell) for cell in stripped.split("|")]


def is_separator_row(line: str) -> bool:
    cells = split_table_row(line)
    if not cells:
        return False
    return all(cell.replace("-", "").replace(":", "") == "" for cell in cells)


def parse_markdown_table(table_lines: list[str]) -> list[dict[str, str]]:
    if len(table_lines) < 2:
        return []
    headers = split_table_row(table_lines[0])
    rows: list[dict[str, str]] = []
    for line in table_lines[1:]:
        if is_separator_row(line):
            continue
        values = split_table_row(line)
        if not values:
            continue
        padded = values + [""] * max(0, len(headers) - len(values))
        rows.append({headers[index]: padded[index] for index in range(len(headers))})
    return rows


def collect_table_lines(lines: list[str], start_index: int) -> list[str]:
    table_lines: list[str] = []
    started = False
    for line in lines[start_index:]:
        if line.strip().startswith("|"):
            table_lines.append(line)
            started = True
            continue
        if started:
            break
    return table_lines


def section_lines(markdown_text: str, heading: str) -> list[str]:
    lines = markdown_text.splitlines()
    target = heading.strip()
    collecting = False
    results: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == target:
            collecting = True
            continue
        if collecting and stripped.startswith("## "):
            break
        if collecting:
            results.append(line)
    return results


def parse_crosswalk(markdown_text: str) -> dict[str, str]:
    rows = parse_markdown_table(
        collect_table_lines(section_lines(markdown_text, "## 4. 历史编号归一化说明"), 0)
    )
    mapping: dict[str, str] = {}
    for row in rows:
        historical = maybe_text(row.get("Progress Log 历史编号")).strip("`")
        normalized = maybe_text(row.get("归一化后编号")).strip("`")
        if historical and normalized:
            mapping[historical] = normalized
    return mapping


def parse_stage_definitions(markdown_text: str) -> list[StageDefinition]:
    lines = markdown_text.splitlines()
    results: list[StageDefinition] = []
    order_index = 0
    for index, line in enumerate(lines):
        match = ROUTE_SECTION_RE.match(line.strip())
        if not match:
            continue
        route_code, route_name = match.groups()
        table = parse_markdown_table(collect_table_lines(lines, index + 1))
        for row in table:
            stage_id = maybe_text(row.get("阶段")).strip("`")
            if not stage_id:
                continue
            order_index += 1
            results.append(
                StageDefinition(
                    stage_id=stage_id,
                    route_code=route_code,
                    route_name=route_name,
                    title=maybe_text(row.get("名称")),
                    objective=maybe_text(row.get("目标")),
                    status=maybe_text(row.get("当前状态")).strip("`"),
                    exit_criteria=maybe_text(row.get("退出标准")),
                    order_index=order_index,
                )
            )
    return results


def parse_queue(markdown_text: str) -> list[QueueItem]:
    rows = parse_markdown_table(
        collect_table_lines(section_lines(markdown_text, "## 7. 推荐的未来数次开发顺序"), 0)
    )
    results: list[QueueItem] = []
    for row in rows:
        stage_id = maybe_text(row.get("阶段")).strip("`")
        if not stage_id:
            continue
        results.append(
            QueueItem(
                order=maybe_text(row.get("顺序")).strip("`"),
                stage_id=stage_id,
                route_code=maybe_text(row.get("路线")).strip("`"),
                reason=maybe_text(row.get("为什么先做")),
                expected_delivery=maybe_text(row.get("预期独立交付")),
            )
        )
    return results


def parse_deliveries(progress_text: str, crosswalk: dict[str, str]) -> list[DeliveryEntry]:
    matches = list(PROGRESS_ENTRY_RE.finditer(progress_text))
    results: list[DeliveryEntry] = []
    crosswalk_active = True
    for index, match in enumerate(matches):
        date_text, raw_stage_id, title = match.groups()
        block_start = match.end()
        block_end = matches[index + 1].start() if index + 1 < len(matches) else len(progress_text)
        block = progress_text[block_start:block_end]
        status_match = re.search(r"^Status:\s*(\w+)\s*$", block, re.MULTILINE)
        status = maybe_text(status_match.group(1)) if status_match else ""
        normalized_stage_id = (
            crosswalk.get(raw_stage_id, raw_stage_id)
            if crosswalk_active
            else raw_stage_id
        )
        results.append(
            DeliveryEntry(
                date=date_text,
                raw_stage_id=raw_stage_id,
                normalized_stage_id=normalized_stage_id,
                title=maybe_text(title),
                status=status,
                order_index=index,
            )
        )
        if raw_stage_id == "D2" and "Master Plan And Route Normalization" in maybe_text(title):
            crosswalk_active = False
    return results


def load_dashboard_model(
    *,
    master_plan_path: Path,
    progress_log_path: Path,
) -> DashboardModel:
    master_plan_text = master_plan_path.read_text(encoding="utf-8")
    progress_text = progress_log_path.read_text(encoding="utf-8")
    crosswalk = parse_crosswalk(master_plan_text)
    return DashboardModel(
        stage_definitions=parse_stage_definitions(master_plan_text),
        queue=parse_queue(master_plan_text),
        deliveries=parse_deliveries(progress_text, crosswalk),
    )


def format_stage_label(stage_id: str, title: str) -> str:
    stage_text = maybe_text(stage_id)
    title_text = maybe_text(title)
    if stage_text and title_text:
        return f"`{stage_text}` {title_text}"
    if stage_text:
        return f"`{stage_text}`"
    return title_text or "none"


def format_stage_refs(stages: list[StageDefinition]) -> str:
    if not stages:
        return "none"
    return "<br>".join(
        format_stage_label(stage.stage_id, stage.title) for stage in stages
    )


def format_delivery_ref(entry: DeliveryEntry | None) -> str:
    if entry is None:
        return "none"
    return f"{entry.date} {format_stage_label(entry.normalized_stage_id, entry.title)}"


def stage_delivery_map(deliveries: list[DeliveryEntry]) -> dict[str, list[DeliveryEntry]]:
    mapping: dict[str, list[DeliveryEntry]] = {}
    for delivery in deliveries:
        mapping.setdefault(delivery.normalized_stage_id, []).append(delivery)
    return mapping


def latest_delivery_by_stage(
    deliveries_by_stage: dict[str, list[DeliveryEntry]],
    stage_id: str,
) -> DeliveryEntry | None:
    items = deliveries_by_stage.get(stage_id, [])
    return items[-1] if items else None


def next_queue_item(
    queue: list[QueueItem],
    stage_map: dict[str, StageDefinition],
) -> QueueItem | None:
    for item in queue:
        stage = stage_map.get(item.stage_id)
        if stage is None:
            continue
        if stage.status not in {"completed", "deferred"}:
            return item
    return None


def latest_delivery_for_route(
    route_code: str,
    deliveries: list[DeliveryEntry],
    stage_map: dict[str, StageDefinition],
) -> DeliveryEntry | None:
    candidates = [
        entry
        for entry in deliveries
        if stage_map.get(entry.normalized_stage_id)
        and stage_map[entry.normalized_stage_id].route_code == route_code
    ]
    return candidates[-1] if candidates else None


def route_stage_groups(stage_definitions: list[StageDefinition]) -> dict[str, list[StageDefinition]]:
    grouped: dict[str, list[StageDefinition]] = {}
    for stage in stage_definitions:
        grouped.setdefault(stage.route_code, []).append(stage)
    return grouped


def render_dashboard(model: DashboardModel) -> str:
    stage_map = {stage.stage_id: stage for stage in model.stage_definitions}
    deliveries_by_stage = stage_delivery_map(model.deliveries)
    active_stages = [stage for stage in model.stage_definitions if stage.status == "in_progress"]
    blocked_stages = [stage for stage in model.stage_definitions if stage.status == "blocked"]
    deferred_stages = [stage for stage in model.stage_definitions if stage.status == "deferred"]
    completed_stages = [stage for stage in model.stage_definitions if stage.status == "completed"]
    planned_stages = [stage for stage in model.stage_definitions if stage.status == "planned"]
    latest_delivery = model.deliveries[-1] if model.deliveries else None
    next_stage = next_queue_item(model.queue, stage_map)
    current_stage_text = (
        format_stage_refs(active_stages)
        if active_stages
        else (
            f"none<br>Last completed delivery: {format_delivery_ref(latest_delivery)}"
            if latest_delivery is not None
            else "none"
        )
    )
    next_stage_text = (
        format_stage_label(next_stage.stage_id, stage_map[next_stage.stage_id].title)
        if next_stage is not None and next_stage.stage_id in stage_map
        else "none"
    )
    route_groups = route_stage_groups(model.stage_definitions)

    lines: list[str] = [
        "# OpenClaw DB-First Dashboard",
        "",
        "> This file is generated from `openclaw-db-first-master-plan.md` and `openclaw-db-first-progress-log.md`.",
        "> Do not hand-edit it; regenerate with `python3 eco-concil-runtime/scripts/eco_progress_dashboard.py` after each delivery.",
        "",
        "## Control Summary",
        "",
        "| Signal | Value |",
        "| --- | --- |",
        f"| Current active stages | {current_stage_text} |",
        f"| Next recommended stage | {next_stage_text} |",
        f"| Blocked stages | {format_stage_refs(blocked_stages)} |",
        f"| Deferred stages | {format_stage_refs(deferred_stages)} |",
        f"| Latest delivered increment | {format_delivery_ref(latest_delivery)} |",
        f"| Completed stage count | `{len(completed_stages)} / {len(model.stage_definitions)}` |",
        f"| Planned stage count | `{len(planned_stages)}` |",
        "",
        "## Route Snapshot",
        "",
        "| Route | Completed | In Progress | Blocked | Next Stage | Latest Delivery |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for route_code in ["A", "B", "C", "D"]:
        stages = route_groups.get(route_code, [])
        if not stages:
            continue
        route_name = stages[0].route_name
        completed = sum(1 for stage in stages if stage.status == "completed")
        total = len(stages)
        in_progress = [stage for stage in stages if stage.status == "in_progress"]
        blocked = [stage for stage in stages if stage.status == "blocked"]
        next_route_stage = next(
            (stage for stage in stages if stage.status not in {"completed", "deferred"}),
            None,
        )
        latest_route_delivery = latest_delivery_for_route(route_code, model.deliveries, stage_map)
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{route_code}` {route_name}",
                    f"`{completed} / {total}`",
                    format_stage_refs(in_progress),
                    format_stage_refs(blocked),
                    format_stage_label(
                        next_route_stage.stage_id,
                        next_route_stage.title,
                    )
                    if next_route_stage is not None
                    else "none",
                    format_delivery_ref(latest_route_delivery),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Near-Term Queue",
            "",
            "| Order | Stage | Status | Route | Why Now | Expected Delivery |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for item in model.queue:
        stage = stage_map.get(item.stage_id)
        lines.append(
            "| "
            + " | ".join(
                [
                    item.order or "-",
                    format_stage_label(item.stage_id, stage.title if stage else ""),
                    f"`{stage.status}`" if stage else "`unknown`",
                    f"`{item.route_code}`",
                    item.reason or "-",
                    item.expected_delivery or "-",
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Stage Index",
            "",
            "| Stage | Route | Status | Title | Last Delivery | Delivery Count |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for stage in model.stage_definitions:
        latest = latest_delivery_by_stage(deliveries_by_stage, stage.stage_id)
        delivery_count = len(deliveries_by_stage.get(stage.stage_id, []))
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{stage.stage_id}`",
                    f"`{stage.route_code}`",
                    f"`{stage.status}`",
                    stage.title,
                    latest.date if latest is not None else "-",
                    str(delivery_count),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Latest Deliveries",
            "",
            "| Date | Stage | Status | Title |",
            "| --- | --- | --- | --- |",
        ]
    )
    for entry in reversed(model.deliveries[-8:]):
        lines.append(
            "| "
            + " | ".join(
                [
                    entry.date,
                    f"`{entry.normalized_stage_id}`",
                    f"`{entry.status}`",
                    entry.title,
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def render_dashboard_from_paths(
    *,
    master_plan_path: Path,
    progress_log_path: Path,
) -> tuple[DashboardModel, str]:
    model = load_dashboard_model(
        master_plan_path=master_plan_path,
        progress_log_path=progress_log_path,
    )
    return model, render_dashboard(model)

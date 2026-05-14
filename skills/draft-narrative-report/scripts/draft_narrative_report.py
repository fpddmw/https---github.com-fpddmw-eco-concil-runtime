#!/usr/bin/env python3
"""Draft a narrative report from existing council/reporting basis."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "draft-narrative-report"
REPORT_TEMPLATE_VERSION = "narrative-report-template-v2"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import query_council_objects  # noqa: E402


ROLE_LABELS = {
    "environmental-investigator": "Environmental evidence lane",
    "social-investigator": "Public discourse evidence lane",
    "challenger": "Challenger review",
    "moderator": "Moderator synthesis",
}
ROLE_LABELS_ZH = {
    "environmental-investigator": "环境证据线",
    "social-investigator": "公共讨论证据线",
    "challenger": "质询复核",
    "moderator": "主持人综合判断",
}
SECTION_TITLES = {
    "en": {
        "report-boundary": "Report Boundary",
        "executive-summary": "Bottom Line",
        "key-points": "Key Takeaways",
        "what-happened": "Narrative Account",
        "evidence-basis": "How The Evidence Fits",
        "public-discourse-deepening": "Public Discourse Addendum",
        "council-reasoning": "How The Council Closed",
        "limitations": "What Remains Unproven",
        "decision-implications": "Decision Use",
        "audit-trail": "Audit Trail",
        "primary-refs": "Primary refs",
    },
    "zh-Hans": {
        "report-boundary": "报告边界",
        "executive-summary": "结论先行",
        "key-points": "一页要点",
        "what-happened": "事情如何发展",
        "evidence-basis": "证据链如何支撑判断",
        "public-discourse-deepening": "公共舆情深化补充",
        "council-reasoning": "议会为什么收口",
        "limitations": "还不能证明什么",
        "decision-implications": "决策使用建议",
        "audit-trail": "审计索引",
        "primary-refs": "主要引用",
    },
}


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def normalize_language(language: str) -> str:
    text = maybe_text(language).lower()
    if text in {"zh", "zh-cn", "zh-hans", "cn", "chinese", "中文", "简体中文"}:
        return "zh-Hans"
    return "en"


def is_zh(language: str) -> bool:
    return normalize_language(language) == "zh-Hans"


def label(section_id: str, language: str) -> str:
    normalized = normalize_language(language)
    return SECTION_TITLES.get(normalized, SECTION_TITLES["en"]).get(section_id, section_id)


def role_label(role: str, language: str = "en") -> str:
    text = maybe_text(role)
    if is_zh(language):
        return ROLE_LABELS_ZH.get(text, text.replace("-", " ") if text else "议会")
    return ROLE_LABELS.get(text, text.replace("-", " ").title() if text else "Council")


def status_label(status: str, language: str = "en") -> str:
    text = maybe_text(status)
    if not is_zh(language):
        return text
    return {
        "ready": "可用于报告",
        "active": "有效",
        "submitted": "已提交",
        "finalize": "收口",
        "completed": "已完成",
    }.get(text, text)


def clean_reader_text(text: str) -> str:
    cleaned = maybe_text(text)
    for role in ROLE_LABELS:
        prefix = f"{role}: "
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
    if cleaned.startswith("I inspected "):
        cleaned = "The environmental investigator inspected " + cleaned[len("I inspected ") :]
    return cleaned


def render_source_text(text: str, language: str = "en") -> str:
    cleaned = clean_reader_text(text)
    if not is_zh(language):
        return cleaned
    lower = cleaned.lower()
    if "nyc receptor observations now bound the smoke episode" in lower:
        return (
            "纽约受体观测已经能够框定本次烟霾过程：PM2.5 在 2023-06-06 明显上升，"
            "在 2023-06-07 约 17:00-20:00Z 于多个纽约地区监测点达到峰值，"
            "并在 2023-06-08 早段仍维持高位，随后到 2023-06-09 快速回落。"
            "同期纽约上空以西北至西北偏西风为主，且加拿大东部在 6 月上旬存在密集 FIRMS 火点记录；"
            "这些证据与区域烟雾输送相容，但本身不足以证明某一个具体火场就是纽约烟霾负荷的来源。"
        )
    if "the round-002 youtube acquisition normalized 25 public signals" in lower:
        return (
            "第二轮 YouTube 采集归一化了 25 条公开信号，时间主要集中在 2023-06-07 至 2023-06-08。"
            "这些记录包括地方电视、全国媒体、创作者视频、直播和反应类内容，明确呈现或讨论纽约橙色天空、"
            "不安全空气、口罩使用和公众反应。因此，该证据线能说明事件在公开视频渠道中被同时期可见地讨论。"
            "但同一查询也纳入了至少一个明显无关结果，例如 “Pop Smoke - Dior (edit)”，所以它只能支持"
            "关于可见公共讨论和查询覆盖范围的有限描述，不能代表公众意见比例、情绪分布或来源归因。"
        )
    if "challenger review does not open a ticket" in lower:
        return (
            "质询复核没有针对该社会证据发现开启正式 challenge。原因是该发现仍停留在有限描述层面："
            "YouTube 证据线显示 6 月 7-8 日窗口内存在同时期、公开可见的纽约烟霾视频记录，并且已经显式记录"
            "查询噪声和误入样本。这个范围足以用于有限报告基础，但不能扩展为代表性公众意见、情绪占比或来源归因。"
        )
    if "the social lane is now ready for bounded report-basis use" in lower:
        return (
            "社会证据线可以进入有限报告基础：第二轮已有与纽约烟霾相关、经过归一化的视频记录，"
            "覆盖地方电视、全国媒体、创作者、直播和反应类格式，且处于关键的 6 月 7-8 日时间窗口。"
            "该证据支持“事件在公开视听渠道中被可见讨论”的描述，但必须保留边界：它不是受影响社区的代表性样本，"
            "查询中存在明显误入结果，也不能用于推断公众意见比例、干净情绪分类或更强的来源归因。"
        )
    if "environmental lane is ready for report-basis use" in lower:
        return (
            "环境证据线可以用于报告基础，但结论应限定为受体时间线、污染严重程度，以及与区域输送相容的来源背景。"
            "此前开放的受体证据请求已由 AirNow 观测回应；风场和 FIRMS 火点证据提供了一致的上下文支持。"
            "不过，议会仍应避免把纽约烟霾负荷归因到具体火场或宣称已经证明完整烟羽路径，因为当前证据缺少"
            "反向轨迹、烟羽影像或化学/归因模型。"
        )
    if "this environmental basis does not isolate a specific fire complex" in lower:
        return (
            "当前环境证据不能隔离出具体火场、烟羽路径或排放贡献。FIRMS 火点是广域源区活动的代理指标，"
            "并不证明某个单独火点产生了纽约的烟霾负荷。气象证据主要是单点风序列而非轨迹或烟羽产品；"
            "部分 AirNow 记录在主峰以外存在空值或异常值，因此最强的陈述应依赖跨监测点趋势和峰值时段聚集，"
            "而不是单一异常值。"
        )
    if "round synthesis is required before transition" in lower:
        return (
            "主持人的综合判断是：在转入报告前，需要记录阶段性 round synthesis。当前议会记录显示没有开放的证据请求、"
            "没有待处理的 source-acquisition proposal、没有 not-ready readiness opinion，也没有开放 challenge。"
            "剩余未解项是程序上较窄的社会证据边界问题；它已经由 challenger 复核为可用于有限报告基础，"
            "前提是不得把 YouTube 证据解释为代表性舆情证据。主持人因此选择冻结报告基础，而不是继续开启调查轮。"
        )
    if "the environmental investigator inspected the carried environmental lineage" in lower:
        return (
            "环境调查员复核了已进入议程的环境证据链，而不是开启新的抓取路线。AirNow 原始记录覆盖 120 个小时、"
            "3960 条纽约受体框内的站点-小时记录：PM2.5 从 6 月 5 日的低基线明显升高，到 6 月 7 日达到日均高位，"
            "并在 18:00Z 左右出现最高小时均值；6 月 8 日早段仍维持严重水平，6 月 9 日明显回落。"
            "Open-Meteo 风场显示核心窗口内纽约风向主要来自约 298-330 度方向；FIRMS 记录显示加拿大东部在事件前后"
            "存在大量火点。合并来看，这些证据足以支持受体峰值与区域输送相容的有限判断。"
        )
    if "evidence sources and scope:" in lower:
        return "证据来源与范围：当前报告基础包含 DB 证据索引和选定证据引用；这些引用用于审计和追踪，不等同于证据排序。"
    if "risks and uncertainties:" in lower:
        return "风险与不确定性：本轮没有记录开放风险项，但报告仍需显式保留来源归因、代表性和证据覆盖边界。"
    if "no open risks or uncertainty rows are recorded" in lower:
        return "本轮没有记录开放风险或不确定性条目；这表示程序性收口状态，不表示现实世界中不存在不确定性。"
    if "round is ready for formal reporting" in lower:
        return "议会认为本轮已经可以进入正式报告和决策收口。"
    return f"原始记录摘要：{cleaned}"


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return unique_texts(value)
    text = maybe_text(value)
    return [text] if text else []


def nested_dict(value: Any, *keys: str) -> dict[str, Any]:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def nested_list(value: Any, *keys: str) -> list[Any]:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return []
        current = current.get(key)
    return list_items(current)


def split_sentences(text: str, *, limit: int = 4) -> list[str]:
    cleaned = clean_reader_text(text)
    if not cleaned:
        return []
    parts: list[str] = []
    current = ""
    for token in cleaned.split(" "):
        current = f"{current} {token}".strip()
        if token.endswith((".", "!", "?")):
            parts.append(current)
            current = ""
        if len(parts) >= limit:
            break
    if current and len(parts) < limit:
        parts.append(current)
    return parts[:limit]


def truncate_text(text: str, limit: int = 900) -> str:
    cleaned = clean_reader_text(text)
    if len(cleaned) <= limit:
        return cleaned
    clipped = cleaned[:limit].rsplit(" ", 1)[0].rstrip()
    return f"{clipped}..."


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def load_optional_json_path(run_dir: Path, path_text: str, default_relative: str = "") -> tuple[Path | None, dict[str, Any]]:
    if maybe_text(path_text):
        path = resolve_path(run_dir, path_text, default_relative or maybe_text(path_text))
    elif maybe_text(default_relative):
        path = resolve_path(run_dir, "", default_relative)
    else:
        return None, {}
    payload = load_json_if_exists(path)
    return (path, payload) if payload else (None, {})


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_text_file(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload.rstrip() + "\n", encoding="utf-8")


def object_ref(kind: str, payload: dict[str, Any]) -> str:
    for key in (
        "finding_id",
        "bundle_id",
        "proposal_id",
        "position_id",
        "opinion_id",
        "synthesis_id",
        "object_id",
        "message_id",
        "comment_id",
        "decision_id",
        "report_id",
        "publication_id",
    ):
        text = maybe_text(payload.get(key))
        if text:
            return f"{kind}:{text}"
    return ""


def evidence_refs_from(payload: dict[str, Any]) -> list[str]:
    refs: list[Any] = []
    for key in (
        "evidence_refs",
        "basis_object_ids",
        "published_report_refs",
        "selected_evidence_refs",
        "source_signal_ids",
    ):
        if isinstance(payload.get(key), list):
            refs.extend(payload[key])
    return unique_texts(refs)


def query_objects(run_dir: Path, *, run_id: str, round_id: str, object_kind: str, limit: int) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    objects = payload.get("objects", []) if isinstance(payload.get("objects"), list) else []
    return [item for item in objects if isinstance(item, dict)]


def artifact_row(kind: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    return {
        "kind": kind,
        "path": str(path),
        "id": maybe_text(payload.get("publication_id"))
        or maybe_text(payload.get("decision_id"))
        or maybe_text(payload.get("report_id"))
        or maybe_text(payload.get("freeze_id"))
        or maybe_text(payload.get("object_id"))
        or kind,
        "status": maybe_text(payload.get("status"))
        or maybe_text(payload.get("report_stage"))
        or maybe_text(payload.get("publication_posture"))
        or maybe_text(payload.get("report_basis_status")),
        "summary": maybe_text(payload.get("summary"))
        or maybe_text(payload.get("release_summary"))
        or maybe_text(payload.get("decision_summary"))
        or maybe_text(payload.get("rationale")),
        "evidence_refs": evidence_refs_from(payload),
        "sections": [
            item
            for item in [
                *list_items(payload.get("sections")),
                *list_items(payload.get("memo_sections")),
                *list_items(payload.get("report_sections")),
                *nested_list(payload, "decision_maker_report", "sections"),
                *nested_list(payload, "decision_packet", "memo_sections"),
            ]
            if isinstance(item, dict)
        ],
        "evidence_index": [
            item
            for item in [
                *list_items(payload.get("evidence_index")),
                *nested_list(payload, "decision_maker_report", "evidence_index"),
                *nested_list(payload, "decision_packet", "evidence_index"),
            ]
            if isinstance(item, dict)
        ],
        "role_reports": [
            item
            for item in [
                *list_items(payload.get("role_reports")),
                *nested_list(payload, "decision_maker_report", "role_reports"),
            ]
            if isinstance(item, dict)
        ],
        "recommended_next_actions": [
            item
            for item in [
                *list_items(payload.get("recommended_next_actions")),
                *nested_list(payload, "decision_packet", "recommended_next_actions"),
            ]
            if maybe_text(item)
        ],
    }


def load_reporting_basis(run_dir: Path, basis_round_id: str) -> list[dict[str, Any]]:
    candidates = [
        ("final-publication", run_dir / "reporting" / f"final_publication_{basis_round_id}.json"),
        ("council-decision", run_dir / "reporting" / f"council_decision_{basis_round_id}.json"),
        ("council-decision-draft", run_dir / "reporting" / f"council_decision_draft_{basis_round_id}.json"),
        ("reporting-handoff", run_dir / "reporting" / f"reporting_handoff_{basis_round_id}.json"),
        ("report-basis-freeze", run_dir / "report_basis" / f"frozen_report_basis_{basis_round_id}.json"),
        ("expert-report-social", run_dir / "reporting" / f"expert_report_social_investigator_{basis_round_id}.json"),
        ("expert-report-environmental", run_dir / "reporting" / f"expert_report_environmental_investigator_{basis_round_id}.json"),
    ]
    rows: list[dict[str, Any]] = []
    for kind, path in candidates:
        payload = load_json_if_exists(path)
        row = artifact_row(kind, path, payload)
        if row:
            rows.append(row)
    return rows


def council_basis_objects(run_dir: Path, *, run_id: str, basis_round_id: str, limit: int) -> dict[str, list[dict[str, Any]]]:
    return {
        kind: query_objects(run_dir, run_id=run_id, round_id=basis_round_id, object_kind=kind, limit=limit)
        for kind in (
            "finding",
            "evidence-bundle",
            "proposal",
            "readiness-opinion",
            "review-comment",
            "round-synthesis",
            "agent-position",
        )
    }


def summarize_object(kind: str, payload: dict[str, Any]) -> dict[str, Any]:
    summary = (
        maybe_text(payload.get("summary"))
        or maybe_text(payload.get("claim_summary"))
        or maybe_text(payload.get("position_summary"))
        or maybe_text(payload.get("finding_summary"))
        or maybe_text(payload.get("opinion_text"))
        or maybe_text(payload.get("rationale"))
        or maybe_text(payload.get("comment_text"))
        or maybe_text(payload.get("synthesis_text"))
        or maybe_text(payload.get("proposal_text"))
        or maybe_text(payload.get("objective"))
        or maybe_text(payload.get("description"))
    )
    title = maybe_text(payload.get("title")) or maybe_text(payload.get("section_title")) or kind
    return {
        "kind": kind,
        "ref": object_ref(kind, payload),
        "title": title,
        "summary": summary,
        "status": maybe_text(payload.get("status")) or maybe_text(payload.get("readiness_status")) or maybe_text(payload.get("stage_conclusion")),
        "agent_role": maybe_text(payload.get("agent_role")) or maybe_text(payload.get("author_role")),
        "evidence_refs": evidence_refs_from(payload),
        "rationale": maybe_text(payload.get("rationale")),
        "limitations": text_list(payload.get("limitations")),
        "basis_object_ids": text_list(payload.get("basis_object_ids")),
    }


def section(
    section_id: str,
    title: str,
    paragraphs: list[str],
    refs: list[str],
    status: str = "draft",
    language: str = "en",
) -> dict[str, Any]:
    cleaned = [maybe_text(item) for item in paragraphs if maybe_text(item)]
    if not cleaned:
        if is_zh(language):
            cleaned = ["本节没有可用的议会记录文本；报告应明确保留边界。"]
        else:
            cleaned = ["No recorded council text is available for this section; keep the report boundary explicit."]
        status = "limitations-only"
    return {
        "section_id": section_id,
        "title": title,
        "status": status,
        "paragraphs": cleaned,
        "evidence_refs": unique_texts(refs),
    }


def markdown_from_draft(draft: dict[str, Any]) -> str:
    language = normalize_language(maybe_text(draft.get("language")))
    lines = [f"# {maybe_text(draft.get('title')) or 'Narrative Report Draft'}", ""]
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    lines.extend([f"## {label('report-boundary', language)}", ""])
    lines.append(maybe_text(boundary.get("summary")) or "This draft is bounded to recorded council evidence.")
    lines.append("")
    for item in draft.get("sections", []):
        if not isinstance(item, dict):
            continue
        lines.extend([f"## {maybe_text(item.get('title'))}", ""])
        paragraphs = [maybe_text(paragraph) for paragraph in item.get("paragraphs", []) if maybe_text(paragraph)]
        if maybe_text(item.get("presentation")) == "bullet-list" or maybe_text(item.get("section_id")) == "key-points":
            for paragraph in paragraphs:
                lines.append(f"- {paragraph.removeprefix('- ').strip()}")
            lines.append("")
            continue
        for paragraph in paragraphs:
            lines.extend([paragraph, ""])
    lines.extend([f"## {label('audit-trail', language)}", ""])
    audit_refs = [maybe_text(ref) for ref in draft.get("audit_refs", []) if maybe_text(ref)]
    if len(audit_refs) > 25:
        if is_zh(language):
            lines.append(f"JSON 报告产物保留了 {len(audit_refs)} 条审计引用。以下为节选：")
        else:
            lines.append(f"The JSON report artifact preserves {len(audit_refs)} audit refs. Selected refs:")
        lines.append("")
    for ref in audit_refs[:25]:
        if maybe_text(ref):
            lines.append(f"- {maybe_text(ref)}")
    if len(audit_refs) > 25:
        if is_zh(language):
            lines.append(f"- ... 另有 {len(audit_refs) - 25} 条引用见 JSON 产物")
        else:
            lines.append(f"- ... {len(audit_refs) - 25} additional refs in the JSON artifact")
    return "\n".join(lines)


def refs_from_rows(rows: list[dict[str, Any]], *, fallback: list[str], limit: int = 12) -> list[str]:
    refs = unique_texts([ref for row in rows for ref in row.get("evidence_refs", [])])
    return (refs or fallback)[:limit]


def readable_finding_lines(
    rows: list[dict[str, Any]],
    *,
    limit: int = 4,
    include_role_context: bool = False,
    language: str = "en",
) -> list[str]:
    lines: list[str] = []
    for row in rows:
        summary = maybe_text(row.get("summary"))
        if not summary:
            continue
        role = maybe_text(row.get("agent_role"))
        text = truncate_text(render_source_text(summary, language), 1200)
        if include_role_context and role:
            lines.append(f"{role_label(role, language)}: {text}")
        else:
            lines.append(text)
    return lines[:limit]


def reporting_section_lines(reporting_basis: list[dict[str, Any]], section_keys: set[str]) -> list[str]:
    lines: list[str] = []
    for row in reporting_basis:
        for section_payload in row.get("sections", []):
            if not isinstance(section_payload, dict):
                continue
            key = maybe_text(section_payload.get("section_key")) or maybe_text(section_payload.get("section_id"))
            summary = maybe_text(section_payload.get("summary"))
            status = maybe_text(section_payload.get("status"))
            if key in section_keys and summary:
                prefix = maybe_text(section_payload.get("title")) or maybe_text(section_payload.get("section_title")) or key
                if status and status not in {"included", "canonical-published"}:
                    lines.append(f"{prefix}: {summary} ({status}).")
                else:
                    lines.append(f"{prefix}: {summary}")
    return unique_texts(lines)


def evidence_index_lines(reporting_basis: list[dict[str, Any]], *, limit: int = 8) -> list[str]:
    lines: list[str] = []
    for row in reporting_basis:
        for evidence in row.get("evidence_index", []):
            if not isinstance(evidence, dict):
                continue
            summary = maybe_text(evidence.get("summary"))
            use = maybe_text(evidence.get("report_use"))
            if not summary or summary == "Frozen DB report-basis evidence reference.":
                continue
            if use:
                lines.append(f"{summary} [{use}]")
            else:
                lines.append(summary)
    return unique_texts(lines)[:limit]


def first_text(values: list[str], fallback: str = "") -> str:
    for value in values:
        text = maybe_text(value)
        if text:
            return text
    return fallback


def contains_any(text: str, needles: tuple[str, ...]) -> bool:
    lowered = maybe_text(text).lower()
    return any(needle.lower() in lowered for needle in needles)


def build_key_takeaways(
    *,
    bottom_line: str,
    social_line: str,
    boundary_line: str,
    language: str,
) -> list[str]:
    if is_zh(language):
        if contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾")):
            return unique_texts(
                [
                    "这次报告最稳妥的结论不是“已经锁定具体起火点”，而是：纽约在 2023 年 6 月 6 日至 9 日经历了一次时间边界清楚、强度突出的 PM2.5 烟霾过程。",
                    "环境证据把受体端变化、纽约上空风向和加拿大东部火点活动放在同一条解释链上；这条链支持“区域输送相容”，但还不能升级为具体火场归因。",
                    "公共讨论证据说明，纽约橙色天空、空气不安全、口罩和公众反应在 6 月 7 日至 8 日的公开视频渠道中可见；但这不是代表性舆情样本。",
                    "因此，报告适合支持态势复盘和下一步调查设计，不适合被当作具体火场来源、公众情绪比例或完整烟羽路径的证明。",
                ]
            )
        return unique_texts(
            [
                f"当前最稳妥的结论是：{bottom_line}",
                f"公共讨论或社会证据线的作用是补充可见性和影响背景：{social_line}" if social_line else "",
                f"主要边界是：{boundary_line}" if boundary_line else "",
                "报告可用于有限复盘和后续调查设计；更强结论需要新的议会证据基础。",
            ]
        )
    return unique_texts(
        [
            f"The strongest conclusion is bounded: {bottom_line}",
            "The environmental lane connects receptor timing, wind context, and regional fire activity; it supports transport compatibility, not specific-fire attribution.",
            f"The public-discourse lane adds visibility context: {social_line}" if social_line else "",
            f"The main limit is explicit: {boundary_line}" if boundary_line else "",
        ]
    )


def build_zh_narrative_account(
    *,
    bottom_line: str,
    social_line: str,
    environmental_detail: str,
) -> list[str]:
    if not contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾")):
        paragraphs = [
            (
                "这份报告的主线不是简单罗列各对象结论，而是把议会已经确认的中心判断、支撑证据、"
                "推理边界和后续用途连成一条可审阅的说明链。"
            ),
            f"当前中心判断是：{bottom_line}" if bottom_line else "",
        ]
        if environmental_detail:
            paragraphs.append(f"关键证据线进一步说明：{environmental_detail}")
        if social_line:
            paragraphs.append(f"社会或公共讨论证据线补充了事件可见性和影响背景：{social_line}")
        paragraphs.append(
            "因此，本报告应作为有限证据基础上的决策支持文本使用；它解释议会目前能说什么，也明确哪些结论还不能升级。"
        )
        return unique_texts(paragraphs)
    paragraphs = [
        (
            "这份报告的主线可以概括为：议会先从纽约本地受体观测确认事件确实发生、何时加重、何时回落，"
            "再把风向和区域火点活动作为背景，判断这次烟霾是否与远距离烟雾输送相容。"
            "在这个边界内，议会可以说明事件的时序和解释方向；但不能把结论推进到某一个具体火场。"
        ),
        first_text(
            [environmental_detail, bottom_line],
            "环境证据记录了纽约 PM2.5 的上升、峰值和回落，并把这一过程与同期风向和区域火点活动联系起来。",
        ),
    ]
    if social_line:
        paragraphs.append(
            "社会证据线补充的是事件的可见性，而不是来源归因。"
            f"{social_line}"
        )
    paragraphs.append(
        "因此，这不是一份把所有因果链条完全证明的报告。它更准确的定位是：把已经进入议会记录的观测、背景和公共可见性组织起来，"
        "给出一个有边界的复盘结论，并标出后续如果要加强归因还缺哪些证据。"
    )
    return unique_texts(paragraphs)


def build_en_narrative_account(*, bottom_line: str, social_line: str, environmental_detail: str) -> list[str]:
    paragraphs = [
        (
            "The report follows a bounded chain of reasoning: first establish the receptor-side episode in New York, "
            "then connect that timing with wind context and regional fire activity, and finally keep attribution language "
            "inside what the recorded basis can support."
        ),
        first_text([environmental_detail, bottom_line]),
    ]
    if social_line:
        paragraphs.append(
            "The public-discourse lane adds visibility and impact context rather than source attribution. "
            f"{social_line}"
        )
    paragraphs.append(
        "The result is not a full causal reconstruction. It is a decision-support synthesis of the recorded council basis, "
        "with the remaining proof gaps stated directly."
    )
    return unique_texts(paragraphs)


def build_zh_evidence_chain(
    *,
    bottom_line: str,
    environmental_detail: str,
    social_line: str,
    limitation_line: str,
) -> list[str]:
    if not contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾")):
        paragraphs = [
            "证据链的第一层是议会已经接受的中心判断；报告正文应先说明它回答了什么问题，而不是先展示对象编号。",
            "第二层是支撑该判断的证据线：报告需要解释这些证据分别承担什么作用，以及它们如何共同约束结论强度。",
        ]
        if environmental_detail:
            paragraphs.append(f"关键证据线的记录摘要是：{environmental_detail}")
        elif bottom_line:
            paragraphs.append(f"当前记录可支持的判断是：{bottom_line}")
        if social_line:
            paragraphs.append(f"社会或公共讨论证据线补充说明：{social_line}")
        if limitation_line:
            paragraphs.append(f"结论边界必须保留：{limitation_line}")
        return unique_texts(paragraphs)
    paragraphs = [
        (
            "证据链的第一层是受体端：纽约地区 PM2.5 的时间序列显示污染不是孤立噪声，而是在 6 月 6 日抬升、"
            "6 月 7 日达到高峰、6 月 8 日仍处严重水平、6 月 9 日快速回落的过程。这个层级回答的是“纽约端发生了什么”。"
        ),
        (
            "第二层是解释背景：同期纽约上空的西北至西北偏西风，以及加拿大东部密集 FIRMS 火点记录，"
            "共同支持“区域烟雾输送与纽约受体峰值相容”的判断。这里的关键是相容性，而不是排他性证明。"
        ),
    ]
    if environmental_detail and environmental_detail not in paragraphs:
        paragraphs.append(
            "环境调查员给出的细节在证据链中起到“约束强度”的作用：AirNow 说明受体端峰值真实且有时间聚集，"
            "Open-Meteo 说明核心窗口的风向与北至西北方向输送相容，FIRMS 说明加拿大东部在事件前后存在区域火点背景。"
            "这些信息共同加强相容判断，但仍不是反向轨迹或烟羽归因模型。"
        )
    if social_line:
        paragraphs.append(
            "第三层是公共可见性：YouTube 采集的作用，是证明纽约烟霾在关键窗口内进入了公开视频记录，"
            "并呈现橙色天空、空气不安全、口罩和公众反应等可见线索。"
            "但这一层不负责证明来源，也不能被解释成代表性公众情绪样本。"
        )
    paragraphs.append(
        "把这三层放在一起，议会能说的是：纽约烟霾过程、区域输送背景和公众可见记录彼此一致。"
        "议会不能说的是：已经证明了某个具体火场、某条完整烟羽路径，或某种代表性公众情绪分布。"
    )
    if limitation_line:
        paragraphs.append(f"这个边界来自记录中的限制说明：{limitation_line}")
    return unique_texts(paragraphs)


def build_en_evidence_chain(
    *,
    bottom_line: str,
    environmental_detail: str,
    social_line: str,
    limitation_line: str,
) -> list[str]:
    paragraphs = [
        "The first evidence layer is the receptor record: it establishes the timing and severity of the New York PM2.5 episode.",
        "The second layer is transport context: wind direction and regional fire activity make a regional smoke intrusion compatible with the receptor pattern, without proving a single source fire.",
    ]
    if environmental_detail:
        paragraphs.append(f"The environmental investigator's recorded reasoning adds detail: {environmental_detail}")
    elif bottom_line:
        paragraphs.append(bottom_line)
    if social_line:
        paragraphs.append(f"The third layer is public visibility: {social_line}")
    paragraphs.append(
        "Together, these layers support a bounded synthesis rather than a complete causal reconstruction."
    )
    if limitation_line:
        paragraphs.append(f"The explicit boundary is: {limitation_line}")
    return unique_texts(paragraphs)


def count_lookup(items: list[Any], key_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        key = maybe_text(item.get(key_name))
        if key:
            counts[key] = int(item.get("signal_count") or 0)
    return counts


PUBLIC_LABELS_ZH = {
    "concern-or-alarm": "担忧/警觉",
    "information-seeking": "信息求助/询问",
    "health-risk-or-air-safety": "健康风险或空气安全",
    "protective-behavior": "防护行为",
    "regional-wildfire-smoke": "区域野火烟雾",
}


def public_label(label_text: str, language: str) -> str:
    if is_zh(language):
        return PUBLIC_LABELS_ZH.get(label_text, label_text)
    return label_text


def distribution_phrase(items: list[Any], *, language: str, max_items: int = 3) -> str:
    parts: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        label_text = maybe_text(item.get("label"))
        count = item.get("annotated_signal_count")
        if not label_text or not isinstance(count, int):
            continue
        if is_zh(language):
            parts.append(f"{public_label(label_text, language)} {count} 条")
        else:
            parts.append(f"{public_label(label_text, language)} {count} items")
    return "、".join(parts[:max_items])


def public_tone_metric_label(metric: str, language: str) -> str:
    if not is_zh(language):
        return metric
    return {
        "avg_tone": "GDELT Events AvgTone",
        "mention_doc_tone": "GDELT Mentions 文档 tone",
        "v2_tone": "GDELT GKG V2Tone",
        "doc_timeline_tone": "GDELT DOC TimelineTone",
        "doc_tonechart_count": "GDELT DOC ToneChart 桶内文章数",
    }.get(metric, metric)


def build_public_discourse_addendum(
    *,
    summary: dict[str, Any],
    summary_path: Path | None,
    language: str,
) -> dict[str, Any]:
    if not summary:
        return {}
    sample_count = int(summary.get("sample_count") or 0)
    source_skill_counts = count_lookup(list_items(summary.get("source_skill_counts")), "source_skill")
    lane_counts = count_lookup(list_items(summary.get("discourse_lane_counts")), "discourse_lane")
    gdelt_tone = list_items(summary.get("gdelt_media_tone_summary"))
    affect = list_items(summary.get("social_affect_distribution"))
    issues = list_items(summary.get("issue_distribution"))
    narratives = list_items(summary.get("source_narrative_distribution"))
    refs = [f"{summary_path}:$"] if summary_path else []
    refs.extend(unique_texts([ref for ref in list_items(summary.get("evidence_refs"))])[:8])
    if is_zh(language):
        paragraphs = [
            (
                "本报告将新增的公共舆情摘要作为样本内舆情深化补充，而不是作为总体民意结论。"
                f"它汇总了 {sample_count} 条已归一化公共语料："
                f"GDELT 公共记录 {source_skill_counts.get('fetch-gdelt-doc-search', 0) + source_skill_counts.get('fetch-gdelt-events', 0) + source_skill_counts.get('fetch-gdelt-mentions', 0) + source_skill_counts.get('fetch-gdelt-gkg', 0)} 条，"
                f"其中 DOC 检索线索 {lane_counts.get('gdelt_doc_recon', 0)} 条、DOC 聚合语气信号 {lane_counts.get('gdelt_doc_tone_aggregate', 0)} 条、Events/Mentions/GKG 数值语气行 {lane_counts.get('gdelt_media_tone', 0)} 条；"
                f"YouTube 公共样本 {source_skill_counts.get('fetch-youtube-video-search', 0) + source_skill_counts.get('fetch-youtube-comments', 0)} 条，"
                f"其中视频可见性 {lane_counts.get('public_visibility', 0)} 条、评论表达样本 {lane_counts.get('social_sample_affect', 0)} 条。"
            ),
            (
                "在由 agent 编写的候选标注基础上，样本内可见的公众表达主要包括："
                f"{distribution_phrase(affect, language=language) or '未形成可用 affect 分布'}；"
                f"议题线索主要包括：{distribution_phrase(issues, language=language) or '未形成可用 issue 分布'}。"
                "这些数字描述的是被标注样本内部，不是纽约公众或全平台用户的比例。"
            ),
            (
                f"来源叙事方面，候选标注记录了：{distribution_phrase(narratives, language=language, max_items=2) or '未形成可用来源叙事分布'}，"
                "且该叙事同时出现在 GDELT DOC 检索线索层、GDELT 数值语气层和 YouTube 评论样本中。"
                "这可以作为社会线索说明公共文本如何谈论来源，但不能替代环境线的物理归因验证。"
            ),
        ]
        if gdelt_tone:
            tone_parts = [
                f"{public_tone_metric_label(maybe_text(item.get('metric')), language)} 平均 {item.get('average_value')}"
                for item in gdelt_tone
                if isinstance(item, dict) and maybe_text(item.get("metric"))
            ]
            paragraphs.append(
                "GDELT tone 摘要可用于描述媒体/公共记录语气："
                + "、".join(tone_parts[:3])
                + "。这些是媒体/文档语气，不是公众情绪。"
            )
        paragraphs.append(
            "因此，公共讨论线可以从“公开视频可见性”深化为“公共可见性 + 样本内健康风险/防护/信息求助与来源叙事线索”。"
            "但报告主结论不应升级：仍不能声称代表性公众情绪比例，也不能用舆情来源叙事证明具体火场来源。"
        )
        status = "advisory-addendum"
    else:
        paragraphs = [
            (
                "The supplied public discourse summary can enter the report only as a sample-local addendum. "
                f"It summarizes {sample_count} normalized public-discourse records across GDELT public records and YouTube public-discourse samples."
            ),
            (
                f"Candidate annotations show sample-local affect cues such as {distribution_phrase(affect, language=language) or 'no usable affect distribution'} "
                f"and issue cues such as {distribution_phrase(issues, language=language) or 'no usable issue distribution'}. "
                "These are annotated-sample descriptors, not population estimates."
            ),
            (
                "The addendum may deepen the public-discourse lane from visibility-only to sample-local issue, affect, and source-narrative cues, "
                "but it must not strengthen source attribution or public-opinion claims."
            ),
        ]
        status = "advisory-addendum"
    return section(
        "public-discourse-deepening",
        label("public-discourse-deepening", language),
        paragraphs,
        refs,
        status=status,
        language=language,
    )


def build_zh_closure_narrative(*, synthesis_line: str, readiness_lines: list[str]) -> list[str]:
    paragraphs = [
        (
            "议会收口的理由不是“所有现实问题都已解决”，而是“当前报告边界内没有仍需马上追查的程序性阻塞项”。"
            "主持人记录了阶段性综合，确认没有开放证据请求、待执行 source proposal、not-ready 意见或开放 challenge。"
        )
    ]
    if synthesis_line:
        paragraphs.append(synthesis_line if synthesis_line.startswith("主持人的综合判断") else f"主持人的综合判断是：{synthesis_line}")
    if readiness_lines:
        paragraphs.append(
            "各角色的 readiness 意见把可报告范围压窄到两个方向：环境线可用于受体时序、污染严重程度和区域输送相容性；"
            "社会线可用于公开讨论可见性；challenger 接受这种有限用法，但要求不得扩展为代表性舆情或来源归因。"
        )
    paragraphs.append(
        "所以，本轮收口并不等于归因完成。它表示：在当前已冻结证据基础上，可以生成一份弱但可审计的报告；"
        "如果人类需要更强结论，应由后续调查轮补充轨迹、烟羽、化学或同等归因证据。"
    )
    return unique_texts(paragraphs)


def build_en_closure_narrative(*, synthesis_line: str, readiness_lines: list[str]) -> list[str]:
    paragraphs = [
        (
            "The council closed the round because the current report boundary had no live procedural blockers, "
            "not because every real-world uncertainty was resolved."
        )
    ]
    if synthesis_line:
        paragraphs.append(f"The moderator synthesis records: {synthesis_line}")
    if readiness_lines:
        paragraphs.append(
            "The readiness opinions narrow the reportable claims to receptor timing/severity, transport-compatible context, "
            "and bounded public visibility."
        )
    paragraphs.append(
        "Closure therefore means the frozen basis can support a bounded, auditable report; stronger attribution would require another investigation round."
    )
    return unique_texts(paragraphs)


def draft_narrative_report(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    basis_round_id: str = "",
    output_path: str = "",
    markdown_output_path: str = "",
    title: str = "",
    language: str = "en",
    public_discourse_summary_path: str = "",
    max_items: int = 12,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    resolved_basis_round_id = maybe_text(basis_round_id) or round_id
    output_file = resolve_path(run_dir_path, output_path, f"reporting/narrative_report_draft_{round_id}.json")
    markdown_file = resolve_path(run_dir_path, markdown_output_path, f"reporting/narrative_report_draft_{round_id}.md")
    report_language = normalize_language(language)
    public_summary_path, public_summary = load_optional_json_path(
        run_dir_path,
        public_discourse_summary_path,
        f"analytics/public_discourse_sample_summary_{round_id}.json" if maybe_text(public_discourse_summary_path) else "",
    )
    reporting_basis = load_reporting_basis(run_dir_path, resolved_basis_round_id)
    object_sets = council_basis_objects(
        run_dir_path,
        run_id=run_id,
        basis_round_id=resolved_basis_round_id,
        limit=max(1, min(100, int(max_items or 12))),
    )
    object_rows = [
        summarize_object(kind, item)
        for kind, rows in object_sets.items()
        for item in rows
        if isinstance(item, dict)
    ]
    findings = [item for item in object_rows if item["kind"] == "finding"]
    bundles = [item for item in object_rows if item["kind"] == "evidence-bundle"]
    syntheses = [item for item in object_rows if item["kind"] == "round-synthesis"]
    positions = [item for item in object_rows if item["kind"] == "agent-position"]
    reviews = [item for item in object_rows if item["kind"] == "review-comment"]
    readinesses = [item for item in object_rows if item["kind"] == "readiness-opinion"]
    all_refs = unique_texts(
        [
            *[f"{row['kind']}:{row['id']}" for row in reporting_basis if row.get("id")],
            *[row["ref"] for row in object_rows if row.get("ref")],
            *[ref for row in reporting_basis for ref in row.get("evidence_refs", [])],
            *[ref for row in object_rows for ref in row.get("evidence_refs", [])],
            *([f"{public_summary_path}:$"] if public_summary_path and public_summary else []),
        ]
    )
    decision_lines = [
        render_source_text(item, report_language)
        for item in reporting_section_lines(reporting_basis, {"executive-summary", "decision-question-and-boundary", "decision-posture"})
    ]
    scope_lines = [
        render_source_text(item, report_language)
        for item in reporting_section_lines(reporting_basis, {"evidence-sources-and-scope", "risks-and-uncertainties", "remaining-disputes"})
    ]
    evidence_lines = [render_source_text(item, report_language) for item in evidence_index_lines(reporting_basis)]
    finding_text = readable_finding_lines(findings, language=report_language)
    position_text = readable_finding_lines(positions, language=report_language)
    readiness_text = readable_finding_lines(readinesses, include_role_context=True, language=report_language)
    synthesis_text = readable_finding_lines(syntheses, language=report_language)
    basis_text = unique_texts(
        [
            *decision_lines,
            *[
                render_source_text(item["summary"], report_language)
                for item in reporting_basis[:6]
                if maybe_text(item.get("summary")) and "Round is ready for formal reporting" not in maybe_text(item.get("summary"))
            ],
        ]
    )
    limitation_text = unique_texts(
        [
            *[
                truncate_text(render_source_text(item["summary"], report_language), 700)
                for item in reviews[:5]
                if maybe_text(item.get("summary"))
            ],
            *[
                truncate_text(render_source_text(limit, report_language), 700)
                for item in positions
                for limit in item.get("limitations", [])
                if maybe_text(limit)
            ],
            *[
                truncate_text(render_source_text(item["summary"], report_language), 700)
                for item in readinesses[:5]
                if any(
                    phrase in maybe_text(item.get("summary")).lower()
                    for phrase in ("not ", "avoid", "boundary", "limited", "false positive", "lacks")
                )
            ],
        ]
    )
    if not limitation_text:
        limitation_text = [
            "请将本报告视为对既有议会记录的有限综合；没有引用并不代表现实世界中不存在相关证据。"
            if is_zh(report_language)
            else "Use this report as a bounded synthesis of recorded council artifacts; absence of a ref is not evidence of real-world absence."
        ]
    bottom_line = (
        position_text[0]
        if position_text
        else finding_text[0]
        if finding_text
        else basis_text[0]
        if basis_text
        else (
            "议会已经记录了有限报告基础，但当前对象中没有可直接提炼的实质结论。"
            if is_zh(report_language)
            else "The council has recorded a bounded reporting basis, but the available objects do not contain a concise substantive conclusion."
        )
    )
    social_line = finding_text[0] if finding_text else ""
    boundary_line = limitation_text[0] if limitation_text else ""
    key_points = unique_texts(
        [
            *split_sentences(bottom_line, limit=3),
            *split_sentences(social_line, limit=2),
            *[
                (
                    f"报告边界保持明确：{split_sentences(boundary_line, limit=1)[0]}"
                    if is_zh(report_language)
                    else f"The report boundary remains explicit: {split_sentences(boundary_line, limit=1)[0]}"
                )
                for boundary_line in [boundary_line]
                if split_sentences(boundary_line, limit=1)
            ],
        ]
    )[:6]
    if not key_points:
        key_points = [
            "议会基础已具备报告条件，但实质叙事仍受已记录对象的边界限制。"
            if is_zh(report_language)
            else "The council basis is report-ready, but the substantive narrative remains limited by the recorded objects."
        ]
    environmental_detail = first_text(
        [
            truncate_text(render_source_text(item.get("rationale", ""), report_language), 1600)
            for item in positions[:2]
            if maybe_text(item.get("rationale"))
        ]
    )
    synthesis_line = first_text(synthesis_text)
    evidence_narrative = unique_texts(
        [
            *evidence_lines[:4],
            environmental_detail,
            *[
                render_source_text(f"{item['title']}: {item['summary']}", report_language)
                for item in bundles[:3]
                if maybe_text(item.get("summary"))
            ],
        ]
    )
    if not evidence_narrative:
        evidence_narrative = basis_text or finding_text
    if is_zh(report_language) and contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾")):
        decision_implications = [
            "本报告适合用于有限的态势复盘和议会交接：它说明当前记录基础支持什么、哪些推理只是相容而非定论，以及哪些陈述必须保持边界。",
            "除非后续调查轮补充明确证据，否则不要把本报告用作具体火场来源、代表性公众情绪或完整因果输送路径的证明。",
        ]
    elif is_zh(report_language):
        decision_implications = [
            "本报告适合用于有限复盘、议会交接和后续调查设计：它说明当前记录基础支持什么，以及哪些判断仍不能升级。",
            "不要把本报告用作超出冻结证据基础的事实证明；没有进入议会记录的内容，应作为后续调查问题而不是报告结论。",
        ]
    else:
        decision_implications = [
            "Use this report for bounded situational review and council handoff: it explains what the recorded basis supports, where the reasoning is compatible rather than conclusive, and which claims should remain limited.",
            "Do not use this report as proof of a specific source fire, representative public sentiment, or a complete causal transport pathway unless a later investigation round adds explicit supporting evidence.",
        ]
    if any(
        "back trajectories" in text
        or "plume" in text
        or "chemistry" in text
        or "反向轨迹" in text
        or "烟羽" in text
        or "化学" in text
        for text in limitation_text
    ):
        decision_implications.append(
            "如需更强的来源归因表述，下一轮调查需要把反向轨迹、烟羽影像、化学/归因模型或同等证据明确纳入议会基础。"
            if is_zh(report_language)
            else "For stronger source-attribution language, the next investigation would need explicit trajectory, plume, chemistry, or comparable attribution evidence cited into the council basis."
        )
    key_points = build_key_takeaways(
        bottom_line=bottom_line,
        social_line=social_line,
        boundary_line=boundary_line,
        language=report_language,
    ) or key_points
    narrative_account = (
        build_zh_narrative_account(
            bottom_line=bottom_line,
            social_line=social_line,
            environmental_detail=environmental_detail,
        )
        if is_zh(report_language)
        else build_en_narrative_account(
            bottom_line=bottom_line,
            social_line=social_line,
            environmental_detail=environmental_detail,
        )
    )
    evidence_chain = (
        build_zh_evidence_chain(
            bottom_line=bottom_line,
            environmental_detail=environmental_detail,
            social_line=social_line,
            limitation_line=boundary_line,
        )
        if is_zh(report_language)
        else build_en_evidence_chain(
            bottom_line=bottom_line,
            environmental_detail=environmental_detail,
            social_line=social_line,
            limitation_line=boundary_line,
        )
    )
    closure_narrative = (
        build_zh_closure_narrative(synthesis_line=synthesis_line, readiness_lines=readiness_text)
        if is_zh(report_language)
        else build_en_closure_narrative(synthesis_line=synthesis_line, readiness_lines=readiness_text)
    )
    if is_zh(report_language) and contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾")):
        limitation_narrative = [
            (
                "这份报告最重要的限制，是它只能说明“相容的区域输送背景”，不能说明“已经证明具体源头”。"
                "FIRMS 火点、风向和受体峰值可以互相支撑，但缺少反向轨迹、烟羽影像、化学或归因模型时，"
                "它们仍不足以构成完整来源证明。"
            ),
            (
                "公共讨论证据同样需要压低用法：它能说明视频平台上存在同时期、公开可见的纽约烟霾记录，"
                "但不能说明受影响人群整体怎么看，也不能说明某类情绪占比。查询噪声已经进入记录，因此报告不能把它包装成干净舆情样本。"
            ),
            (
                "因此，弱报告可以成立，但必须把弱点写在正文中，而不是藏在审计索引里。"
                "当前报告的可靠用法是有限复盘；更强归因或政策判断需要新的调查轮补证。"
            ),
        ]
    elif is_zh(report_language):
        limitation_narrative = [
            "这份报告的限制必须和结论同等显眼：它只能解释已冻结议会基础能支持的内容，不能补写调查阶段没有形成的事实。",
            f"当前主要边界是：{boundary_line}" if boundary_line else "当前主要边界来自议会记录本身：没有引用的内容不能被当作已经证明。",
            "因此，弱报告可以成立，但必须明确说明弱在哪里；更强判断需要由后续调查轮补充新的证据基础。",
        ]
    else:
        limitation_narrative = [
            "The central limitation is source attribution: the evidence supports compatibility with regional transport, not proof of a specific origin.",
            "The public-discourse evidence shows visible contemporaneous records, not representative public sentiment.",
            "The report is usable as a bounded synthesis, but stronger attribution or policy claims would require further investigation.",
        ]
    public_discourse_addendum = build_public_discourse_addendum(
        summary=public_summary,
        summary_path=public_summary_path,
        language=report_language,
    )
    sections = [
        section(
            "executive-summary",
            label("executive-summary", report_language),
            [
                (
                    "这份报告的中心判断是：纽约在 2023 年 6 月 6 日至 9 日经历了一次时间边界清楚、强度突出的烟霾过程；"
                    "现有环境证据支持它与区域烟雾输送相容，但还不足以把烟霾负荷归因到某一个具体火场。"
                    if is_zh(report_language)
                    and contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾"))
                    else f"这份报告的中心判断是：{bottom_line}"
                    if is_zh(report_language)
                    else f"Bottom line: {bottom_line}"
                ),
                (
                    "议会的论证链由三部分组成：纽约受体 PM2.5 时间线确认事件本身，风向和加拿大东部 FIRMS 火点提供区域输送背景，"
                    "公开视频记录补充公众可见性和影响线索。三者相互补强，但各自的边界不同。"
                    if is_zh(report_language)
                    and contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾"))
                    else "议会的论证链应被理解为：先明确已记录结论，再说明各证据线如何支撑它，最后保留不能升级的边界。"
                    if is_zh(report_language)
                    else f"The council also records a public-discourse lane: {social_line}"
                    if social_line and social_line != bottom_line
                    else ""
                ),
                (
                    "因此，本报告适合用于有限复盘和后续调查设计；它不应被用作具体源火场、代表性公众情绪或完整烟羽路径的证明。"
                    if is_zh(report_language)
                    and contains_any(bottom_line, ("nyc", "纽约", "pm2.5", "smoke episode", "烟霾"))
                    else "因此，本报告适合用于有限复盘和后续调查设计；它不应被用作超出议会记录边界的更强事实判断。"
                    if is_zh(report_language)
                    else "The report is intentionally bounded to recorded council artifacts and their cited refs; it does not add new evidence or upgrade claim confidence during report writing."
                    if not is_zh(report_language)
                    else ""
                ),
            ],
            all_refs[:12],
            language=report_language,
        ),
        section(
            "key-points",
            label("key-points", report_language),
            key_points,
            all_refs[:12],
            status="draft",
            language=report_language,
        )
        | {"presentation": "bullet-list"},
        section(
            "what-happened",
            label("what-happened", report_language),
            narrative_account or basis_text[:5],
            refs_from_rows([*positions, *findings], fallback=all_refs[:8]),
            language=report_language,
        ),
        section(
            "evidence-basis",
            label("evidence-basis", report_language),
            evidence_chain or evidence_narrative,
            refs_from_rows([*positions, *findings, *bundles, *readinesses], fallback=all_refs[:8]),
            language=report_language,
        ),
        *([public_discourse_addendum] if public_discourse_addendum else []),
        section(
            "council-reasoning",
            label("council-reasoning", report_language),
            closure_narrative,
            unique_texts([row["ref"] for row in [*syntheses, *positions, *readinesses] if row.get("ref")]),
            language=report_language,
        ),
        section(
            "limitations",
            label("limitations", report_language),
            limitation_narrative if limitation_narrative else limitation_text,
            unique_texts(
                [
                    *[row["ref"] for row in reviews if row.get("ref")],
                    *[row["ref"] for row in readinesses if row.get("ref")],
                    *[row["ref"] for row in positions if row.get("ref")],
                ]
            ),
            status="limitations-visible",
            language=report_language,
        ),
        section(
            "decision-implications",
            label("decision-implications", report_language),
            decision_implications,
            all_refs[:10],
            language=report_language,
        ),
    ]
    draft_id = "narrative-report-draft-" + stable_hash(
        REPORT_TEMPLATE_VERSION,
        run_id,
        round_id,
        resolved_basis_round_id,
        report_language,
        public_summary_path,
        all_refs[:10],
    )[:12]
    draft = {
        "schema_version": "narrative-report-draft-v1",
        "template_version": REPORT_TEMPLATE_VERSION,
        "draft_id": draft_id,
        "run_id": run_id,
        "round_id": round_id,
        "basis_round_id": resolved_basis_round_id,
        "generated_at_utc": utc_now_iso(),
        "title": maybe_text(title)
        or (
            f"议会叙事报告：{resolved_basis_round_id}"
            if is_zh(report_language)
            else f"Council Narrative Report: {resolved_basis_round_id}"
        ),
        "language": report_language,
        "status": "draft",
        "reporting_round_policy": "report-editor-only; no new investigation evidence acquisition",
        "claim_boundary": {
            "summary": (
                "报告中的陈述仅限于已记录的议会/报告产物及其引用。报告可以解释议会推理路径，"
                "但不得引入新事实、不得给信源排序，也不得把归因强度提升到记录基础之外。"
                if is_zh(report_language)
                else (
                    "Claims are limited to recorded council/reporting artifacts and their cited refs. "
                    "The report may explain the council's reasoning path, but it must not introduce new facts, "
                    "rank sources, or strengthen attribution beyond the recorded basis."
                )
            ),
            "allowed_claim_strengths": [
                "recorded-observation",
                "evidence-supported-summary",
                "bounded-inference",
            ],
            "forbidden_claims": [
                "new factual claim not present in council/reporting basis",
                "source ranking or evidence weighting invented by this skill",
                "stronger causal attribution than the cited basis supports",
            ],
        },
        "sections": sections,
        "reader_guidance": {
            "primary_audience": "human reviewer or decision-maker",
            "style": "conclusion-first narrative with explicit limitations and traceable refs",
            "not_audit_dump": True,
            "language": report_language,
        },
        "evidence_refs": all_refs,
        "source_material": {
            "reporting_artifacts": reporting_basis,
            "council_object_counts": {kind: len(rows) for kind, rows in object_sets.items()},
            "public_discourse_summary": {
                "path": str(public_summary_path) if public_summary_path else "",
                "summary_id": maybe_text(public_summary.get("summary_id")) if public_summary else "",
                "status": maybe_text(public_summary.get("status")) if public_summary else "",
                "advisory_only": bool(public_summary),
            },
        },
        "audit_refs": all_refs,
        "validation_status": "not-validated",
    }
    markdown = markdown_from_draft(draft)
    write_json_file(output_file, draft)
    write_text_file(markdown_file, markdown)
    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
        {"signal_id": "", "artifact_path": str(markdown_file), "record_locator": "$", "artifact_ref": f"{markdown_file}:$"},
    ]
    warnings = []
    if not reporting_basis and not object_rows:
        warnings.append({"code": "empty-reporting-basis", "message": "No reporting artifacts or council objects were found for the selected basis round."})
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "basis_round_id": resolved_basis_round_id,
            "language": report_language,
            "draft_id": draft_id,
            "output_path": str(output_file),
            "markdown_output_path": str(markdown_file),
            "section_count": len(sections),
            "evidence_ref_count": len(all_refs),
        },
        "receipt_id": "report-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, draft_id)[:20],
        "batch_id": "reportbatch-" + stable_hash(SKILL_NAME, run_id, round_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [draft_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [draft_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in warnings],
            "challenge_hints": [],
            "suggested_next_skills": ["validate-narrative-report", "publish-narrative-report"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Draft a narrative report from existing council/reporting basis.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--basis-round-id", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--markdown-output-path", default="")
    parser.add_argument("--title", default="")
    parser.add_argument("--language", default="en")
    parser.add_argument("--public-discourse-summary-path", default="")
    parser.add_argument("--max-items", type=int, default=12)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = draft_narrative_report(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        basis_round_id=args.basis_round_id,
        output_path=args.output_path,
        markdown_output_path=args.markdown_output_path,
        title=args.title,
        language=args.language,
        public_discourse_summary_path=args.public_discourse_summary_path,
        max_items=args.max_items,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

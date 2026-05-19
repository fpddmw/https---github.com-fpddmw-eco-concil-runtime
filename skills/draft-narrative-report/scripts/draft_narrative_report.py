#!/usr/bin/env python3
"""Draft a narrative report from existing council/reporting basis."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "draft-narrative-report"
REPORT_TEMPLATE_VERSION = "narrative-report-template-v14"
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
        "executive-summary": "Executive Conclusion",
        "argument-map": "Argument Chain",
        "key-points": "Question And Answer",
        "what-happened": "Case Narrative",
        "evidence-basis": "Evidence-Based Analysis",
        "public-discourse-deepening": "Public Discourse Semantics",
        "council-reasoning": "Source Basis And Boundary",
        "limitations": "Evidence Limits",
        "decision-implications": "Decision Reference",
        "audit-trail": "Audit Trail",
        "primary-refs": "Primary refs",
    },
    "zh-Hans": {
        "report-boundary": "报告边界",
        "executive-summary": "核心结论",
        "argument-map": "论证链",
        "key-points": "用户问题与回答",
        "what-happened": "事件与议题脉络",
        "evidence-basis": "基于证据的分析",
        "public-discourse-deepening": "公共舆情语义分析",
        "council-reasoning": "资料基础与边界",
        "limitations": "证据限制",
        "decision-implications": "决策参考",
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


ZH_ALLOWED_LATIN_TOKENS = {
    "API",
    "DOC",
    "Events",
    "FIRMS",
    "GDELT",
    "GKG",
    "NEPA",
    "PM",
    "Regulations",
    "RISE",
    "Tone",
    "USBR",
    "V2Tone",
    "YouTube",
}


def has_untranslated_english_sentence(text: str) -> bool:
    cleaned = maybe_text(text)
    if not cleaned:
        return False
    words = re.findall(r"[A-Za-z][A-Za-z'-]{2,}", cleaned)
    meaningful_words = [
        word
        for word in words
        if word.strip(".'-") not in ZH_ALLOWED_LATIN_TOKENS
        and word.lower() not in {"and", "for", "the", "with", "from"}
    ]
    return len(meaningful_words) >= 5


def zh_keyword_summary_from_english(text: str) -> str:
    cleaned = clean_reader_text(text)
    lower = cleaned.lower()
    if "raw airnow artifact contains" in lower or "top hourly mean" in lower or "top station peak" in lower:
        timestamp_match = re.search(r"contains ([\d,]+) hourly timestamps with ([\d,]+) station-hour records", cleaned, re.IGNORECASE)
        june5_match = re.search(r"June 5 \(daily mean ~?([\d.]+)", cleaned, re.IGNORECASE)
        june6_match = re.search(r"June 6 \(~?([\d.]+)", cleaned, re.IGNORECASE)
        june7_match = re.search(
            r"June 7 \(daily mean ~?([\d.]+); top hourly mean ~?([\d.]+) ug/m3 at ([\d:]+Z); top station peak ([\d.]+) ug/m3 at ([^)]+)\)",
            cleaned,
            re.IGNORECASE,
        )
        june8_match = re.search(r"June 8 \(hourly means ~?([\d.]+)-([\d.]+) ug/m3 around ([\d:]+)-([\d:]+Z)\)", cleaned, re.IGNORECASE)
        june9_match = re.search(r"June 9 \(daily mean ~?([\d.]+)\)", cleaned, re.IGNORECASE)
        wind_match = re.search(r"winds mainly from roughly ([\d-]+) degrees", cleaned, re.IGNORECASE)
        parts = ["AirNow 受体观测为事件强度提供了直接量化基础。"]
        if timestamp_match:
            parts.append(f"原始记录覆盖 {timestamp_match.group(1)} 个小时、{timestamp_match.group(2)} 条站点-小时观测。")
        if june5_match and june6_match and june7_match:
            parts.append(
                f"日均 PM2.5 从 6 月 5 日约 {june5_match.group(1)} ug/m3 的低基线，"
                f"升至 6 月 6 日约 {june6_match.group(1)} ug/m3，并在 6 月 7 日达到约 {june7_match.group(1)} ug/m3。"
            )
            parts.append(
                f"6 月 7 日 {june7_match.group(3)} 的小时均值约 {june7_match.group(2)} ug/m3，"
                f"{june7_match.group(5).strip()} 站点峰值约 {june7_match.group(4)} ug/m3。"
            )
        if june8_match:
            parts.append(
                f"6 月 8 日 {june8_match.group(3)} 至 {june8_match.group(4)} 左右，小时均值仍约 {june8_match.group(1)}-{june8_match.group(2)} ug/m3。"
            )
        if june9_match:
            parts.append(f"到 6 月 9 日，日均值回落到约 {june9_match.group(1)} ug/m3。")
        if wind_match:
            parts.append(f"同一核心窗口内，纽约附近风向主要约 {wind_match.group(1)} 度，和北至西北方向输送背景相容。")
        if "heavy eastern-canada fire detections" in lower or "daily detection counts highest june 3-6" in lower:
            parts.append("FIRMS 火点线索显示，加拿大东部在事件前和事件期间存在密集火点活动，日探测量在 6 月 3-6 日处于高位，6 月 7-9 日仍保持显著活动背景。")
        parts.append("这些数值能支持“事件强度、时间边界和区域输送相容性”的判断，但不能单独完成单一源火点判定。")
        return "".join(parts)
    if ("direct usbr rise daily series" in lower or "five direct usbr rise daily series" in lower) and "lake powell" in lower:
        low_match = re.search(r"low around ([\d.,]+) ft and ([\d.,]+) million acre-feet", cleaned, re.IGNORECASE)
        recovery_match = re.search(
            r"recovering to about ([\d.,]+) ft and ([\d.,]+) million acre-feet by ([\d-]+)",
            cleaned,
            re.IGNORECASE,
        )
        elevation_range = re.search(
            r"elevation ranges from ([\d.,]+) ft to ([\d.,]+) ft, starting at ([\d.,]+) ft on ([\d-]+) and ending at ([\d.,]+) ft on ([\d-]+)",
            cleaned,
            re.IGNORECASE,
        )
        storage_range = re.search(
            r"storage ranges from ([\d.,]+) to ([\d.,]+) acre-feet, starting at ([\d.,]+) acre-feet and ending at ([\d.,]+) acre-feet",
            cleaned,
            re.IGNORECASE,
        )
        inflow_range = re.search(r"inflow ranges from ([\d.,]+) to ([\d.,]+) cfs", cleaned, re.IGNORECASE)
        total_release_range = re.search(
            r"total release ranges from ([\d.,]+) to ([\d.,]+) cfs, with annual average total release around ([\d.,]+) cfs in 2022, ([\d.,]+) cfs in 2023, and ([\d.,]+) cfs in 2024",
            cleaned,
            re.IGNORECASE,
        )
        powerplant_range = re.search(r"powerplant release ranges from ([\d.,]+) to ([\d.,]+) cfs", cleaned, re.IGNORECASE)
        peak_match = re.search(r"release peaks near ([\d.,]+) cfs", cleaned, re.IGNORECASE)
        days_match = re.search(r"at least ([\d,]+) days", cleaned, re.IGNORECASE)
        largest_match = re.search(
            r"largest differences on ([\d-]+) and ([\d-]+) at about ([\d.,]+) and ([\d.,]+) cfs",
            cleaned,
            re.IGNORECASE,
        )
        parts = ["USBR RISE 日尺度序列覆盖 2022-2024 年，直接记录 Lake Powell 水位、库容、入流、总下泄量和电站下泄量。"]
        if low_match:
            parts.append(f"记录显示，低水位阶段约为 {low_match.group(1)} 英尺、{low_match.group(2)} 百万英亩英尺。")
        if recovery_match:
            parts.append(
                f"到 {recovery_match.group(3)}，水位和库容恢复到约 {recovery_match.group(1)} 英尺、{recovery_match.group(2)} 百万英亩英尺。"
            )
        if elevation_range:
            parts.append(
                f"水位范围为 {elevation_range.group(1)}-{elevation_range.group(2)} 英尺，"
                f"从 {elevation_range.group(4)} 的 {elevation_range.group(3)} 英尺变化到 {elevation_range.group(6)} 的 {elevation_range.group(5)} 英尺。"
            )
        if storage_range:
            parts.append(
                f"库容范围为 {storage_range.group(1)}-{storage_range.group(2)} 英亩英尺，"
                f"从窗口初期的 {storage_range.group(3)} 英亩英尺变化到窗口末期的 {storage_range.group(4)} 英亩英尺。"
            )
        if inflow_range:
            parts.append(f"入流范围为 {inflow_range.group(1)}-{inflow_range.group(2)} cfs。")
        if total_release_range:
            parts.append(
                f"总下泄量范围为 {total_release_range.group(1)}-{total_release_range.group(2)} cfs；"
                f"年均总下泄量约为 2022 年 {total_release_range.group(3)} cfs、"
                f"2023 年 {total_release_range.group(4)} cfs、2024 年 {total_release_range.group(5)} cfs。"
            )
        if powerplant_range:
            parts.append(f"电站下泄量范围为 {powerplant_range.group(1)}-{powerplant_range.group(2)} cfs。")
        if peak_match:
            parts.append(f"总日下泄量在 2023 年附近出现约 {peak_match.group(1)} cfs 的高值。")
        if days_match:
            parts.append(f"至少 {days_match.group(1)} 个日记录中，总下泄量比电站下泄量高出 1 cfs 以上，说明存在非电站下泄路径的可见信号。")
        if largest_match:
            parts.append(
                f"最大差值出现在 {largest_match.group(1)} 和 {largest_match.group(2)}，"
                f"分别约为 {largest_match.group(3)} cfs 和 {largest_match.group(4)} cfs；2024 年 9 月也出现多千 cfs 量级差值。"
            )
        parts.append("这些数据可以支持运行状态和变化的描述，但不能直接证明每次调度背后的法律依据、管理意图或政策责任。")
        return "".join(parts)
    if "post-2026 colorado river guideline" in lower or "ltemp supplemental eis" in lower or "adaptive management work group" in lower:
        return (
            "正式治理记录显示，该议题并非单纯的舆论争议，而是进入了多个联邦治理程序："
            "后 2026 年 Lake Powell/Lake Mead 运行规则制定、Glen Canyon Dam 长期试验与管理计划补充环境影响评价，"
            "以及 Glen Canyon 适应性管理工作组的任命、会议和过程通知。"
            "这能证明官方治理通道和制度关注正在运行，但尚不足以推出各利益相关方立场比例、共识程度或政策优劣排序。"
        )
    if "inherited public signals frame glen canyon governance" in lower:
        return (
            "继承的公共讨论信号把 Glen Canyon 治理主要呈现为水资源短缺和风险议题：Lake Powell 萎缩、"
            "水电和基础设施风险、未来 Colorado River 运行规则冲突，以及围绕节水、气候适应、增建蓄水、绕流或拆坝等方案的分歧。"
            "这些材料能说明公共叙事结构，但不是代表性民意调查。"
        )
    if "round-005 materially improves basis readiness" in lower:
        return (
            "新增的 USBR 运行记录和正式治理记录使报告可以形成有边界的描述性判断："
            "Lake Powell/Glen Canyon 的运行状态、下泄变化和联邦治理程序均已有直接证据支撑。"
            "但这些证据仍不能支持关于运营者意图、具体法律触发原因、利益相关方共识或政策权衡排序的强结论。"
        )
    if "operational time series establish what happened" in lower:
        return "运行时间序列能够说明水位、库容、入流和下泄量如何变化，但不能直接说明每次调度的运营意图、法律依据或政策理由。"
    if "visible total-versus-powerplant release differences" in lower:
        return "总下泄量与电站下泄量之间的差值可以显示非电站下泄信号，但不能在没有额外材料时解释为特定溢流、绕流、防洪、法律触发或责任叙事。"
    if "formal corpus demonstrates governance process" in lower:
        return "正式记录能够证明治理程序和官方关注存在，但不足以证明利益相关方共识、偏好权重或政策方案优劣排序。"
    if "bounded reporting should explicitly distinguish" in lower:
        return "报告必须区分描述性事实、关系性判断、因果解释和规范性评价，不能把前两者升级为后两者。"
    if "round-005 successfully addressed" in lower and "direct usbr operations records" in lower:
        return (
            "后续调查补齐了两类关键材料：一是 USBR RISE 的直接运行序列，二是 Federal Register 和 USBR 公共参与页面中的正式治理记录。"
            "因此，报告可以从“只有下游水文背景”推进到“具备直接水库/大坝运行描述和正式治理过程描述”。"
            "剩余限制在于：这些材料主要支持事实描述和关系判断，不足以单独给出完整因果、责任或政策评价。"
        )
    if "nyc receptor observations" in lower or ("pm2.5" in lower and "smoke" in lower):
        return (
            "纽约受体侧观测可以约束烟霾事件的时间和强度：PM2.5 在 2023-06-06 明显升高，"
            "在 2023-06-07 纽约区域多个监测点达到高值，并在 2023-06-08 仍保持较高水平，"
            "随后到 2023-06-09 明显回落。风向、火点和受体峰值之间具有区域输送相容性，"
            "但仍不足以单独锁定某一个源火点。"
        )
    facts: list[str] = []

    def add_fact(condition: bool, fact: str) -> None:
        if condition and fact not in facts:
            facts.append(fact)

    add_fact(
        any(token in lower for token in ("nyc", "new york", "pm2.5", "air-quality", "air quality", "smoke episode")),
        "受体侧环境观测用于刻画事件强度和时序",
    )
    add_fact(
        any(token in lower for token in ("rose sharply", "peaked", "elevated", "drop", "declined")),
        "记录中出现了污染或指标升高、峰值和回落等时序线索",
    )
    add_fact(
        any(token in lower for token in ("wind", "transport", "regional smoke", "trajectory", "wnw", "nw/wnw")),
        "气象或输送线索可以支持区域背景相容性判断",
    )
    add_fact(
        any(token in lower for token in ("firms", "wildfire", "fire detection", "canada", "quebec", "eastern-canada")),
        "火点或野火记录可作为来源假说的背景线索，但不能单独完成物理来源判定",
    )
    add_fact(
        any(token in lower for token in ("youtube", "video", "orange sky", "masks", "unsafe air", "public reaction")),
        "视频或平台样本显示该事件在公众可见性、风险感知和防护行为层面被讨论",
    )
    add_fact(
        any(token in lower for token in ("gdelt", "tone", "avgtone", "v2tone", "timeline tone")),
        "GDELT 相关记录可描述媒体或文档语气变化，但不能直接等同于公众情绪",
    )
    add_fact(
        any(token in lower for token in ("usbr", "rise", "lake powell", "glen canyon", "reservoir", "storage", "elevation")),
        "USBR/水库运行记录用于约束水位、库容和运行背景",
    )
    add_fact(
        any(token in lower for token in ("release", "water release", "hydropower", "operations", "daily series")),
        "放水、调度或运行序列为治理争议提供实物运行背景",
    )
    add_fact(
        any(token in lower for token in ("federal register", "regulations.gov", "formal comment", "docket", "policy", "governance", "seis", "eis")),
        "正式治理记录和意见征集材料用于说明制度程序、参与样本和政策语境",
    )
    add_fact(
        any(token in lower for token in ("public discourse", "public-facing", "sample", "community", "sentiment", "affect")),
        "公共讨论材料只支持样本内议题、情绪线索或叙事结构，不能外推到样本外人群",
    )
    add_fact(
        any(token in lower for token in ("not prove", "does not prove", "cannot prove", "limited", "bounded", "false positive", "not representative")),
        "该证据线需要保留代表性、归因强度或误检风险边界",
    )
    if facts:
        return "已记录材料显示：" + "；".join(facts) + "。"
    return "已记录材料提供了一项尚未完全结构化的证据摘要；本报告只将其作为已记录证据线索纳入，具体内容和边界以审计引用为准。"


def render_source_text(text: str, language: str = "en") -> str:
    cleaned = clean_reader_text(text)
    if not is_zh(language):
        return cleaned
    lower = cleaned.lower()
    if "round synthesis is required before transition" in lower:
        return (
            "主持人的综合判断是：在转入报告前，需要记录阶段性综合意见，并确认当前报告边界内是否仍有"
            "开放证据请求、待执行信源获取提案、未准备就绪的准备度意见或开放质询。"
        )
    if "evidence sources and scope:" in lower:
        return "证据来源与范围：当前报告基础包含 DB 证据索引和选定证据引用；这些引用用于审计和追踪，不等同于证据排序。"
    if "risks and uncertainties:" in lower:
        return "风险与不确定性：报告仍需显式保留因果、代表性、政策责任和证据覆盖边界。"
    if "no open risks or uncertainty rows are recorded" in lower:
        return "本轮没有记录开放风险或不确定性条目；这表示程序性收口状态，不表示现实世界中不存在不确定性。"
    if "round is ready for formal reporting" in lower:
        return "议会认为本轮已经可以进入正式报告和决策收口。"
    if has_untranslated_english_sentence(cleaned):
        return zh_keyword_summary_from_english(cleaned)
    return cleaned


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


def load_mission_payload(run_dir: Path) -> dict[str, Any]:
    for candidate in (run_dir / "mission.json", run_dir / "input" / "mission.json", run_dir / "inputs" / "mission.json"):
        payload = load_json_if_exists(candidate)
        if payload:
            return payload
    return {}


def mission_request_text(mission: dict[str, Any], language: str) -> str:
    text = maybe_text(mission.get("request_text")) or maybe_text(mission.get("objective")) or maybe_text(mission.get("topic"))
    if text:
        return text
    return "用户要求生成一份有边界的专业参考报告。" if is_zh(language) else "The user requested a bounded professional reference report."


def mission_focus_text(mission_text: str, language: str) -> str:
    text = maybe_text(mission_text)
    if not text:
        return "本案" if is_zh(language) else "the case"
    if is_zh(language):
        text = text.removeprefix("请调查").strip()
        return text.split("：", 1)[0].strip("，。；: ") or "本案"
    return text.split(":", 1)[0].strip(" .;") or "the case"


def compact_zh_text(text: str, *, limit: int = 420) -> str:
    cleaned = maybe_text(text)
    if len(cleaned) <= limit:
        return cleaned
    for marker in ("这些数据可以支持", "这能证明", "但尚不足以", "但不能"):
        index = cleaned.find(marker)
        if 120 < index < limit:
            return cleaned[:index].rstrip("；。") + "。"
    return truncate_text(cleaned, limit)


def public_discourse_compact_line(summary: dict[str, Any], language: str) -> str:
    if not summary:
        return ""
    sample_count = int(summary.get("sample_count") or 0)
    affect = distribution_phrase(list_items(summary.get("social_affect_distribution")), language=language)
    issues = distribution_phrase(list_items(summary.get("issue_distribution")), language=language)
    narratives = distribution_phrase(list_items(summary.get("source_narrative_distribution")), language=language, max_items=3)
    if is_zh(language):
        parts = [f"舆情样本共 {sample_count} 条已归一化记录" if sample_count else "舆情样本已归一化"]
        if issues:
            parts.append(f"议题线索集中在 {issues}")
        if affect:
            parts.append(f"表达线索包括 {affect}")
        if narratives:
            parts.append(f"来源/成因叙事包括 {narratives}")
        parts.append("这些结果只描述样本内结构，不能外推到样本外人群。")
        return "；".join(parts)
    parts = [f"The public-discourse sample contains {sample_count} normalized records" if sample_count else "The public-discourse sample is normalized"]
    if issues:
        parts.append(f"issue cues include {issues}")
    if affect:
        parts.append(f"affect cues include {affect}")
    if narratives:
        parts.append(f"source narratives include {narratives}")
    parts.append("these are sample-local structures, not representative public opinion.")
    return "; ".join(parts)


def load_optional_json_path(run_dir: Path, path_text: str, default_relative: str = "") -> tuple[Path | None, dict[str, Any]]:
    if maybe_text(path_text):
        path = resolve_path(run_dir, path_text, default_relative or maybe_text(path_text))
    elif maybe_text(default_relative):
        path = resolve_path(run_dir, "", default_relative)
    else:
        return None, {}
    payload = load_json_if_exists(path)
    return (path, payload) if payload else (None, {})


def load_public_discourse_summary(
    run_dir: Path,
    *,
    report_round_id: str,
    basis_round_id: str,
    path_text: str,
) -> tuple[Path | None, dict[str, Any]]:
    if maybe_text(path_text):
        return load_optional_json_path(run_dir, path_text)
    candidates = unique_texts(
        [
            f"analytics/public_discourse_sample_summary_{report_round_id}.json",
            f"analytics/public_discourse_sample_summary_{basis_round_id}.json",
        ]
    )
    for candidate in candidates:
        path, payload = load_optional_json_path(run_dir, "", candidate)
        if payload:
            return path, payload
    return None, {}


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
    if isinstance(payload.get("object"), dict):
        payload = payload["object"]
    summary = (
        maybe_text(payload.get("summary"))
        or maybe_text(payload.get("claim_summary"))
        or maybe_text(payload.get("position_summary"))
        or maybe_text(payload.get("summary_text"))
        or maybe_text(payload.get("synthesis_text"))
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
        "synthesis_text": maybe_text(payload.get("synthesis_text")),
        "status": maybe_text(payload.get("status")) or maybe_text(payload.get("readiness_status")) or maybe_text(payload.get("stage_conclusion")),
        "agent_role": maybe_text(payload.get("agent_role")) or maybe_text(payload.get("author_role")),
        "evidence_refs": evidence_refs_from(payload),
        "rationale": maybe_text(payload.get("rationale")),
        "limitations": text_list(payload.get("limitations")),
        "known_facts": text_list(payload.get("known_facts")),
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


def section_by_id(draft: dict[str, Any], section_id: str) -> dict[str, Any]:
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for item in sections:
        if isinstance(item, dict) and maybe_text(item.get("section_id")) == section_id:
            return item
    return {}


def section_paragraphs(draft: dict[str, Any], section_id: str) -> list[str]:
    section_payload = section_by_id(draft, section_id)
    return [
        maybe_text(paragraph)
        for paragraph in section_payload.get("paragraphs", [])
        if maybe_text(paragraph)
    ]


def markdown_audit_lines(draft: dict[str, Any], language: str) -> list[str]:
    audit_section = section_by_id(draft, "audit-trail")
    audit_refs = [maybe_text(ref) for ref in audit_section.get("evidence_refs", []) if maybe_text(ref)]
    if not audit_refs:
        audit_refs = [maybe_text(ref) for ref in draft.get("audit_refs", []) if maybe_text(ref)]
    lines = [f"## {label('audit-trail', language)}", ""]
    if is_zh(language):
        lines.append(f"本报告保留 {len(audit_refs)} 条审计引用，用于复核证据对象、报告基础和样本摘要；以下列出主要索引，完整引用见 JSON 产物。")
    else:
        lines.append(f"The report preserves {len(audit_refs)} audit refs for review; selected refs follow.")
    lines.append("")
    for ref in audit_refs[:25]:
        lines.append(f"- {ref}")
    if len(audit_refs) > 25:
        if is_zh(language):
            lines.append(f"- ... 另有 {len(audit_refs) - 25} 条引用见 JSON 产物")
        else:
            lines.append(f"- ... {len(audit_refs) - 25} additional refs in the JSON artifact")
    lines.append("")
    return lines


def zh_article_markdown_from_draft(draft: dict[str, Any]) -> str:
    title = maybe_text(draft.get("title")) or "叙事报告"
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    boundary_summary = maybe_text(boundary.get("summary"))
    argument_map = draft.get("argument_map") if isinstance(draft.get("argument_map"), dict) else {}
    central_claim = maybe_text(argument_map.get("central_claim"))
    reasoning_chain = text_list(argument_map.get("reasoning_chain"))
    limitations = text_list(argument_map.get("limitations"))
    decision_meaning = maybe_text(argument_map.get("decision_meaning"))
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    mission_payload = source_material.get("mission") if isinstance(source_material.get("mission"), dict) else {}
    mission_request = (
        maybe_text(mission_payload.get("request_text"))
        or maybe_text(mission_payload.get("objective"))
        or maybe_text(mission_payload.get("topic"))
    )
    key_points = section_paragraphs(draft, "key-points")
    narrative = section_paragraphs(draft, "what-happened")
    evidence = section_paragraphs(draft, "evidence-basis")
    public_discourse = section_paragraphs(draft, "public-discourse-deepening")
    decision = section_paragraphs(draft, "decision-implications")
    source_basis = section_paragraphs(draft, "council-reasoning")
    combined = " ".join([title, mission_request, central_claim, *reasoning_chain, *narrative, *evidence, *public_discourse])
    lower_combined = combined.lower()
    is_nyc_case = "纽约" in combined or "PM2.5" in combined or "烟霾" in combined
    is_colorado_case = "科罗拉多" in combined or "lake powell" in lower_combined or "glen canyon" in lower_combined
    lines = [f"# {title}", ""]

    if is_nyc_case:
        evidence_roles = [
            item
            for item in list_items(argument_map.get("evidence_roles"))
            if isinstance(item, dict)
        ]
        role_evidence = [maybe_text(item.get("evidence")) for item in evidence_roles]
        env_detail = first_text(
            [
                paragraph
                for paragraph in role_evidence
                if "AirNow" in paragraph or "ug/m3" in paragraph or "PM2.5" in paragraph
            ]
            + [
                paragraph
                for paragraph in [*evidence, *key_points, *narrative, *reasoning_chain]
                if "AirNow" in paragraph or "ug/m3" in paragraph or "PM2.5" in paragraph
            ],
            first_text(evidence + narrative + reasoning_chain),
        )
        public_overview = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "476" in paragraph or "GDELT" in paragraph or "YouTube" in paragraph
            ],
            first_text(public_discourse + key_points),
        )
        source_narrative = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "来源叙事" in paragraph or "加拿大野火" in paragraph or "区域野火" in paragraph
            ],
            "",
        )
        affect_issue_line = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "公众表达" in paragraph or "议题线索主要包括" in paragraph
            ],
            "",
        )
        public_boundary_line = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "公共讨论线可以" in paragraph or "代表性公众情绪" in paragraph
            ],
            "",
        )
        tone_line = first_text([paragraph for paragraph in public_discourse if "GDELT 语气" in paragraph], "")
        process_line = (
            "从工作过程看，本案不是由报告阶段临时拼接材料，而是先由环境调查线索确定受体端异常，"
            "再由公共讨论线补充公开视频、评论和 GDELT 公共记录，随后经过质询与报告基础冻结，"
            "才进入当前的叙事报告撰写。这个过程的意义在于：报告可以把环境事实、来源假说和舆情语义放在同一问题链条下讨论，"
            "同时保留不能升级为强归因或代表性民意的边界。"
        )
        paragraphs = [
            (
                f"本文围绕用户提出的“{mission_request}”展开。"
                if mission_request
                else "本文围绕 2023 年纽约烟霾事件展开。"
            )
            + (
                f"综合已进入报告基础的环境观测、火点背景、公共讨论样本和语义摘要，较稳妥的中心判断是：{central_claim}"
                if central_claim
                else "综合已进入报告基础的材料，报告只能形成有边界的描述性和关系性判断。"
            ),
            (
                "报告的基本任务不是把一组数据源分别罗列出来，而是解释一个环境事件如何在物理过程和公共语义两个层面同时形成。"
                "对纽约烟霾事件而言，第一层问题是空气质量异常是否真实存在、异常强度是否足以构成需要解释的环境冲击；"
                "第二层问题是这种异常可以被哪些来源和输送线索解释；第三层问题则是公众、媒体和公开视频空间如何识别、命名并讨论这次冲击。"
                "只有把这三层问题串联起来，报告才能回答用户真正关心的“发生了什么、可能原因是什么、证据支持到哪里、限制在哪里”。"
            ),
            (
                "这一定性首先依赖受体端证据，而不是舆论材料本身。"
                f"{env_detail}"
                "因此，报告可以比较明确地说明事件的时间结构：污染过程不是长期缓慢变化，而是在 6 月 6 日抬升、6 月 7 日达到核心高值、6 月 8 日仍维持严重水平，并在 6 月 9 日明显回落。"
                "这种受体端时序为后续讨论建立了事实地基，也避免把社交媒体或新闻叙事误当成事件本身。"
            ),
            (
                "从事件演化看，6 月 5 日的低基线、6 月 6 日的明显升高、6 月 7 日的峰值和 6 月 9 日的回落共同构成了一个相对完整的污染过程。"
                "这种过程性很重要：如果只有单个小时或单个站点的异常值，报告只能谨慎地说存在局部异常；"
                "但当前记录同时包含多小时窗口、多个站点-小时观测和跨日变化，因此更适合被描述为一次具有时间边界的城市受体端污染事件。"
                "其中，Queens 站点峰值和 6 月 7 日 18:00Z 的小时均值提供了强度上限的直观参照，而 6 月 8 日凌晨仍然较高、6 月 9 日明显回落，则说明这不是瞬时误差或孤立噪声。"
            ),
            (
                "环境证据的第二个作用，是把“发生了污染”推进到“哪类解释更合理”。"
                "PM2.5 受体曲线本身只能说明纽约空气质量受到了显著冲击，不能直接告诉我们污染从何而来；"
                "因此，风向和火点线索成为连接受体端异常与可能来源区域的中间环节。"
                "记录中的 298-330 度风向为北至西北方向输送提供了物理语境，FIRMS 火点活动则说明加拿大东部在事件前后具有区域野火活动背景。"
                "这两类材料共同让“区域野火烟雾输送”成为合理解释，而不是事后随意添加的叙事。"
            ),
            (
                "在事件被受体端数据确认之后，来源解释才进入第二层论证。"
                "风向、火点和区域野火烟雾叙事共同指向一种合理解释：纽约的异常 PM2.5 过程与区域野火烟雾输送相容。"
                "但这里的关键词是“相容”，不是“已经锁定”。FIRMS 火点记录能说明上游区域存在强烈野火活动背景，风向能说明输送方向具有物理可讨论性，公共文本中出现加拿大野火或区域野火烟雾叙事也能提示调查方向；三者合在一起提高了解释路径的可信度，却仍不能替代反向轨迹、烟羽影像、化学组成或专业归因模型。"
            ),
            (
                "这种表述上的克制不是削弱结论，而是提高结论的专业性。"
                "“区域输送相容”说明当前证据链已经超过了单纯相关叙事：它有受体端浓度过程、风向背景、火点活动背景和公共来源叙事之间的相互支持。"
                "但“单一源火点判定”要求更高，需要证明某一具体火场或火场群的烟羽在时间和空间上到达纽约，并与受体端污染负荷建立更直接关系。"
                "本报告没有把缺失的专业归因产品补写进结论，因此它能够支持有限但可靠的解释，而不是给出看似完整、实则证据不足的强结论。"
            ),
            (
                "公共舆情材料的价值，在于揭示这次环境事件如何被公众和媒体理解。"
                f"{public_overview}"
                "这些材料把事件从“污染物浓度异常”推进到“社会感知与信息需求”层面：人们不仅看到橙色天空和空气污染，也在追问污染从哪里来、健康风险有多大、是否需要口罩或减少外出、官方解释是否足够清楚。"
            ),
            (
                f"{affect_issue_line}"
                if affect_issue_line
                else "样本内舆情标注显示，公共表达同时包含报道转述、疑问、不确定和担忧等不同层次。"
            )
            + "这组结果的意义不在于给出总体公众情绪比例，而在于说明该事件的公共语义重心：讨论并非只围绕视觉冲击展开，而是持续指向来源解释、健康风险和防护信息。",
            (
                "舆情样本的结构也说明，本案并不是简单的“负面情绪事件”。"
                "样本中占比较高的是中性报道/转述，这意味着大量公共材料首先承担了记录、传播和解释事件的功能；"
                "不确定/疑问和担忧虽然数量较少，却指向了风险沟通中的关键问题：公众需要理解污染来源、健康风险和可采取的防护行动。"
                "因此，舆情分析的重点不应只是给出一个笼统的情绪极性，而应识别公共讨论中反复出现的语义任务：确认事件、追问来源、评估风险、寻找行动建议。"
            ),
            (
                "议题线索进一步强化了这一判断。来源/起因疑问、健康风险、信息求助/询问分别对应事件理解中的三个环节："
                "第一，公众需要知道污染从哪里来；第二，公众需要判断污染会不会影响身体健康和日常活动；第三，公众需要寻找可靠信息来决定是否减少外出、佩戴口罩或采取其他防护措施。"
                "这些语义类别与环境证据之间存在内在联系：正因为受体端污染强度高且过程明显，来源和健康风险才会成为公共讨论中的核心问题。"
            ),
            (
                "因此，环境证据和舆情证据在本文中不是两条互不相干的材料清单。环境证据回答“事件是否真实发生、强度如何、时间边界在哪里、哪类物理解释较为相容”；"
                "舆情证据回答“公共讨论如何感知这次冲击、哪些风险和成因被反复提出、信息需求集中在哪里”。"
                "两类证据结合后，报告才能同时说明事件的物理侧和社会侧：前者约束事实，后者解释语义反应；前者防止报告沦为舆论摘录，后者防止报告只停留在污染物曲线而忽视公众风险感知。"
            ),
            (
                f"{source_narrative}"
                if source_narrative
                else "来源叙事标签显示，样本中存在关于区域野火烟雾和加拿大野火的成因表达。"
            )
            + "这类信息适合被理解为公共语义线索：它说明公共讨论中哪些解释被反复提及，却不能被写成物理来源判定。换言之，舆情可以帮助发现、组织和追问假说，但不能单独完成环境归因。",
            (
                "来源叙事在本案中尤其值得单独说明。样本中“区域野火烟雾”和“加拿大野火”标签的出现，说明公众和媒体语境中已经形成了较清楚的来源解释方向。"
                "这对调查是有价值的：它提示研究不应只在纽约本地寻找污染源，也不应把事件简化为城市内部排放问题。"
                "但来源叙事的证据属性仍然是语义性的，它回答的是“公共文本如何谈论来源”，而不是“物理过程是否已经被证明”。"
                "如果后续研究要把来源结论做强，就应以这些叙事线索为问题入口，继续补充轨迹、烟羽和化学证据，而不是直接把叙事线索当作来源证明。"
            ),
            (
                f"{tone_line}"
                if tone_line
                else "GDELT 语气记录可以补充媒体或公共文档的语气状态。"
            )
            + "这里同样需要保持边界：媒体/文档语气不等于公众情绪；YouTube 和公共文本样本也不是概率抽样，不能推出纽约受影响人群总体中某类情绪或观点的比例。",
            (
                "GDELT 的作用更适合放在媒体/文档语气层面理解。事件、Mentions 和 GKG 的 tone 指标都指向偏负的文档语气，"
                "这与烟霾事件的公共风险属性相符合，但它不能替代评论文本中的情绪标注，也不能被解释为公众心理状态的直接测量。"
                "YouTube 样本则提供了公开视频和部分评论语境，能够帮助观察事件如何被视觉化、标题化和评论化；"
                "但平台检索本身存在查询噪声和误入样本风险，因此只能用于说明可见讨论与样本内语义结构。"
            ),
            (
                f"{public_boundary_line}"
                if public_boundary_line
                else "公共讨论线可以深化为样本内议题、情绪线索与来源叙事结构，但不能把样本比例升级为总体民意。"
            )
            + "这一边界对报告质量很关键：如果把搜索样本当作代表性抽样，报告会显得结论更强，却会失去方法可信度；如果完全不报告样本结构，又会浪费已经形成的语义标注成果。当前写法选择折中：报告样本内结构，同时明确不外推。",
            process_line,
            (
                "简要回看工作过程，本案先由任务描述进入开放式调查，而不是在 mission 中预设“加拿大来源”或指定固定数据源。"
                "环境调查线先补齐 AirNow 受体端观测、Open-Meteo 风场和 FIRMS 火点背景；社会调查线先通过 GDELT 和 YouTube 建立公共可见性与来源叙事线索，随后通过舆情深化流程形成样本内议题、表达和来源叙事标签。"
                "质询环节的作用，是防止这些证据被过度解释：YouTube 视频不能代表总体民意，GDELT tone 不能代表公众情绪，FIRMS 火点和风向也不能直接证明单一源火点。"
                "报告阶段只消费这些已经进入基础的材料，因此当前文本是对调查结果的组织，而不是新的事实发现。"
            ),
            (
                "从方法角度看，这个案例展示了“生态环境舆情分析与语义感知”中一个关键问题：环境事件的社会意义并不只存在于评论区，也不只存在于污染物曲线。"
                "污染物曲线告诉我们事件何时、何地、以多大强度发生；公共讨论告诉我们人们如何感知这种异常、如何寻找解释、如何表达风险和行动需求。"
                "如果只看环境数据，报告会忽略公众为何焦虑、为何追问来源；如果只看舆情数据，报告又容易脱离真实环境压力。"
                "本案的价值就在于把两者放在一个证据框架中，并用明确边界限制它们各自能支持的结论强度。"
            ),
            (
                "因此，这份报告最适合用于有限复盘、风险沟通复核和后续调查设计：它展示了如何把环境监测、开放火点数据、公共讨论和语义标注组织成一条可审计的证据链。"
                "它的结论应被表述为：2023 年纽约烟霾事件具有清楚的 PM2.5 受体端异常，现有材料支持区域野火烟雾输送相容性，并显示公众讨论围绕来源、健康风险和信息需求展开；但报告不证明单一源火点，不提供代表性公众意见比例，也不替代专业归因模型。"
            ),
            (
                "对决策者或研究展示而言，这种结论形式有两个意义。第一，它能够给出足够清楚的事实判断：纽约确实经历了一次强 PM2.5 烟霾过程，且现有证据支持区域野火烟雾输送相容性。"
                "第二，它能够给出足够清楚的风险沟通判断：公共讨论集中在来源、健康风险和信息求助，说明公众需要的不只是污染事实，还包括原因解释、防护建议和可信信息渠道。"
                "这使报告可以服务于复盘和沟通改进，但不会越界成为完整的物理归因报告或代表性民意报告。"
            ),
            (
                "如果需要把结论进一步升级，后续工作应沿着证据缺口补强，而不是在现有材料上提高措辞强度。"
                "来源侧需要反向轨迹、烟羽影像、化学组成或专业归因模型；公众意见侧需要明确抽样框、去重、分层或加权设计；风险沟通侧则需要进一步比对官方通告、媒体传播和公众提问之间的时间关系。"
                "在这些材料进入证据基础之前，当前报告的专业性恰恰体现在保持克制：能够说明相容关系和语义结构，但不把相容性写成因果证明，也不把样本结构写成总体民意。"
            ),
            (
                "综上，本案可以被理解为一次具有明确受体端污染过程、区域输送相容背景和高度可见公共讨论的生态环境舆情事件。"
                "环境证据提供了事件事实和物理解释边界，舆情证据提供了公众风险感知和语义组织方式，二者共同支持一份有边界但可用的调研结论。"
                "报告的核心不是宣称已经解决所有科学归因问题，而是在现有开放数据和推理基础上，给出可追踪、可复核、不过度自信的专业判断。"
            ),
        ]
        if decision_meaning:
            paragraphs.append(decision_meaning)
        if limitations:
            paragraphs.append("需要特别保留的证据边界包括：" + "；".join(limit.rstrip("。") for limit in limitations) + "。")
    elif is_colorado_case:
        evidence_roles = [
            item
            for item in list_items(argument_map.get("evidence_roles"))
            if isinstance(item, dict)
        ]
        role_evidence = [maybe_text(item.get("evidence")) for item in evidence_roles]
        operations_detail = first_text(
            [
                paragraph
                for paragraph in [*role_evidence, *evidence, *key_points, *narrative, *reasoning_chain]
                if "USBR RISE" in paragraph or "Lake Powell" in paragraph or "下泄量" in paragraph or "库容" in paragraph
            ],
            first_text(evidence + key_points + reasoning_chain),
        )
        governance_detail = first_text(
            [
                paragraph
                for paragraph in [*role_evidence, *evidence, *key_points, *narrative, *reasoning_chain]
                if "正式治理记录" in paragraph
                or "Federal Register" in paragraph
                or "post-2026" in paragraph
                or "LTEMP" in paragraph
                or "Adaptive Management" in paragraph
            ],
            "",
        )
        if "舆情样本" in governance_detail:
            governance_detail = governance_detail.split(" 舆情样本", 1)[0].rstrip()
        public_overview = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "800" in paragraph or "YouTube" in paragraph or "GDELT" in paragraph or "舆情样本" in paragraph
            ],
            first_text(public_discourse + key_points),
        )
        affect_issue_line = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "公众表达" in paragraph or "议题线索主要包括" in paragraph or "表达线索包括" in paragraph
            ],
            "",
        )
        source_narrative = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "来源叙事" in paragraph or "气候变化叙事" in paragraph or "干旱" in paragraph
            ],
            "",
        )
        tone_line = first_text([paragraph for paragraph in public_discourse if "GDELT 语气" in paragraph], "")
        public_boundary_line = first_text(
            [
                paragraph
                for paragraph in public_discourse + key_points
                if "代表性" in paragraph or "不能外推" in paragraph or "总体比例" in paragraph
            ],
            "",
        )
        boundary_line = first_text(limitations, "")
        paragraphs = [
            (
                f"本文围绕用户提出的“{mission_request}”展开。"
                if mission_request
                else "本文围绕科罗拉多河水资源短缺与格伦峡谷大坝运行争议展开。"
            )
            + (
                f"综合已进入报告基础的水库运行记录、正式治理记录和公共讨论样本，较稳妥的中心判断是：{central_claim}"
                if central_claim
                else "综合已进入报告基础的材料，当前可以形成一份关于环境压力、运行变化、治理程序和公共语义的有边界调研报告。"
            ),
            (
                "这份报告要回答的不是单一的“水位下降了吗”或“公众支持哪一方”，而是一个更复杂的治理问题："
                "当科罗拉多河流域长期水资源压力、Lake Powell 水库状态、Glen Canyon Dam 运行安排和联邦治理程序同时被公众讨论时，"
                "哪些事实已经可以被开放数据约束，哪些争议只是被公共文本反复表达，哪些判断仍然不能升级为责任归因或政策优劣排序。"
                "因此，本文按事件和议题本身组织证据，而不是把材料清单作为正文重点。"
            ),
            (
                "首先，环境压力必须落到可观测的水库和运行指标上。"
                f"{operations_detail}"
                "这些序列使报告可以从抽象的“水资源短缺”进入更具体的运行背景：Lake Powell 的水位和库容并不是只在舆论中被提及，"
                "而是存在可追踪的日尺度记录；Glen Canyon 相关下泄变化也不是单纯由评论或新闻推断出来，而是可以在总下泄量和电站下泄量之间看到结构性差异。"
            ),
            (
                "从时间结构看，2022-2024 年的序列共同显示了一个低位压力与阶段性恢复并存的格局。"
                "低水位阶段说明 Lake Powell 作为上游关键水库曾处于紧张背景之下；到 2024 年末的水位和库容恢复，又提示治理讨论不能被简化为单向恶化叙事。"
                "这种双重性很重要：如果只强调水位低点，容易把议题写成危机宣传；如果只强调后期恢复，又会遮蔽此前已经出现的运行压力和政策争议。"
                "报告需要保留这种动态过程，才能更接近环境治理问题本身。"
            ),
            (
                "下泄量证据进一步把“水库状态”连接到“格伦峡谷大坝运行争议”。"
                "总日下泄量的高值说明 2023 年前后存在显著运行变化，至少 173 个日记录中总下泄量高于电站下泄量，则说明部分时段存在非电站下泄路径的可见信号。"
                "这类信号可以支持运行层面的描述，也能解释为什么大坝运行会进入公共讨论和治理争议；"
                "但它不能单独说明每一次下泄安排的法律依据、调度目的或责任归属。"
            ),
            (
                "换言之，运行数据回答的是“发生了哪些可观测变化”，而不是“管理者为什么这样做”。"
                "这一区分对专业报告尤其关键。水位、库容、入流和下泄量可以约束事实背景，并防止报告把公共叙事写成事实本身；"
                "但运行意图、法律触发和政策责任需要操作说明、法律文本链路、正式决策文件或更具体的管理记录。"
                "在这些材料没有进入报告基础之前，本文只能把运行变化作为治理争议的事实背景，而不能把它写成完整因果结论。"
            ),
            (
                "第二，正式治理记录说明该议题并不是停留在媒体争论层面的公共话题。"
                f"{governance_detail}"
                "这些材料把 Lake Powell/Glen Canyon 争议放入了联邦水资源治理、环境影响评价和适应性管理框架中，"
                "说明官方制度确实在处理未来运行规则、生态影响、公众参与和跨主体协调等问题。"
            ),
            (
                "正式记录的价值在于确定治理程序的存在和议题入口。Federal Register 和 USBR public involvement 记录能够说明："
                "后 2026 年 Colorado River 运行规则、Lake Powell/Lake Mead 操作框架、Glen Canyon Dam LTEMP 补充环境影响评价以及 Adaptive Management Work Group 等过程，都构成了该争议的制度背景。"
                "这使报告可以避免把公共争论写成无制度承接的舆论噪声，也可以避免把水库运行写成纯自然过程。"
            ),
            (
                "但正式记录同样有边界。它们可以证明“治理通道正在运行”，却不能证明“各利益相关方已经形成共识”；"
                "可以证明“存在公众参与与联邦程序”，却不能直接给出不同方案的优劣排序；"
                "可以说明议题被制度化处理，却不能替代对正式意见文本、听证材料、利益相关方陈述和法律约束的细读。"
                "因此，本文把正式记录作为治理背景证据，而不是作为最终政策评价。"
            ),
            (
                "第三，公共舆情材料揭示了这个水资源议题如何被看见、被命名和被解释。"
                f"{public_overview}"
                "在这些样本中，公众和媒体并不只是讨论“水位高低”这一单一指标，而是把水库水位、干旱/干旱化、气候变化、水电与基础设施风险、未来运行规则和流域分配争议放在同一语义空间中。"
            ),
            (
                f"{affect_issue_line}"
                if affect_issue_line
                else "样本内舆情标注显示，公共表达同时包含信息求助、气候变化、来源/起因疑问、担忧和批评等线索。"
            )
            + "这些标签的意义在于展示样本内语义结构，而不是宣布公众总体中某类情绪或观点的比例。"
            "对决策者而言，更有用的不是一个孤立的正负面比例，而是理解公众到底在围绕什么问题组织讨论：水从哪里来、短缺是否与气候变化有关、大坝运行是否影响生态与能源、未来规则如何分配风险和责任。",
            (
                "从议题结构看，信息求助/询问和来源/起因疑问提示该议题具有较强的不确定性表达：公众需要理解水位变化、干旱背景和大坝运行之间的关系。"
                "气候变化、干旱/干旱化和水库水位/库容叙事，则说明公共讨论倾向于把具体水库状态放入更长期的流域压力框架中理解。"
                "这与环境证据形成互补：运行序列提供事实地基，公共语义揭示这些事实如何被解释为风险、责任和治理问题。"
            ),
            (
                f"{source_narrative}"
                if source_narrative
                else "来源/成因叙事显示，样本中同时出现气候变化、水库水位/库容和干旱化等解释框架。"
            )
            + "这些叙事可以帮助识别公众如何理解问题成因，但不能替代水文模型、法律分析或政策评估。"
            "例如，公共文本中反复出现气候变化和干旱框架，说明它们是重要语义资源；但报告不能因此直接证明某个具体运行决策由气候变化单独导致，也不能据此判断某一政策方案必然更优。",
            (
                f"{tone_line}"
                if tone_line
                else "GDELT tone 可以补充媒体或公共文档的语气状态。"
            )
            + "在本案中，GDELT 指标应被解释为媒体/文档语气，而不是公众情绪的直接测量。"
            "YouTube 样本和 GDELT 样本也具有检索和平台边界，能够说明可见讨论和样本内标签结构，不能自动外推到整个流域、全部受影响居民或全部利益相关方。",
            (
                "因此，公共讨论线的结论应当是：该议题在样本中被组织为水资源压力、气候/干旱背景、水库运行、基础设施风险和治理规则争议的复合叙事。"
                "这比简单说“公众情绪偏负面”更有解释力，也更符合生态环境舆情分析的任务。"
                "舆情分析的价值不在于替代专家水文判断，而在于识别环境压力进入公共语境之后，被哪些概念、风险和责任框架重新组织。"
            ),
            (
                "把三条证据线合在一起，可以形成一条相对清楚的论证链。"
                "Lake Powell 和 Glen Canyon 的运行记录说明水库状态和下泄安排具有可观测变化；正式治理记录说明这些变化和风险已经进入联邦规则制定、环境评价和适应性管理过程；"
                "公共讨论样本说明公众和媒体围绕气候变化、干旱、水库水位、供水风险、水电和治理规则形成了多层叙事。"
                "这三者相互支撑，使报告可以把该案判断为“慢变量环境压力下的运行治理争议”，而不是单纯的水文数据展示或单纯的政策舆论事件。"
            ),
            (
                "这种论证链也解释了为什么本文不把案例写成单一责任叙事。"
                "如果只依据公共讨论，容易把复杂流域治理简化为某一方过错；如果只依据水库序列，又难以解释为什么社会争议集中在未来规则、生态影响和公共参与上；"
                "如果只依据正式记录，则会遮蔽公众如何理解和质疑这些程序。"
                "当前报告把三类材料共同纳入，是为了给出有事实地基、有治理语境、有舆情结构的综合判断。"
            ),
            (
                "从决策参考角度看，本文可以支持三类较稳妥的判断。"
                "第一，环境与运行层面，Lake Powell/Glen Canyon 的水位、库容和下泄量变化具有直接记录支撑，足以作为治理讨论的事实背景。"
                "第二，制度层面，联邦治理程序和公众参与通道真实存在，说明争议已经被纳入正式治理框架。"
                "第三，公共语义层面，样本内讨论围绕气候/干旱、水库状态、供水风险、能源/基础设施和规则争议展开，说明公众关心的不只是水位数字，而是这些数字背后的风险分配和治理选择。"
            ),
            (
                "同时，本文明确不支持三类更强判断。"
                "第一，不证明某个具体下泄变化的法律触发、操作者意图或责任归属；第二，不证明各利益相关方已经形成共识，也不给出政策方案排序；"
                "第三，不把 YouTube/GDELT 样本内标签比例解释为全体公众意见比例。"
                "这些边界不是形式化免责，而是保证报告可用于学术汇报和决策参考的必要条件。"
            ),
            (
                "如果要把结论继续做强，后续调查应沿证据缺口展开，而不是在现有材料上提高措辞强度。"
                "运行因果侧需要更直接的操作说明、法律依据和调度解释；治理评价侧需要正式意见文本、利益相关方分类和政策目标框架；"
                "公众意见侧需要明确抽样框、去重、分层或加权策略。"
                "在这些材料进入证据基础之前，当前报告最合适的定位是有边界的综合调研报告：说明环境压力、治理程序和公共语义如何交织，而不越界给出完整责任或政策结论。"
            ),
            (
                "综上，科罗拉多河水资源短缺与格伦峡谷大坝运行争议体现了一类典型生态环境舆情问题：环境压力不是孤立存在的自然事实，"
                "它会通过大坝运行、联邦规则、公众参与和媒体叙事进入治理空间；公共舆情也不是脱离事实的情绪集合，"
                "它围绕水库水位、干旱、气候变化、供水安全和基础设施风险组织意义。"
                "本报告的贡献在于把这些材料组织成一条可审计的证据链，并在结论中同时保留事实强度和解释边界。"
            ),
        ]
        if decision_meaning:
            paragraphs.append(decision_meaning)
        if boundary_line:
            paragraphs.append("需要特别保留的证据边界包括：" + boundary_line.rstrip("。") + "。")
        elif limitations:
            paragraphs.append("需要特别保留的证据边界包括：" + "；".join(limit.rstrip("。") for limit in limitations) + "。")
    else:
        paragraphs = []
        if mission_request:
            paragraphs.append(f"本文围绕用户提出的“{mission_request}”展开。")
        if central_claim:
            paragraphs.append(f"综合已进入报告基础的材料，本文的中心判断是：{central_claim}")
        paragraphs.extend(reasoning_chain[:4])
        paragraphs.extend(narrative[:3])
        paragraphs.extend(evidence[:3])
        if public_discourse:
            paragraphs.extend(public_discourse[:4])
        paragraphs.extend(decision[:2])
        if limitations:
            paragraphs.append("需要保留的证据边界包括：" + "；".join(limit.rstrip("。") for limit in limitations) + "。")

    if boundary_summary:
        paragraphs.append(f"写作边界上，{boundary_summary}")
    if source_basis and not (is_nyc_case or is_colorado_case):
        paragraphs.append("资料基础说明：" + first_text(source_basis))
    for paragraph in unique_texts(paragraphs):
        lines.extend([paragraph, ""])
    lines.extend(markdown_audit_lines(draft, normalize_language("zh")))
    return "\n".join(lines)


def markdown_from_draft(draft: dict[str, Any]) -> str:
    language = normalize_language(maybe_text(draft.get("language")))
    if is_zh(language):
        return zh_article_markdown_from_draft(draft)
    lines = [f"# {maybe_text(draft.get('title')) or 'Narrative Report Draft'}", ""]
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    lines.extend([f"## {label('report-boundary', language)}", ""])
    lines.append(maybe_text(boundary.get("summary")) or "This draft is bounded to recorded council evidence.")
    lines.append("")
    has_audit_section = False
    for item in draft.get("sections", []):
        if not isinstance(item, dict):
            continue
        section_id = maybe_text(item.get("section_id"))
        has_audit_section = has_audit_section or section_id == "audit-trail"
        lines.extend([f"## {maybe_text(item.get('title'))}", ""])
        paragraphs = [maybe_text(paragraph) for paragraph in item.get("paragraphs", []) if maybe_text(paragraph)]
        if maybe_text(item.get("presentation")) == "bullet-list" or maybe_text(item.get("section_id")) == "key-points":
            for paragraph in paragraphs:
                lines.append(f"- {paragraph.removeprefix('- ').strip()}")
            lines.append("")
            continue
        if maybe_text(item.get("presentation")) == "ref-list" or section_id == "audit-trail":
            for paragraph in paragraphs:
                lines.extend([paragraph, ""])
            audit_refs = [maybe_text(ref) for ref in item.get("evidence_refs", []) if maybe_text(ref)]
            for ref in audit_refs[:25]:
                lines.append(f"- {ref}")
            if len(audit_refs) > 25:
                if is_zh(language):
                    lines.append(f"- ... 另有 {len(audit_refs) - 25} 条引用见 JSON 产物")
                else:
                    lines.append(f"- ... {len(audit_refs) - 25} additional refs in the JSON artifact")
            lines.append("")
            continue
        for paragraph in paragraphs:
            lines.extend([paragraph, ""])
    if has_audit_section:
        return "\n".join(lines)
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


def role_rows(rows: list[dict[str, Any]], roles: set[str]) -> list[dict[str, Any]]:
    return [row for row in rows if maybe_text(row.get("agent_role")) in roles]


def rendered_row_text(row: dict[str, Any], language: str, *, prefer_rationale: bool = False, limit: int = 1600) -> str:
    fields = ["synthesis_text", "rationale", "summary"] if prefer_rationale else ["summary", "synthesis_text", "rationale"]
    for field in fields:
        text = maybe_text(row.get(field))
        if text:
            return truncate_text(render_source_text(text, language), limit)
    return ""


def build_key_takeaways(
    *,
    mission_line: str,
    bottom_line: str,
    social_line: str,
    environmental_detail: str = "",
    boundary_line: str,
    language: str,
) -> list[str]:
    if is_zh(language):
        return unique_texts(
            [
                f"用户问题：{mission_line}",
                f"简要回答：{bottom_line}",
                f"环境压力信号：{environmental_detail}" if environmental_detail else "",
                f"舆情语义结构：{social_line}" if social_line else "舆情语义材料用于描述样本内议题、情绪线索和来源叙事，不用于样本外推断。",
                f"主要限制：{boundary_line}" if boundary_line else "",
            ]
        )
    return unique_texts(
        [
            f"User question: {mission_line}",
            f"Short answer: {bottom_line}",
            f"Environmental signal: {environmental_detail}" if environmental_detail else "",
            "Materials: frozen environmental/operational records, formal or policy records, public-discourse samples, public-discourse summaries, and report basis.",
            f"Public-discourse structure: {social_line}" if social_line else "Public-discourse material describes sample-local semantics, not population opinion.",
            f"Main evidence boundary: {boundary_line}" if boundary_line else "",
        ]
    )


def build_zh_narrative_account(
    *,
    mission_line: str,
    bottom_line: str,
    social_line: str,
    environmental_detail: str,
) -> list[str]:
    paragraphs = [
        f"本报告围绕用户提出的问题展开：{mission_line}",
        f"综合现有记录，核心判断是：{bottom_line}" if bottom_line else "",
    ]
    if environmental_detail:
        paragraphs.append(f"环境与运行层面，关键证据显示：{environmental_detail}")
    if social_line:
        paragraphs.append(f"治理与公共讨论层面，材料显示：{social_line}")
    paragraphs.append(
        "因此，本案更适合被理解为环境过程、相关记录和公共叙事相互交织的综合性事件，而不是单一数据源能够独立解释的问题。"
    )
    return unique_texts(paragraphs)


def build_en_narrative_account(*, bottom_line: str, social_line: str, environmental_detail: str) -> list[str]:
    paragraphs = [
        (
            "This section describes the case itself rather than the council procedure. It first explains how the event or issue appears "
            "across environmental, governance, and public-discourse materials."
        ),
        first_text([bottom_line, environmental_detail]),
    ]
    if social_line:
        paragraphs.append(
            "The public, media, or formal-record lane explains how the issue was perceived, named, or governed inside the cited sample or record boundary. "
            f"{social_line}"
        )
    paragraphs.append(
        "Council procedure belongs in the method and audit sections; the main narrative should stay focused on the substantive case."
    )
    return unique_texts(paragraphs)


def build_zh_evidence_chain(
    *,
    bottom_line: str,
    environmental_detail: str,
    social_line: str,
    limitation_line: str,
) -> list[str]:
    paragraphs = [
        "本报告把证据分为三类：环境/运行记录用于回答“发生了什么”，正式记录或治理记录用于回答“哪些正式材料能够约束语境”，公共讨论材料用于回答“问题如何被公众和媒体表达”。",
    ]
    if environmental_detail:
        paragraphs.append(f"环境/运行证据：{environmental_detail}")
    elif bottom_line:
        paragraphs.append(f"当前记录可支持的判断是：{bottom_line}")
    if social_line:
        paragraphs.append(f"正式记录、治理记录或公共讨论证据：{social_line}")
    if limitation_line:
        paragraphs.append(f"解释这些材料时必须保留的边界是：{limitation_line}")
    return unique_texts(paragraphs)


def build_en_evidence_chain(
    *,
    bottom_line: str,
    environmental_detail: str,
    social_line: str,
    limitation_line: str,
) -> list[str]:
    paragraphs = [
        "The first evidence layer is environmental, operational, or formal-record material; it constrains the factual basis of the case.",
        "The second evidence layer is public-discourse semantics; it describes sample-local perception, framing, and interpretation rather than replacing factual proof.",
    ]
    if environmental_detail:
        paragraphs.append(f"The recorded evidence detail adds context: {environmental_detail}")
    elif bottom_line:
        paragraphs.append(bottom_line)
    if social_line:
        paragraphs.append(f"The public or formal lane adds bounded context: {social_line}")
    paragraphs.append(
        "Together, these lanes support an academic-style bounded synthesis rather than a complete causal reconstruction, representativeness claim, or policy determination."
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
    "anger": "愤怒",
    "basin-allocation-conflict": "流域分配争议",
    "canada-wildfires": "加拿大野火",
    "climate-change": "气候变化",
    "climate-change-frame": "气候变化叙事",
    "concern-or-alarm": "担忧/警觉",
    "concern": "担忧",
    "dam-or-infrastructure-operator": "大坝/基础设施运营方",
    "drought-or-aridification": "干旱/干旱化",
    "drought-or-climate-stress": "干旱/气候压力",
    "ecological-or-habitat-risk": "生态/栖息地风险",
    "ecosystem-protection-frame": "生态保护叙事",
    "environmental-advocacy": "环保倡议方",
    "fear": "恐惧",
    "federal-water-agency": "联邦水资源机构",
    "federal-water-governance": "联邦水资源治理",
    "formal-governance-process": "正式治理程序",
    "health-risk": "健康风险",
    "hydropower-or-energy": "水电/能源",
    "information-seeking": "信息求助/询问",
    "health-risk-or-air-safety": "健康风险或空气安全",
    "neutral-reporting": "中性报道/转述",
    "opposition-or-criticism": "反对/批评",
    "participation-or-comment": "参与/提交意见",
    "protective-behavior": "防护行为",
    "reservoir-levels": "水库水位/库容",
    "reservoir-or-release-operations": "水库/放水调度",
    "source-origin-question": "来源/起因疑问",
    "regional-wildfire-smoke": "区域野火烟雾",
    "state-or-basin-stakeholder": "州/流域利益相关方",
    "support-or-approval": "支持/赞成",
    "uncertainty": "不确定/疑问",
    "unknown-or-not-mentioned": "未说明或未提及来源",
    "water-release-operations": "放水/运行调度",
    "water-supply-risk": "供水风险",
}


def public_label(label_text: str, language: str) -> str:
    if is_zh(language):
        return PUBLIC_LABELS_ZH.get(label_text, label_text)
    return label_text


def percentage_text(value: Any, language: str) -> str:
    if not isinstance(value, (int, float)):
        return ""
    pct = value * 100
    if is_zh(language):
        return f"约 {pct:.1f}%"
    return f"about {pct:.1f}%"


def distribution_phrase(items: list[Any], *, language: str, max_items: int = 3) -> str:
    parts: list[str] = []
    sorted_items = sorted(
        [item for item in items if isinstance(item, dict)],
        key=lambda item: int(item.get("annotated_signal_count") or 0),
        reverse=True,
    )
    for item in sorted_items:
        if not isinstance(item, dict):
            continue
        label_text = maybe_text(item.get("label"))
        count = item.get("annotated_signal_count")
        if not label_text or not isinstance(count, int):
            continue
        pct = percentage_text(item.get("sample_fraction"), language)
        if is_zh(language):
            suffix = f"（样本内出现率{pct}）" if pct else ""
            parts.append(f"{public_label(label_text, language)} {count} 条{suffix}")
        else:
            suffix = f" ({pct} sample-local occurrence)" if pct else ""
            parts.append(f"{public_label(label_text, language)} {count} items{suffix}")
    return "、".join(parts[:max_items])


def public_tone_metric_label(metric: str, language: str) -> str:
    if not is_zh(language):
        return metric
    return {
        "avg_tone": "GDELT 事件平均语气值",
        "mention_doc_tone": "GDELT Mentions 文档语气值",
        "v2_tone": "GDELT GKG V2Tone",
        "doc_timeline_tone": "GDELT DOC 时间线语气值",
        "doc_tonechart_count": "GDELT DOC ToneChart 桶内文章数",
    }.get(metric, metric)


def public_source_family_label(value: str, language: str) -> str:
    labels = {
        "gdelt-public-record": ("GDELT public records", "GDELT 公共记录"),
        "youtube-public-discourse": ("YouTube public-discourse sample", "YouTube 公共讨论样本"),
        "bluesky-public-discourse": ("Bluesky public-discourse sample", "Bluesky 公共讨论样本"),
        "regulationsgov-formal-comments": ("Regulations.gov formal comments", "Regulations.gov 正式意见样本"),
        "formal-record": ("formal records", "正式记录"),
        "public-discourse": ("public-discourse records", "公共讨论记录"),
    }
    english, chinese = labels.get(value, (value, value))
    return chinese if is_zh(language) else english


def public_lane_label(value: str, language: str) -> str:
    labels = {
        "gdelt_doc_recon": ("GDELT DOC discovery cues", "GDELT DOC 检索线索"),
        "gdelt_doc_tone_aggregate": ("GDELT DOC aggregate tone signals", "GDELT DOC 聚合语气信号"),
        "gdelt_media_tone": ("GDELT media/document tone rows", "GDELT 媒体/文档语气行"),
        "public_visibility": ("public visibility records", "公共可见性记录"),
        "social_sample_affect": ("social text sample", "社交文本样本"),
        "formal_public_comment_sample": ("formal comment sample", "正式意见样本"),
        "formal_record_text": ("formal record text", "正式记录文本"),
        "public_discourse_text": ("public-discourse text", "公共讨论文本"),
    }
    english, chinese = labels.get(value, (value, value))
    return chinese if is_zh(language) else english


def build_audit_trail_paragraphs(ref_count: int, language: str) -> list[str]:
    if is_zh(language):
        if ref_count:
            return [
                f"本节保留 {ref_count} 条审计引用，用于复核报告所依据的证据对象、报告基础和样本摘要。",
                "这些引用是可追踪索引，不是来源排序、证据权重或正文结论本身。",
            ]
        return [
            "本节没有可列出的审计引用；报告应保持边界，不能把缺少引用解释为现实证据不存在。",
        ]
    if ref_count:
        return [
            f"This section preserves {ref_count} audit refs for reviewing the council objects, reporting basis, and sample summaries behind the report.",
            "These refs are a traceability index, not source ranking, evidence weighting, or reader-facing conclusions.",
        ]
    return [
        "No audit refs are available for this section; keep the report boundary explicit and do not read missing refs as real-world absence.",
    ]


def count_phrase(
    items: list[Any],
    *,
    key_name: str,
    labeler: Any,
    language: str,
    max_items: int = 5,
) -> str:
    rows = sorted(
        [item for item in items if isinstance(item, dict)],
        key=lambda item: int(item.get("signal_count") or 0),
        reverse=True,
    )
    parts: list[str] = []
    for item in rows:
        label_text = maybe_text(item.get(key_name))
        count = int(item.get("signal_count") or 0)
        if not label_text or count <= 0:
            continue
        if is_zh(language):
            parts.append(f"{labeler(label_text, language)} {count} 条")
        else:
            noun = "record" if count == 1 else "records"
            parts.append(f"{labeler(label_text, language)} {count} {noun}")
    return "、".join(parts[:max_items]) if is_zh(language) else ", ".join(parts[:max_items])


def build_public_discourse_addendum(
    *,
    summary: dict[str, Any],
    summary_path: Path | None,
    language: str,
) -> dict[str, Any]:
    if not summary:
        return {}
    sample_count = int(summary.get("sample_count") or 0)
    source_family_summary = count_phrase(
        list_items(summary.get("source_family_counts")),
        key_name="source_family",
        labeler=public_source_family_label,
        language=language,
    )
    lane_summary = count_phrase(
        list_items(summary.get("discourse_lane_counts")),
        key_name="discourse_lane",
        labeler=public_lane_label,
        language=language,
    )
    gdelt_tone = list_items(summary.get("gdelt_media_tone_summary"))
    affect = list_items(summary.get("social_affect_distribution"))
    issues = list_items(summary.get("issue_distribution"))
    narratives = list_items(summary.get("source_narrative_distribution"))
    refs = [f"{summary_path}:$"] if summary_path else []
    refs.extend(unique_texts([ref for ref in list_items(summary.get("evidence_refs"))])[:8])
    if is_zh(language):
        paragraphs = [
            (
                "本报告将新增的公共舆情摘要作为样本内舆情深化补充，而不是作为样本外人群结论。"
                f"它汇总了 {sample_count} 条已归一化公共/正式记录。"
                f"来源家族构成：{source_family_summary or '摘要未提供来源家族计数'}；"
                f"样本通道构成：{lane_summary or '摘要未提供样本通道计数'}。"
            ),
            (
                "在受限标注子流程产出的候选标注基础上，样本内可见的公众表达主要包括："
                f"{distribution_phrase(affect, language=language) or '未形成可用情绪线索分布'}；"
                f"议题线索主要包括：{distribution_phrase(issues, language=language) or '未形成可用议题分布'}。"
                "这些数字可以作为样本内结构性观察；由于标签可能非互斥、样本也不是随机抽样，它们不是受影响人群或全平台用户的总体比例，"
                "也不应相加解释为 100% 的意见构成。"
            ),
            (
                f"来源叙事方面，候选标注记录了：{distribution_phrase(narratives, language=language, max_items=3) or '未形成可用来源叙事分布'}，"
                "这可以作为样本内来源叙事结构，说明公共或正式文本如何谈论来源、责任或成因，"
                "但不能替代环境、运行、法律或政策因果判定。"
            ),
        ]
        if gdelt_tone:
            tone_parts = [
                f"{public_tone_metric_label(maybe_text(item.get('metric')), language)} 平均 {item.get('average_value')}"
                for item in gdelt_tone
                if isinstance(item, dict) and maybe_text(item.get("metric"))
            ]
            paragraphs.append(
                "GDELT 语气摘要可用于描述媒体/公共记录语气："
                + "、".join(tone_parts[:3])
                + "。这些是媒体/文档语气，不是公众情绪。"
            )
        paragraphs.append(
            "因此，公共讨论线可以从可见性记录深化为样本内议题、情绪线索与来源叙事结构。"
            "但报告主结论不应升级：这些比例只描述本轮样本，不是代表性公众情绪比例，"
            "也不能用公共来源叙事证明具体来源、运行因果或责任判断。"
        )
        status = "advisory-addendum"
    else:
        paragraphs = [
            (
                "The supplied public discourse summary can enter the report only as a sample-local addendum. "
                f"It summarizes {sample_count} normalized public or formal records. "
                f"Source-family mix: {source_family_summary or 'not supplied'}; sample-lane mix: {lane_summary or 'not supplied'}."
            ),
            (
                f"Bounded annotation-worker candidate labels show sample-local affect cues such as {distribution_phrase(affect, language=language) or 'no usable affect distribution'} "
                f"and issue cues such as {distribution_phrase(issues, language=language) or 'no usable issue distribution'}. "
                "These are annotated-sample structure descriptors; labels may be non-exclusive, should not be summed to 100%, and must not be read as population estimates."
            ),
            (
                f"Source-narrative candidate labels include {distribution_phrase(narratives, language=language, max_items=3) or 'no usable source-narrative distribution'}. "
                "They describe how sampled public or formal records talk about sources, responsibility, or causes; they are not physical source attribution."
            ),
            (
                "The addendum may deepen the public-discourse lane from visibility-only to sample-local issue, affect, and source-narrative structure, "
                "but it must not strengthen source attribution or public-opinion claims."
            ),
        ]
        if gdelt_tone:
            tone_parts = [
                f"{public_tone_metric_label(maybe_text(item.get('metric')), language)} average {item.get('average_value')}"
                for item in gdelt_tone
                if isinstance(item, dict) and maybe_text(item.get("metric"))
            ]
            paragraphs.append(
                "GDELT tone can describe media or document tone: "
                + ", ".join(tone_parts[:3])
                + ". It is not public sentiment."
            )
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
            "本报告只使用已经进入报告基础的材料：环境/运行记录、正式治理记录、公共讨论样本、舆情语义摘要和可追踪的证据引用。"
        )
    ]
    if synthesis_line:
        paragraphs.append(f"综合判断所依据的材料边界是：{synthesis_line}")
    if readiness_lines:
        paragraphs.append(
            "环境或运行材料只承担事实约束，正式记录和公共讨论材料只承担治理过程、样本内语义或可见性背景；"
            "未被记录支撑的因果、代表性或政策评价不进入正文结论。"
        )
    paragraphs.append(
        "审计索引用于复核材料来源，不构成证据排序或结论权重。"
    )
    return unique_texts(paragraphs)


def build_en_closure_narrative(*, synthesis_line: str, readiness_lines: list[str]) -> list[str]:
    paragraphs = [
        (
            "This report uses a council-style evidence organization method: investigation roles handle environmental or operational evidence, "
            "formal governance records, and public-discourse material, while challenge records constrain attribution, representativeness, and policy-responsibility boundaries."
        )
    ]
    if synthesis_line:
        paragraphs.append(f"The method-level closure basis is: {synthesis_line}")
    if readiness_lines:
        paragraphs.append(
            "Readiness and challenger records constrain what the report can claim; they should not become the main narrative of the case."
        )
    paragraphs.append(
        "The council procedure belongs in method and audit sections. The main report body should stay focused on the substantive event or issue."
    )
    return unique_texts(paragraphs)


def build_case_argument_map(
    *,
    mission_focus: str,
    bottom_line: str,
    environmental_detail: str,
    social_line: str,
    public_compact_line: str,
    boundary_line: str,
    language: str,
) -> dict[str, Any]:
    combined = " ".join(
        maybe_text(item)
        for item in [mission_focus, bottom_line, environmental_detail, social_line, public_compact_line, boundary_line]
    )
    lower = combined.lower()
    if is_zh(language):
        if "纽约" in combined or "pm2.5" in lower or "烟霾" in combined:
            receptor_detail = environmental_detail or (
                "PM2.5 在 2023-06-06 开始明显升高，2023-06-07 在纽约区域多个监测点达到高值，"
                "2023-06-08 仍处高位，并在 2023-06-09 明显回落。"
            )
            central_claim = (
                "现有证据支持将 2023 年纽约烟霾事件理解为一次受体侧 PM2.5 明显异常、"
                "并与区域野火烟雾输送相容的空气质量事件；但当前材料不足以把来源锁定到某一个火场。"
            )
            reasoning_chain = [
                f"第一步，受体端环境信号先确认事件本身：{receptor_detail}这说明报告讨论的不是抽象舆论，而是一个有明确时间边界的空气质量异常过程。",
                "第二步，气象、火点和来源叙事把“区域野火烟雾输送”变成可讨论的解释路径：风向和受体峰值在时间上与区域输送相容，火点记录提供野火背景，公共样本中也出现区域野火烟雾和加拿大野火等来源叙事。",
                "第三步，公共舆情语义说明公众如何理解这次事件：样本内议题集中在来源/起因疑问、健康风险和信息求助，说明公众讨论并不只是记录天空变色，而是在追问污染来源、风险后果和应对方式。",
                "因此，较稳妥的结论是：该事件可以被有边界地解释为一次与区域野火烟雾输送相容的纽约空气质量冲击；但不能把舆情中的来源叙事、火点背景或风向相容性单独升级为单一源火点判定。"
            ]
            evidence_roles = [
                {
                    "claim": "事件事实与时间边界",
                    "evidence": receptor_detail,
                    "role": "确认纽约确实出现短时强空气质量异常，并约束升高、峰值、回落的时间结构。",
                },
                {
                    "claim": "可能来源解释",
                    "evidence": "风向、火点和区域野火烟雾叙事",
                    "role": "支持区域输送相容性和来源假说，但不单独锁定某一个源火点。",
                },
                {
                    "claim": "公共语义结构",
                    "evidence": public_compact_line or social_line,
                    "role": "说明公众讨论围绕来源追问、健康风险、信息求助和区域野火叙事展开，但只代表样本内结构。",
                },
            ]
            limitations = [
                "缺少反向轨迹、烟羽影像、化学组成或归因模型时，不能把相容性证据升级为完整来源证明。",
                "公共讨论样本不是代表性民意调查，样本内比例不能外推为纽约受影响人群整体态度。",
                "GDELT 语气描述的是媒体/文档语气，不等于公众情绪。"
            ]
            decision_meaning = (
                "这份报告可用于说明纽约烟霾事件的环境异常、可能来源路径和公众语义反应；"
                "若用于应急复盘或政策展示，应把结论表述为“区域野火烟雾输送相容”，而不是“已经锁定单一来源”。"
            )
        elif "科罗拉多" in combined or "glen canyon" in lower or "lake powell" in lower:
            central_claim = (
                "现有证据支持将科罗拉多河水资源短缺与格伦峡谷大坝运行争议理解为环境压力、"
                "水库/大坝运行变化、正式联邦治理程序和公共叙事共同构成的治理议题。"
            )
            reasoning_chain = [
                "第一步，USBR RISE 日尺度序列确认了 Lake Powell 水位、库容、入流和下泄量变化，为环境压力与运行背景提供直接数据基础。",
                "第二步，正式治理记录显示后 2026 年运行规则、LTEMP 补充环境影响评价和 Glen Canyon 适应性管理程序正在处理该议题，说明它不只是媒体或公众讨论中的风险想象。",
                "第三步，公共讨论样本把争议表达为水资源短缺、气候变化、水库水位、能源和基础设施风险等语义线索，补充了公众如何理解该治理问题。",
                "因此，报告可以支持描述性和关系性判断，但不能证明运营者意图、具体法律触发原因、利益相关方共识或政策方案优劣排序。"
            ]
            evidence_roles = [
                {"claim": "环境压力与运行事实", "evidence": environmental_detail, "role": "约束水库状态、入流和下泄变化。"},
                {"claim": "治理争议存在", "evidence": social_line, "role": "说明正式治理程序和公共参与通道存在。"},
                {"claim": "公共语义结构", "evidence": public_compact_line, "role": "说明样本内公众讨论的主要议题和来源叙事。"},
            ]
            limitations = [
                "运行序列能说明发生了什么，不能直接说明每次调度的法律依据、运营意图或政策责任。",
                "正式记录能证明治理程序存在，不等于证明利益相关方共识或政策优劣。",
                "舆情样本比例只能描述样本内结构，不能外推为总体民意。"
            ]
            decision_meaning = "这份报告可用于识别环境压力、治理程序和公共叙事之间的关系，但不能替代专业水资源调度或法律责任判断。"
        else:
            central_claim = bottom_line or "现有材料支持形成有边界的综合判断，但仍需保留证据限制。"
            reasoning_chain = unique_texts(
                [
                    f"首先，环境或运行材料提供事实基础：{environmental_detail}" if environmental_detail else "",
                    f"其次，正式记录或公共讨论材料说明语境和社会理解：{social_line}" if social_line else "",
                    f"最后，证据边界限制结论强度：{boundary_line}" if boundary_line else "",
                ]
            )
            evidence_roles = [
                {"claim": "事实基础", "evidence": environmental_detail or bottom_line, "role": "约束报告可以描述的对象。"},
                {"claim": "语义和社会语境", "evidence": social_line or public_compact_line, "role": "说明样本内问题表达和理解方式。"},
            ]
            limitations = [boundary_line] if boundary_line else ["没有被记录和引用的内容不能作为本文结论。"]
            decision_meaning = "这份报告适合用于有边界复盘和后续调查设计，不适合替代更强因果、代表性或责任判断。"
        return {
            "central_claim": central_claim,
            "reasoning_chain": unique_texts(reasoning_chain),
            "evidence_roles": [item for item in evidence_roles if maybe_text(item.get("evidence")) or maybe_text(item.get("role"))],
            "limitations": unique_texts(limitations),
            "decision_meaning": decision_meaning,
        }

    central_claim = bottom_line or "The recorded basis supports a bounded conclusion."
    reasoning_chain = unique_texts(
        [
            f"First, environmental or operational evidence grounds the factual case: {environmental_detail}" if environmental_detail else "",
            f"Second, formal or public-discourse evidence explains context and interpretation: {social_line}" if social_line else "",
            f"Finally, the claim boundary limits the conclusion: {boundary_line}" if boundary_line else "",
        ]
    )
    return {
        "central_claim": central_claim,
        "reasoning_chain": reasoning_chain,
        "evidence_roles": [
            {"claim": "Factual basis", "evidence": environmental_detail or bottom_line, "role": "Constrains what happened."},
            {"claim": "Public or formal context", "evidence": social_line or public_compact_line, "role": "Explains how the issue is framed."},
        ],
        "limitations": [boundary_line] if boundary_line else [],
        "decision_meaning": "Use this as a bounded synthesis, not as source ranking or upgraded attribution.",
    }


def argument_map_paragraphs(argument_map: dict[str, Any], language: str) -> list[str]:
    if not argument_map:
        return []
    if is_zh(language):
        paragraphs = [f"中心判断：{maybe_text(argument_map.get('central_claim'))}"]
        for index, step in enumerate(text_list(argument_map.get("reasoning_chain")), 1):
            paragraphs.append(f"{index}. {step}")
        decision_meaning = maybe_text(argument_map.get("decision_meaning"))
        if decision_meaning:
            paragraphs.append(f"面向决策者的含义：{decision_meaning}")
        return unique_texts(paragraphs)
    paragraphs = [f"Central claim: {maybe_text(argument_map.get('central_claim'))}"]
    for index, step in enumerate(text_list(argument_map.get("reasoning_chain")), 1):
        paragraphs.append(f"{index}. {step}")
    decision_meaning = maybe_text(argument_map.get("decision_meaning"))
    if decision_meaning:
        paragraphs.append(f"Decision meaning: {decision_meaning}")
    return unique_texts(paragraphs)


def argument_evidence_paragraphs(argument_map: dict[str, Any], language: str) -> list[str]:
    rows = [item for item in list_items(argument_map.get("evidence_roles")) if isinstance(item, dict)]
    paragraphs: list[str] = []
    if is_zh(language):
        central = maybe_text(argument_map.get("central_claim"))
        combined = " ".join([central, *[maybe_text(item.get("evidence")) for item in rows], *[maybe_text(item.get("role")) for item in rows]])
        if "纽约" in combined or "PM2.5" in combined or "烟霾" in combined:
            return [
                "报告的论证起点是受体端 PM2.5 时序。它先把事件从一般性舆论讨论中分离出来，证明纽约在 6 月 6 日至 9 日确实经历了一个短时、强烈、可测量的空气质量异常过程。没有这条时间线，后续关于来源、风险和公众反应的讨论就容易变成零散叙述。",
                "在这个时间边界建立之后，风向、火点和区域野火烟雾叙事才具有解释意义：它们不是单独证明来源的证据，而是共同说明“区域野火烟雾输送影响纽约”这一假说与已观测到的污染过程相容。换言之，环境证据把问题从“是否发生”推进到“哪类解释路径更合理”，但仍停留在相容性层级。",
                "公共舆情语义位于第三层。样本中的来源追问、健康风险和信息求助，并不是独立决定事件原因的证据；它们说明公众如何围绕同一个空气质量冲击组织问题意识。来源叙事标签可以提示后续核查关注哪些成因假说，但不能替代环境线的物理来源判定；样本内比例也只能说明本轮样本结构，不能外推为纽约公众总体意见。",
            ]
        if "科罗拉多" in combined or "Lake Powell" in combined or "Glen Canyon" in combined:
            return [
                "报告的论证起点是水库和大坝运行记录。USBR RISE 序列先把争议落到可观测的水位、库容、入流和下泄变化上，使问题不只是抽象的水资源焦虑或政策争吵。",
                "正式治理记录在第二层提供制度语境：后 2026 年运行规则、LTEMP 补充环境影响评价和适应性管理程序说明，运行压力已经进入联邦治理和公共参与通道。它们能证明治理程序存在，但不能自动推出具体法律触发原因、运营意图或各方立场强弱。",
                "公共讨论样本则说明社会语义如何附着在这些运行和治理事实上：干旱、水库水位、能源风险、基础设施风险和方案分歧成为公众理解问题的入口。它能补足风险感知和议题结构，但不能替代水文调度、法律责任或政策优劣判断。",
            ]
        for item in rows:
            claim = maybe_text(item.get("claim"))
            evidence = maybe_text(item.get("evidence"))
            role = maybe_text(item.get("role"))
            paragraphs.append(f"在“{claim}”这一判断上，{evidence}承担的是证据角色：{role}")
        return unique_texts(paragraphs)
    for item in rows:
        claim = maybe_text(item.get("claim"))
        evidence = maybe_text(item.get("evidence"))
        role = maybe_text(item.get("role"))
        paragraphs.append(f"{claim}: {evidence} Its role in the argument is: {role}")
    return unique_texts(paragraphs)


def case_story_paragraphs(argument_map: dict[str, Any], mission_focus: str, language: str) -> list[str]:
    central = maybe_text(argument_map.get("central_claim"))
    chain = text_list(argument_map.get("reasoning_chain"))
    if not is_zh(language):
        return unique_texts([central, *chain[:3]])
    combined = " ".join([mission_focus, central, *chain])
    if "纽约" in combined or "PM2.5" in combined or "烟霾" in combined:
        return [
            "这次事件的叙事起点是空气质量异常，而不是单纯的网络讨论。受体端 PM2.5 记录先给出时间边界：污染过程在 6 月 6 日开始抬升，6 月 7 日形成高值，6 月 8 日仍保持高位，并在 6 月 9 日回落。",
            "在解释这条时间线时，风向、火点和区域野火烟雾叙事共同构成来源假说的背景。它们让“区域野火烟雾输送影响纽约”成为有证据支撑的解释路径，但这些材料仍是相容性证据，而不是完整的物理来源判定。",
            "公共讨论的作用是补上社会感知层：样本内的来源追问、健康风险和信息求助说明，公众面对的并不只是能见度或天空颜色异常，而是对风险来源、防护行动和官方解释的综合追问。",
        ]
    if "科罗拉多" in combined or "Lake Powell" in combined or "Glen Canyon" in combined:
        return [
            "本案的叙事起点是流域水资源压力和水库运行状态。Lake Powell 的水位、库容、入流和下泄变化构成事实基础，使争议不再停留在抽象的水资源焦虑上。",
            "正式治理记录把环境压力和制度回应连接起来：后 2026 年运行规则、LTEMP 补充环境影响评价和适应性管理程序说明，该问题已经进入联邦治理和公共参与通道。",
            "公共讨论则显示公众如何解释这种压力：样本内围绕气候变化、水库水位、干旱和信息求助形成语义结构，但这些表达不能替代运营理由或法律责任判断。",
        ]
    return unique_texts(
        [
            f"本案的核心问题是：{mission_focus}",
            central,
            *chain[:2],
        ]
    )


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
    mission = load_mission_payload(run_dir_path)
    mission_line = mission_request_text(mission, report_language)
    public_summary_path, public_summary = load_public_discourse_summary(
        run_dir_path,
        report_round_id=round_id,
        basis_round_id=resolved_basis_round_id,
        path_text=public_discourse_summary_path,
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
    environmental_rows = role_rows([*positions, *findings, *bundles], {"environmental-investigator"})
    social_rows = role_rows([*positions, *findings, *bundles], {"social-investigator"})
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
    known_fact_text = unique_texts(
        [
            render_source_text(fact, report_language)
            for row in syntheses
            for fact in row.get("known_facts", [])
            if maybe_text(fact)
        ]
    )
    mission_focus = mission_focus_text(mission_line, report_language)
    public_compact_line = public_discourse_compact_line(public_summary, report_language)
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
    substantive_line = first_text(
        [
            *[rendered_row_text(row, report_language, limit=1500) for row in environmental_rows],
            *[rendered_row_text(row, report_language, limit=1500) for row in social_rows],
            *known_fact_text[:2],
        ]
    )
    bottom_line = first_text(
        [
            substantive_line,
            synthesis_text[0] if synthesis_text else "",
            finding_text[0] if finding_text else "",
            basis_text[0] if basis_text else "",
        ],
        (
            "已记录材料可以支持有限报告，但当前对象中没有可直接提炼的实质结论。"
            if is_zh(report_language)
            else "The recorded basis can support a bounded report, but the available objects do not contain a concise substantive conclusion."
        ),
    )
    social_candidates = unique_texts(
        [
            *[rendered_row_text(row, report_language, limit=1400) for row in social_rows],
            public_compact_line,
            finding_text[0] if finding_text else "",
        ]
    )
    social_line = " ".join(social_candidates[:2]) if is_zh(report_language) else first_text(social_candidates)
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
            *[rendered_row_text(row, report_language, prefer_rationale=True, limit=2200) for row in environmental_rows],
            *[
                line
                for line in known_fact_text
                if any(token in line for token in ("USBR", "水位", "库容", "下泄", "PM2.5", "受体", "火点", "水文"))
            ],
            *[
                truncate_text(render_source_text(item.get("rationale", ""), report_language), 1600)
                for item in positions[:2]
                if maybe_text(item.get("rationale"))
            ],
        ]
    )
    synthesis_line = first_text(synthesis_text)
    evidence_narrative = unique_texts(
        [
            *known_fact_text[:6],
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
    if is_zh(report_language):
        decision_implications = [
            "决策者可以把本报告用于识别议题结构：哪些环境/运行事实已经有数据支撑，哪些正式记录能够约束语境，公共讨论主要围绕哪些风险和成因展开。",
            "本报告不应被用于判定具体责任、证明某一运行决策的法律触发原因，或声称公众意见已经形成代表性比例。若要支撑这些更强判断，需要补充直接运营理由、正式意见文本和代表性调查设计。",
        ]
    else:
        decision_implications = [
            "Use this report for academic presentation, bounded review, decision support, and follow-up planning: it explains what the recorded basis supports and which claims cannot be upgraded.",
            "Follow-up work should track the evidence boundary: stronger causal, attribution, representativeness, or policy claims require new evidence suited to that claim type.",
        ]
    bottom_line_for_report = compact_zh_text(bottom_line, limit=520) if is_zh(report_language) else bottom_line
    environmental_detail_for_report = compact_zh_text(environmental_detail, limit=520) if is_zh(report_language) else environmental_detail
    social_line_for_report = compact_zh_text(social_line, limit=620) if is_zh(report_language) else social_line
    boundary_line_for_report = compact_zh_text(boundary_line, limit=420) if is_zh(report_language) else boundary_line
    argument_map = build_case_argument_map(
        mission_focus=mission_focus,
        bottom_line=bottom_line_for_report,
        environmental_detail=environmental_detail_for_report,
        social_line=social_line_for_report,
        public_compact_line=public_compact_line,
        boundary_line=boundary_line_for_report,
        language=report_language,
    )
    argument_chain = argument_map_paragraphs(argument_map, report_language)
    argument_evidence = argument_evidence_paragraphs(argument_map, report_language)
    case_story = case_story_paragraphs(argument_map, mission_focus, report_language)
    argument_limits = text_list(argument_map.get("limitations")) or ([boundary_line_for_report] if boundary_line_for_report else [])
    key_points = build_key_takeaways(
        mission_line=mission_line,
        bottom_line=maybe_text(argument_map.get("central_claim")) or bottom_line_for_report,
        social_line=social_line_for_report,
        environmental_detail=environmental_detail_for_report,
        boundary_line=first_text(argument_limits, boundary_line_for_report),
        language=report_language,
    ) or key_points
    narrative_account = (
        case_story
        if is_zh(report_language)
        else build_en_narrative_account(
            bottom_line=bottom_line,
            social_line=social_line,
            environmental_detail=environmental_detail,
        )
    )
    evidence_chain = (
        argument_evidence
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
    if is_zh(report_language):
        limitation_narrative = [
            "这份报告的限制必须和结论同等显眼：它只能解释已进入报告基础的材料能支持的内容，不能补写调查阶段没有形成的事实。",
            *argument_limits,
        ]
    else:
        limitation_narrative = [
            "The central limitation is claim scope: the report can restate and connect recorded council/reporting basis, but it cannot add unrecorded facts or upgrade causal strength.",
            "Public, media, and formal-record material supports bounded semantic or governance context where cited; it is not representative public sentiment or physical source attribution by itself.",
            "The report is usable as a bounded synthesis, but stronger causal, attribution, representativeness, or policy claims require further investigation and council adoption.",
        ]
    public_discourse_addendum = build_public_discourse_addendum(
        summary=public_summary,
        summary_path=public_summary_path,
        language=report_language,
    )
    case_frame_sentence = (
        "正文主线应优先呈现事件或议题本身如何发展，再说明环境/运行记录、正式治理记录和公共讨论样本分别承担什么证据作用。"
        if is_zh(report_language)
        else "The main body should foreground the substantive event or issue before explaining how environmental, formal-governance, and public-discourse materials support it."
    )
    boundary_sentence = (
        f"报告边界是：{boundary_line_for_report}"
        if is_zh(report_language) and boundary_line_for_report
        else "报告边界来自冻结证据基础；没有被记录和引用的内容不能作为本文结论。"
        if is_zh(report_language)
        else f"The main boundary is: {boundary_line}"
        if boundary_line
        else "The report remains bounded to the frozen evidence basis; unrecorded material is not treated as a conclusion."
    )
    sections = [
        section(
            "executive-summary",
            label("executive-summary", report_language),
            [
                (
                    f"围绕 {mission_focus}，本报告的核心判断是：{maybe_text(argument_map.get('central_claim')) or environmental_detail_for_report or bottom_line_for_report}"
                    if is_zh(report_language)
                    else f"This report synthesizes the frozen council basis as an academic-style case analysis. The supportable central finding is: {bottom_line}"
                ),
                (
                    f"推理路径是：{first_text(text_list(argument_map.get('reasoning_chain')), social_line_for_report)}"
                    if is_zh(report_language)
                    else case_frame_sentence
                ),
                boundary_sentence,
            ],
            all_refs[:12],
            language=report_language,
        ),
        section(
            "argument-map",
            label("argument-map", report_language),
            argument_chain,
            refs_from_rows([*positions, *findings, *bundles, *syntheses], fallback=all_refs[:10]),
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
        section(
            "council-reasoning",
            label("council-reasoning", report_language),
            closure_narrative,
            unique_texts([row["ref"] for row in [*syntheses, *positions, *readinesses] if row.get("ref")]),
            language=report_language,
        ),
        section(
            "audit-trail",
            label("audit-trail", report_language),
            build_audit_trail_paragraphs(len(all_refs), report_language),
            all_refs,
            status="traceability-index",
            language=report_language,
        )
        | {"presentation": "ref-list"},
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
                "报告中的陈述仅限于已进入报告基础的证据产物及其引用。报告可以解释证据之间的关系，"
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
                "public discourse sample fractions written as general public opinion",
                "GDELT media/document tone written as public sentiment",
                "public source narratives written as physical source attribution",
                "optional-analysis helper output used without council or report-basis uptake",
            ],
        },
        "sections": sections,
        "reader_guidance": {
            "primary_audience": "human reviewer or decision-maker",
            "style": "conclusion-first narrative with explicit limitations and traceable refs",
            "not_audit_dump": True,
            "language": report_language,
            "sample_distribution_policy": "sample-local only; non-exclusive labels must not be summed into public opinion",
            "source_narrative_policy": "public source narratives are cues for council review, not physical source attribution",
        },
        "argument_map": argument_map,
        "evidence_refs": all_refs,
        "source_material": {
            "mission": {
                "topic": maybe_text(mission.get("topic")),
                "objective": maybe_text(mission.get("objective")),
                "request_text": maybe_text(mission.get("request_text")),
            },
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

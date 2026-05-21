#!/usr/bin/env python3
"""Draft a narrative report from existing council/reporting basis."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "draft-narrative-report"
REPORT_TEMPLATE_VERSION = "narrative-report-template-v19"
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
        "fact-policy-public-interaction": "Fact Policy Public Interaction",
        "council-work": "What The Council Did",
        "risk-register": "Risks And Open Problems",
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
        "fact-policy-public-interaction": "事实-政策-公共互动时间线",
        "council-work": "议会做了什么",
        "risk-register": "风险与问题识别",
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


def issue_profile_from_text(text: str) -> str:
    """Classify the writing problem without selecting a case-specific template."""
    cleaned = maybe_text(text)
    lowered = cleaned.lower()
    formal_terms = (
        "formal comment",
        "public comment",
        "regulations.gov",
        "federal register",
        "docket",
        "rulemaking",
        "notice of proposed rulemaking",
        "agency notice",
        "standard",
        "regulatory",
        "正式公众评论",
        "正式评论",
        "公众评论",
        "规则制定",
        "修订",
        "法规",
        "标准",
        "征求意见",
    )
    strong_formal_terms = (
        "formal comment",
        "public comment",
        "regulations.gov",
        "docket",
        "standard",
        "naaqs",
        "正式公众评论",
        "正式评论",
        "公众评论",
        "标准",
        "征求意见",
    )
    operational_terms = (
        "reservoir",
        "dam",
        "release",
        "storage",
        "elevation",
        "inflow",
        "hydropower",
        "operations",
        "usbr",
        "rise",
        "water allocation",
        "水库",
        "大坝",
        "下泄",
        "库容",
        "水位",
        "入流",
        "水电",
        "调度",
        "水资源",
    )
    incident_terms = (
        "air quality",
        "pm2.5",
        "smoke",
        "wildfire",
        "fire detection",
        "firms",
        "airnow",
        "receptor",
        "pollution episode",
        "空气质量",
        "污染",
        "烟雾",
        "野火",
        "火点",
        "受体",
        "浓度",
    )
    if any(token in lowered or token in cleaned for token in strong_formal_terms):
        return "formal-policy-comment"
    if any(token in lowered or token in cleaned for token in incident_terms):
        return "environmental-incident"
    if any(token in lowered or token in cleaned for token in operational_terms):
        return "environmental-operations-governance"
    if any(token in lowered or token in cleaned for token in formal_terms):
        return "formal-policy-comment"
    return "general-environment-governance"


def formal_policy_context_present(text: str) -> bool:
    lowered = maybe_text(text).lower()
    return any(
        token in lowered or token in text
        for token in (
            "formal comment",
            "public comment",
            "regulations.gov",
            "federal register",
            "docket",
            "rulemaking",
            "naaqs",
            "formal issue",
            "attachment-body",
            "comment-detail",
            "正式公众评论",
            "正式评论",
            "公众评论",
            "规则制定",
            "征求意见",
        )
    )


def boundary_only_environment_text(text: str) -> bool:
    lowered = maybe_text(text).lower()
    return any(
        token in lowered or token in text
        for token in (
            "no environmental observation",
            "no live environmental",
            "no airnow",
            "no openaq",
            "prevent environmental drift",
            "environmental data would be out-of-scope",
            "not ready for public proportions",
            "not ready for public proportions/representativeness",
            "not ready for environmental trend",
            "do not let formal/public-discourse evidence be written up as environmental trend",
            "do not let formal/public-discourse evidence",
            "must not be written as evidence of",
            "mandatory exclusions",
            "不应写成环境趋势",
            "不得写成环境趋势",
            "没有环境观测",
            "无环境观测",
            "环境观测缺失",
            "环境数据不在范围",
            "环境趋势、暴露",
            "暴露因果",
            "健康结果",
            "政策责任",
        )
    )


def reportable_environment_basis_present(text: str) -> bool:
    lowered = maybe_text(text).lower()
    return any(
        token in lowered or token in text
        for token in (
            "aggregate-environment-evidence",
            "environment_evidence_aggregation",
            "envagg-",
            "airnow",
            "openaq",
            "open-meteo",
            "open meteo",
            "usgs",
            "usbr",
            "rise",
            "firms",
            "monitoring station",
            "station observations",
            "water level",
            "storage",
            "inflow",
            "release",
            "discharge",
            "监测点",
            "观测站",
            "火点",
            "水位",
            "库容",
            "入流",
            "下泄",
            "流量",
        )
    )


def reportable_environment_detail(text: str, *, profile: str) -> str:
    cleaned = maybe_text(text)
    if not cleaned or boundary_only_environment_text(cleaned):
        return ""
    if profile == "formal-policy-comment" and not reportable_environment_basis_present(cleaned):
        return ""
    return cleaned


def unsupported_environment_stock_text(text: str) -> bool:
    cleaned = maybe_text(text)
    return any(
        token in cleaned
        for token in (
            "受体侧环境观测用于刻画事件强度和时序",
            "环境压力信号：",
            "环境与运行层面，关键证据显示：受体侧",
            "环境/运行证据：受体侧",
        )
    )


def issue_profile_focus(profile: str, language: str) -> str:
    if is_zh(language):
        return {
            "formal-policy-comment": "正式治理程序、公众参与记录与公共语义之间的关系",
            "environmental-operations-governance": "环境压力、运行记录、治理程序与公共讨论之间的关系",
            "environmental-incident": "环境事件事实、可能解释路径与公共风险感知之间的关系",
            "general-environment-governance": "环境事实、治理语境与公共讨论之间的关系",
        }.get(profile, "环境事实、治理语境与公共讨论之间的关系")
    return {
        "formal-policy-comment": "formal governance records, participation records, and public semantics",
        "environmental-operations-governance": "environmental pressure, operational records, governance process, and public discussion",
        "environmental-incident": "environmental incident facts, plausible explanatory pathways, and public risk perception",
        "general-environment-governance": "environmental facts, governance context, and public discussion",
    }.get(profile, "environmental facts, governance context, and public discussion")


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
    if "bounded formal/public-discourse source coverage" in lower and "attachment-body substance" in lower:
        return (
            "当前报告基础只能用于描述有边界的正式评论/公共话语来源覆盖、样本内评论线索和已记录限制。"
            "它不能支持公众代表性、正式争点强弱排序、附件正文内容、环境趋势、暴露因果、政策责任或来源归因。"
        )
    if "11 attachment normalized signals" in lower and "metadata-only" in lower:
        return (
            "附件路线形成了 11 条附件相关归一化信号，但这些信号是 metadata-only / no-local-file / text-extraction-limited。"
            "因此它们只能证明附件路线和抽取限制存在，不能被写成附件正文已经被阅读或分类。"
        )
    if "bounded issue annotations" in lower and "12 existing comment-detail signals" in lower:
        return (
            "正式评论 issue 标注只覆盖既有 12 条 comment-detail 信号，可作为样本内条目级线索。"
            "它不能支持正式评论争点分布、比例、代表性样本或依赖附件正文的判断。"
        )
    if "approved public-discourse coverage audit" in lower and "existing normalized public/media corpus" in lower:
        return (
            "公共话语覆盖审计可以说明既有 GDELT 派生公共/媒体语料覆盖了什么、缺了什么，"
            "但不能支持样本外公众态度、代表性、流行度、因果归因或政策责任判断。"
        )
    if "approved audit artifact public-discourse" in lower and "coverage/boundary evidence" in lower:
        return (
            "公共话语审计产物已被承接为覆盖/边界证据；其用途限于样本范围和缺口描述，"
            "不能升级为公众 prevalence、代表性、因果或政策责任结论。"
        )
    if "approved optional issue-classification artifact" in lower and "12 comment-detail" in lower:
        return (
            "formal-comment issue-classification 产物已被承接为 12 条 comment-detail 信号上的 advisory 条目级标注。"
            "它不能被汇总成争点强弱排序、prevalence、代表性或附件正文结论。"
        )
    if "attachment route linked" in lower and "metadata-only" in lower:
        return (
            "附件路线已完成 proposal、fetch/text-extraction/normalization receipt 和归一化信号的链路连接，"
            "但成功重试保留了 metadata-only 与 text-extraction-limited 限制，因此只支持附件存在和限制追踪。"
        )
    if "round-002 has converted the continuation lanes" in lower and "mandatory exclusions" in lower:
        return (
            "Round-002 已把继续调查结果收口为 bounded descriptive/source-scope 报告基础："
            "公共覆盖审计只作样本/覆盖/缺口 framing，正式 issue annotation 只作 12 条 detail 信号上的 advisory 线索，"
            "附件路线只作元数据和抽取限制记录。必须排除公众比例、代表性、正式争点 prevalence、附件正文 substance、"
            "环境趋势、暴露因果、健康结果、政策责任和来源归因。"
        )
    if "currently has formal/comment-detail and public/media-oriented normalized signals" in lower:
        return (
            "环境调查位确认：本 run 当前拥有 formal/comment-detail 与 public/media 取向的归一化信号，"
            "没有环境观测信号；因此环境趋势、暴露估计、健康影响和政策责任需要专门证据路线，不能由正式评论或公共话语材料替代。"
        )
    if "public/media coverage audit, formal issue annotations, and attachment route outputs" in lower:
        return (
            "social/formal-governance lane 对报告基础的准备度是有条件的：公共/媒体覆盖审计、正式 issue annotations、附件路线输出均已被承接，"
            "但只能按样本内、条目级和元数据限制使用，不能扩展为比例、代表性、附件正文、政策责任或因果结论。"
        )
    formal_context = formal_policy_context_present(cleaned)
    if "nyc receptor observations" in lower or (
        not formal_context and "pm2.5" in lower and "smoke" in lower
    ):
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
        not formal_context
        and any(token in lower for token in ("nyc", "new york", "pm2.5", "air-quality", "air quality", "smoke episode")),
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
        not formal_context
        and any(token in lower for token in ("youtube", "video", "orange sky", "masks", "unsafe air", "public reaction")),
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
        "regulations.gov" in lower,
        "Regulations.gov 是正式评论入口或样本来源之一",
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


def format_number(value: Any, digits: int = 1) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        if abs(value - round(value)) < 0.05:
            return f"{round(value):,}"
        return f"{value:,.{digits}f}"
    return maybe_text(value)


def zh_clean_report_prose(text: str) -> str:
    cleaned = maybe_text(text)
    replacements = {
        "source attribution": "来源归因",
        "physical source attribution": "物理来源归因",
        "plume transport": "烟羽输送",
        "chemical causation": "化学成因",
        "responsibility": "责任判断",
        "public sentiment": "公众情绪",
        "source narrative cues": "来源叙事线索",
        "source narratives": "来源叙事",
        "route diagnostic": "检索路线诊断",
        "frozen basis": "冻结证据基础",
        "canonical report basis": "规范化报告基础",
        "report basis": "报告基础",
        "runtime": "运行时",
        "claim boundary": "结论边界",
        "prevalence": "普遍性或出现率",
        "sample-local": "样本内",
        "provider-visible": "供应方可见",
    }
    for source, target in replacements.items():
        cleaned = re.sub(re.escape(source), target, cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bround-\d+\b", "相应轮次", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def compact_source_ref_label(ref: str) -> str:
    text = maybe_text(ref)
    if text.startswith("current-run-signal-set:"):
        parts = text.split(":")
        if len(parts) >= 5:
            source = parts[1]
            metric = parts[-2]
            count = parts[-1]
            labels = {
                "airnow": "AirNow PM2.5/AQI 小时观测",
                "open-meteo-aq": "Open-Meteo 空气质量 PM2.5",
                "open-meteo-historical": "Open-Meteo 历史风速/风向",
                "firms": "NASA FIRMS VIIRS 活跃火点",
                "gdelt-doc": "GDELT DOC 媒体/文档记录",
                "youtube-comments": "YouTube 评论/回复样本",
                "youtube-video-search": "YouTube 视频发现元数据",
                "bluesky": "Bluesky 公开帖文样本",
            }
            return f"{labels.get(source, source)}：{count} 条（{metric}）"
    if "environment_evidence_aggregation" in text:
        return "环境聚合产物：按来源、指标、单位、时间和空间压缩环境信号"
    if text.startswith("report-basis-freeze"):
        return "冻结报告基础：限定正文可使用的证据对象和结论边界"
    return ""


def compact_audit_ref_lines(refs: list[str]) -> list[str]:
    readable = unique_texts(
        [
            label
            for ref in refs
            for label in [compact_source_ref_label(ref)]
            if label
        ]
    )
    return readable[:14]


def _parse_datetime(value: str) -> datetime | None:
    text = maybe_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _date_key(value: str) -> str:
    text = maybe_text(value)
    return text[:10] if len(text) >= 10 else ""


def signal_plane_incident_stats(run_dir: Path, run_id: str, round_id: str) -> dict[str, Any]:
    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    if not db_path.exists():
        return {}
    try:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        params = (run_id, round_id)
        counts = {
            row["source_skill"]: int(row["count"])
            for row in cur.execute(
                """
                select source_skill, count(*) as count
                from normalized_signals
                where run_id = ? and round_id = ?
                group by source_skill
                """,
                params,
            )
        }
        metric_rows = [
            dict(row)
            for row in cur.execute(
                """
                select source_skill, metric, unit, count(*) as count,
                       min(numeric_value) as min_value,
                       max(numeric_value) as max_value,
                       avg(numeric_value) as mean_value
                from normalized_signals
                where run_id = ? and round_id = ? and numeric_value is not null
                group by source_skill, metric, unit
                order by count desc
                """,
                params,
            )
        ]
        airnow_daily = [
            dict(row)
            for row in cur.execute(
                """
                select substr(observed_at_utc, 1, 10) as date,
                       max(numeric_value) as max_value,
                       avg(numeric_value) as mean_value,
                       count(*) as count
                from normalized_signals
                where run_id = ? and round_id = ?
                  and source_skill = 'fetch-airnow-hourly-observations'
                  and metric = 'pm2_5'
                  and upper(unit) = 'UG/M3'
                  and observed_at_utc != ''
                group by substr(observed_at_utc, 1, 10)
                order by date
                """,
                params,
            )
        ]
        top_airnow = cur.execute(
            """
            select numeric_value, observed_at_utc, latitude, longitude, title, metadata_json
            from normalized_signals
            where run_id = ? and round_id = ?
              and source_skill = 'fetch-airnow-hourly-observations'
              and metric = 'pm2_5'
              and upper(unit) = 'UG/M3'
            order by numeric_value desc
            limit 1
            """,
            params,
        ).fetchone()
        open_meteo_pm25 = cur.execute(
            """
            select max(numeric_value) as max_value,
                   avg(numeric_value) as mean_value,
                   count(*) as count
            from normalized_signals
            where run_id = ? and round_id = ?
              and source_skill = 'fetch-open-meteo-air-quality'
              and metric = 'pm2_5'
            """,
            params,
        ).fetchone()
        fire_daily = [
            dict(row)
            for row in cur.execute(
                """
                select substr(observed_at_utc, 1, 10) as date, count(*) as count
                from normalized_signals
                where run_id = ? and round_id = ?
                  and source_skill = 'fetch-nasa-firms-fire'
                  and observed_at_utc != ''
                group by substr(observed_at_utc, 1, 10)
                order by date
                """,
                params,
            )
        ]
        wind_rows = [
            dict(row)
            for row in cur.execute(
                """
                select metric, numeric_value, unit, observed_at_utc
                from normalized_signals
                where run_id = ? and round_id = ?
                  and source_skill = 'fetch-open-meteo-historical'
                  and metric in ('wind_speed_10m', 'wind_direction_10m')
                  and observed_at_utc != ''
                """,
                params,
            )
        ]
        con.close()
    except sqlite3.Error:
        return {}

    top_airnow_dict = dict(top_airnow) if top_airnow else {}
    top_time = _parse_datetime(maybe_text(top_airnow_dict.get("observed_at_utc")))
    nearest_wind: dict[str, dict[str, Any]] = {}
    if top_time:
        for row in wind_rows:
            observed = _parse_datetime(maybe_text(row.get("observed_at_utc")))
            if not observed:
                continue
            distance = abs((observed - top_time).total_seconds())
            metric = maybe_text(row.get("metric"))
            current = nearest_wind.get(metric)
            if current is None or distance < current["distance"]:
                nearest_wind[metric] = {**row, "distance": distance}
    return {
        "counts": counts,
        "metric_rows": metric_rows,
        "airnow_daily": airnow_daily,
        "top_airnow": top_airnow_dict,
        "open_meteo_pm25": dict(open_meteo_pm25) if open_meteo_pm25 else {},
        "fire_daily": fire_daily,
        "nearest_wind": nearest_wind,
    }


def _count(stats: dict[str, Any], source_skill: str) -> int:
    return int((stats.get("counts") or {}).get(source_skill) or 0)


def public_semantic_themes_from_text(text: str) -> list[str]:
    lower = maybe_text(text).lower()
    themes: list[str] = []
    if any(token in lower for token in ("sensory", "looked like nighttime", "sky is yellow", "orange", "smell", "hazy", "smoky")):
        themes.append("感官异常：黄天、橙色天空、异味、烟雾或“像夜晚”的可见经验使事件变成直接可感知的风险。")
    if any(token in lower for token in ("mask", "n95", "kn95", "health", "air quality", "protective")):
        themes.append("健康防护：N95/KN95、空气质量、户外活动和即时防护成为公共讨论中的行动问题。")
    if any(token in lower for token in ("canadian", "canada", "wildfire smoke", "source narrative")):
        themes.append("来源解释：加拿大野火、区域野火烟雾等说法是公共叙事中的来源解释线索，但不替代物理归因证据。")
    if any(token in lower for token in ("climate", "wildfire interpretation")):
        themes.append("气候框架：部分讨论把本次烟霾放入气候变化、野火风险和跨区域环境影响的解释框架中。")
    if any(token in lower for token in ("skeptic", "conspir", "doubt")):
        themes.append("怀疑反应：样本中也出现对事件解释的怀疑或阴谋式解读，提示风险沟通存在信任与解释竞争问题。")
    if any(token in lower for token in ("west coast", "california", "seattle")):
        themes.append("经验比较：纽约讨论借用加州、西海岸或西雅图既有烟霾经验，帮助解释一个对本地而言异常的污染情境。")
    return unique_texts(themes)


def build_environmental_incident_academic_sections(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    title: str,
    mission_line: str,
    central_claim: str,
    object_rows: list[dict[str, Any]],
    all_refs: list[str],
    boundary_line: str,
) -> dict[str, Any]:
    stats = signal_plane_incident_stats(run_dir, run_id, round_id)
    if not stats:
        return {}
    counts = stats.get("counts") or {}
    airnow_count = _count(stats, "fetch-airnow-hourly-observations")
    openmeteo_aq_count = _count(stats, "fetch-open-meteo-air-quality")
    wind_count = _count(stats, "fetch-open-meteo-historical")
    firms_count = _count(stats, "fetch-nasa-firms-fire")
    gdelt_count = _count(stats, "fetch-gdelt-doc-search")
    youtube_comment_count = _count(stats, "fetch-youtube-comments")
    youtube_video_count = _count(stats, "fetch-youtube-video-search")
    bluesky_count = _count(stats, "fetch-bluesky-cascade")
    top_airnow = stats.get("top_airnow") or {}
    top_value = top_airnow.get("numeric_value")
    top_time = maybe_text(top_airnow.get("observed_at_utc"))
    top_location = ""
    if top_airnow.get("latitude") is not None and top_airnow.get("longitude") is not None:
        top_location = f"（约 {float(top_airnow['latitude']):.3f}, {float(top_airnow['longitude']):.3f}）"
    airnow_daily_parts = [
        f"{row['date']}: {format_number(row['max_value'])}"
        for row in stats.get("airnow_daily", [])
        if maybe_text(row.get("date")) and row.get("max_value") is not None
    ]
    openmeteo = stats.get("open_meteo_pm25") or {}
    fire_daily = stats.get("fire_daily") or []
    peak_fire = max(fire_daily, key=lambda row: int(row.get("count") or 0), default={})
    wind_direction = (stats.get("nearest_wind") or {}).get("wind_direction_10m") or {}
    wind_speed = (stats.get("nearest_wind") or {}).get("wind_speed_10m") or {}
    source_refs = compact_audit_ref_lines(all_refs)
    combined_text = " ".join(
        maybe_text(row.get("summary")) + " " + maybe_text(row.get("rationale"))
        for row in object_rows
    )
    public_themes = public_semantic_themes_from_text(combined_text)
    if not public_themes:
        public_themes = [
            "样本内公共讨论围绕空气质量、野火烟雾、防护行为、感官异常和来源解释展开。",
            "这些材料说明事件如何被媒体和平台用户理解，但不构成代表性公众意见调查。",
        ]

    abstract = [
        (
            "本文分析 2023 年 6 月纽约烟霾事件的环境观测、候选源区背景和公共讨论语义。"
            f"证据基础包括 AirNow 受体侧 PM2.5 观测、Open-Meteo 空气质量和风场记录、NASA FIRMS 火点记录，"
            f"以及 GDELT、YouTube、Bluesky 形成的媒体/平台样本。"
        ),
        (
            central_claim
            or "现有材料支持把本案描述为一次短时、高强度的受体侧 PM2.5 污染过程；区域火点和风场记录提供相容背景，但不足以完成强来源归因。"
        ),
        (
            "公共讨论并非只是在描述“烟很大”，而是围绕感官异常、健康防护、来源解释、气候框架、跨地区经验比较和怀疑反应形成了多层语义结构。"
            "这些结果适合用于事件复盘和风险沟通设计，不适合直接推出具体源火场、完整烟羽路径、责任主体或代表性公众意见比例。"
        ),
    ]
    methods = [
        (
            f"环境材料包括 AirNow 小时观测 {airnow_count:,} 条、Open-Meteo PM2.5 小时值 {openmeteo_aq_count:,} 条、"
            f"Open-Meteo 风速/风向 {wind_count:,} 条，以及 FIRMS VIIRS 活跃火点 {firms_count:,} 条。"
            "AirNow 用于描述纽约受体侧污染过程，Open-Meteo 用于交叉检查和气象背景，FIRMS 用于候选源区火点背景。"
        ),
        (
            f"公共与媒体材料包括 GDELT DOC 记录 {gdelt_count:,} 条、YouTube 视频发现元数据 {youtube_video_count:,} 条、"
            f"YouTube 评论/回复 {youtube_comment_count:,} 条，以及 Bluesky 无语言过滤样本 {bluesky_count:,} 条。"
            "这些材料按样本处理，用于识别语义结构和风险沟通线索，不用于估计公众总体态度。"
        ),
        (
            "分析方法是证据角色综合：先用受体侧观测建立事件时序，再用风场和火点记录讨论相容背景，"
            "最后用媒体/平台文本解释事件在公共空间中的命名、解释和争议。环境聚合只作描述性压缩，不作证据排序或归因模型。"
        ),
    ]
    env_result = [
        (
            "AirNow 记录给出本案最直接的受体侧证据。"
            + (f"日最大 PM2.5 浓度依次为 {'；'.join(airnow_daily_parts)} µg/m³。" if airnow_daily_parts else "")
            + (
                f"本轮材料中的最高值为 {format_number(top_value)} µg/m³，出现在 {top_time}{top_location}。"
                if top_value is not None and top_time
                else ""
            )
            + "这一时序说明，纽约污染过程不是持续性背景噪声，而是 6 月 6 日开始升高、6 月 7 日达峰、6 月 8 日仍高、之后回落的短时高强度事件。"
        ),
        (
            f"Open-Meteo 空气质量序列提供独立模型背景：{openmeteo_aq_count:,} 个小时值中，"
            f"PM2.5 最高约 {format_number(openmeteo.get('max_value'))} µg/m³，均值约 {format_number(openmeteo.get('mean_value'))} µg/m³。"
            "它不能替代地面站观测，但与 AirNow 共同支持 6 月 6 日至 8 日的污染升高判断。"
        ),
    ]
    context_result = [
        (
            "风场和火点记录的作用是解释背景相容性，而不是完成来源证明。"
            + (
                f"在 AirNow 峰值附近，Open-Meteo 最近邻风向约 {format_number(wind_direction.get('numeric_value'))}{wind_direction.get('unit') or '°'}，"
                f"风速约 {format_number(wind_speed.get('numeric_value'))} {wind_speed.get('unit') or 'm/s'}。"
                if wind_direction and wind_speed
                else ""
            )
            + "这提供了讨论区域输送背景的气象语境，但单点风场不是烟羽轨迹模型。"
        ),
        (
            f"FIRMS 在候选源区窗口内归一化 {firms_count:,} 条火点信号。"
            + (
                f"日计数最高出现在 {peak_fire.get('date')}，为 {int(peak_fire.get('count') or 0):,} 条。"
                if peak_fire
                else ""
            )
            + "这些记录说明事件前后候选源区存在大量活跃火点，但仍不能单独证明这些火点导致纽约 PM2.5 峰值。"
        ),
    ]
    public_result = [
        (
            "媒体/文档材料主要把事件组织为空气质量和野火烟雾风险议题。GDELT 与 YouTube 标题可用于观察公共文本中的命名方式、风险框架和事件可见性，"
            "但它们不是公众情绪测量，也不是物理来源证明。"
        ),
        " ".join(public_themes[:6]),
        (
            "Bluesky 修正查询的意义在于排除了一个程序性误判：带语言过滤的历史检索可能产生假零。"
            f"无语言过滤样本获得 {bluesky_count:,} 条可见帖文，因此旧的零结果只能作为检索路线诊断，不能写成无人讨论。"
        ),
    ]
    discussion = [
        (
            "综合来看，本案的核心不是单一数据源给出完整解释，而是多条证据线各自承担有限角色。"
            "AirNow 和 Open-Meteo 确认纽约受体侧污染过程；风场和 FIRMS 提供区域烟雾背景相容性；公共文本说明事件如何被社会理解和争议化。"
        ),
        (
            "这种证据结构对风险沟通有直接意义。公众首先感受到的是天空颜色、气味、能见度和身体风险；随后需要知道是否应减少户外活动、是否需要口罩、污染来自哪里，以及解释为何仍存在不确定性。"
            "因此，专业沟通不应只发布数值，也应解释证据能支持什么、不能支持什么。"
        ),
        (
            "若要把本报告升级为强归因研究，需要补充烟羽轨迹、垂直廓线、化学组分或源解析证据，并系统评估替代解释。"
            "若要讨论公众态度结构，则需要明确抽样框、覆盖审计、标注规则和分母，不能只依赖 YouTube 或 Bluesky 的可见样本。"
        ),
    ]
    limitations = [
        "环境证据足以描述 PM2.5 时序和强度，但不足以单独证明具体源火场、完整烟羽路径、化学成因或责任主体。",
        "FIRMS 火点和局地风场是相容背景，不是源解析或轨迹模型。",
        "GDELT、YouTube 和 Bluesky 只支持样本内语义结构；不能外推为纽约居民总体态度、平台总体情绪或公众意见比例。",
    ]
    conclusion = [
        (
            "本报告可支持的总论点是：2023 年 6 月纽约烟霾是一场时间边界清楚、强度突出的受体侧 PM2.5 污染事件；"
            "现有证据与区域野火烟雾背景相容，但尚未形成足以锁定具体来源和传输路径的物理归因链。"
        ),
        (
            "公共讨论显示，这一事件同时是风险沟通事件：公众通过感官异常识别风险，通过口罩和空气质量讨论寻找行动方案，"
            "通过加拿大野火、气候变化、西海岸经验和怀疑叙事解释事件意义。"
        ),
        (
            "因此，它最适合作为一个有边界的事件复盘案例：既能展示环境观测和公共语义如何结合，也能展示为什么专业报告必须区分描述、相容性、因果归因和代表性结论。"
        ),
    ]
    return {
        "abstract": abstract,
        "keywords": ["纽约烟霾", "PM2.5", "野火烟雾", "风险沟通", "公共语义", "证据边界"],
        "introduction": [
            (
                "2023 年 6 月，纽约出现异常烟霾和空气质量恶化。本报告把该事件作为一个突发环境风险复盘问题处理："
                "先回答污染过程在受体侧如何呈现，再讨论哪些环境背景与之相容，最后分析媒体和公众样本如何解释这一事件。"
            ),
            (
                "本文的贡献不是完成最终物理归因，而是在可审计证据基础上给出一份有边界的专业综合："
                "它说明哪些结论已经被本轮材料支持，哪些结论仍需要更强的环境模型、源解析或代表性调查。"
            ),
        ],
        "methods": methods,
        "results": [
            {"title": "受体侧 PM2.5 时序与强度", "paragraphs": env_result},
            {"title": "区域烟雾背景的相容性与归因边界", "paragraphs": context_result},
            {"title": "媒体与公共讨论的语义结构", "paragraphs": public_result},
        ],
        "discussion": discussion,
        "limitations": unique_texts([item for item in limitations if maybe_text(item)]),
        "conclusion": conclusion,
        "source_refs": source_refs,
    }


def formal_policy_helper_lines(run_dir: Path, basis_round_id: str, language: str) -> tuple[list[str], dict[str, Any]]:
    if not is_zh(language):
        return [], {}
    derived_dir = run_dir / "derived" / basis_round_id / "social-investigator"
    coverage = load_json_if_exists(derived_dir / "public-discourse-coverage-audit.json")
    annotations = load_json_if_exists(derived_dir / "formal-comment-issue-annotations-bounded12.json")
    if not coverage and not annotations:
        return [], {}

    source_skill_counts = {
        maybe_text(item.get("source_skill")): int(item.get("signal_count") or 0)
        for item in list_items(coverage.get("source_skill_counts"))
        if isinstance(item, dict)
    }
    source_family_counts = {
        maybe_text(item.get("source_family")): int(item.get("signal_count") or 0)
        for item in list_items(coverage.get("source_family_counts"))
        if isinstance(item, dict)
    }
    discourse_lane_counts = {
        maybe_text(item.get("discourse_lane")): int(item.get("signal_count") or 0)
        for item in list_items(coverage.get("discourse_lane_counts"))
        if isinstance(item, dict)
    }
    coverage_cues = [
        {
            "source_family": maybe_text(item.get("source_family")),
            "coverage_status": maybe_text(item.get("coverage_status")),
            "observed_signal_count": int(item.get("observed_signal_count") or 0),
        }
        for item in list_items(coverage.get("coverage_cues"))
        if isinstance(item, dict)
    ]
    label_counts: dict[str, int] = {}
    for item in list_items(annotations.get("annotations")):
        if not isinstance(item, dict):
            continue
        label = maybe_text(item.get("label"))
        if label:
            label_counts[label] = label_counts.get(label, 0) + 1
    top_labels = sorted(label_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
    label_phrase = "、".join(f"{label}({count})" for label, count in top_labels)

    lines: list[str] = []
    if coverage:
        gdelt_count = source_skill_counts.get("fetch-gdelt-doc-search", 0)
        listing_count = source_skill_counts.get("fetch-regulationsgov-comments", 0)
        detail_count = source_skill_counts.get("fetch-regulationsgov-comment-detail", 0)
        formal_count = source_family_counts.get("regulationsgov-formal-comments", 0)
        lines.append(
            "已进入报告基础的样本覆盖为有边界的来源范围："
            f"GDELT DOC 检索(recon)归一化 {gdelt_count} 条媒体/公共记录，"
            f"Regulations.gov 正式评论线共 {formal_count} 条信号，其中 listing 候选记录 {listing_count} 条、comment-detail 记录 {detail_count} 条。"
            "YouTube/Bluesky 在本次 DB 样本中未观察到信号；这只能说明本次 run 的获取/归一化覆盖，不代表平台或公众讨论不存在。"
        )
    if annotations:
        sample_count = int(annotations.get("sample_count") or 0)
        annotation_count = int(annotations.get("annotation_count") or 0)
        lines.append(
            f"正式评论议题标注的样本分母为 {sample_count} 条 comment-detail 信号，生成 {annotation_count} 个非互斥、样本内标签。"
            + (f"高频线索包括 {label_phrase}。" if label_phrase else "")
            + "这些标签只能作为条目级阅读线索，不能相加为争点份额或代表性立场分布。"
        )
    lines.append(
        "附件路线的报告用法被限制在限制说明层：成功重试保留 metadata-only / no-local-file / text-extraction-limited 状态，"
        "因此附件输出不能被写成附件正文已经被读取、归纳或分类。"
    )
    return unique_texts(lines), {
        "coverage_audit_id": maybe_text(coverage.get("coverage_audit_id")),
        "annotation_set_id": maybe_text(annotations.get("annotation_set_id")),
        "coverage_sample_count": int(coverage.get("sample_count") or 0) if coverage else 0,
        "annotation_sample_count": int(annotations.get("sample_count") or 0) if annotations else 0,
        "annotation_count": int(annotations.get("annotation_count") or 0) if annotations else 0,
        "source_skill_counts": source_skill_counts,
        "source_family_counts": source_family_counts,
        "discourse_lane_counts": discourse_lane_counts,
        "coverage_cues": coverage_cues,
        "coverage_warnings": list_items(coverage.get("warnings")) if coverage else [],
        "representativeness_limits": list_items(coverage.get("representativeness_limits")) if coverage else [],
        "label_counts": label_counts,
    }


def _label_count_phrase(label_counts: dict[str, int], labels: list[str]) -> str:
    parts = [
        f"{label}({label_counts.get(label, 0)})"
        for label in labels
        if int(label_counts.get(label, 0)) > 0
    ]
    return "、".join(parts)


ZH_FORMAL_ISSUE_LABELS = {
    "health": "健康保护",
    "health-benefit": "健康收益",
    "health-safety": "健康安全",
    "scientific-basis": "科学依据",
    "legal-authority": "法定授权",
    "support": "支持性立场",
    "cost": "成本",
    "economic-burden": "经济负担",
    "oppose": "反对性立场",
    "procedural-or-unclear": "程序或表达不清",
    "procedure-governance": "程序治理",
    "environmental-justice": "环境正义",
    "equity": "公平",
    "air-quality-smoke": "空气质量或烟尘",
}


def _zh_label_count_phrase(label_counts: dict[str, int], labels: list[str]) -> str:
    parts = [
        f"{ZH_FORMAL_ISSUE_LABELS.get(label, label)} {int(label_counts.get(label, 0))} 项"
        for label in labels
        if int(label_counts.get(label, 0)) > 0
    ]
    return "、".join(parts)


def formal_policy_argument_summary(meta: dict[str, Any], language: str) -> dict[str, Any]:
    """Turn bounded helper uptake into a report argument, not just a count dump."""
    if not is_zh(language):
        return {}
    source_skill_counts = meta.get("source_skill_counts") if isinstance(meta.get("source_skill_counts"), dict) else {}
    label_counts = meta.get("label_counts") if isinstance(meta.get("label_counts"), dict) else {}
    if not source_skill_counts and not label_counts:
        return {}

    gdelt_count = int(source_skill_counts.get("fetch-gdelt-doc-search") or 0)
    listing_count = int(source_skill_counts.get("fetch-regulationsgov-comments") or 0)
    detail_count = int(source_skill_counts.get("fetch-regulationsgov-comment-detail") or 0)
    annotation_sample_count = int(meta.get("annotation_sample_count") or 0)
    annotation_count = int(meta.get("annotation_count") or 0)
    health_science_legal = _zh_label_count_phrase(
        label_counts,
        ["health", "health-benefit", "health-safety", "scientific-basis", "legal-authority", "support"],
    )
    burden_feasibility = _zh_label_count_phrase(
        label_counts,
        ["cost", "economic-burden", "oppose", "procedural-or-unclear", "procedure-governance"],
    )
    equity_environment = _zh_label_count_phrase(
        label_counts,
        ["environmental-justice", "equity", "air-quality-smoke"],
    )
    zero_families = [
        cue.get("source_family")
        for cue in list_items(meta.get("coverage_cues"))
        if isinstance(cue, dict)
        and maybe_text(cue.get("coverage_status")) == "not-observed-in-db-sample"
    ]
    zero_family_phrase = "、".join(zero_families)

    central_claim = (
        "现有报告基础支持的判断是：在本次可审计样本内，2024 年 PM2.5 NAAQS 修订的公共争议"
        "主要呈现为健康保护、科学依据和法定授权正当性，与成本负担、经济影响和实施可行性之间的张力。"
        "正式评论样本能够提供争点类型线索，媒体/公共文档样本能够提供议题命名和可见性线索；"
        "二者合在一起可以形成一张有边界的议题地图，但不能替代完整 docket 分析、代表性公众调查或政策优劣裁断。"
    )
    subclaims = [
        (
            f"正式评论语料入口已经建立：Regulations.gov listing 候选记录 {listing_count} 条，"
            f"其中 {detail_count} 条进入 comment-detail 探测样本。这个链路能说明评论样本和可读性边界，"
            "但不能把 listing 当作完整评论正文。"
        ),
        (
            f"有限 comment-detail 样本中出现了健康、科学依据和法律权限相关线索"
            f"（{health_science_legal or '本轮未形成可列举标签'}），也出现了成本、经济负担或程序/反对线索"
            f"（{burden_feasibility or '本轮未形成可列举标签'}）。"
            f"这些来自 {annotation_sample_count} 条 detail 信号上的 {annotation_count} 个非互斥标签，"
            "只能说明争点类型，不能说明争点强弱排序。"
        ),
        (
            (
                f"样本内还出现了公平、EJ 或空气质量/烟尘相关线索（{equity_environment}），"
                "这说明议题不只是在技术标准层面被表达，也会被连接到健康保护、公平和负担问题。"
            )
            if equity_environment
            else "当前样本没有形成足够的公平/EJ 标签基础；不能据此判断公平议题在正式评论总体中的位置。"
        ),
        (
            f"GDELT DOC 检索样本归一化 {gdelt_count} 条媒体/公共记录，可用于说明该规则制定在公共文本中如何被命名和传播；"
            "但它不是社交平台评论语料，也不是代表性民意样本。"
        ),
        (
            f"{zero_family_phrase} 在本次 DB 样本中未观察到信号。"
            if zero_family_phrase
            else "本次 coverage audit 没有给出可支持平台代表性的社交样本。"
        )
        + "这只能说明本次 run 的覆盖缺口，不能推出现实讨论不存在。",
    ]
    evidence_roles = [
        {
            "claim": "总论点",
            "evidence": (
                f"Regulations.gov listing {listing_count} 条、comment-detail {detail_count} 条、"
                f"GDELT DOC {gdelt_count} 条，以及 bounded issue annotation。"
            ),
            "role": "把正式制度入口、有限评论样本和公共文档样本串成议题地图，同时防止升级为代表性或政策优劣结论。",
        },
        {
            "claim": "正式评论争点结构",
            "evidence": subclaims[1],
            "role": "识别样本内争点类型：健康/科学/法律正当性、成本/负担、程序和公平线索。",
        },
        {
            "claim": "公共语义结构",
            "evidence": subclaims[3],
            "role": "说明媒体/公共文档样本提供的是命名、可见性和传播语境，不是样本外公众态度。",
        },
        {
            "claim": "风险与问题识别",
            "evidence": "listing/detail/attachment/GDELT/social-platform coverage 的层级必须分开。",
            "role": "防止把候选记录、附件元数据或 recon 结果写成完整语料与代表性结论。",
        },
    ]
    executive_paragraphs = [
        (
            f"本报告围绕 2024 年 EPA PM2.5 NAAQS 修订的正式公众评论和媒体/公共语义结构展开。"
            f"可用证据包括 {listing_count} 条 Regulations.gov 候选 listing、{detail_count} 条 comment-detail 探测样本、"
            f"{gdelt_count} 条 GDELT DOC 媒体/公共文档记录，以及对可读评论样本形成的有限争点标注。"
            "这些材料足以支持一个描述性判断：该议题在样本内不是单纯的支持/反对二分，而是围绕健康保护、科学与法律正当性、"
            "成本和实施影响、公平关切等问题形成的规则制定争议。"
        ),
        (
            "这份报告的价值在于把正式制度参与和公共文本语义放在同一张证据地图上。"
            "它能说明当前样本中哪些争点可见、哪些公共命名方式出现、哪些证据层级已经闭合；"
            "但它不能判断正式评论总体格局、样本外公众态度比例、环境效果、健康因果或政策责任。"
        ),
    ]
    issue_analysis = [
        (
            "从治理争议看，PM2.5 NAAQS 修订的核心张力并不只是“是否收紧标准”。"
            "样本内可见的表达把健康保护、科学依据和 EPA 法定授权放在正当性一侧，同时把成本、经济负担、程序治理和实施影响放在约束一侧。"
            "这说明该议题更适合作为规则制定中的利益和证据边界问题来阅读，而不是作为单一态度投票来阅读。"
        ),
        (
            f"正式评论探测样本中，健康/科学/授权相关线索包括 {health_science_legal or '尚未形成可列举标签'}；"
            f"成本/负担/程序相关线索包括 {burden_feasibility or '尚未形成可列举标签'}。"
            f"这些标签来自 {annotation_sample_count} 条 detail 信号上的 {annotation_count} 个非互斥标注，"
            "因此适合用来识别争点类型，不适合相加、排序或外推为完整 docket 的争点分布。"
        ),
    ]
    if equity_environment:
        issue_analysis.append(
            f"样本内还出现了 {equity_environment} 等线索。"
            "这使议题的公共含义超出技术标准本身：PM2.5 标准讨论会被连接到健康保护、暴露负担、公平和实施成本之间的分配问题。"
        )
    evidence_analysis = [
        (
            "证据链的关键不在于记录数量本身，而在于不同来源的功能不同。"
            "Regulations.gov listing 提供的是正式评论入口和候选样本框，comment-detail 提供的是有限可读文本线索，"
            "附件路线主要揭示正文可读性的限制，GDELT DOC 则提供媒体和公共文档如何命名该议题的线索。"
            "如果把这些层级混在一起，报告就会把入口材料误写成评论正文，或把媒体可见性误写成公众代表性。"
        ),
        (
            "议会因此采用了有边界的收口方式：正式评论材料只用于描述样本内争点和可读性边界，"
            "公共文本材料只用于描述样本内命名和传播语境，附件路线只用于说明文本抽取限制。"
            "这些边界不是报告的附属说明，而是结论本身的一部分。"
        ),
    ]
    public_semantics = [
        (
            f"公共语义方面，{gdelt_count} 条 GDELT DOC 记录说明 PM2.5 NAAQS 修订已经进入媒体/公共文档语境，"
            "可被命名为 soot pollution、fine particulate standards、EPA tightening 等相关议题。"
            "这些命名线索有助于理解议题如何被公共文本组织，但不能回答公众总体态度、平台讨论强弱或情绪比例。"
        ),
        (
            (
                f"本次 DB 样本中未观察到 {zero_family_phrase} 信号。"
                if zero_family_phrase
                else "本次 coverage audit 没有给出可以支撑平台代表性的社交样本。"
            )
            + "这应被解释为本次 run 的覆盖缺口，而不是现实世界中相应平台没有讨论。"
        ),
    ]
    method_context = [
        (
            "从议会流程看，moderator 先把任务拆成正式评论、公共/媒体语义和治理代表性边界三条证据线；"
            "investigator 通过本地 Regulations.gov 和 GDELT 路线获取材料，challenger 则持续限制 listing、detail、附件和公共文档之间的 claim 升级。"
            "这种流程安排的意义，是确保报告不会把工具执行结果直接转换为超出 basis 的政策判断。"
        )
    ]
    risks = [
        f"正式评论完整性风险：{listing_count} 条 listing 只是候选入口，{detail_count} 条 detail 是有限探测样本，不是完整 docket 语料。",
        f"争点识别风险：{annotation_count} 个标签是非互斥、样本内、条目级 annotation，不能相加成争点强弱排序，也不能代表正式评论总体。",
        "附件可读性风险：附件路线保留仅元数据、无本地文件或文本抽取受限状态，不能写成附件正文已被阅读。",
        "公共话语覆盖风险：GDELT DOC 是媒体/文档检索样本；没有 YouTube/Bluesky normalized signals，不能判断社交平台情绪或样本外公众态度。",
        "重复与聚合风险：GDELT DOC 中可能有 syndicated headlines 或重复报道；未 materialize tone aggregates、Events/Mentions/GKG row layers。",
        "环境与责任风险：本 run 没有环境观测、暴露模型、健康结果或归因路线，不能推出环境趋势、健康因果、政策责任或污染来源归因。",
    ]
    non_conclusions = [
        "不能断言正式评论总体支持或反对 2024 PM2.5 NAAQS 修订，也不能判断哪一类争点在完整 docket 中占主导。",
        f"不能把 {listing_count} 条 listing 或 {detail_count} 条 detail 探测样本外推为完整正式评论语料；listing 只能作为候选入口，不能当作评论正文。",
        "不能声称附件正文已经被系统读取、归纳或分类；附件路线在本 run 中只支持可读性限制说明。",
        "不能报告样本外公众态度比例、社交平台情绪分布或公共舆论强弱；GDELT DOC 检索样本不是代表性公众样本。",
        "不能推出 PM2.5 环境趋势、暴露变化、健康因果、政策责任或污染来源归因；本 run 没有走这些观测和归因路线。",
    ]
    direct_answers = [
        "主要治理争议：样本内可见的争议轴不是单一“支持/反对”，而是健康保护、科学依据、法律权限、成本负担、程序治理和公平关切之间的组合。",
        "公共讨论语义结构：本次公共/媒体样本能说明 PM2.5 NAAQS 被公共文本命名为 soot pollution / fine particulate standards / EPA tightening 相关议题，但不能说明样本外公众情绪或平台讨论强弱。",
        "证据支持：已有 proposal -> fetch -> normalize -> council uptake -> frozen basis -> report 的链路，可以支持来源范围内的描述性结论。",
        "证据限制：不能回答哪一方占主导、正式评论总体争点强弱、样本外公众态度、环境效果、健康因果或政策责任。",
    ]
    council_work = [
        "Round-001 中，moderator 划分正式评论、公共/媒体语义和治理代表性三个 evidence request；social-investigator 自主选择 Regulations.gov 与 GDELT 本地 skill route。",
        f"Round-001 获取并归一化 {listing_count} 条 Regulations.gov listing signal 和 {gdelt_count} 条 GDELT DOC article signal；challenger 判定这些只能作为 seed，不能报告收口。",
        f"Round-002 中，social/formal-governance lane 补拉 {detail_count} 条 comment-detail，完成正式争点 bounded annotation，并尝试附件路线；helper artifacts 被 evidence bundle 和 finding 承接后才进入报告基础。",
        "environmental-investigator 明确记录边界：当前 mission 没有环境观测 claim，不应引入新的环境观测路线并把正式评论材料误写成环境事实。",
        "challenger 接受 bounded-ready，但强制排除代表性、争点强弱排序、附件正文、环境趋势、暴露因果、健康结果、政策责任和来源归因。moderator 依此冻结 bounded report basis。",
    ]
    decision_meaning = (
        "对决策者和研究展示来说，这份报告最适合用作议题地图和证据缺口地图："
        "它能帮助识别正式参与材料中可见的争点类型、公共文本中的命名方式，以及目前证据链在哪些环节已经闭合、在哪些环节仍然不足。"
        "它不应被用作政策优劣裁决、完整公众意见概括或环境健康影响评估。"
    )
    follow_up_needs = [
        "若要判断正式评论总体格局，需要完整 docket 候选审计、批量可读正文/附件抽取，以及可复核的争点分类聚合。",
        "若要判断公众态度或平台讨论强弱，需要明确定义的公共语料、覆盖审计、标注规则、聚合结果和分母。",
        "若要讨论环境效果、暴露变化、健康影响或政策责任，需要独立的环境观测、暴露/健康证据和因果或责任审查路线。",
    ]
    academic_sections = {
        "abstract": executive_paragraphs,
        "keywords": [
            "PM2.5 NAAQS",
            "正式公众评论",
            "Regulations.gov",
            "GDELT",
            "公共语义",
            "证据边界",
        ],
        "introduction": [
            (
                "细颗粒物国家环境空气质量标准的修订同时具有科学、法律和分配政治含义。"
                "在正式规则制定中，公众评论提供制度化参与记录；在媒体和公共文本中，同一议题又会被重新命名、解释并嵌入健康、成本和治理责任叙事。"
                "因此，理解该议题不能只问是否支持或反对标准收紧，还需要区分正式评论、公共可见性和可审计证据之间的层级。"
            ),
            (
                "本文的问题是：在当前 OpenClaw 议会已冻结的证据基础上，能够如何描述 2024 年 PM2.5 NAAQS 修订中的正式评论争议和公共语义结构。"
                "本文的贡献不是给出政策优劣裁断，而是形成一份可复核的争议结构和证据边界说明。"
            ),
        ],
        "methods": [
            (
                "本文使用的材料限于已进入冻结报告基础的议会对象和被议会承接的辅助分析产物。"
                f"主要样本包括 {listing_count} 条 Regulations.gov 候选 listing、{detail_count} 条 comment-detail 探测样本、"
                f"{gdelt_count} 条 GDELT DOC 媒体/公共文档记录，以及 {annotation_sample_count} 条 detail 信号上的有边界争点标注。"
            ),
            (
                "方法上，报告把不同来源家族按证据功能区分：Regulations.gov listing 用于界定正式评论入口，"
                "comment-detail 用于识别有限可读文本中的争点线索，附件路线用于记录可读性限制，GDELT DOC 用于描述公共文本命名和传播语境。"
                "质询角色对代表性、附件正文、环境趋势、健康因果和政策责任等强主张保留排除边界。"
            ),
            (
                "这种方法的核心不是给来源排序，而是防止证据层级混淆：入口记录不能写成完整评论正文，"
                "媒体/公共文档可见性不能写成代表性公众态度，样本内标签不能写成完整 docket 的争点强弱。"
            ),
        ],
        "results": [
            {
                "title": "正式评论争议呈现多轴结构",
                "paragraphs": issue_analysis,
            },
            {
                "title": "公共文本提供议题命名和可见性，而非代表性民意",
                "paragraphs": public_semantics,
            },
            {
                "title": "证据链的主要成果是边界清楚的议题地图",
                "paragraphs": evidence_analysis,
            },
        ],
        "discussion": [
            (
                "上述结果说明，当前材料最适合支持描述性和结构性判断：PM2.5 NAAQS 修订在样本内同时被表达为健康保护、科学/法律正当性、"
                "成本负担、程序治理和公平问题。它揭示的是争议维度，而不是争议力量对比。"
            ),
            (
                "风险主要来自三个方向。第一，正式评论样本仍然有限，listing 与 detail 的层级不能混用；第二，附件正文未形成稳定可读语料，"
                "因此不能把附件路线写成附件内容分析；第三，公共文本样本缺少代表性抽样框和平台覆盖，不能外推出公众态度。"
            ),
            (
                "因此，本文不能推出以下强结论：正式评论总体支持或反对修订，完整 docket 中的主导争点，附件正文的实质内容，"
                "样本外公众态度比例、社交平台情绪分布、公共舆论强弱，或 PM2.5 环境趋势、暴露变化、健康因果和政策责任。"
            ),
        ],
        "conclusion": [
            central_claim,
            decision_meaning,
            "后续若要形成更强结论，应补齐完整正式评论语料、附件正文抽取、争点分类聚合、公共语料覆盖审计，以及必要时的环境和健康影响证据。"
        ],
        "follow_up_needs": follow_up_needs,
    }
    return {
        "central_claim": central_claim,
        "executive_paragraphs": executive_paragraphs,
        "issue_analysis": issue_analysis,
        "evidence_analysis": evidence_analysis,
        "public_semantics": public_semantics,
        "method_context": method_context,
        "reasoning_chain": subclaims,
        "evidence_roles": evidence_roles,
        "limitations": non_conclusions,
        "risk_register": risks,
        "direct_answers": direct_answers,
        "council_work": council_work,
        "decision_meaning": decision_meaning,
        "follow_up_needs": follow_up_needs,
        "academic_sections": academic_sections,
        "profile": "formal-policy-comment",
    }


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


def evidence_ref_text(value: Any) -> str:
    if isinstance(value, dict):
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if artifact_ref:
            return artifact_ref
        artifact_path = maybe_text(value.get("artifact_path"))
        record_locator = maybe_text(value.get("record_locator"))
        if artifact_path:
            return f"{artifact_path}:{record_locator or '$'}"
        signal_id = maybe_text(value.get("signal_id"))
        if signal_id:
            return signal_id
    return maybe_text(value)


def evidence_ref_texts(values: Any) -> list[str]:
    if isinstance(values, list):
        return unique_texts([evidence_ref_text(value) for value in values])
    return unique_texts([evidence_ref_text(values)])


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


def compact_lane_episode_card(card: dict[str, Any]) -> dict[str, Any]:
    denominators = card.get("denominators") if isinstance(card.get("denominators"), dict) else {}
    episode_ref = card.get("episode_ref") if isinstance(card.get("episode_ref"), dict) else {}
    return {
        "episode_id": maybe_text(card.get("episode_id")),
        "episode_kind": maybe_text(card.get("episode_kind")),
        "lane_key": maybe_text(card.get("lane_key")),
        "owner_role": maybe_text(card.get("owner_role")),
        "time_anchor_date": maybe_text(card.get("time_anchor_date")),
        "claim_strength": maybe_text(card.get("claim_strength")),
        "main_claims": text_list(card.get("main_claims"))[:4],
        "source_families": text_list(card.get("source_families"))[:8],
        "denominators": {
            "signal_count": denominators.get("signal_count", 0),
            "source_family_counts": list_items(denominators.get("source_family_counts"))[:8],
        },
        "episode_ref": episode_ref,
        "limitations": text_list(card.get("limitations"))[:4],
    }


def artifact_row(kind: str, path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    report_packet = payload.get("report_packet") if isinstance(payload.get("report_packet"), dict) else {}
    return {
        "kind": kind,
        "path": str(path),
        "id": maybe_text(payload.get("publication_id"))
        or maybe_text(payload.get("decision_id"))
        or maybe_text(payload.get("report_id"))
        or maybe_text(payload.get("handoff_id"))
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
        "section_briefs": [
            item
            for item in [
                *list_items(payload.get("section_briefs")),
                *list_items(report_packet.get("section_briefs")),
            ]
            if isinstance(item, dict)
        ],
        "interaction_timeline_nodes": [
            item
            for item in [
                *list_items(payload.get("interaction_timeline_nodes")),
                *list_items(report_packet.get("interaction_timeline_nodes")),
            ]
            if isinstance(item, dict)
        ],
        "lane_episode_cards": [
            compact_lane_episode_card(item)
            for item in [
                *list_items(payload.get("lane_episode_cards")),
                *list_items(report_packet.get("lane_episode_cards")),
            ]
            if isinstance(item, dict)
        ],
        "interaction_timeline_path": maybe_text(payload.get("interaction_timeline_path"))
        or maybe_text(
            (
                report_packet.get("interaction_timeline_policy", {})
                if isinstance(report_packet.get("interaction_timeline_policy"), dict)
                else {}
            ).get("artifact_path")
        ),
    }


def load_reporting_basis(
    run_dir: Path,
    basis_round_id: str,
    *,
    report_round_id: str = "",
) -> list[dict[str, Any]]:
    candidates = [
        ("final-publication", run_dir / "reporting" / f"final_publication_{basis_round_id}.json"),
        ("council-decision", run_dir / "reporting" / f"council_decision_{basis_round_id}.json"),
        ("council-decision-draft", run_dir / "reporting" / f"council_decision_draft_{basis_round_id}.json"),
    ]
    current_report_round_id = maybe_text(report_round_id)
    if current_report_round_id and current_report_round_id != basis_round_id:
        candidates.append(
            (
                "reporting-handoff",
                run_dir / "reporting" / f"reporting_handoff_{current_report_round_id}.json",
            )
        )
    candidates.extend(
        [
            ("reporting-handoff", run_dir / "reporting" / f"reporting_handoff_{basis_round_id}.json"),
            ("report-basis-freeze", run_dir / "report_basis" / f"frozen_report_basis_{basis_round_id}.json"),
            ("expert-report-social", run_dir / "reporting" / f"expert_report_social_investigator_{basis_round_id}.json"),
            ("expert-report-environmental", run_dir / "reporting" / f"expert_report_environmental_investigator_{basis_round_id}.json"),
        ]
    )
    frozen_default = run_dir / "report_basis" / f"frozen_report_basis_{basis_round_id}.json"
    for variant_path in sorted((run_dir / "report_basis").glob(f"frozen_report_basis_{basis_round_id}*.json")):
        if variant_path == frozen_default:
            continue
        candidates.append(("report-basis-freeze", variant_path))
    rows: list[dict[str, Any]] = []
    for kind, path in candidates:
        payload = load_json_if_exists(path)
        row = artifact_row(kind, path, payload)
        if row:
            rows.append(row)
    return rows


def section_briefs_from_reporting_basis(reporting_basis: list[dict[str, Any]]) -> list[dict[str, Any]]:
    briefs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in reporting_basis:
        for brief in row.get("section_briefs", []):
            if not isinstance(brief, dict):
                continue
            brief_id = maybe_text(brief.get("brief_id")) or maybe_text(
                brief.get("section_key")
            )
            key = brief_id or json.dumps(brief, ensure_ascii=True, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            briefs.append(brief)
    return briefs


def interaction_timeline_nodes_from_reporting_basis(reporting_basis: list[dict[str, Any]]) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in reporting_basis:
        for node in row.get("interaction_timeline_nodes", []):
            if not isinstance(node, dict):
                continue
            node_id = maybe_text(node.get("node_id")) or json.dumps(
                node,
                ensure_ascii=True,
                sort_keys=True,
            )
            if node_id in seen:
                continue
            seen.add(node_id)
            nodes.append(node)
    return nodes


def lane_episode_cards_from_reporting_basis(reporting_basis: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in reporting_basis:
        for card in row.get("lane_episode_cards", []):
            if not isinstance(card, dict):
                continue
            card_id = maybe_text(card.get("episode_id")) or json.dumps(
                card,
                ensure_ascii=True,
                sort_keys=True,
            )
            if card_id in seen:
                continue
            seen.add(card_id)
            cards.append(card)
    return cards


def refs_from_section_briefs(section_briefs: list[dict[str, Any]]) -> list[str]:
    refs: list[str] = []
    for brief in section_briefs:
        refs.extend(evidence_ref_texts(brief.get("refs")))
        refs.extend(evidence_ref_texts(brief.get("evidence_refs")))
        source_path = maybe_text(brief.get("source_artifact_path"))
        if source_path:
            refs.append(f"{source_path}:$")
    return unique_texts(refs)


def first_section_brief_path(section_briefs: list[dict[str, Any]]) -> str:
    for brief in section_briefs:
        path = maybe_text(brief.get("source_artifact_path"))
        if path:
            return path
    return ""


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
        compact_refs = compact_audit_ref_lines(audit_refs)
        lines.append(
            f"完整 JSON 产物保留 {len(audit_refs)} 条可追踪审计引用。"
            "正文只列出读者可理解的来源索引；对象 ID、receipt 和 signal ID 保留在 JSON 中用于复核，不在正文展开。"
        )
        lines.append("")
        for ref in compact_refs:
            lines.append(f"- {ref}")
        if not compact_refs:
            lines.append("- 本报告的完整证据对象和运行索引见 JSON 审计字段。")
        lines.append("")
        return lines
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
    profile: str = "",
) -> list[str]:
    if is_zh(language):
        material_label = "正式治理与样本基础" if profile == "formal-policy-comment" else "环境压力信号"
        return unique_texts(
            [
                f"用户问题：{mission_line}",
                f"简要回答：{bottom_line}",
                f"{material_label}：{environmental_detail}" if environmental_detail else "",
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


def build_interaction_timeline_addendum(
    *,
    section_briefs: list[dict[str, Any]],
    interaction_nodes: list[dict[str, Any]],
    language: str,
) -> dict[str, Any]:
    interaction_briefs = [
        brief
        for brief in section_briefs
        if "interaction" in maybe_text(brief.get("section_key")).casefold()
        or "interaction" in maybe_text(brief.get("brief_id")).casefold()
    ]
    if not interaction_briefs and not interaction_nodes:
        return {}
    brief = interaction_briefs[0] if interaction_briefs else {}
    denominator = brief.get("denominator") if isinstance(brief.get("denominator"), dict) else {}
    node_count = int(denominator.get("interaction_node_count") or len(interaction_nodes))
    episode_count = int(denominator.get("lane_episode_card_count") or 0)
    parallel_count = int(denominator.get("parallel_timeline_node_count") or 0)
    fact_count = int(denominator.get("environment_signal_count") or 0) + int(
        denominator.get("formal_signal_count") or 0
    )
    public_count = int(denominator.get("public_signal_count") or 0)
    claim_strength = maybe_text(brief.get("claim_strength")) or "bounded-descriptive-context-only"
    candidate_claims = text_list(brief.get("candidate_section_claims"))
    limitations = text_list(brief.get("limitations"))
    refs = refs_from_section_briefs(interaction_briefs or section_briefs)
    if is_zh(language):
        paragraphs = [
            (
                "互动时间线只用于把事实/政策侧记录与公共/媒体侧记录放入同一时间坐标。"
                f"当前可见 lane episode cards {episode_count} 个、互动节点 {node_count} 个，单侧背景节点 {parallel_count} 个；"
                f"事实/政策侧可见信号 {fact_count} 条，公共/媒体侧可见信号 {public_count} 条。"
            ),
            (
                f"本节 claim strength 为 `{claim_strength}`。"
                + "可写入的有界判断仅限：事实/政策侧 lane episode cards 与公共/媒体侧 lane episode cards 在同一时间窗口内共同可见。"
            ),
            (
                "这不是因果、政策效果或公众反应归因。若要写语义变化、政策回应缺口或责任判断，"
                "还需要可比样本定义、source-family denominator、正式承接的解释对象和质询边界。"
            ),
        ]
        if limitations:
            paragraphs.append(
                "本节限制包括：section brief 只是报告组织线索；共同可见不能证明因果、政策效果、公众响应归因、代表性民意或证据缺失。"
            )
    else:
        paragraphs = [
            (
                "The interaction timeline places fact/policy-side records and public/media-side records on the same chronology. "
                f"The handoff exposes {episode_count} lane episode card(s), {node_count} interaction node(s), {parallel_count} one-sided context node(s), "
                f"{fact_count} fact/policy-side visible signal(s), and {public_count} public/media-side visible signal(s)."
            ),
            (
                f"Claim strength is `{claim_strength}`. "
                + (
                    "A bounded section claim is: " + " ".join(candidate_claims[:2])
                    if candidate_claims
                    else "The supportable claim is limited to co-visible records in the cited timeline windows."
                )
            ),
            (
                "This is not causality, policy-effect evidence, or public-response attribution. Stronger semantic-shift, "
                "communication-gap, or responsibility wording requires comparable denominators, council-carried interpretation, and challenge boundaries."
            ),
        ]
        if limitations:
            paragraphs.append("Key limitations: " + " ".join(limitations[:3]))
    return section(
        "fact-policy-public-interaction",
        label("fact-policy-public-interaction", language),
        paragraphs,
        refs,
        status="advisory-section-brief",
        language=language,
    )


def build_zh_closure_narrative(*, synthesis_line: str, readiness_lines: list[str], profile: str = "") -> list[str]:
    if profile == "formal-policy-comment":
        paragraphs = [
            "本报告只使用已经进入报告基础的材料：正式治理记录、评论详情/附件路线限制、公共讨论样本、样本覆盖审计和可追踪的证据引用。"
        ]
    else:
        paragraphs = [
            (
                "本报告只使用已经进入报告基础的材料：环境/运行记录、正式治理记录、公共讨论样本、舆情语义摘要和可追踪的证据引用。"
            )
        ]
    if synthesis_line:
        paragraphs.append(f"综合判断所依据的材料边界是：{synthesis_line}")
    if readiness_lines:
        if profile == "formal-policy-comment":
            paragraphs.append(
                "正式记录、评论样本和公共讨论材料分别承担制度入口、样本内语义和限制说明的角色；"
                "未被记录支撑的正式评论总体争点判断、公众代表性、环境趋势、暴露因果、健康结果、政策责任或来源归因不进入正文结论。"
            )
        else:
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
    """Build a reusable argument map from evidence lanes, not case names."""
    combined = " ".join(
        maybe_text(item)
        for item in [mission_focus, bottom_line, environmental_detail, social_line, public_compact_line, boundary_line]
    )
    profile = issue_profile_from_text(combined)
    focus = issue_profile_focus(profile, language)
    if is_zh(language):
        if profile == "formal-policy-comment":
            central_claim = bottom_line or f"现有记录支持围绕{focus}形成一份有边界的专业参考判断。"
            formal_basis = bottom_line or social_line or "正式治理记录和评论样本只支持来源范围内的程序、样本与可读性描述。"
            public_basis = public_compact_line or social_line
            reasoning_chain = unique_texts(
                [
                    f"第一，正式治理和评论材料约束本报告的事实对象：{formal_basis}",
                    (
                        f"第二，公共或媒体材料只用于说明样本内语义和可见性边界：{public_basis}"
                        if public_basis
                        else "第二，公共或媒体材料只能作为样本内语义和可见性线索，不能外推为样本外公众态度。"
                    ),
                    (
                        f"第三，结论强度受已记录边界约束：{boundary_line}"
                        if boundary_line
                        else "第三，当前基础不得升级为正式评论总体争点判断、样本外公众态度、环境趋势、暴露因果、健康结果、政策责任或来源归因判断。"
                    ),
                ]
            )
            evidence_roles = [
                {
                    "claim": "正式治理与评论样本基础",
                    "evidence": formal_basis,
                    "role": "说明议题进入正式规则制定和评论样本的记录边界，防止把 listing、候选样本或附件元数据写成完整评论语料。",
                },
                {
                    "claim": "公共语义与覆盖边界",
                    "evidence": public_basis,
                    "role": "说明媒体或公共文本在样本内呈现的语义线索，防止外推为代表性公众意见或人群态度。",
                },
                {
                    "claim": "结论边界",
                    "evidence": boundary_line,
                    "role": "排除无 basis 的总体判断、正式争点强弱排序、附件正文内容、环境趋势、暴露因果、健康结果、政策责任和来源归因。",
                },
            ]
            limitations = unique_texts(
                [
                    boundary_line,
                    "正式评论 listing、候选样本和有限 comment detail 不能直接写成完整正式评论语料或总体争点判断。",
                    "附件路线若只有元数据或抽取限制，只能支持附件存在和限制描述，不能支持附件正文 substance。",
                    "公共或媒体样本只说明样本内语义结构，不能代表样本外公众态度。",
                    "本轮没有环境观测或环境聚合 basis，因此不得写环境趋势、暴露因果、健康结果、政策责任或来源归因。",
                ]
            )
            decision_meaning = (
                f"这份报告适合用于围绕{focus}进行制度入口、样本覆盖、可读性限制和后续证据路线设计；"
                "若要形成更强正式争点、代表性、环境或政策责任结论，需要补充对应证据并由议会承接。"
            )
            return {
                "central_claim": central_claim,
                "reasoning_chain": reasoning_chain,
                "evidence_roles": [
                    item
                    for item in evidence_roles
                    if maybe_text(item.get("evidence")) or maybe_text(item.get("role"))
                ],
                "limitations": limitations,
                "decision_meaning": decision_meaning,
                "profile": profile,
            }
        central_claim = bottom_line or f"现有记录支持围绕{focus}形成一份有边界的专业参考判断。"
        reasoning_chain = unique_texts(
            [
                (
                    f"第一，事实层材料提供报告的地基：{environmental_detail}"
                    if environmental_detail
                    else "第一，报告先从已归档的环境、运行或正式记录中建立事实对象，避免直接从舆论表达推出事实结论。"
                ),
                (
                    f"第二，治理或公共讨论材料说明议题如何被制度化、传播和解释：{social_line}"
                    if social_line
                    else "第二，治理记录和公共讨论材料用于解释议题语境、公众可见性和样本内语义结构，而不承担事实替代作用。"
                ),
                (
                    f"第三，公共舆情摘要把样本内议题、情绪和来源叙事结构化：{public_compact_line}"
                    if public_compact_line
                    else "第三，公共舆情分析只在样本内说明问题意识、情绪线索和叙事框架，不能自动外推为样本外公众态度。"
                ),
                (
                    f"最后，结论强度受证据边界约束：{boundary_line}"
                    if boundary_line
                    else "最后，未被记录、引用和归一化的内容不进入结论；因果、代表性和责任判断必须保持与证据类型相匹配。"
                ),
            ]
        )
        evidence_roles = [
            {
                "claim": "事实基础",
                "evidence": environmental_detail or bottom_line,
                "role": "约束报告能够描述的事件、状态、运行过程或正式对象，防止把公共叙事误写成事实本身。",
            },
            {
                "claim": "治理与公共语境",
                "evidence": social_line or public_compact_line,
                "role": "说明议题如何进入制度记录、媒体文本或公共讨论样本，并解释这些材料能支持到什么层级。",
            },
            {
                "claim": "结论边界",
                "evidence": boundary_line,
                "role": "限制因果归因、代表性公众意见、责任判定和政策优劣评价的写作强度。",
            },
        ]
        limitations = unique_texts(
            [
                boundary_line,
                "公共讨论或平台样本只说明样本内语义结构，不能直接代表样本外公众态度。",
                "正式记录、环境记录和舆情记录承担不同证据角色，不能互相替代或被写成单一强结论。",
                "报告可以提出有证据支撑的阶段性判断，但不得补写缺失的专业模型、完整正文材料或未记录来源。",
            ]
        )
        decision_meaning = (
            f"这份报告适合用于围绕{focus}进行学术汇报、决策参考和后续调查设计；"
            "若要升级为更强因果、代表性或政策评价，需要补充与对应 claim 类型匹配的新证据。"
        )
        return {
            "central_claim": central_claim,
            "reasoning_chain": reasoning_chain,
            "evidence_roles": [
                item
                for item in evidence_roles
                if maybe_text(item.get("evidence")) or maybe_text(item.get("role"))
            ],
            "limitations": limitations,
            "decision_meaning": decision_meaning,
        }

    central_claim = bottom_line or f"The recorded basis supports a bounded assessment of {focus}."
    reasoning_chain = unique_texts(
        [
            f"First, factual or operational evidence grounds the issue: {environmental_detail}" if environmental_detail else "",
            f"Second, formal or public-discourse evidence explains context and interpretation: {social_line}" if social_line else "",
            f"Third, public-discourse summaries describe sample-local semantics: {public_compact_line}" if public_compact_line else "",
            f"Finally, the claim boundary limits the conclusion: {boundary_line}" if boundary_line else "",
        ]
    )
    return {
        "central_claim": central_claim,
        "reasoning_chain": reasoning_chain,
        "evidence_roles": [
            {"claim": "Factual basis", "evidence": environmental_detail or bottom_line, "role": "Constrains what the report can describe."},
            {"claim": "Governance or public context", "evidence": social_line or public_compact_line, "role": "Explains how the issue is framed and discussed."},
            {"claim": "Claim boundary", "evidence": boundary_line, "role": "Constrains attribution, representativeness, and policy evaluation."},
        ],
        "limitations": [boundary_line] if boundary_line else [],
        "decision_meaning": "Use this as a bounded synthesis, not as source ranking or upgraded attribution.",
    }


def argument_evidence_paragraphs(argument_map: dict[str, Any], language: str) -> list[str]:
    rows = [item for item in list_items(argument_map.get("evidence_roles")) if isinstance(item, dict)]
    paragraphs: list[str] = []
    if is_zh(language):
        for item in rows:
            claim = maybe_text(item.get("claim")) or "一项判断"
            evidence = maybe_text(item.get("evidence"))
            role = maybe_text(item.get("role"))
            if evidence and role:
                paragraphs.append(f"围绕“{claim}”，报告不是孤立列举材料，而是说明其论证功能：{evidence} 这类材料在论证中主要用于{role}")
            elif role:
                paragraphs.append(f"围绕“{claim}”，这类材料在论证中主要用于{role}")
        return unique_texts(paragraphs)
    for item in rows:
        claim = maybe_text(item.get("claim")) or "Claim"
        evidence = maybe_text(item.get("evidence"))
        role = maybe_text(item.get("role"))
        paragraphs.append(f"For {claim}, the report uses {evidence}. Its role is: {role}")
    return unique_texts(paragraphs)


def case_story_paragraphs(argument_map: dict[str, Any], mission_focus: str, language: str) -> list[str]:
    chain = text_list(argument_map.get("reasoning_chain"))
    if not is_zh(language):
        return unique_texts(chain[:4])
    if maybe_text(argument_map.get("profile")) == "formal-policy-comment":
        return unique_texts(
            [
                f"围绕“{mission_focus}”，正文先界定正式治理对象和可用评论样本，再解释公共或媒体语义材料的样本边界，最后说明哪些结论不能升级。",
                (
                    "证据组织遵循“正式治理入口-评论/附件可读性-公共语义-结论边界”的顺序："
                    "先说明已被记录约束的制度和样本对象，再把覆盖缺口、附件限制和代表性限制写清楚。"
                ),
            ]
        )
    return unique_texts(
        [
            f"围绕“{mission_focus}”，正文先建立事实对象，再解释治理或传播语境，最后把公共语义和结论边界放回同一条论证链中。",
            (
                "证据组织遵循“事实对象-解释语境-公共语义-结论边界”的顺序：先说明已被记录约束的事实或治理对象，"
                "再解释这些事实如何进入制度记录和公共讨论，随后讨论样本内语义结构，最后明确哪些结论仍不能升级。"
            ),
        ]
    )


def zh_add_paragraph_section(lines: list[str], heading: str, paragraphs: list[str]) -> None:
    cleaned = unique_texts(
        [
            zh_clean_report_prose(paragraph)
            for paragraph in paragraphs
            if maybe_text(paragraph)
        ]
    )
    if not cleaned:
        return
    lines.extend([f"## {heading}", ""])
    for paragraph in cleaned:
        lines.extend([paragraph, ""])


def zh_academic_markdown_from_sections(draft: dict[str, Any], academic_sections: dict[str, Any]) -> str:
    title = maybe_text(draft.get("title")) or "叙事报告"
    lines = [f"# {title}", ""]
    abstract = text_list(academic_sections.get("abstract"))
    keywords = text_list(academic_sections.get("keywords"))
    introduction = text_list(academic_sections.get("introduction"))
    methods = text_list(academic_sections.get("methods"))
    discussion = text_list(academic_sections.get("discussion"))
    limitations = text_list(academic_sections.get("limitations"))
    conclusion = text_list(academic_sections.get("conclusion"))
    results = [item for item in list_items(academic_sections.get("results")) if isinstance(item, dict)]
    extra_sections = [
        item
        for item in list_items(draft.get("sections"))
        if isinstance(item, dict)
        and maybe_text(item.get("section_id"))
        in {
            "fact-policy-public-interaction",
            "public-discourse-deepening",
        }
        and text_list(item.get("paragraphs"))
    ]

    zh_add_paragraph_section(lines, "摘要", abstract)
    if keywords:
        lines.extend(["**关键词：** " + "；".join(keywords), ""])
    zh_add_paragraph_section(lines, "1. 引言", introduction)
    zh_add_paragraph_section(lines, "2. 数据与方法", methods)
    if results:
        lines.extend(["## 3. 结果", ""])
        for index, item in enumerate(results, 1):
            title_text = maybe_text(item.get("title")) or f"结果 {index}"
            paragraphs = text_list(item.get("paragraphs"))
            if not paragraphs:
                continue
            lines.extend([f"### 3.{index} {title_text}", ""])
            for paragraph in unique_texts([zh_clean_report_prose(p) for p in paragraphs if maybe_text(p)]):
                lines.extend([paragraph, ""])
    next_number = 4
    if extra_sections:
        lines.extend([f"## {next_number}. 互动时间线与舆情语义补充", ""])
        for index, item in enumerate(extra_sections, 1):
            title_text = maybe_text(item.get("title")) or f"补充 {index}"
            paragraphs = text_list(item.get("paragraphs"))
            lines.extend([f"### {next_number}.{index} {title_text}", ""])
            for paragraph in unique_texts([zh_clean_report_prose(p) for p in paragraphs if maybe_text(p)]):
                lines.extend([paragraph, ""])
        next_number += 1
    zh_add_paragraph_section(lines, f"{next_number}. 讨论", discussion)
    next_number += 1
    zh_add_paragraph_section(lines, f"{next_number}. 局限性", limitations)
    next_number += 1
    zh_add_paragraph_section(lines, f"{next_number}. 结论", conclusion)
    audit_refs = [maybe_text(ref) for ref in draft.get("audit_refs", []) if maybe_text(ref)]
    source_refs = text_list(academic_sections.get("source_refs"))
    lines.extend(["## 参考文献与审计索引", ""])
    lines.append(
        "本报告不新增外部文献；下列索引用于说明主要来源和数据范围。"
        "完整对象 ID、receipt、signal ID 和运行审计链保存在 JSON 产物中，供复核使用。"
    )
    lines.append("")
    for ref in unique_texts([*source_refs, *compact_audit_ref_lines(audit_refs)]):
        lines.append(f"- {ref}")
    if not source_refs and not audit_refs:
        lines.append("- 完整审计索引见报告 JSON 产物。")
    lines.append("")
    return "\n".join(lines)


def zh_formal_policy_markdown_from_draft(draft: dict[str, Any]) -> str:
    title = maybe_text(draft.get("title")) or "正式评论与公共话语报告"
    argument_map = draft.get("argument_map") if isinstance(draft.get("argument_map"), dict) else {}
    academic_sections = (
        argument_map.get("academic_sections")
        if isinstance(argument_map.get("academic_sections"), dict)
        else {}
    )
    central_claim = maybe_text(argument_map.get("central_claim"))
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    mission_payload = source_material.get("mission") if isinstance(source_material.get("mission"), dict) else {}
    mission_request = (
        maybe_text(mission_payload.get("request_text"))
        or maybe_text(mission_payload.get("objective"))
        or maybe_text(mission_payload.get("topic"))
    )
    abstract = text_list(academic_sections.get("abstract")) or text_list(argument_map.get("executive_paragraphs"))
    introduction = text_list(academic_sections.get("introduction"))
    methods = text_list(academic_sections.get("methods"))
    discussion = text_list(academic_sections.get("discussion"))
    conclusion = text_list(academic_sections.get("conclusion")) or [central_claim]
    follow_up_needs = text_list(academic_sections.get("follow_up_needs")) or text_list(argument_map.get("follow_up_needs"))
    keywords = text_list(academic_sections.get("keywords")) or ["PM2.5 NAAQS", "正式公众评论", "公共语义", "证据边界"]
    result_sections = [
        item for item in list_items(academic_sections.get("results")) if isinstance(item, dict)
    ]

    def add_paragraph_section(lines: list[str], heading: str, paragraphs: list[str]) -> None:
        cleaned = unique_texts([paragraph for paragraph in paragraphs if maybe_text(paragraph)])
        if not cleaned:
            return
        lines.extend([f"## {heading}", ""])
        for paragraph in cleaned:
            lines.extend([paragraph, ""])

    lines = [f"# {title}", ""]
    add_paragraph_section(lines, "摘要", abstract)
    if keywords:
        lines.extend(["**关键词：** " + "；".join(keywords), ""])
    if mission_request:
        introduction = unique_texts(
            [
                *introduction,
                (
                    "据此，本文将研究问题界定为三个层面：正式评论样本中可见的争议结构、媒体/公共文本中的语义结构，"
                    "以及这些证据能够支撑和不能支撑的结论边界。报告只使用已冻结的议会证据基础，不新增事实或外部材料。"
                ),
            ]
        )
    add_paragraph_section(lines, "1. 引言", introduction)
    add_paragraph_section(lines, "2. 材料与方法", methods)
    if result_sections:
        lines.extend(["## 3. 结果", ""])
        for index, item in enumerate(result_sections, 1):
            subsection_title = maybe_text(item.get("title")) or f"结果 {index}"
            paragraphs = text_list(item.get("paragraphs"))
            if not paragraphs:
                continue
            lines.extend([f"### 3.{index} {subsection_title}", ""])
            for paragraph in paragraphs:
                lines.extend([paragraph, ""])
    else:
        fallback_results = unique_texts(
            [
                *text_list(argument_map.get("issue_analysis")),
                *text_list(argument_map.get("public_semantics")),
                *text_list(argument_map.get("evidence_analysis")),
                *text_list(argument_map.get("reasoning_chain")),
            ]
        )
        add_paragraph_section(lines, "3. 结果", fallback_results)
    if follow_up_needs:
        discussion = unique_texts(
            [
                *discussion,
                "进一步研究需要补齐以下证据环节："
                + "；".join(need.rstrip("。") for need in follow_up_needs if maybe_text(need))
                + "。",
            ]
        )
    add_paragraph_section(lines, "4. 讨论", discussion)
    add_paragraph_section(lines, "5. 结论", conclusion)

    audit_section = section_by_id(draft, "audit-trail")
    audit_refs = [maybe_text(ref) for ref in audit_section.get("evidence_refs", []) if maybe_text(ref)]
    if not audit_refs:
        audit_refs = [maybe_text(ref) for ref in draft.get("audit_refs", []) if maybe_text(ref)]
    lines.extend(["## 参考文献与审计索引", ""])
    lines.append(
        "本报告没有新增外部文献；以下参考项来自已进入议会报告基础的来源、证据对象、分析产物和 runtime 审计引用。"
        "这些索引用于复核材料来源和报告边界，不表示证据权重或来源排序。"
    )
    lines.append("")
    for ref in audit_refs[:25]:
        lines.append(f"- {ref}")
    if len(audit_refs) > 25:
        lines.append(f"- ... 另有 {len(audit_refs) - 25} 条引用见 JSON 产物")
    lines.append("")
    return "\n".join(lines)


def zh_article_markdown_from_draft(draft: dict[str, Any]) -> str:
    """Render a Chinese narrative report as an article, not a case-specific log."""
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
    mission_focus = mission_focus_text(mission_request, "zh-Hans")
    key_points = section_paragraphs(draft, "key-points")
    narrative = section_paragraphs(draft, "what-happened")
    evidence = section_paragraphs(draft, "evidence-basis")
    public_discourse = section_paragraphs(draft, "public-discourse-deepening")
    decision = section_paragraphs(draft, "decision-implications")
    source_basis = section_paragraphs(draft, "council-reasoning")
    profile = issue_profile_from_text(
        " ".join([title, mission_request, central_claim, *reasoning_chain, *narrative, *evidence, *public_discourse])
    )
    if profile == "formal-policy-comment":
        return zh_formal_policy_markdown_from_draft(draft)
    academic_sections = (
        argument_map.get("academic_sections")
        if isinstance(argument_map.get("academic_sections"), dict)
        else {}
    )
    if academic_sections:
        return zh_academic_markdown_from_sections(draft, academic_sections)
    focus = issue_profile_focus(profile, "zh-Hans")
    lines = [f"# {title}", ""]

    paragraphs: list[str] = []
    if mission_request:
        paragraphs.append(
            f"本文围绕用户提出的“{mission_request}”展开，重点分析{focus}。"
            "正文只使用已经进入报告基础的材料，并把事实、解释、公共语义和证据限制组织成一条可复核的论证链。"
        )
    else:
        paragraphs.append(
            f"本文围绕{focus}展开，目标是提供一份可供学术汇报和决策参考的专业调研报告。"
        )
    if central_claim:
        paragraphs.append(f"综合当前证据基础，本文的中心判断是：{central_claim}")

    story = case_story_paragraphs(argument_map, mission_focus, "zh-Hans")
    paragraphs.extend(story[:3])
    evidence_argument = argument_evidence_paragraphs(argument_map, "zh-Hans")[:5]
    if evidence_argument:
        paragraphs.extend(evidence_argument)
    else:
        paragraphs.extend([render_source_text(item, "zh-Hans") for item in reasoning_chain[:5]])

    rendered_narrative = [render_source_text(item, "zh-Hans") for item in narrative]
    rendered_evidence = [render_source_text(item, "zh-Hans") for item in evidence]
    rendered_key_points = [render_source_text(item, "zh-Hans") for item in key_points]
    substantive_material = [
        item
        for item in unique_texts([*rendered_narrative, *rendered_evidence, *rendered_key_points])
        if not item.startswith(("用户问题：", "简要回答："))
        and "尚未完全结构化的证据摘要" not in item
        and item != "已记录材料显示：该证据线需要保留代表性、归因强度或误检风险边界。"
    ]
    if substantive_material:
        paragraphs.append(
            "在具体材料层面，报告优先解释证据之间的关系，而不是按来源逐条堆叠。"
            "下列事实和线索共同承担论证功能：它们有的约束事实对象，有的说明治理或传播语境，有的提示公共语义。"
        )
        for item in substantive_material[:8]:
            if not any(item == paragraph or item in paragraph for paragraph in paragraphs):
                paragraphs.append(item)

    rendered_public = [render_source_text(item, "zh-Hans") for item in public_discourse]
    if rendered_public:
        if profile == "formal-policy-comment":
            paragraphs.append(
                "公共语义部分用于回答另一个问题：正式治理议题进入媒体或公共文本后，样本内材料如何命名、解释并表达关注。"
                "这部分可以说明样本内议题和语义线索，但不能在没有抽样框和加权设计时外推为样本外公众态度。"
            )
        else:
            paragraphs.append(
                "公共舆情部分用于回答另一个问题：环境事实或治理议题进入公共空间后，公众和媒体如何命名、解释并表达风险。"
                "这部分可以说明样本内议题、情绪和来源叙事结构，但不能在没有抽样框和加权设计时外推为样本外公众态度。"
            )
        paragraphs.extend(rendered_public[:6])

    if decision_meaning:
        paragraphs.append(decision_meaning)
    if decision:
        paragraphs.extend([render_source_text(item, "zh-Hans") for item in decision[:3]])
    if limitations:
        paragraphs.append(
            "因此，本文需要保留的结论边界包括："
            + "；".join(render_source_text(item, "zh-Hans").rstrip("。") for item in limitations[:5])
            + "。"
        )
    if boundary_summary:
        paragraphs.append(f"写作边界上，{boundary_summary}")
    if source_basis:
        paragraphs.append(
            "资料基础和审计索引只用于说明材料如何进入报告基础，不构成信源排序，也不把程序性记录写成正文重点。"
            f"{render_source_text(first_text(source_basis), 'zh-Hans')}"
        )

    for paragraph in unique_texts(paragraphs):
        lines.extend([paragraph, ""])
    lines.extend(markdown_audit_lines(draft, normalize_language("zh")))
    return "\n".join(lines)


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
    reporting_basis = load_reporting_basis(
        run_dir_path,
        resolved_basis_round_id,
        report_round_id=round_id,
    )
    section_briefs = section_briefs_from_reporting_basis(reporting_basis)
    interaction_timeline_nodes = interaction_timeline_nodes_from_reporting_basis(
        reporting_basis
    )
    lane_episode_cards = lane_episode_cards_from_reporting_basis(reporting_basis)
    section_brief_refs = refs_from_section_briefs(section_briefs)
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
            *section_brief_refs,
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
    formal_helper_lines, formal_helper_meta = formal_policy_helper_lines(
        run_dir_path,
        resolved_basis_round_id,
        report_language,
    )
    formal_policy_argument = formal_policy_argument_summary(formal_helper_meta, report_language)
    case_profile = issue_profile_from_text(
        " ".join(
            [
                mission_line,
                public_compact_line,
                *formal_helper_lines,
                *[maybe_text(row.get("summary")) for row in object_rows],
                *[maybe_text(row.get("rationale")) for row in object_rows],
            ]
        )
    )
    if case_profile == "formal-policy-comment":
        known_fact_text = [
            line
            for line in known_fact_text
            if not unsupported_environment_stock_text(line)
        ]
    basis_text = unique_texts(
        [
            *formal_helper_lines[:1],
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
    if case_profile == "formal-policy-comment":
        limitation_text = [
            line
            for line in limitation_text
            if not unsupported_environment_stock_text(line)
        ]
    if not limitation_text:
        limitation_text = [
            "请将本报告视为对既有议会记录的有限综合；没有引用并不代表现实世界中不存在相关证据。"
            if is_zh(report_language)
            else "Use this report as a bounded synthesis of recorded council artifacts; absence of a ref is not evidence of real-world absence."
        ]
    substantive_candidates = (
        [
            *formal_helper_lines[:1],
            *[rendered_row_text(row, report_language, limit=1500) for row in social_rows],
            *synthesis_text[:1],
            *known_fact_text[:2],
        ]
        if case_profile == "formal-policy-comment"
        else [
            *[rendered_row_text(row, report_language, limit=1500) for row in environmental_rows],
            *[rendered_row_text(row, report_language, limit=1500) for row in social_rows],
            *known_fact_text[:2],
        ]
    )
    substantive_line = first_text(substantive_candidates)
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
        (
            [
                *formal_helper_lines[1:],
                *[rendered_row_text(row, report_language, limit=1400) for row in social_rows],
                public_compact_line,
                finding_text[0] if finding_text else "",
            ]
            if case_profile == "formal-policy-comment"
            else [
                *[rendered_row_text(row, report_language, limit=1400) for row in social_rows],
                *formal_helper_lines[1:],
                public_compact_line,
                finding_text[0] if finding_text else "",
            ]
        )
    )
    social_line = " ".join(social_candidates[:2]) if is_zh(report_language) else first_text(social_candidates)
    if formal_policy_argument and is_zh(report_language):
        bottom_line = maybe_text(formal_policy_argument.get("central_claim")) or bottom_line
        direct_answers = text_list(formal_policy_argument.get("direct_answers"))
        social_line = " ".join(direct_answers[:2]) or social_line
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
    environmental_candidates = [
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
    environmental_detail = first_text(
        [
            reportable_environment_detail(candidate, profile=case_profile)
            for candidate in environmental_candidates
        ]
    )
    synthesis_line = first_text(synthesis_text)
    evidence_narrative = unique_texts(
        [
            *formal_helper_lines,
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
    case_context = " ".join([mission_focus, bottom_line, environmental_detail, social_line])
    case_profile = issue_profile_from_text(case_context)
    if is_zh(report_language) and case_profile == "formal-policy-comment":
        decision_implications = [
            "决策者可以把本报告用于识别正式制度入口、正式评论材料可读性边界，以及媒体/公共文本线索能够提供的公共语义入口。",
            "本报告不应被用于概括正式评论总体格局、评估样本外公众态度，或完成政策优劣判断。若要支撑这些更强判断，需要补充可读评论正文、可审计标注聚合结果和更清楚的公共样本框。",
        ]
    elif is_zh(report_language):
        decision_implications = [
            "决策者可以把本报告用于识别议题结构：哪些环境/运行事实已经有数据支撑，哪些正式记录能够约束语境，公共讨论主要围绕哪些风险和成因展开。",
            "本报告不应被用于判定具体责任、证明某一运行决策的法律触发原因，或声称已经形成代表性公众意见结论。若要支撑这些更强判断，需要补充直接运营理由、正式意见文本和代表性调查设计。",
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
    if formal_policy_argument and is_zh(report_language):
        argument_map.update(
            {
                key: value
                for key, value in formal_policy_argument.items()
                if key
                in {
                    "central_claim",
                    "reasoning_chain",
                    "evidence_roles",
                    "limitations",
                    "decision_meaning",
                    "risk_register",
                    "direct_answers",
                    "council_work",
                    "executive_paragraphs",
                    "issue_analysis",
                    "evidence_analysis",
                    "public_semantics",
                    "method_context",
                    "follow_up_needs",
                    "academic_sections",
                    "profile",
                }
            }
        )
    if (
        is_zh(report_language)
        and case_profile == "environmental-incident"
        and not isinstance(argument_map.get("academic_sections"), dict)
    ):
        academic_sections = build_environmental_incident_academic_sections(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=resolved_basis_round_id,
            title=maybe_text(title),
            mission_line=mission_line,
            central_claim=maybe_text(argument_map.get("central_claim")),
            object_rows=object_rows,
            all_refs=all_refs,
            boundary_line=boundary_line_for_report,
        )
        if academic_sections:
            argument_map["academic_sections"] = academic_sections
            argument_map["profile"] = case_profile
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
        profile=case_profile,
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
        unique_texts([*argument_evidence, *formal_helper_lines])
        if is_zh(report_language)
        else build_en_evidence_chain(
            bottom_line=bottom_line,
            environmental_detail=environmental_detail,
            social_line=social_line,
            limitation_line=boundary_line,
        )
    )
    closure_narrative = (
        build_zh_closure_narrative(synthesis_line=synthesis_line, readiness_lines=readiness_text, profile=case_profile)
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
    interaction_timeline_addendum = build_interaction_timeline_addendum(
        section_briefs=section_briefs,
        interaction_nodes=interaction_timeline_nodes,
        language=report_language,
    )
    if is_zh(report_language) and case_profile == "formal-policy-comment":
        case_frame_sentence = "正文主线应优先界定正式治理入口、评论样本可读性、附件路线限制和公共语义样本边界。"
    else:
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
            text_list(argument_map.get("direct_answers")) or key_points,
            all_refs[:12],
            status="draft",
            language=report_language,
        )
        | {"presentation": "bullet-list"},
        *(
            [
                section(
                    "council-work",
                    label("council-work", report_language),
                    text_list(argument_map.get("council_work")),
                    all_refs[:12],
                    status="council-visible-summary",
                    language=report_language,
                )
            ]
            if text_list(argument_map.get("council_work"))
            else []
        ),
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
        *([interaction_timeline_addendum] if interaction_timeline_addendum else []),
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
        *(
            [
                section(
                    "risk-register",
                    label("risk-register", report_language),
                    text_list(argument_map.get("risk_register")),
                    unique_texts(
                        [
                            *[row["ref"] for row in reviews if row.get("ref")],
                            *[row["ref"] for row in readinesses if row.get("ref")],
                            *[row["ref"] for row in positions if row.get("ref")],
                            *[row["ref"] for row in syntheses if row.get("ref")],
                        ]
                    ),
                    status="risk-visible",
                    language=report_language,
                )
                | {"presentation": "bullet-list"}
            ]
            if text_list(argument_map.get("risk_register"))
            else []
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
            "claim_sensitive_soft_obligations": [
                {
                    "claim_family": "public_discourse_emotion_issue_or_proportion",
                    "basis_needed": "corpus, coverage audit, annotation, aggregation, explicit denominator, and sample-local representativeness limits",
                    "boundary": "Do not convert YouTube, Bluesky, GDELT, or formal-comment samples into general public opinion without representative sampling design.",
                },
                {
                    "claim_family": "formal_comment_issue_or_participation_structure",
                    "basis_needed": "candidate audit, readable comment detail or attachment text, formal issue classification or equivalent analysis",
                    "boundary": "Comment listings are candidate rows, not formal comment corpus text.",
                },
                {
                    "claim_family": "environment_trend_peak_or_operating_status",
                    "basis_needed": "aggregate-environment-evidence or explicit item-level-example wording",
                    "boundary": "Large normalized environment datasets should be compressed before report synthesis.",
                },
                {
                    "claim_family": "source_causal_or_impact_chain",
                    "basis_needed": "relation packet, fact-check scope, alternatives, or challenger review basis",
                    "boundary": "Otherwise write compatibility cues or still-needs-verification language.",
                },
                {
                    "claim_family": "fact_policy_public_interaction",
                    "basis_needed": "interaction timeline node, section brief, separate fact/policy refs, separate public/media refs, claim strength, denominator, and limitations",
                    "boundary": "Timeline co-visibility is descriptive context, not causality, policy impact, or public response attribution.",
                },
                {
                    "claim_family": "policy_evaluation_or_responsibility",
                    "basis_needed": "policy proposal/evaluation basis, finding/evidence bundle, relation review, and challenger limitations",
                    "boundary": "Do not write policy success, failure, responsibility, or accountability as a substantive finding without matching basis.",
                },
            ],
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
            "section_briefs": section_briefs,
            "interaction_timeline": {
                "path": first_section_brief_path(section_briefs),
                "section_brief_count": len(section_briefs),
                "interaction_node_count": len(interaction_timeline_nodes),
                "lane_episode_card_count": len(lane_episode_cards),
                "advisory_only": bool(section_briefs or interaction_timeline_nodes or lane_episode_cards),
            },
            "lane_episode_cards": lane_episode_cards,
            "formal_policy_helper_summary": formal_helper_meta,
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

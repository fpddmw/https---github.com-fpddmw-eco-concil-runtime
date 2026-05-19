from __future__ import annotations

from typing import Any

from .research_issues import (
    approved_helper_input_payload,
    load_json_file,
    unapproved_input_warning,
)
from .support import (
    artifact_ref,
    dict_items,
    helper_metadata,
    list_items,
    maybe_text,
    normalize_space,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json,
)


DEFAULT_ANNOTATION_BASIS_REF = "public-discourse-affect-worker-v1"
TEXTUAL_DISCOURSE_LANES = {
    "social_sample_affect",
    "formal_public_comment_sample",
    "gdelt_doc_recon",
    "public_visibility",
    "public_discourse_text",
    "formal_record_text",
}

ANNOTATION_CUE_SETS: dict[str, dict[str, tuple[str, ...]]] = {
    "issue_facets": {
        "health-risk": (
            "health",
            "asthma",
            "breathe",
            "breathing",
            "respiratory",
            "mask",
            "pm2.5",
            "air quality",
            "空气",
            "健康",
            "口罩",
            "呼吸",
        ),
        "visibility/orange-sky": (
            "orange sky",
            "orange skies",
            "visibility",
            "skyline",
            "haze",
            "smog",
            "橙色",
            "能见度",
            "烟霾",
        ),
        "mask/protection": ("mask", "n95", "protect", "stay inside", "indoor", "防护", "口罩"),
        "school/work/disruption": ("school", "work", "office", "class", "cancel", "closed", "停课", "上班"),
        "travel/flight-disruption": ("flight", "airport", "delay", "travel", "航班", "机场", "延误"),
        "government-response": ("mayor", "governor", "agency", "warning", "advisory", "政府", "预警"),
        "climate-change": ("climate", "warming", "气候"),
        "source-origin-question": ("where", "source", "from", "origin", "canada", "quebec", "wildfire", "来自", "来源"),
        "water-supply-risk": (
            "water supply",
            "drinking water",
            "shortage",
            "shortages",
            "allocation",
            "conservation",
            "供水",
            "用水",
            "缺水",
            "分配",
        ),
        "reservoir-or-release-operations": (
            "reservoir",
            "lake powell",
            "lake mead",
            "glen canyon",
            "dam release",
            "releases",
            "release volume",
            "水库",
            "放水",
            "调度",
            "大坝",
        ),
        "hydropower-or-energy": ("hydropower", "power generation", "electricity", "energy", "水电", "发电", "能源"),
        "ecological-or-habitat-risk": (
            "ecosystem",
            "habitat",
            "endangered",
            "fish",
            "riparian",
            "ecological",
            "生态",
            "栖息地",
        ),
        "formal-governance-process": (
            "federal register",
            "notice",
            "comment period",
            "public comment",
            "rulemaking",
            "consultation",
            "正式意见",
            "征求意见",
            "公告",
        ),
        "drought-or-climate-stress": ("drought", "aridification", "low snowpack", "climate stress", "干旱", "气候压力"),
        "information-seeking": ("?", "what", "why", "how", "where", "guidance", "信息", "为什么", "怎么"),
    },
    "affect_labels": {
        "concern": ("worried", "worry", "concern", "unsafe", "bad", "danger", "担心", "不安全"),
        "fear": ("scared", "afraid", "terrifying", "apocalyptic", "fear", "害怕", "恐惧"),
        "anger": ("angry", "furious", "outrage", "mad", "愤怒"),
        "frustration": ("frustrated", "annoyed", "sick of", "can't believe", "受够", "烦"),
        "sarcasm/humor": ("lol", "lmao", "joke", "meme", "sarcasm", "哈哈", "笑死"),
        "sympathy": ("hope everyone", "stay safe", "take care", "sympathy", "保重", "注意安全"),
        "neutral-reporting": ("reported", "reports", "update", "advisory", "news", "报道", "更新"),
        "uncertainty": ("maybe", "unclear", "not sure", "unknown", "?", "可能", "不确定"),
        "support-or-approval": ("support", "approve", "benefit", "reasonable", "good plan", "赞成", "支持", "认可"),
        "opposition-or-criticism": ("oppose", "criticize", "bad plan", "unfair", "harmful", "反对", "批评", "不公平"),
    },
    "source_narrative_labels": {
        "canada-wildfires": ("canada", "canadian", "加拿大"),
        "quebec-wildfires": ("quebec", "québec", "魁北克"),
        "nova-scotia-wildfires": ("nova scotia", "新斯科舍"),
        "regional-wildfire-smoke": ("wildfire smoke", "wildfires", "wildfire", "forest fire", "野火", "山火"),
        "climate-change-frame": ("climate change", "climate", "warming", "气候变化"),
        "local-pollution": ("pollution", "traffic", "local emissions", "污染", "排放"),
        "drought-or-aridification": ("drought", "aridification", "megadrought", "干旱", "干旱化"),
        "water-release-operations": ("dam release", "releases", "flow", "operations", "放水", "流量", "调度"),
        "reservoir-levels": ("reservoir level", "lake powell", "lake mead", "storage", "水位", "库容"),
        "federal-water-governance": ("reclamation", "bureau of reclamation", "usbr", "secretary of the interior", "联邦", "水资源治理"),
        "basin-allocation-conflict": ("basin states", "allocation", "compact", "water rights", "分配", "水权", "流域"),
        "ecosystem-protection-frame": ("ecosystem", "habitat", "endangered species", "fish", "生态保护", "栖息地"),
    },
    "actor_responsibility_labels": {
        "government-response": ("mayor", "governor", "city", "state", "government", "政府"),
        "agency-warning": ("epa", "airnow", "weather service", "warning", "advisory", "agency", "预警"),
        "individual-protection": ("mask", "stay inside", "protect", "n95", "indoor", "自我防护"),
        "platform/media-amplification": ("news", "media", "viral", "youtube", "视频", "媒体"),
        "natural-hazard": ("wildfire", "smoke", "wind", "weather", "hazard", "野火", "天气"),
        "regulatory-failure": ("regulation", "policy failure", "permit", "监管"),
        "federal-water-agency": ("reclamation", "bureau of reclamation", "usbr", "interior department", "联邦水资源", "垦务局"),
        "state-or-basin-stakeholder": ("basin state", "state water", "tribe", "tribal", "irrigation district", "州", "流域", "部落"),
        "dam-or-infrastructure-operator": ("dam operator", "glen canyon dam", "hoover dam", "hydropower", "大坝", "基础设施"),
        "environmental-advocacy": ("environmental group", "conservation", "advocacy", "生态保护", "环保组织"),
    },
    "action_orientation_labels": {
        "seeking-information": ("?", "what", "why", "how", "where", "guidance", "seek", "help", "怎么", "为什么"),
        "protective-action": ("mask", "stay inside", "close windows", "n95", "protect", "口罩", "关窗"),
        "policy-demand": ("should", "must", "demand", "policy", "need to", "应该", "必须"),
        "fact-checking": ("source", "evidence", "verify", "fact", "check", "证明", "核查"),
        "sharing-experience": ("i see", "i saw", "my", "we are", "here in", "我看到", "我们"),
        "humor/reaction": ("lol", "lmao", "meme", "joke", "reaction", "哈哈"),
        "uncertainty": ("maybe", "unclear", "not sure", "unknown", "可能", "不确定"),
        "participation-or-comment": ("comment", "submit", "public meeting", "hearing", "participate", "提交意见", "听证", "参与"),
    },
}


def _load_corpus_payload(corpus_path: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    if not maybe_text(corpus_path):
        return {}, [
            {
                "code": "corpus-path-required",
                "message": "classify-public-discourse-affect requires a materialized corpus artifact.",
            }
        ]
    payload = load_json_file(corpus_path, {})
    approved, reason = approved_helper_input_payload(
        payload,
        allowed_skills={"materialize-public-discourse-corpus"},
    )
    if not approved:
        return {}, [unapproved_input_warning(corpus_path, reason)]
    return payload, []


def _item_text(item: dict[str, Any]) -> str:
    return normalize_space(
        " ".join(
            [
                maybe_text(item.get("title")),
                maybe_text(item.get("text_excerpt")),
            ]
        )
    )


def _matched_cues(text: str, cues: tuple[str, ...]) -> list[str]:
    text_lower = text.casefold()
    matches: list[str] = []
    for cue in cues:
        cue_text = maybe_text(cue)
        if cue_text and cue_text.casefold() in text_lower:
            matches.append(cue_text)
    return unique_texts(matches)


def _candidate_labels_for_text(text: str, *, max_labels_per_family: int) -> list[dict[str, Any]]:
    labels: list[dict[str, Any]] = []
    for family, family_labels in ANNOTATION_CUE_SETS.items():
        family_count = 0
        for label, cues in family_labels.items():
            matches = _matched_cues(text, cues)
            if not matches:
                continue
            labels.append(
                {
                    "label_family": family,
                    "label": label,
                    "matched_cues": matches[:8],
                    "label_basis": "structured worker rubric",
                }
            )
            family_count += 1
            if max_labels_per_family > 0 and family_count >= max_labels_per_family:
                break
    return labels


def _default_labels_for_item(item: dict[str, Any], text: str) -> list[dict[str, Any]]:
    lane = maybe_text(item.get("discourse_lane"))
    labels: list[dict[str, Any]] = []
    if lane == "social_sample_affect" and text:
        labels.append(
            {
                "label_family": "affect_labels",
                "label": "neutral-reporting",
                "matched_cues": [],
                "label_basis": "default social sample label when no stronger affect cue is present",
            }
        )
    if lane in {"public_visibility", "gdelt_doc_recon", "public_discourse_text"} and text:
        labels.append(
            {
                "label_family": "source_narrative_labels",
                "label": "unknown-or-not-mentioned",
                "matched_cues": [],
                "label_basis": "default source narrative label when no origin cue is present",
            }
        )
    return labels


def _annotation_rows_for_item(
    item: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    annotation_basis_ref: str,
    max_labels_per_family: int,
) -> list[dict[str, Any]]:
    signal_id = maybe_text(item.get("signal_id"))
    lane = maybe_text(item.get("discourse_lane"))
    if not signal_id or lane not in TEXTUAL_DISCOURSE_LANES:
        return []
    text = _item_text(item)
    if not text:
        return []
    label_candidates = _candidate_labels_for_text(text, max_labels_per_family=max_labels_per_family)
    present = {(maybe_text(row.get("label_family")), maybe_text(row.get("label"))) for row in label_candidates}
    for default_row in _default_labels_for_item(item, text):
        key = (maybe_text(default_row.get("label_family")), maybe_text(default_row.get("label")))
        if key not in present:
            label_candidates.append(default_row)
            present.add(key)
    evidence_refs = list_items(item.get("evidence_refs"))
    rows: list[dict[str, Any]] = []
    for label_row in label_candidates:
        family = maybe_text(label_row.get("label_family"))
        label = maybe_text(label_row.get("label"))
        if not family or not label:
            continue
        rows.append(
            {
                "annotation_id": "public-discourse-worker-annotation-"
                + stable_hash(run_id, round_id, signal_id, family, label, annotation_basis_ref)[:12],
                "signal_id": signal_id,
                "label_family": family,
                "label": label,
                "annotation_source": "public-discourse-annotation-worker",
                "annotation_basis_ref": annotation_basis_ref,
                "audit_status": "worker-labeled",
                "source_family": maybe_text(item.get("source_family")),
                "discourse_lane": lane,
                "source_skill": maybe_text(item.get("source_skill")),
                "text_excerpt": maybe_text(item.get("text_excerpt"))[:500],
                "matched_cues": list_items(label_row.get("matched_cues")),
                "label_basis": maybe_text(label_row.get("label_basis")),
                "evidence_refs": evidence_refs,
            }
        )
    return rows


def run_classify_public_discourse_affect(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    corpus_path: str,
    annotation_basis_ref: str = "",
    output_path: str = "",
    max_items: int = 500,
    max_labels_per_family: int = 3,
) -> dict[str, Any]:
    skill_name = "classify-public-discourse-affect"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"public_discourse_affect_annotations_{round_id}.json")
    corpus_payload, corpus_warnings = _load_corpus_payload(corpus_path)
    annotation_basis = maybe_text(annotation_basis_ref) or DEFAULT_ANNOTATION_BASIS_REF
    corpus_items = [
        item
        for item in list_items(corpus_payload.get("corpus_items"))
        if isinstance(item, dict)
    ][: max(1, int(max_items or 500))]
    annotations: list[dict[str, Any]] = []
    skipped_non_textual = 0
    for item in corpus_items:
        item_annotations = _annotation_rows_for_item(
            item,
            run_id=run_id,
            round_id=round_id,
            annotation_basis_ref=annotation_basis,
            max_labels_per_family=max_labels_per_family,
        )
        if not item_annotations and maybe_text(item.get("discourse_lane")) not in TEXTUAL_DISCOURSE_LANES:
            skipped_non_textual += 1
        annotations.extend(item_annotations)

    warnings = [*corpus_warnings]
    if skipped_non_textual:
        warnings.append(
            {
                "code": "non-textual-discourse-lanes-skipped",
                "message": f"Skipped {skipped_non_textual} non-textual or provider-tone corpus items.",
            }
        )
    if not annotations:
        warnings.append(
            {
                "code": "no-worker-annotations",
                "message": "The annotation worker produced no item-level labels from the supplied corpus.",
            }
        )
    annotation_set_id = "public-discourse-affect-annotation-set-" + stable_hash(
        run_id,
        round_id,
        corpus_path,
        annotation_basis,
        len(annotations),
    )[:12]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["bounded-annotation-worker-boundary", "sample-local-labels-only"],
        caveats=[
            "This worker emits sample-local annotation labels only.",
            "It does not infer public opinion, source truth, or physical attribution.",
            "Challenger review is boundary/taxonomy/outlier review, not mandatory item-by-item relabeling.",
        ],
    )
    payload = {
        "schema_version": "optional-analysis-public-discourse-affect-classification-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "annotation_set_id": annotation_set_id,
        "annotation_basis_ref": annotation_basis,
        "annotation_worker_role": "public-discourse-annotation-worker",
        "annotation_worker_scope": {
            "role_kind": "bounded optional-analysis worker, not a council agent",
            "allowed_output": "item-level sample annotations",
            "forbidden_output": [
                "findings",
                "readiness decisions",
                "source ranking",
                "public-opinion estimates",
                "physical source attribution",
            ],
        },
        "challenger_review_model": {
            "default_review_scope": "sample boundary, taxonomy fit, ambiguous-label clusters, outlier examples, and report wording",
            "item_level_review_required": False,
            "item_level_review_when": [
                "the report will cite a specific controversial example",
                "labels drive a high-impact claim",
                "sarcasm, quotation, or translation ambiguity is visible",
            ],
        },
        "sample_definition": dict_items(corpus_payload.get("sample_definition")),
        "sample_count": len(corpus_items),
        "annotation_count": len(annotations),
        "annotations": annotations,
        "representativeness_limits": [
            "Labels describe only the selected corpus sample.",
            "Sample label counts are not general public-opinion estimates.",
            "GDELT provider tone and social sample affect remain separate.",
        ],
        "observed_inputs": {
            "corpus_path": maybe_text(corpus_path),
            "corpus_item_count": len(corpus_items),
        },
        "source_parameters": {
            "annotation_basis_ref": annotation_basis,
            "max_items": int(max_items or 500),
            "max_labels_per_family": int(max_labels_per_family or 3),
        },
        "query_parameters": {
            "run_id": run_id,
            "round_id": round_id,
            "corpus_path": maybe_text(corpus_path),
        },
        "provenance": {
            "source_skill": skill_name,
            "decision_source": metadata["decision_source"],
            "corpus_path": maybe_text(corpus_path),
        },
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "annotation_count": len(annotations),
            "sample_count": len(corpus_items),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "public-discourse-affect-classification-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, annotation_set_id)[:20],
        "batch_id": "public-discourse-affect-classification-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.annotations")],
        "canonical_ids": [annotation_set_id],
        "warnings": warnings,
        "annotation_set_id": annotation_set_id,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.annotations",
            candidate_ids=[annotation_set_id],
            gap_hints=[warning["message"] for warning in warnings],
        ),
    }

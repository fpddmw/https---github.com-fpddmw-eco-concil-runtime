from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .public_discourse import public_discourse_lane, public_discourse_source_family
from .support import (
    artifact_ref,
    date_key,
    first_timestamp,
    helper_metadata,
    lineage_from_signals,
    list_items,
    maybe_text,
    query_signals,
    refs_from_signals,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    signal_metric_distribution,
    signal_source_distribution,
    stable_hash,
    text_terms,
    unique_values,
    utc_now_iso,
    write_json,
)


__all__ = (
    "build_fact_policy_public_interaction_timeline",
    "run_build_fact_policy_public_interaction_timeline",
)


EVIDENCE_REF_SAMPLE_LIMIT = 50
DAILY_CLUSTER_LIMIT = 12


def _signal_sample(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        bucket_key = "|".join(
            [
                maybe_text(signal.get("source_skill")) or "unknown-source",
                maybe_text(signal.get("metric")) or "unknown-metric",
                public_discourse_source_family(signal) or maybe_text(signal.get("plane")),
            ]
        )
        buckets[bucket_key].append(signal)
    sampled: list[dict[str, Any]] = []
    offset = 0
    while len(sampled) < EVIDENCE_REF_SAMPLE_LIMIT:
        appended = False
        for bucket_key in sorted(buckets):
            bucket = buckets[bucket_key]
            if offset >= len(bucket):
                continue
            sampled.append(bucket[offset])
            appended = True
            if len(sampled) >= EVIDENCE_REF_SAMPLE_LIMIT:
                break
        if not appended:
            break
        offset += 1
    return sampled


def _load_json_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _helper_artifacts(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    specs = [
        (
            "public_discourse_corpus",
            run_dir / "analytics" / f"public_discourse_corpus_{round_id}.json",
            "corpus_items",
        ),
        (
            "public_discourse_coverage_audit",
            run_dir / "analytics" / f"public_discourse_coverage_audit_{round_id}.json",
            "coverage_cues",
        ),
        (
            "public_discourse_annotation_aggregation",
            run_dir / "analytics" / f"public_discourse_annotation_aggregation_{round_id}.json",
            "semantic_distributions",
        ),
        (
            "public_discourse_sample_summary",
            run_dir / "analytics" / f"public_discourse_sample_summary_{round_id}.json",
            "summary",
        ),
    ]
    artifacts: list[dict[str, Any]] = []
    for artifact_key, path, item_key in specs:
        payload = _load_json_artifact(path)
        artifacts.append(
            {
                "artifact_key": artifact_key,
                "artifact_path": str(path.resolve()),
                "present": bool(payload),
                "item_key": item_key,
                "item_count": len(list_items(payload.get(item_key))),
                "artifact_ref": artifact_ref(path.resolve(), f"$.{item_key}"),
                "payload": payload,
            }
        )
    return artifacts


def _query_plane_signals_for_rounds(
    run_dir: Path,
    *,
    run_id: str,
    source_round_ids: list[str],
    plane: str,
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    signals: list[dict[str, Any]] = []
    seen_signal_ids: set[str] = set()
    db_path = ""
    effective_limit = max(1, min(100000, int(limit or 200)))
    for source_round_id in source_round_ids:
        round_signals, round_db_path = query_signals(
            run_dir,
            run_id=run_id,
            round_id=source_round_id,
            plane=plane,
            limit=effective_limit,
        )
        db_path = db_path or round_db_path
        for signal in round_signals:
            signal_id = maybe_text(signal.get("signal_id"))
            if signal_id and signal_id in seen_signal_ids:
                continue
            if signal_id:
                seen_signal_ids.add(signal_id)
            signals.append(signal)
            if len(signals) >= effective_limit:
                return signals, db_path
    if not db_path:
        _, db_path = query_signals(
            run_dir,
            run_id=run_id,
            round_id=source_round_ids[0] if source_round_ids else "",
            plane=plane,
            limit=1,
        )
    return signals, db_path


def _semantic_distribution_cues(helper_artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregation = next(
        (
            artifact
            for artifact in helper_artifacts
            if artifact.get("artifact_key") == "public_discourse_annotation_aggregation"
        ),
        {},
    )
    payload = aggregation.get("payload") if isinstance(aggregation.get("payload"), dict) else {}
    cues: list[dict[str, Any]] = []
    for distribution in list_items(payload.get("semantic_distributions"))[:12]:
        if not isinstance(distribution, dict):
            continue
        cues.append(
            {
                "label_family": maybe_text(distribution.get("label_family")),
                "source_family": maybe_text(distribution.get("source_family")),
                "discourse_lane": maybe_text(distribution.get("discourse_lane")),
                "semantic_scope": maybe_text(distribution.get("semantic_scope")),
                "sample_definition": distribution.get("sample_definition", {})
                if isinstance(distribution.get("sample_definition"), dict)
                else {},
                "denominator_scope": distribution.get("denominator_scope", {})
                if isinstance(distribution.get("denominator_scope"), dict)
                else {},
                "distribution": list_items(distribution.get("distribution"))[:8],
            }
        )
    return cues


def _source_family_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for signal in signals:
        source_family = public_discourse_source_family(signal)
        if not source_family:
            source_family = maybe_text(signal.get("plane")) or "unknown"
        counts[source_family] += 1
    return [
        {"source_family": source_family, "item_count": item_count}
        for source_family, item_count in sorted(counts.items())
        if source_family
    ]


def _lane_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for signal in signals:
        plane = maybe_text(signal.get("plane"))
        lane = public_discourse_lane(signal) if plane in {"public", "formal"} else plane
        if not lane:
            lane = plane or "unknown"
        counts[lane] += 1
    return [
        {"discourse_lane": lane, "item_count": item_count}
        for lane, item_count in sorted(counts.items())
        if lane
    ]


def _signal_excerpt(signal: dict[str, Any], limit: int = 180) -> dict[str, str]:
    title = maybe_text(signal.get("title"))
    body = maybe_text(signal.get("body_text"))
    text = title or body
    if len(text) > limit:
        text = text[: limit - 3].rstrip() + "..."
    return {
        "signal_id": maybe_text(signal.get("signal_id")),
        "source_skill": maybe_text(signal.get("source_skill")),
        "timestamp_utc": maybe_text(first_timestamp(signal)),
        "text": text,
    }


def _top_text_terms(signals: list[dict[str, Any]], *, limit: int = 10) -> list[str]:
    counts: Counter[str] = Counter()
    for signal in signals[:500]:
        text = maybe_text(signal.get("title")) + " " + maybe_text(signal.get("body_text"))
        counts.update(text_terms(text, limit=20))
    return [term for term, _ in counts.most_common(limit)]


def _time_range(signals: list[dict[str, Any]]) -> dict[str, str]:
    timestamps = [
        maybe_text(first_timestamp(signal))
        for signal in signals
        if maybe_text(first_timestamp(signal))
    ]
    return {
        "start": min(timestamps) if timestamps else "",
        "end": max(timestamps) if timestamps else "",
    }


def _cluster_key(signal: dict[str, Any], side_key: str) -> tuple[str, str, str]:
    source_skill = maybe_text(signal.get("source_skill")) or "unknown-source"
    metric = maybe_text(signal.get("metric"))
    if side_key == "public-media":
        return (
            public_discourse_source_family(signal) or "unknown-public-source-family",
            public_discourse_lane(signal) or "unknown-public-lane",
            source_skill,
        )
    if side_key == "policy":
        return (
            source_skill,
            public_discourse_lane(signal) or maybe_text(signal.get("plane")) or "formal",
            "formal-or-policy-text",
        )
    return (source_skill, metric or "unspecified-metric", maybe_text(signal.get("unit")))


def _daily_cluster_cues(
    signals: list[dict[str, Any]],
    *,
    side_key: str,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        grouped[_cluster_key(signal, side_key)].append(signal)
    cues: list[dict[str, Any]] = []
    for cluster_key, members in sorted(
        grouped.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )[:DAILY_CLUSTER_LIMIT]:
        source_or_family, metric_or_lane, unit_or_source = cluster_key
        cue: dict[str, Any] = {
            "cluster_id": "daily-cluster-" + stable_hash(side_key, *cluster_key)[:12],
            "cluster_basis": (
                "date-scoped source/metric compression"
                if side_key != "public-media"
                else "date-scoped source-family/lane compression"
            ),
            "side_key": side_key,
            "item_count": len(members),
            "time_range_utc": _time_range(members),
            "source_skill_counts": signal_source_distribution(members),
            "evidence_refs": refs_from_signals(_signal_sample(members)),
            "evidence_ref_sample_limit": EVIDENCE_REF_SAMPLE_LIMIT,
            "examples": [_signal_excerpt(signal) for signal in _signal_sample(members)[:3]],
        }
        if side_key == "public-media":
            cue.update(
                {
                    "source_family": source_or_family,
                    "discourse_lane": metric_or_lane,
                    "source_skill": unit_or_source,
                    "top_text_terms": _top_text_terms(members),
                    "cluster_interpretation_boundary": (
                        "Text terms are sample-local salience cues, not representative public opinion."
                    ),
                }
            )
        else:
            cue.update(
                {
                    "source_skill": source_or_family,
                    "metric": metric_or_lane,
                    "unit": unit_or_source,
                    "metric_distribution": signal_metric_distribution(members),
                    "cluster_interpretation_boundary": (
                        "Numeric summaries are descriptive cluster cues, not source attribution or exposure assessment."
                    ),
                }
            )
        cues.append(cue)
    return cues


def _side_summary(
    signals: list[dict[str, Any]],
    *,
    side_key: str,
) -> dict[str, Any]:
    return {
        "side_key": side_key,
        "item_count": len(signals),
        "signal_ids": lineage_from_signals(_signal_sample(signals)),
        "evidence_refs": refs_from_signals(_signal_sample(signals)),
        "evidence_ref_sample_limit": EVIDENCE_REF_SAMPLE_LIMIT,
        "source_skill_counts": signal_source_distribution(signals),
        "source_family_counts": _source_family_counts(signals),
        "discourse_lane_counts": _lane_counts(signals),
        "time_range_utc": _time_range(signals),
        "daily_cluster_cues": _daily_cluster_cues(signals, side_key=side_key),
        "examples": [_signal_excerpt(signal) for signal in _signal_sample(signals)[:4]],
    }


def _owner_role_for_lane(lane_key: str) -> str:
    return {
        "fact": "environmental-investigator",
        "policy": "policy-investigator",
        "public-media": "social-investigator",
    }.get(lane_key, "investigator")


def _lane_section_role(lane_key: str) -> str:
    return {
        "fact": "Organize date-scoped environmental and factual observations before report synthesis.",
        "policy": "Organize date-scoped official or formal policy records before report synthesis.",
        "public-media": "Organize date-scoped public/media semantic sample cues before report synthesis.",
    }.get(lane_key, "Organize date-scoped lane evidence before report synthesis.")


def _lane_main_claims(lane_key: str, date_value: str, summary: dict[str, Any]) -> list[str]:
    count = int(summary.get("item_count") or 0)
    if lane_key == "fact":
        return [
            f"Fact lane has {count} date-scoped environmental/factual signal(s) visible on {date_value}."
        ]
    if lane_key == "policy":
        return [
            f"Policy lane has {count} date-scoped official/formal signal(s) visible on {date_value}."
        ]
    if lane_key == "public-media":
        return [
            f"Public/media lane has {count} date-scoped sample signal(s) visible on {date_value}."
        ]
    return [f"{lane_key} lane has {count} date-scoped signal(s) visible on {date_value}."]


def _lane_limitations(lane_key: str) -> list[str]:
    common = [
        "Episode cards are lane-level synthesis inputs; report prose still requires reporting handoff, frozen basis, or council-carried uptake.",
        "Date-scoped visibility does not establish causality, response, policy effect, representativeness, or evidence absence.",
    ]
    if lane_key == "fact":
        return [
            *common,
            "Environmental/factual episode cards are descriptive and do not prove source attribution, exposure, responsibility, or policy effectiveness.",
        ]
    if lane_key == "policy":
        return [
            *common,
            "Official/formal episode cards prove visible records or actions only; they do not prove implementation quality, compliance, or policy effectiveness.",
        ]
    if lane_key == "public-media":
        return [
            *common,
            "Public/media episode cards are sample-local semantic cues and cannot be written as representative public opinion or population sentiment.",
        ]
    return common


def _blocked_phrases_for_lane(lane_key: str) -> list[str]:
    if lane_key == "fact":
        return [
            "proved the exact source",
            "caused by this specific source",
            "policy failed because",
        ]
    if lane_key == "policy":
        return [
            "policy was effective",
            "official action caused public response",
            "all affected groups were reached",
        ]
    if lane_key == "public-media":
        return [
            "public opinion was",
            "the public believed",
            "representative sentiment",
        ]
    return ["caused", "proved", "representative"]


def _source_families_from_summary(summary: dict[str, Any]) -> list[str]:
    families: list[str] = []
    for item in list_items(summary.get("source_family_counts")):
        if isinstance(item, dict):
            family = maybe_text(item.get("source_family"))
            if family:
                families.append(family)
    return unique_values(families)


def _make_lane_episode_card(
    *,
    run_id: str,
    round_id: str,
    date_value: str,
    lane_key: str,
    side_key: str,
    signals: list[dict[str, Any]],
    semantic_cues: list[dict[str, Any]],
) -> dict[str, Any]:
    summary = _side_summary(signals, side_key=side_key)
    episode_id = "lane-episode-" + stable_hash(run_id, round_id, date_value, lane_key)[:16]
    return {
        "episode_id": episode_id,
        "episode_kind": "lane-episode-card",
        "episode_basis": "date-scoped lane aggregation before interaction timeline composition",
        "run_id": run_id,
        "round_id": round_id,
        "time_anchor_date": date_value,
        "lane_key": lane_key,
        "owner_role": _owner_role_for_lane(lane_key),
        "section_role": _lane_section_role(lane_key),
        "main_claims": _lane_main_claims(lane_key, date_value, summary),
        "evidence_refs": list_items(summary.get("evidence_refs")),
        "source_families": _source_families_from_summary(summary),
        "claim_strength": "bounded-lane-episode-summary",
        "denominators": {
            "signal_count": int(summary.get("item_count") or 0),
            "source_skill_counts": list_items(summary.get("source_skill_counts")),
            "source_family_counts": list_items(summary.get("source_family_counts")),
            "discourse_lane_counts": list_items(summary.get("discourse_lane_counts")),
            "evidence_ref_sample_limit": EVIDENCE_REF_SAMPLE_LIMIT,
            "daily_cluster_limit": DAILY_CLUSTER_LIMIT,
        },
        "limitations": _lane_limitations(lane_key),
        "recommended_report_use": (
            "Use as an input to interaction timeline and section brief synthesis; do not cite as standalone proof of causality or representativeness."
        ),
        "blocked_phrases": _blocked_phrases_for_lane(lane_key),
        "side_summary": summary,
        "cluster_cues": list_items(summary.get("daily_cluster_cues")),
        "semantic_cues": semantic_cues if lane_key == "public-media" else [],
    }


def _build_lane_episode_cards(
    *,
    run_id: str,
    round_id: str,
    output_file: Path,
    env_buckets: dict[str, list[dict[str, Any]]],
    formal_buckets: dict[str, list[dict[str, Any]]],
    public_buckets: dict[str, list[dict[str, Any]]],
    semantic_cues: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    episode_cards: list[dict[str, Any]] = []
    all_dates = sorted(set(env_buckets) | set(formal_buckets) | set(public_buckets))
    for date_value in all_dates:
        if env_buckets.get(date_value):
            episode_cards.append(
                _make_lane_episode_card(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    lane_key="fact",
                    side_key="fact",
                    signals=env_buckets[date_value],
                    semantic_cues=[],
                )
            )
        if formal_buckets.get(date_value):
            episode_cards.append(
                _make_lane_episode_card(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    lane_key="policy",
                    side_key="policy",
                    signals=formal_buckets[date_value],
                    semantic_cues=[],
                )
            )
        if public_buckets.get(date_value):
            episode_cards.append(
                _make_lane_episode_card(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    lane_key="public-media",
                    side_key="public-media",
                    signals=public_buckets[date_value],
                    semantic_cues=semantic_cues,
                )
            )
    for index, card in enumerate(episode_cards):
        card["episode_index"] = index
        card["episode_ref"] = artifact_ref(output_file, f"$.lane_episode_cards[{index}]")
    return episode_cards


def _episode_refs(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        ref
        for ref in (episode.get("episode_ref") for episode in episodes)
        if isinstance(ref, dict)
    ]


def _compact_episode_for_node(episode: dict[str, Any]) -> dict[str, Any]:
    denominators = episode.get("denominators") if isinstance(episode.get("denominators"), dict) else {}
    return {
        "episode_id": maybe_text(episode.get("episode_id")),
        "episode_kind": maybe_text(episode.get("episode_kind")),
        "lane_key": maybe_text(episode.get("lane_key")),
        "owner_role": maybe_text(episode.get("owner_role")),
        "time_anchor_date": maybe_text(episode.get("time_anchor_date")),
        "claim_strength": maybe_text(episode.get("claim_strength")),
        "main_claims": list_items(episode.get("main_claims"))[:4],
        "source_families": list_items(episode.get("source_families"))[:8],
        "denominators": {
            "signal_count": denominators.get("signal_count", 0),
            "source_family_counts": list_items(denominators.get("source_family_counts"))[:8],
            "source_skill_counts": list_items(denominators.get("source_skill_counts"))[:8],
        },
        "episode_ref": episode.get("episode_ref") if isinstance(episode.get("episode_ref"), dict) else {},
        "limitations": list_items(episode.get("limitations"))[:4],
    }


def _episodes_by_date_and_lane(
    episode_cards: list[dict[str, Any]],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for episode in episode_cards:
        date_value = maybe_text(episode.get("time_anchor_date"))
        lane_key = maybe_text(episode.get("lane_key"))
        if not date_value or not lane_key:
            continue
        grouped[date_value][lane_key].append(episode)
    return grouped


def _episode_side_summary(episodes: list[dict[str, Any]], *, side_key: str) -> dict[str, Any]:
    evidence_refs: list[Any] = []
    source_families: list[str] = []
    cluster_cues: list[Any] = []
    main_claims: list[str] = []
    signal_count = 0
    for episode in episodes:
        evidence_refs.extend(list_items(episode.get("evidence_refs")))
        source_families.extend(list_items(episode.get("source_families")))
        cluster_cues.extend(list_items(episode.get("cluster_cues")))
        main_claims.extend(list_items(episode.get("main_claims")))
        denominators = episode.get("denominators") if isinstance(episode.get("denominators"), dict) else {}
        signal_count += int(denominators.get("signal_count") or 0)
    return {
        "side_key": side_key,
        "episode_count": len(episodes),
        "episode_ids": [maybe_text(episode.get("episode_id")) for episode in episodes],
        "episode_refs": _episode_refs(episodes),
        "item_count": signal_count,
        "source_families": unique_values(source_families),
        "main_claims": unique_values([claim for claim in main_claims if maybe_text(claim)])[:8],
        "cluster_cues": cluster_cues[:DAILY_CLUSTER_LIMIT],
        "evidence_refs": unique_values(evidence_refs)[:EVIDENCE_REF_SAMPLE_LIMIT],
        "evidence_ref_sample_limit": EVIDENCE_REF_SAMPLE_LIMIT,
    }


def _readable_node_summary(
    *,
    date_value: str,
    fact_count: int,
    policy_count: int,
    public_count: int,
    status: str,
) -> str:
    parts: list[str] = []
    if fact_count:
        parts.append(f"{fact_count} fact-side episode(s)")
    if policy_count:
        parts.append(f"{policy_count} policy/official episode(s)")
    if public_count:
        parts.append(f"{public_count} public/media episode(s)")
    visible = ", ".join(parts) if parts else "no lane episodes"
    return (
        f"On {date_value}, {visible} were visible in the timeline. "
        f"Status: {status}. This is descriptive co-visibility, not causality, "
        "policy impact, public response attribution, or evidence absence."
    )


def _warning(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _group_by_date(signals: list[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], int]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_timestamp_count = 0
    for signal in signals:
        key = date_key(first_timestamp(signal))
        if not key:
            missing_timestamp_count += 1
            continue
        buckets[key].append(signal)
    return buckets, missing_timestamp_count


def _context_node(
    *,
    run_id: str,
    round_id: str,
    date_value: str,
    signals: list[dict[str, Any]],
    node_kind: str,
) -> dict[str, Any]:
    node_id = "fpp-context-" + stable_hash(run_id, round_id, date_value, node_kind)[:14]
    return {
        "node_id": node_id,
        "node_kind": node_kind,
        "time_anchor_date": date_value,
        "node_summary": _readable_node_summary(
            date_value=date_value,
            fact_count=1 if node_kind.startswith("fact") else 0,
            policy_count=1 if node_kind.startswith("policy") else 0,
            public_count=1 if node_kind.startswith("public") else 0,
            status="single-sided-context",
        ),
        "interaction_status": "single-sided-context",
        "side_summary": _side_summary(signals, side_key=node_kind),
        "claim_boundary": {
            "report_boundary": (
                "Use this as one-sided chronology only; do not write an interaction "
                "claim without both fact/policy-side and public/media-side refs."
            ),
            "excluded_inferences": [
                "causality",
                "policy impact",
                "public response attribution",
                "evidence absence",
            ],
        },
        "evidence_refs": refs_from_signals(_signal_sample(signals)),
        "lineage": lineage_from_signals(_signal_sample(signals)),
        "evidence_ref_sample_limit": EVIDENCE_REF_SAMPLE_LIMIT,
    }


def _context_node_from_episodes(
    *,
    run_id: str,
    round_id: str,
    date_value: str,
    episodes: list[dict[str, Any]],
    node_kind: str,
) -> dict[str, Any]:
    node_id = "fpp-context-" + stable_hash(run_id, round_id, date_value, node_kind)[:14]
    side_key = "public-media" if node_kind.startswith("public") else "fact-policy"
    summary = _episode_side_summary(episodes, side_key=side_key)
    return {
        "node_id": node_id,
        "node_kind": node_kind,
        "time_anchor_date": date_value,
        "node_summary": _readable_node_summary(
            date_value=date_value,
            fact_count=summary.get("episode_count", 0) if side_key == "fact-policy" else 0,
            policy_count=0,
            public_count=summary.get("episode_count", 0) if side_key == "public-media" else 0,
            status="single-sided-episode-context",
        ),
        "interaction_status": "single-sided-episode-context",
        "interaction_basis": "lane_episode_cards",
        "episode_refs": _episode_refs(episodes),
        "side_summary": summary,
        "claim_boundary": {
            "report_boundary": (
                "Use this as one-sided chronology only; do not write an interaction "
                "claim without both fact/policy-side and public/media-side lane episodes."
            ),
            "excluded_inferences": [
                "causality",
                "policy impact",
                "public response attribution",
                "evidence absence",
            ],
        },
        "evidence_refs": list_items(summary.get("evidence_refs")),
        "lineage": [maybe_text(episode.get("episode_id")) for episode in episodes],
    }


def _semantic_shift_events(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for previous, current in zip(nodes, nodes[1:]):
        previous_cues = list_items(previous.get("semantic_cues"))
        current_cues = list_items(current.get("semantic_cues"))
        if not previous_cues and not current_cues:
            continue
        events.append(
            {
                "event_id": "fpp-semantic-shift-"
                + stable_hash(previous.get("node_id"), current.get("node_id"))[:14],
                "event_status": "candidate-review-needed",
                "from_node_id": maybe_text(previous.get("node_id")),
                "to_node_id": maybe_text(current.get("node_id")),
                "from_date": maybe_text(previous.get("time_anchor_date")),
                "to_date": maybe_text(current.get("time_anchor_date")),
                "semantic_basis": "sample-local helper distributions only",
                "claim_boundary": {
                    "report_boundary": (
                        "Do not write a semantic shift claim unless comparable "
                        "sample definitions, source-family denominators, and "
                        "council-carried interpretation are present."
                    ),
                    "excluded_inferences": [
                        "public opinion trend",
                        "causal response",
                        "representative semantic change",
                    ],
                },
                "lineage": unique_values(
                    [
                        *list_items(previous.get("lineage")),
                        *list_items(current.get("lineage")),
                    ]
                ),
            }
        )
    return events


def build_fact_policy_public_interaction_timeline(
    *,
    environment_signals: list[dict[str, Any]],
    formal_signals: list[dict[str, Any]],
    public_signals: list[dict[str, Any]],
    run_id: str,
    round_id: str,
    skill_name: str,
    db_path: str,
    output_file: Path,
    metadata: dict[str, Any],
    helper_artifacts: list[dict[str, Any]],
    max_nodes: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]], dict[str, Any], list[dict[str, Any]]]:
    env_buckets, env_missing = _group_by_date(environment_signals)
    formal_buckets, formal_missing = _group_by_date(formal_signals)
    public_buckets, public_missing = _group_by_date(public_signals)
    all_dates = sorted(set(env_buckets) | set(formal_buckets) | set(public_buckets))
    semantic_cues = _semantic_distribution_cues(helper_artifacts)
    lane_episode_cards = _build_lane_episode_cards(
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        env_buckets=env_buckets,
        formal_buckets=formal_buckets,
        public_buckets=public_buckets,
        semantic_cues=semantic_cues,
    )
    episodes_by_date = _episodes_by_date_and_lane(lane_episode_cards)
    interaction_nodes: list[dict[str, Any]] = []
    parallel_timeline_nodes: list[dict[str, Any]] = []
    max_node_count = max(1, min(500, int(max_nodes or 200)))

    for date_value in all_dates:
        fact_episodes = episodes_by_date.get(date_value, {}).get("fact", [])
        policy_episodes = episodes_by_date.get(date_value, {}).get("policy", [])
        public_media_episodes = episodes_by_date.get(date_value, {}).get("public-media", [])
        fact_or_policy_episodes = [*fact_episodes, *policy_episodes]
        if fact_or_policy_episodes and public_media_episodes and len(interaction_nodes) < max_node_count:
            node_id = "fpp-node-" + stable_hash(run_id, round_id, date_value)[:16]
            fact_or_policy_episode_refs = _episode_refs(fact_or_policy_episodes)
            public_episode_refs = _episode_refs(public_media_episodes)
            fact_or_policy_evidence_refs = unique_values(
                [
                    ref
                    for episode in fact_or_policy_episodes
                    for ref in list_items(episode.get("evidence_refs"))
                ]
            )[:EVIDENCE_REF_SAMPLE_LIMIT]
            public_evidence_refs = unique_values(
                [
                    ref
                    for episode in public_media_episodes
                    for ref in list_items(episode.get("evidence_refs"))
                ]
            )[:EVIDENCE_REF_SAMPLE_LIMIT]
            interaction_nodes.append(
                {
                    "node_id": node_id,
                    "node_kind": "fact-policy-public-interaction-context",
                    "time_anchor_date": date_value,
                    "node_summary": _readable_node_summary(
                        date_value=date_value,
                        fact_count=len(fact_episodes),
                        policy_count=len(policy_episodes),
                        public_count=len(public_media_episodes),
                        status="candidate-context",
                    ),
                    "interaction_status": "candidate-context",
                    "interaction_basis": "lane_episode_cards",
                    "fact_episodes": [_compact_episode_for_node(episode) for episode in fact_episodes],
                    "policy_episodes": [_compact_episode_for_node(episode) for episode in policy_episodes],
                    "public_media_episodes": [
                        _compact_episode_for_node(episode)
                        for episode in public_media_episodes
                    ],
                    "fact_side": _episode_side_summary(fact_episodes, side_key="fact"),
                    "policy_side": _episode_side_summary(policy_episodes, side_key="policy"),
                    "public_media_side": _episode_side_summary(
                        public_media_episodes,
                        side_key="public-media",
                    ),
                    "fact_or_policy_episode_refs": fact_or_policy_episode_refs,
                    "public_or_media_episode_refs": public_episode_refs,
                    "fact_or_policy_evidence_refs": fact_or_policy_evidence_refs,
                    "public_or_media_evidence_refs": public_evidence_refs,
                    "evidence_ref_sample_limit": EVIDENCE_REF_SAMPLE_LIMIT,
                    "semantic_cues": semantic_cues,
                    "communication_gap_notes": [
                        "Compare lane episode claims before writing interaction prose; do not infer response or policy effect from co-visibility.",
                        "Treat differences as chronology and framing context until a council object carries a stronger claim.",
                    ],
                    "misalignment_and_uncertainty_register": [
                        "Same-date lane episodes do not establish response, influence, policy effect, or representativeness.",
                        "Source-family denominators and text coverage must be cited for any semantic public claim.",
                    ],
                    "claim_boundary": {
                        "report_boundary": (
                            "Can support bounded wording that fact/policy-side lane episodes and "
                            "public/media lane episodes were visible in the same timeline window; "
                            "cannot support causality, policy impact, or public sentiment "
                            "without council-carried basis and denominators."
                        ),
                        "excluded_inferences": [
                            "causality",
                            "policy impact",
                            "public response attribution",
                            "representative public opinion",
                            "evidence absence",
                        ],
                    },
                    "evidence_refs": unique_values(
                        [*fact_or_policy_evidence_refs, *public_evidence_refs]
                    ),
                    "episode_refs": unique_values(
                        [*fact_or_policy_episode_refs, *public_episode_refs]
                    ),
                    "lineage": [
                        maybe_text(episode.get("episode_id"))
                        for episode in [*fact_or_policy_episodes, *public_media_episodes]
                        if maybe_text(episode.get("episode_id"))
                    ],
                    "provenance": {
                        "source_skill": skill_name,
                        "decision_source": metadata["decision_source"],
                        "db_path": db_path,
                        "artifact_path": str(output_file),
                    },
                    "helper_governance": metadata,
                }
            )
            continue
        if fact_or_policy_episodes:
            parallel_timeline_nodes.append(
                _context_node_from_episodes(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    episodes=fact_or_policy_episodes,
                    node_kind="fact-policy-only-context",
                )
            )
        if public_media_episodes:
            parallel_timeline_nodes.append(
                _context_node_from_episodes(
                    run_id=run_id,
                    round_id=round_id,
                    date_value=date_value,
                    episodes=public_media_episodes,
                    node_kind="public-media-only-context",
                )
            )

    warnings: list[dict[str, str]] = []
    missing_total = env_missing + formal_missing + public_missing
    if missing_total:
        warnings.append(
            _warning(
                "missing-timestamps",
                f"{missing_total} signals lacked usable timestamps and were not placed on the interaction timeline.",
            )
        )
    if not interaction_nodes:
        warnings.append(
            _warning(
                "insufficient-interaction-basis",
                "No date bucket had both fact/policy-side and public/media-side signal refs.",
            )
        )
    basis = {
        "environment_signal_count": len(environment_signals),
        "formal_signal_count": len(formal_signals),
        "public_signal_count": len(public_signals),
        "lane_episode_card_count": len(lane_episode_cards),
        "lane_episode_counts": {
            lane_key: count
            for lane_key, count in Counter(
                maybe_text(card.get("lane_key")) for card in lane_episode_cards
            ).items()
            if lane_key
        },
        "missing_timestamp_count": missing_total,
        "date_bucket_count": len(all_dates),
        "interaction_node_limit": max_node_count,
        "helper_artifacts_present": [
            maybe_text(artifact.get("artifact_key"))
            for artifact in helper_artifacts
            if artifact.get("present")
        ],
    }
    return interaction_nodes, parallel_timeline_nodes, warnings, basis, lane_episode_cards


def run_build_fact_policy_public_interaction_timeline(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    max_nodes: int = 200,
    limit: int = 100000,
    source_round_ids: list[str] | None = None,
    helper_round_id: str = "",
) -> dict[str, Any]:
    skill_name = "build-fact-policy-public-interaction-timeline"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(
        run_dir_path,
        output_path,
        f"fact_policy_public_interaction_timeline_{round_id}.json",
    )
    effective_source_round_ids = unique_values(
        [maybe_text(item) for item in list(source_round_ids or []) if maybe_text(item)]
    ) or [round_id]
    effective_helper_round_id = maybe_text(helper_round_id) or round_id
    environment_signals, db_path = _query_plane_signals_for_rounds(
        run_dir_path,
        run_id=run_id,
        source_round_ids=effective_source_round_ids,
        plane="environment",
        limit=limit,
    )
    formal_signals, _ = _query_plane_signals_for_rounds(
        run_dir_path,
        run_id=run_id,
        source_round_ids=effective_source_round_ids,
        plane="formal",
        limit=limit,
    )
    public_signals, _ = _query_plane_signals_for_rounds(
        run_dir_path,
        run_id=run_id,
        source_round_ids=effective_source_round_ids,
        plane="public",
        limit=limit,
    )
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=[
            "db-signal-date-buckets",
            "lane-episode-card-synthesis",
            "helper-artifact-context",
        ],
        caveats=[
            "Interaction timeline nodes are descriptive chronology/context only.",
            "Nodes are composed from lane episode cards, not raw signal co-visibility alone.",
            "Nodes do not prove causality, policy impact, public response attribution, representativeness, or evidence absence.",
            "Public semantic claims still require corpus, coverage, denominator, and council/reporting uptake.",
        ],
    )
    helper_artifacts = _helper_artifacts(run_dir_path, effective_helper_round_id)
    interaction_nodes, parallel_nodes, warnings, basis, lane_episode_cards = (
        build_fact_policy_public_interaction_timeline(
            environment_signals=environment_signals,
            formal_signals=formal_signals,
            public_signals=public_signals,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            db_path=db_path,
            output_file=output_file,
            metadata=metadata,
            helper_artifacts=helper_artifacts,
            max_nodes=max_nodes,
        )
    )
    basis["source_round_ids"] = effective_source_round_ids
    basis["helper_round_id"] = effective_helper_round_id
    helper_artifact_inputs = [
        {
            key: value
            for key, value in artifact.items()
            if key != "payload"
        }
        for artifact in helper_artifacts
    ]
    status = "completed" if interaction_nodes else "insufficient-interaction-basis"
    semantic_shift_events = _semantic_shift_events(interaction_nodes)
    payload = {
        "schema_version": "optional-analysis-fact-policy-public-interaction-timeline-v2",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": status,
        "helper_governance": metadata,
        "timeline_scope": {
            "bucket_key": "UTC date from first available signal timestamp",
            "composition_input": "date-scoped lane_episode_cards built before interaction nodes",
            "fact_side": "fact lane episode cards derived from environment-plane normalized signals",
            "policy_side": "policy lane episode cards derived from formal-plane normalized signals",
            "public_media_side": "public/media lane episode cards derived from public-plane normalized signals and public discourse helper artifacts",
        },
        "interaction_policy": {
            "advisory_only": True,
            "does_not_schedule_or_execute": True,
            "does_not_sort_or_select_sources": True,
            "report_use_requires_council_or_reporting_uptake": True,
            "excluded_inferences": [
                "causality",
                "policy impact",
                "public response attribution",
                "representative public opinion",
                "evidence absence",
            ],
        },
        "observed_input_summary": basis,
        "helper_artifact_inputs": helper_artifact_inputs,
        "lane_episode_cards": lane_episode_cards,
        "lane_episode_card_count": len(lane_episode_cards),
        "interaction_nodes": interaction_nodes,
        "interaction_node_count": len(interaction_nodes),
        "semantic_shift_events": semantic_shift_events,
        "semantic_shift_event_count": len(semantic_shift_events),
        "semantic_shift_policy": {
            "advisory_only": True,
            "report_use_requires_comparable_denominators": True,
            "excluded_inferences": [
                "public opinion trend",
                "causal response",
                "representative semantic change",
            ],
        },
        "parallel_timeline_nodes": parallel_nodes,
        "parallel_timeline_node_count": len(parallel_nodes),
        "warnings": warnings,
        "query_basis": {
            "run_id": run_id,
            "round_id": round_id,
            "source_round_ids": effective_source_round_ids,
            "helper_round_id": effective_helper_round_id,
            "db_path": db_path,
            "input_artifact_refs": [
                artifact["artifact_ref"]
                for artifact in helper_artifact_inputs
                if artifact.get("present")
            ],
        },
    }
    write_json(output_file, payload)
    from eco_council_runtime.kernel.planes.analysis_plane import (
        sync_fact_policy_public_interaction_node_result_set,
    )

    analysis_sync = sync_fact_policy_public_interaction_node_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        interaction_timeline_path=output_file,
    )
    payload["analysis_sync"] = analysis_sync
    write_json(output_file, payload)
    return {
        "status": status,
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "interaction_node_count": len(interaction_nodes),
            "parallel_timeline_node_count": len(parallel_nodes),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
            "analysis_kind": analysis_sync.get("analysis_kind"),
        },
        "receipt_id": "fpp-timeline-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "fpp-timeline-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.interaction_nodes")],
        "canonical_ids": [
            maybe_text(item.get("node_id")) for item in interaction_nodes
        ],
        "warnings": warnings,
        "interaction_nodes": interaction_nodes,
        "parallel_timeline_nodes": parallel_nodes,
        "analysis_sync": analysis_sync,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.interaction_nodes",
            candidate_ids=[maybe_text(item.get("node_id")) for item in interaction_nodes],
            gap_hints=[item["message"] for item in warnings],
            challenge_hints=[
                "Treat the timeline as descriptive context, not causality, policy impact, or public response proof."
            ],
        ),
    }

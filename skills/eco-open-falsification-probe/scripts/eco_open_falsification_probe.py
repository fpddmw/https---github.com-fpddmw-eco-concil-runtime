#!/usr/bin/env python3
"""Open falsification probes from the next-action queue."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-open-falsification-probe"


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def requested_skills_for_action(action: dict[str, Any]) -> list[str]:
    action_kind = maybe_text(action.get("action_kind"))
    assigned_role = maybe_text(action.get("assigned_role"))
    suggestions: list[str] = []
    if action_kind in {"resolve-challenge", "resolve-contradiction"}:
        suggestions.extend(["eco-post-board-note", "eco-close-challenge-ticket"])
    if action_kind in {"resolve-contradiction", "expand-coverage"}:
        suggestions.extend(["eco-query-environment-signals", "eco-query-public-signals"])
    if action_kind == "stabilize-hypothesis":
        suggestions.extend(["eco-update-hypothesis-status", "eco-post-board-note"])
    if assigned_role == "environmentalist":
        suggestions.append("eco-query-environment-signals")
    if assigned_role == "sociologist":
        suggestions.append("eco-query-public-signals")
    return unique_texts(suggestions)


def probe_candidates(actions: list[dict[str, Any]], action_id: str) -> list[dict[str, Any]]:
    filtered = [action for action in actions if isinstance(action, dict)]
    if maybe_text(action_id):
        filtered = [action for action in filtered if maybe_text(action.get("action_id")) == maybe_text(action_id)]
    return [
        action
        for action in filtered
        if bool(action.get("probe_candidate")) or maybe_text(action.get("action_kind")) in {"resolve-challenge", "resolve-contradiction", "stabilize-hypothesis"}
    ]


def build_probe(action: dict[str, Any]) -> dict[str, Any]:
    target = action.get("target", {}) if isinstance(action.get("target"), dict) else {}
    action_id = maybe_text(action.get("action_id"))
    hypothesis_id = maybe_text(target.get("hypothesis_id"))
    claim_id = maybe_text(target.get("claim_id"))
    ticket_id = maybe_text(target.get("ticket_id"))
    probe_id = "probe-" + stable_hash(SKILL_NAME, action_id, hypothesis_id, claim_id, ticket_id)[:12]
    objective = maybe_text(action.get("objective")) or maybe_text(action.get("reason")) or "Probe the current contradiction or uncertainty."
    return {
        "probe_id": probe_id,
        "run_id": maybe_text(action.get("run_id")),
        "round_id": maybe_text(action.get("round_id")),
        "opened_at_utc": utc_now_iso(),
        "probe_status": "open",
        "action_id": action_id,
        "priority": maybe_text(action.get("priority")) or "high",
        "owner_role": maybe_text(action.get("assigned_role")) or "challenger",
        "target_hypothesis_id": hypothesis_id,
        "target_claim_id": claim_id,
        "target_ticket_id": ticket_id,
        "probe_goal": objective,
        "falsification_question": f"What evidence would materially weaken: {objective}",
        "success_criteria": [
            "Collect at least one new observation or contradiction-aware note.",
            "Explain whether the target remains credible after focused challenge review.",
        ],
        "disconfirm_signals": [
            "A contradiction-leaning evidence link becomes more plausible than the current support path.",
            "The target can no longer be defended with matching-ready evidence scope.",
        ],
        "requested_skills": requested_skills_for_action(action),
        "evidence_refs": unique_texts(action.get("evidence_refs", []) if isinstance(action.get("evidence_refs"), list) else []),
        "source_ids": unique_texts(action.get("source_ids", []) if isinstance(action.get("source_ids"), list) else []),
    }


def open_falsification_probe_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    next_actions_path: str,
    output_path: str,
    action_id: str,
    max_probes: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"investigation/falsification_probes_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    next_actions_wrapper = load_json_if_exists(next_actions_file)
    if not isinstance(next_actions_wrapper, dict):
        warnings.append({"code": "missing-next-actions", "message": f"No next-actions artifact was found at {next_actions_file}."})
        next_actions_wrapper = {"ranked_actions": [], "action_count": 0}

    ranked_actions = next_actions_wrapper.get("ranked_actions", []) if isinstance(next_actions_wrapper.get("ranked_actions"), list) else []
    candidates = probe_candidates(ranked_actions, action_id)[: max(1, max_probes)]
    probes = [build_probe(action) for action in candidates]

    wrapper = {
        "schema_version": "d1.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "next_actions_path": str(next_actions_file),
        "probe_count": len(probes),
        "probes": probes,
    }
    write_json_file(output_file, wrapper)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$.probes", "artifact_ref": f"{output_file}:$.probes"}]
    if not probes:
        warnings.append({"code": "no-probes", "message": "No probe-worthy next actions were found."})
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "probe_count": len(probes)},
        "receipt_id": "investigation-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "investigationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [maybe_text(probe.get("probe_id")) for probe in probes if maybe_text(probe.get("probe_id"))],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [maybe_text(probe.get("probe_id")) for probe in probes if maybe_text(probe.get("probe_id"))],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if probes else ["No falsification probes are open for this round yet."],
            "challenge_hints": ["Open probes should be reviewed before marking the round fully ready."] if probes else [],
            "suggested_next_skills": ["eco-summarize-round-readiness", "eco-post-board-note", "eco-update-hypothesis-status"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Open falsification probes from the next-action queue.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--action-id", default="")
    parser.add_argument("--max-probes", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = open_falsification_probe_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        next_actions_path=args.next_actions_path,
        output_path=args.output_path,
        action_id=args.action_id,
        max_probes=args.max_probes,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
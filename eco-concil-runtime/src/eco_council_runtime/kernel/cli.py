from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .controller import run_phase2_round
from .executor import SkillExecutionError, run_skill, stable_hash, utc_now_iso
from .gate import apply_promotion_gate
from .ledger import append_ledger_event, load_ledger_tail
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists
from .paths import (
    controller_state_path,
    cursor_path,
    ensure_runtime_dirs,
    ledger_path,
    manifest_path,
    promotion_gate_path,
    registry_path,
    resolve_run_dir,
    supervisor_state_path,
)
from .registry import write_registry
from .supervisor import supervise_round


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def init_run(run_dir: Path, run_id: str) -> dict[str, Any]:
    ensure_runtime_dirs(run_dir)
    registry = write_registry(run_dir)
    manifest = init_run_manifest(run_dir, run_id)
    cursor = init_round_cursor(run_dir, run_id)
    return {
        "status": "completed",
        "summary": {"run_id": run_id, "run_dir": str(run_dir), "skill_count": int(registry.get("skill_count") or 0)},
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
        "paths": {
            "manifest_path": str(manifest_path(run_dir)),
            "cursor_path": str(cursor_path(run_dir)),
            "ledger_path": str(ledger_path(run_dir)),
            "registry_path": str(registry_path(run_dir)),
        },
    }


def show_run_state(run_dir: Path, tail: int) -> dict[str, Any]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    cursor = load_json_if_exists(cursor_path(run_dir)) or {}
    registry = load_json_if_exists(registry_path(run_dir)) or {}
    current_round_id = str(cursor.get("current_round_id") or "")
    phase2_state: dict[str, Any] = {}
    if current_round_id:
        phase2_state = {
            "promotion_gate": load_json_if_exists(promotion_gate_path(run_dir, current_round_id)) or {},
            "controller": load_json_if_exists(controller_state_path(run_dir, current_round_id)) or {},
            "supervisor": load_json_if_exists(supervisor_state_path(run_dir, current_round_id)) or {},
        }
    return {
        "status": "completed",
        "summary": {"run_dir": str(run_dir), "ledger_events": len(load_ledger_tail(run_dir, 1000000)) if ledger_path(run_dir).exists() else 0},
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
        "phase2": phase2_state,
        "ledger_tail": load_ledger_tail(run_dir, tail),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Minimal runtime kernel for skill-first investigation runs.")
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init-run", help="Initialize runtime manifest, cursor, and registry for a run.")
    init_cmd.add_argument("--run-dir", required=True)
    init_cmd.add_argument("--run-id", required=True)
    init_cmd.add_argument("--pretty", action="store_true")

    run_cmd = sub.add_parser("run-skill", help="Execute one skill through the runtime kernel and append a ledger event.")
    run_cmd.add_argument("--run-dir", required=True)
    run_cmd.add_argument("--run-id", required=True)
    run_cmd.add_argument("--round-id", required=True)
    run_cmd.add_argument("--skill-name", required=True)
    run_cmd.add_argument("--pretty", action="store_true")
    run_cmd.add_argument("skill_args", nargs=argparse.REMAINDER)

    gate_cmd = sub.add_parser("apply-promotion-gate", help="Evaluate round readiness and write a promote-or-freeze gate artifact.")
    gate_cmd.add_argument("--run-dir", required=True)
    gate_cmd.add_argument("--run-id", required=True)
    gate_cmd.add_argument("--round-id", required=True)
    gate_cmd.add_argument("--pretty", action="store_true")

    phase2_cmd = sub.add_parser("run-phase2-round", help="Run the board -> D1 -> D2 -> promotion phase-2 chain in one command.")
    phase2_cmd.add_argument("--run-dir", required=True)
    phase2_cmd.add_argument("--run-id", required=True)
    phase2_cmd.add_argument("--round-id", required=True)
    phase2_cmd.add_argument("--pretty", action="store_true")

    supervisor_cmd = sub.add_parser("supervise-round", help="Run the phase-2 controller and materialize a compact supervisor state.")
    supervisor_cmd.add_argument("--run-dir", required=True)
    supervisor_cmd.add_argument("--run-id", required=True)
    supervisor_cmd.add_argument("--round-id", required=True)
    supervisor_cmd.add_argument("--pretty", action="store_true")

    show_cmd = sub.add_parser("show-run-state", help="Show manifest, cursor, registry, and a tail of runtime ledger events.")
    show_cmd.add_argument("--run-dir", required=True)
    show_cmd.add_argument("--tail", type=int, default=10)
    show_cmd.add_argument("--pretty", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    run_dir = resolve_run_dir(args.run_dir)

    if args.command == "init-run":
        payload = init_run(run_dir, args.run_id)
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "run-skill":
        init_run(run_dir, args.run_id)
        skill_args = list(args.skill_args or [])
        if skill_args and skill_args[0] == "--":
            skill_args = skill_args[1:]
        try:
            payload = run_skill(run_dir, run_id=args.run_id, round_id=args.round_id, skill_name=args.skill_name, skill_args=skill_args)
        except SkillExecutionError as exc:
            failure = {"status": "failed", "summary": {"skill_name": args.skill_name, "run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "apply-promotion-gate":
        init_run(run_dir, args.run_id)
        payload = apply_promotion_gate(run_dir, run_id=args.run_id, round_id=args.round_id)
        append_ledger_event(
            run_dir,
            {
                "schema_version": "runtime-event-v1",
                "event_id": "runtimeevt-" + stable_hash(args.run_id, args.round_id, "promotion-gate", utc_now_iso())[:12],
                "event_type": "promotion-gate",
                "run_id": args.run_id,
                "round_id": args.round_id,
                "started_at_utc": payload.get("generated_at_utc"),
                "completed_at_utc": payload.get("generated_at_utc"),
                "status": "completed",
                "gate_status": payload.get("gate_status"),
                "readiness_status": payload.get("readiness_status"),
                "promote_allowed": bool(payload.get("promote_allowed")),
                "gate_path": payload.get("output_path"),
            },
        )
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "run-phase2-round":
        try:
            payload = run_phase2_round(run_dir, run_id=args.run_id, round_id=args.round_id)
        except SkillExecutionError as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "supervise-round":
        try:
            payload = supervise_round(run_dir, run_id=args.run_id, round_id=args.round_id)
        except SkillExecutionError as exc:
            failure = {"status": "failed", "summary": {"run_id": args.run_id, "round_id": args.round_id}, "message": str(exc)}
            print(pretty_json(failure, args.pretty))
            return 1
        print(pretty_json(payload, args.pretty))
        return 0

    if args.command == "show-run-state":
        payload = show_run_state(run_dir, args.tail)
        print(pretty_json(payload, args.pretty))
        return 0

    return 1
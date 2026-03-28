from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .executor import SkillExecutionError, run_skill
from .ledger import load_ledger_tail
from .manifest import init_round_cursor, init_run_manifest, load_json_if_exists
from .paths import cursor_path, ensure_runtime_dirs, ledger_path, manifest_path, registry_path, resolve_run_dir
from .registry import write_registry


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
    return {
        "status": "completed",
        "summary": {"run_dir": str(run_dir), "ledger_events": len(load_ledger_tail(run_dir, 1000000)) if ledger_path(run_dir).exists() else 0},
        "manifest": manifest,
        "cursor": cursor,
        "registry": registry,
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

    if args.command == "show-run-state":
        payload = show_run_state(run_dir, args.tail)
        print(pretty_json(payload, args.pretty))
        return 0

    return 1
"""CLI assembly and dispatch for the legacy contract entrypoint."""

from __future__ import annotations

import argparse

from eco_council_runtime import contract as contract_module


def add_pretty_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")


def build_contract_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate and scaffold eco-council shared contracts.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_list = subparsers.add_parser("list-kinds", help="List canonical object kinds.")
    add_pretty_flag(parser_list)

    parser_write = subparsers.add_parser("write-example", help="Write one example payload to disk.")
    parser_write.add_argument("--kind", required=True, choices=contract_module.OBJECT_KINDS)
    parser_write.add_argument("--output", required=True, help="Output JSON path.")
    add_pretty_flag(parser_write)

    parser_validate = subparsers.add_parser("validate", help="Validate one JSON file.")
    parser_validate.add_argument("--kind", required=True, choices=contract_module.OBJECT_KINDS)
    parser_validate.add_argument("--input", required=True, help="Input JSON path.")
    add_pretty_flag(parser_validate)

    parser_init_db = subparsers.add_parser("init-db", help="Initialize the canonical SQLite database.")
    parser_init_db.add_argument("--db", required=True, help="SQLite database path.")
    add_pretty_flag(parser_init_db)

    parser_scaffold = subparsers.add_parser("scaffold-run", help="Scaffold one eco-council run directory.")
    parser_scaffold.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold.add_argument("--run-id", required=True, help="Stable run identifier.")
    parser_scaffold.add_argument("--topic", required=True, help="Mission topic.")
    parser_scaffold.add_argument("--objective", required=True, help="Mission objective.")
    parser_scaffold.add_argument("--start-utc", required=True, help="Mission start datetime in UTC.")
    parser_scaffold.add_argument("--end-utc", required=True, help="Mission end datetime in UTC.")
    parser_scaffold.add_argument("--region-label", required=True, help="Human-readable region label.")
    parser_scaffold.add_argument("--point", help="Point geometry as latitude,longitude.")
    parser_scaffold.add_argument("--bbox", help="BBox geometry as west,south,east,north.")
    add_pretty_flag(parser_scaffold)

    parser_scaffold_mission = subparsers.add_parser(
        "scaffold-run-from-mission",
        help="Scaffold one eco-council run directory from an existing mission JSON payload.",
    )
    parser_scaffold_mission.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold_mission.add_argument("--mission-input", required=True, help="Mission JSON path.")
    parser_scaffold_mission.add_argument(
        "--tasks-input",
        default="",
        help="Optional JSON path containing initial round-task list for round-001.",
    )
    add_pretty_flag(parser_scaffold_mission)

    parser_scaffold_round = subparsers.add_parser(
        "scaffold-round",
        help="Scaffold one additional round directory from a validated round-task list.",
    )
    parser_scaffold_round.add_argument("--run-dir", required=True, help="Run directory.")
    parser_scaffold_round.add_argument("--round-id", required=True, help="Round identifier, for example round-002.")
    parser_scaffold_round.add_argument("--tasks-input", required=True, help="JSON path containing round-task list.")
    parser_scaffold_round.add_argument(
        "--mission-input",
        default="",
        help="Optional mission JSON path. Defaults to <run-dir>/mission.json.",
    )
    add_pretty_flag(parser_scaffold_round)

    parser_bundle = subparsers.add_parser(
        "validate-bundle",
        help="Validate a scaffolded run bundle and any canonical files already produced.",
    )
    parser_bundle.add_argument("--run-dir", required=True, help="Run directory to inspect.")
    add_pretty_flag(parser_bundle)

    return parser


def contract_command_map() -> dict[str, object]:
    return {
        "list-kinds": contract_module.command_list_kinds,
        "write-example": contract_module.command_write_example,
        "validate": contract_module.command_validate,
        "init-db": contract_module.command_init_db,
        "scaffold-run": contract_module.command_scaffold_run,
        "scaffold-run-from-mission": contract_module.command_scaffold_run_from_mission,
        "scaffold-round": contract_module.command_scaffold_round,
        "validate-bundle": contract_module.command_validate_bundle,
    }


def run_contract_cli(argv: list[str] | None = None) -> int:
    parser = build_contract_parser()
    args = parser.parse_args(argv)
    command_map = contract_command_map()
    try:
        payload = command_map[args.command](args)
    except Exception as exc:
        error_payload = {
            "command": args.command,
            "ok": False,
            "error": str(exc),
        }
        print(contract_module.pretty_json(error_payload, pretty=getattr(args, "pretty", False)))
        return 1

    result = {
        "command": args.command,
        "ok": True,
        "payload": payload,
    }
    print(contract_module.pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0

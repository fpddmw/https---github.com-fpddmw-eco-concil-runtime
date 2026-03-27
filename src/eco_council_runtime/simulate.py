#!/usr/bin/env python3
"""Deterministic raw-data simulator for eco-council rounds."""

from __future__ import annotations

import argparse

from eco_council_runtime.application import simulation_workflow as application_simulation_workflow

MODE_VALUES = application_simulation_workflow.MODE_VALUES
pretty_json = application_simulation_workflow.pretty_json
command_list_presets = application_simulation_workflow.command_list_presets
command_write_preset = application_simulation_workflow.command_write_preset
command_simulate_round = application_simulation_workflow.command_simulate_round


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministically simulate eco-council raw fetch artifacts.")
    sub = parser.add_subparsers(dest="command", required=True)

    list_presets = sub.add_parser("list-presets", help="List built-in simulation presets.")
    list_presets.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    write_preset = sub.add_parser("write-preset", help="Copy one built-in preset to a writable JSON file.")
    write_preset.add_argument("--preset", required=True, help="Preset name or scenario_id.")
    write_preset.add_argument("--output", required=True, help="Output JSON path.")
    write_preset.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    simulate = sub.add_parser("simulate-round", help="Write simulated raw artifacts and canonical fetch_execution.json for one round.")
    simulate.add_argument("--run-dir", required=True, help="Eco-council run directory.")
    simulate.add_argument("--round-id", default="", help="Round identifier. Defaults to latest round.")
    scenario_group = simulate.add_mutually_exclusive_group()
    scenario_group.add_argument("--scenario-input", default="", help="Custom scenario JSON path.")
    scenario_group.add_argument("--preset", default="", help="Built-in preset name or scenario_id.")
    simulate.add_argument("--claim-type", default="", help="Optional claim type when using auto scenario mode.")
    simulate.add_argument("--mode", default="support", choices=sorted(MODE_VALUES), help="Auto-scenario mode.")
    simulate.add_argument("--seed", type=int, default=None, help="Optional deterministic seed override.")
    simulate.add_argument("--continue-on-error", action="store_true", help="Continue simulating later steps after a failure.")
    overwrite_group = simulate.add_mutually_exclusive_group()
    overwrite_group.add_argument("--overwrite", action="store_true", help="Overwrite any existing artifact paths.")
    overwrite_group.add_argument("--skip-existing", action="store_true", help="Mark existing artifacts as skipped/artifact_exists.")
    simulate.add_argument("--skip-input-check", action="store_true", help="Skip fetch_plan input snapshot validation.")
    simulate.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "list-presets": command_list_presets,
        "write-preset": command_write_preset,
        "simulate-round": command_simulate_round,
    }
    try:
        payload = handlers[args.command](args)
    except Exception as exc:
        result = {"command": args.command, "ok": False, "error": str(exc)}
        print(pretty_json(result, pretty=getattr(args, "pretty", False)))
        return 1
    result = {"command": args.command, "ok": True, "payload": payload}
    print(pretty_json(result, pretty=getattr(args, "pretty", False)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Compatibility wrapper re-exporting the supervisor CLI builder."""

from __future__ import annotations

import argparse

from eco_council_runtime.cli.supervisor_cli import build_supervisor_parser as build_runtime_supervisor_parser


def build_supervisor_parser() -> argparse.ArgumentParser:
    return build_runtime_supervisor_parser()


__all__ = ["build_supervisor_parser"]

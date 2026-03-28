#!/usr/bin/env python3
"""Compatibility facade for reporting runtime entrypoints and exports."""

from eco_council_runtime.application.reporting.runtime_cli import *  # noqa: F401,F403
from eco_council_runtime.application.reporting.runtime_cli import main


if __name__ == "__main__":
    raise SystemExit(main())

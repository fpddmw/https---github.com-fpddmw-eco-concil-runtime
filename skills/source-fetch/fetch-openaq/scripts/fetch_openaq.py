#!/usr/bin/env python3
"""Compatibility wrapper so runtime tooling can invoke fetch-openaq by skill name."""

from __future__ import annotations

from openaq_router import main


if __name__ == "__main__":
    raise SystemExit(main())

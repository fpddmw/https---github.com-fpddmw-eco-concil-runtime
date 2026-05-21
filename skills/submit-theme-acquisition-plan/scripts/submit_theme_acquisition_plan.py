#!/usr/bin/env python3
"""Submit an investigator-authored or investigator-adopted acquisition plan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SKILL_NAME = "submit-theme-acquisition-plan"
OBJECT_KIND = "theme-acquisition-plan"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.dynamic_investigation_submission_support import main_for_dynamic_skill  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(
        main_for_dynamic_skill(
            skill_name=SKILL_NAME,
            object_kind=OBJECT_KIND,
            default_author_role="social-investigator",
        )
    )

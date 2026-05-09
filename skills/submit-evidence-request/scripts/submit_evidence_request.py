#!/usr/bin/env python3
"""Submit one evidence-request coordination object."""

from __future__ import annotations

import sys
from pathlib import Path

SKILL_NAME = "submit-evidence-request"
OBJECT_KIND = "evidence-request"
DEFAULT_AUTHOR_ROLE = "moderator"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.dynamic_investigation_submission_support import (  # noqa: E402
    main_for_dynamic_skill,
)


if __name__ == "__main__":
    raise SystemExit(
        main_for_dynamic_skill(
            skill_name=SKILL_NAME,
            object_kind=OBJECT_KIND,
            default_author_role=DEFAULT_AUTHOR_ROLE,
        )
    )

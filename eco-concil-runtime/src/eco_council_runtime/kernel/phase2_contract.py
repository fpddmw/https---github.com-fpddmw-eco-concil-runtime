from __future__ import annotations

# Compatibility-only facade. Canonical phase-2 stage defaults and validation
# now live outside kernel so controller/runtime no longer own the default
# deliberation stage profile.
from ..phase2_stage_profile import (  # noqa: F401
    DEFAULT_PHASE2_PLANNER_SKILL_NAME,
    DEFAULT_PHASE2_STAGE_DEFINITIONS,
    default_gate_steps,
    default_post_gate_steps,
    expected_output_path,
    lookup_stage_contract,
    maybe_text,
    normalized_required_previous_stages,
    resolve_stage_definitions,
    stage_contract,
    validate_skill_stage,
    validate_stage_blueprints,
    validate_stage_sequence,
)

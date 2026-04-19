#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from eco_council_runtime.kernel.cli import main
from eco_council_runtime.phase2_agent_entry_profile import (
    default_phase2_agent_entry_profile,
)
from eco_council_runtime.phase2_agent_handoff import (
    default_phase2_entry_chain,
    default_phase2_hard_gate_commands,
)
from eco_council_runtime.phase2_posture_profile import (
    default_phase2_posture_profile,
)
from eco_council_runtime.phase2_planning_profile import default_phase2_planning_sources
from eco_council_runtime.phase2_gate_profile import phase2_gate_handler_registry
from eco_council_runtime.phase2_stage_profile import DEFAULT_PHASE2_STAGE_DEFINITIONS


if __name__ == "__main__":
    raise SystemExit(
        main(
            default_gate_handlers=phase2_gate_handler_registry(),
            default_agent_entry_profile=default_phase2_agent_entry_profile(),
            default_posture_profile=default_phase2_posture_profile(),
            hard_gate_command_builder=default_phase2_hard_gate_commands,
            entry_chain_builder=default_phase2_entry_chain,
            default_planning_sources=default_phase2_planning_sources(),
            default_stage_definitions=DEFAULT_PHASE2_STAGE_DEFINITIONS,
        )
    )

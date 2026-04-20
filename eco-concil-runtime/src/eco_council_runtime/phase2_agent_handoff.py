from __future__ import annotations

from pathlib import Path
from typing import Callable

from .runtime_command_hints import kernel_command, run_skill_command

HardGateCommandBuilder = Callable[..., dict[str, str]]
EntryChainBuilder = Callable[..., list[dict[str, str]]]


def default_phase2_hard_gate_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    next_round_id: str,
    contract_mode: str,
) -> dict[str, str]:
    return {
        "show_run_state": kernel_command(
            "show-run-state",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--tail",
            "20",
            "--pretty",
        ),
        "supervise_round": kernel_command(
            "supervise-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "apply_promotion_gate": kernel_command(
            "apply-promotion-gate",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "close_round": kernel_command(
            "close-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "open_next_round": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=next_round_id,
            skill_name="eco-open-investigation-round",
            contract_mode=contract_mode,
            skill_args=["--source-round-id", round_id],
        ),
    }


def default_phase2_entry_chain(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
    next_round_id: str,
) -> list[dict[str, str]]:
    return [
        {
            "step_id": "inspect-runtime",
            "mode": "runtime-gate",
            "objective": "Inspect governance, health, and current round posture before agent work begins.",
            "command": kernel_command(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                round_id,
                "--tail",
                "20",
                "--pretty",
            ),
        },
        {
            "step_id": "read-board-delta",
            "mode": "advisory-read",
            "objective": "Read the current round directly from deliberation-plane state instead of depending on stale summaries.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-read-board-delta",
                contract_mode=contract_mode,
                skill_args=["--include-closed", "--event-limit", "20"],
            ),
        },
        {
            "step_id": "query-shared-planes",
            "mode": "advisory-read",
            "objective": "Read normalized public/environment signals and current analysis result sets without leaving governed runtime surfaces.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-query-public-signals",
                contract_mode=contract_mode,
            ),
        },
        {
            "step_id": "materialize-agent-advisory-plan",
            "mode": "advisory-plan",
            "objective": "Refresh one advisory-only orchestration plan so the agent route has an explicit but non-binding next-step hint.",
            "command": kernel_command(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--refresh-advisory-plan",
                "--pretty",
            ),
        },
        {
            "step_id": "submit-council-proposal",
            "mode": "governed-write",
            "objective": "Push findings back as one structured council proposal in the deliberation DB instead of hiding judgement in freeform notes or in-memory state.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-submit-council-proposal",
                contract_mode=contract_mode,
                skill_args=[
                    "--agent-role",
                    "moderator",
                    "--proposal-kind",
                    "<proposal_kind>",
                    "--rationale",
                    "<rationale>",
                    "--target-kind",
                    "<target_kind>",
                    "--target-id",
                    "<target_id>",
                ],
            ),
        },
        {
            "step_id": "submit-readiness-opinion",
            "mode": "governed-write",
            "objective": "When the council is making a readiness judgement, submit it as a DB-backed readiness opinion before returning to runtime gates.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-submit-readiness-opinion",
                contract_mode=contract_mode,
                skill_args=[
                    "--agent-role",
                    "moderator",
                    "--readiness-status",
                    "<ready|needs-more-data|blocked>",
                    "--rationale",
                    "<rationale>",
                    "--basis-object-id",
                    "<basis_object_id>",
                ],
            ),
        },
        {
            "step_id": "return-to-runtime-gate",
            "mode": "runtime-gate",
            "objective": "Hand the updated round back to the runtime supervisor or promotion gate for a governed decision.",
            "command": kernel_command(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--pretty",
            ),
        },
        {
            "step_id": "open-follow-up-round",
            "mode": "governed-write",
            "objective": "If the round remains open, advance the investigation with an explicit next-round transition rather than implicit carryover.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=next_round_id,
                skill_name="eco-open-investigation-round",
                contract_mode=contract_mode,
                skill_args=["--source-round-id", round_id],
            ),
        },
    ]

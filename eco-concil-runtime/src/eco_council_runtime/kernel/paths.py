from __future__ import annotations

from pathlib import Path


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def runtime_dir(run_dir: Path) -> Path:
    return run_dir / "runtime"


def receipts_dir(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "receipts"


def dead_letters_dir(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "dead_letters"


def manifest_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "run_manifest.json"


def cursor_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "round_cursor.json"


def ledger_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "audit_ledger.jsonl"


def execution_lock_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "execution.lock"


def registry_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "skill_registry.json"


def report_basis_gate_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"report_basis_gate_{round_id}.json"


def orchestration_plan_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"orchestration_plan_{round_id}.json"


def mission_scaffold_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"mission_scaffold_{round_id}.json"


def agent_entry_gate_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"agent_entry_gate_{round_id}.json"


def controller_state_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"round_controller_{round_id}.json"


def supervisor_state_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"supervisor_state_{round_id}.json"


def round_close_state_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"round_close_{round_id}.json"


def history_bootstrap_state_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"history_bootstrap_{round_id}.json"


def scenario_fixture_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"scenario_fixture_{round_id}.json"


def scenario_baseline_manifest_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"scenario_baseline_manifest_{round_id}.json"


def benchmark_manifest_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"benchmark_manifest_{round_id}.json"


def replay_report_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"replay_report_{round_id}.json"


def benchmark_compare_path(run_dir: Path, round_id: str) -> Path:
    return runtime_dir(run_dir) / f"benchmark_compare_{round_id}.json"


def admission_policy_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "admission_policy.json"


def runtime_health_path(run_dir: Path) -> Path:
    return runtime_dir(run_dir) / "runtime_health.json"


def operator_runbook_path(run_dir: Path, round_id: str = "") -> Path:
    if round_id:
        return runtime_dir(run_dir) / f"operator_runbook_{round_id}.md"
    return runtime_dir(run_dir) / "operator_runbook.md"


def dead_letter_path(run_dir: Path, dead_letter_id: str) -> Path:
    return dead_letters_dir(run_dir) / f"{dead_letter_id}.json"


def receipt_path(run_dir: Path, receipt_id: str) -> Path:
    return receipts_dir(run_dir) / f"{receipt_id}.json"


def ensure_runtime_dirs(run_dir: Path) -> None:
    runtime_dir(run_dir).mkdir(parents=True, exist_ok=True)
    receipts_dir(run_dir).mkdir(parents=True, exist_ok=True)
    dead_letters_dir(run_dir).mkdir(parents=True, exist_ok=True)

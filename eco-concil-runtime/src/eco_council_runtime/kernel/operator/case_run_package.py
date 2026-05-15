from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists, write_json
from eco_council_runtime.kernel.core.paths import (
    agent_entry_gate_path,
    case_run_package_path,
    cursor_path,
    ensure_runtime_dirs,
    history_bootstrap_state_path,
    ledger_path,
    manifest_path,
    mission_scaffold_path,
    operator_runbook_path,
    round_close_state_path,
    runtime_health_path,
)
from eco_council_runtime.kernel.operator.operations_common import maybe_text, utc_now_iso
from eco_council_runtime.kernel.operator.runbook import materialize_operator_runbook
from eco_council_runtime.kernel.operator.runtime_health import materialize_runtime_health


def _resolved_run_and_round(run_dir: Path, run_id: str, round_id: str) -> tuple[str, str]:
    manifest = load_json_if_exists(manifest_path(run_dir)) or {}
    cursor = load_json_if_exists(cursor_path(run_dir)) or {}
    resolved_run_id = maybe_text(run_id) or maybe_text(manifest.get("run_id")) or maybe_text(cursor.get("run_id"))
    resolved_round_id = maybe_text(round_id) or maybe_text(cursor.get("round_id"))
    return resolved_run_id, resolved_round_id


def _artifact_entry(path: Path, *, artifact_kind: str, required: bool = False) -> dict[str, Any]:
    resolved = path.resolve()
    exists = resolved.exists()
    entry: dict[str, Any] = {
        "artifact_kind": artifact_kind,
        "artifact_path": str(resolved),
        "exists": exists,
        "required_for_package": bool(required),
    }
    if exists and resolved.is_file():
        entry["file_size_bytes"] = resolved.stat().st_size
    return entry


def _glob_entries(run_dir: Path, pattern: str, *, artifact_kind: str, limit: int = 20) -> list[dict[str, Any]]:
    return [
        _artifact_entry(path, artifact_kind=artifact_kind)
        for path in sorted(run_dir.glob(pattern))[: max(1, int(limit or 20))]
        if path.is_file()
    ]


def _reporting_artifacts(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    reporting_dir = run_dir / "reporting"
    names = [
        ("reporting-handoff", f"reporting_handoff_{round_id}.json"),
        ("council-decision-draft", f"council_decision_draft_{round_id}.json"),
        ("council-decision", f"council_decision_{round_id}.json"),
        ("expert-report-social", f"expert_report_social_investigator_{round_id}.json"),
        ("expert-report-environmental", f"expert_report_environmental_investigator_{round_id}.json"),
        ("narrative-report-draft-json", f"narrative_report_draft_{round_id}.json"),
        ("narrative-report-draft-md", f"narrative_report_draft_{round_id}.md"),
        ("narrative-report-validation", f"narrative_report_validation_{round_id}.json"),
        ("narrative-report-json", f"narrative_report_{round_id}.json"),
        ("narrative-report-md", f"narrative_report_{round_id}.md"),
        ("final-publication", f"final_publication_{round_id}.json"),
    ]
    return [
        _artifact_entry(reporting_dir / file_name, artifact_kind=artifact_kind)
        for artifact_kind, file_name in names
    ]


def _existing_count(groups: dict[str, list[dict[str, Any]]]) -> int:
    return sum(
        1
        for entries in groups.values()
        for entry in entries
        if isinstance(entry, dict) and bool(entry.get("exists"))
    )


def _existing_artifact_kinds(groups: dict[str, list[dict[str, Any]]]) -> set[str]:
    return {
        maybe_text(entry.get("artifact_kind"))
        for entries in groups.values()
        for entry in entries
        if isinstance(entry, dict) and bool(entry.get("exists"))
    }


def _checklist_item(
    *,
    checklist_id: str,
    title: str,
    purpose: str,
    artifact_kinds: list[str],
    command_keys: list[str],
    existing_kinds: set[str],
    optional: bool = False,
) -> dict[str, Any]:
    observed = [kind for kind in artifact_kinds if kind in existing_kinds]
    missing = [kind for kind in artifact_kinds if kind not in existing_kinds]
    return {
        "checklist_id": checklist_id,
        "title": title,
        "purpose": purpose,
        "artifact_kinds": artifact_kinds,
        "observed_artifact_kinds": observed,
        "missing_artifact_kinds": missing,
        "command_keys": command_keys,
        "optional": bool(optional),
        "status": "complete" if not missing else "partial" if observed or optional else "not-started",
    }


def _operator_checklists(
    artifact_groups: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    existing_kinds = _existing_artifact_kinds(artifact_groups)
    return [
        _checklist_item(
            checklist_id="case-run-start",
            title="Case Run Start Checklist",
            purpose=(
                "Human/operator review of mission, runbook, runtime health, and "
                "agent-entry surfaces before or during a case run."
            ),
            artifact_kinds=[
                "mission",
                "operator-runbook",
                "runtime-health",
                "agent-entry-gate",
                "audit-ledger",
            ],
            command_keys=["refresh_runtime_health", "refresh_operator_runbook"],
            existing_kinds=existing_kinds,
        ),
        _checklist_item(
            checklist_id="report-publication",
            title="Report Publication Checklist",
            purpose=(
                "Report-editor review of draft, validation, published report, and "
                "final publication artifacts from frozen/reporting basis only."
            ),
            artifact_kinds=[
                "narrative-report-draft-json",
                "narrative-report-validation",
                "narrative-report-json",
                "narrative-report-md",
                "final-publication",
            ],
            command_keys=["show_reporting_state"],
            existing_kinds=existing_kinds,
        ),
        _checklist_item(
            checklist_id="case-archive",
            title="Case Archive Checklist",
            purpose=(
                "Archive review of closeout state, history bootstrap, archive "
                "artifacts, runtime health, and rebuildable package metadata."
            ),
            artifact_kinds=[
                "round-close-state",
                "history-bootstrap-state",
                "archive-artifact",
                "archive-db",
                "runtime-health",
            ],
            command_keys=[
                "show_archive_status",
                "refresh_runtime_health",
                "rebuild_case_run_package",
            ],
            existing_kinds=existing_kinds,
            optional=True,
        ),
        _checklist_item(
            checklist_id="showcase-review",
            title="Showcase Review Checklist",
            purpose=(
                "Demonstration review of final report, runtime health, public "
                "discourse addenda, and descriptive environment aggregation artifacts."
            ),
            artifact_kinds=[
                "narrative-report-md",
                "runtime-health",
                "public-discourse-analysis",
                "environment-evidence-aggregation",
            ],
            command_keys=["show_reporting_state", "refresh_runtime_health"],
            existing_kinds=existing_kinds,
            optional=True,
        ),
    ]


def materialize_case_run_package(
    run_dir: Path,
    *,
    run_id: str = "",
    round_id: str = "",
) -> dict[str, Any]:
    from eco_council_runtime.runtime_command_hints import kernel_command

    ensure_runtime_dirs(run_dir)
    resolved_run_id, resolved_round_id = _resolved_run_and_round(run_dir, run_id, round_id)
    health = materialize_runtime_health(run_dir, round_id=resolved_round_id)
    runbook_path = Path(materialize_operator_runbook(run_dir, round_id=resolved_round_id))
    output_file = case_run_package_path(run_dir, resolved_round_id)
    artifact_groups = {
        "mission": [
            _artifact_entry(run_dir / "mission.json", artifact_kind="mission"),
            _artifact_entry(run_dir / "input" / "mission.json", artifact_kind="mission-input"),
            _artifact_entry(
                mission_scaffold_path(run_dir, resolved_round_id),
                artifact_kind="mission-scaffold",
            ),
        ],
        "runtime": [
            _artifact_entry(runtime_health_path(run_dir), artifact_kind="runtime-health", required=True),
            _artifact_entry(runbook_path, artifact_kind="operator-runbook", required=True),
            _artifact_entry(ledger_path(run_dir), artifact_kind="audit-ledger", required=True),
            _artifact_entry(
                agent_entry_gate_path(run_dir, resolved_round_id),
                artifact_kind="agent-entry-gate",
            ),
            _artifact_entry(
                round_close_state_path(run_dir, resolved_round_id),
                artifact_kind="round-close-state",
            ),
            _artifact_entry(
                history_bootstrap_state_path(run_dir, resolved_round_id),
                artifact_kind="history-bootstrap-state",
            ),
        ],
        "reporting": _reporting_artifacts(run_dir, resolved_round_id),
        "analytics": [
            *_glob_entries(
                run_dir,
                "analytics/public_discourse_*",
                artifact_kind="public-discourse-analysis",
            ),
            *_glob_entries(
                run_dir,
                "analytics/environment_evidence_aggregation_*",
                artifact_kind="environment-evidence-aggregation",
            ),
            *_glob_entries(
                run_dir,
                "analytics/*relation*",
                artifact_kind="environment-or-relation-analysis",
            ),
        ],
        "archive": [
            *_glob_entries(run_dir, "archive/*.json", artifact_kind="archive-artifact"),
            *_glob_entries(run_dir, "../archives/*.sqlite", artifact_kind="archive-db"),
        ],
    }
    warnings: list[dict[str, str]] = []
    if not resolved_run_id:
        warnings.append({"code": "run-id-not-resolved", "message": "No run_id was supplied or found in runtime manifest/cursor."})
    if not resolved_round_id:
        warnings.append({"code": "round-id-not-resolved", "message": "No round_id was supplied or found in runtime cursor."})
    if not any(entry.get("exists") for entry in artifact_groups["reporting"]):
        warnings.append({"code": "no-reporting-artifacts", "message": "No reporting artifacts were present for the selected round."})
    if maybe_text(health.get("alert_status")) != "green":
        warnings.append({"code": "runtime-health-not-green", "message": "Runtime health is not green; inspect dead letters and health alerts before treating the package as final."})
    payload = {
        "schema_version": "case-run-package-manifest-v1",
        "status": "completed",
        "generated_at_utc": utc_now_iso(),
        "run_id": resolved_run_id,
        "round_id": resolved_round_id,
        "package_semantics": (
            "Operator-visible case manifest for demonstration, archive, and review. "
            "It lists artifacts and commands but does not copy evidence, rank sources, "
            "select evidence, decide report readiness, or fix an investigation agenda."
        ),
        "checklist_semantics": (
            "Checklists are human/operator review aids for case execution, reporting, archive, "
            "and demonstration packaging. They do not prescribe sources, agent order, round count, "
            "or conclusions."
        ),
        "does_not_decide": [
            "truth",
            "source selection",
            "source ranking",
            "evidence sufficiency",
            "report readiness",
            "case conclusion",
        ],
        "artifact_groups": artifact_groups,
        "operator_checklists": _operator_checklists(artifact_groups),
        "screenshot_slots": [
            {"slot_id": "report", "suggested_artifact_kind": "narrative-report-md"},
            {"slot_id": "runtime-health", "suggested_artifact_kind": "runtime-health"},
            {"slot_id": "agent-entry", "suggested_artifact_kind": "agent-entry-gate"},
            {"slot_id": "council-status", "suggested_command": "show-council-status"},
        ],
        "operator_commands": {
            "refresh_runtime_health": kernel_command(
                "materialize-runtime-health",
                "--run-dir",
                str(run_dir),
                "--round-id",
                resolved_round_id,
                actor_role="runtime-operator",
            ),
            "refresh_operator_runbook": kernel_command(
                "materialize-operator-runbook",
                "--run-dir",
                str(run_dir),
                "--round-id",
                resolved_round_id,
                actor_role="runtime-operator",
            ),
            "show_reporting_state": kernel_command(
                "show-reporting-state",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
                "--pretty",
            ),
            "show_archive_status": kernel_command(
                "show-archive-status",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
                "--pretty",
            ),
            "rebuild_case_run_package": kernel_command(
                "materialize-case-run-package",
                "--run-dir",
                str(run_dir),
                "--run-id",
                resolved_run_id,
                "--round-id",
                resolved_round_id,
                actor_role="runtime-operator",
            ),
        },
        "runtime_health_summary": health.get("summary", {}) if isinstance(health.get("summary"), dict) else {},
        "warnings": warnings,
        "output_path": str(output_file.resolve()),
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "run_id": resolved_run_id,
            "round_id": resolved_round_id,
            "output_path": str(output_file.resolve()),
            "artifact_group_count": len(artifact_groups),
            "existing_artifact_count": _existing_count(artifact_groups),
            "warning_count": len(warnings),
        },
        "case_run_package": payload,
        "case_run_package_path": str(output_file.resolve()),
        "warnings": warnings,
    }


__all__ = (
    "materialize_case_run_package",
)

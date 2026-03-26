"""Build canonical reporting and normalization artifacts for supervisor stages."""

from __future__ import annotations

from pathlib import Path

from eco_council_runtime.controller.io import run_json_command
from eco_council_runtime.layout import (
    CONTRACT_SCRIPT_PATH,
    NORMALIZE_SCRIPT_PATH,
    PROJECT_DIR,
    REPORTING_SCRIPT_PATH,
)

REPO_DIR = PROJECT_DIR


def materialize_curations_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(NORMALIZE_SCRIPT_PATH),
            "materialize-curations",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(NORMALIZE_SCRIPT_PATH),
            "build-round-context",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )


def build_data_readiness_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "build-data-readiness-packets",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(CONTRACT_SCRIPT_PATH),
            "validate-bundle",
            "--run-dir",
            str(run_dir),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )


def build_matching_authorization_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "build-matching-authorization-packet",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )


def build_matching_adjudication_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(NORMALIZE_SCRIPT_PATH),
            "prepare-matching-adjudication",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "build-matching-adjudication-packet",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )


def build_investigation_review_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "build-investigation-review-packet",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(CONTRACT_SCRIPT_PATH),
            "validate-bundle",
            "--run-dir",
            str(run_dir),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )


def build_report_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "build-report-packets",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(CONTRACT_SCRIPT_PATH),
            "validate-bundle",
            "--run-dir",
            str(run_dir),
            "--pretty",
        ],
        cwd=REPO_DIR,
    )


def build_decision_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "build-decision-packet",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--prefer-draft-reports",
            "--pretty",
        ],
        cwd=REPO_DIR,
    )
    run_json_command(
        [
            "python3",
            str(REPORTING_SCRIPT_PATH),
            "render-openclaw-prompts",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ],
        cwd=REPO_DIR,
    )

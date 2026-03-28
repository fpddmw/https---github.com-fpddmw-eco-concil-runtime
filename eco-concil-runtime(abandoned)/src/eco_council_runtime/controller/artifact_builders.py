"""Build canonical reporting and normalization artifacts for supervisor stages."""

from __future__ import annotations

from pathlib import Path

from eco_council_runtime.cli_invocation import runtime_module_argv
from eco_council_runtime.controller.io import run_json_command
from eco_council_runtime.layout import PROJECT_DIR

REPO_DIR = PROJECT_DIR


def materialize_curations_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "normalize",
            "materialize-curations",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "normalize",
            "build-round-context",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )


def build_data_readiness_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "reporting",
            "build-data-readiness-packets",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "render-openclaw-prompts",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "contract",
            "validate-bundle",
            "--run-dir",
            run_dir,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )


def build_matching_authorization_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "reporting",
            "build-matching-authorization-packet",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "render-openclaw-prompts",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )


def build_matching_adjudication_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "normalize",
            "prepare-matching-adjudication",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "build-matching-adjudication-packet",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "render-openclaw-prompts",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )


def build_investigation_review_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "reporting",
            "build-investigation-review-packet",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "render-openclaw-prompts",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "contract",
            "validate-bundle",
            "--run-dir",
            run_dir,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )


def build_report_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "reporting",
            "build-report-packets",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "render-openclaw-prompts",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "contract",
            "validate-bundle",
            "--run-dir",
            run_dir,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )


def build_decision_artifacts_for_supervisor(run_dir: Path, round_id: str) -> None:
    run_json_command(
        runtime_module_argv(
            "reporting",
            "build-decision-packet",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--prefer-draft-reports",
            "--pretty",
        ),
        cwd=REPO_DIR,
    )
    run_json_command(
        runtime_module_argv(
            "reporting",
            "render-openclaw-prompts",
            "--run-dir",
            run_dir,
            "--round-id",
            round_id,
            "--pretty",
        ),
        cwd=REPO_DIR,
    )

"""Shared filesystem layout helpers for the standalone eco-council runtime."""

from __future__ import annotations

from pathlib import Path
import os

PACKAGE_DIR = Path(__file__).resolve().parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_DIR = SRC_DIR.parent
ASSETS_ROOT = PROJECT_DIR / "assets"
CONTRACT_ASSETS_DIR = ASSETS_ROOT / "contract"
NORMALIZE_ASSETS_DIR = ASSETS_ROOT / "normalize"
SUPERVISOR_ASSETS_DIR = ASSETS_ROOT / "supervisor"
SIMULATE_ASSETS_DIR = ASSETS_ROOT / "simulate"
DOCS_ROOT = PROJECT_DIR / "docs"
SCRIPTS_DIR = PROJECT_DIR / "scripts"
RUNS_ROOT = Path(os.environ.get("ECO_COUNCIL_RUNS_ROOT", PROJECT_DIR / "runs")).expanduser().resolve()
CONTRACT_EXAMPLES_DIR = CONTRACT_ASSETS_DIR / "examples"
CONTRACT_DDL_PATH = CONTRACT_ASSETS_DIR / "sqlite" / "eco_council.sql"
CONTRACT_SCHEMA_PATH = CONTRACT_ASSETS_DIR / "schemas" / "eco_council.schema.json"
NORMALIZE_PUBLIC_DDL_PATH = NORMALIZE_ASSETS_DIR / "sqlite" / "public_signals.sql"
NORMALIZE_ENVIRONMENT_DDL_PATH = NORMALIZE_ASSETS_DIR / "sqlite" / "environment_signals.sql"
SUPERVISOR_CASE_LIBRARY_DDL_PATH = SUPERVISOR_ASSETS_DIR / "sqlite" / "eco_council_case_library.sql"
SUPERVISOR_SIGNAL_CORPUS_DDL_PATH = SUPERVISOR_ASSETS_DIR / "sqlite" / "eco_council_signal_corpus.sql"
SUPERVISOR_EVAL_CASES_DIR = SUPERVISOR_ASSETS_DIR / "eval-cases"
SIMULATE_SCENARIO_DIR = SIMULATE_ASSETS_DIR / "scenarios"
CONTRACT_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_contract.py"
NORMALIZE_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_normalize.py"
ORCHESTRATE_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_orchestrate.py"
REPORTING_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_reporting.py"
SUPERVISOR_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_supervisor.py"
CASE_LIBRARY_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_case_library.py"
SIGNAL_CORPUS_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_signal_corpus.py"
SIMULATE_SCRIPT_PATH = SCRIPTS_DIR / "eco_council_simulate.py"

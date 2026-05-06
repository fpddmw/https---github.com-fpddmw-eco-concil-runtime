#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = ROOT / "eco-concil-runtime" / "src"

SYNTAX_ROOTS = (
    ROOT / "_workflow_support.py",
    ROOT / "eco-concil-runtime" / "scripts",
    RUNTIME_SRC,
    ROOT / "skills",
    ROOT / "tests",
)

SKIP_DIR_NAMES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
}

TEST_SUITES: dict[str, tuple[str, ...]] = {
    "relation-taxonomy": ("tests.test_spatiotemporal_relation_taxonomy",),
    "optional-guardrails": ("tests.test_optional_analysis_guardrails",),
    "db-recovery": ("tests.test_db_only_recovery",),
    "schema-migration": ("tests.test_schema_migrations",),
    "runtime-governance": (
        "tests.test_runtime_kernel",
        "tests.test_skill_approval_workflow",
    ),
    "reporting": (
        "tests.test_reporting_contracts",
        "tests.test_reporting_workflow",
        "tests.test_reporting_publish_workflow",
        "tests.test_reporting_query_surface",
    ),
    "case-study": (
        "tests.test_benchmark_replay_workflow",
        "tests.test_policy_research_case_fixtures",
    ),
}

DEFAULT_TARGETED_SUITES = (
    "relation-taxonomy",
    "optional-guardrails",
    "db-recovery",
    "schema-migration",
    "runtime-governance",
    "reporting",
    "case-study",
)


def literal_key_identity(node: ast.AST) -> tuple[type[object], object] | None:
    try:
        value = ast.literal_eval(node)
    except (ValueError, TypeError):
        return None
    if isinstance(value, (str, bytes, int, float, complex, bool, type(None))):
        return (type(value), value)
    return None


def duplicate_literal_dict_keys(tree: ast.AST) -> list[tuple[int, int, str]]:
    duplicates: list[tuple[int, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        seen: dict[tuple[type[object], object], ast.AST] = {}
        for key_node in node.keys:
            if key_node is None:
                continue
            key_identity = literal_key_identity(key_node)
            if key_identity is None:
                continue
            if key_identity in seen:
                duplicates.append(
                    (
                        getattr(key_node, "lineno", 0),
                        getattr(key_node, "col_offset", 0) + 1,
                        repr(key_identity[1]),
                    )
                )
            else:
                seen[key_identity] = key_node
    return duplicates


def python_env() -> dict[str, str]:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    path_parts = [str(ROOT), str(RUNTIME_SRC)]
    if existing:
        path_parts.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(path_parts)
    return env


def iter_python_files(paths: Iterable[Path]) -> Iterable[Path]:
    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            if path.suffix == ".py":
                yield path
            continue
        for candidate in sorted(path.rglob("*.py")):
            if any(part in SKIP_DIR_NAMES for part in candidate.parts):
                continue
            yield candidate


def syntax_check() -> int:
    failures: list[tuple[Path, SyntaxError]] = []
    duplicate_key_failures: list[tuple[Path, int, int, str]] = []
    checked = 0
    for path in iter_python_files(SYNTAX_ROOTS):
        checked += 1
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            failures.append((path, exc))
            continue
        for line, column, key_repr in duplicate_literal_dict_keys(tree):
            duplicate_key_failures.append((path, line, column, key_repr))

    if failures or duplicate_key_failures:
        for path, exc in failures:
            location = f"{path}:{exc.lineno}:{exc.offset}"
            print(f"syntax error: {location}: {exc.msg}", file=sys.stderr)
        for path, line, column, key_repr in duplicate_key_failures:
            print(
                f"duplicate literal dict key: {path}:{line}:{column}: {key_repr}",
                file=sys.stderr,
            )
        print(
            (
                "syntax check failed: "
                f"{len(failures)} syntax errors, "
                f"{len(duplicate_key_failures)} duplicate keys, "
                f"{checked} files checked"
            ),
            file=sys.stderr,
        )
        return 1

    print(f"syntax check passed: {checked} Python files, no duplicate literal dict keys")
    return 0


def run_command(args: Sequence[str]) -> int:
    printable = " ".join(args)
    print(f"+ {printable}", flush=True)
    completed = subprocess.run(
        list(args),
        cwd=ROOT,
        env=python_env(),
        check=False,
    )
    return completed.returncode


def run_unittest_modules(modules: Sequence[str]) -> int:
    return run_command([sys.executable, "-m", "unittest", *modules])


def run_test_suites(suites: Sequence[str]) -> int:
    modules: list[str] = []
    for suite in suites:
        modules.extend(TEST_SUITES[suite])
    return run_unittest_modules(modules)


def run_full_unittest() -> int:
    return run_command([sys.executable, "-m", "unittest", "discover", "-s", "tests"])


def run_ci_gate() -> int:
    syntax_status = syntax_check()
    if syntax_status != 0:
        return syntax_status

    targeted_status = run_test_suites(DEFAULT_TARGETED_SUITES)
    if targeted_status != 0:
        return targeted_status

    return run_full_unittest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run OpenClaw repository quality gates.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("syntax", help="Parse-check runtime, skills, and tests without writing pyc files.")
    subparsers.add_parser("full", help="Run the full unittest discovery suite.")
    subparsers.add_parser("ci", help="Run the default CI gate: syntax, targeted tests, then full unittest.")

    test_parser = subparsers.add_parser("test", help="Run one or more targeted unittest suites.")
    test_parser.add_argument(
        "suites",
        nargs="+",
        choices=sorted(TEST_SUITES),
        help="Targeted suite name.",
    )

    list_parser = subparsers.add_parser("list", help="List available targeted suites.")
    list_parser.set_defaults(command="list")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "syntax":
        return syntax_check()
    if args.command == "full":
        return run_full_unittest()
    if args.command == "ci":
        return run_ci_gate()
    if args.command == "test":
        return run_test_suites(args.suites)
    if args.command == "list":
        for suite_name in sorted(TEST_SUITES):
            modules = ", ".join(TEST_SUITES[suite_name])
            print(f"{suite_name}: {modules}")
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

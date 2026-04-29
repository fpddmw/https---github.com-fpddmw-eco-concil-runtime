from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.milestone_package import (
    materialize_milestone_package_from_paths,
    render_milestone_package_from_paths,
)


SYNTHETIC_MASTER_PLAN = """
# Plan

## 4. 历史编号归一化说明

### 5.1 Route A: Runtime / Governance Stabilization

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `A1` | Runtime Baseline | stabilize | `completed` | stable |
| `A2` | Agent Entry Gate | operator entry | `planned` | visible |

### 5.4 Route D: Program Control / Documentation

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `D1` | Demo Package Foundation | package | `completed` | materialized |
| `D2` | Milestone / Demo Packaging | package | `planned` | reusable |

## 7. 推荐的未来数次开发顺序

| 顺序 | 阶段 | 路线 | 为什么先做 | 预期独立交付 |
| --- | --- | --- | --- | --- |
| `1` | `D2` | `D` | package | milestone |
| `2` | `A2` | `A` | operator entry | entry gate |
"""


SYNTHETIC_PROGRESS_LOG = """
# Log

## 2026-04-01 A1: Runtime Baseline

Status: completed

Objective:
- Stabilize the runtime baseline.

Implementation:
- Added baseline runtime state checks.

Validation:
- `python3 -m unittest tests/test_runtime_kernel.py -q`

Known limitations:
- Runtime recovery still depends on fixture-style seed data.

Next:
- Continue `D1`.

## 2026-04-02 D1: Demo Package Foundation

Status: completed

Objective:
- Build the first reusable milestone package foundation.

Implementation:
- Added manifest rendering.

Validation:
- `python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty`
- `pytest tests/test_progress_dashboard.py -q`

Known limitations:
- The package is still a control-plane summary, not a full artifact archive.

Next:
- Move to `D2`.
- After `D2`, continue `A2`.
"""


def write_synthetic_docs(root: Path) -> tuple[Path, Path, Path]:
    master_plan_path = root / "plan.md"
    progress_log_path = root / "log.md"
    dashboard_path = root / "dashboard.md"
    master_plan_path.write_text(SYNTHETIC_MASTER_PLAN.strip() + "\n", encoding="utf-8")
    progress_log_path.write_text(SYNTHETIC_PROGRESS_LOG.strip() + "\n", encoding="utf-8")
    dashboard_path.write_text("# Dashboard\n", encoding="utf-8")
    return master_plan_path, progress_log_path, dashboard_path


class MilestonePackageTests(unittest.TestCase):
    def test_render_and_materialize_package_from_synthetic_docs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_plan_path, progress_log_path, dashboard_path = write_synthetic_docs(root)
            output_dir = root / "docs" / "archive" / "2026-04-06-milestone-package"

            rendered = render_milestone_package_from_paths(
                master_plan_path=master_plan_path,
                progress_log_path=progress_log_path,
                dashboard_path=dashboard_path,
                package_date="2026-04-06",
                output_dir=output_dir,
            )

            self.assertEqual(
                {
                    "package_manifest.json",
                    "README.md",
                    "01-executive-summary.md",
                    "02-acceptance-and-demo.md",
                    "03-risk-register.md",
                    "04-next-steps.md",
                },
                set(rendered.files),
            )
            self.assertEqual("D1", rendered.manifest["latest_delivery"]["stage_id"])
            self.assertEqual("D2", rendered.manifest["next_stage"]["stage_id"])
            self.assertEqual(
                [
                    "python3 -m unittest tests/test_runtime_kernel.py -q",
                    "python3 eco-concil-runtime/scripts/eco_progress_dashboard.py --pretty",
                    "pytest tests/test_progress_dashboard.py -q",
                ],
                [item["command"] for item in rendered.manifest["validation_commands"]],
            )
            self.assertIn(
                f"python3 eco-concil-runtime/scripts/eco_milestone_package.py --output-dir {output_dir}",
                rendered.files["02-acceptance-and-demo.md"],
            )
            self.assertIn(
                "Immediate recommended stage: `D2` Milestone / Demo Packaging",
                rendered.files["04-next-steps.md"],
            )

            payload = materialize_milestone_package_from_paths(
                master_plan_path=master_plan_path,
                progress_log_path=progress_log_path,
                dashboard_path=dashboard_path,
                output_dir=output_dir,
                package_date="2026-04-06",
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual(6, payload["summary"]["generated_file_count"])
            self.assertTrue((output_dir / "package_manifest.json").exists())
            manifest = json.loads((output_dir / "package_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("D2", manifest["next_stage"]["stage_id"])
            self.assertIn(
                "The package is still a control-plane summary, not a full artifact archive.",
                (output_dir / "03-risk-register.md").read_text(encoding="utf-8"),
            )

    def test_package_render_tracks_completed_plan_without_archive_docs(self) -> None:
        master_plan_text = """
# Plan

### 5.1 Route A: Runtime / Governance Stabilization

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `A1` | Runtime Baseline | stabilize | `completed` | stable |
| `A2` | Agent Entry Gate | operator entry | `completed` | visible |

## 7. 推荐的未来数次开发顺序

| 顺序 | 阶段 | 路线 | 为什么先做 | 预期独立交付 |
| --- | --- | --- | --- | --- |
"""
        progress_log_text = """
# Log

## 2026-04-01 A1: Runtime Baseline

Status: completed

Objective:
- Stabilize runtime.

## 2026-04-06 A2: Agent Entry Gate

Status: completed

Objective:
- Expose the agent entry gate.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_plan_path = root / "plan.md"
            progress_log_path = root / "log.md"
            dashboard_path = root / "dashboard.md"
            output_dir = root / "package"
            master_plan_path.write_text(master_plan_text.strip() + "\n", encoding="utf-8")
            progress_log_path.write_text(progress_log_text.strip() + "\n", encoding="utf-8")
            dashboard_path.write_text("# Dashboard\n", encoding="utf-8")

            rendered = render_milestone_package_from_paths(
                master_plan_path=master_plan_path,
                progress_log_path=progress_log_path,
                dashboard_path=dashboard_path,
                package_date="2026-04-06",
                output_dir=output_dir,
            )

        self.assertEqual("A2", rendered.manifest["latest_delivery"]["stage_id"])
        self.assertEqual("", rendered.manifest["next_stage"]["stage_id"])
        self.assertEqual("completed", rendered.manifest["latest_delivery"]["status"])
        self.assertIn("Immediate recommended stage: none", rendered.files["04-next-steps.md"])
        self.assertIn(
            "| `A` Runtime / Governance Stabilization | `2 / 2` | none | `A2` Agent Entry Gate |",
            rendered.files["01-executive-summary.md"],
        )

    def test_package_cli_materializes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_plan_path, progress_log_path, dashboard_path = write_synthetic_docs(root)
            output_dir = root / "custom-package"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(
                        WORKSPACE_ROOT
                        / "eco-concil-runtime"
                        / "scripts"
                        / "eco_milestone_package.py"
                    ),
                    "--master-plan-path",
                    str(master_plan_path),
                    "--progress-log-path",
                    str(progress_log_path),
                    "--dashboard-path",
                    str(dashboard_path),
                    "--output-dir",
                    str(output_dir),
                    "--package-date",
                    "2026-04-06",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(0, completed.returncode, msg=completed.stderr)
            self.assertTrue(output_dir.exists())
            payload = json.loads(completed.stdout)
            self.assertEqual("completed", payload["status"])
            self.assertEqual("D2", payload["summary"]["next_stage"])
            self.assertEqual("D1", payload["summary"]["latest_delivery_stage"])
            self.assertEqual(6, payload["summary"]["generated_file_count"])
            self.assertTrue((output_dir / "README.md").exists())


if __name__ == "__main__":
    unittest.main()

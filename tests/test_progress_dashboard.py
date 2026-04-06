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

from eco_council_runtime.kernel.progress_dashboard import render_dashboard_from_paths


class ProgressDashboardTests(unittest.TestCase):
    def test_repo_dashboard_renders_current_control_view(self) -> None:
        model, markdown_text = render_dashboard_from_paths(
            master_plan_path=WORKSPACE_ROOT / "openclaw-db-first-master-plan.md",
            progress_log_path=WORKSPACE_ROOT / "openclaw-db-first-progress-log.md",
        )

        self.assertGreaterEqual(len(model.stage_definitions), 10)
        self.assertIn(
            "| Current active stages | none<br>Last completed delivery: 2026-04-06 `D4` Milestone / Demo Packaging |",
            markdown_text,
        )
        self.assertIn(
            "| Next recommended stage | `A4` Agent Entry Gate |",
            markdown_text,
        )
        self.assertIn("| Blocked stages | none |", markdown_text)
        self.assertIn(
            "2026-04-06 `D4` Milestone / Demo Packaging", markdown_text
        )
        self.assertIn(
            "| 2026-04-06 | `C2.1` | `completed` | Candidate / Cluster Result Migration |",
            markdown_text,
        )
        self.assertIn("2026-04-06 `A3` Governance Regression Hardening", markdown_text)
        self.assertIn("2026-04-06 `D4` Milestone / Demo Packaging", markdown_text)
        self.assertIn(
            "| `D` Program Control / Documentation | `4 / 4` | none | none | none | 2026-04-06 `D4` Milestone / Demo Packaging |",
            markdown_text,
        )
        self.assertIn("| `A3` | `A` | `completed` | Governance Regression Hardening | 2026-04-06 | 1 |", markdown_text)
        self.assertIn("| `A4` | `A` | `planned` | Agent Entry Gate | - | 0 |", markdown_text)
        self.assertIn(
            "| `C2.2` | `C` | `completed` | Non-Python Query Surface | 2026-04-06 | 1 |",
            markdown_text,
        )
        self.assertIn(
            "| `C2.1` | `C` | `completed` | Candidate / Cluster Result Migration | 2026-04-06 | 1 |",
            markdown_text,
        )
        self.assertIn("| `D3` | `D` | `completed` | Progress Dashboard Conventions | 2026-04-04 | 1 |", markdown_text)
        self.assertIn("| `D4` | `D` | `completed` | Milestone / Demo Packaging | 2026-04-06 | 1 |", markdown_text)
        self.assertIn("| `B2` | `B` | `completed` | Board Write-Path Migration | 2026-04-03 | 2 |", markdown_text)
        self.assertIn("| `B3` | `B` | `completed` | Moderator Control Consolidation | 2026-04-06 | 5 |", markdown_text)
        self.assertIn("| `C1` | `C` | `completed` | Coverage Analysis Query Surface | 2026-04-02 | 1 |", markdown_text)

    def test_dashboard_surfaces_active_and_blocked_stages(self) -> None:
        master_plan_text = """
# Plan

## 4. 历史编号归一化说明

### 5.1 Route A: Runtime / Governance Stabilization

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `A1` | Review Fix Pack | fix | `completed` | done |
| `A2` | Governance Regression Hardening | harden | `blocked` | stable |

### 5.4 Route D: Program Control / Documentation

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `D1` | Progress Dashboard Conventions | dashboard | `in_progress` | visible |
| `D2` | Milestone / Demo Packaging | package | `planned` | ready |

## 7. 推荐的未来数次开发顺序

| 顺序 | 阶段 | 路线 | 为什么先做 | 预期独立交付 |
| --- | --- | --- | --- | --- |
| `1` | `D1` | `D` | dashboard | control view |
| `2` | `A2` | `A` | harden | regressions |
| `3` | `D2` | `D` | package | milestone |
"""
        progress_log_text = """
# Log

## 2026-04-01 A1: Review Fix Pack

Status: completed
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "plan.md"
            progress_path = root / "log.md"
            master_path.write_text(master_plan_text.strip() + "\n", encoding="utf-8")
            progress_path.write_text(progress_log_text.strip() + "\n", encoding="utf-8")

            _model, markdown_text = render_dashboard_from_paths(
                master_plan_path=master_path,
                progress_log_path=progress_path,
            )

            self.assertIn(
                "| Current active stages | `D1` Progress Dashboard Conventions |",
                markdown_text,
            )
            self.assertIn(
                "| Next recommended stage | `D1` Progress Dashboard Conventions |",
                markdown_text,
            )
            self.assertIn(
                "| Blocked stages | `A2` Governance Regression Hardening |",
                markdown_text,
            )

    def test_dashboard_cli_writes_output_file(self) -> None:
        master_plan_text = """
# Plan

## 4. 历史编号归一化说明

### 5.4 Route D: Program Control / Documentation

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `D1` | Progress Dashboard Conventions | dashboard | `planned` | visible |

## 7. 推荐的未来数次开发顺序

| 顺序 | 阶段 | 路线 | 为什么先做 | 预期独立交付 |
| --- | --- | --- | --- | --- |
| `1` | `D1` | `D` | dashboard | control view |
"""
        progress_log_text = """
# Log
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "plan.md"
            progress_path = root / "log.md"
            output_path = root / "dashboard.md"
            master_path.write_text(master_plan_text.strip() + "\n", encoding="utf-8")
            progress_path.write_text(progress_log_text.strip() + "\n", encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    str(WORKSPACE_ROOT / "eco-concil-runtime" / "scripts" / "eco_progress_dashboard.py"),
                    "--master-plan-path",
                    str(master_path),
                    "--progress-log-path",
                    str(progress_path),
                    "--output-path",
                    str(output_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(0, completed.returncode, msg=completed.stderr)
            self.assertTrue(output_path.exists())
            payload = json.loads(completed.stdout)
            self.assertEqual("completed", payload["status"])
            self.assertEqual("D1", payload["summary"]["next_stage"])
            self.assertIn("# OpenClaw DB-First Dashboard", output_path.read_text(encoding="utf-8"))

    def test_crosswalk_stops_after_d2_cutover(self) -> None:
        master_plan_text = """
# Plan

## 4. 历史编号归一化说明

| Progress Log 历史编号 | 归一化后编号 | 归属路线 | 说明 |
| --- | --- | --- | --- |
| `B2` | `C1` | `C` | old |

### 5.2 Route B: Deliberation Plane / Moderator Loop

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `B2` | Board Write-Path Migration | board | `completed` | stable |

### 5.3 Route C: Analysis Plane / DB-First Analysis

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `C1` | Coverage Analysis Query Surface | coverage | `completed` | stable |

## 7. 推荐的未来数次开发顺序

| 顺序 | 阶段 | 路线 | 为什么先做 | 预期独立交付 |
| --- | --- | --- | --- | --- |
| `1` | `B2` | `B` | board | board |
"""
        progress_log_text = """
# Log

## 2026-04-02 B2: Coverage Analysis Plane Query Surface

Status: completed

## 2026-04-03 D2: Master Plan And Route Normalization

Status: completed

## 2026-04-03 B2: Core Board Mutation DB-First

Status: completed
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            master_path = root / "plan.md"
            progress_path = root / "log.md"
            master_path.write_text(master_plan_text.strip() + "\n", encoding="utf-8")
            progress_path.write_text(progress_log_text.strip() + "\n", encoding="utf-8")

            _model, markdown_text = render_dashboard_from_paths(
                master_plan_path=master_path,
                progress_log_path=progress_path,
            )

            self.assertIn(
                "| `B2` | `B` | `completed` | Board Write-Path Migration | 2026-04-03 | 1 |",
                markdown_text,
            )
            self.assertIn(
                "| `C1` | `C` | `completed` | Coverage Analysis Query Surface | 2026-04-02 | 1 |",
                markdown_text,
            )


if __name__ == "__main__":
    unittest.main()

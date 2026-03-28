from __future__ import annotations

import json
import shlex
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.orchestration_execution import (  # noqa: E402
    execute_fetch_plan,
    run_data_plane,
    run_matching_adjudication,
)
from eco_council_runtime.application.orchestration_planning import fetch_plan_input_snapshot  # noqa: E402
from eco_council_runtime.contract import default_round_tasks, scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.paths import (  # noqa: E402
    data_plane_execution_path,
    fetch_execution_path,
    fetch_plan_path,
    matching_adjudication_path,
    matching_authorization_path,
    matching_execution_path,
    round_dir,
    source_selection_path,
)
from eco_council_runtime.domain.text import maybe_text  # noqa: E402

ROUND_ID = "round-001"


def example_mission() -> dict[str, object]:
    return json.loads(Path("assets/contract/examples/mission.json").read_text(encoding="utf-8"))


def example_matching_authorization() -> dict[str, object]:
    return json.loads(Path("assets/contract/examples/matching_authorization.json").read_text(encoding="utf-8"))


def example_matching_adjudication() -> dict[str, object]:
    return json.loads(Path("assets/contract/examples/matching_adjudication.json").read_text(encoding="utf-8"))


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def shell_write_json_command(path: Path, payload: object) -> str:
    return (
        f"mkdir -p {shlex.quote(str(path.parent))} && "
        f"printf '%s\\n' {shlex.quote(json.dumps(payload))} > {shlex.quote(str(path))}"
    )


def shell_print_json_command(payload: object) -> str:
    return f"printf '%s\\n' {shlex.quote(json.dumps(payload))}"


def scaffold_run(root: Path) -> Path:
    mission = example_mission()
    tasks = default_round_tasks(mission=mission, round_id=ROUND_ID)
    run_dir = root / maybe_text(mission.get("run_id"))
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=mission,
        tasks=tasks,
        pretty=True,
    )
    write_json(source_selection_path(run_dir, ROUND_ID, "sociologist"), {"status": "complete", "selected_sources": []})
    write_json(source_selection_path(run_dir, ROUND_ID, "environmentalist"), {"status": "complete", "selected_sources": []})
    return run_dir


def fake_run_json_cli(argv: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> dict[str, object]:
    return {
        "ok": True,
        "payload": {
            "argv": argv,
            "cwd": str(cwd) if cwd is not None else "",
            "has_env": env is not None,
        },
    }


class OrchestrationExecutionTests(unittest.TestCase):
    def test_execute_fetch_plan_runs_steps_and_materializes_stdout_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_run(Path(temp_dir))
            base_round_dir = round_dir(run_dir, ROUND_ID)
            step1_artifact = base_round_dir / "sociologist" / "raw" / "seed.json"
            step2_artifact = base_round_dir / "sociologist" / "raw" / "manifest.json"
            stdout1 = base_round_dir / "sociologist" / "raw" / "_meta" / "seed.stdout.json"
            stderr1 = base_round_dir / "sociologist" / "raw" / "_meta" / "seed.stderr.log"
            stdout2 = base_round_dir / "sociologist" / "raw" / "_meta" / "manifest.stdout.json"
            stderr2 = base_round_dir / "sociologist" / "raw" / "_meta" / "manifest.stderr.log"
            snapshot = fetch_plan_input_snapshot(
                run_dir=run_dir,
                round_id=ROUND_ID,
                sociologist_selection={"status": "complete"},
                environmentalist_selection={"status": "complete"},
            )
            plan = {
                "plan_kind": "eco-council-fetch-plan",
                "schema_version": "1.0.0",
                "input_snapshot": snapshot,
                "steps": [
                    {
                        "step_id": "step-sociologist-01-seed",
                        "role": "sociologist",
                        "source_skill": "seed-fetch",
                        "artifact_path": str(step1_artifact),
                        "stdout_path": str(stdout1),
                        "stderr_path": str(stderr1),
                        "cwd": str(run_dir),
                        "command": shell_write_json_command(step1_artifact, {"seed": True}),
                        "depends_on": [],
                    },
                    {
                        "step_id": "step-sociologist-02-manifest",
                        "role": "sociologist",
                        "source_skill": "manifest-fetch",
                        "artifact_path": str(step2_artifact),
                        "stdout_path": str(stdout2),
                        "stderr_path": str(stderr2),
                        "cwd": str(run_dir),
                        "command": shell_print_json_command({"manifest": True}),
                        "depends_on": ["step-sociologist-01-seed"],
                        "artifact_capture": "stdout-json",
                    },
                ],
            }
            write_json(fetch_plan_path(run_dir, ROUND_ID), plan)

            payload = execute_fetch_plan(
                run_dir=run_dir,
                round_id=ROUND_ID,
                continue_on_error=False,
                skip_existing=False,
                timeout_seconds=30,
            )

            self.assertEqual(2, payload["completed_count"])
            self.assertEqual(0, payload["failed_count"])
            self.assertTrue(step1_artifact.exists())
            self.assertTrue(step2_artifact.exists())
            self.assertTrue(any(status.get("artifact_materialized") is True for status in payload["statuses"]))
            execution = json.loads(fetch_execution_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            self.assertEqual(2, execution["completed_count"])

    def test_run_data_plane_writes_execution_snapshot_and_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_run(Path(temp_dir))

            with patch(
                "eco_council_runtime.application.orchestration_execution.discover_normalize_inputs",
                side_effect=lambda run_dir, round_id, role, sources: [f"{role}=dummy.json"],
            ), patch(
                "eco_council_runtime.application.orchestration_execution.run_json_cli",
                side_effect=fake_run_json_cli,
            ), patch(
                "eco_council_runtime.application.orchestration_execution.record_normalize_phase_receipt",
                return_value={},
            ):
                payload = run_data_plane(run_dir=run_dir, round_id=ROUND_ID)

            execution_path = Path(payload["execution_path"])
            self.assertTrue(execution_path.exists())
            self.assertTrue(Path(payload["reporting_handoff_path"]).exists())
            execution = json.loads(data_plane_execution_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            self.assertEqual(8, execution["completed_count"])
            self.assertEqual("normalize-init-run", execution["statuses"][0]["step_id"])
            self.assertEqual("build-reporting-handoff", execution["statuses"][-1]["step_id"])

    def test_run_matching_adjudication_requires_authorized_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_run(Path(temp_dir))
            authorization = example_matching_authorization()
            authorization["authorization_status"] = "blocked"
            write_json(matching_authorization_path(run_dir, ROUND_ID), authorization)

            with self.assertRaisesRegex(ValueError, "authorization_status=authorized"):
                run_matching_adjudication(run_dir=run_dir, round_id=ROUND_ID)

    def test_run_matching_adjudication_executes_pipeline_when_authorized(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_run(Path(temp_dir))
            write_json(matching_authorization_path(run_dir, ROUND_ID), example_matching_authorization())
            write_json(matching_adjudication_path(run_dir, ROUND_ID), example_matching_adjudication())

            with patch(
                "eco_council_runtime.application.orchestration_execution.run_json_cli",
                side_effect=fake_run_json_cli,
            ):
                payload = run_matching_adjudication(run_dir=run_dir, round_id=ROUND_ID)

            execution_path = Path(payload["execution_path"])
            self.assertTrue(execution_path.exists())
            self.assertTrue(Path(payload["reporting_handoff_path"]).exists())
            execution = json.loads(matching_execution_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            self.assertEqual(8, execution["completed_count"])
            self.assertEqual("apply-matching-adjudication", execution["statuses"][0]["step_id"])


if __name__ == "__main__":
    unittest.main()

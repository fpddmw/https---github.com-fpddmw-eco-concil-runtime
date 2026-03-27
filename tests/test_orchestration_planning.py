from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.orchestration_planning import (  # noqa: E402
    build_fetch_plan,
    build_gdelt_query,
    ensure_fetch_plan_inputs_match,
    ensure_source_selection_respects_governance,
)
from eco_council_runtime.contract import default_round_tasks, scaffold_run_from_mission, source_governance  # noqa: E402
from eco_council_runtime.controller.paths import source_selection_path, tasks_path  # noqa: E402
from eco_council_runtime.domain.text import maybe_text  # noqa: E402

ROUND_ID = "round-001"


def example_mission() -> dict[str, object]:
    return json.loads(Path("assets/contract/examples/mission.json").read_text(encoding="utf-8"))


def fake_fetch_script_path(skill_name: str, *, skills_root_text: str = "") -> Path:
    return Path("/detached-skills") / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_source_selection(
    *,
    mission: dict[str, object],
    role: str,
    round_id: str,
    task_ids: list[str],
    selected_layers: dict[str, dict[str, list[str]]],
) -> dict[str, object]:
    governance = source_governance(mission)
    families = [
        family
        for family in governance.get("families", [])
        if isinstance(family, dict) and maybe_text(family.get("role")) == role
    ]
    approved_layers = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id")))
        for item in governance.get("approved_layers", [])
        if isinstance(item, dict) and maybe_text(item.get("family_id")) and maybe_text(item.get("layer_id"))
    }

    allowed_sources: list[str] = []
    selected_sources: list[str] = []
    family_plans: list[dict[str, object]] = []
    source_decisions: list[dict[str, object]] = []

    for family in families:
        family_id = maybe_text(family.get("family_id"))
        layer_selection = selected_layers.get(family_id, {})
        entry_layer_id = next(
            (
                maybe_text(layer.get("layer_id"))
                for layer in family.get("layers", [])
                if isinstance(layer, dict) and maybe_text(layer.get("tier")) == "l1"
            ),
            "",
        )
        layer_plans: list[dict[str, object]] = []
        family_selected = False

        for layer in family.get("layers", []):
            if not isinstance(layer, dict):
                continue
            layer_id = maybe_text(layer.get("layer_id"))
            tier = maybe_text(layer.get("tier"))
            layer_skills = [maybe_text(skill) for skill in layer.get("skills", []) if maybe_text(skill)]
            allowed_sources.extend(layer_skills)
            requested_skills = layer_selection.get(layer_id, [])
            selected = bool(requested_skills)
            if selected:
                family_selected = True
                selected_sources.extend(requested_skills)
            if tier == "l1":
                authorization_basis = "entry-layer"
            elif (family_id, layer_id) in approved_layers:
                authorization_basis = "upstream-approval"
            elif layer.get("auto_selectable") is True:
                authorization_basis = "policy-auto"
            else:
                authorization_basis = "not-authorized"
            anchor_mode = "none"
            anchor_refs: list[str] = []
            if selected and tier != "l1":
                anchor_mode = "same_round_l1"
                anchor_refs = [f"{round_id}:family:{family_id}:{entry_layer_id}"]
            layer_plans.append(
                {
                    "layer_id": layer_id,
                    "tier": tier,
                    "selected": selected,
                    "reason": f"{'Select' if selected else 'Skip'} {family_id}:{layer_id}.",
                    "source_skills": requested_skills,
                    "anchor_mode": anchor_mode,
                    "anchor_refs": anchor_refs,
                    "authorization_basis": authorization_basis,
                }
            )

        family_plans.append(
            {
                "family_id": family_id,
                "selected": family_selected,
                "reason": f"{'Use' if family_selected else 'Skip'} {family_id}.",
                "evidence_requirement_ids": [],
                "layer_plans": layer_plans,
            }
        )

    for source_skill in allowed_sources:
        source_decisions.append(
            {
                "source_skill": source_skill,
                "selected": source_skill in selected_sources,
                "reason": f"{'Selected' if source_skill in selected_sources else 'Not selected'} for {role}.",
            }
        )

    return {
        "schema_version": "1.0.0",
        "selection_id": f"source-selection-{role}-{round_id}",
        "run_id": maybe_text(mission.get("run_id")),
        "round_id": round_id,
        "agent_role": role,
        "status": "complete",
        "summary": f"Selection for {role}.",
        "task_ids": task_ids,
        "allowed_sources": allowed_sources,
        "selected_sources": selected_sources,
        "override_requests": [],
        "family_plans": family_plans,
        "source_decisions": source_decisions,
    }


def scaffold_plan_run(root: Path) -> tuple[Path, dict[str, object]]:
    mission = example_mission()
    tasks = default_round_tasks(mission=mission, round_id=ROUND_ID)
    tasks[0]["inputs"]["query_hints"] = ["Chiang Mai smoke", "northern Thailand haze"]
    tasks[1]["inputs"]["openaq_parameter_names"] = ["pm25", "o3"]
    run_dir = root / maybe_text(mission.get("run_id"))
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=mission,
        tasks=tasks,
        pretty=True,
    )

    sociologist_selection = build_source_selection(
        mission=mission,
        role="sociologist",
        round_id=ROUND_ID,
        task_ids=[maybe_text(tasks[0].get("task_id"))],
        selected_layers={
            "gdelt": {
                "recon": ["gdelt-doc-search"],
                "bulk": ["gdelt-gkg-fetch"],
            },
            "bluesky": {
                "posts": ["bluesky-cascade-fetch"],
            },
        },
    )
    environmentalist_selection = build_source_selection(
        mission=mission,
        role="environmentalist",
        round_id=ROUND_ID,
        task_ids=[maybe_text(tasks[1].get("task_id"))],
        selected_layers={
            "firms": {
                "fire-detections": ["nasa-firms-fire-fetch"],
            },
            "openaq": {
                "stations": ["openaq-data-fetch"],
            },
        },
    )
    write_json(source_selection_path(run_dir, ROUND_ID, "sociologist"), sociologist_selection)
    write_json(source_selection_path(run_dir, ROUND_ID, "environmentalist"), environmentalist_selection)
    return run_dir, mission


class OrchestrationPlanningTests(unittest.TestCase):
    def test_build_fetch_plan_constructs_role_steps_and_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir, _mission = scaffold_plan_run(Path(temp_dir))

            with patch(
                "eco_council_runtime.application.orchestration_planning.fetch_script_path",
                side_effect=fake_fetch_script_path,
            ):
                plan = build_fetch_plan(run_dir=run_dir, round_id=ROUND_ID, firms_point_padding_deg=0.5)

            step_sources = [step["source_skill"] for step in plan["steps"]]
            self.assertEqual(
                [
                    "gdelt-doc-search",
                    "gdelt-gkg-fetch",
                    "bluesky-cascade-fetch",
                    "nasa-firms-fire-fetch",
                    "openaq-data-fetch",
                ],
                step_sources,
            )

            gdelt_bulk_step = next(step for step in plan["steps"] if step["source_skill"] == "gdelt-gkg-fetch")
            openaq_step = next(step for step in plan["steps"] if step["source_skill"] == "openaq-data-fetch")
            firms_step = next(step for step in plan["steps"] if step["source_skill"] == "nasa-firms-fire-fetch")

            self.assertEqual(["step-sociologist-01-gdelt-doc-search"], gdelt_bulk_step["depends_on"])
            self.assertEqual("stdout-json", gdelt_bulk_step["artifact_capture"])
            self.assertIn("collect-openaq", openaq_step["command"])
            self.assertIn("--bbox=", firms_step["command"])
            self.assertEqual(
                ["gdelt-doc-search", "gdelt-gkg-fetch", "bluesky-cascade-fetch"],
                plan["roles"]["sociologist"]["selected_sources"],
            )
            self.assertEqual(
                ["nasa-firms-fire-fetch", "openaq-data-fetch"],
                plan["roles"]["environmentalist"]["selected_sources"],
            )
            self.assertEqual("complete", plan["input_snapshot"]["source_selections"]["sociologist"]["status"])

    def test_ensure_fetch_plan_inputs_match_detects_task_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir, _mission = scaffold_plan_run(Path(temp_dir))

            with patch(
                "eco_council_runtime.application.orchestration_planning.fetch_script_path",
                side_effect=fake_fetch_script_path,
            ):
                plan = build_fetch_plan(run_dir=run_dir, round_id=ROUND_ID, firms_point_padding_deg=0.5)

            ensure_fetch_plan_inputs_match(run_dir=run_dir, round_id=ROUND_ID, plan=plan)

            tasks_payload = json.loads(tasks_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            tasks_payload[0]["objective"] = "Updated objective after prepare-round."
            write_json(tasks_path(run_dir, ROUND_ID), tasks_payload)

            with self.assertRaisesRegex(RuntimeError, "tasks.json changed"):
                ensure_fetch_plan_inputs_match(run_dir=run_dir, round_id=ROUND_ID, plan=plan)

    def test_ensure_source_selection_respects_governance_rejects_invalid_skill(self) -> None:
        mission = example_mission()
        selection = build_source_selection(
            mission=mission,
            role="sociologist",
            round_id=ROUND_ID,
            task_ids=["task-sociologist-round-001-01"],
            selected_layers={
                "gdelt": {
                    "recon": ["gdelt-doc-search"],
                },
            },
        )

        invalid = copy.deepcopy(selection)
        gdelt_family = next(item for item in invalid["family_plans"] if item["family_id"] == "gdelt")
        bulk_layer = next(item for item in gdelt_family["layer_plans"] if item["layer_id"] == "bulk")
        bulk_layer["selected"] = True
        bulk_layer["source_skills"] = ["youtube-comments-fetch"]
        bulk_layer["anchor_mode"] = "same_round_l1"
        bulk_layer["anchor_refs"] = [f"{ROUND_ID}:family:gdelt:recon"]
        bulk_layer["authorization_basis"] = "upstream-approval"

        with self.assertRaisesRegex(ValueError, "selected invalid skills"):
            ensure_source_selection_respects_governance(
                mission=mission,
                role="sociologist",
                source_selection=invalid,
            )

    def test_build_gdelt_query_preserves_literal_and_operator_hints(self) -> None:
        mission = example_mission()
        tasks = [
            {
                "objective": "Track public discussion.",
                "inputs": {
                    "query_hints": [
                        "Chiang Mai smoke",
                        "sourcecountry:THA haze",
                    ]
                },
            }
        ]

        query = build_gdelt_query(mission=mission, tasks=tasks)

        self.assertIn('"Chiang Mai smoke"', query)
        self.assertIn("sourcecountry:THA haze", query)


if __name__ == "__main__":
    unittest.main()

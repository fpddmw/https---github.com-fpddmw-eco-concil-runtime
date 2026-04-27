from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import write_json

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
sys.path.insert(0, str(RUNTIME_SRC))


def load_modules():
    selection_module = importlib.import_module("eco_council_runtime.kernel.source_queue_selection")
    planner_module = importlib.import_module("eco_council_runtime.kernel.source_queue_planner")
    return selection_module, planner_module


def mission_payload(raw_artifact_path: Path) -> dict[str, object]:
    return {
        "run_id": "run-family-memory-001",
        "topic": "NYC smoke verification",
        "objective": "Track how prior-round source families should influence current source selection.",
        "window": {"start_utc": "2023-06-07T00:00:00Z", "end_utc": "2023-06-07T23:59:59Z"},
        "region": {
            "label": "New York City, USA",
            "geometry": {"type": "Point", "latitude": 40.7128, "longitude": -74.0060},
        },
        "allowed_sources_by_role": {
            "sociologist": ["fetch-youtube-video-search", "fetch-gdelt-doc-search", "fetch-bluesky-cascade"],
        },
        "artifact_imports": [
            {
                "source_skill": "fetch-youtube-video-search",
                "artifact_path": str(raw_artifact_path),
                "query_text": "nyc smoke wildfire",
            }
        ],
    }


def sociologist_tasks(round_id: str) -> list[dict[str, object]]:
    return [
        {
            "task_id": f"task-sociologist-{round_id}-01",
            "run_id": "run-family-memory-001",
            "round_id": round_id,
            "assigned_role": "sociologist",
            "status": "planned",
            "objective": "Collect public-signal evidence for the current round.",
            "inputs": {
                "evidence_requirements": [
                    {
                        "requirement_id": f"req-sociologist-{round_id}-01",
                        "summary": "Need public signal evidence.",
                    }
                ]
            },
        }
    ]


def write_prior_round_artifacts(run_dir: Path, round_id: str) -> None:
    write_json(run_dir / "investigation" / f"round_tasks_{round_id}.json", sociologist_tasks(round_id))
    write_json(
        run_dir / "runtime" / f"source_selection_sociologist_{round_id}.json",
        {
            "schema_version": "1.0.0",
            "selection_id": f"source-selection-sociologist-{round_id}",
            "run_id": "run-family-memory-001",
            "round_id": round_id,
            "agent_role": "sociologist",
            "status": "complete",
            "summary": "Prior round selected YouTube public signals.",
            "task_ids": [f"task-sociologist-{round_id}-01"],
            "allowed_sources": ["fetch-youtube-video-search", "fetch-gdelt-doc-search", "fetch-bluesky-cascade"],
            "selected_sources": ["fetch-youtube-video-search"],
            "override_requests": [],
            "evidence_requirements": [],
            "family_plans": [
                {
                    "family_id": "youtube",
                    "selected": True,
                    "reason": "Use youtube.",
                    "evidence_requirement_ids": [],
                    "layer_plans": [
                        {
                            "layer_id": "video-search",
                            "tier": "l1",
                            "selected": True,
                            "reason": "Select youtube:video-search.",
                            "source_skills": ["fetch-youtube-video-search"],
                            "anchor_mode": "none",
                            "anchor_refs": [],
                            "authorization_basis": "entry-layer",
                        }
                    ],
                }
            ],
            "family_memory": [],
            "source_decisions": [
                {
                    "source_skill": "fetch-youtube-video-search",
                    "selected": True,
                    "reason": "Selected for sociologist.",
                }
            ],
        },
    )
    write_json(
        run_dir / "runtime" / f"import_execution_{round_id}.json",
        {
            "statuses": [
                {
                    "step_id": f"step-sociologist-01-{round_id}",
                    "step_kind": "import",
                    "status": "completed",
                    "role": "sociologist",
                    "source_skill": "fetch-youtube-video-search",
                }
            ]
        },
    )


def set_ordered_times(*paths: Path) -> None:
    for index, path in enumerate(paths, start=1):
        timestamp_ns = index * 1_000_000_000
        os.utime(path, ns=(timestamp_ns, timestamp_ns))


class SourceQueueFamilyMemoryTests(unittest.TestCase):
    def test_build_source_selection_includes_prior_round_family_memory(self) -> None:
        selection_module, _ = load_modules()
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            raw_artifact_path = Path(tmpdir) / "youtube.json"
            write_json(raw_artifact_path, [])

            write_prior_round_artifacts(run_dir, "round-001")
            current_tasks_path = run_dir / "investigation" / "round_tasks_round-002.json"
            write_json(current_tasks_path, sociologist_tasks("round-002"))
            set_ordered_times(
                run_dir / "investigation" / "round_tasks_round-001.json",
                run_dir / "runtime" / "source_selection_sociologist_round-001.json",
                run_dir / "runtime" / "import_execution_round-001.json",
                current_tasks_path,
            )

            payload = selection_module.build_source_selection(
                run_dir=run_dir,
                mission=mission_payload(raw_artifact_path),
                tasks=sociologist_tasks("round-002"),
                run_id="run-family-memory-001",
                round_id="round-002",
                role="sociologist",
            )

            youtube_family = next(item for item in payload["family_memory"] if item["family_id"] == "youtube")
            self.assertEqual(["fetch-youtube-video-search"], youtube_family["completed_sources"])
            self.assertEqual("round-001", youtube_family["prior_rounds"][0]["round_id"])
            self.assertEqual(["video-search"], youtube_family["prior_rounds"][0]["selected_layers"])

    def test_fetch_plan_roles_surface_family_memory(self) -> None:
        selection_module, planner_module = load_modules()
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            raw_artifact_path = Path(tmpdir) / "youtube.json"
            write_json(raw_artifact_path, [])

            write_prior_round_artifacts(run_dir, "round-001")
            current_tasks_path = run_dir / "investigation" / "round_tasks_round-002.json"
            write_json(current_tasks_path, sociologist_tasks("round-002"))
            mission_path = run_dir / "mission.json"
            write_json(mission_path, mission_payload(raw_artifact_path))
            set_ordered_times(
                run_dir / "investigation" / "round_tasks_round-001.json",
                run_dir / "runtime" / "source_selection_sociologist_round-001.json",
                run_dir / "runtime" / "import_execution_round-001.json",
                current_tasks_path,
                mission_path,
            )

            mission = mission_payload(raw_artifact_path)
            tasks = sociologist_tasks("round-002")
            selections = selection_module.build_source_selections(
                run_dir=run_dir,
                mission=mission,
                tasks=tasks,
                run_id="run-family-memory-001",
                round_id="round-002",
            )
            planner_module.write_source_selections(run_dir, "round-002", selections)
            plan, warnings = planner_module.build_fetch_plan(
                run_dir=run_dir,
                run_id="run-family-memory-001",
                round_id="round-002",
                mission=mission,
                tasks=tasks,
                selections=selections,
            )

            self.assertEqual([], warnings)
            youtube_family = next(item for item in plan["roles"]["sociologist"]["family_memory"] if item["family_id"] == "youtube")
            self.assertEqual(["fetch-youtube-video-search"], youtube_family["completed_sources"])
            self.assertEqual("round-001", youtube_family["prior_rounds"][0]["round_id"])


if __name__ == "__main__":
    unittest.main()

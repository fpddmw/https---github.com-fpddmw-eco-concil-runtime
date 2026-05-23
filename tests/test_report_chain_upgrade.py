from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from _workflow_support import (
    load_json,
    reporting_path,
    request_and_approve_transition,
    run_script,
    runtime_src_path,
    script_path,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-report-chain-upgrade"
ROUND_ID = "round-report-chain-upgrade"


def insert_signal(
    run_dir: Path,
    *,
    signal_id: str,
    plane: str,
    source_skill: str,
    timestamp: str = "2023-06-07T12:00:00Z",
) -> None:
    from eco_council_runtime.kernel.planes.signal import INSERT_SQL, ensure_signal_plane_schema

    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "signal_id": signal_id,
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "plane": plane,
        "batch_id": f"batch-{signal_id}",
        "source_skill": source_skill,
        "signal_kind": f"{plane}-signal",
        "canonical_object_kind": f"{plane}-signal",
        "external_id": signal_id,
        "dedupe_key": signal_id,
        "title": f"{plane} fixture",
        "body_text": "Fixture text for report-chain upgrade tests.",
        "url": f"https://example.test/{signal_id}",
        "author_name": "",
        "channel_name": "",
        "language": "en",
        "query_text": "smoke policy concern",
        "metric": "",
        "numeric_value": None,
        "unit": "",
        "published_at_utc": timestamp if plane in {"public", "formal"} else "",
        "observed_at_utc": timestamp if plane == "environment" else "",
        "window_start_utc": "",
        "window_end_utc": "",
        "captured_at_utc": "",
        "latitude": None,
        "longitude": None,
        "bbox_json": "{}",
        "quality_flags_json": "[]",
        "engagement_json": "{}",
        "metadata_json": "{}",
        "raw_json": "{}",
        "artifact_path": str(run_dir / "raw" / f"{signal_id}.json"),
        "record_locator": "$",
        "artifact_sha256": "",
    }
    with sqlite3.connect(db_path) as connection:
        ensure_signal_plane_schema(connection)
        connection.execute(INSERT_SQL, row)
        connection.commit()


def materialize_blueprint(
    run_dir: Path,
    *,
    run_id: str = RUN_ID,
    round_id: str = ROUND_ID,
    mission_text: str = "Analyze NYC wildfire smoke air quality, official advisories, public discourse, and policy response boundaries.",
) -> dict[str, Any]:
    payload = run_script(
        script_path("materialize-report-blueprint"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--mission-text",
        mission_text,
    )
    return load_json(Path(payload["summary"]["output_path"]))


def scaffold_program_run(run_dir: Path, *, run_id: str, round_id: str) -> None:
    mission_path = run_dir / "mission_input.json"
    write_json(
        mission_path,
        {
            "run_id": run_id,
            "topic": "Program-aware council smoke test",
            "objective": "Frame issue council rounds without source route precommitment.",
            "request_text": "Build a program-aware council flow for smoke-test reporting.",
            "window": {
                "start_utc": "2023-06-01T00:00:00Z",
                "end_utc": "2023-06-10T00:00:00Z",
            },
            "region": {
                "label": "NYC smoke governance context",
                "geometry": {"type": "Point", "coordinates": [-73.985, 40.748]},
            },
            "hypotheses": [
                {
                    "title": "Program-aware framing hypothesis",
                    "statement": "The council should split report questions into issue rounds before source route choices.",
                    "owner_role": "moderator",
                    "status": "active",
                }
            ],
            "artifact_imports": [],
            "source_requests": [],
        },
    )
    run_script(
        script_path("scaffold-mission-run"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--mission-path",
        str(mission_path),
        "--orchestration-mode",
        "openclaw-agent",
    )


def minimal_draft() -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    for section_id in (
        "executive-summary",
        "key-points",
        "what-happened",
        "evidence-basis",
        "council-reasoning",
        "limitations",
        "decision-implications",
        "audit-trail",
    ):
        sections.append(
            {
                "section_id": section_id,
                "title": section_id.replace("-", " ").title(),
                "status": "draft",
                "paragraphs": ["Bounded fixture paragraph."],
                "evidence_refs": ["signal:fixture"],
            }
        )
    return {
        "schema_version": "narrative-report-draft-v1",
        "draft_id": "draft-report-chain-upgrade",
        "run_id": RUN_ID,
        "round_id": ROUND_ID,
        "basis_round_id": ROUND_ID,
        "title": "Report Chain Upgrade Fixture",
        "claim_boundary": {
            "summary": "Claims stay within section briefs, sufficiency review, and frozen basis.",
            "forbidden_claims": ["unsupported policy effectiveness"],
        },
        "sections": sections,
        "reader_guidance": {"primary_audience": "test"},
        "evidence_refs": ["signal:fixture"],
        "audit_refs": ["signal:fixture"],
        "source_material": {"reporting_artifacts": [], "council_object_counts": {}},
    }


def validate_draft(run_dir: Path, draft: dict[str, Any]) -> set[str]:
    draft_path = reporting_path(run_dir, f"narrative_report_draft_{ROUND_ID}.json")
    write_json(draft_path, draft)
    payload = run_script(
        script_path("validate-narrative-report"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--draft-path",
        str(draft_path),
    )
    validation = load_json(Path(payload["summary"]["output_path"]))
    return {
        str(item.get("code"))
        for item in validation.get("issues", [])
        if isinstance(item, dict)
    }


class ReportChainUpgradeTests(unittest.TestCase):
    def test_report_blueprint_frames_claim_slots_without_source_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            artifact = materialize_blueprint(run_dir)

            claim_slots = artifact["claim_slots"]
            themes = artifact["investigation_themes"]
            self.assertGreaterEqual(len(claim_slots), 4)
            self.assertTrue(all(slot["status"] == "open-question-not-conclusion" for slot in claim_slots))
            self.assertFalse(any("source_skill" in json.dumps(slot) for slot in claim_slots))
            self.assertFalse(any(theme["theme_id"] == "policy_evaluation_basis" for theme in themes))
            self.assertTrue(all("source_selection_policy" in theme for theme in themes))
            self.assertTrue(artifact["synthesis_targets"][0]["not_acquisition_theme"])

    def test_framing_positions_synthesize_council_investigation_program(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            artifact = materialize_blueprint(run_dir)
            blueprint_id = artifact["report_blueprint"]["blueprint_id"]
            round_proposals = {
                "environmental-investigator": [
                    {
                        "round_title": "Smoke chronology basis council",
                        "program_order": 10,
                        "round_subtitle_question": "What basis must be acquired or downgraded before the smoke chronology can be stated?",
                        "round_category": "evidence-acquisition",
                        "round_mode": "evidence-acquisition-council",
                        "active_theme_ids": ["theme-fact-event-process"],
                        "agent_responsibility_boundaries": [
                            "environmental-investigator: establish item-level chronology basis and visible limitation boundaries."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "agent-evidence-boundary-turns",
                            "agent-acquisition-turns",
                            "moderator-synthesis",
                        ],
                    },
                    {
                        "round_title": "Smoke chronology interpretation council",
                        "program_order": 20,
                        "round_subtitle_question": "Which smoke chronology claims can be carried after the acquired basis is reviewed?",
                        "round_category": "evidence-analysis",
                        "round_mode": "evidence-analysis-council",
                        "active_theme_ids": ["theme-fact-event-process"],
                        "agent_responsibility_boundaries": [
                            "environmental-investigator: distinguish supported observations from causal, responsibility, and effect wording."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "agent-analysis-turns",
                            "challenger-boundary-review",
                            "moderator-synthesis",
                        ],
                    },
                ],
                "social-investigator": [
                    {
                        "round_title": "Public meaning interpretation council",
                        "program_order": 30,
                        "round_subtitle_question": "What semantic structures and limitations should be analyzed after agents define the public material boundary?",
                        "round_category": "semantic-analysis",
                        "round_mode": "semantic-analysis-council",
                        "active_theme_ids": ["theme-public-semantic-perception"],
                        "agent_responsibility_boundaries": [
                            "social-investigator: separate issue frames, risk meanings, trust cues, uncertainty, policy demands, and attribution language within bounded material."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "agent-analysis-turns",
                            "challenger-boundary-review",
                            "moderator-synthesis",
                        ],
                    }
                ],
                "challenger": [
                    {
                        "round_title": "Report claim boundary council",
                        "program_order": 40,
                        "round_subtitle_question": "Which strong report claims must be downgraded before report writing?",
                        "round_category": "claim-boundary-review",
                        "round_mode": "claim-boundary-review-council",
                        "active_theme_ids": [
                            "theme-fact-event-process",
                            "theme-public-semantic-perception",
                            "theme-interaction-timeline",
                        ],
                        "agent_responsibility_boundaries": [
                            "challenger: review public-proportion, causal, effectiveness, attribution, and absence wording before report handoff."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "challenger-boundary-review",
                            "moderator-synthesis",
                        ],
                    }
                ],
            }
            framing_roles = (
                "report-editor",
                "environmental-investigator",
                "social-investigator",
                "challenger",
                "moderator",
            )
            for role in framing_roles:
                position_payload = {
                    "position_text": f"{role} accepts the report questions with bounded claim language.",
                    "boundary_notes": [
                        "Do not convert report questions into fixed source or task queues."
                    ],
                    "proposed_agenda_questions": [
                        f"Which {role} boundary should the issue council preserve?"
                    ],
                }
                if role in round_proposals:
                    position_payload["proposed_program_rounds"] = round_proposals[role]
                run_script(
                    script_path("submit-agent-position"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--author-role",
                    role,
                    "--target-kind",
                    "report-blueprint",
                    "--target-id",
                    blueprint_id,
                    "--rationale",
                    f"{role} records framing boundary position.",
                    "--payload-json",
                    json.dumps(
                        position_payload,
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                )

            result = run_script(
                script_path("synthesize-council-investigation-program"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--blueprint-id",
                blueprint_id,
            )
            program_artifact = load_json(Path(result["summary"]["output_path"]))
            program = program_artifact["council_investigation_program"]
            round_briefs = program_artifact["materialized_round_briefs"]
            self.assertEqual("council-investigation-program", program["object_kind"])
            self.assertEqual("agent-authored-round-proposals", program["program_synthesis_mode"])
            self.assertEqual(4, program["agent_authored_round_proposal_count"])
            self.assertGreaterEqual(len(program["round_sequence"]), 5)
            self.assertEqual(len(program["round_sequence"]), len(round_briefs))
            self.assertEqual(len(round_briefs), result["summary"]["materialized_round_brief_count"])
            self.assertFalse(result["summary"]["missing_agent_position_roles"])
            self.assertEqual(
                {
                    "report-editor",
                    "environmental-investigator",
                    "social-investigator",
                    "challenger",
                    "moderator",
                },
                set(program["framing_position_roles"]),
            )
            self.assertTrue(
                any(
                    "accepts the report questions" in summary["position_text"]
                    for summary in program["agent_position_summaries"]
                )
            )
            self.assertIn(
                "Which challenger boundary should the issue council preserve?",
                program["council_agenda_questions"],
            )
            self.assertTrue(
                all(
                    item["round_subtitle_question"].endswith("?")
                    for item in program["round_sequence"]
                )
            )
            categories = [item["round_category"] for item in program["round_sequence"]]
            self.assertIn("evidence-acquisition", categories)
            self.assertIn("evidence-analysis", categories)
            self.assertIn("semantic-analysis", categories)
            self.assertIn("claim-boundary-review", categories)
            self.assertIn("reporting", categories)
            self.assertLess(categories.index("evidence-acquisition"), categories.index("evidence-analysis"))
            self.assertIn(
                "Smoke chronology basis council",
                [item["round_title"] for item in program["round_sequence"]],
            )
            self.assertFalse(
                any(item["round_id"] == "round-002-fact-official-acquisition" for item in program["round_sequence"])
            )
            first_acquisition_brief = next(
                brief for brief in round_briefs if brief["round_category"] == "evidence-acquisition"
            )
            first_analysis_brief = next(
                brief for brief in round_briefs if brief["round_category"] == "evidence-analysis"
            )
            self.assertEqual(program["program_id"], first_acquisition_brief["program_id"])
            self.assertEqual("round-brief", first_acquisition_brief["object_kind"])
            self.assertTrue(first_acquisition_brief["active_theme_ids"])
            self.assertTrue(first_acquisition_brief["agent_responsibility_boundaries"])
            self.assertIn("theme-progress-review", first_acquisition_brief["expected_council_objects"])
            self.assertIn("source-acquisition-proposal", first_acquisition_brief["expected_council_objects"])
            self.assertNotIn("source-acquisition-proposal", first_analysis_brief["expected_council_objects"])
            for forbidden in (
                "source_family",
                "source_skill",
                "query",
                "query_parameters",
                "priority_score",
                "route_ranking",
                "scheduler_queue",
                "auto_execute",
            ):
                self.assertIn(forbidden, program["forbidden_scheduler_fields"])
                self.assertNotIn(forbidden, {key for key in program if key != "forbidden_scheduler_fields"})
            for round_item in program["round_sequence"]:
                self.assertNotIn("source_family", round_item)
                self.assertNotIn("source_skill", round_item)
                self.assertNotIn("query_parameters", round_item)
            forbidden_keys = {
                "source_family",
                "source_skill",
                "query",
                "query_parameters",
                "priority_score",
                "route_ranking",
                "scheduler_queue",
                "auto_execute",
            }
            violations: list[str] = []

            def walk(value: Any, path: str = "") -> None:
                if isinstance(value, dict):
                    for key, child in value.items():
                        child_path = f"{path}.{key}" if path else key
                        if key in forbidden_keys and child_path != "forbidden_scheduler_fields":
                            violations.append(child_path)
                        walk(child, child_path)
                elif isinstance(value, list):
                    for index, child in enumerate(value):
                        walk(child, f"{path}[{index}]")

            walk(program)
            self.assertEqual([], violations)

    def test_open_issue_round_loads_program_projected_round_brief(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            framing_round_id = "round-001-framing-scope"
            scaffold_program_run(run_dir, run_id=RUN_ID, round_id=framing_round_id)
            blueprint_result = run_script(
                script_path("materialize-report-blueprint"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                framing_round_id,
                "--mission-text",
                "Analyze NYC wildfire smoke air quality, official advisories, public discourse, and policy response boundaries.",
            )
            blueprint_artifact = load_json(Path(blueprint_result["summary"]["output_path"]))
            blueprint_id = blueprint_artifact["report_blueprint"]["blueprint_id"]
            for role in (
                "report-editor",
                "environmental-investigator",
                "social-investigator",
                "challenger",
                "moderator",
            ):
                run_script(
                    script_path("submit-agent-position"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    framing_round_id,
                    "--author-role",
                    role,
                    "--target-kind",
                    "report-blueprint",
                    "--target-id",
                    blueprint_id,
                    "--rationale",
                    f"{role} records framing boundary position.",
                )

            program_result = run_script(
                script_path("synthesize-council-investigation-program"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                framing_round_id,
                "--blueprint-id",
                blueprint_id,
            )
            program_artifact = load_json(Path(program_result["summary"]["output_path"]))
            program = program_artifact["council_investigation_program"]
            first_issue_round = next(
                item
                for item in program["round_sequence"]
                if item["round_category"] == "evidence-acquisition"
            )
            target_round_id = first_issue_round["round_id"]
            transition_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=framing_round_id,
                transition_kind="open-investigation-round",
                target_round_id=target_round_id,
                source_round_id=framing_round_id,
                rationale="Open the first program-aware issue council round.",
                request_payload={"program_id": program["program_id"]},
            )
            opened = run_script(
                script_path("open-investigation-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                target_round_id,
                "--source-round-id",
                framing_round_id,
                "--transition-request-id",
                transition_request_id,
            )
            transition = load_json(Path(opened["summary"]["output_path"]))
            self.assertEqual(program["program_id"], transition["program_id"])
            self.assertEqual("evidence-acquisition", transition["round_category"])
            self.assertTrue(transition["round_brief_id"])
            self.assertEqual(first_issue_round["active_theme_ids"], transition["active_theme_ids"])
            self.assertTrue(transition["round_internal_phases"])
            self.assertTrue(transition["agent_responsibility_boundaries"])
            self.assertTrue(transition["observed_inputs"]["program_round_brief_loaded"])

    def test_fresh_framing_scope_run_materializes_reviewable_program(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_id = "run-fresh-framing-scope-program"
            framing_round_id = "round-001-framing-scope"
            scaffold_program_run(run_dir, run_id=run_id, round_id=framing_round_id)
            artifact = materialize_blueprint(
                run_dir,
                run_id=run_id,
                round_id=framing_round_id,
            )
            blueprint_id = artifact["report_blueprint"]["blueprint_id"]
            for role in (
                "report-editor",
                "environmental-investigator",
                "social-investigator",
                "challenger",
                "moderator",
            ):
                run_script(
                    script_path("submit-agent-position"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    run_id,
                    "--round-id",
                    framing_round_id,
                    "--author-role",
                    role,
                    "--target-kind",
                    "report-blueprint",
                    "--target-id",
                    blueprint_id,
                    "--rationale",
                    f"{role} records a framing position for fresh program smoke.",
                    "--payload-json",
                    json.dumps(
                        {
                            "position_text": f"{role} accepts the blueprint as an agenda basis.",
                            "boundary_notes": [
                                "Keep the program as agenda context, not a source or query plan."
                            ],
                            "proposed_agenda_questions": [
                                f"Which {role} responsibility boundary should be visible before issue rounds open?"
                            ],
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                )

            result = run_script(
                script_path("synthesize-council-investigation-program"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                framing_round_id,
                "--blueprint-id",
                blueprint_id,
            )
            program_artifact = load_json(Path(result["summary"]["output_path"]))
            program = program_artifact["council_investigation_program"]
            review_packet = program_artifact["human_review_packet"]
            quality = program_artifact["program_quality_review"]

            self.assertEqual("council-investigation-program", program["object_kind"])
            self.assertEqual("round-001-framing-scope", program["round_sequence"][0]["round_id"])
            self.assertEqual("planning", program["round_sequence"][0]["round_category"])
            self.assertEqual([], result["summary"]["missing_agent_position_roles"])
            self.assertEqual(
                {
                    "report-editor",
                    "environmental-investigator",
                    "social-investigator",
                    "challenger",
                    "moderator",
                },
                set(review_packet["framing_position_roles"]),
            )
            self.assertEqual("reviewable", result["summary"]["human_review_status"])
            self.assertEqual("reviewable", quality["review_status"])
            self.assertTrue(quality["checks"]["first_round_is_framing_scope"])
            self.assertTrue(quality["checks"]["has_issue_round_before_reporting"])
            self.assertTrue(quality["checks"]["has_reporting_round"])
            self.assertTrue(quality["checks"]["round_brief_projection_complete"])
            self.assertTrue(quality["checks"]["forbidden_scheduler_field_paths_absent"])
            self.assertTrue(review_packet["issue_rounds"])
            self.assertTrue(review_packet["next_round_suggestion"]["round_id"])
            self.assertEqual(
                len(program["round_sequence"]),
                review_packet["materialized_round_brief_count"],
            )
            self.assertEqual(
                "council-program-human-review-v1",
                review_packet["schema_version"],
            )
            self.assertTrue(review_packet["program_boundary"]["not_scheduler"])

            forbidden_keys = {
                "source_family",
                "source_skill",
                "query",
                "query_parameters",
                "priority_score",
                "route_ranking",
                "scheduler_queue",
                "auto_execute",
            }
            violations: list[str] = []

            def walk(value: Any, path: str = "") -> None:
                if isinstance(value, dict):
                    for key, child in value.items():
                        child_path = f"{path}.{key}" if path else key
                        if key in forbidden_keys:
                            violations.append(child_path)
                        walk(child, child_path)
                elif isinstance(value, list):
                    for index, child in enumerate(value):
                        walk(child, f"{path}[{index}]")

            walk(program)
            self.assertEqual([], violations)

            from eco_council_runtime.objects.council import query_council_objects

            queried = query_council_objects(
                run_dir,
                object_kind="council-investigation-program",
                run_id=run_id,
                round_id=framing_round_id,
                limit=10,
            )
            self.assertEqual(1, len(queried["objects"]))
            self.assertEqual(program["program_id"], queried["objects"][0]["program_id"])

    def test_colorado_river_fixture_program_is_mission_specific_without_routes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            run_id = "run-colorado-river-framing-fixture"
            framing_round_id = "round-001-framing-scope"
            scaffold_program_run(run_dir, run_id=run_id, round_id=framing_round_id)
            mission_text = (
                "Analyze Colorado River reservoir operations, hydrologic background, "
                "official governance records, public and formal semantics, multi-actor "
                "tradeoff narratives, and policy evaluation boundaries."
            )
            artifact = materialize_blueprint(
                run_dir,
                run_id=run_id,
                round_id=framing_round_id,
                mission_text=mission_text,
            )
            blueprint = artifact["report_blueprint"]
            blueprint_id = blueprint["blueprint_id"]
            slot_questions = " ".join(
                slot["question"] for slot in artifact["claim_slots"]
            ).casefold()
            self.assertIn("operating or hydrologic", slot_questions)
            self.assertIn("official operating", slot_questions)
            self.assertIn("tradeoffs and governance", slot_questions)
            self.assertNotIn("smoke", slot_questions)
            round_proposals = {
                "environmental-investigator": [
                    {
                        "round_title": "Colorado hydrologic background council",
                        "program_order": 10,
                        "round_subtitle_question": "What hydrologic and reservoir-operation basis is needed before the report can state the background?",
                        "round_category": "hydrologic-background-analysis",
                        "round_mode": "issue-council",
                        "active_theme_ids": ["theme-fact-event-process"],
                        "agent_responsibility_boundaries": [
                            "environmental-investigator: define hydrologic background, operation visibility, denominator limits, and downgrade boundaries."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "agent-analysis-turns",
                            "progress-review",
                            "moderator-synthesis",
                        ],
                    }
                ],
                "social-investigator": [
                    {
                        "round_title": "Colorado governance and public-formal semantics council",
                        "program_order": 20,
                        "round_subtitle_question": "Which official governance records and public or formal semantics can be analyzed as bounded tradeoff narratives?",
                        "round_category": "governance-semantic-analysis",
                        "round_mode": "issue-council",
                        "active_theme_ids": [
                            "theme-official-policy-action",
                            "theme-public-semantic-perception",
                        ],
                        "agent_responsibility_boundaries": [
                            "social-investigator: separate official governance record presence, public/formal semantic tradeoffs, actor narratives, and representativeness limits."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "agent-analysis-turns",
                            "challenger-boundary-review",
                            "moderator-synthesis",
                        ],
                    }
                ],
                "challenger": [
                    {
                        "round_title": "Colorado multi-actor policy boundary council",
                        "program_order": 30,
                        "round_subtitle_question": "Which multi-actor, causality, allocation, or policy-effect claims must be downgraded before report writing?",
                        "round_category": "multi-actor-policy-boundary-review",
                        "round_mode": "claim-boundary-review-council",
                        "active_theme_ids": [
                            "theme-interaction-timeline",
                            "theme-public-semantic-perception",
                        ],
                        "agent_responsibility_boundaries": [
                            "challenger: review multi-actor attribution, water allocation causality, public-proportion, and policy-effect overreach."
                        ],
                        "round_internal_phases": [
                            "agenda-question",
                            "challenger-boundary-review",
                            "moderator-synthesis",
                        ],
                    }
                ],
            }
            for role in (
                "report-editor",
                "environmental-investigator",
                "social-investigator",
                "challenger",
                "moderator",
            ):
                position_payload = {
                    "position_text": (
                        f"{role} accepts the Colorado River fixture framing as "
                        "water-governance agenda context only."
                    ),
                    "boundary_notes": [
                        "Do not convert Colorado River questions into source, query, route, or scheduler instructions."
                    ],
                    "proposed_agenda_questions": [
                        f"Which {role} boundary matters for hydrologic, governance, or multi-actor report use?"
                    ],
                }
                if role in round_proposals:
                    position_payload["proposed_program_rounds"] = round_proposals[role]
                run_script(
                    script_path("submit-agent-position"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    run_id,
                    "--round-id",
                    framing_round_id,
                    "--author-role",
                    role,
                    "--target-kind",
                    "report-blueprint",
                    "--target-id",
                    blueprint_id,
                    "--rationale",
                    f"{role} records Colorado River fixture framing position.",
                    "--payload-json",
                    json.dumps(position_payload, ensure_ascii=True, sort_keys=True),
                )

            result = run_script(
                script_path("synthesize-council-investigation-program"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                framing_round_id,
                "--blueprint-id",
                blueprint_id,
                "--program-id",
                "council-program-colorado-river-fixture",
            )
            program_artifact = load_json(Path(result["summary"]["output_path"]))
            program = program_artifact["council_investigation_program"]
            self.assertEqual("reviewable", result["summary"]["human_review_status"])
            self.assertFalse(result["summary"]["missing_agent_position_roles"])
            self.assertEqual(
                "agent-authored-round-proposals",
                program["program_synthesis_mode"],
            )
            program_text = json.dumps(program, ensure_ascii=True, sort_keys=True).casefold()
            self.assertIn("colorado", program_text)
            self.assertIn("hydrologic", program_text)
            self.assertIn("reservoir-operation", program_text)
            self.assertIn("official governance", program_text)
            self.assertIn("tradeoff narratives", program_text)
            self.assertIn("multi-actor", program_text)
            self.assertNotIn("smoke chronology", program_text)
            categories = [item["round_category"] for item in program["round_sequence"]]
            self.assertIn("hydrologic-background-analysis", categories)
            self.assertIn("governance-semantic-analysis", categories)
            self.assertIn("multi-actor-policy-boundary-review", categories)
            self.assertTrue(program_artifact["human_review_packet"]["issue_rounds"])
            self.assertTrue(
                program_artifact["program_quality_review"]["checks"][
                    "forbidden_scheduler_field_paths_absent"
                ]
            )
            forbidden_keys = {
                "source_family",
                "source_skill",
                "query",
                "query_parameters",
                "priority_score",
                "route_ranking",
                "scheduler_queue",
                "auto_execute",
            }
            violations: list[str] = []

            def walk(value: Any, path: str = "") -> None:
                if isinstance(value, dict):
                    for key, child in value.items():
                        child_path = f"{path}.{key}" if path else key
                        if key in forbidden_keys:
                            violations.append(child_path)
                        walk(child, child_path)
                elif isinstance(value, list):
                    for index, child in enumerate(value):
                        walk(child, f"{path}[{index}]")

            walk(program)
            self.assertEqual([], violations)

    def test_agent_round_proposals_merge_consensus_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            artifact = materialize_blueprint(run_dir)
            blueprint_id = artifact["report_blueprint"]["blueprint_id"]
            proposals = {
                "environmental-investigator": {
                    "round_title": "Event boundary and fact basis",
                    "round_subtitle_question": "What happened environmentally within the bounded event window?",
                    "round_category": "data-acquisition",
                    "round_mode": "issue-council",
                    "agent_responsibility_boundaries": {
                        "environmental-investigator": "establish event-boundary evidence without policy-effect claims",
                    },
                },
                "social-investigator": {
                    "round_title": "Case scope for public semantics",
                    "round_subtitle_question": "What case boundary is needed before public semantics can be interpreted?",
                    "round_category": "scoping-deliberation",
                    "round_mode": "issue-council",
                    "agent_responsibility_boundaries": {
                        "social-investigator": "state public and official-record boundary needs without source routing",
                    },
                },
                "challenger": {
                    "round_title": "Scope challenge council",
                    "round_subtitle_question": "Which vague case boundaries would create unsupported strong claims?",
                    "round_category": "framing-scope",
                    "round_mode": "issue-council",
                    "agent_responsibility_boundaries": {
                        "challenger": "challenge vague scope before public, causal, or policy claims appear",
                    },
                },
                "report-editor": {
                    "round_title": "Scope the reportable smoke episode",
                    "round_subtitle_question": "What event window and claim-strength target should govern later investigation?",
                    "round_category": "scope-deliberation",
                    "round_mode": "issue-council",
                    "agent_responsibility_boundaries": {
                        "report-editor": "state which report sections depend on the adopted case boundary",
                    },
                },
            }
            for offset, (role, proposal) in enumerate(proposals.items()):
                proposal["program_order"] = 10 + offset
                proposal["active_theme_ids"] = ["theme-fact-event-process"]
                proposal["round_internal_phases"] = [
                    "role position exchange",
                    "boundary challenge discussion",
                    "moderator synthesis",
                ]
                run_script(
                    script_path("submit-agent-position"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--author-role",
                    role,
                    "--target-kind",
                    "report-blueprint",
                    "--target-id",
                    blueprint_id,
                    "--rationale",
                    f"{role} submits a consensus-variant scope proposal.",
                    "--payload-json",
                    json.dumps(
                        {
                            "position_text": f"{role} proposes a scope variant for the same council issue.",
                            "proposed_program_rounds": [proposal],
                        },
                        ensure_ascii=True,
                        sort_keys=True,
                    ),
                )

            result = run_script(
                script_path("synthesize-council-investigation-program"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--blueprint-id",
                blueprint_id,
            )
            program = load_json(Path(result["summary"]["output_path"]))["council_investigation_program"]
            merged_rounds = [
                item
                for item in program["round_sequence"]
                if item.get("proposal_source") == "merged-agent-position-rounds"
            ]
            self.assertEqual(4, program["agent_authored_round_proposal_count"])
            self.assertEqual(1, len(merged_rounds))
            merged = merged_rounds[0]
            self.assertEqual(
                {
                    "environmental-investigator",
                    "social-investigator",
                    "challenger",
                    "report-editor",
                },
                set(merged["contributing_agent_roles"]),
            )
            self.assertEqual(4, len(merged["contributing_round_titles"]))
            self.assertIn(
                "environmental-investigator: establish event-boundary evidence without policy-effect claims",
                merged["agent_responsibility_boundaries"],
            )
            self.assertLessEqual(len(program["round_sequence"]), 3)

    def test_acquisition_checkpoints_only_emit_when_claim_impact_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            insert_signal(run_dir, signal_id="sig-env", plane="environment", source_skill="fetch-airnow-observations")
            insert_signal(run_dir, signal_id="sig-formal", plane="formal", source_skill="fetch-federal-register-documents")
            insert_signal(run_dir, signal_id="sig-public", plane="public", source_skill="fetch-youtube-comments")
            write_json(
                run_dir / "analytics" / f"fact_policy_public_interaction_timeline_{ROUND_ID}.json",
                {
                    "interaction_nodes": [
                        {
                            "node_id": "node-supported",
                            "fact_or_policy_evidence_refs": ["signal:sig-formal"],
                            "public_or_media_evidence_refs": ["signal:sig-public"],
                        }
                    ],
                    "lane_episode_cards": [],
                },
            )

            complete = run_script(
                script_path("materialize-acquisition-checkpoints"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            complete_artifact = load_json(Path(complete["summary"]["output_path"]))
            self.assertEqual(0, complete_artifact["checkpoint_count"])

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            impacted = run_script(
                script_path("materialize-acquisition-checkpoints"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            impacted_artifact = load_json(Path(impacted["summary"]["output_path"]))
            self.assertGreater(impacted_artifact["checkpoint_count"], 0)
            self.assertTrue(impacted_artifact["checkpoint_policy"]["not_per_tool_call_form"])

    def test_sufficiency_review_agent_brief_and_handoff_are_connected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            review = run_script(
                script_path("review-theme-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            review_artifact = load_json(Path(review["summary"]["output_path"]))
            self.assertTrue(review_artifact["unsupported_claim_slots"])
            self.assertTrue(review_artifact["required_downgrades"])
            self.assertEqual(4, len(review_artifact["theme_progress_reviews"]))
            review_json = json.dumps(review_artifact, sort_keys=True)
            self.assertNotIn("source_skill_counts", review_json)
            self.assertNotIn('"source_skill"', review_json)
            from eco_council_runtime.objects.council import query_council_objects

            queried_progress = query_council_objects(
                run_dir,
                object_kind="theme-progress-review",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                limit=20,
            )
            self.assertEqual(4, len(queried_progress["objects"]))
            self.assertTrue(
                all(
                    item["recommended_disposition"] in {
                        "downgrade-required",
                        "needs-in-round-recovery",
                        "satisfied-for-current-claim-strength",
                        "blocked-by-program-mismatch",
                    }
                    for item in review_artifact["theme_progress_reviews"]
                )
            )

            brief = run_script(
                script_path("draft-agent-section-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "social-investigator",
                "--sufficiency-review-path",
                str(review["summary"]["output_path"]),
            )
            self.assertEqual("insufficient-basis-downgrade-required", brief["summary"]["claim_strength"])

            handoff = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_artifact = load_json(Path(handoff["summary"]["output_path"]))
            self.assertEqual(1, handoff_artifact["agent_section_brief_count"])
            self.assertEqual(4, handoff_artifact["theme_sufficiency_review_count"])
            self.assertEqual(4, handoff_artifact["theme_progress_review_count"])
            self.assertEqual(
                "social-investigator",
                handoff_artifact["report_packet"]["section_briefs"][0]["agent_role"],
            )
            self.assertEqual(0, handoff_artifact["situation_analysis_brief_count"])
            self.assertEqual(
                "materialize-situation-analysis-brief",
                handoff_artifact["report_packet"]["situation_analysis_brief_policy"][
                    "missing_next_skill"
                ],
            )
            self.assertIn(
                "materialize-situation-analysis-brief",
                handoff["board_handoff"]["suggested_next_skills"],
            )
            self.assertTrue(
                any(
                    warning.get("code")
                    == "situation-analysis-brief-missing-before-narrative"
                    for warning in handoff_artifact["warnings"]
                    if isinstance(warning, dict)
                )
            )

    def test_theme_progress_supplemental_recommendation_requires_transition_uptake(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            scaffold_program_run(run_dir, run_id=RUN_ID, round_id=ROUND_ID)
            materialize_blueprint(run_dir)
            brief = run_script(
                script_path("submit-round-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-id",
                ROUND_ID,
                "--rationale",
                "Program-aware issue council brief for public semantic boundary review.",
                "--program-id",
                "council-program-supplemental-test",
                "--round-mode",
                "issue-council",
                "--round-category",
                "issue-deliberation",
                "--round-title",
                "Public semantic issue council",
                "--round-subtitle-question",
                "Which public semantic denominator boundary remains unresolved?",
                "--active-theme-id",
                "theme-public-semantic-perception",
                "--agent-responsibility-boundary",
                "social-investigator: explain public semantic denominator and downgrade boundary.",
                "--agent-responsibility-boundary",
                "challenger: review denominator dispute and source-limit implications.",
                "--round-internal-phase",
                "agent-acquisition-turns",
                "--round-internal-phase",
                "agent-analysis-turns",
                "--round-internal-phase",
                "progress-review",
                "--round-internal-phase",
                "moderator-synthesis",
            )
            write_json(
                run_dir / "analytics" / f"public_discourse_coverage_audit_{ROUND_ID}.json",
                {
                    "source_limit_records": [
                        {
                            "limit_kind": "denominator-dispute",
                            "theme_id": "theme-public-semantic-perception",
                            "rationale": (
                                "No reasonable recovery remains after denominator "
                                "dispute and challenger concern."
                            ),
                        }
                    ]
                },
            )

            review = run_script(
                script_path("review-theme-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--program-id",
                "council-program-supplemental-test",
                "--round-brief-id",
                brief["summary"]["object_id"],
                "--active-theme-id",
                "theme-public-semantic-perception",
                "--theme-id",
                "theme-public-semantic-perception",
            )
            review_artifact = load_json(Path(review["summary"]["output_path"]))
            progress_review = review_artifact["theme_progress_reviews"][0]

            self.assertEqual(
                "needs-supplemental-round",
                progress_review["recommended_disposition"],
            )
            self.assertEqual(1, review_artifact["supplemental_recommendation_count"])
            self.assertEqual(1, len(review_artifact["supplemental_round_recommendations"]))
            self.assertEqual(1, len(review_artifact["supplemental_transition_payload_suggestions"]))
            self.assertEqual(1, len(review_artifact["supplemental_transition_request_templates"]))
            self.assertTrue(review_artifact["review_policy"]["does_not_open_supplemental_round"])
            suggestion = review_artifact["supplemental_transition_payload_suggestions"][0]
            target_round_id = suggestion["suggested_target_round_id"]
            self.assertRegex(target_round_id, r"^round-\d{3}-supplemental-")
            self.assertEqual(["theme-public-semantic-perception"], suggestion["active_theme_ids"])
            self.assertIn(progress_review["review_id"], suggestion["parent_theme_progress_review_refs"])
            self.assertFalse((run_dir / "runtime" / f"round_transition_{target_round_id}.json").exists())
            self.assertIn(
                "request-phase-transition",
                review["board_handoff"]["suggested_next_skills"],
            )

            transition_request_id = request_and_approve_transition(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                transition_kind="open-investigation-round",
                target_round_id=target_round_id,
                source_round_id=ROUND_ID,
                rationale="Moderator adopts advisory supplemental recommendation after council uptake.",
                request_payload=suggestion,
            )
            opened = run_script(
                script_path("open-investigation-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                target_round_id,
                "--source-round-id",
                ROUND_ID,
                "--transition-request-id",
                transition_request_id,
            )
            transition_artifact = load_json(Path(opened["summary"]["output_path"]))
            self.assertEqual(
                "council-program-supplemental-test",
                transition_artifact["program_id"],
            )
            self.assertEqual(
                ["theme-public-semantic-perception"],
                transition_artifact["active_theme_ids"],
            )
            self.assertIn(
                progress_review["review_id"],
                transition_artifact["parent_theme_progress_review_refs"],
            )
            self.assertIn(
                "theme-public-semantic-perception",
                transition_artifact["unresolved_responsibility_boundary_refs"],
            )

            prepared = run_script(
                script_path("prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                target_round_id,
            )
            plan_artifact = load_json(Path(prepared["summary"]["output_path"]))
            program_context = plan_artifact["program_coordination_context"]
            self.assertEqual(
                "council-program-supplemental-test",
                program_context["program_id"],
            )
            self.assertIn(
                progress_review["review_id"],
                program_context["parent_theme_progress_review_refs"],
            )
            self.assertTrue(program_context["runtime_boundary"]["context_only"])

    def test_situation_analysis_brief_feeds_reporting_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            review = run_script(
                script_path("review-theme-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
                script_path("draft-agent-section-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "social-investigator",
                "--sufficiency-review-path",
                str(review["summary"]["output_path"]),
            )

            situation = run_script(
                script_path("materialize-situation-analysis-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            situation_artifact = load_json(Path(situation["summary"]["output_path"]))
            brief = situation_artifact["brief"]
            self.assertEqual("situation-analysis-brief", brief["object_kind"])
            self.assertTrue(brief["mission_answerable_question"])
            self.assertTrue(brief["central_bounded_judgement"])
            self.assertTrue(brief["recommended_report_spine"])
            self.assertIn("not_runtime_gate", brief["provenance"])

            from eco_council_runtime.reporting_objects import query_reporting_objects

            queried = query_reporting_objects(
                run_dir,
                object_kind="situation-analysis-brief",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                limit=10,
            )
            self.assertEqual(1, queried["summary"]["returned_object_count"])
            self.assertEqual(brief["brief_id"], queried["objects"][0]["brief_id"])

            handoff = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            handoff_artifact = load_json(Path(handoff["summary"]["output_path"]))
            self.assertEqual(1, handoff_artifact["situation_analysis_brief_count"])
            self.assertEqual(
                brief["brief_id"],
                handoff_artifact["report_packet"]["situation_analysis_briefs"][0]["brief_id"],
            )
            self.assertTrue(
                handoff_artifact["report_packet"]["situation_analysis_brief_policy"][
                    "present"
                ]
            )
            self.assertEqual(
                "",
                handoff_artifact["report_packet"]["situation_analysis_brief_policy"][
                    "missing_next_skill"
                ],
            )
            self.assertNotIn(
                "materialize-situation-analysis-brief",
                handoff["board_handoff"]["suggested_next_skills"],
            )

    def test_non_production_report_chain_harness_reaches_validator_with_situation_spine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            basis_round_id = "round-fixture-basis"
            report_round_id = "round-fixture-report-writing"
            write_json(
                run_dir / "mission.json",
                {
                    "schema_version": "1.0.0",
                    "run_id": RUN_ID,
                    "topic": "Fixture-only smoke situation report",
                    "objective": (
                        "Use a non-production harness to verify section briefs, "
                        "situation-analysis brief, handoff, narrative, and validator."
                    ),
                    "request_text": (
                        "What bounded situation analysis can be written from carried "
                        "fact, official, public semantic, interaction, and challenger basis?"
                    ),
                },
            )
            write_json(
                run_dir
                / "analytics"
                / f"fact_policy_public_interaction_timeline_{basis_round_id}.json",
                {
                    "interaction_nodes": [
                        {
                            "node_id": "node-fixture-co-visible",
                            "summary": (
                                "Fixture fact, official advisory, and public semantic "
                                "records are co-visible in the carried basis."
                            ),
                            "fact_or_policy_evidence_refs": [
                                "signal:fixture-env",
                                "signal:fixture-official",
                            ],
                            "public_or_media_evidence_refs": ["signal:fixture-public"],
                        }
                    ],
                    "lane_episode_cards": [
                        {"episode_id": "episode-fact"},
                        {"episode_id": "episode-public"},
                    ],
                },
            )
            write_json(
                run_dir / "report_basis" / f"report_basis_freeze_{basis_round_id}.json",
                {
                    "selected_basis_object_ids": [
                        "finding:fixture-fact",
                        "proposal:fixture-policy-boundary",
                    ],
                    "selected_evidence_refs": [
                        "signal:fixture-env",
                        "signal:fixture-official",
                        "signal:fixture-public",
                    ],
                },
            )
            section_specs = [
                (
                    "environmental-investigator",
                    "fact_process",
                    "Fact process brief",
                    [
                        (
                            "Fixture fact process supports only a bounded description "
                            "of smoke timing and observed environmental conditions."
                        )
                    ],
                    ["signal:fixture-env"],
                    ["Do not infer health impact or physical source attribution."],
                ),
                (
                    "social-investigator",
                    "public_semantic_and_official_action",
                    "Public semantic and official action brief",
                    [
                        (
                            "Fixture official advisory and public semantic records "
                            "support a sample-local account of public concern and "
                            "official communication timing."
                        )
                    ],
                    ["signal:fixture-official", "signal:fixture-public"],
                    ["Do not convert sample-local public concern into public opinion."],
                ),
                (
                    "challenger",
                    "challenger_limitations",
                    "Challenger boundary brief",
                    [
                        (
                            "Fixture interaction wording must stay descriptive: "
                            "co-visibility is not causality or policy effectiveness."
                        )
                    ],
                    ["signal:fixture-public"],
                    [
                        "No representative public proportion is supported.",
                        "No causal policy effectiveness claim is supported.",
                    ],
                ),
            ]
            for (
                agent_role,
                section_key,
                section_role,
                main_claims,
                evidence_refs,
                limitations,
            ) in section_specs:
                run_script(
                    script_path("draft-agent-section-brief"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    basis_round_id,
                    "--agent-role",
                    agent_role,
                    "--section-key",
                    section_key,
                    "--section-role",
                    section_role,
                    "--claim-strength",
                    "bounded-supported",
                    "--main-claims-json",
                    json.dumps(main_claims, ensure_ascii=True),
                    "--evidence-refs-json",
                    json.dumps(evidence_refs, ensure_ascii=True),
                    "--limitations-json",
                    json.dumps(limitations, ensure_ascii=True),
                    "--blocked-phrases-json",
                    json.dumps(
                        [
                            "representative public opinion",
                            "policy caused public response",
                            "policy was effective",
                        ],
                        ensure_ascii=True,
                    ),
                )

            situation = run_script(
                script_path("materialize-situation-analysis-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                basis_round_id,
                "--basis-round-id",
                basis_round_id,
            )
            situation_artifact = load_json(Path(situation["summary"]["output_path"]))
            situation_brief = situation_artifact["brief"]
            self.assertEqual(3, situation_artifact["section_brief_count"])
            self.assertTrue(situation_brief["section_brief_refs"])
            self.assertTrue(situation_brief["challenger_boundary_refs"])
            self.assertTrue(situation_brief["interaction_claims"])
            self.assertIn(
                "co-visible",
                json.dumps(situation_brief["interaction_claims"]).casefold(),
            )
            self.assertIn(
                "central bounded judgement",
                " ".join(situation_brief["recommended_report_spine"]).casefold(),
            )

            handoff = run_script(
                script_path("materialize-reporting-handoff"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                basis_round_id,
            )
            handoff_artifact = load_json(Path(handoff["summary"]["output_path"]))
            self.assertEqual(3, handoff_artifact["agent_section_brief_count"])
            self.assertEqual(1, handoff_artifact["situation_analysis_brief_count"])
            self.assertEqual(
                "",
                handoff_artifact["report_packet"]["situation_analysis_brief_policy"][
                    "missing_next_skill"
                ],
            )
            handoff_artifact.update(
                {
                    "handoff_status": "ready",
                    "reporting_ready": True,
                    "reporting_blockers": [],
                    "report_basis_status": "frozen",
                    "selected_basis_object_ids": [
                        "finding:fixture-fact",
                        "proposal:fixture-policy-boundary",
                    ],
                    "selected_evidence_refs": [
                        "signal:fixture-env",
                        "signal:fixture-official",
                        "signal:fixture-public",
                    ],
                    "fixture_semantics": (
                        "Non-production harness: marks an already constructed "
                        "fixture handoff ready without running a real council."
                    ),
                }
            )
            write_json(
                run_dir / "reporting" / f"reporting_handoff_{basis_round_id}.json",
                handoff_artifact,
            )

            draft = run_script(
                script_path("draft-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                report_round_id,
                "--basis-round-id",
                basis_round_id,
                "--title",
                "Fixture Situation Analysis Harness",
            )
            draft_artifact = load_json(Path(draft["summary"]["output_path"]))
            self.assertTrue(draft_artifact["reader_guidance"]["situation_analysis_brief_first"])
            self.assertTrue(draft_artifact["source_material"]["situation_analysis_preferred"])
            self.assertEqual(
                situation_brief["brief_id"],
                draft_artifact["source_material"]["situation_analysis_briefs"][0]["brief_id"],
            )

            validation = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                report_round_id,
                "--draft-path",
                str(Path(draft["summary"]["output_path"])),
            )
            validation_artifact = load_json(Path(validation["summary"]["output_path"]))
            codes = {
                str(item.get("code"))
                for item in validation_artifact.get("issues", [])
                if isinstance(item, dict)
            }
            self.assertNotIn("missing-situation-analysis-brief", codes)
            self.assertNotIn("missing-situation-analysis-mission-question", codes)
            self.assertNotIn("weak-report-mainline", codes)
            self.assertNotIn("situation-analysis-brief-missing-chain", codes)
            self.assertNotIn("situation-analysis-unresolved-index-missing", codes)
            self.assertNotIn("challenger-boundary-review-missing", codes)

    def test_agent_section_brief_carries_program_theme_progress_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            framing_round_id = "round-001-framing-scope"
            scaffold_program_run(run_dir, run_id=RUN_ID, round_id=framing_round_id)
            blueprint = materialize_blueprint(
                run_dir,
                run_id=RUN_ID,
                round_id=framing_round_id,
            )
            program_result = run_script(
                script_path("synthesize-council-investigation-program"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                framing_round_id,
                "--blueprint-id",
                blueprint["report_blueprint"]["blueprint_id"],
                "--program-id",
                "council-program-section-brief-context",
            )
            program_artifact = load_json(Path(program_result["summary"]["output_path"]))
            program = program_artifact["council_investigation_program"]
            social_round = next(
                item
                for item in program["round_sequence"]
                if "semantic" in item["round_category"]
                and item["round_category"] != "reporting"
            )
            target_round_id = social_round["round_id"]
            active_theme_id = social_round["active_theme_ids"][0]
            review_path = run_dir / "analytics" / f"theme_sufficiency_review_{target_round_id}.json"
            write_json(
                review_path,
                {
                    "theme_sufficiency_reviews": [
                        {
                            "review_id": "theme-sufficiency-section-context",
                            "theme_id": active_theme_id,
                            "supported_claim_slots": ["claim-public-semantic-boundary"],
                            "valid_denominators": [
                                {
                                    "source_family": "public-discourse",
                                    "sample_count": 12,
                                }
                            ],
                            "evidence_refs": ["signal:public-fixture"],
                        }
                    ],
                    "theme_progress_reviews": [
                        {
                            "review_id": "theme-progress-section-context",
                            "program_id": program["program_id"],
                            "active_theme_id": active_theme_id,
                            "recommended_disposition": "needs-in-round-recovery",
                            "denominator_status": "sample-denominator-visible-but-limited",
                            "coverage_or_policy_lane_limits": [
                                "Public semantic proportions remain sample-local."
                            ],
                            "in_round_recovery_options": [
                                "Social investigator may submit a denominator note before report use."
                            ],
                            "evidence_refs": ["signal:public-fixture"],
                        }
                    ],
                },
            )
            section = run_script(
                script_path("draft-agent-section-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                target_round_id,
                "--agent-role",
                "social-investigator",
                "--program-id",
                program["program_id"],
                "--sufficiency-review-path",
                str(review_path),
            )
            section_artifact = load_json(Path(section["summary"]["output_path"]))
            brief = section_artifact["brief"]
            program_context = brief["program_context"]
            self.assertEqual(
                "agent-section-brief-program-context-v1",
                program_context["schema_version"],
            )
            self.assertEqual(program["program_id"], brief["program_id"])
            self.assertEqual(target_round_id, program_context["round_id"])
            self.assertEqual(
                social_round["round_subtitle_question"],
                program_context["round_subtitle_question"],
            )
            self.assertIn(active_theme_id, brief["theme_ids"])
            self.assertIn(active_theme_id, program_context["active_theme_ids"])
            self.assertTrue(program_context["theme_threads"])
            self.assertIn(
                "needs-in-round-recovery",
                program_context["theme_progress_dispositions"],
            )
            self.assertEqual(
                ["needs-in-round-recovery"],
                brief["theme_progress_dispositions"],
            )
            self.assertTrue(
                any(
                    "social-investigator" in boundary
                    for boundary in brief["role_responsibility_boundaries"]
                )
            )
            self.assertTrue(program_context["runtime_boundary"]["context_only"])
            self.assertTrue(
                program_context["runtime_boundary"]["does_not_filter_source_selection"]
            )
            forbidden_context_keys = {
                "source_family",
                "source_skill",
                "query",
                "query_parameters",
                "priority_score",
                "route_ranking",
                "scheduler_queue",
                "auto_execute",
            }
            violations: list[str] = []

            def walk(value: Any, path: str = "") -> None:
                if isinstance(value, dict):
                    for key, child in value.items():
                        child_path = f"{path}.{key}" if path else key
                        if key in forbidden_context_keys:
                            violations.append(child_path)
                        walk(child, child_path)
                elif isinstance(value, list):
                    for index, child in enumerate(value):
                        walk(child, f"{path}[{index}]")

            walk(program_context)
            self.assertEqual([], violations)

    def test_narrative_report_prefers_situation_analysis_brief_spine(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            basis_round_id = "round-situation-first-basis"
            report_round_id = "round-situation-first-report"
            write_json(
                run_dir / "reporting" / f"reporting_handoff_{basis_round_id}.json",
                {
                    "schema_version": "reporting-handoff-v1",
                    "handoff_id": "handoff-situation-first",
                    "run_id": RUN_ID,
                    "round_id": basis_round_id,
                    "handoff_status": "ready",
                    "reporting_ready": True,
                    "report_basis_status": "frozen",
                    "selected_evidence_refs": ["signal:fact-1", "signal:public-1"],
                    "selected_basis_object_ids": ["finding:situation-first"],
                    "evidence_refs": ["signal:fact-1", "signal:public-1"],
                    "report_packet": {
                        "section_briefs": [
                            {
                                "brief_id": "section-brief-situation-first",
                                "agent_role": "social-investigator",
                                "section_key": "public_semantic_and_policy_record",
                                "main_claims": [
                                    "Scattered section brief text should not be the primary report line."
                                ],
                                "claim_strength": "bounded-supported",
                                "evidence_refs": ["signal:public-1"],
                            }
                        ],
                        "situation_analysis_briefs": [
                            {
                                "brief_id": "situation-analysis-brief-situation-first",
                                "program_id": "program-situation-first",
                                "basis_round_id": basis_round_id,
                                "mission_answerable_question": "What bounded situation line can the carried basis answer?",
                                "central_bounded_judgement": "Situation-first central answer from brief.",
                                "event_stage_map": [
                                    {
                                        "stage_id": "stage-unique",
                                        "stage_label": "Situation-first event stage from brief.",
                                    }
                                ],
                                "fact_process_chain": [
                                    {"summary": "Situation-first fact process from brief."}
                                ],
                                "official_action_chain": [
                                    {"summary": "Situation-first official action from brief."}
                                ],
                                "public_semantic_chain": [
                                    {"summary": "Situation-first public semantic chain from brief."}
                                ],
                                "policy_semantic_chain": [
                                    {"summary": "Situation-first policy semantic boundary from brief."}
                                ],
                                "interaction_claims": [
                                    {
                                        "summary": "Situation-first interaction line from brief.",
                                        "boundary": "Not causality.",
                                    }
                                ],
                                "downgraded_claims": [
                                    {"summary": "Situation-first downgraded claim from brief."}
                                ],
                                "unresolved_claim_needs": [
                                    {"summary": "Situation-first unresolved need from brief."}
                                ],
                                "recommended_report_spine": [
                                    "Situation-first report spine from brief."
                                ],
                                "forbidden_writing_upgrades": [
                                    "Situation-first forbidden upgrade from brief."
                                ],
                                "evidence_refs": ["signal:fact-1", "signal:public-1"],
                            }
                        ],
                    },
                },
            )

            result = run_script(
                script_path("draft-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                report_round_id,
                "--basis-round-id",
                basis_round_id,
                "--title",
                "Situation First Fixture",
            )
            draft = load_json(Path(result["summary"]["output_path"]))
            section_text = {
                section["section_id"]: "\n".join(section["paragraphs"])
                for section in draft["sections"]
            }
            self.assertTrue(draft["reader_guidance"]["situation_analysis_brief_first"])
            self.assertTrue(draft["source_material"]["situation_analysis_preferred"])
            self.assertIn(
                "Situation-first central answer from brief.",
                section_text["executive-summary"],
            )
            self.assertIn(
                "Situation-first event stage from brief.",
                section_text["what-happened"],
            )
            self.assertIn(
                "Situation-first fact process from brief.",
                section_text["evidence-basis"],
            )
            self.assertIn(
                "Situation-first public semantic chain from brief.",
                section_text["evidence-basis"],
            )
            self.assertIn(
                "Situation-first policy semantic boundary from brief.",
                section_text["evidence-basis"],
            )
            self.assertIn(
                "Situation-first report spine from brief.",
                section_text["council-reasoning"],
            )
            self.assertIn(
                "Situation-first forbidden upgrade from brief.",
                section_text["limitations"],
            )

    def test_sufficiency_review_does_not_support_interaction_from_fact_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            materialize_blueprint(run_dir)
            insert_signal(run_dir, signal_id="sig-env-only", plane="environment", source_skill="fetch-airnow-observations")
            review = run_script(
                script_path("review-theme-sufficiency"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            review_artifact = load_json(Path(review["summary"]["output_path"]))
            interaction_review = next(
                item
                for item in review_artifact["theme_sufficiency_reviews"]
                if item["theme_id"] == "theme-interaction-timeline"
            )
            self.assertFalse(interaction_review["supported_claim_slots"])
            self.assertIn("claim-slot-interaction-timeline", interaction_review["unsupported_claim_slots"])

    def test_theme_evidence_boundary_plan_requires_investigator_authorship(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            common_args = (
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--rationale",
                "Investigator adopts a bounded public semantic evidence boundary.",
                "--theme-id",
                "theme-public-semantic-perception",
                "--authoring-mode",
                "agent-authored",
                "--sample-unit",
                "public-posts",
                "--downgrade-boundary",
                "examples-only",
                "--claim-slot-supported",
                "claim-slot-public-semantics",
                "--evidence-obligation",
                "Materialize enough item-level text evidence to support only sample-local public semantic claims.",
                "--success-criterion",
                "The investigator can cite item-level refs, sample boundary, and annotation/aggregation basis.",
                "--denominator-obligation",
                "source-family-count",
                "--failure-recovery-plan",
                "downgrade-to-example-only",
                "--forbidden-precommitment",
                "Do not preselect sources, source skills, query variants, or route rankings in the plan.",
                "--payload-json",
                '{"time_window":{"start":"2023-06-01","end":"2023-06-10"}}',
            )
            with self.assertRaises(AssertionError):
                run_script(
                    script_path("submit-theme-evidence-boundary-plan"),
                    *common_args,
                    "--author-role",
                    "moderator",
                )
            payload = run_script(
                script_path("submit-theme-evidence-boundary-plan"),
                *common_args,
                "--author-role",
                "social-investigator",
            )
            self.assertEqual("completed", payload["status"])
            with self.assertRaises(AssertionError):
                run_script(
                    script_path("submit-theme-evidence-boundary-plan"),
                    *common_args,
                    "--author-role",
                    "social-investigator",
                    "--source-family-candidate",
                    "youtube-public-discourse",
                )

    def test_validator_blocks_policy_lane_misuse_and_missing_interaction_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = ["The policy was effective and improved the smoke response."]
            draft["source_material"] = {
                "theme_sufficiency_reviews": [{"review_id": "rev-policy"}],
                "policy_lane": {"official_action_or_governance_record_basis_visible": False},
                "theme_evidence_boundary_plans": [
                    {
                        "object_kind": "theme-evidence-boundary-plan",
                        "theme_id": "policy_evaluation_basis",
                        "source_family_candidates": ["policy_evaluation_basis"],
                    }
                ],
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("policy-evaluation-basis-as-acquisition-lane", codes)
            self.assertIn("policy-lane-absence-claim-downgrade-required", codes)

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "The same-day interaction is reported as co-visible descriptive chronology, not causality."
            ]
            draft["source_material"] = {
                "section_briefs": [
                    {
                        "brief_id": "section-brief-fpp",
                        "section_key": "fact-policy-public-interaction-timeline",
                        "denominator": {"interaction_node_count": 1, "lane_episode_card_count": 2},
                    }
                ],
                "interaction_timeline": {
                    "section_brief_count": 1,
                    "interaction_node_count": 1,
                    "lane_episode_card_count": 2,
                },
                "interaction_timeline_nodes": [
                    {
                        "node_id": "node-missing-summary",
                        "fact_or_policy_evidence_refs": ["signal:formal"],
                        "public_or_media_evidence_refs": ["signal:public"],
                    }
                ],
                "lane_episode_cards": [{"episode_id": "fact"}, {"episode_id": "public"}],
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("interaction-node-summary-missing", codes)

    def test_validator_requires_explicit_policy_evaluation_basis_not_only_official_action(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "The policy was effective and improved the public response."
            ]
            draft["source_material"] = {
                "policy_lane": {
                    "official_action_or_governance_record_basis_visible": True
                },
                "section_briefs": [
                    {
                        "brief_id": "section-brief-official-only",
                        "agent_role": "social-investigator",
                        "section_key": "official_action_record",
                        "section_role": "Official action record",
                        "claim_strength": "bounded-supported",
                        "main_claims": [
                            "Official action record is visible in the carried basis."
                        ],
                        "evidence_refs": ["signal:official-action"],
                    },
                    {
                        "brief_id": "section-brief-challenger-policy",
                        "agent_role": "challenger",
                        "section_key": "challenger_limitations",
                        "limitations": [
                            "Policy effectiveness needs explicit policy-evaluation basis."
                        ],
                    },
                ],
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("policy-evaluation-claim-without-basis", codes)

            draft["source_material"]["council_object_counts"] = {"proposal": 1}
            draft["source_material"]["situation_analysis_briefs"] = [
                {
                    "brief_id": "situation-analysis-brief-policy-basis",
                    "mission_answerable_question": "What bounded policy evaluation can the carried basis support?",
                    "central_bounded_judgement": "Policy evaluation is bounded to communication timing and coverage.",
                    "event_stage_map": [{"stage_id": "stage-policy", "stage_label": "Policy action"}],
                    "fact_process_chain": [{"summary": "Fact process indexed."}],
                    "official_action_chain": [{"summary": "Official action indexed."}],
                    "public_semantic_chain": [{"summary": "Public semantic boundary indexed."}],
                    "policy_semantic_chain": [
                        {
                            "summary": "Policy evaluation basis links official action to carried public semantics.",
                            "evidence_refs": [
                                "signal:official-action",
                                "signal:public-boundary",
                            ],
                        }
                    ],
                    "interaction_claims": [
                        {
                            "summary": "Interaction remains descriptive, not causality.",
                            "boundary": "Not causality.",
                        }
                    ],
                    "policy_evaluation_basis": [
                        {
                            "summary": "Policy evaluation basis is explicitly carried by proposal and situation brief.",
                            "basis_object_ids": ["proposal:policy-evaluation-basis"],
                            "evidence_refs": [
                                "signal:official-action",
                                "signal:public-boundary",
                            ],
                        }
                    ],
                    "recommended_report_spine": [
                        "Answer policy evaluation only within carried basis."
                    ],
                    "unresolved_claim_needs": [
                        {"summary": "Do not generalize policy effectiveness beyond carried basis."}
                    ],
                    "forbidden_writing_upgrades": [
                        "Do not write broad policy success or causal public response."
                    ],
                }
            ]
            codes_with_basis = validate_draft(run_dir, draft)
            self.assertNotIn(
                "policy-evaluation-claim-without-basis",
                codes_with_basis,
            )

    def test_validator_requires_public_semantic_source_family_denominator(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            summary_path = run_dir / "analytics" / f"public_discourse_sample_summary_{ROUND_ID}.json"
            write_json(
                summary_path,
                {
                    "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
                    "skill": "summarize-public-discourse-sample",
                    "corpus_id": "corpus-fixture",
                    "aggregation_id": "aggregation-fixture",
                    "coverage_audit_summary": {"coverage_audit_id": "coverage-fixture"},
                    "sample_definition": {"sample_count": 2},
                    "source_family_counts": [],
                    "discourse_lane_counts": [{"discourse_lane": "public", "signal_count": 2}],
                    "warnings": [],
                    "evidence_refs": ["signal:public-1"],
                    "distribution_use_policy": {
                        "label_sets_are_non_exclusive": True,
                        "sample_fractions_are_sample_local": True,
                        "do_not_sum_to_population_opinion": True,
                        "requires_council_uptake_before_reporting": True,
                    },
                    "issue_distribution": [
                        {
                            "label": "health concern",
                            "sample_fraction": 0.5,
                            "label_family_denominator": 2,
                            "annotated_signal_count": 2,
                        }
                    ],
                },
            )
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "Within the sample, the public discourse issue distribution was 50% health concern and not representative."
            ]
            draft["source_material"] = {
                "theme_sufficiency_reviews": [{"review_id": "rev-public"}],
                "public_discourse_summary": {"path": str(summary_path), "summary_id": "summary-fixture", "status": "completed"},
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("public-semantic-source-family-denominator-missing", codes)

    def test_validator_marks_missing_challenger_boundary_review_for_strong_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "The same-day interaction is reported as co-visible descriptive chronology, not causality."
            ]
            draft["source_material"] = {
                "section_briefs": [
                    {
                        "brief_id": "section-brief-interaction",
                        "agent_role": "moderator",
                        "section_key": "fact-policy-public-interaction-timeline",
                        "denominator": {
                            "interaction_node_count": 1,
                            "lane_episode_card_count": 2,
                        },
                    }
                ],
                "interaction_timeline": {
                    "section_brief_count": 1,
                    "interaction_node_count": 1,
                    "lane_episode_card_count": 2,
                },
                "interaction_timeline_nodes": [
                    {
                        "node_id": "node-bounded",
                        "summary": "Bounded co-visibility node.",
                        "fact_or_policy_evidence_refs": ["signal:formal"],
                        "public_or_media_evidence_refs": ["signal:public"],
                    }
                ],
                "lane_episode_cards": [{"episode_id": "fact"}, {"episode_id": "public"}],
            }
            self.assertIn("challenger-boundary-review-missing", validate_draft(run_dir, draft))

            draft["source_material"]["section_briefs"].append(
                {
                    "brief_id": "section-brief-challenger",
                    "agent_role": "challenger",
                    "section_key": "challenger_limitations",
                    "limitations": ["Interaction is descriptive, not causal."],
                }
            )
            self.assertNotIn("challenger-boundary-review-missing", validate_draft(run_dir, draft))

    def test_validator_treats_raw_progress_review_as_advisory_until_carried(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["sections"][0]["paragraphs"] = [
                "The policy was effective and caused a public response."
            ]
            draft["source_material"] = {
                "theme_progress_reviews": [
                    {
                        "review_id": "theme-progress-review-advisory",
                        "recommended_disposition": "satisfied-for-current-claim-strength",
                    }
                ],
            }
            codes = validate_draft(run_dir, draft)
            self.assertIn("strong-claim-without-brief-review-or-frozen-basis", codes)

            draft["source_material"]["theme_progress_reviews"][0][
                "uptake_status"
            ] = "council-carried"
            codes_carried = validate_draft(run_dir, draft)
            self.assertNotIn(
                "strong-claim-without-brief-review-or-frozen-basis",
                codes_carried,
            )

    def test_validator_warns_when_situation_analysis_brief_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            codes = validate_draft(run_dir, draft)
            self.assertIn("missing-situation-analysis-brief", codes)
            self.assertIn("weak-report-mainline", codes)

            draft["source_material"] = {
                "situation_analysis_briefs": [
                    {
                        "brief_id": "situation-analysis-brief-weak-fixture",
                    }
                ]
            }
            weak_codes = validate_draft(run_dir, draft)
            self.assertNotIn("missing-situation-analysis-brief", weak_codes)
            self.assertIn("missing-situation-analysis-mission-question", weak_codes)
            self.assertIn("missing-mission-answer", weak_codes)
            self.assertIn("weak-report-mainline", weak_codes)
            self.assertIn("situation-analysis-brief-missing-chain", weak_codes)
            self.assertIn("situation-analysis-unresolved-index-missing", weak_codes)

            draft["source_material"]["situation_analysis_briefs"][0].update(
                {
                    "mission_answerable_question": "What bounded situation analysis can the carried basis answer?",
                    "central_bounded_judgement": "The carried basis supports a bounded situation-analysis answer.",
                    "event_stage_map": [{"stage_id": "stage-1", "stage_label": "Carried basis"}],
                    "fact_process_chain": [{"summary": "Fact process chain is indexed."}],
                    "official_action_chain": [{"summary": "Official action chain is indexed."}],
                    "public_semantic_chain": [{"summary": "Public semantic chain is indexed."}],
                    "policy_semantic_chain": [{"summary": "Policy semantic chain is indexed."}],
                    "interaction_claims": [
                        {
                            "summary": "Interaction claims remain descriptive co-visibility only.",
                            "boundary": "Not causality.",
                        }
                    ],
                    "recommended_report_spine": [
                        "Answer the mission, then connect facts, public semantics, and policy limits."
                    ],
                    "unresolved_claim_needs": [
                        {
                            "summary": "Strong causal wording remains unresolved.",
                        }
                    ],
                    "forbidden_writing_upgrades": [
                        "Do not add facts absent from carried basis.",
                    ],
                }
            )
            codes_with_brief = validate_draft(run_dir, draft)
            self.assertNotIn("missing-situation-analysis-brief", codes_with_brief)
            self.assertNotIn("missing-situation-analysis-mission-question", codes_with_brief)
            self.assertNotIn("missing-mission-answer", codes_with_brief)
            self.assertNotIn("weak-report-mainline", codes_with_brief)
            self.assertNotIn("situation-analysis-brief-missing-chain", codes_with_brief)
            self.assertNotIn("situation-analysis-unresolved-index-missing", codes_with_brief)

    def test_validator_warns_when_situation_analysis_brief_is_not_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            draft["source_material"] = {
                "situation_analysis_briefs": [
                    {
                        "brief_id": "situation-analysis-brief-not-consumed",
                        "mission_answerable_question": "What bounded situation analysis can the carried basis answer?",
                        "central_bounded_judgement": (
                            "The carried basis supports a linked smoke exposure, public concern, "
                            "and policy-boundary situation line."
                        ),
                        "event_stage_map": [
                            {"stage_id": "stage-1", "stage_label": "Smoke exposure became a public concern."}
                        ],
                        "fact_process_chain": [
                            {"summary": "Smoke exposure levels and alert timing are connected as a fact process."}
                        ],
                        "official_action_chain": [
                            {"summary": "Official advisories are visible as bounded governance actions."}
                        ],
                        "public_semantic_chain": [
                            {"summary": "Public meaning centers on health risk, uncertainty, and trust cues."}
                        ],
                        "policy_semantic_chain": [
                            {"summary": "Policy assessment is limited to documented response boundaries."}
                        ],
                        "interaction_claims": [
                            {
                                "summary": "Public concern and official actions are co-visible without causal upgrading.",
                                "boundary": "Not causal attribution.",
                            }
                        ],
                        "recommended_report_spine": [
                            "Answer the mission through exposure facts, public meaning, official action, and policy limits."
                        ],
                        "unresolved_claim_needs": [
                            {"summary": "Effectiveness wording remains unresolved."}
                        ],
                        "forbidden_writing_upgrades": [
                            "Do not state policy effectiveness without an accepted policy-evaluation basis."
                        ],
                    }
                ]
            }

            codes = validate_draft(run_dir, draft)
            self.assertIn("situation-analysis-brief-not-consumed", codes)

            draft["reader_guidance"] = {
                "primary_audience": "test",
                "situation_analysis_brief_first": True,
            }
            draft["source_material"]["situation_analysis_preferred"] = True
            for section in draft["sections"]:
                if section["section_id"] == "executive-summary":
                    section["paragraphs"] = [
                        (
                            "The carried basis supports a linked smoke exposure, public concern, "
                            "and policy-boundary situation line."
                        )
                    ]
                elif section["section_id"] == "council-reasoning":
                    section["paragraphs"] = [
                        (
                            "Answer the mission through exposure facts, public meaning, "
                            "official action, and policy limits."
                        )
                    ]
            consumed_codes = validate_draft(run_dir, draft)
            self.assertNotIn("situation-analysis-brief-not-consumed", consumed_codes)

    def test_validator_flags_unfinished_audit_index_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            draft = minimal_draft()
            for section in draft["sections"]:
                if section["section_id"] == "audit-trail":
                    section["paragraphs"] = ["Audit index pending for unresolved claim refs."]
                    break
            codes = validate_draft(run_dir, draft)
            self.assertIn("unfinished-audit-index", codes)

    def test_nyc_smoke_legacy_report_backtest_flags_missing_situation_analysis(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        run_dir = repo_root / "runs" / "openclaw-realcase-nyc-smoke-authentic-council-flow-20260521"
        draft_rel_path = Path("reporting") / "narrative_report_draft_round-008-narrative-report-writing.json"
        draft_path = run_dir / draft_rel_path
        if not draft_path.exists():
            self.skipTest("NYC smoke legacy report fixture is not available")
        draft = load_json(draft_path)
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "nyc_legacy_validation.json"
            result = run_script(
                script_path("validate-narrative-report"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                draft.get("run_id", "openclaw-realcase-nyc-smoke-authentic-council-flow-20260521"),
                "--round-id",
                draft.get("round_id", "round-008-narrative-report-writing"),
                "--draft-path",
                str(draft_rel_path),
                "--output-path",
                str(output_path),
            )
            validation = load_json(Path(result["summary"]["output_path"]))
        codes = {
            str(item.get("code"))
            for item in validation.get("issues", [])
            if isinstance(item, dict)
        }
        self.assertIn("missing-situation-analysis-brief", codes)
        self.assertIn("weak-report-mainline", codes)


if __name__ == "__main__":
    unittest.main()

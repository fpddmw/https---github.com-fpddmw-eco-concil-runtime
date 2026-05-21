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


def materialize_blueprint(run_dir: Path) -> dict[str, Any]:
    payload = run_script(
        script_path("materialize-report-blueprint"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--mission-text",
        "Analyze NYC wildfire smoke air quality, official advisories, public discourse, and policy response boundaries.",
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
            for role in ("environmental-investigator", "social-investigator", "challenger"):
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
                        {
                            "position_text": f"{role} accepts the report questions with bounded claim language.",
                            "boundary_notes": [
                                "Do not convert report questions into fixed source or task queues."
                            ],
                            "proposed_agenda_questions": [
                                f"Which {role} boundary should the issue council preserve?"
                            ],
                            "proposed_program_rounds": round_proposals[role],
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
                    "environmental-investigator",
                    "social-investigator",
                    "challenger",
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
            for role in ("environmental-investigator", "social-investigator", "challenger"):
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


if __name__ == "__main__":
    unittest.main()

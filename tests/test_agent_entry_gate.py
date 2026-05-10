from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import (
    kernel_script_path,
    load_json,
    run_kernel,
    run_script,
    runtime_src_path,
    runtime_path,
    script_path,
    seed_analysis_chain,
    write_json,
)

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-agent-entry-001"
ROUND_ID = "round-agent-entry-001"


def build_raw_artifacts(root: Path) -> dict[str, Path]:
    youtube_path = root / "youtube.json"
    bluesky_path = root / "bluesky.json"
    openaq_path = root / "openaq.json"
    airnow_path = root / "airnow.json"

    write_json(
        youtube_path,
        [
            {
                "query": "nyc smoke wildfire",
                "video_id": "vid-agent-entry-001",
                "video": {
                    "id": "vid-agent-entry-001",
                    "title": "Smoke over New York City",
                    "description": "Wildfire smoke covered New York City and reduced visibility.",
                    "channel_title": "City Desk",
                    "published_at": "2023-06-07T13:00:00Z",
                    "default_language": "en",
                    "statistics": {"view_count": 1250},
                },
            }
        ],
    )
    write_json(
        bluesky_path,
        {
            "seed_posts": [
                {
                    "uri": "at://did:plc:smoke/app.bsky.feed.post/seed001",
                    "author_handle": "smoke.reporter.test",
                    "author_did": "did:plc:smoke",
                    "text": "Smoke haze over the New York skyline is intense today.",
                    "timestamp_utc": "2023-06-07T12:30:00Z",
                    "reply_count": 1,
                    "repost_count": 2,
                    "like_count": 3,
                    "quote_count": 0,
                }
            ]
        },
    )
    write_json(
        openaq_path,
        {
            "results": [
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 41.5,
                    "date": {"utc": "2023-06-07T12:00:00Z"},
                    "coordinates": {"latitude": 40.7004, "longitude": -74.0004},
                    "location": {"id": 1, "name": "NYC"},
                    "provider": {"name": "OpenAQ"},
                }
            ]
        },
    )
    write_json(
        airnow_path,
        {
            "records": [
                {
                    "parameter_name": "PM25",
                    "raw_concentration": 52.0,
                    "aqi_value": 155,
                    "latitude": 40.7002,
                    "longitude": -74.0002,
                    "observed_at_utc": "2023-06-07T12:00:00Z",
                    "site_name": "Test Site",
                    "country_code": "US",
                }
            ]
        },
    )
    return {
        "youtube": youtube_path,
        "bluesky": bluesky_path,
        "openaq": openaq_path,
        "airnow": airnow_path,
    }


def build_mission_file(root: Path, artifacts: dict[str, Path]) -> Path:
    mission_path = root / "mission.json"
    write_json(
        mission_path,
        {
            "schema_version": "1.0.0",
            "run_id": RUN_ID,
            "topic": "NYC smoke verification",
            "objective": "Determine whether public smoke reports are supported by physical evidence.",
            "policy_profile": "standard",
            "window": {
                "start_utc": "2023-06-07T00:00:00Z",
                "end_utc": "2023-06-07T23:59:59Z",
            },
            "region": {
                "label": "New York City, USA",
                "geometry": {
                    "type": "Point",
                    "latitude": 40.7128,
                    "longitude": -74.0060,
                },
            },
            "hypotheses": [
                {
                    "title": "Smoke over NYC was materially significant",
                    "statement": "Public smoke reports are backed by elevated PM2.5 observations.",
                    "confidence": 0.55,
                }
            ],
            "artifact_imports": [
                {
                    "source_skill": "fetch-youtube-video-search",
                    "artifact_path": str(artifacts["youtube"]),
                    "query_text": "nyc smoke wildfire",
                },
                {
                    "source_skill": "fetch-bluesky-cascade",
                    "artifact_path": str(artifacts["bluesky"]),
                },
                {
                    "source_skill": "fetch-openaq",
                    "artifact_path": str(artifacts["openaq"]),
                    "source_mode": "test-fixture",
                },
                {
                    "source_skill": "fetch-airnow-hourly-observations",
                    "artifact_path": str(artifacts["airnow"]),
                },
            ],
        },
    )
    return mission_path


class AgentEntryGateTests(unittest.TestCase):
    def test_materialize_agent_entry_gate_creates_gate_and_capability_surface(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            payload = run_kernel(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            gate_artifact = load_json(runtime_path(run_dir, f"agent_entry_gate_{ROUND_ID}.json"))
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual("runtime-operator", payload["summary"]["requested_by_role"])
            self.assertEqual("ready", payload["summary"]["entry_status"])
            self.assertEqual("openclaw-agent", payload["agent_entry"]["orchestration_mode"])
            self.assertEqual("runtime-operator", payload["agent_entry"]["requested_by_role"])
            self.assertEqual(
                "Determine whether public smoke reports are supported by physical evidence.",
                payload["agent_entry"]["mission"]["request_text"],
            )
            self.assertIn(
                "user-facing request envelope",
                payload["agent_entry"]["mission"]["mission_input_semantics"]["meaning"],
            )
            self.assertEqual(
                "runtime-operator",
                payload["agent_entry"]["resolved_requested_by_role"],
            )
            self.assertEqual([], payload["agent_entry"]["recommended_entry_skills"])
            self.assertEqual(
                payload["agent_entry"]["capability_surface"],
                payload["agent_entry"]["role_entry_points"],
            )
            self.assertTrue(
                any(
                    entry.get("role") == "environmental-investigator"
                    for entry in payload["agent_entry"]["capability_surface"]
                    if isinstance(entry, dict)
                )
            )

    def test_agent_entry_gate_exposes_coordination_surface_without_narrowing_roles(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )
            context_packet = run_kernel(
                "submit-dynamic-investigation-object",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "moderator",
                "--object-kind",
                "context-packet",
                "--author-role",
                "moderator",
                "--target-id",
                ROUND_ID,
                "--rationale",
                "Expose a compact context pointer for agent entry.",
                "--payload-json",
                json.dumps(
                    {
                        "object_id": "context-packet-agent-entry-001",
                        "packet_profile": "investigation",
                        "target_round_id": ROUND_ID,
                        "included_object_refs": ["round:" + ROUND_ID],
                        "excluded_object_refs": ["raw-records:*"],
                        "summary_text": "Compact context only; agents may request expansion.",
                        "raw_data_policy": "refs-only",
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
            )
            brief_payload = run_script(
                script_path("submit-round-brief"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--object-id",
                "round-brief-agent-entry-001",
                "--target-id",
                ROUND_ID,
                "--rationale",
                "Moderator exposes non-binding round context.",
                "--round-mode",
                "investigation",
                "--context-packet-id",
                context_packet["summary"]["object_id"],
                "--primary-focus-ref",
                "subissue:air-quality",
                "--requested-output",
                "evidence-request",
                "--source-boundary-note",
                "Do not narrow legal read or write surfaces.",
                "--open-question",
                "What additional evidence would clarify timing?",
                "--brief-text",
                "Use this as context, not a hard agenda.",
            )

            payload = run_kernel(
                "materialize-agent-entry-gate",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            coordination = payload["agent_entry"]["coordination_surface"]
            social_entry = next(
                entry
                for entry in payload["agent_entry"]["role_entry_points"]
                if isinstance(entry, dict) and entry.get("role") == "social-investigator"
            )
            moderator_entry = next(
                entry
                for entry in payload["agent_entry"]["role_entry_points"]
                if isinstance(entry, dict) and entry.get("role") == "moderator"
            )
            challenger_entry = next(
                entry
                for entry in payload["agent_entry"]["role_entry_points"]
                if isinstance(entry, dict) and entry.get("role") == "challenger"
            )
            social_write_surface = "\n".join(social_entry["write_commands"])
            social_read_surface = "\n".join(social_entry["coordination_read_commands"])
            moderator_write_surface = "\n".join(moderator_entry["write_commands"])
            challenger_write_surface = "\n".join(challenger_entry["write_commands"])
            challenger_read_surface = "\n".join(
                challenger_entry["coordination_read_commands"]
            )

            self.assertEqual(
                brief_payload["summary"]["object_id"],
                coordination["latest_round_brief"]["object_id"],
            )
            self.assertEqual(
                context_packet["summary"]["object_id"],
                coordination["context_packet"]["object_id"],
            )
            self.assertIn("does not restrict", coordination["semantics"])
            self.assertEqual(
                ["Do not narrow legal read or write surfaces."],
                coordination["latest_round_brief"]["source_boundary_notes"],
            )
            self.assertIn("query-council-objects", social_read_surface)
            self.assertIn("--object-kind round-brief", social_read_surface)
            self.assertIn("--object-kind context-packet", social_read_surface)
            self.assertIn("--object-kind challenge-disposition", challenger_read_surface)
            self.assertIn("materialize-context-packet", moderator_write_surface)
            self.assertIn("submit-evidence-request", social_write_surface)
            self.assertIn("submit-agent-position", social_write_surface)
            self.assertIn(
                "submit-challenge-disposition",
                challenger_write_surface,
            )
            self.assertTrue(
                state_payload["agent_entry"]["operator"]["coordination_surface_present"]
            )
            self.assertEqual(
                brief_payload["summary"]["object_id"],
                state_payload["agent_entry"]["operator"]["latest_round_brief_id"],
            )
            self.assertIn(
                "--object-kind round-brief",
                state_payload["agent_entry"]["operator"]["query_round_briefs_command"],
            )
            self.assertIn(
                "--object-kind context-packet",
                state_payload["agent_entry"]["operator"]["query_context_packets_command"],
            )

    def test_start_council_run_scaffolds_prepare_and_registration_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)
            mission_input_path = run_dir / "input" / "mission.json"
            mission_input_path.parent.mkdir(parents=True, exist_ok=True)
            mission_input_path.write_text(mission_path.read_text(encoding="utf-8"), encoding="utf-8")

            payload = run_kernel(
                "start-council-run",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_input_path),
                "--pretty",
            )

            registration = payload["openclaw_agent_registration"]
            entry_gate = payload["agent_entry_gate"]["agent_entry"]
            gate_artifact = load_json(runtime_path(run_dir, f"agent_entry_gate_{ROUND_ID}.json"))
            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            roles = {
                item["role"]
                for item in registration["registrations"]
                if isinstance(item, dict)
            }

            self.assertEqual("completed", payload["status"])
            self.assertEqual("openclaw-agent", payload["summary"]["orchestration_mode"])
            self.assertEqual("ready", payload["summary"]["entry_status"])
            self.assertIn("social-investigator", roles)
            self.assertIn("environmental-investigator", roles)
            self.assertNotIn("social_investigator", roles)
            self.assertNotIn("public-discourse-investigator", roles)
            self.assertTrue((run_dir / "supervisor" / "openclaw-workspaces" / "social-investigator").exists())
            social_workspace = run_dir / "supervisor" / "openclaw-workspaces" / "social-investigator"
            social_role_surface = load_json(social_workspace / "council_runtime" / "role_surface.json")
            social_runtime_context = load_json(social_workspace / "council_runtime" / "runtime_context.json")
            social_coordination_surface = load_json(social_workspace / "council_runtime" / "coordination_surface.json")
            self.assertEqual("social-investigator", social_role_surface["role"])
            self.assertEqual("social-investigator", social_runtime_context["role"])
            self.assertEqual(
                entry_gate["mission"]["request_text"],
                social_runtime_context["mission"]["request_text"],
            )
            self.assertIn(
                "user-facing request envelope",
                social_runtime_context["mission"]["mission_input_semantics"]["meaning"],
            )
            self.assertTrue(
                social_runtime_context["coordination_surface_path"].endswith(
                    "coordination_surface.json"
                )
            )
            self.assertEqual(
                "agent-entry-coordination-surface-v1",
                social_coordination_surface["schema_version"],
            )
            self.assertTrue((social_workspace / "COUNCIL_RUNTIME.md").exists())
            self.assertIn("openclaw agents add", registration["register_all_command"])
            self.assertIn("openclaw agents set-identity", registration["register_all_command"])
            self.assertNotIn(" --identity ", registration["register_all_command"])
            self.assertTrue(
                any(
                    item.get("role") == "social-investigator"
                    and item.get("role_surface_path", "").endswith("role_surface.json")
                    for item in registration["workspace_contexts"]
                    if isinstance(item, dict)
                )
            )
            self.assertTrue(runtime_path(run_dir, f"source_selection_social-investigator_{ROUND_ID}.json").exists())
            self.assertTrue(runtime_path(run_dir, f"agent_entry_gate_{ROUND_ID}.json").exists())
            self.assertTrue(
                all(
                    entry.get("analysis_commands") == []
                    for entry in entry_gate["capability_surface"]
                    if isinstance(entry, dict)
                )
            )
            self.assertTrue(
                any(
                    "submit-council-proposal" in command
                    for entry in entry_gate["capability_surface"]
                    if isinstance(entry, dict)
                    for command in entry.get("write_commands", [])
                    if isinstance(entry.get("write_commands"), list)
                )
            )
            self.assertTrue(
                any(
                    "submit-readiness-opinion" in command
                    for entry in entry_gate["capability_surface"]
                    if isinstance(entry, dict)
                    for command in entry.get("write_commands", [])
                    if isinstance(entry.get("write_commands"), list)
                )
            )
            environmental_entries = [
                entry
                for entry in entry_gate["role_entry_points"]
                if isinstance(entry, dict) and entry.get("role") == "environmental-investigator"
            ]
            self.assertEqual(1, len(environmental_entries))
            environmental_fetch_surface = "\n".join(environmental_entries[0].get("fetch_commands", []))
            environmental_normalize_surface = "\n".join(environmental_entries[0].get("normalize_commands", []))
            self.assertIn("fetch-open-meteo-air-quality", environmental_fetch_surface)
            self.assertIn("--actor-role environmental-investigator", environmental_fetch_surface)
            self.assertIn("--allow-side-effect network-external", environmental_fetch_surface)
            self.assertIn("<skill_specific_args>", environmental_fetch_surface)
            self.assertIn("normalize-fetch-execution", environmental_normalize_surface)
            self.assertIn("--actor-role environmental-investigator", environmental_normalize_surface)
            challenger_entries = [
                entry
                for entry in entry_gate["role_entry_points"]
                if isinstance(entry, dict) and entry.get("role") == "challenger"
            ]
            self.assertEqual(1, len(challenger_entries))
            challenger_fetch_surface = "\n".join(challenger_entries[0].get("fetch_commands", []))
            challenger_normalize_surface = "\n".join(challenger_entries[0].get("normalize_commands", []))
            self.assertIn("fetch-gdelt-doc-search", challenger_fetch_surface)
            self.assertIn("--actor-role challenger", challenger_fetch_surface)
            self.assertIn("normalize-fetch-execution", challenger_normalize_surface)
            self.assertIn("--actor-role challenger", challenger_normalize_surface)
            challenger_write_surface = "\n".join(challenger_entries[0].get("write_commands", []))
            self.assertIn("post-review-comment", challenger_write_surface)
            self.assertIn("--actor-role challenger", challenger_write_surface)
            self.assertIn("--author-role challenger", challenger_write_surface)
            self.assertIn("submit-finding-record", challenger_write_surface)
            self.assertIn("submit-evidence-bundle", challenger_write_surface)
            self.assertIn("post-board-note", challenger_write_surface)
            self.assertIn("open-challenge-ticket", challenger_write_surface)
            self.assertIn("open-falsification-probe", challenger_write_surface)
            self.assertNotIn("request-phase-transition", challenger_write_surface)
            self.assertTrue(any(item["step_id"] == "submit-council-proposal" for item in entry_gate["entry_chain"]))
            self.assertTrue(any(item["step_id"] == "submit-readiness-opinion" for item in entry_gate["entry_chain"]))
            self.assertTrue(any(item["step_id"] == "request-report-basis-transition" for item in entry_gate["entry_chain"]))
            self.assertTrue(any(item["step_id"] == "approve-transition-request" for item in entry_gate["entry_chain"]))
            self.assertTrue(any(item["step_id"] == "return-to-runtime-gate" for item in entry_gate["entry_chain"]))
            self.assertEqual("runtime-agent-entry-gate-v1", gate_artifact["schema_version"])
            self.assertTrue(state_payload["agent_entry"]["operator"]["entry_gate_present"])
            self.assertIn(
                "materialize-agent-entry-gate",
                state_payload["agent_entry"]["operator"]["refresh_agent_entry_gate_command"],
            )
            self.assertIn(
                "--actor-role runtime-operator",
                state_payload["agent_entry"]["operator"]["refresh_agent_entry_gate_command"],
            )
            self.assertIn(
                "query-council-objects",
                state_payload["agent_entry"]["operator"]["query_council_proposals_command"],
            )
            self.assertIn(
                "query-control-objects",
                state_payload["agent_entry"]["operator"]["query_transition_requests_command"],
            )
            self.assertIn(
                "skill-approval-request",
                state_payload["agent_entry"]["operator"]["query_skill_approval_requests_command"],
            )
            self.assertIn(
                "request-skill-approval",
                state_payload["agent_entry"]["operator"]["request_optional_analysis_approval_command_template"],
            )
            approved_optional_command = state_payload["agent_entry"]["operator"]["run_approved_optional_analysis_command_template"]
            self.assertIn("--skill-approval-request-id '<request_id>'", approved_optional_command)
            self.assertLess(
                approved_optional_command.index("--skill-approval-request-id"),
                approved_optional_command.index("-- '<skill_specific_args>'"),
            )
            self.assertIn(
                "query-council-objects",
                state_payload["agent_entry"]["operator"]["query_finding_records_command"],
            )
            self.assertIn(
                "query-council-objects",
                state_payload["agent_entry"]["operator"]["query_discussion_messages_command"],
            )
            self.assertIn(
                "query-council-objects",
                state_payload["agent_entry"]["operator"]["query_review_comments_command"],
            )
            self.assertIn(
                "query-council-objects",
                state_payload["agent_entry"]["operator"]["query_evidence_bundles_command"],
            )
            self.assertIn(
                "query-reporting-objects",
                state_payload["agent_entry"]["operator"]["query_report_section_drafts_command"],
            )
            self.assertIn(
                "submit-council-proposal",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--actor-role",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--confidence",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--evidence-ref",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--response-to-id '<finding_or_bundle_id>'",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "--provenance-json",
                state_payload["agent_entry"]["operator"]["submit_council_proposal_command_template"],
            )
            self.assertIn(
                "submit-finding-record",
                state_payload["agent_entry"]["operator"]["submit_finding_record_command_template"],
            )
            self.assertIn(
                "post-discussion-message",
                state_payload["agent_entry"]["operator"]["post_discussion_message_command_template"],
            )
            self.assertIn(
                "post-review-comment",
                state_payload["agent_entry"]["operator"]["post_review_comment_command_template"],
            )
            self.assertIn(
                "submit-evidence-bundle",
                state_payload["agent_entry"]["operator"]["submit_evidence_bundle_command_template"],
            )
            self.assertIn(
                "submit-report-section-draft",
                state_payload["agent_entry"]["operator"]["submit_report_section_draft_command_template"],
            )
            self.assertIn(
                "request-phase-transition",
                state_payload["agent_entry"]["operator"]["request_report_basis_transition_command"],
            )
            self.assertIn(
                "approve-phase-transition",
                state_payload["agent_entry"]["operator"]["approve_transition_request_command_template"],
            )
            self.assertNotIn("materialize_agent_advisory_plan_command", state_payload["agent_entry"]["operator"])
            self.assertNotIn("agent_advisory_plan_path", state_payload["agent_entry"]["operator"])
            self.assertIn(
                "supervise-round",
                state_payload["agent_entry"]["operator"]["return_to_supervisor_command"],
            )
            self.assertEqual("eco_runtime_kernel.py", kernel_script_path().name)

    def test_materialize_agent_entry_gate_does_not_expose_advisory_surface(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            from eco_council_runtime.objects.council import store_readiness_opinion_records
            from eco_council_runtime.kernel.governance.agent_entry import materialize_agent_entry_gate
            from eco_council_runtime.kernel.governance.agent_entry.profile import (
                default_agent_entry_profile,
            )
            from eco_council_runtime.kernel.governance.agent_entry.handoff import (
                default_agent_entry_chain,
                default_agent_entry_hard_gate_commands,
            )

            store_readiness_opinion_records(
                run_dir,
                opinion_bundle={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "opinions": [
                        {
                            "agent_role": "moderator",
                            "readiness_status": "ready",
                            "sufficient_for_report_basis": True,
                            "rationale": "The council has already converged on report-basis readiness.",
                            "decision_source": "agent-council",
                            "basis_object_ids": ["issue-001"],
                            "evidence_refs": ["evidence://issue-001"],
                            "lineage": [],
                        }
                    ],
                },
            )

            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=default_agent_entry_profile(),
                hard_gate_command_builder=default_agent_entry_hard_gate_commands,
                entry_chain_builder=default_agent_entry_chain,
                contract_mode="warn",
            )
            self.assertEqual("completed", payload["status"])
            self.assertNotIn("advisory_plan_materialized", payload["summary"])
            self.assertNotIn("advisory_plan_source", payload["summary"])
            self.assertNotIn("advisory_plan_present", payload["summary"])
            self.assertNotIn("advisory_plan", payload["agent_entry"])

    def test_materialize_agent_entry_gate_accepts_injected_handoff_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            from eco_council_runtime.kernel.governance.agent_entry import materialize_agent_entry_gate
            from eco_council_runtime.kernel.governance.agent_entry.profile import (
                default_agent_entry_profile,
            )

            def custom_hard_gate_commands(**_: object) -> dict[str, str]:
                return {
                    "show_run_state": "custom-show-state",
                    "supervise_round": "custom-supervise-round",
                    "apply_report_basis_gate": "custom-apply-gate",
                    "close_round": "custom-close-round",
                    "open_next_round": "custom-open-next-round",
                }

            def custom_entry_chain(**_: object) -> list[dict[str, str]]:
                return [
                    {
                        "step_id": "custom-runtime-handoff",
                        "mode": "runtime-gate",
                        "objective": "Use the injected handoff profile.",
                        "command": "custom-supervise-round",
                    }
                ]

            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=default_agent_entry_profile(),
                hard_gate_command_builder=custom_hard_gate_commands,
                entry_chain_builder=custom_entry_chain,
                contract_mode="warn",
            )

            self.assertEqual(
                "custom-supervise-round",
                payload["agent_entry"]["hard_gate_commands"]["supervise_round"],
            )
            self.assertEqual(
                "custom-runtime-handoff",
                payload["agent_entry"]["entry_chain"][0]["step_id"],
            )

    def test_materialize_agent_entry_gate_accepts_injected_agent_entry_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            artifacts = build_raw_artifacts(root)
            mission_path = build_mission_file(root, artifacts)

            run_script(
                script_path("scaffold-mission-run"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--mission-path",
                str(mission_path),
                "--orchestration-mode",
                "openclaw-agent",
            )
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)

            from eco_council_runtime.kernel.governance.agent_entry import (
                agent_entry_state,
                materialize_agent_entry_gate,
            )
            from eco_council_runtime.kernel.governance.agent_entry.profile import (
                default_agent_entry_profile,
            )
            from eco_council_runtime.kernel.governance.agent_entry.handoff import (
                default_agent_entry_chain,
                default_agent_entry_hard_gate_commands,
            )

            custom_profile = default_agent_entry_profile()
            custom_profile["role_definitions"] = [
                {
                    "role": "auditor",
                    "focus": "Custom review lane only.",
                    "read_skills": [],
                    "write_skills": [],
                    "analysis_kinds": [],
                }
            ]
            custom_profile["role_entry_builder"] = lambda **_: [
                {
                    "role": "auditor",
                    "focus": "Custom review lane only.",
                    "read_commands": ["custom-read-state"],
                    "analysis_commands": ["custom-query-analysis"],
                    "write_commands": ["custom-write-note"],
                }
            ]
            custom_profile["recommended_skills_builder"] = lambda **_: ["audit-round"]
            custom_profile["operator_notes_builder"] = lambda **_: ["Use the injected entry profile."]
            custom_profile["next_round_id_builder"] = lambda **_: "round-agent-entry-custom-next"
            custom_profile["operator_commands_builder"] = lambda **_: {
                "materialize_agent_entry_gate_command": "custom-materialize-entry-gate",
                "refresh_agent_entry_gate_command": "custom-refresh-entry-gate",
                "read_board_delta_command": "custom-read-board",
                "query_public_signals_command": "custom-query-public",
                "query_formal_signals_command": "custom-query-formal",
                "query_environment_signals_command": "custom-query-environment",
            }

            payload = materialize_agent_entry_gate(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=custom_profile,
                hard_gate_command_builder=default_agent_entry_hard_gate_commands,
                entry_chain_builder=default_agent_entry_chain,
                contract_mode="warn",
            )
            state_payload = agent_entry_state(
                run_dir,
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_entry_profile=custom_profile,
                hard_gate_command_builder=default_agent_entry_hard_gate_commands,
            )

            self.assertEqual(["audit-round"], payload["agent_entry"]["recommended_entry_skills"])
            self.assertEqual("auditor", payload["agent_entry"]["role_entry_points"][0]["role"])
            self.assertEqual(
                "Use the injected entry profile.",
                payload["agent_entry"]["operator_notes"][0],
            )
            self.assertEqual(
                "custom-materialize-entry-gate",
                state_payload["operator"]["materialize_agent_entry_gate_command"],
            )
            self.assertEqual(
                "custom-query-formal",
                state_payload["operator"]["query_formal_signals_command"],
            )
            self.assertIn(
                "round-agent-entry-custom-next",
                payload["agent_entry"]["hard_gate_commands"]["open_next_round"],
            )
            self.assertIn(
                "round-agent-entry-custom-next",
                state_payload["operator"]["open_next_round_command_template"],
            )

    def test_show_run_state_surfaces_agent_entry_commands_before_gate_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_analysis_chain(run_dir, root, RUN_ID, ROUND_ID, include_airnow=True)
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)

            state_payload = run_kernel(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )

            operator = state_payload["agent_entry"]["operator"]
            self.assertFalse(operator["entry_gate_present"])
            self.assertIn("materialize-agent-entry-gate", operator["materialize_agent_entry_gate_command"])
            self.assertNotIn("list_claim_cluster_result_sets_command", operator)
            self.assertNotIn("query_claim_cluster_items_command_template", operator)
            self.assertTrue(
                all(
                    "claim-cluster" not in command
                    for command in operator.values()
                    if isinstance(command, str)
                )
            )
            self.assertIn("query-board-delta", operator["read_board_delta_command"])
            self.assertIn("query-public-signals", operator["query_public_signals_command"])
            self.assertIn("query-formal-signals", operator["query_formal_signals_command"])
            self.assertIn("query-council-objects", operator["query_council_proposals_command"])
            self.assertIn("query-council-objects", operator["query_readiness_opinions_command"])
            self.assertIn("query-control-objects", operator["query_transition_requests_command"])
            self.assertIn("skill-approval-request", operator["query_skill_approval_requests_command"])
            self.assertIn("skill-approval", operator["query_skill_approvals_command"])
            self.assertIn("request-skill-approval", operator["request_optional_analysis_approval_command_template"])
            self.assertIn("approve-skill-approval", operator["approve_skill_approval_command_template"])
            self.assertIn("--skill-approval-request-id", operator["run_approved_optional_analysis_command_template"])
            self.assertLess(
                operator["run_approved_optional_analysis_command_template"].index("--skill-approval-request-id"),
                operator["run_approved_optional_analysis_command_template"].index("-- '<skill_specific_args>'"),
            )
            self.assertIn("query-council-objects", operator["query_finding_records_command"])
            self.assertIn("query-council-objects", operator["query_discussion_messages_command"])
            self.assertIn("query-council-objects", operator["query_review_comments_command"])
            self.assertIn("query-council-objects", operator["query_evidence_bundles_command"])
            self.assertIn("query-reporting-objects", operator["query_report_section_drafts_command"])
            self.assertIn("submit-council-proposal", operator["submit_council_proposal_command_template"])
            self.assertIn("--confidence", operator["submit_council_proposal_command_template"])
            self.assertIn("--response-to-id '<finding_or_bundle_id>'", operator["submit_council_proposal_command_template"])
            self.assertIn("--evidence-ref", operator["submit_council_proposal_command_template"])
            self.assertIn("--provenance-json", operator["submit_council_proposal_command_template"])
            self.assertIn("submit-readiness-opinion", operator["submit_readiness_opinion_command_template"])
            self.assertIn("submit-finding-record", operator["submit_finding_record_command_template"])
            self.assertIn("post-discussion-message", operator["post_discussion_message_command_template"])
            self.assertIn("post-review-comment", operator["post_review_comment_command_template"])
            self.assertIn("submit-evidence-bundle", operator["submit_evidence_bundle_command_template"])
            self.assertIn("submit-report-section-draft", operator["submit_report_section_draft_command_template"])
            self.assertIn("request-phase-transition", operator["request_report_basis_transition_command"])

    def test_operator_runbook_includes_agent_entry_section_for_round(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            run_kernel("init-run", "--run-dir", str(run_dir), "--run-id", RUN_ID)

            payload = run_kernel(
                "materialize-operator-runbook",
                "--run-dir",
                str(run_dir),
                "--round-id",
                ROUND_ID,
                "--pretty",
            )
            runbook_text = Path(payload["operator_runbook_path"]).read_text(encoding="utf-8")

            self.assertIn("## Agent Entry", runbook_text)
            self.assertIn("materialize-agent-entry-gate", runbook_text)
            self.assertIn("--actor-role runtime-operator", runbook_text)
            self.assertIn("--actor-role moderator", runbook_text)
            self.assertIn("query-council-objects", runbook_text)
            self.assertIn("request-phase-transition", runbook_text)
            self.assertIn("request-skill-approval", runbook_text)
            self.assertIn("--skill-approval-request-id '<request_id>'", runbook_text)
            self.assertLess(
                runbook_text.index("--skill-approval-request-id '<request_id>'"),
                runbook_text.index("-- --example-arg"),
            )


if __name__ == "__main__":
    unittest.main()

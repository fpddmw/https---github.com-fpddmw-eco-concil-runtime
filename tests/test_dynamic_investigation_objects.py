from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.contracts import canonical_contract  # noqa: E402
from eco_council_runtime.objects.council import (  # noqa: E402
    DYNAMIC_INVESTIGATION_OBJECT_KINDS,
    append_dynamic_investigation_object_record,
    query_council_objects,
)

RUN_ID = "run-dynamic-investigation-001"
ROUND_ID = "round-dynamic-investigation-001"


class DynamicInvestigationObjectTests(unittest.TestCase):
    def test_dynamic_investigation_contracts_are_thin_envelopes(self) -> None:
        for object_kind in DYNAMIC_INVESTIGATION_OBJECT_KINDS:
            contract = canonical_contract(object_kind)

            self.assertEqual("object_id", contract.id_field)
            self.assertEqual((), contract.required_number_fields)
            self.assertEqual((), contract.required_non_empty_list_fields)
            self.assertNotIn("confidence", contract.required_text_fields)
            self.assertIn("object_kind", contract.required_text_fields)
            self.assertIn("evidence_refs", contract.required_list_fields)

    def test_dynamic_objects_store_and_query_without_evidence_scoring(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            plan_record = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="investigation-plan",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "investigation-plan",
                    "author_role": "moderator",
                    "status": "draft",
                    "target_kind": "mission",
                    "target_id": "mission-001",
                    "rationale": "Frame the next investigation pass without binding evidence uptake.",
                    "open_questions": ["What sources should agents inspect next?"],
                    "subissue_ids": ["subissue-air-quality"],
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )
            append_dynamic_investigation_object_record(
                run_dir,
                object_kind="evidence-request",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "evidence-request",
                    "author_role": "challenger",
                    "target_kind": "subissue",
                    "target_id": "subissue-air-quality",
                    "rationale": "Ask for contradicting records without ranking sources.",
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )

            query = query_council_objects(
                run_dir,
                object_kind="investigation-plan",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                agent_role="moderator",
                status="draft",
                target_kind="mission",
                target_id="mission-001",
            )

            self.assertEqual(1, query["summary"]["returned_object_count"])
            plan = query["objects"][0]
            self.assertEqual(plan_record["object"]["object_id"], plan["object_id"])
            self.assertEqual("investigation-plan", plan["object_kind"])
            self.assertEqual([], plan["evidence_refs"])
            self.assertNotIn("confidence", plan)
            self.assertNotIn("priority", plan)

    def test_dynamic_objects_reject_heuristic_control_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            with self.assertRaisesRegex(ValueError, "heuristic/control field `priority`"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="round-brief",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "round-brief",
                        "author_role": "moderator",
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "Brief agents without imposing ordered agenda.",
                        "priority": "high",
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_source_acquisition_proposal_is_thin_and_queryable_by_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            record = append_dynamic_investigation_object_record(
                run_dir,
                object_kind="source-acquisition-proposal",
                object_payload={
                    "run_id": RUN_ID,
                    "round_id": ROUND_ID,
                    "object_kind": "source-acquisition-proposal",
                    "author_role": "environmental-investigator",
                    "source_skill": "fetch-open-meteo-historical",
                    "query_parameters": {
                        "latitude": 40.7128,
                        "longitude": -74.0060,
                        "start_date": "2023-06-07",
                    },
                    "declared_side_effects": [
                        "network-external",
                        "writes-artifacts",
                    ],
                    "requested_side_effect_approvals": [],
                    "target_kind": "evidence-request",
                    "target_id": "evidence-request-weather-context",
                    "rationale": "Record the agent-selected weather context source without ranking it.",
                    "evidence_refs": [],
                    "provenance": {"source": "unit-test"},
                },
            )

            query = query_council_objects(
                run_dir,
                object_kind="source-acquisition-proposal",
                run_id=RUN_ID,
                round_id=ROUND_ID,
                source_skill="fetch-open-meteo-historical",
                target_evidence_request_id="evidence-request-weather-context",
            )

            self.assertEqual(1, query["summary"]["returned_object_count"])
            proposal = query["objects"][0]
            self.assertEqual(record["object"]["object_id"], proposal["object_id"])
            self.assertEqual(record["object"]["proposal_id"], proposal["proposal_id"])
            self.assertEqual("source-acquisition-proposal", proposal["object_kind"])
            self.assertEqual("proposed", proposal["status"])
            self.assertEqual(
                "fetch-open-meteo-historical",
                proposal["source_skill"],
            )
            self.assertEqual([], proposal["evidence_refs"])
            self.assertNotIn("score", proposal)
            self.assertNotIn("rank", proposal)
            self.assertNotIn("priority", proposal)

            status_surface = run_kernel(
                "show-source-acquisition-intents",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            submit_template = status_surface["commands"]["submit_source_acquisition_proposal_template"]
            self.assertIn("--author-role '<agent_role>'", submit_template)
            self.assertIn("--query-parameters-json", submit_template)
            self.assertIn("--rationale", submit_template)
            self.assertNotIn("--proposal-text", submit_template)
            self.assertEqual(1, len(status_surface["objects"]))
            execution_surface = status_surface["objects"][0]["source_execution_surface"]
            self.assertEqual("fetch-open-meteo-historical", execution_surface["source_skill"])
            self.assertTrue(execution_surface["provider_modes"])
            self.assertTrue(execution_surface["fetch_command_templates"])
            self.assertIn("normalize-fetch-execution", execution_surface["normalize_fetch_execution_command"])

    def test_source_acquisition_proposal_rejects_wrong_role_for_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            with self.assertRaisesRegex(ValueError, "cannot execute fetch-open-meteo-historical"):
                append_dynamic_investigation_object_record(
                    run_dir,
                    object_kind="source-acquisition-proposal",
                    object_payload={
                        "run_id": RUN_ID,
                        "round_id": ROUND_ID,
                        "object_kind": "source-acquisition-proposal",
                        "author_role": "social-investigator",
                        "source_skill": "fetch-open-meteo-historical",
                        "query_parameters": {},
                        "target_kind": "round",
                        "target_id": ROUND_ID,
                        "rationale": "This role should not acquire environmental weather data.",
                        "evidence_refs": [],
                        "provenance": {"source": "unit-test"},
                    },
                )

    def test_kernel_submits_and_queries_dynamic_investigation_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            submit_payload = run_kernel(
                "submit-dynamic-investigation-object",
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--actor-role",
                "social-investigator",
                "--object-kind",
                "agent-position",
                "--author-role",
                "social-investigator",
                "--target-kind",
                "subissue",
                "--target-id",
                "subissue-public-record",
                "--rationale",
                "State a provisional social-record position without scoring evidence.",
                "--evidence-ref",
                "evidence://public-record/001",
                "--payload-json",
                json.dumps(
                    {
                        "position_text": "Public records appear relevant, pending agent synthesis.",
                        "source_object_ids": ["formal-record-001"],
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                ),
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            object_id = submit_payload["canonical_ids"][0]

            query_payload = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "agent-position",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "social-investigator",
                "--target-kind",
                "subissue",
                "--target-id",
                "subissue-public-record",
            )

            self.assertEqual(1, query_payload["summary"]["returned_object_count"])
            self.assertEqual(object_id, query_payload["objects"][0]["object_id"])
            self.assertEqual("agent-position", query_payload["objects"][0]["object_kind"])
            self.assertEqual(
                ["evidence://public-record/001"],
                query_payload["objects"][0]["evidence_refs"],
            )


if __name__ == "__main__":
    unittest.main()

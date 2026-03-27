from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.reporting_artifacts import (  # noqa: E402
    curation_artifacts,
    data_readiness_artifacts,
    decision_artifacts,
    promote_decision_draft,
    render_openclaw_prompts,
)
from eco_council_runtime.contract import scaffold_run_from_mission  # noqa: E402
from eco_council_runtime.controller.audit_chain import read_jsonl, validate_round_audit_chain  # noqa: E402
from eco_council_runtime.controller.paths import (  # noqa: E402
    audit_chain_ledger_path,
    claim_candidates_path,
    claim_curation_packet_path,
    claim_curation_prompt_path,
    decision_draft_path,
    decision_packet_path,
    decision_target_path,
    observation_candidates_path,
    observation_curation_packet_path,
    observation_curation_prompt_path,
    report_draft_path,
    report_target_path,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "assets" / "contract" / "examples"
ROUND_ID = "round-001"
NEXT_ROUND_ID = "round-002"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_example_json(name: str) -> dict[str, object]:
    return json.loads((EXAMPLES_DIR / name).read_text(encoding="utf-8"))


def example_mission(*, run_id: str) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "topic": "Chiang Mai smoke verification",
        "objective": "Determine whether public smoke claims are supported by physical evidence.",
        "policy_profile": "standard",
        "constraints": {
            "max_rounds": 3,
            "max_claims_per_round": 4,
            "max_tasks_per_round": 4,
        },
        "window": {
            "start_utc": "2026-03-18T00:00:00Z",
            "end_utc": "2026-03-19T23:59:59Z",
        },
        "region": {
            "label": "Chiang Mai, Thailand",
            "geometry": {
                "type": "Point",
                "latitude": 18.7883,
                "longitude": 98.9853,
            },
        },
        "hypotheses": [
            "Smoke discussion is driven by real fire activity upwind of Chiang Mai.",
        ],
        "source_governance": {
            "approved_layers": [
                {
                    "family_id": "gdelt",
                    "layer_id": "bulk",
                    "approved_by": "human",
                    "reason": "This run may use one anchored GDELT bulk layer after article recon.",
                }
            ]
        },
    }


def scaffold_temp_run(root: Path, *, run_id: str = "reporting-artifacts-run-001") -> Path:
    run_dir = root / run_id
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=example_mission(run_id=run_id),
        tasks=None,
        pretty=True,
    )
    return run_dir


class ReportingArtifactsTests(unittest.TestCase):
    def test_curation_artifacts_write_packets_and_prompts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="reporting-artifacts-curation")
            write_json(
                claim_candidates_path(run_dir, ROUND_ID),
                [
                    {
                        "claim_id": "claim-001",
                        "claim_type": "smoke",
                        "summary": "Residents reported a heavy smoke plume over Chiang Mai.",
                        "statement": "A smoke plume affected Chiang Mai during the mission window.",
                        "source_signal_count": 2,
                        "needs_physical_validation": True,
                        "public_refs": [{"source_skill": "gdelt-doc-search"}],
                        "time_window": {
                            "start_utc": "2026-03-18T04:00:00Z",
                            "end_utc": "2026-03-18T10:00:00Z",
                        },
                        "place_scope": {"label": "Chiang Mai"},
                    }
                ],
            )
            write_json(
                observation_candidates_path(run_dir, ROUND_ID),
                [
                    {
                        "observation_id": "obs-001",
                        "source_skill": "openaq-data-fetch",
                        "metric": "pm2_5",
                        "aggregation": "instant",
                        "value": 55.0,
                        "unit": "ug/m3",
                        "statistics": {"sample_count": 1, "mean": 55.0},
                        "distribution_summary": {
                            "signal_count": 1,
                            "metric_counts": [{"value": "pm2_5", "count": 1}],
                            "source_skill_counts": [{"value": "openaq-data-fetch", "count": 1}],
                        },
                        "time_window": {
                            "start_utc": "2026-03-18T05:00:00Z",
                            "end_utc": "2026-03-18T05:00:00Z",
                        },
                        "place_scope": {
                            "label": "Chiang Mai",
                            "geometry": {
                                "type": "Point",
                                "latitude": 18.7883,
                                "longitude": 98.9853,
                            },
                        },
                    }
                ],
            )

            result = curation_artifacts(run_dir=run_dir, round_id=ROUND_ID, pretty=True)
            prompts = render_openclaw_prompts(run_dir=run_dir, round_id=ROUND_ID)

            self.assertEqual(1, result["claim_candidate_count"])
            self.assertEqual(1, result["observation_candidate_count"])
            self.assertTrue(claim_curation_packet_path(run_dir, ROUND_ID).exists())
            self.assertTrue(observation_curation_packet_path(run_dir, ROUND_ID).exists())
            self.assertIn("sociologist_claim_curation", prompts)
            self.assertIn("environmentalist_observation_curation", prompts)
            self.assertTrue(claim_curation_prompt_path(run_dir, ROUND_ID).exists())
            self.assertTrue(observation_curation_prompt_path(run_dir, ROUND_ID).exists())
            self.assertIn("claim-curation", claim_curation_prompt_path(run_dir, ROUND_ID).read_text(encoding="utf-8"))
            self.assertIn(
                "observation-curation",
                observation_curation_prompt_path(run_dir, ROUND_ID).read_text(encoding="utf-8"),
            )

    def test_data_readiness_artifacts_require_materialized_curations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="reporting-artifacts-readiness")

            with self.assertRaisesRegex(ValueError, "materialize-curations"):
                data_readiness_artifacts(run_dir=run_dir, round_id=ROUND_ID, pretty=True)

    def test_decision_artifacts_prefer_draft_reports_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="reporting-artifacts-decision")
            base_report = load_example_json("expert_report.json")
            for role in ("sociologist", "environmentalist"):
                draft_payload = dict(base_report)
                draft_payload["run_id"] = "reporting-artifacts-decision"
                draft_payload["round_id"] = ROUND_ID
                draft_payload["agent_role"] = role
                draft_payload["report_id"] = f"report-{role}-draft-{ROUND_ID}"
                draft_payload["summary"] = f"Draft summary for {role}."

                final_payload = dict(draft_payload)
                final_payload["report_id"] = f"report-{role}-final-{ROUND_ID}"
                final_payload["summary"] = f"Final summary for {role}."

                write_json(report_draft_path(run_dir, ROUND_ID, role), draft_payload)
                write_json(report_target_path(run_dir, ROUND_ID, role), final_payload)

            result = decision_artifacts(
                run_dir=run_dir,
                round_id=ROUND_ID,
                next_round_id=NEXT_ROUND_ID,
                pretty=True,
                prefer_draft_reports=True,
            )

            self.assertEqual({"sociologist": "draft", "environmentalist": "draft"}, result["report_sources"])
            self.assertTrue(decision_packet_path(run_dir, ROUND_ID).exists())
            self.assertTrue(decision_draft_path(run_dir, ROUND_ID).exists())
            packet = read_json(decision_packet_path(run_dir, ROUND_ID))
            self.assertEqual("draft", packet["report_sources"]["sociologist"])
            self.assertEqual("draft", packet["report_sources"]["environmentalist"])

    def test_promote_decision_draft_records_decision_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="reporting-artifacts-promote")
            payload = load_example_json("council_decision.json")
            payload["run_id"] = "reporting-artifacts-promote"
            payload["round_id"] = ROUND_ID
            payload["decision_id"] = "decision-round-001"
            next_round_tasks = payload.get("next_round_tasks")
            if isinstance(next_round_tasks, list) and next_round_tasks:
                next_round_tasks[0]["run_id"] = "reporting-artifacts-promote"
                next_round_tasks[0]["round_id"] = NEXT_ROUND_ID
            write_json(decision_draft_path(run_dir, ROUND_ID), payload)

            result = promote_decision_draft(
                run_dir=run_dir,
                round_id=ROUND_ID,
                draft_path_text="",
                pretty=True,
                allow_overwrite=True,
            )

            self.assertEqual(str(decision_target_path(run_dir, ROUND_ID)), result["target_path"])
            validation = validate_round_audit_chain(run_dir, ROUND_ID, require_exists=True)
            self.assertTrue(validation["validation"]["ok"])
            ledger_entries = [item for item in read_jsonl(audit_chain_ledger_path(run_dir, ROUND_ID)) if isinstance(item, dict)]
            self.assertEqual("council-decision-promoted", ledger_entries[0]["event_kind"])


if __name__ == "__main__":
    unittest.main()

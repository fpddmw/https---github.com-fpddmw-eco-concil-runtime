from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.contract import scaffold_run_from_mission, validate_bundle  # noqa: E402
from eco_council_runtime.controller.audit_chain import (  # noqa: E402
    read_jsonl,
    record_decision_phase_receipt,
    record_fetch_phase_receipt,
    record_import_receipt,
    record_match_phase_receipt,
    record_normalize_phase_receipt,
    validate_round_audit_chain,
)
from eco_council_runtime.controller.io import file_sha256  # noqa: E402
from eco_council_runtime.controller.paths import (  # noqa: E402
    audit_chain_ledger_path,
    cards_active_path,
    claim_candidates_path,
    claim_submissions_path,
    claims_active_path,
    data_plane_execution_path,
    decision_draft_path,
    decision_target_path,
    evidence_adjudication_path,
    fetch_execution_path,
    fetch_plan_path,
    isolated_active_path,
    matching_adjudication_path,
    matching_result_path,
    observation_candidates_path,
    observation_submissions_path,
    observations_active_path,
    remands_open_path,
    report_target_path,
    reporting_handoff_path,
    round_dir,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
    tasks_path,
)
from eco_council_runtime.controller.stage_imports import import_fetch_execution_payload  # noqa: E402
from eco_council_runtime.reporting import promote_decision_draft  # noqa: E402

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "assets" / "contract" / "examples"
ROUND_ID = "round-001"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")


def example_mission(*, run_id: str) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "topic": "Chiang Mai smoke verification",
        "objective": "Determine whether public smoke claims are supported by physical evidence.",
        "policy_profile": "standard",
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
            "Public AQ concern aligns with modeled PM and meteorological background.",
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


def scaffold_temp_run(root: Path, *, run_id: str = "audit-run-001") -> Path:
    run_dir = root / run_id
    scaffold_run_from_mission(
        run_dir=run_dir,
        mission=example_mission(run_id=run_id),
        tasks=None,
        pretty=True,
    )
    return run_dir


def load_example_json(name: str) -> dict[str, object]:
    return json.loads((EXAMPLES_DIR / name).read_text(encoding="utf-8"))


class AuditChainTests(unittest.TestCase):
    def test_audit_chain_records_all_phase_receipts_and_dedupes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir))
            round_path = round_dir( run_dir, ROUND_ID)

            source_path = run_dir / "imports" / "task-review.json"
            write_json(source_path, [{"task_id": "task-001"}])
            duplicate_result = record_import_receipt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                imported_kind="round-task",
                source_path=source_path,
                target_path=tasks_path(run_dir, ROUND_ID),
                role="moderator",
                stage_after_import="awaiting-source-selection",
            )

            raw_artifact = round_path / "sociologist" / "raw" / "gdelt-doc-search.json"
            stdout_path = round_path / "moderator" / "derived" / "fetch-step.stdout.log"
            stderr_path = round_path / "moderator" / "derived" / "fetch-step.stderr.log"
            write_json(fetch_plan_path(run_dir, ROUND_ID), {"steps": [{"step_id": "step-sociologist-001", "role": "sociologist", "source_skill": "gdelt-doc-search", "artifact_path": str(raw_artifact), "stdout_path": str(stdout_path), "stderr_path": str(stderr_path)}]})
            write_json(raw_artifact, {"articles": [{"title": "Smoke article"}]})
            stdout_path.write_text("stdout\n", encoding="utf-8")
            stderr_path.write_text("stderr\n", encoding="utf-8")
            plan_sha256 = file_sha256(fetch_plan_path(run_dir, ROUND_ID))
            fetch_payload = {
                "run_dir": str(run_dir),
                "round_id": ROUND_ID,
                "plan_path": str(fetch_plan_path(run_dir, ROUND_ID)),
                "plan_sha256": plan_sha256,
                "step_count": 1,
                "completed_count": 1,
                "failed_count": 0,
                "statuses": [
                    {
                        "step_id": "step-sociologist-001",
                        "role": "sociologist",
                        "source_skill": "gdelt-doc-search",
                        "status": "completed",
                        "artifact_path": str(raw_artifact),
                        "stdout_path": str(stdout_path),
                        "stderr_path": str(stderr_path),
                    }
                ],
            }
            write_json(fetch_execution_path(run_dir, ROUND_ID), fetch_payload)
            record_fetch_phase_receipt(run_dir=run_dir, round_id=ROUND_ID, payload=fetch_payload)

            normalize_payload = {
                "run_dir": str(run_dir),
                "round_id": ROUND_ID,
                "step_count": 8,
                "completed_count": 8,
                "failed_count": 0,
                "statuses": [],
                "reporting_handoff_path": str(reporting_handoff_path(run_dir, ROUND_ID)),
            }
            write_json(data_plane_execution_path(run_dir, ROUND_ID), normalize_payload)
            write_json(claim_candidates_path(run_dir, ROUND_ID), [{"claim_id": "claim-001"}])
            write_json(observation_candidates_path(run_dir, ROUND_ID), [{"observation_id": "obs-001"}])
            write_json(claim_submissions_path(run_dir, ROUND_ID), [{"claim_id": "claim-001"}])
            write_json(observation_submissions_path(run_dir, ROUND_ID), [{"observation_id": "obs-001"}])
            write_json(shared_claims_path(run_dir, ROUND_ID), [{"claim_id": "claim-001"}])
            write_json(shared_observations_path(run_dir, ROUND_ID), [{"observation_id": "obs-001"}])
            write_json(claims_active_path(run_dir, ROUND_ID), [{"claim_id": "claim-001"}])
            write_json(observations_active_path(run_dir, ROUND_ID), [{"observation_id": "obs-001"}])
            write_json(reporting_handoff_path(run_dir, ROUND_ID), {"ok": True})
            record_normalize_phase_receipt(run_dir=run_dir, round_id=ROUND_ID, payload=normalize_payload)

            write_json(matching_adjudication_path(run_dir, ROUND_ID), {"adjudication_id": "adjudication-round-001"})
            write_json(shared_evidence_path(run_dir, ROUND_ID), [{"evidence_id": "evidence-001"}])
            write_json(matching_result_path(run_dir, ROUND_ID), {"result_id": "matchres-round-001"})
            write_json(evidence_adjudication_path(run_dir, ROUND_ID), {"adjudication_id": "adjudication-round-001"})
            write_json(cards_active_path(run_dir, ROUND_ID), [{"evidence_id": "evidence-001"}])
            write_json(isolated_active_path(run_dir, ROUND_ID), [])
            write_json(remands_open_path(run_dir, ROUND_ID), [])
            record_match_phase_receipt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                evidence_count=1,
                isolated_count=0,
                remand_count=0,
            )

            decision_payload = {
                "decision_id": "decision-round-001",
                "moderator_status": "continue",
                "evidence_sufficiency": "partial",
                "next_round_required": True,
            }
            write_json(decision_draft_path(run_dir, ROUND_ID), decision_payload)
            write_json(decision_target_path(run_dir, ROUND_ID), decision_payload)
            write_json(report_target_path(run_dir, ROUND_ID, "sociologist"), {"report_id": "soc-report"})
            write_json(report_target_path(run_dir, ROUND_ID, "environmentalist"), {"report_id": "env-report"})
            record_decision_phase_receipt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                decision_payload=decision_payload,
            )

            duplicate_result = record_import_receipt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                imported_kind="round-task",
                source_path=source_path,
                target_path=tasks_path(run_dir, ROUND_ID),
                role="moderator",
                stage_after_import="awaiting-source-selection",
            )
            self.assertFalse(duplicate_result["recorded"])

            validation = validate_round_audit_chain(run_dir, ROUND_ID, require_exists=True)
            self.assertTrue(validation["validation"]["ok"])
            self.assertEqual(5, validation["receipt_count"])
            self.assertGreaterEqual(validation["latest_artifact_checks"], 5)

            ledger_entries = [item for item in read_jsonl(audit_chain_ledger_path(run_dir, ROUND_ID)) if isinstance(item, dict)]
            self.assertEqual(
                ["import", "fetch", "normalize", "match", "decision"],
                [item["phase_kind"] for item in ledger_entries],
            )

            bundle_validation = validate_bundle(run_dir)
            audit_results = [item for item in bundle_validation["results"] if item.get("kind") == "audit-chain"]
            self.assertTrue(audit_results)
            self.assertTrue(all(item["validation"]["ok"] for item in audit_results))

    def test_validate_round_audit_chain_detects_tampered_latest_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="audit-run-tamper")
            decision_payload = {
                "decision_id": "decision-round-001",
                "moderator_status": "continue",
                "evidence_sufficiency": "partial",
                "next_round_required": True,
            }
            write_json(decision_draft_path(run_dir, ROUND_ID), decision_payload)
            write_json(decision_target_path(run_dir, ROUND_ID), decision_payload)
            record_decision_phase_receipt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                decision_payload=decision_payload,
            )

            write_json(decision_target_path(run_dir, ROUND_ID), {"decision_id": "tampered"})
            validation = validate_round_audit_chain(run_dir, ROUND_ID, require_exists=True)
            self.assertFalse(validation["validation"]["ok"])
            self.assertTrue(
                any("Latest canonical artifact digest" in issue.get("message", "") for issue in validation["validation"]["issues"])
            )

    def test_import_fetch_execution_payload_records_import_and_fetch_receipts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="audit-run-fetch")
            round_path = round_dir(run_dir, ROUND_ID)
            raw_artifact = round_path / "sociologist" / "raw" / "gdelt-doc-search.json"
            stdout_path = round_path / "moderator" / "derived" / "fetch-step.stdout.log"
            stderr_path = round_path / "moderator" / "derived" / "fetch-step.stderr.log"
            write_json(fetch_plan_path(run_dir, ROUND_ID), {"steps": [{"step_id": "step-sociologist-001", "role": "sociologist", "source_skill": "gdelt-doc-search", "artifact_path": str(raw_artifact), "stdout_path": str(stdout_path), "stderr_path": str(stderr_path)}]})
            write_json(raw_artifact, {"articles": [{"title": "Smoke article"}]})
            stdout_path.write_text("stdout\n", encoding="utf-8")
            stderr_path.write_text("stderr\n", encoding="utf-8")
            source_path = run_dir / "imports" / "fetch_execution_input.json"
            plan_sha256 = file_sha256(fetch_plan_path(run_dir, ROUND_ID))
            payload = {
                "run_dir": str(run_dir),
                "round_id": ROUND_ID,
                "plan_path": str(fetch_plan_path(run_dir, ROUND_ID)),
                "plan_sha256": plan_sha256,
                "step_count": 1,
                "completed_count": 1,
                "failed_count": 0,
                "statuses": [
                    {
                        "step_id": "step-sociologist-001",
                        "role": "sociologist",
                        "source_skill": "gdelt-doc-search",
                        "status": "completed",
                        "artifact_path": str(raw_artifact),
                        "stdout_path": str(stdout_path),
                        "stderr_path": str(stderr_path),
                    }
                ],
            }
            write_json(source_path, payload)

            result = import_fetch_execution_payload(
                run_dir=run_dir,
                state={"current_round_id": ROUND_ID, "stage": "ready-to-execute-fetch-plan"},
                payload=payload,
                source_path=source_path,
                save_state=lambda _run_dir, _state: None,
                status_builder=lambda _run_dir, state: {"stage": state.get("stage")},
            )

            self.assertEqual("fetch-execution", result["imported_kind"])
            validation = validate_round_audit_chain(run_dir, ROUND_ID, require_exists=True)
            self.assertTrue(validation["validation"]["ok"])
            self.assertEqual(2, validation["receipt_count"])
            ledger_entries = [item for item in read_jsonl(audit_chain_ledger_path(run_dir, ROUND_ID)) if isinstance(item, dict)]
            self.assertEqual(["import", "fetch"], [item["phase_kind"] for item in ledger_entries])

    def test_validate_round_audit_chain_reports_malformed_chain_index(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="audit-run-malformed")
            decision_payload = {
                "decision_id": "decision-round-001",
                "moderator_status": "continue",
                "evidence_sufficiency": "partial",
                "next_round_required": True,
            }
            write_json(decision_draft_path(run_dir, ROUND_ID), decision_payload)
            write_json(decision_target_path(run_dir, ROUND_ID), decision_payload)
            record_decision_phase_receipt(
                run_dir=run_dir,
                round_id=ROUND_ID,
                decision_payload=decision_payload,
            )

            ledger_path = audit_chain_ledger_path(run_dir, ROUND_ID)
            rows = read_jsonl(ledger_path)
            self.assertEqual(1, len(rows))
            rows[0]["chain_index"] = "oops"
            ledger_path.write_text(
                "".join(json.dumps(row, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )

            validation = validate_round_audit_chain(run_dir, ROUND_ID, require_exists=True)
            self.assertFalse(validation["validation"]["ok"])
            self.assertTrue(any("chain_index must be an integer" in issue.get("message", "") for issue in validation["validation"]["issues"]))

    def test_promote_decision_draft_records_decision_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = scaffold_temp_run(Path(temp_dir), run_id="audit-run-decision")
            payload = load_example_json("council_decision.json")
            payload["run_id"] = "audit-run-decision"
            payload["round_id"] = ROUND_ID
            payload["decision_id"] = "decision-round-001"
            next_round_tasks = payload.get("next_round_tasks")
            if isinstance(next_round_tasks, list) and next_round_tasks:
                next_round_tasks[0]["run_id"] = "audit-run-decision"
                next_round_tasks[0]["round_id"] = "round-002"
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
            self.assertEqual(1, validation["receipt_count"])
            ledger_entries = [item for item in read_jsonl(audit_chain_ledger_path(run_dir, ROUND_ID)) if isinstance(item, dict)]
            self.assertEqual("council-decision-promoted", ledger_entries[0]["event_kind"])


if __name__ == "__main__":
    unittest.main()

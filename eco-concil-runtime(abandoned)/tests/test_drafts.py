from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.drafts import (  # noqa: E402
    can_replace_existing_exact,
    can_replace_existing_report,
    load_draft_payload,
    promote_draft,
    report_prompt_text,
)


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def validate_payload(kind: str, payload: Any) -> None:
    _ = (kind, payload)


def write_json(path: Path, payload: Any, pretty: bool) -> None:
    if pretty:
        text = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    else:
        text = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    path.write_text(text, encoding="utf-8")


class DraftHelpersTests(unittest.TestCase):
    def test_load_draft_payload_validates_round_and_role(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            draft_path = Path(temp_dir) / "report_draft.json"
            draft_path.write_text(
                json.dumps(
                    {
                        "round_id": "round-002",
                        "agent_role": "sociologist",
                        "summary": "ok",
                    }
                ),
                encoding="utf-8",
            )

            resolved_path, payload = load_draft_payload(
                draft_path_text="",
                default_path=draft_path,
                label="sociologist report draft",
                round_error_label="Report draft",
                expected_round_id="round-002",
                expected_role="sociologist",
                role_error_label="Report draft",
                kind="expert-report",
                load_json_if_exists=load_json_if_exists,
                validate_payload=validate_payload,
            )

        self.assertEqual(draft_path, resolved_path)
        self.assertEqual("sociologist", payload["agent_role"])

    def test_load_draft_payload_rejects_role_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            draft_path = Path(temp_dir) / "report_draft.json"
            draft_path.write_text(
                json.dumps(
                    {
                        "round_id": "round-002",
                        "agent_role": "environmentalist",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "Report draft role mismatch"):
                load_draft_payload(
                    draft_path_text="",
                    default_path=draft_path,
                    label="sociologist report draft",
                    round_error_label="Report draft",
                    expected_round_id="round-002",
                    expected_role="sociologist",
                    role_error_label="Report draft",
                    kind="expert-report",
                    load_json_if_exists=load_json_if_exists,
                    validate_payload=validate_payload,
                )

    def test_promote_draft_respects_replacement_policy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            draft_path = Path(temp_dir) / "decision_draft.json"
            target_path = Path(temp_dir) / "decision.json"
            payload = {"decision_id": "decision-round-002"}
            target_path.write_text(json.dumps({"decision_id": "old"}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Refusing to overwrite canonical decision"):
                promote_draft(
                    draft_path=draft_path,
                    payload=payload,
                    target_path=target_path,
                    pretty=False,
                    allow_overwrite=False,
                    existing_label="canonical decision",
                    overwrite_error_message="Refusing to overwrite canonical decision without --allow-overwrite",
                    can_replace_existing=can_replace_existing_exact,
                    load_json_if_exists=load_json_if_exists,
                    write_json=write_json,
                )

            result = promote_draft(
                draft_path=draft_path,
                payload=payload,
                target_path=target_path,
                pretty=False,
                allow_overwrite=True,
                existing_label="canonical decision",
                overwrite_error_message="Refusing to overwrite canonical decision without --allow-overwrite",
                can_replace_existing=can_replace_existing_exact,
                load_json_if_exists=load_json_if_exists,
                write_json=write_json,
            )

            self.assertTrue(result["overwrote_existing"])
            self.assertEqual(payload, json.loads(target_path.read_text(encoding="utf-8")))

    def test_can_replace_existing_report_accepts_placeholder(self) -> None:
        existing_payload = {"status": "pending"}
        new_payload = {"status": "complete"}

        allowed = can_replace_existing_report(
            existing_payload,
            new_payload,
            report_is_placeholder=lambda payload: payload == {"status": "pending"},
        )

        self.assertTrue(allowed)

    def test_report_prompt_text_embeds_paths_and_validation(self) -> None:
        prompt = report_prompt_text(
            role="sociologist",
            packet_path=Path("/tmp/report_packet.json"),
            packet={
                "run": {"run_id": "run-001", "round_id": "round-002"},
                "validation": {
                    "draft_report_path": "/tmp/report_draft.json",
                    "validate_command": "eco-council-reporting validate-report",
                },
            },
        )

        self.assertIn("You are the sociologist for eco-council run run-001 round round-002.", prompt)
        self.assertIn("/tmp/report_packet.json", prompt)
        self.assertIn("/tmp/report_draft.json", prompt)
        self.assertIn("eco-council-reporting validate-report", prompt)
        self.assertIn("context.causal_focus", prompt)
        self.assertIn("investigation_review", prompt)


if __name__ == "__main__":
    unittest.main()

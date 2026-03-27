from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.adapters.filesystem import (  # noqa: E402
    file_sha256,
    load_canonical_list,
    read_json,
    read_jsonl,
    stable_hash,
    stable_json,
    write_json,
    write_jsonl,
)
from eco_council_runtime.adapters.run_paths import (  # noqa: E402
    discover_round_ids,
    load_mission,
    prior_round_ids,
    round_dir,
)
from eco_council_runtime.controller.io import stable_hash as controller_stable_hash  # noqa: E402
from eco_council_runtime.domain.contract_bridge import resolve_schema_version  # noqa: E402
from eco_council_runtime.domain.rounds import (  # noqa: E402
    current_round_number,
    next_round_id,
    next_round_id_for,
    normalize_round_id,
    parse_round_components,
    round_dir_name,
    round_sort_key,
    strict_round_sort_key,
)
from eco_council_runtime.domain.text import (  # noqa: E402
    maybe_text,
    text_truthy,
    truncate_text,
    unique_strings,
)


class SharedFoundationsTests(unittest.TestCase):
    def test_text_and_round_helpers_cover_shared_runtime_cases(self) -> None:
        self.assertEqual("hello world", maybe_text(" hello   world "))
        self.assertTrue(text_truthy(" YES "))
        self.assertEqual("ab...", truncate_text("abcdef", 5))
        self.assertEqual(["A", "a", "B"], unique_strings(["A", "a", "B"]))
        self.assertEqual(["A", "B"], unique_strings(["A", "a", "B"], casefold=True))

        self.assertEqual("round-007", normalize_round_id("round_007"))
        self.assertEqual("round_007", round_dir_name("round-007"))
        self.assertEqual(("round-", 12, 3), parse_round_components("round-012"))
        self.assertEqual(12, current_round_number("round-012"))
        self.assertEqual("round-013", next_round_id("round-012"))
        self.assertEqual("round-013", next_round_id_for("round-012"))
        self.assertEqual(("round-", 12, "round-012"), round_sort_key("round-012"))
        self.assertEqual((12, "round-012"), strict_round_sort_key("round-012"))

    def test_filesystem_adapter_round_trips_json_and_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            payload_path = root / "payload.json"
            ledger_path = root / "ledger.jsonl"

            write_json(payload_path, {"alpha": 1, "beta": ["x"]}, pretty=True)
            write_jsonl(ledger_path, [{"id": "a"}, {"id": "b"}])

            self.assertEqual({"alpha": 1, "beta": ["x"]}, read_json(payload_path))
            self.assertEqual([{"id": "a"}, {"id": "b"}], read_jsonl(ledger_path))
            self.assertEqual([], load_canonical_list(ledger_path.with_name("missing.json")))
            self.assertEqual(stable_hash("x", {"y": 1}), controller_stable_hash("x", {"y": 1}))
            self.assertEqual(json.dumps({"alpha": 1}, ensure_ascii=True, sort_keys=True), stable_json({"alpha": 1}))
            self.assertEqual(64, len(file_sha256(payload_path)))

    def test_run_path_helpers_discover_rounds_and_load_mission(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            mission = {"run_id": "run-123", "topic": "Smoke", "region": {"label": "X", "geometry": {"type": "Point", "latitude": 1.0, "longitude": 2.0}}}
            (run_dir / "mission.json").write_text(json.dumps(mission, ensure_ascii=True), encoding="utf-8")
            round_dir(run_dir, "round-002").mkdir(parents=True, exist_ok=True)
            round_dir(run_dir, "round-001").mkdir(parents=True, exist_ok=True)

            self.assertEqual(["round-001", "round-002"], discover_round_ids(run_dir))
            self.assertEqual(["round-001"], prior_round_ids(run_dir, "round-002"))
            self.assertEqual("run-123", load_mission(run_dir)["run_id"])

    def test_contract_bridge_exposes_runtime_schema_version(self) -> None:
        self.assertEqual("1.0.0", resolve_schema_version("0.0.0"))


if __name__ == "__main__":
    unittest.main()

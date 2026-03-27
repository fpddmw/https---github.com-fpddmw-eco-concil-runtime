from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.adapters.normalize_storage import (  # noqa: E402
    load_or_build_manifest,
    resolve_analytics_db_paths,
    save_environment_db,
    save_public_db,
)
from eco_council_runtime.application.normalize_matching import (  # noqa: E402
    build_matching_adjudication_draft,
    build_matching_candidate_set,
)


def write_json_artifact(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")


class NormalizeStorageAndMatchingTests(unittest.TestCase):
    def test_storage_adapter_builds_and_resolves_manifest_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "run"
            run_dir.mkdir(parents=True, exist_ok=True)

            manifest = load_or_build_manifest(run_dir, run_id="run-001")
            self.assertEqual("run-001", manifest["run_id"])
            self.assertEqual(
                str(run_dir / "analytics" / "public_signals.sqlite"),
                manifest["databases"]["public_signals"],
            )
            self.assertEqual(
                str(run_dir / "analytics" / "environment_signals.sqlite"),
                manifest["databases"]["environment_signals"],
            )

            custom_environment = (run_dir / "external" / "environment.sqlite").resolve()
            write_json_artifact(
                run_dir / "run_manifest.json",
                {
                    "run_id": "run-001",
                    "databases": {
                        "public_signals": "analytics/custom_public.sqlite",
                        "environment_signals": str(custom_environment),
                    },
                },
            )

            public_db_path, environment_db_path = resolve_analytics_db_paths(run_dir)
            self.assertEqual((run_dir / "analytics" / "custom_public.sqlite").resolve(), public_db_path)
            self.assertEqual(custom_environment, environment_db_path)

    def test_storage_adapter_writes_public_and_environment_sqlite_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            public_db = root / "public.sqlite"
            environment_db = root / "environment.sqlite"

            save_public_db(
                public_db,
                [
                    {
                        "signal_id": "pubsig-001",
                        "run_id": "run-001",
                        "round_id": "round-01",
                        "source_skill": "youtube-video-search",
                        "signal_kind": "video",
                        "external_id": "vid-001",
                        "title": "Smoke over NYC",
                        "text": "Wildfire smoke covered the skyline.",
                        "url": "https://example.test/video",
                        "author_name": "City Desk",
                        "channel_name": "City Desk",
                        "language": "en",
                        "query_text": "nyc smoke",
                        "published_at_utc": "2023-06-07T12:00:00Z",
                        "captured_at_utc": "2026-03-27T00:00:00Z",
                        "engagement": {"view_count": 12},
                        "metadata": {"kind": "video"},
                        "artifact_path": "/tmp/public.json",
                        "record_locator": "$[0]",
                        "sha256": "a" * 64,
                        "raw_json": {"id": "vid-001"},
                    }
                ],
                [
                    {
                        "claim_id": "claim-001",
                        "run_id": "run-001",
                        "round_id": "round-01",
                        "claim_type": "smoke",
                        "priority": 1,
                        "summary": "Smoke impacted New York City",
                        "statement": "Canadian wildfire smoke impacted New York City.",
                        "public_refs": [{"external_id": "vid-001"}],
                    }
                ],
            )

            save_environment_db(
                environment_db,
                [
                    {
                        "signal_id": "envsig-001",
                        "run_id": "run-001",
                        "round_id": "round-01",
                        "source_skill": "openaq-data-fetch",
                        "signal_kind": "station-measurement",
                        "metric": "pm2_5",
                        "value": 41.5,
                        "unit": "ug/m3",
                        "observed_at_utc": "2023-06-07T12:00:00Z",
                        "window_start_utc": None,
                        "window_end_utc": None,
                        "latitude": 40.7,
                        "longitude": -74.0,
                        "bbox": None,
                        "quality_flags": ["station-observation"],
                        "metadata": {"provider": "OpenAQ"},
                        "artifact_path": "/tmp/environment.json",
                        "record_locator": "$[0]",
                        "sha256": "b" * 64,
                        "raw_json": {"value": 41.5},
                    }
                ],
                [
                    {
                        "observation_id": "obs-001",
                        "run_id": "run-001",
                        "round_id": "round-01",
                        "metric": "pm2_5",
                        "source_skill": "openaq-data-fetch",
                        "value": 41.5,
                    }
                ],
            )

            with sqlite3.connect(public_db) as conn:
                public_count = conn.execute("SELECT COUNT(*) FROM public_signals").fetchone()[0]
                claim_count = conn.execute("SELECT COUNT(*) FROM claim_candidates").fetchone()[0]
            with sqlite3.connect(environment_db) as conn:
                signal_count = conn.execute("SELECT COUNT(*) FROM environment_signals").fetchone()[0]
                observation_count = conn.execute("SELECT COUNT(*) FROM observation_summaries").fetchone()[0]

            self.assertEqual(1, public_count)
            self.assertEqual(1, claim_count)
            self.assertEqual(1, signal_count)
            self.assertEqual(1, observation_count)

    def test_matching_application_builders_are_directly_importable(self) -> None:
        compact_claim = lambda claim: {  # noqa: E731
            "claim_id": claim.get("claim_id"),
            "summary": claim.get("summary"),
        }
        compact_observation = lambda observation: {  # noqa: E731
            "observation_id": observation.get("observation_id"),
            "metric": observation.get("metric"),
        }
        authorization = {
            "run_id": "run-001",
            "round_id": "round-01",
            "authorization_id": "auth-001",
        }
        matches = [
            {
                "claim": {"claim_id": "claim-001", "summary": "Smoke impacted NYC"},
                "verdict": "supports",
                "confidence": "high",
                "support_score": 2,
                "contradict_score": 0,
                "observations": [{"observation_id": "obs-001", "metric": "pm2_5"}],
                "matching_scope": {"place": "nyc"},
                "gaps": ["Need more transport context."],
                "notes": ["pm2_5 spike aligns with smoke."],
                "observation_assessments": [
                    {
                        "observation": {
                            "observation_id": "obs-001",
                            "metric": "pm2_5",
                            "source_skill": "openaq-data-fetch",
                        },
                        "assessment": {
                            "support_score": 2,
                            "contradict_score": 0,
                            "primary_support_hits": 1,
                            "contradict_hits": 0,
                            "contextual_hits": 0,
                            "notes": ["pm2_5=41.5"],
                        },
                    }
                ],
            }
        ]
        observations = [
            {"observation_id": "obs-001", "metric": "pm2_5"},
            {"observation_id": "obs-002", "metric": "wind_speed"},
        ]

        candidate_set = build_matching_candidate_set(
            authorization=authorization,
            matches=matches,
            observations=observations,
            schema_version="1.0.0",
            compact_claim=compact_claim,
            compact_observation=compact_observation,
        )

        self.assertEqual("1.0.0", candidate_set["schema_version"])
        self.assertEqual("matchcand-round-01", candidate_set["candidate_set_id"])
        self.assertEqual(1, len(candidate_set["claim_candidates"]))
        self.assertEqual(["obs-001"], candidate_set["claim_candidates"][0]["matched_observation_ids"])
        self.assertEqual(
            [{"observation_id": "obs-002", "metric": "wind_speed"}],
            candidate_set["unpaired_observation_candidates"],
        )

        validation_calls: list[tuple[str, str]] = []
        draft = build_matching_adjudication_draft(
            authorization=authorization,
            candidate_set=candidate_set,
            matching_result={"result_id": "matchres-001"},
            evidence_cards=[],
            isolated_entries=[],
            remands=[],
            evidence_adjudication={
                "adjudication_id": "adjudication-001",
                "open_questions": ["Does transport evidence remain sufficient?"],
            },
            schema_version="1.0.0",
            validate_payload=lambda kind, payload: validation_calls.append(  # noqa: E731
                (kind, str(payload.get("candidate_set_id")))
            ),
        )

        self.assertEqual(
            [("matching-adjudication", "matchcand-round-01")],
            validation_calls,
        )
        self.assertEqual("adjudication-001", draft["adjudication_id"])
        self.assertEqual("matchcand-round-01", draft["candidate_set_id"])
        self.assertEqual(
            ["Does transport evidence remain sufficient?"],
            draft["open_questions"],
        )


if __name__ == "__main__":
    unittest.main()

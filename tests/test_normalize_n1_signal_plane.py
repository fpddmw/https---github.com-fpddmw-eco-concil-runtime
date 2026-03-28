from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
ROUND_ID = "round-001"
RUN_ID = "run-001"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def script_path(skill_name: str) -> Path:
    return WORKSPACE_ROOT / "skills" / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"


def signal_plane_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "signal_plane.sqlite"


def run_script(script: Path, *args: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(script), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"Script failed: {script}\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"Script did not emit valid JSON:\n{completed.stdout}") from exc


class NormalizeN1SignalPlaneTests(unittest.TestCase):
    def test_public_skill_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            youtube_path = root / "youtube_videos.json"
            write_json(
                youtube_path,
                [
                    {
                        "query": "nyc smoke wildfire",
                        "video_id": "vid-001",
                        "video": {
                            "id": "vid-001",
                            "title": "Smoke over New York City",
                            "description": "Canadian wildfire smoke over NYC skyline.",
                            "channel_title": "City Desk",
                            "published_at": "2023-06-07T13:00:00Z",
                            "default_language": "en",
                            "statistics": {"view_count": 1250},
                        },
                    }
                ],
            )

            normalize_payload = run_script(
                script_path("eco-normalize-youtube-video-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(youtube_path),
            )
            assert isinstance(normalize_payload.get("canonical_ids"), list)
            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, len(normalize_payload["canonical_ids"]))
            self.assertTrue(signal_plane_db_path(run_dir).exists())

            query_payload = run_script(
                script_path("eco-query-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--keyword",
                "smoke",
            )
            assert isinstance(query_payload.get("results"), list)
            self.assertEqual(1, query_payload["result_count"])

            signal_id = query_payload["results"][0]["signal_id"]
            lookup_payload = run_script(
                script_path("eco-lookup-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            assert isinstance(lookup_payload.get("results"), list)
            self.assertEqual(1, lookup_payload["result_count"])
            self.assertEqual("Smoke over New York City", lookup_payload["results"][0]["title"])

            raw_payload = run_script(
                script_path("eco-lookup-raw-record"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            assert isinstance(raw_payload.get("results"), list)
            self.assertEqual(1, raw_payload["result_count"])
            self.assertEqual("vid-001", raw_payload["results"][0]["raw_record"]["video_id"])

    def test_environment_skill_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            openaq_path = root / "openaq.json"
            write_json(
                openaq_path,
                {
                    "results": [
                        {
                            "parameter": {"name": "pm25", "units": "ug/m3"},
                            "value": 41.5,
                            "date": {"utc": "2023-06-07T12:00:00Z"},
                            "coordinates": {"latitude": 40.7, "longitude": -74.0},
                            "location": {"id": 1, "name": "NYC"},
                            "provider": {"name": "OpenAQ"},
                        }
                    ]
                },
            )

            normalize_payload = run_script(
                script_path("eco-normalize-openaq-observation-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(openaq_path),
            )
            assert isinstance(normalize_payload.get("canonical_ids"), list)
            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, len(normalize_payload["canonical_ids"]))

            query_payload = run_script(
                script_path("eco-query-environment-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
                "--bbox",
                "-75.0",
                "40.0",
                "-73.0",
                "41.0",
            )
            assert isinstance(query_payload.get("results"), list)
            self.assertEqual(1, query_payload["result_count"])

            signal_id = query_payload["results"][0]["signal_id"]
            lookup_payload = run_script(
                script_path("eco-lookup-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            assert isinstance(lookup_payload.get("results"), list)
            self.assertEqual("pm2_5", lookup_payload["results"][0]["metric"])

            raw_payload = run_script(
                script_path("eco-lookup-raw-record"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            assert isinstance(raw_payload.get("results"), list)
            self.assertEqual(41.5, raw_payload["results"][0]["raw_record"]["value"])

    def test_all_n1_skill_scripts_smoke_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"

            gdelt_path = root / "gdelt_doc.json"
            write_json(
                gdelt_path,
                {
                    "articles": [
                        {
                            "title": "Smoke haze chokes New York City",
                            "url": "https://example.test/article",
                            "domain": "example.test",
                            "language": "en",
                            "seendate": "2023-06-07T13:00:00Z",
                        }
                    ]
                },
            )

            bluesky_path = root / "bluesky.json"
            write_json(
                bluesky_path,
                {
                    "seed_posts": [
                        {
                            "uri": "at://did:plc:smoke/app.bsky.feed.post/abc123",
                            "author_handle": "smoke.reporter.test",
                            "author_did": "did:plc:smoke",
                            "text": "Smoke visible over the city skyline.",
                            "timestamp_utc": "2023-06-07T12:30:00Z",
                            "reply_count": 1,
                            "repost_count": 2,
                            "like_count": 3,
                            "quote_count": 0,
                        }
                    ]
                },
            )

            youtube_path = root / "youtube.json"
            write_json(
                youtube_path,
                [
                    {
                        "query": "nyc smoke wildfire",
                        "video_id": "vid-001",
                        "video": {
                            "id": "vid-001",
                            "title": "Smoke over New York City",
                            "description": "Canadian wildfire smoke over NYC skyline.",
                            "channel_title": "City Desk",
                            "published_at": "2023-06-07T13:00:00Z",
                            "default_language": "en",
                            "statistics": {"view_count": 1250},
                        },
                    }
                ],
            )

            airnow_path = root / "airnow.json"
            write_json(
                airnow_path,
                {
                    "records": [
                        {
                            "parameter_name": "PM25",
                            "raw_concentration": 54.1,
                            "aqi_value": 155,
                            "latitude": 40.7,
                            "longitude": -74.0,
                            "observed_at_utc": "2023-06-07T12:00:00Z",
                            "site_name": "Test Site",
                            "country_code": "US",
                        }
                    ]
                },
            )

            openaq_path = root / "openaq.json"
            write_json(
                openaq_path,
                {
                    "results": [
                        {
                            "parameter": {"name": "pm25", "units": "ug/m3"},
                            "value": 41.5,
                            "date": {"utc": "2023-06-07T12:00:00Z"},
                            "coordinates": {"latitude": 40.7, "longitude": -74.0},
                            "location": {"id": 1, "name": "NYC"},
                            "provider": {"name": "OpenAQ"},
                        }
                    ]
                },
            )

            openmeteo_path = root / "openmeteo.json"
            write_json(
                openmeteo_path,
                {
                    "records": [
                        {
                            "latitude": 40.7128,
                            "longitude": -74.006,
                            "timezone": "America/New_York",
                            "hourly_units": {"temperature_2m": "C", "pm2_5": "ug/m3"},
                            "hourly": {
                                "time": ["2023-06-07T00:00:00Z"],
                                "temperature_2m": [23.5],
                                "pm2_5": [52.0],
                            },
                        }
                    ]
                },
            )

            write_payloads = [
                (
                    script_path("eco-normalize-gdelt-doc-public-signals"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--artifact-path",
                    str(gdelt_path),
                ),
                (
                    script_path("eco-normalize-bluesky-cascade-public-signals"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--artifact-path",
                    str(bluesky_path),
                ),
                (
                    script_path("eco-normalize-youtube-video-public-signals"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--artifact-path",
                    str(youtube_path),
                ),
                (
                    script_path("eco-normalize-airnow-observation-signals"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--artifact-path",
                    str(airnow_path),
                ),
                (
                    script_path("eco-normalize-openaq-observation-signals"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--artifact-path",
                    str(openaq_path),
                ),
                (
                    script_path("eco-normalize-open-meteo-historical-signals"),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--artifact-path",
                    str(openmeteo_path),
                ),
            ]

            for argv in write_payloads:
                payload = run_script(*argv)
                assert isinstance(payload.get("canonical_ids"), list)
                self.assertEqual("completed", payload["status"])
                self.assertGreaterEqual(len(payload["canonical_ids"]), 1)

            public_payload = run_script(
                script_path("eco-query-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--keyword",
                "smoke",
            )
            assert isinstance(public_payload.get("results"), list)
            self.assertGreaterEqual(public_payload["result_count"], 2)

            environment_payload = run_script(
                script_path("eco-query-environment-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
                "--bbox",
                "-75.0",
                "40.0",
                "-73.0",
                "41.0",
            )
            assert isinstance(environment_payload.get("results"), list)
            self.assertGreaterEqual(environment_payload["result_count"], 2)

            signal_id = public_payload["results"][0]["signal_id"]
            lookup_payload = run_script(
                script_path("eco-lookup-normalized-signal"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            assert isinstance(lookup_payload.get("results"), list)
            self.assertEqual(1, lookup_payload["result_count"])

            raw_payload = run_script(
                script_path("eco-lookup-raw-record"),
                "--run-dir",
                str(run_dir),
                "--signal-id",
                signal_id,
            )
            assert isinstance(raw_payload.get("results"), list)
            self.assertEqual(1, raw_payload["result_count"])


if __name__ == "__main__":
    unittest.main()

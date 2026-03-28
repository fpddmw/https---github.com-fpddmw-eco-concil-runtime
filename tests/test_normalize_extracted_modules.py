from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.normalize import (  # noqa: E402
    normalize_cache_dir,
    normalize_environment_source,
    normalize_environment_source_cached,
    normalize_public_source,
)


def write_json_artifact(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


class NormalizeExtractedModuleTests(unittest.TestCase):
    def test_public_source_pipeline_normalizes_youtube_video_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_path = Path(tmpdir) / "youtube_videos.json"
            write_json_artifact(
                artifact_path,
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

            signals = normalize_public_source(
                "youtube-video-search",
                artifact_path,
                run_id="run-001",
                round_id="round-01",
            )

            self.assertEqual(1, len(signals))
            signal = signals[0]
            self.assertEqual("youtube-video-search", signal["source_skill"])
            self.assertEqual("video", signal["signal_kind"])
            self.assertEqual("vid-001", signal["external_id"])
            self.assertEqual("Smoke over New York City", signal["title"])
            self.assertEqual("Canadian wildfire smoke over NYC skyline.", signal["text"])
            self.assertEqual("https://www.youtube.com/watch?v=vid-001", signal["url"])
            self.assertEqual("2023-06-07T13:00:00Z", signal["published_at_utc"])
            self.assertEqual(str(artifact_path), signal["artifact_path"])

    def test_environment_source_pipeline_normalizes_open_meteo_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_path = Path(tmpdir) / "open_meteo_air_quality.json"
            write_json_artifact(
                artifact_path,
                {
                    "records": [
                        {
                            "latitude": 40.7128,
                            "longitude": -74.0060,
                            "timezone": "America/New_York",
                            "hourly_units": {
                                "pm2_5": "ug/m3",
                                "pm10": "ug/m3",
                            },
                            "hourly": {
                                "time": ["2023-06-07T00:00:00Z"],
                                "pm2_5": [54.3],
                                "pm10": [71.2],
                            },
                        }
                    ]
                },
            )

            signals, extra_observations = normalize_environment_source(
                "open-meteo-air-quality-fetch",
                artifact_path,
                run_id="run-001",
                round_id="round-01",
                schema_version="1.0.0",
            )

            self.assertEqual([], extra_observations)
            self.assertEqual(2, len(signals))
            self.assertEqual({"pm2_5", "pm10"}, {signal["metric"] for signal in signals})
            self.assertEqual({"modeled-background"}, {flag for signal in signals for flag in signal["quality_flags"]})
            pm25_signal = next(signal for signal in signals if signal["metric"] == "pm2_5")
            self.assertEqual("hourly", pm25_signal["signal_kind"])
            self.assertEqual(54.3, pm25_signal["value"])
            self.assertEqual("2023-06-07T00:00:00Z", pm25_signal["observed_at_utc"])
            self.assertEqual(str(artifact_path), pm25_signal["artifact_path"])

    def test_environment_cached_wrapper_reports_miss_then_hit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir)
            run_dir = base_dir / "run"
            artifact_path = base_dir / "nasa_firms_empty.json"
            write_json_artifact(
                artifact_path,
                {
                    "request": {
                        "start_date": "2023-06-06T00:00:00Z",
                        "end_date": "2023-06-08T00:00:00Z",
                    },
                    "records": [],
                },
            )

            first_signals, first_extra_observations, first_status = normalize_environment_source_cached(
                run_dir=run_dir,
                source_skill="nasa-firms-fire-fetch",
                path=artifact_path,
                run_id="run-001",
                round_id="round-01",
                schema_version="1.0.0",
            )
            second_signals, second_extra_observations, second_status = normalize_environment_source_cached(
                run_dir=run_dir,
                source_skill="nasa-firms-fire-fetch",
                path=artifact_path,
                run_id="run-001",
                round_id="round-01",
                schema_version="1.0.0",
            )

            self.assertEqual("miss", first_status)
            self.assertEqual("hit", second_status)
            self.assertEqual([], first_signals)
            self.assertEqual([], second_signals)
            self.assertEqual(first_extra_observations, second_extra_observations)
            self.assertEqual(1, len(first_extra_observations))
            placeholder = first_extra_observations[0]
            self.assertEqual("1.0.0", placeholder["schema_version"])
            self.assertEqual("nasa-firms-fire-fetch", placeholder["source_skill"])
            self.assertEqual("fire_detection_count", placeholder["metric"])
            self.assertIn("zero-detections", placeholder["quality_flags"])

            cache_files = list(normalize_cache_dir(run_dir).rglob("*.json"))
            self.assertEqual(1, len(cache_files))


if __name__ == "__main__":
    unittest.main()

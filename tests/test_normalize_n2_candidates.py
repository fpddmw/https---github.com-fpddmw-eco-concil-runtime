from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
ROUND_ID = "round-002"
RUN_ID = "run-002"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def script_path(skill_name: str) -> Path:
    return WORKSPACE_ROOT / "skills" / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"


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
    return json.loads(completed.stdout)


def claim_candidates_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / f"claim_candidates_{ROUND_ID}.json"


def observation_candidates_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / f"observation_candidates_{ROUND_ID}.json"


def normalization_audit_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / f"normalization_audit_{ROUND_ID}.json"


def seed_signal_plane(run_dir: Path, root: Path) -> None:
    youtube_path = root / "youtube.json"
    bluesky_path = root / "bluesky.json"
    openaq_path = root / "openaq.json"
    write_json(
        youtube_path,
        [
            {
                "query": "nyc smoke wildfire",
                "video_id": "vid-201",
                "video": {
                    "id": "vid-201",
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
                    "uri": "at://did:plc:smoke/app.bsky.feed.post/abc999",
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
                    "coordinates": {"latitude": 40.7, "longitude": -74.0},
                    "location": {"id": 1, "name": "NYC"},
                    "provider": {"name": "OpenAQ"},
                },
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 44.0,
                    "date": {"utc": "2023-06-07T13:00:00Z"},
                    "coordinates": {"latitude": 40.7004, "longitude": -74.0004},
                    "location": {"id": 1, "name": "NYC"},
                    "provider": {"name": "OpenAQ"},
                },
            ]
        },
    )

    run_script(
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
    run_script(
        script_path("eco-normalize-bluesky-cascade-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        RUN_ID,
        "--round-id",
        ROUND_ID,
        "--artifact-path",
        str(bluesky_path),
    )
    run_script(
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


class NormalizeN2CandidateTests(unittest.TestCase):
    def test_roundtrip_extracts_candidates_and_builds_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_signal_plane(run_dir, root)

            claim_payload = run_script(
                script_path("eco-extract-claim-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            assert isinstance(claim_payload.get("canonical_ids"), list)
            self.assertEqual("completed", claim_payload["status"])
            self.assertGreaterEqual(len(claim_payload["canonical_ids"]), 1)
            claim_artifact = load_json(claim_candidates_path(run_dir))
            self.assertGreaterEqual(claim_artifact["candidate_count"], 1)

            observation_payload = run_script(
                script_path("eco-extract-observation-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
            )
            assert isinstance(observation_payload.get("canonical_ids"), list)
            self.assertEqual("completed", observation_payload["status"])
            self.assertGreaterEqual(len(observation_payload["canonical_ids"]), 1)
            observation_artifact = load_json(observation_candidates_path(run_dir))
            self.assertGreaterEqual(observation_artifact["candidate_count"], 1)

            audit_payload = run_script(
                script_path("eco-build-normalization-audit"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            self.assertEqual("completed", audit_payload["status"])
            audit_artifact = load_json(normalization_audit_path(run_dir))
            self.assertGreaterEqual(audit_artifact["report"]["claim_candidate_count"], 1)
            self.assertEqual(1, audit_artifact["report"]["observation_candidate_count"])

    def test_custom_output_paths_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_signal_plane(run_dir, root)

            custom_claims = root / "custom_claims.json"
            custom_observations = root / "custom_observations.json"
            custom_audit = root / "custom_audit.json"

            claim_payload = run_script(
                script_path("eco-extract-claim-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--keyword",
                "smoke",
                "--output-path",
                str(custom_claims),
            )
            assert isinstance(claim_payload.get("summary"), dict)
            self.assertEqual(str(custom_claims), claim_payload["summary"]["output_path"])

            observation_payload = run_script(
                script_path("eco-extract-observation-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
                "--output-path",
                str(custom_observations),
            )
            assert isinstance(observation_payload.get("summary"), dict)
            self.assertEqual(str(custom_observations), observation_payload["summary"]["output_path"])

            audit_payload = run_script(
                script_path("eco-build-normalization-audit"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--claim-candidates-path",
                str(custom_claims),
                "--observation-candidates-path",
                str(custom_observations),
                "--output-path",
                str(custom_audit),
            )
            assert isinstance(audit_payload.get("summary"), dict)
            self.assertEqual(str(custom_audit), audit_payload["summary"]["output_path"])
            self.assertTrue(custom_claims.exists())
            self.assertTrue(custom_observations.exists())
            self.assertTrue(custom_audit.exists())


if __name__ == "__main__":
    unittest.main()

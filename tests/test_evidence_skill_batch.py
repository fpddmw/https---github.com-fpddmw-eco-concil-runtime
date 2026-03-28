from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
ROUND_ID = "round-003"
RUN_ID = "run-003"


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
    payload = json.loads(completed.stdout)
    assert isinstance(payload, dict)
    return payload


def claim_clusters_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / f"claim_candidate_clusters_{ROUND_ID}.json"


def merged_observations_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / f"merged_observation_candidates_{ROUND_ID}.json"


def claim_observation_links_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / f"claim_observation_links_{ROUND_ID}.json"


def seed_signal_plane(run_dir: Path, root: Path, include_airnow: bool) -> None:
    youtube_path = root / "youtube.json"
    bluesky_path = root / "bluesky.json"
    openaq_path = root / "openaq.json"
    write_json(
        youtube_path,
        [
            {
                "query": "nyc smoke wildfire",
                "video_id": "vid-301",
                "video": {
                    "id": "vid-301",
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
                    "uri": "at://did:plc:smoke/app.bsky.feed.post/evidence301",
                    "author_handle": "smoke.reporter.test",
                    "author_did": "did:plc:smoke",
                    "text": "Smoke over New York City reduced visibility and left the skyline hazy.",
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
                },
                {
                    "parameter": {"name": "pm25", "units": "ug/m3"},
                    "value": 44.0,
                    "date": {"utc": "2023-06-07T13:00:00Z"},
                    "coordinates": {"latitude": 40.7001, "longitude": -74.0001},
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

    if include_airnow:
        airnow_path = root / "airnow.json"
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
        run_script(
            script_path("eco-normalize-airnow-observation-signals"),
            "--run-dir",
            str(run_dir),
            "--run-id",
            RUN_ID,
            "--round-id",
            ROUND_ID,
            "--artifact-path",
            str(airnow_path),
        )


class EvidenceSkillBatchTests(unittest.TestCase):
    def test_roundtrip_clusters_merges_and_links(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_signal_plane(run_dir, root, include_airnow=True)

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
            self.assertGreaterEqual(len(claim_payload["canonical_ids"]), 1)

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
            self.assertGreaterEqual(len(observation_payload["canonical_ids"]), 2)

            cluster_payload = run_script(
                script_path("eco-cluster-claim-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            assert isinstance(cluster_payload.get("canonical_ids"), list)
            self.assertGreaterEqual(len(cluster_payload["canonical_ids"]), 1)
            cluster_artifact = load_json(claim_clusters_path(run_dir))
            self.assertGreaterEqual(cluster_artifact["cluster_count"], 1)

            merge_payload = run_script(
                script_path("eco-merge-observation-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--metric",
                "pm2_5",
            )
            assert isinstance(merge_payload.get("canonical_ids"), list)
            self.assertEqual(1, len(merge_payload["canonical_ids"]))
            merged_artifact = load_json(merged_observations_path(run_dir))
            self.assertEqual(1, merged_artifact["merged_count"])
            merged_items = merged_artifact.get("merged_observations", [])
            assert isinstance(merged_items, list)
            self.assertEqual(2, merged_items[0]["member_count"])

            link_payload = run_script(
                script_path("eco-link-claims-to-observations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            assert isinstance(link_payload.get("canonical_ids"), list)
            self.assertGreaterEqual(len(link_payload["canonical_ids"]), 1)
            link_artifact = load_json(claim_observation_links_path(run_dir))
            self.assertGreaterEqual(link_artifact["link_count"], 1)
            links = link_artifact.get("links", [])
            assert isinstance(links, list)
            self.assertEqual("support", links[0]["relation"])

    def test_link_skill_falls_back_to_candidate_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            seed_signal_plane(run_dir, root, include_airnow=False)

            run_script(
                script_path("eco-extract-claim-candidates"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            run_script(
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

            custom_output = root / "custom_claim_observation_links.json"
            link_payload = run_script(
                script_path("eco-link-claims-to-observations"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--output-path",
                str(custom_output),
            )
            assert isinstance(link_payload.get("summary"), dict)
            self.assertEqual(str(custom_output), link_payload["summary"]["output_path"])
            self.assertTrue(custom_output.exists())
            self.assertGreaterEqual(link_payload["summary"]["link_count"], 1)


if __name__ == "__main__":
    unittest.main()
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]


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


def analytics_path(run_dir: Path, file_name: str) -> Path:
    return run_dir / "analytics" / file_name


def board_path(run_dir: Path) -> Path:
    return run_dir / "board" / "investigation_board.json"


def investigation_path(run_dir: Path, file_name: str) -> Path:
    return run_dir / "investigation" / file_name


def reporting_path(run_dir: Path, file_name: str) -> Path:
    return run_dir / "reporting" / file_name


def promotion_path(run_dir: Path, file_name: str) -> Path:
    return run_dir / "promotion" / file_name


def kernel_script_path() -> Path:
    return WORKSPACE_ROOT / "eco-concil-runtime" / "scripts" / "eco_runtime_kernel.py"


def run_kernel(*args: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, str(kernel_script_path()), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"Kernel failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )
    payload = json.loads(completed.stdout)
    assert isinstance(payload, dict)
    return payload


def seed_signal_plane(
    run_dir: Path,
    root: Path,
    run_id: str,
    round_id: str,
    *,
    include_airnow: bool = False,
    include_openmeteo: bool = False,
) -> None:
    youtube_path = root / "youtube.json"
    bluesky_path = root / "bluesky.json"
    openaq_path = root / "openaq.json"
    write_json(
        youtube_path,
        [
            {
                "query": "nyc smoke wildfire",
                "video_id": "vid-seed-001",
                "video": {
                    "id": "vid-seed-001",
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
                    "uri": "at://did:plc:smoke/app.bsky.feed.post/seed001",
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
        run_id,
        "--round-id",
        round_id,
        "--artifact-path",
        str(youtube_path),
    )
    run_script(
        script_path("eco-normalize-bluesky-cascade-public-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--artifact-path",
        str(bluesky_path),
    )
    run_script(
        script_path("eco-normalize-openaq-observation-signals"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
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
            run_id,
            "--round-id",
            round_id,
            "--artifact-path",
            str(airnow_path),
        )

    if include_openmeteo:
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
        run_script(
            script_path("eco-normalize-open-meteo-historical-signals"),
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--artifact-path",
            str(openmeteo_path),
        )


def seed_analysis_chain(
    run_dir: Path,
    root: Path,
    run_id: str,
    round_id: str,
    *,
    include_airnow: bool = True,
) -> dict[str, dict[str, Any]]:
    seed_signal_plane(run_dir, root, run_id, round_id, include_airnow=include_airnow)
    outputs: dict[str, dict[str, Any]] = {}
    outputs["extract_claims"] = run_script(
        script_path("eco-extract-claim-candidates"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    outputs["extract_observations"] = run_script(
        script_path("eco-extract-observation-candidates"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--metric",
        "pm2_5",
    )
    outputs["cluster_claims"] = run_script(
        script_path("eco-cluster-claim-candidates"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    outputs["merge_observations"] = run_script(
        script_path("eco-merge-observation-candidates"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--metric",
        "pm2_5",
    )
    outputs["link_evidence"] = run_script(
        script_path("eco-link-claims-to-observations"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    return outputs
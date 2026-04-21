from __future__ import annotations

import importlib
import importlib.util
import json
import sqlite3
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

from _workflow_support import load_json, run_script, runtime_path, script_path, write_json

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
sys.path.insert(0, str(RUNTIME_SRC))

RUN_ID = "run-migrated-sources-001"
ROUND_ID = "round-migrated-sources-001"

MIGRATED_SOURCE_SKILLS = [
    "bluesky-cascade-fetch",
    "gdelt-doc-search",
    "gdelt-events-fetch",
    "gdelt-mentions-fetch",
    "gdelt-gkg-fetch",
    "youtube-video-search",
    "youtube-comments-fetch",
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
    "airnow-hourly-obs-fetch",
    "openaq-data-fetch",
    "open-meteo-historical-fetch",
    "open-meteo-air-quality-fetch",
    "open-meteo-flood-fetch",
    "usgs-water-iv-fetch",
    "nasa-firms-fire-fetch",
]


def load_contract_module():
    return importlib.import_module("eco_council_runtime.kernel.source_queue_contract")


def write_script(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def base_mission() -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "run_id": RUN_ID,
        "topic": "NYC smoke verification",
        "objective": "Verify whether public-signal and regulatory-source chains can be normalized into the runtime signal plane.",
        "policy_profile": "standard",
        "window": {
            "start_utc": "2023-06-07T00:00:00Z",
            "end_utc": "2023-06-07T23:59:59Z",
        },
        "region": {
            "label": "New York City, USA",
            "geometry": {
                "type": "Point",
                "latitude": 40.7128,
                "longitude": -74.0060,
            },
        },
    }


def round_tasks(*, role: str, source_skills: list[str]) -> list[dict[str, object]]:
    return [
        {
            "task_id": f"task-{role}-{ROUND_ID}-01",
            "run_id": RUN_ID,
            "round_id": ROUND_ID,
            "assigned_role": role,
            "status": "planned",
            "objective": f"Process {'public' if role == 'sociologist' else 'environment'} source inputs for runtime ingress.",
            "inputs": {
                "source_skills": source_skills,
                "evidence_requirements": [
                    {
                        "requirement_id": f"req-{role}-{ROUND_ID}-01",
                        "summary": f"Normalize {role} source evidence into the signal plane.",
                    }
                ],
            },
        }
    ]


def write_prepare_inputs(run_dir: Path, *, mission: dict[str, object], tasks: list[dict[str, object]]) -> None:
    write_json(run_dir / "mission.json", mission)
    write_json(run_dir / "investigation" / f"round_tasks_{ROUND_ID}.json", tasks)


def analytics_db_path(run_dir: Path) -> Path:
    return run_dir / "analytics" / "signal_plane.sqlite"


def normalized_counts_by_source(run_dir: Path) -> dict[str, int]:
    with sqlite3.connect(analytics_db_path(run_dir)) as connection:
        rows = connection.execute(
            "SELECT source_skill, COUNT(*) FROM normalized_signals GROUP BY source_skill ORDER BY source_skill"
        ).fetchall()
    return {str(source_skill): int(count) for source_skill, count in rows}


def normalized_rows_for_source(run_dir: Path, source_skill: str) -> list[dict[str, object]]:
    with sqlite3.connect(analytics_db_path(run_dir)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT signal_id, source_skill, plane, signal_kind,
                   canonical_object_kind, external_id, artifact_path,
                   record_locator, title
            FROM normalized_signals
            WHERE source_skill = ?
            ORDER BY signal_id
            """,
            (source_skill,),
        ).fetchall()
    return [dict(row) for row in rows]


def load_skill_module(skill_name: str):
    module_name = f"test_{skill_name.replace('-', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, script_path(skill_name))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tsv_row(field_names: list[str], values: dict[str, str]) -> str:
    index = {name: position for position, name in enumerate(field_names)}
    columns = [""] * len(field_names)
    for field_name, value in values.items():
        columns[index[field_name]] = value
    return "\t".join(columns)


def write_zip_lines(path: Path, member_name: str, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member_name, "\n".join(lines) + "\n")
    return path


def write_gdelt_manifest(path: Path, zip_path: Path | None, *, request_url: str, timestamp_utc: str) -> Path:
    payload: dict[str, object] = {
        "downloads": [
            {
                "request_url": request_url,
                "entry": {
                    "timestamp_utc": timestamp_utc,
                    "url": request_url,
                    "size_bytes": zip_path.stat().st_size if zip_path is not None and zip_path.exists() else 0,
                    "md5": "fixture-md5",
                },
            }
        ]
    }
    if zip_path is not None:
        payload["downloads"][0]["output_path"] = str(zip_path)
    write_json(path, payload)
    return path


class MigratedSourceRuntimeIntegrationTests(unittest.TestCase):
    def test_source_catalog_covers_migrated_source_skills(self) -> None:
        contract = load_contract_module()
        missing = [skill for skill in MIGRATED_SOURCE_SKILLS if skill not in contract.SOURCE_CATALOG]
        self.assertEqual([], missing)

        for source_skill in MIGRATED_SOURCE_SKILLS:
            config = contract.SOURCE_CATALOG[source_skill]
            self.assertTrue(config.get("role"))
            self.assertTrue(config.get("family_id"))
            self.assertTrue(config.get("layer_id"))
            self.assertTrue(config.get("normalizer_skill"))
            self.assertTrue(script_path(str(config["normalizer_skill"])).exists())

        self.assertTrue(script_path("openaq-data-fetch").exists())

    def test_prepare_and_execute_youtube_comment_anchor_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            search_script = write_script(
                root / "emit_youtube_search.py",
                (
                    "import json\n"
                    "video_id='vidanchr001'\n"
                    "payload=[{'query':'nyc smoke wildfire','video_id':video_id,'video':{'id':video_id,'title':'Smoke over New York City','description':'Wildfire smoke covered the city.','channel_title':'City Desk','published_at':'2023-06-07T13:00:00Z','default_language':'en','statistics':{'view_count':1250}}}]\n"
                    "print(json.dumps(payload))\n"
                ),
            )
            comments_script = write_script(
                root / "emit_youtube_comments.py",
                (
                    "import json, pathlib, sys\n"
                    "args = sys.argv[1:]\n"
                    "anchor = ''\n"
                    "for index, token in enumerate(args):\n"
                    "    if token == '--video-ids-file' and index + 1 < len(args):\n"
                    "        anchor = args[index + 1]\n"
                    "        break\n"
                    "if not anchor:\n"
                    "    raise SystemExit('missing --video-ids-file')\n"
                    "payload = json.loads(pathlib.Path(anchor).read_text(encoding='utf-8'))\n"
                    "video_id = payload[0]['video_id']\n"
                    "records = [{'comment_id':'yt-comment-001','video_id':video_id,'thread_id':'yt-thread-001','parent_comment_id':'','comment_type':'top-level','text_original':'Smoke is clearly visible from Brooklyn.','text_display':'Smoke is clearly visible from Brooklyn.','published_at':'2023-06-07T13:10:00Z','updated_at':'2023-06-07T13:10:00Z','author_display_name':'Observer','channel_id':'channel-yt-001','like_count':7,'source':{'search_terms':'nyc smoke wildfire'}}]\n"
                    "print(json.dumps({'generated_at_utc':'2023-06-07T13:15:00Z','records':records}))\n"
                ),
            )
            mission = {
                **base_mission(),
                "source_governance": {
                    "max_selected_sources_per_role": 2,
                    "max_non_entry_layers_per_role": 1,
                    "approved_layers": [
                        {"family_id": "youtube", "layer_id": "comments"},
                    ],
                },
                "source_requests": [
                    {
                        "source_skill": "youtube-video-search",
                        "query_text": "nyc smoke wildfire",
                        "artifact_capture": "stdout-json",
                        "fetch_argv": [sys.executable, str(search_script)],
                    },
                    {
                        "source_skill": "youtube-comments-fetch",
                        "artifact_capture": "stdout-json",
                        "fetch_argv": [sys.executable, str(comments_script)],
                    },
                ],
            }
            write_prepare_inputs(
                run_dir,
                mission=mission,
                tasks=round_tasks(role="sociologist", source_skills=["youtube-video-search", "youtube-comments-fetch"]),
            )

            prepare_payload = run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))

            self.assertEqual("1.3.0", plan["schema_version"])
            self.assertEqual(2, prepare_payload["summary"]["step_count"])
            self.assertEqual(["youtube-video-search", "youtube-comments-fetch"], plan["roles"]["sociologist"]["selected_sources"])

            video_step, comments_step = plan["steps"]
            self.assertEqual("youtube-video-search", video_step["source_skill"])
            self.assertEqual("youtube-comments-fetch", comments_step["source_skill"])
            self.assertEqual([video_step["step_id"]], comments_step["depends_on"])
            self.assertEqual("same-round-source", comments_step["anchor_mode"])
            self.assertEqual([video_step["artifact_path"]], comments_step["anchor_artifact_paths"])
            self.assertEqual("current-round", comments_step["anchor_refs"][0]["scope"])
            self.assertEqual("youtube-video-search", comments_step["anchor_refs"][0]["source_skill"])
            video_arg_index = comments_step["fetch_argv"].index("--video-ids-file")
            self.assertEqual(video_step["artifact_path"], comments_step["fetch_argv"][video_arg_index + 1])

            import_payload = run_script(
                script_path("eco-import-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            execution = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            counts = normalized_counts_by_source(run_dir)

            self.assertEqual(0, import_payload["summary"]["failed_step_count"])
            self.assertEqual(2, execution["completed_count"])
            self.assertEqual(1, counts["youtube-video-search"])
            self.assertEqual(1, counts["youtube-comments-fetch"])

    def test_prepare_and_execute_regulationsgov_comment_detail_anchor_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            comments_script = write_script(
                root / "emit_reggov_comments.py",
                (
                    "import json\n"
                    "records=[{'id':'REG-2024-0001','attributes':{'title':'Air rule comment','comment':'The proposed rule should consider smoke impacts.','postedDate':'2024-03-01T00:00:00Z','agencyId':'EPA','docketId':'EPA-HQ-2024-0001','submitterName':'Concerned Citizen'}}]\n"
                    "print(json.dumps({'records':records}))\n"
                ),
            )
            detail_script = write_script(
                root / "emit_reggov_comment_detail.py",
                (
                    "import json, pathlib, sys\n"
                    "args = sys.argv[1:]\n"
                    "anchor = ''\n"
                    "for index, token in enumerate(args):\n"
                    "    if token == '--comment-ids-file' and index + 1 < len(args):\n"
                    "        anchor = args[index + 1]\n"
                    "        break\n"
                    "if not anchor:\n"
                    "    raise SystemExit('missing --comment-ids-file')\n"
                    "payload = json.loads(pathlib.Path(anchor).read_text(encoding='utf-8'))\n"
                    "comment_id = payload['records'][0]['id']\n"
                    "records = [{'comment_id':comment_id,'response_url':'https://www.regulations.gov/comment/' + comment_id,'detail':{'attributes':{'title':'Air rule comment detail','comment':'Detailed feedback about smoke and air quality impacts.','postedDate':'2024-03-01T00:00:00Z','modifyDate':'2024-03-01T01:00:00Z','receiveDate':'2024-03-01T00:30:00Z','agencyId':'EPA','docketId':'EPA-HQ-2024-0001','submitterName':'Concerned Citizen'}}}]\n"
                    "print(json.dumps({'records':records}))\n"
                ),
            )
            mission = {
                **base_mission(),
                "source_governance": {
                    "max_selected_sources_per_role": 2,
                    "max_non_entry_layers_per_role": 1,
                    "approved_layers": [
                        {"family_id": "regulationsgov", "layer_id": "comment-detail"},
                    ],
                },
                "source_requests": [
                    {
                        "source_skill": "regulationsgov-comments-fetch",
                        "artifact_capture": "stdout-json",
                        "fetch_argv": [sys.executable, str(comments_script)],
                    },
                    {
                        "source_skill": "regulationsgov-comment-detail-fetch",
                        "artifact_capture": "stdout-json",
                        "fetch_argv": [sys.executable, str(detail_script)],
                    },
                ],
            }
            write_prepare_inputs(
                run_dir,
                mission=mission,
                tasks=round_tasks(
                    role="sociologist",
                    source_skills=["regulationsgov-comments-fetch", "regulationsgov-comment-detail-fetch"],
                ),
            )

            run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan = load_json(runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json"))
            list_step, detail_step = plan["steps"]

            self.assertEqual("regulationsgov-comments-fetch", list_step["source_skill"])
            self.assertEqual("regulationsgov-comment-detail-fetch", detail_step["source_skill"])
            self.assertEqual([list_step["step_id"]], detail_step["depends_on"])
            self.assertEqual("same-round-source", detail_step["anchor_mode"])
            self.assertEqual([list_step["artifact_path"]], detail_step["anchor_artifact_paths"])
            comment_arg_index = detail_step["fetch_argv"].index("--comment-ids-file")
            self.assertEqual(list_step["artifact_path"], detail_step["fetch_argv"][comment_arg_index + 1])

            import_payload = run_script(
                script_path("eco-import-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            execution = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            counts = normalized_counts_by_source(run_dir)

            self.assertEqual(0, import_payload["summary"]["failed_step_count"])
            self.assertEqual(2, execution["completed_count"])
            self.assertEqual(1, counts["regulationsgov-comments-fetch"])
            self.assertEqual(1, counts["regulationsgov-comment-detail-fetch"])
            self.assertEqual(
                {"formal"},
                {str(row["plane"]) for row in normalized_rows_for_source(run_dir, "regulationsgov-comments-fetch")},
            )
            self.assertEqual(
                {"formal"},
                {str(row["plane"]) for row in normalized_rows_for_source(run_dir, "regulationsgov-comment-detail-fetch")},
            )
            self.assertEqual(
                {"formal-comment-signal"},
                {
                    str(row["canonical_object_kind"])
                    for row in normalized_rows_for_source(run_dir, "regulationsgov-comments-fetch")
                },
            )
            self.assertEqual(
                {"formal-comment-signal"},
                {
                    str(row["canonical_object_kind"])
                    for row in normalized_rows_for_source(run_dir, "regulationsgov-comment-detail-fetch")
                },
            )

    def test_import_execution_falls_back_to_raw_only_when_normalizer_missing(self) -> None:
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
                            "coordinates": {"latitude": 40.7004, "longitude": -74.0004},
                            "location": {"id": 1, "name": "NYC"},
                            "provider": {"name": "OpenAQ"},
                        }
                    ]
                },
            )
            mission = {
                **base_mission(),
                "artifact_imports": [
                    {
                        "source_skill": "openaq-data-fetch",
                        "artifact_path": str(openaq_path),
                        "source_mode": "test-fixture",
                    }
                ],
            }
            write_prepare_inputs(
                run_dir,
                mission=mission,
                tasks=round_tasks(role="environmentalist", source_skills=["openaq-data-fetch"]),
            )

            run_script(
                script_path("eco-prepare-round"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            plan_path = runtime_path(run_dir, f"fetch_plan_{ROUND_ID}.json")
            plan = load_json(plan_path)
            plan["steps"][0]["normalizer_skill"] = "eco-normalize-missing-observation-signals"
            write_json(plan_path, plan)

            payload = run_script(
                script_path("eco-import-fetch-execution"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
            )
            execution = load_json(runtime_path(run_dir, f"import_execution_{ROUND_ID}.json"))
            status = execution["statuses"][0]

            self.assertEqual(1, execution["completed_count"])
            self.assertEqual(0, execution["failed_count"])
            self.assertEqual(0, status["canonical_count"])
            self.assertEqual(1, status["warning_count"])
            self.assertTrue(any(item.get("code") == "raw-only-ingest" for item in payload["warnings"]))
            self.assertTrue(Path(status["artifact_path"]).exists())

    def test_gdelt_export_normalizers_write_row_level_signals_into_signal_plane(self) -> None:
        events_module = load_skill_module("eco-normalize-gdelt-events-public-signals")
        mentions_module = load_skill_module("eco-normalize-gdelt-mentions-public-signals")
        gkg_module = load_skill_module("eco-normalize-gdelt-gkg-public-signals")

        specs = [
            {
                "source_skill": "gdelt-events-fetch",
                "normalizer_skill": "eco-normalize-gdelt-events-public-signals",
                "field_names": list(events_module.EVENT_FIELDS),
                "member_name": "20230607130000.export.CSV",
                "signal_kind": "event-row",
                "rows": [
                    tsv_row(
                        list(events_module.EVENT_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-001",
                            "SQLDATE": "20230607",
                            "Actor1Name": "EPA",
                            "Actor2Name": "Residents",
                            "IsRootEvent": "1",
                            "EventCode": "112",
                            "EventBaseCode": "11",
                            "EventRootCode": "1",
                            "QuadClass": "4",
                            "NumMentions": "5",
                            "NumSources": "2",
                            "NumArticles": "3",
                            "AvgTone": "-1.2",
                            "ActionGeo_FullName": "New York, USA",
                            "ActionGeo_Lat": "40.7128",
                            "ActionGeo_Long": "-74.0060",
                            "DATEADDED": "20230607130000",
                            "SOURCEURL": "https://example.com/event-1",
                        },
                    ),
                    tsv_row(
                        list(events_module.EVENT_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-002",
                            "SQLDATE": "20230607",
                            "Actor1Name": "Health Department",
                            "Actor2Name": "Residents",
                            "EventCode": "120",
                            "EventBaseCode": "12",
                            "EventRootCode": "1",
                            "QuadClass": "2",
                            "NumMentions": "7",
                            "NumSources": "4",
                            "NumArticles": "5",
                            "AvgTone": "-0.3",
                            "ActionGeo_FullName": "Boston, USA",
                            "ActionGeo_Lat": "42.3601",
                            "ActionGeo_Long": "-71.0589",
                            "DATEADDED": "20230607130500",
                            "SOURCEURL": "https://example.com/event-2",
                        },
                    ),
                ],
            },
            {
                "source_skill": "gdelt-mentions-fetch",
                "normalizer_skill": "eco-normalize-gdelt-mentions-public-signals",
                "field_names": list(mentions_module.MENTION_FIELDS),
                "member_name": "20230607131500.mentions.CSV",
                "signal_kind": "mention-row",
                "rows": [
                    tsv_row(
                        list(mentions_module.MENTION_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-101",
                            "EventTimeDate": "20230607110000",
                            "MentionTimeDate": "20230607113000",
                            "MentionType": "1",
                            "MentionSourceName": "Example News",
                            "MentionIdentifier": "https://example.com/mention-1",
                            "SentenceID": "2",
                            "InRawText": "1",
                            "Confidence": "87",
                            "MentionDocLen": "420",
                            "MentionDocTone": "-0.8",
                        },
                    ),
                    tsv_row(
                        list(mentions_module.MENTION_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-102",
                            "EventTimeDate": "20230607120000",
                            "MentionTimeDate": "20230607124000",
                            "MentionType": "2",
                            "MentionSourceName": "Metro Desk",
                            "MentionIdentifier": "https://example.com/mention-2",
                            "SentenceID": "5",
                            "Confidence": "92",
                            "MentionDocLen": "510",
                            "MentionDocTone": "0.4",
                        },
                    ),
                ],
            },
            {
                "source_skill": "gdelt-gkg-fetch",
                "normalizer_skill": "eco-normalize-gdelt-gkg-public-signals",
                "field_names": list(gkg_module.GKG_FIELDS),
                "member_name": "20230607133000.gkg.csv",
                "signal_kind": "gkg-row",
                "rows": [
                    tsv_row(
                        list(gkg_module.GKG_FIELDS),
                        {
                            "GKGRECORDID": "gkg-001",
                            "DATE": "20230607133000",
                            "SourceCollectionIdentifier": "1",
                            "SourceCommonName": "Example News",
                            "DocumentIdentifier": "https://example.com/gkg-1",
                            "V2Themes": "ENV_POLLUTION;SMOKE",
                            "V2Persons": "John Doe",
                            "V2Organizations": "EPA",
                            "V2Tone": "-2.0,0,0,0,0,0,0",
                            "TranslationInfo": "srclc:eng",
                        },
                    ),
                    tsv_row(
                        list(gkg_module.GKG_FIELDS),
                        {
                            "GKGRECORDID": "gkg-002",
                            "DATE": "20230607134500",
                            "SourceCollectionIdentifier": "1",
                            "SourceCommonName": "City Paper",
                            "DocumentIdentifier": "https://example.com/gkg-2",
                            "V2Themes": "CLIMATE;AIR_POLLUTION",
                            "V2Persons": "Jane Roe",
                            "V2Organizations": "WHO",
                            "V2Tone": "1.5,0,0,0,0,0,0",
                        },
                    ),
                ],
            },
        ]

        for spec in specs:
            with self.subTest(source_skill=spec["source_skill"]):
                with tempfile.TemporaryDirectory() as tmpdir:
                    root = Path(tmpdir)
                    run_dir = root / "run"
                    zip_path = write_zip_lines(root / f"{spec['source_skill']}.zip", spec["member_name"], spec["rows"])
                    manifest_path = write_gdelt_manifest(
                        root / f"{spec['source_skill']}.json",
                        zip_path,
                        request_url=f"https://example.com/{spec['source_skill']}",
                        timestamp_utc="2023-06-07T13:00:00Z",
                    )

                    payload = run_script(
                        script_path(spec["normalizer_skill"]),
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        RUN_ID,
                        "--round-id",
                        ROUND_ID,
                        "--artifact-path",
                        str(manifest_path),
                    )
                    rows = normalized_rows_for_source(run_dir, str(spec["source_skill"]))

                    self.assertEqual(2, payload["summary"]["signal_count"])
                    self.assertEqual(2, len(rows))
                    self.assertEqual({str(spec["signal_kind"])}, {str(row["signal_kind"]) for row in rows})
                    self.assertEqual({str(zip_path)}, {str(row["artifact_path"]) for row in rows})
                    self.assertTrue(all(str(row["record_locator"]).startswith(f"zip://{spec['member_name']}#row=") for row in rows))

    def test_gdelt_events_normalizer_replaces_manifest_fallback_rows_on_rerun(self) -> None:
        events_module = load_skill_module("eco-normalize-gdelt-events-public-signals")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            manifest_path = write_gdelt_manifest(
                root / "gdelt-events.json",
                None,
                request_url="https://example.com/gdelt-events-manifest",
                timestamp_utc="2023-06-07T13:00:00Z",
            )

            first_payload = run_script(
                script_path("eco-normalize-gdelt-events-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(manifest_path),
            )
            first_rows = normalized_rows_for_source(run_dir, "gdelt-events-fetch")

            self.assertEqual(1, first_payload["summary"]["signal_count"])
            self.assertEqual(1, len(first_rows))
            self.assertEqual("export-download", first_rows[0]["signal_kind"])
            self.assertEqual(str(manifest_path), first_rows[0]["artifact_path"])

            zip_path = write_zip_lines(
                root / "gdelt-events.zip",
                "20230607130000.export.CSV",
                [
                    tsv_row(
                        list(events_module.EVENT_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-rerun-001",
                            "SQLDATE": "20230607",
                            "Actor1Name": "EPA",
                            "Actor2Name": "Residents",
                            "EventCode": "112",
                            "EventBaseCode": "11",
                            "DATEADDED": "20230607130000",
                            "SOURCEURL": "https://example.com/event-rerun",
                        },
                    )
                ],
            )
            write_gdelt_manifest(
                manifest_path,
                zip_path,
                request_url="https://example.com/gdelt-events-zip",
                timestamp_utc="2023-06-07T13:05:00Z",
            )

            second_payload = run_script(
                script_path("eco-normalize-gdelt-events-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(manifest_path),
            )
            second_rows = normalized_rows_for_source(run_dir, "gdelt-events-fetch")

            self.assertEqual(1, second_payload["summary"]["signal_count"])
            self.assertEqual(1, len(second_rows))
            self.assertEqual("event-row", second_rows[0]["signal_kind"])
            self.assertEqual(str(zip_path), second_rows[0]["artifact_path"])
            self.assertNotIn(str(manifest_path), {str(row["artifact_path"]) for row in second_rows})

    def test_gdelt_events_normalizer_total_row_limit_does_not_emit_manifest_fallback(self) -> None:
        events_module = load_skill_module("eco-normalize-gdelt-events-public-signals")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            zip_one = write_zip_lines(
                root / "gdelt-events-1.zip",
                "20230607130000.export.CSV",
                [
                    tsv_row(
                        list(events_module.EVENT_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-limit-001",
                            "SQLDATE": "20230607",
                            "Actor1Name": "EPA",
                            "Actor2Name": "Residents",
                            "EventCode": "112",
                            "EventBaseCode": "11",
                            "DATEADDED": "20230607130000",
                            "SOURCEURL": "https://example.com/event-limit-1",
                        },
                    )
                ],
            )
            zip_two = write_zip_lines(
                root / "gdelt-events-2.zip",
                "20230607131500.export.CSV",
                [
                    tsv_row(
                        list(events_module.EVENT_FIELDS),
                        {
                            "GLOBALEVENTID": "evt-limit-002",
                            "SQLDATE": "20230607",
                            "Actor1Name": "Health Department",
                            "Actor2Name": "Residents",
                            "EventCode": "120",
                            "EventBaseCode": "12",
                            "DATEADDED": "20230607131500",
                            "SOURCEURL": "https://example.com/event-limit-2",
                        },
                    )
                ],
            )
            manifest_path = root / "gdelt-events-limit.json"
            write_json(
                manifest_path,
                {
                    "downloads": [
                        {
                            "output_path": str(zip_one),
                            "request_url": "https://example.com/gdelt-events-1",
                            "entry": {"timestamp_utc": "2023-06-07T13:00:00Z", "url": "https://example.com/gdelt-events-1"},
                        },
                        {
                            "output_path": str(zip_two),
                            "request_url": "https://example.com/gdelt-events-2",
                            "entry": {"timestamp_utc": "2023-06-07T13:15:00Z", "url": "https://example.com/gdelt-events-2"},
                        },
                    ]
                },
            )

            payload = run_script(
                script_path("eco-normalize-gdelt-events-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(manifest_path),
                "--max-total-rows",
                "1",
            )
            rows = normalized_rows_for_source(run_dir, "gdelt-events-fetch")

            self.assertEqual(1, payload["summary"]["signal_count"])
            self.assertTrue(any(item.get("code") == "total-row-limit-reached" for item in payload["warnings"]))
            self.assertEqual(1, len(rows))
            self.assertEqual({"event-row"}, {str(row["signal_kind"]) for row in rows})
            self.assertEqual({str(zip_one)}, {str(row["artifact_path"]) for row in rows})


if __name__ == "__main__":
    unittest.main()

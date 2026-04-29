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


def report_basis_path(run_dir: Path, file_name: str) -> Path:
    return run_dir / "report_basis" / file_name


def runtime_path(run_dir: Path, file_name: str) -> Path:
    return run_dir / "runtime" / file_name


def kernel_script_path() -> Path:
    return WORKSPACE_ROOT / "eco-concil-runtime" / "scripts" / "eco_runtime_kernel.py"


def runtime_src_path() -> Path:
    return WORKSPACE_ROOT / "eco-concil-runtime" / "src"


def _kernel_command_args_with_actor_role(args: tuple[str, ...]) -> list[str]:
    if not args:
        return []
    command = args[0]
    argv = list(args)
    if "--actor-role" in argv:
        return argv

    runtime_src = runtime_src_path()
    if str(runtime_src) not in sys.path:
        sys.path.insert(0, str(runtime_src))

    from eco_council_runtime.kernel.access_policy import (
        command_requires_explicit_actor_role,
        kernel_command_actor_role_hint,
    )
    from eco_council_runtime.kernel.skill_registry import default_actor_role_hint

    if not command_requires_explicit_actor_role(command):
        return argv

    actor_role = ""
    if command in {"run-skill", "preflight-skill"}:
        skill_name = ""
        if "--skill-name" in argv:
            try:
                skill_name = argv[argv.index("--skill-name") + 1]
            except IndexError:
                skill_name = ""
        for flag in ("--agent-role", "--author-role", "--owner-role", "--claimed-by-role"):
            if flag in argv:
                try:
                    actor_role = argv[argv.index(flag) + 1]
                except IndexError:
                    actor_role = ""
                if actor_role:
                    break
        if not actor_role and skill_name:
            actor_role = default_actor_role_hint(skill_name)
        if not actor_role or actor_role.startswith("<"):
            actor_role = "moderator"
        if "--" in argv:
            marker_index = argv.index("--")
            return [*argv[:marker_index], "--actor-role", actor_role, *argv[marker_index:]]
        return [*argv, "--actor-role", actor_role]

    actor_role = kernel_command_actor_role_hint(command) or "runtime-operator"
    return [*argv, "--actor-role", actor_role]


def run_kernel_process(*args: str, auto_actor_role: bool = True) -> subprocess.CompletedProcess[str]:
    argv = _kernel_command_args_with_actor_role(args) if auto_actor_role else list(args)
    return subprocess.run(
        [sys.executable, str(kernel_script_path()), *argv],
        capture_output=True,
        text=True,
        check=False,
    )


def run_kernel(*args: str) -> dict[str, Any]:
    completed = run_kernel_process(*args)
    if completed.returncode != 0:
        raise AssertionError(
            f"Kernel failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
        )
    payload = json.loads(completed.stdout)
    assert isinstance(payload, dict)
    return payload


def request_phase_transition(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    transition_kind: str,
    target_round_id: str = "",
    source_round_id: str = "",
    rationale: str = "Test transition request.",
    evidence_refs: list[str] | None = None,
    basis_object_ids: list[str] | None = None,
    request_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    args = [
        "request-phase-transition",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--transition-kind",
        transition_kind,
        "--rationale",
        rationale,
    ]
    if target_round_id:
        args.extend(["--target-round-id", target_round_id])
    if source_round_id:
        args.extend(["--source-round-id", source_round_id])
    for evidence_ref in evidence_refs or []:
        args.extend(["--evidence-ref", evidence_ref])
    for basis_object_id in basis_object_ids or []:
        args.extend(["--basis-object-id", basis_object_id])
    if request_payload:
        args.extend(
            [
                "--request-payload-json",
                json.dumps(request_payload, ensure_ascii=True, sort_keys=True),
            ]
        )
    return run_kernel(*args)


def approve_phase_transition(
    run_dir: Path,
    *,
    request_id: str,
    approval_reason: str = "Approved for test execution.",
    evidence_refs: list[str] | None = None,
    basis_object_ids: list[str] | None = None,
    operator_notes: list[str] | None = None,
) -> dict[str, Any]:
    args = [
        "approve-phase-transition",
        "--run-dir",
        str(run_dir),
        "--request-id",
        request_id,
        "--approval-reason",
        approval_reason,
    ]
    for evidence_ref in evidence_refs or []:
        args.extend(["--evidence-ref", evidence_ref])
    for basis_object_id in basis_object_ids or []:
        args.extend(["--basis-object-id", basis_object_id])
    for operator_note in operator_notes or []:
        args.extend(["--operator-note", operator_note])
    return run_kernel(*args)


def request_and_approve_transition(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    transition_kind: str,
    target_round_id: str = "",
    source_round_id: str = "",
    rationale: str = "Test transition request.",
    approval_reason: str = "Approved for test execution.",
    evidence_refs: list[str] | None = None,
    basis_object_ids: list[str] | None = None,
    request_payload: dict[str, Any] | None = None,
    operator_notes: list[str] | None = None,
) -> str:
    request_payload_result = request_phase_transition(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        transition_kind=transition_kind,
        target_round_id=target_round_id,
        source_round_id=source_round_id,
        rationale=rationale,
        evidence_refs=evidence_refs,
        basis_object_ids=basis_object_ids,
        request_payload=request_payload,
    )
    request = (
        request_payload_result.get("request", {})
        if isinstance(request_payload_result.get("request"), dict)
        else {}
    )
    request_id = str(request.get("request_id") or request_payload_result["summary"]["request_id"])
    approve_phase_transition(
        run_dir,
        request_id=request_id,
        approval_reason=approval_reason,
        evidence_refs=evidence_refs,
        basis_object_ids=basis_object_ids,
        operator_notes=operator_notes,
    )
    return request_id


def request_skill_approval(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    rationale: str = "Request optional-analysis approval for test execution.",
    requested_actor_role: str = "",
    requested_skill_args: list[str] | None = None,
    evidence_refs: list[str] | None = None,
    basis_object_ids: list[str] | None = None,
    request_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    args = [
        "request-skill-approval",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--skill-name",
        skill_name,
        "--rationale",
        rationale,
    ]
    if requested_actor_role:
        args.extend(["--requested-actor-role", requested_actor_role])
    for requested_skill_arg in requested_skill_args or []:
        args.extend(["--requested-skill-arg", requested_skill_arg])
    for evidence_ref in evidence_refs or []:
        args.extend(["--evidence-ref", evidence_ref])
    for basis_object_id in basis_object_ids or []:
        args.extend(["--basis-object-id", basis_object_id])
    if request_payload:
        args.extend(
            [
                "--request-payload-json",
                json.dumps(request_payload, ensure_ascii=True, sort_keys=True),
            ]
        )
    return run_kernel(*args)


def approve_skill_approval(
    run_dir: Path,
    *,
    request_id: str,
    approval_reason: str = "Approved optional-analysis request for test execution.",
    evidence_refs: list[str] | None = None,
    basis_object_ids: list[str] | None = None,
    operator_notes: list[str] | None = None,
) -> dict[str, Any]:
    args = [
        "approve-skill-approval",
        "--run-dir",
        str(run_dir),
        "--request-id",
        request_id,
        "--approval-reason",
        approval_reason,
    ]
    for evidence_ref in evidence_refs or []:
        args.extend(["--evidence-ref", evidence_ref])
    for basis_object_id in basis_object_ids or []:
        args.extend(["--basis-object-id", basis_object_id])
    for operator_note in operator_notes or []:
        args.extend(["--operator-note", operator_note])
    return run_kernel(*args)


def request_and_approve_skill_approval(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    skill_name: str,
    rationale: str = "Request optional-analysis approval for test execution.",
    approval_reason: str = "Approved optional-analysis request for test execution.",
    requested_actor_role: str = "",
    requested_skill_args: list[str] | None = None,
    evidence_refs: list[str] | None = None,
    basis_object_ids: list[str] | None = None,
    request_payload: dict[str, Any] | None = None,
    operator_notes: list[str] | None = None,
) -> str:
    request_payload_result = request_skill_approval(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=skill_name,
        rationale=rationale,
        requested_actor_role=requested_actor_role,
        requested_skill_args=requested_skill_args,
        evidence_refs=evidence_refs,
        basis_object_ids=basis_object_ids,
        request_payload=request_payload,
    )
    request = (
        request_payload_result.get("request", {})
        if isinstance(request_payload_result.get("request"), dict)
        else {}
    )
    request_id = str(request.get("request_id") or request_payload_result["summary"]["request_id"])
    approve_skill_approval(
        run_dir,
        request_id=request_id,
        approval_reason=approval_reason,
        evidence_refs=evidence_refs,
        basis_object_ids=basis_object_ids,
        operator_notes=operator_notes,
    )
    return request_id


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
        script_path("normalize-youtube-video-public-signals"),
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
        script_path("normalize-bluesky-cascade-public-signals"),
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
        script_path("normalize-openaq-observation-signals"),
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
            script_path("normalize-airnow-observation-signals"),
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
            script_path("normalize-open-meteo-historical-signals"),
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
    outputs["discourse_issues"] = run_script(
        script_path("discover-discourse-issues"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    outputs["environment_aggregation"] = run_script(
        script_path("aggregate-environment-evidence"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
    )
    outputs["evidence_lanes"] = run_script(
        script_path("suggest-evidence-lanes"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--input-path",
        outputs["discourse_issues"]["summary"]["output_path"],
    )
    outputs["research_issue_surface"] = run_script(
        script_path("materialize-research-issue-surface"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--input-path",
        outputs["discourse_issues"]["summary"]["output_path"],
    )
    outputs["research_issue_views"] = run_script(
        script_path("project-research-issue-views"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--input-path",
        outputs["research_issue_surface"]["summary"]["output_path"],
    )
    return outputs


def primary_research_issue_id(outputs: dict[str, dict[str, Any]]) -> str:
    candidates = (
        outputs.get("research_issue_surface", {}).get("canonical_ids", [])
        if isinstance(outputs.get("research_issue_surface"), dict)
        else []
    )
    if isinstance(candidates, list) and candidates:
        return str(candidates[0])
    raise AssertionError("Expected seed_analysis_chain to produce a research issue id.")


def primary_successor_evidence_ref(outputs: dict[str, dict[str, Any]]) -> str:
    for key in (
        "environment_aggregation",
        "research_issue_surface",
        "discourse_issues",
    ):
        refs = (
            outputs.get(key, {}).get("artifact_refs", [])
            if isinstance(outputs.get(key), dict)
            else []
        )
        if not isinstance(refs, list) or not refs:
            continue
        first = refs[0]
        if isinstance(first, dict) and first.get("artifact_ref"):
            return str(first["artifact_ref"])
    raise AssertionError("Expected seed_analysis_chain to produce a successor evidence ref.")


def submit_ready_council_support(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    issue_id: str,
    evidence_ref: str,
    agent_role: str = "moderator",
    materialize_readiness_summary: bool = True,
) -> dict[str, dict[str, Any]]:
    proposal = run_script(
        script_path("submit-council-proposal"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--proposal-kind",
        "freeze-report-basis",
        "--agent-role",
        agent_role,
        "--rationale",
        "DB-backed successor evidence is sufficient for this test report_basis.",
        "--status",
        "submitted",
        "--confidence",
        "0.93",
        "--target-kind",
        "research-issue",
        "--target-id",
        issue_id,
        "--target-claim-id",
        issue_id,
        "--action-kind",
        "freeze-report-basis",
        "--assigned-role",
        "moderator",
        "--objective",
        "Promote the DB-backed evidence basis for reporting.",
        "--summary",
        "Promotion is supported by successor helper evidence and council review.",
        "--evidence-ref",
        evidence_ref,
        "--lineage-id",
        issue_id,
        "--report-basis-disposition",
        "freeze-report-basis",
        "--report-basis-freeze-allowed",
        "true",
        "--publication-readiness",
        "ready",
        "--handoff-status",
        "ready",
        "--moderator-status",
        "ready",
    )
    readiness = run_script(
        script_path("submit-readiness-opinion"),
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--agent-role",
        agent_role,
        "--readiness-status",
        "ready-for-report-basis",
        "--rationale",
        "DB-backed successor evidence and council proposal support report-basis freeze.",
        "--opinion-status",
        "submitted",
        "--sufficient-for-report-basis",
        "true",
        "--confidence",
        "0.93",
        "--basis-object-id",
        issue_id,
        "--evidence-ref",
        evidence_ref,
        "--lineage-id",
        issue_id,
    )
    result = {
        "proposal": proposal,
        "readiness": readiness,
    }
    if materialize_readiness_summary:
        result["readiness_summary"] = run_script(
            script_path("summarize-round-readiness"),
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
        )
    return result


def submit_report_basis_records(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    target_id: str,
    evidence_ref: str,
    case_label: str = "Policy research case fixture",
    agent_role: str = "environmental-investigator",
    finding_kind: str = "policy-research-finding",
    section_key: str = "key-findings",
) -> dict[str, str]:
    finding_payload = run_kernel(
        "submit-finding-record",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--actor-role",
        agent_role,
        "--agent-role",
        agent_role,
        "--finding-kind",
        finding_kind,
        "--title",
        f"{case_label} finding",
        "--summary",
        f"{case_label} has DB-backed evidence that can be cited in the decision-maker report.",
        "--rationale",
        "The finding is anchored to a normalized query result or approved evidence basis, not to a helper conclusion.",
        "--confidence",
        "0.84",
        "--target-kind",
        "research-issue",
        "--target-id",
        target_id,
        "--basis-object-id",
        target_id,
        "--evidence-ref",
        evidence_ref,
        "--provenance-json",
        json.dumps(
            {"source": "policy-research-fixture", "case_label": case_label},
            sort_keys=True,
        ),
    )
    finding_id = finding_payload["canonical_ids"][0]
    bundle_payload = run_kernel(
        "submit-evidence-bundle",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--actor-role",
        agent_role,
        "--agent-role",
        agent_role,
        "--bundle-kind",
        "report-evidence-bundle",
        "--title",
        f"{case_label} evidence bundle",
        "--summary",
        "Bundle links the investigator finding to the evidence ref that reporting may cite.",
        "--rationale",
        "Report basis is carried through a DB evidence bundle before proposal and publication.",
        "--confidence",
        "0.86",
        "--target-kind",
        "finding",
        "--target-id",
        finding_id,
        "--basis-object-id",
        target_id,
        "--finding-id",
        finding_id,
        "--evidence-ref",
        evidence_ref,
        "--provenance-json",
        json.dumps(
            {"source": "policy-research-fixture", "case_label": case_label},
            sort_keys=True,
        ),
    )
    bundle_id = bundle_payload["canonical_ids"][0]
    section_payload = run_kernel(
        "submit-report-section-draft",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--actor-role",
        "report-editor",
        "--agent-role",
        "report-editor",
        "--report-id",
        round_id,
        "--section-key",
        section_key,
        "--section-title",
        section_key.replace("-", " ").title(),
        "--section-text",
        f"{case_label} report section cites the finding and evidence bundle as the report_basis.",
        "--basis-object-id",
        finding_id,
        "--basis-object-id",
        bundle_id,
        "--bundle-id",
        bundle_id,
        "--finding-id",
        finding_id,
        "--evidence-ref",
        evidence_ref,
        "--provenance-json",
        json.dumps(
            {"source": "policy-research-fixture", "case_label": case_label},
            sort_keys=True,
        ),
    )
    return {
        "finding_id": finding_id,
        "bundle_id": bundle_id,
        "section_id": section_payload["canonical_ids"][0],
    }

from __future__ import annotations

from pathlib import Path
from typing import Any

from .kernel.deliberation_plane import store_promotion_freeze_record
from .kernel.manifest import write_json
from .kernel.paths import promotion_gate_path
from .kernel.phase2_state_surfaces import load_round_readiness_wrapper


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_path(run_dir: Path, override: str, default_path: Path) -> Path:
    text = maybe_text(override)
    if not text:
        return default_path.resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def apply_promotion_gate(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    readiness_path_override: str = "",
    output_path_override: str = "",
) -> dict[str, Any]:
    readiness_file = resolve_path(
        run_dir,
        readiness_path_override,
        run_dir / "reporting" / f"round_readiness_{round_id}.json",
    )
    output_file = resolve_path(
        run_dir,
        output_path_override,
        promotion_gate_path(run_dir, round_id),
    )

    readiness_context = load_round_readiness_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        readiness_path=readiness_path_override,
    )
    readiness = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else {}
    )
    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"
    gate_reasons = (
        readiness.get("gate_reasons", [])
        if isinstance(readiness.get("gate_reasons"), list)
        else []
    )
    recommended_next_skills = (
        readiness.get("recommended_next_skills", [])
        if isinstance(readiness.get("recommended_next_skills"), list)
        else []
    )
    warnings: list[dict[str, str]] = []
    if not readiness:
        warnings.append(
            {
                "code": "missing-readiness",
                "message": (
                    "No round readiness artifact or DB assessment was found at "
                    f"{readiness_file}."
                ),
            }
        )
    promote_allowed = readiness_status == "ready"
    gate_status = "allow-promote" if promote_allowed else "freeze-withheld"

    payload = {
        "schema_version": "runtime-gate-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "readiness_path": str(readiness_file),
        "readiness_status": readiness_status,
        "promote_allowed": promote_allowed,
        "gate_status": gate_status,
        "gate_reasons": [
            maybe_text(item) for item in gate_reasons if maybe_text(item)
        ],
        "recommended_next_skills": [
            maybe_text(item)
            for item in recommended_next_skills
            if maybe_text(item)
        ],
        "warnings": warnings,
    }
    write_json(output_file, payload)
    payload["output_path"] = str(output_file)
    store_promotion_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        gate_snapshot=payload,
        artifact_paths={"promotion_gate_path": str(output_file)},
    )
    return payload

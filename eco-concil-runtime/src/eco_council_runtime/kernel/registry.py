from __future__ import annotations

import json
from pathlib import Path

from .paths import registry_path


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def scan_skills(root: Path | None = None) -> list[dict[str, str]]:
    resolved_root = root or workspace_root()
    skills_root = resolved_root / "skills"
    entries: list[dict[str, str]] = []
    if not skills_root.exists():
        return entries
    for child in sorted(skills_root.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        skill_name = child.name
        script_path = child / "scripts" / f"{skill_name.replace('-', '_')}.py"
        if not script_path.exists():
            continue
        entries.append({"skill_name": skill_name, "script_path": str(script_path.resolve())})
    return entries


def registry_snapshot(root: Path | None = None) -> dict[str, object]:
    resolved_root = root or workspace_root()
    skills = scan_skills(resolved_root)
    return {
        "schema_version": "runtime-registry-v1",
        "workspace_root": str(resolved_root),
        "skill_count": len(skills),
        "skills": skills,
    }


def write_registry(run_dir: Path, root: Path | None = None) -> dict[str, object]:
    snapshot = registry_snapshot(root)
    path = registry_path(run_dir)
    path.write_text(json.dumps(snapshot, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return snapshot


def resolve_skill_script(skill_name: str, root: Path | None = None) -> Path:
    resolved_root = root or workspace_root()
    script_path = resolved_root / "skills" / skill_name / "scripts" / f"{skill_name.replace('-', '_')}.py"
    if not script_path.exists():
        raise ValueError(f"Unknown skill: {skill_name}")
    return script_path.resolve()
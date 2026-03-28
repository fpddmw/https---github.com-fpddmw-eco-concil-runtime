"""External eco-council skills integration for detached runtime deployments."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from eco_council_runtime.controller.io import maybe_text, run_json_command, utc_now_iso, write_json
from eco_council_runtime.layout import PROJECT_DIR

SKILLS_ROOT_ENV = "ECO_COUNCIL_SKILLS_ROOT"
SKILL_MANIFEST_FILENAME = ".eco-council-managed-skills.json"
IGNORED_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".hg",
    ".svn",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".DS_Store",
    ".idea",
    ".vscode",
    "node_modules",
    ".venv",
    "venv",
}
IGNORED_FILE_NAMES = {SKILL_MANIFEST_FILENAME}
EXPLICIT_FETCH_SCRIPT_FILENAMES = {
    "openaq-data-fetch": "openaq_api_client.py",
}


def _existing_dir(path: Path | None) -> Path | None:
    if path is None:
        return None
    if path.exists() and path.is_dir():
        return path.resolve()
    return None


def resolve_skills_root(configured: str = "") -> Path:
    candidate_text = maybe_text(configured) or os.environ.get(SKILLS_ROOT_ENV, "")
    if candidate_text:
        resolved = _existing_dir(Path(candidate_text).expanduser())
        if resolved is not None:
            return resolved
        raise FileNotFoundError(
            f"Configured skills root does not exist: {candidate_text}. "
            f"Set {SKILLS_ROOT_ENV} to the detached skills repository checkout."
        )

    candidates = [
        _existing_dir(PROJECT_DIR.parent),
        _existing_dir(PROJECT_DIR.parent / "skills"),
    ]
    for candidate in candidates:
        if candidate is None:
            continue
        if (candidate / "gdelt-doc-search").exists() and (candidate / "airnow-hourly-obs-fetch").exists():
            return candidate
    raise FileNotFoundError(
        "Unable to locate the detached eco-council skills repository. "
        f"Set {SKILLS_ROOT_ENV} explicitly."
    )


def mission_skill_names(mission: dict[str, Any] | None = None) -> list[str]:
    try:
        from eco_council_runtime import contract as contract_module
    except Exception as exc:
        raise RuntimeError("Unable to load eco-council contract while resolving skill catalog.") from exc

    families: list[dict[str, Any]] = []
    if isinstance(mission, dict):
        governance = contract_module.source_governance(mission)
        if isinstance(governance, dict):
            families = [
                family
                for family in governance.get("families", [])
                if isinstance(family, dict)
            ]
    if not families:
        families = [
            family
            for family in contract_module.DEFAULT_SOURCE_FAMILY_CATALOG
            if isinstance(family, dict)
        ]

    skill_names: list[str] = []
    seen: set[str] = set()
    for family in families:
        layers = family.get("layers")
        if not isinstance(layers, list):
            continue
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            for skill_name in layer.get("skills", []):
                text = maybe_text(skill_name)
                if not text or text.casefold() in seen:
                    continue
                seen.add(text.casefold())
                skill_names.append(text)
    return skill_names


def skill_dir(skill_name: str, *, skills_root_text: str = "") -> Path:
    root = resolve_skills_root(skills_root_text)
    path = root / skill_name
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Skill directory not found for {skill_name}: {path}")
    return path.resolve()


def default_env_file(skill_name: str, *, skills_root_text: str = "") -> Path | None:
    path = skill_dir(skill_name, skills_root_text=skills_root_text)
    primary = path / "assets" / "config.env"
    example = path / "assets" / "config.example.env"
    if primary.exists():
        return primary
    if example.exists():
        return example
    return None


def openaq_api_script_path(*, skills_root_text: str = "") -> Path:
    path = skill_dir("openaq-data-fetch", skills_root_text=skills_root_text) / "scripts" / "openaq_api_client.py"
    if not path.exists():
        raise FileNotFoundError(f"OpenAQ API client script not found: {path}")
    return path.resolve()


def fetch_script_path(skill_name: str, *, skills_root_text: str = "") -> Path:
    skill_path = skill_dir(skill_name, skills_root_text=skills_root_text)
    scripts_dir = skill_path / "scripts"
    explicit_name = EXPLICIT_FETCH_SCRIPT_FILENAMES.get(skill_name)
    candidates: list[Path] = []
    if explicit_name:
        candidates.append(scripts_dir / explicit_name)
    candidates.append(scripts_dir / f"{skill_name.replace('-', '_')}.py")
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    script_files = sorted(path for path in scripts_dir.glob("*.py") if path.is_file())
    if len(script_files) == 1:
        return script_files[0].resolve()
    raise FileNotFoundError(
        f"Unable to resolve fetch script for skill {skill_name}. Checked {candidates or [scripts_dir]}."
    )


def _managed_skills_dir(openclaw_env: dict[str, str]) -> Path:
    payload = run_json_command(["openclaw", "skills", "list", "--json"], cwd=PROJECT_DIR, env=openclaw_env)
    managed_text = maybe_text(payload.get("managedSkillsDir")) if isinstance(payload, dict) else ""
    if managed_text:
        managed_dir = Path(managed_text).expanduser().resolve()
    else:
        state_dir = Path(maybe_text(openclaw_env.get("OPENCLAW_STATE_DIR"))).expanduser().resolve()
        managed_dir = state_dir / "skills"
    managed_dir.mkdir(parents=True, exist_ok=True)
    return managed_dir


def _manifest_path(managed_dir: Path) -> Path:
    return managed_dir / SKILL_MANIFEST_FILENAME


def _list_openclaw_skill_names(openclaw_env: dict[str, str]) -> list[str]:
    payload = run_json_command(["openclaw", "skills", "list", "--json"], cwd=PROJECT_DIR, env=openclaw_env)
    skills = payload.get("skills") if isinstance(payload, dict) else []
    names: list[str] = []
    seen: set[str] = set()
    if isinstance(skills, list):
        for skill_info in skills:
            if not isinstance(skill_info, dict):
                continue
            name = maybe_text(skill_info.get("name"))
            if not name or name.casefold() in seen:
                continue
            seen.add(name.casefold())
            names.append(name)
    return names


def _sha256_for_path(path: Path, digest: Any) -> None:
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)


def _iter_skill_files(skill_path: Path) -> list[Path]:
    files: list[Path] = []
    for path in sorted(skill_path.rglob("*")):
        if any(part in IGNORED_DIR_NAMES for part in path.parts):
            continue
        if path.name in IGNORED_FILE_NAMES:
            continue
        if path.is_file():
            files.append(path)
    return files


def _projection_signature(skill_sources: dict[str, Path]) -> str:
    digest = hashlib.sha256()
    for skill_name in sorted(skill_sources):
        skill_path = skill_sources[skill_name]
        digest.update(skill_name.encode("utf-8"))
        for path in _iter_skill_files(skill_path):
            digest.update(str(path.relative_to(skill_path)).encode("utf-8"))
            _sha256_for_path(path, digest)
    return digest.hexdigest()


def _copy_skill_tree(source_dir: Path, destination_dir: Path) -> None:
    if destination_dir.exists():
        shutil.rmtree(destination_dir)
    ignore = shutil.ignore_patterns(*sorted(IGNORED_DIR_NAMES | IGNORED_FILE_NAMES))
    shutil.copytree(source_dir, destination_dir, ignore=ignore)


def sync_openclaw_managed_skills(
    *,
    openclaw_env: dict[str, str],
    mission: dict[str, Any] | None = None,
    skills_root_text: str = "",
) -> dict[str, Any]:
    skill_names = mission_skill_names(mission)
    skills_root = resolve_skills_root(skills_root_text)
    managed_dir = _managed_skills_dir(openclaw_env)
    manifest_path = _manifest_path(managed_dir)
    previous_manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            previous_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            previous_manifest = {}

    skill_sources = {skill_name: skill_dir(skill_name, skills_root_text=str(skills_root)) for skill_name in skill_names}
    signature = _projection_signature(skill_sources)

    previous_signature = maybe_text(previous_manifest.get("projection_signature"))
    previous_skills = [
        maybe_text(item)
        for item in previous_manifest.get("projected_skills", [])
        if maybe_text(item)
    ]
    current_skill_names = _list_openclaw_skill_names(openclaw_env)
    desired_present = all((managed_dir / skill_name).exists() for skill_name in skill_names)
    if (
        previous_signature == signature
        and previous_skills == skill_names
        and desired_present
        and all(skill_name in current_skill_names for skill_name in skill_names)
    ):
        return {
            "skills_root": str(skills_root),
            "managed_skills_dir": str(managed_dir),
            "projected_skills": skill_names,
            "recognized_skills": current_skill_names,
            "projection_signature": signature,
            "projected_at_utc": maybe_text(previous_manifest.get("projected_at_utc")) or utc_now_iso(),
            "changed": False,
            "manifest_path": str(manifest_path),
        }

    managed_dir.mkdir(parents=True, exist_ok=True)
    staging_root = Path(tempfile.mkdtemp(prefix="eco-council-skills-", dir=str(managed_dir)))
    try:
        staged_dirs: dict[str, Path] = {}
        for skill_name, source_dir in skill_sources.items():
            staged_dir = staging_root / skill_name
            _copy_skill_tree(source_dir, staged_dir)
            staged_dirs[skill_name] = staged_dir

        for stale_skill in previous_skills:
            if stale_skill in skill_names:
                continue
            stale_dir = managed_dir / stale_skill
            if stale_dir.exists():
                shutil.rmtree(stale_dir)

        for skill_name, staged_dir in staged_dirs.items():
            destination_dir = managed_dir / skill_name
            if destination_dir.exists():
                shutil.rmtree(destination_dir)
            os.replace(staged_dir, destination_dir)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)

    manifest = {
        "skills_root": str(skills_root),
        "managed_skills_dir": str(managed_dir),
        "projected_skills": skill_names,
        "projection_signature": signature,
        "projected_at_utc": utc_now_iso(),
    }
    write_json(manifest_path, manifest, pretty=True)

    recognized_skills = _list_openclaw_skill_names(openclaw_env)
    missing = [skill_name for skill_name in skill_names if skill_name not in recognized_skills]
    if missing:
        raise RuntimeError(
            "Projected eco-council skills were copied into OpenClaw but are still not recognized: "
            + ", ".join(missing)
        )

    return {
        "skills_root": str(skills_root),
        "managed_skills_dir": str(managed_dir),
        "projected_skills": skill_names,
        "recognized_skills": recognized_skills,
        "projection_signature": signature,
        "projected_at_utc": manifest["projected_at_utc"],
        "changed": True,
        "manifest_path": str(manifest_path),
    }

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .paths import registry_path


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[4]


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def parse_frontmatter(markdown_text: str) -> dict[str, str]:
    match = re.match(r"^---\s*\n(.*?)\n---\s*(?:\n|$)", markdown_text, re.DOTALL)
    if not match:
        return {}
    result: dict[str, str] = {}
    for line in match.group(1).splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        result[maybe_text(key)] = maybe_text(value).strip('"')
    return result


def section_lines(markdown_text: str, heading: str) -> list[str]:
    lines = markdown_text.splitlines()
    target = f"## {heading}"
    in_section = False
    collected: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped == target:
            in_section = True
            continue
        if in_section and stripped.startswith("## "):
            break
        if in_section:
            collected.append(line)
    return collected


def first_section_lines(markdown_text: str, headings: list[str]) -> list[str]:
    for heading in headings:
        lines = section_lines(markdown_text, heading)
        if lines:
            return lines
    return []


def parse_contract(markdown_text: str) -> dict[str, list[str]]:
    reads: list[str] = []
    writes: list[str] = []
    for line in section_lines(markdown_text, "Read/Write Contract"):
        stripped = line.strip()
        read_write_match = re.match(r"^\s*-\s*Reads and writes\s+`([^`]+)`", stripped)
        if read_write_match:
            path_pattern = read_write_match.group(1)
            reads.append(path_pattern)
            writes.append(path_pattern)
            continue
        match = re.match(r"^\s*-\s*(Reads|Writes)\s+`([^`]+)`", stripped)
        if not match:
            continue
        mode, path_pattern = match.groups()
        if mode == "Reads":
            reads.append(path_pattern)
        else:
            writes.append(path_pattern)
    return {"reads": reads, "writes": writes}


def parse_side_effects(markdown_text: str, contract: dict[str, list[str]]) -> list[str]:
    results: list[str] = []
    for line in first_section_lines(markdown_text, ["Side Effects", "Runtime Side Effects"]):
        stripped = line.strip()
        match = re.match(r"^\s*-\s*`?([^`]+?)`?\s*$", stripped)
        if not match:
            continue
        value = maybe_text(match.group(1))
        if value:
            results.append(value)
    if contract.get("reads"):
        results.append("reads-artifacts")
    if contract.get("writes"):
        results.append("writes-artifacts")
    if any("run_dir/../" in maybe_text(path) for path in contract.get("reads", [])):
        results.append("reads-shared-state")
    if any("run_dir/../" in maybe_text(path) for path in contract.get("writes", [])):
        results.append("writes-shared-state")
    deduped: list[str] = []
    seen: set[str] = set()
    for item in results:
        text = maybe_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def parse_execution_policy(markdown_text: str) -> dict[str, object]:
    payload: dict[str, object] = {}
    for line in first_section_lines(markdown_text, ["Runtime Policy", "Execution Policy"]):
        stripped = line.strip()
        match = re.match(r"^\s*-\s*`?([a-zA-Z0-9_]+)`?\s*:\s*`?([^`]+?)`?\s*$", stripped)
        if not match:
            continue
        key, raw_value = match.groups()
        normalized_key = maybe_text(key)
        value_text = maybe_text(raw_value)
        if normalized_key == "timeout_seconds":
            try:
                payload[normalized_key] = float(value_text)
            except ValueError:
                continue
        elif normalized_key in {"retry_budget", "retry_backoff_ms"}:
            try:
                payload[normalized_key] = int(float(value_text))
            except ValueError:
                continue
    return payload


def parse_required_inputs(markdown_text: str) -> dict[str, list[str]]:
    required: list[str] = []
    optional: list[str] = []
    target = required
    for line in section_lines(markdown_text, "Required Input"):
        stripped = line.strip()
        if not stripped.startswith("-"):
            continue
        value = stripped[1:].strip()
        if value.lower().startswith("optional"):
            target = optional
            continue
        target.append(value.strip("`").rstrip(":"))
    return {"required": required, "optional": optional}


def parse_agent_metadata(agent_config_path: Path) -> dict[str, str]:
    text = load_text_if_exists(agent_config_path)
    if not text:
        return {}
    interface_indent: int | None = None
    interface: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if interface_indent is None:
            if stripped == "interface:":
                interface_indent = len(line) - len(line.lstrip())
            continue
        if not stripped:
            continue
        current_indent = len(line) - len(line.lstrip())
        if current_indent <= interface_indent:
            break
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        interface[maybe_text(key)] = maybe_text(value).strip('"')
    return interface


def build_skill_entry(skill_dir: Path) -> dict[str, object] | None:
    skill_name = skill_dir.name
    script_path = skill_dir / "scripts" / f"{skill_name.replace('-', '_')}.py"
    if not script_path.exists():
        return None
    skill_doc_path = skill_dir / "SKILL.md"
    agent_config_path = skill_dir / "agents" / "openai.yaml"
    skill_doc_text = load_text_if_exists(skill_doc_path)
    frontmatter = parse_frontmatter(skill_doc_text)
    contract = parse_contract(skill_doc_text)
    return {
        "skill_name": skill_name,
        "script_path": str(script_path.resolve()),
        "skill_doc_path": str(skill_doc_path.resolve()) if skill_doc_path.exists() else "",
        "agent_config_path": str(agent_config_path.resolve()) if agent_config_path.exists() else "",
        "description": maybe_text(frontmatter.get("description")),
        "declared_contract": contract,
        "declared_inputs": parse_required_inputs(skill_doc_text),
        "declared_side_effects": parse_side_effects(skill_doc_text, contract),
        "execution_policy": parse_execution_policy(skill_doc_text),
        "agent": parse_agent_metadata(agent_config_path),
    }


def scan_skills(root: Path | None = None) -> list[dict[str, object]]:
    resolved_root = root or workspace_root()
    skills_root = resolved_root / "skills"
    entries: list[dict[str, object]] = []
    if not skills_root.exists():
        return entries
    for child in sorted(skills_root.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        entry = build_skill_entry(child)
        if entry is not None:
            entries.append(entry)
    return entries


def registry_snapshot(root: Path | None = None) -> dict[str, object]:
    resolved_root = root or workspace_root()
    skills = scan_skills(resolved_root)
    return {
        "schema_version": "runtime-registry-v2",
        "workspace_root": str(resolved_root),
        "skill_count": len(skills),
        "skills": skills,
    }


def write_registry(run_dir: Path, root: Path | None = None) -> dict[str, object]:
    snapshot = registry_snapshot(root)
    path = registry_path(run_dir)
    path.write_text(json.dumps(snapshot, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return snapshot


def resolve_skill_entry(skill_name: str, root: Path | None = None) -> dict[str, object]:
    resolved_root = root or workspace_root()
    skill_dir = resolved_root / "skills" / skill_name
    if not skill_dir.exists():
        raise ValueError(f"Unknown skill: {skill_name}")
    entry = build_skill_entry(skill_dir)
    if entry is None:
        raise ValueError(f"Unknown skill: {skill_name}")
    return entry


def resolve_skill_script(skill_name: str, root: Path | None = None) -> Path:
    entry = resolve_skill_entry(skill_name, root)
    return Path(str(entry["script_path"]))
from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


def _profile(
    *,
    queue_status: str,
    stage: str,
    queue_role: str,
    default_invocation: str,
    core_queue_default: bool,
    notes: str,
    downstream_hints: list[str] | None = None,
) -> dict[str, object]:
    return {
        "source_queue_ready": True,
        "queue_status": queue_status,
        "stage": stage,
        "queue_role": queue_role,
        "default_invocation": default_invocation,
        "core_queue_default": core_queue_default,
        "downstream_hints": list(downstream_hints or []),
        "notes": notes,
    }


BRIDGE_PROFILES: dict[str, dict[str, object]] = {
    "eco-scaffold-mission-run": _profile(
        queue_status="bridge",
        stage="ingress",
        queue_role="run-bootstrap",
        default_invocation="operator-triggered",
        core_queue_default=True,
        notes="Bootstrap one run from mission input and seed the first round state.",
        downstream_hints=["eco-prepare-round"],
    ),
    "eco-prepare-round": _profile(
        queue_status="bridge",
        stage="source-selection",
        queue_role="queue-planner",
        default_invocation="operator-triggered",
        core_queue_default=True,
        notes="Prepare a governed source queue from mission inputs, role coverage, and selected sources.",
        downstream_hints=["eco-import-fetch-execution"],
    ),
    "eco-import-fetch-execution": _profile(
        queue_status="bridge",
        stage="fetch-execution",
        queue_role="queue-executor",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Execute one prepared fetch queue by importing or fetching raw artifacts before normalization.",
        downstream_hints=["eco-build-normalization-audit", "eco-extract-claim-candidates", "eco-extract-observation-candidates"],
    ),
}


EXACT_PROFILES: dict[str, dict[str, object]] = {
    "eco-plan-round-orchestration": _profile(
        queue_status="advisory",
        stage="orchestration",
        queue_role="advisory-planner",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Keep as an advisory planner; do not let it become the only owner of the runtime queue.",
        downstream_hints=["eco-propose-next-actions", "eco-summarize-round-readiness"],
    ),
    "eco-build-normalization-audit": _profile(
        queue_status="direct",
        stage="audit",
        queue_role="normalization-audit",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Summarize normalization coverage and anomalies after fetch or import execution.",
        downstream_hints=["eco-extract-claim-candidates", "eco-extract-observation-candidates"],
    ),
    "eco-score-evidence-coverage": _profile(
        queue_status="direct",
        stage="audit",
        queue_role="coverage-audit",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Provide one deterministic evidence-coverage gate for runtime queue progression.",
        downstream_hints=["eco-summarize-round-readiness", "eco-promote-evidence-basis"],
    ),
    "eco-post-board-note": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="board-write",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Queue-compatible board write, but usually triggered by an agent or operator decision rather than the fixed queue.",
        downstream_hints=["eco-summarize-board-state"],
    ),
    "eco-read-board-delta": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="board-read",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Read board deltas as context for queue recovery, replay, or agent handoff.",
        downstream_hints=["eco-propose-next-actions", "eco-summarize-round-readiness"],
    ),
    "eco-update-hypothesis-status": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="board-write",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Allow queue-controlled status updates, but keep hypothesis mutation outside the fixed core queue by default.",
        downstream_hints=["eco-summarize-board-state", "eco-summarize-round-readiness"],
    ),
    "eco-open-challenge-ticket": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="challenge-write",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Challenge-ticket creation stays queue-compatible while remaining an explicit decision point.",
        downstream_hints=["eco-open-falsification-probe", "eco-summarize-board-state"],
    ),
    "eco-close-challenge-ticket": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="challenge-write",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Challenge closure should remain explicit, even in runtime queue mode.",
        downstream_hints=["eco-summarize-round-readiness"],
    ),
    "eco-claim-board-task": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="task-write",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Task-claim writes are queue-compatible but are usually driven by a role decision rather than the fixed queue.",
        downstream_hints=["eco-read-board-delta", "eco-summarize-board-state"],
    ),
    "eco-open-investigation-round": _profile(
        queue_status="advisory",
        stage="board",
        queue_role="round-transition",
        default_invocation="moderator-controlled",
        core_queue_default=False,
        notes="Moderator-controlled council-state skill that opens a follow-up round while preserving prior board state and carryover context.",
        downstream_hints=["eco-prepare-round", "eco-read-board-delta", "eco-query-public-signals", "eco-query-environment-signals"],
    ),
    "eco-summarize-board-state": _profile(
        queue_status="direct",
        stage="board",
        queue_role="board-snapshot",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Materialize one stable board summary inside runtime queue or replay flows.",
        downstream_hints=["eco-materialize-board-brief", "eco-propose-next-actions"],
    ),
    "eco-materialize-board-brief": _profile(
        queue_status="direct",
        stage="board",
        queue_role="board-brief",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Produce one compact handoff artifact from board state for queue replay or agent handoff.",
        downstream_hints=["eco-propose-next-actions", "eco-summarize-round-readiness"],
    ),
    "eco-propose-next-actions": _profile(
        queue_status="direct",
        stage="investigation",
        queue_role="round-planning",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Generate the next action set from board and evidence state inside queue mode.",
        downstream_hints=["eco-open-falsification-probe", "eco-summarize-round-readiness"],
    ),
    "eco-open-falsification-probe": _profile(
        queue_status="direct",
        stage="investigation",
        queue_role="probe-planning",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Materialize falsification probes as a controlled investigation step in runtime queue mode.",
        downstream_hints=["eco-summarize-round-readiness"],
    ),
    "eco-summarize-round-readiness": _profile(
        queue_status="direct",
        stage="investigation",
        queue_role="readiness-gate",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Provide one round-readiness gate before promotion or reporting.",
        downstream_hints=["eco-promote-evidence-basis", "eco-materialize-reporting-handoff"],
    ),
    "eco-promote-evidence-basis": _profile(
        queue_status="direct",
        stage="promotion",
        queue_role="promotion-gate",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Freeze promoted evidence as the runtime queue boundary between work state and reporting state.",
        downstream_hints=["eco-materialize-reporting-handoff"],
    ),
    "eco-materialize-reporting-handoff": _profile(
        queue_status="direct",
        stage="reporting",
        queue_role="reporting-handoff",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Convert promoted evidence into the reporting handoff artifact.",
        downstream_hints=["eco-draft-expert-report", "eco-draft-council-decision"],
    ),
    "eco-materialize-final-publication": _profile(
        queue_status="direct",
        stage="reporting",
        queue_role="publication-aggregation",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Aggregate canonical reports and decisions into the final publication artifact.",
        downstream_hints=[],
    ),
    "eco-materialize-history-context": _profile(
        queue_status="advisory",
        stage="context",
        queue_role="history-context",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="History context remains queue-compatible, but should stay optional and scenario-driven.",
        downstream_hints=["eco-post-board-note", "eco-materialize-board-brief"],
    ),
    "eco-archive-case-library": _profile(
        queue_status="direct",
        stage="archive",
        queue_role="archive-write",
        default_invocation="post-round",
        core_queue_default=False,
        notes="Archive the completed case after runtime queue or agent flow finishes.",
        downstream_hints=[],
    ),
    "eco-archive-signal-corpus": _profile(
        queue_status="direct",
        stage="archive",
        queue_role="archive-write",
        default_invocation="post-round",
        core_queue_default=False,
        notes="Archive normalized signal results after the run or queue replay finishes.",
        downstream_hints=[],
    ),
}


DIRECT_PREFIXES: tuple[tuple[str, dict[str, object]], ...] = (
    (
        "eco-normalize-",
        _profile(
            queue_status="direct",
            stage="normalize",
            queue_role="normalizer",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Normalize one raw artifact into canonical signal-plane rows.",
            downstream_hints=["eco-build-normalization-audit", "eco-extract-claim-candidates", "eco-extract-observation-candidates"],
        ),
    ),
    (
        "eco-extract-",
        _profile(
            queue_status="direct",
            stage="analysis",
            queue_role="extractor",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Extract claim or observation candidates from the canonical signal plane.",
            downstream_hints=["eco-cluster-claim-candidates", "eco-merge-observation-candidates", "eco-link-claims-to-observations"],
        ),
    ),
    (
        "eco-cluster-",
        _profile(
            queue_status="direct",
            stage="analysis",
            queue_role="clusterer",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Consolidate extracted claims into queue-stable claim clusters.",
            downstream_hints=["eco-link-claims-to-observations", "eco-derive-claim-scope"],
        ),
    ),
    (
        "eco-merge-",
        _profile(
            queue_status="direct",
            stage="analysis",
            queue_role="merger",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Merge observation candidates into queue-stable observation sets.",
            downstream_hints=["eco-link-claims-to-observations", "eco-derive-observation-scope"],
        ),
    ),
    (
        "eco-link-",
        _profile(
            queue_status="direct",
            stage="evidence",
            queue_role="evidence-linker",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Link claims and observations into evidence relations.",
            downstream_hints=["eco-derive-claim-scope", "eco-derive-observation-scope", "eco-score-evidence-coverage"],
        ),
    ),
    (
        "eco-derive-",
        _profile(
            queue_status="direct",
            stage="evidence",
            queue_role="scope-derivation",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Derive scope objects needed by readiness and reporting gates.",
            downstream_hints=["eco-score-evidence-coverage", "eco-summarize-round-readiness"],
        ),
    ),
    (
        "eco-draft-",
        _profile(
            queue_status="direct",
            stage="reporting",
            queue_role="report-draft",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Draft reporting artifacts after readiness and promotion have passed.",
            downstream_hints=["eco-publish-expert-report", "eco-publish-council-decision", "eco-materialize-final-publication"],
        ),
    ),
    (
        "eco-publish-",
        _profile(
            queue_status="direct",
            stage="reporting",
            queue_role="publication",
            default_invocation="planned-step",
            core_queue_default=True,
            notes="Publish canonical reporting outputs after draft validation.",
            downstream_hints=["eco-materialize-final-publication"],
        ),
    ),
)


ADVISORY_PREFIXES: tuple[tuple[str, dict[str, object]], ...] = (
    (
        "eco-query-",
        _profile(
            queue_status="advisory",
            stage="context",
            queue_role="context-query",
            default_invocation="on-demand",
            core_queue_default=False,
            notes="Query skills stay queue-compatible, but should remain selective context tools rather than fixed queue steps.",
            downstream_hints=["eco-post-board-note", "eco-propose-next-actions"],
        ),
    ),
    (
        "eco-lookup-",
        _profile(
            queue_status="advisory",
            stage="context",
            queue_role="forensic-lookup",
            default_invocation="on-demand",
            core_queue_default=False,
            notes="Lookup skills remain queue-compatible for replay or forensic recovery, but should stay optional.",
            downstream_hints=["eco-post-board-note", "eco-open-challenge-ticket"],
        ),
    ),
)


def source_queue_profile(skill_name: str) -> dict[str, object]:
    profile = BRIDGE_PROFILES.get(skill_name)
    if profile is not None:
        return dict(profile)

    profile = EXACT_PROFILES.get(skill_name)
    if profile is not None:
        return dict(profile)

    for prefix, prefix_profile in DIRECT_PREFIXES:
        if skill_name.startswith(prefix):
            return dict(prefix_profile)

    for prefix, prefix_profile in ADVISORY_PREFIXES:
        if skill_name.startswith(prefix):
            return dict(prefix_profile)

    return _profile(
        queue_status="advisory",
        stage="auxiliary",
        queue_role="manual-review",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="No specific runtime source-queue role is defined yet, so keep this skill queue-compatible but operator-triggered.",
        downstream_hints=[],
    )


def source_queue_profile_summary(skill_entries: Iterable[dict[str, object]]) -> dict[str, object]:
    queue_status_counts: Counter[str] = Counter()
    stage_counts: Counter[str] = Counter()
    queue_role_counts: Counter[str] = Counter()
    skill_count = 0
    source_queue_ready_count = 0
    core_queue_default_count = 0

    for entry in skill_entries:
        profile = entry.get("source_queue_profile") if isinstance(entry.get("source_queue_profile"), dict) else source_queue_profile(str(entry.get("skill_name", "")))
        queue_status = str(profile.get("queue_status", ""))
        stage = str(profile.get("stage", ""))
        queue_role = str(profile.get("queue_role", ""))
        skill_count += 1
        if profile.get("source_queue_ready") is True:
            source_queue_ready_count += 1
        if profile.get("core_queue_default") is True:
            core_queue_default_count += 1
        if queue_status:
            queue_status_counts[queue_status] += 1
        if stage:
            stage_counts[stage] += 1
        if queue_role:
            queue_role_counts[queue_role] += 1

    return {
        "skill_count": skill_count,
        "source_queue_ready_count": source_queue_ready_count,
        "core_queue_default_count": core_queue_default_count,
        "queue_status_counts": dict(sorted(queue_status_counts.items())),
        "stage_counts": dict(sorted(stage_counts.items())),
        "queue_role_counts": dict(sorted(queue_role_counts.items())),
    }


__all__ = ["source_queue_profile", "source_queue_profile_summary"]

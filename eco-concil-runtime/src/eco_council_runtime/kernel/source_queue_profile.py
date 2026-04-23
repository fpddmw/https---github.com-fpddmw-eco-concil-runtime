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
    if queue_status == "bridge":
        phase2_behavior = "governed-bridge"
    elif queue_status == "direct" and core_queue_default:
        phase2_behavior = "non-owning-runtime-surface"
    elif queue_status == "direct":
        phase2_behavior = "optional-runtime-surface"
    else:
        phase2_behavior = "on-demand-runtime-surface"
    return {
        "source_queue_ready": True,
        "queue_status": queue_status,
        "stage": stage,
        "queue_role": queue_role,
        "default_invocation": default_invocation,
        "phase2_behavior": phase2_behavior,
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
    "eco-classify-claim-verifiability": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="lane-classifier",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Classify claim-side verifiability posture before any downstream empirical work is treated as eligible.",
        downstream_hints=["eco-route-verification-lane", "eco-extract-issue-candidates"],
    ),
    "eco-route-verification-lane": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="lane-router",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Freeze whether each issue stays empirical, formal-record, discourse, or mixed before downstream evidence work proceeds.",
        downstream_hints=["eco-extract-issue-candidates", "eco-cluster-issue-candidates"],
    ),
    "eco-extract-issue-candidates": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="issue-extraction",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Project claim scopes into scope-level canonical issue-cluster candidates before claim-cluster merge compresses them.",
        downstream_hints=["eco-cluster-issue-candidates", "eco-materialize-controversy-map"],
    ),
    "eco-cluster-issue-candidates": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="issue-clustering",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Merge claim-side issue candidates into canonical issue-cluster rows before any board-facing controversy wrapper is aggregated.",
        downstream_hints=[
            "eco-extract-stance-candidates",
            "eco-extract-concern-facets",
            "eco-extract-actor-profiles",
            "eco-extract-evidence-citation-types",
            "eco-materialize-controversy-map",
        ],
    ),
    "eco-extract-stance-candidates": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="stance-decomposition",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Materialize typed stance-group rows from canonical issue clusters when the council needs an explicit stance breakdown.",
        downstream_hints=["eco-materialize-controversy-map", "eco-propose-next-actions"],
    ),
    "eco-extract-concern-facets": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="concern-decomposition",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Materialize typed concern-facet rows from canonical issue clusters when the council needs explicit controversy concerns.",
        downstream_hints=["eco-materialize-controversy-map", "eco-propose-next-actions"],
    ),
    "eco-extract-actor-profiles": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="actor-decomposition",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Materialize typed actor-profile rows from canonical issue clusters when board or reporting logic needs actor posture.",
        downstream_hints=["eco-materialize-controversy-map", "eco-propose-next-actions"],
    ),
    "eco-extract-evidence-citation-types": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="citation-decomposition",
        default_invocation="on-demand",
        core_queue_default=False,
        notes="Materialize typed evidence-citation-type rows from canonical issue clusters when the council needs explicit citation posture.",
        downstream_hints=["eco-materialize-controversy-map", "eco-propose-next-actions"],
    ),
    "eco-materialize-controversy-map": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="controversy-map",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Aggregate a board-facing controversy map from DB-native typed issue surfaces so the council can branch by controversy posture without relying on one monolithic extractor.",
        downstream_hints=["eco-propose-next-actions", "eco-post-board-note"],
    ),
    "eco-extract-observation-candidates": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="observation-extractor",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Extract observation candidates only when the current issue set still has empirical routes worth matching.",
        downstream_hints=["eco-merge-observation-candidates", "eco-route-verification-lane"],
    ),
    "eco-merge-observation-candidates": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="observation-merger",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Merge observation candidates only for rounds that still keep claims in the environmental-observation lane.",
        downstream_hints=["eco-link-claims-to-observations", "eco-route-verification-lane"],
    ),
    "eco-derive-claim-scope": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="claim-scope",
        default_invocation="planned-step",
        core_queue_default=True,
        notes="Derive claim-side scope and evidence-lane posture before deciding whether empirical matching is even allowed.",
        downstream_hints=["eco-classify-claim-verifiability", "eco-route-verification-lane"],
    ),
    "eco-link-formal-comments-to-public-discourse": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="formal-public-linker",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Link formal comments and public discourse only when formal-record or discourse lanes matter to the current issue set.",
        downstream_hints=["eco-identify-representation-gaps", "eco-detect-cross-platform-diffusion"],
    ),
    "eco-identify-representation-gaps": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="representation-gap-analysis",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Representation-gap analysis stays available, but should run only when the current issue set needs discourse versus formal balancing.",
        downstream_hints=["eco-propose-next-actions", "eco-post-board-note"],
    ),
    "eco-detect-cross-platform-diffusion": _profile(
        queue_status="direct",
        stage="analysis",
        queue_role="diffusion-analysis",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Cross-platform diffusion tracing is a lane-specific analysis aid, not a mandatory queue step for every round.",
        downstream_hints=["eco-propose-next-actions", "eco-post-board-note"],
    ),
    "eco-score-evidence-coverage": _profile(
        queue_status="direct",
        stage="audit",
        queue_role="coverage-audit",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Provide an empirical-only evidence-coverage gate after the route actually commits a claim set to observation matching.",
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
    "eco-link-claims-to-observations": _profile(
        queue_status="direct",
        stage="evidence",
        queue_role="empirical-linker",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Link claims to observations only for issues that remain in the environmental-observation lane.",
        downstream_hints=["eco-derive-observation-scope", "eco-score-evidence-coverage"],
    ),
    "eco-derive-observation-scope": _profile(
        queue_status="direct",
        stage="evidence",
        queue_role="observation-scope",
        default_invocation="route-gated",
        core_queue_default=False,
        notes="Derive observation scopes only for claims that still warrant empirical matching.",
        downstream_hints=["eco-score-evidence-coverage", "eco-post-board-note"],
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
    phase2_behavior_counts: Counter[str] = Counter()
    skill_count = 0
    source_queue_ready_count = 0

    for entry in skill_entries:
        profile = entry.get("source_queue_profile") if isinstance(entry.get("source_queue_profile"), dict) else source_queue_profile(str(entry.get("skill_name", "")))
        queue_status = str(profile.get("queue_status", ""))
        stage = str(profile.get("stage", ""))
        queue_role = str(profile.get("queue_role", ""))
        phase2_behavior = str(profile.get("phase2_behavior", ""))
        skill_count += 1
        if profile.get("source_queue_ready") is True:
            source_queue_ready_count += 1
        if queue_status:
            queue_status_counts[queue_status] += 1
        if stage:
            stage_counts[stage] += 1
        if queue_role:
            queue_role_counts[queue_role] += 1
        if phase2_behavior:
            phase2_behavior_counts[phase2_behavior] += 1

    return {
        "skill_count": skill_count,
        "source_queue_ready_count": source_queue_ready_count,
        "queue_status_counts": dict(sorted(queue_status_counts.items())),
        "stage_counts": dict(sorted(stage_counts.items())),
        "queue_role_counts": dict(sorted(queue_role_counts.items())),
        "phase2_behavior_counts": dict(sorted(phase2_behavior_counts.items())),
    }


__all__ = ["source_queue_profile", "source_queue_profile_summary"]

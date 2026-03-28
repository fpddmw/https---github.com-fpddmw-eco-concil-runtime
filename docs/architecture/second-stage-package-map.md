# Second-Stage Package Map

## Purpose

This document freezes the `T07.1` target map after the first-stage `T06` extraction.

The goal is not another broad "move code somewhere under `application/`" phase. The goal is to prevent new second-generation monoliths by assigning each hotspot to a concrete subdomain home before the next extraction slices begin.

## Structural Rules

- Treat root modules such as `contract.py`, `normalize.py`, `reporting.py`, `orchestrate.py`, `simulate.py`, and `supervisor.py` as public compatibility facades only.
- Treat flat first-stage `application/*.py` hotspots as migration shells, not final homes.
- New long-lived code should target the second-stage subpackages listed below rather than extending the current hotspot modules.
- Keep deterministic workflow state and auditable artifact emission at package boundaries; do not hide planning state inside prompt-only flows.

## Second-Stage Package Skeleton

### Application

- `application/contract/`
  - contract-side runtime services
  - run or round scaffolding helpers
  - bundle validation runtime helpers
- `application/normalize/`
  - normalization pipelines
  - evidence materialization
  - candidate and matching-prep workflows
- `application/reporting/`
  - round-state assembly
  - draft construction
  - packet and prompt materialization
  - promotion workflows
- `application/orchestration/`
  - prepare-stage services
  - fetch-plan assembly
  - execution orchestration
- `application/simulation/`
  - presets
  - synthetic payload builders
  - simulated raw artifact generation
  - simulation workflow runners
- `application/supervisor/`
  - lifecycle state transitions
  - import workflows
  - operator-facing lifecycle assembly
- `application/archive/`
  - run import and export workflows
  - cross-run case and signal archive services
- `application/investigation/`
  - investigation-state derivation
  - next-action planning
  - history-context assembly
  - comparative review helpers

### Domain

- `domain/mission/`
  - mission or window semantics
  - region, stage, and policy primitives that should stay free of storage or runtime side effects
- `domain/investigation/`
  - hypothesis and leg semantics
  - profile-independent planning rules
  - alternative or contradiction semantics
- `domain/matching/`
  - claim or observation matching semantics
  - verdict and confidence rules
- `domain/evidence/`
  - evidence-card, remand, isolation, and review semantics

### Adapters

- `adapters/storage/`
  - sqlite and cache adapters
  - archive database access
  - analytics path resolution
- `adapters/archive/`
  - case-library and signal-corpus persistence bridges
  - archive import or export helpers
- `adapters/audit/`
  - append-only ledger, snapshot, and validation adapters
- `adapters/openclaw/`
  - provisioning
  - managed-skill projection
  - turn execution and workspace bridges

## Hotspot Ownership Freeze

### Root Facades

- `contract.py`
  - remains a public facade and validation-heavy compatibility surface
  - long-lived implementation target: `application/contract/` plus small pure helpers in `domain/mission/`
- `normalize.py`
  - remains a public facade only
  - long-lived implementation target: `application/normalize/` and `domain/matching/`, `domain/evidence/`
- `reporting.py`
  - remains a public facade only
  - long-lived implementation target: `application/reporting/` and `application/investigation/`
- `orchestrate.py`
  - remains a public facade only
  - long-lived implementation target: `application/orchestration/`
- `simulate.py`
  - remains a thin public command surface only
  - long-lived implementation target: `application/simulation/`
- `supervisor.py`
  - remains a public lifecycle facade only
  - long-lived implementation target: `application/supervisor/`
- `investigation.py`
  - should stop being the main mixed home for profiles, planners, history-query assembly, and role focus
  - long-lived implementation target: split between `domain/investigation/` and `application/investigation/`
- `signal_corpus.py`
  - should stop loading `supervisor.py` as its runtime service surface
  - long-lived implementation target: `application/archive/` plus `adapters/archive/`
- `case_library.py`
  - should stop loading `supervisor.py` as its runtime service surface
  - long-lived implementation target: `application/archive/` plus `adapters/archive/`
- `external_skills.py`
  - remains a detached-runtime integration surface for now
  - long-lived implementation target: `adapters/openclaw/` with public compatibility wrappers if needed

### Current `application/` Hotspots

- `application/contract_runtime.py`
  - target split:
    - `application/contract/scaffolding.py`
    - `application/contract/validation_runtime.py`
    - `application/contract/cli_services.py`
- `application/normalize_sources.py`
  - target split:
    - `application/normalize/public_sources.py`
    - `application/normalize/environment_sources.py`
    - `application/normalize/source_cache.py`
- `application/normalize_library.py`
  - target split:
    - `application/normalize/library_state.py`
    - `application/normalize/materialization.py`
    - `application/normalize/library_merge.py`
- `application/normalize_evidence.py`
  - target split:
    - `application/normalize/matching_workflow.py`
    - `application/normalize/evidence_workflow.py`
    - `application/normalize/round_snapshot.py`
- `application/reporting_drafts.py`
  - target split:
    - `application/reporting/readiness.py`
    - `application/reporting/expert_reports.py`
    - `application/investigation/review.py`
    - `application/reporting/council_decision.py`
- `application/reporting_artifacts.py`
  - target split:
    - `application/reporting/packets.py`
    - `application/reporting/prompts.py`
    - `application/reporting/promotion.py`
    - `application/reporting/artifact_pipeline.py`
- `application/reporting_state.py`
  - target split:
    - `application/reporting/state_loader.py`
    - `application/reporting/phase_state.py`
    - `application/reporting/override_state.py`
- `application/orchestration_prepare.py`
  - target split:
    - `application/orchestration/prepare_outputs.py`
    - `application/orchestration/prepare_prompts.py`
- `application/orchestration_planning.py`
  - target split:
    - `application/orchestration/governance.py`
    - `application/orchestration/query_builders.py`
    - `application/orchestration/geometry.py`
    - `application/orchestration/step_synthesis.py`
    - `application/orchestration/fetch_plan_builder.py`
- `application/orchestration_execution.py`
  - target split:
    - `application/orchestration/fetch_execution.py`
    - `application/orchestration/data_plane.py`
    - `application/orchestration/matching_stage.py`
- `application/simulation_workflow.py`
  - target split:
    - `application/simulation/presets.py`
    - `application/simulation/payload_synthesis.py`
    - `application/simulation/raw_artifacts.py`
    - `application/simulation/workflow_runner.py`
- `application/supervisor_lifecycle.py`
  - target split:
    - `application/supervisor/state_store.py`
    - `application/supervisor/continue_run.py`
    - `application/supervisor/auto_archive.py`
    - `application/supervisor/outbox_refresh.py`

### Small Or Stable Modules

- `planning/next_round.py`
  - remains a small support package for now
  - if it grows materially, move decision-follow-up planning under `application/investigation/` or `application/reporting/`
- `domain/text.py`, `domain/rounds.py`, `domain/contract_bridge.py`
  - remain shared foundations unless they become hotspots
- `adapters/filesystem.py`, `adapters/run_paths.py`, `adapters/normalize_storage.py`
  - remain valid first-stage adapter modules
  - future growth should prefer `adapters/storage/` or another owned second-stage subpackage rather than expanding them indefinitely

## Controller Exit Map

The following responsibilities should leave `controller/` over `T07` and later slices:

- `controller/audit_chain.py`
  - move toward `adapters/audit/ledger.py`, `adapters/audit/snapshots.py`, and `adapters/audit/validation.py`
- `controller/openclaw.py`
  - move toward `adapters/openclaw/provisioning.py` and `adapters/openclaw/workspaces.py`
- `controller/agent_turns.py`
  - move toward `adapters/openclaw/turns.py`
- `external_skills.py`
  - merge long-lived projection and managed-skill logic into `adapters/openclaw/managed_skills.py`
- `controller/stage_imports.py`
  - move toward `application/supervisor/imports.py`
- `controller/artifact_builders.py`
  - move toward `application/reporting/` and `application/supervisor/` artifact assembly homes
- `controller/state_config.py`
  - move history-context assembly toward `application/investigation/history_context.py`
- `signal_corpus.py` and `case_library.py`
  - move runtime import flows toward `application/archive/importers.py`
  - move sqlite-facing logic toward `adapters/archive/`
- `controller/operator_surface.py` and `controller/run_summary.py`
  - remain transitional until lifecycle and archive boundaries settle, then split between `application/supervisor/` and `application/archive/`

## Public Import Policy

- Supported public runtime module paths remain the current root modules and CLI entrypoints unless a later task explicitly changes them.
- New internal code should import second-stage homes directly instead of routing through root facades.
- Transitional imports through `controller/` or root facades are allowed only when no second-stage home exists yet.

## Done Condition For `T07.1`

`T07.1` is considered complete when:

- this target map is present and linked from the active planning docs
- the second-stage package skeleton exists in source control
- topology regression tests lock the new package skeleton and the controller transitional contract

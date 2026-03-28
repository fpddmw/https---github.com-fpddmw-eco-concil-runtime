# Runtime Task Board

## Board Scope

- Persist forward execution planning in this file.
- Keep this board focused on open work only; do not keep old completed slice detail here.
- Historical baseline for completed `T01` to `T06` now lives in `docs/architecture/runtime-task-board-history-t01-t06.md`.

## Working Rules

- Keep at most one sub-slice in `in_progress` at a time.
- Finish one sub-slice end-to-end before opening the next one.
- A sub-slice is only `completed` when code, schema or contract updates, tests, and a short outcome note land together.
- If a change spans `schema + runtime + tests`, land it as one sub-slice rather than splitting it across partially merged tasks.
- Any new investigation feature must declare its token budget, deterministic inputs, emitted artifacts, and audit boundary before implementation starts.
- Any planner, retrieval, or review upgrade must remain replayable from canonical run artifacts; do not depend on hidden model-only memory.

## Status Legend

- `completed`: implemented and validated
- `in_progress`: the only active task
- `planned`: approved backlog, not yet active
- `blocked`: cannot proceed without a prior task or a design decision

## Current Baseline

- First-stage layered extraction is complete: `domain/`, `application/`, `adapters/`, and `cli/` all exist and are now the main long-lived destinations.
- The current structure is materially better than the original monolith, but `application/` now contains second-generation hotspots:
  - `application/simulation_workflow.py` (`2164` lines)
  - `application/normalize_sources.py` (`2154` lines)
  - `application/reporting_drafts.py` (`1677` lines)
  - `application/reporting_artifacts.py` (`1644` lines)
  - `application/orchestration_planning.py` (`1582` lines)
- Legacy root facades are thinner but still not fully settled as final public API surfaces:
  - `contract.py` (`2793` lines)
  - `normalize.py` (`1502` lines)
  - `reporting.py` (`902` lines)
  - `supervisor.py` (`839` lines)
  - `orchestrate.py` (`726` lines)
  - `simulate.py` (`68` lines)
- Strongest current capability: stage-machine discipline, run orchestration, and local tamper-evident audit flow.
- Weakest current capability: investigation planning and reasoning still rely mainly on static profiles, fixed gap mappings, deterministic matching heuristics, and compact status aggregation.
- Baseline regression note: the repository was green at `110` tests when this board was reset.

## Execution Order

- Recommended order:
  - `T07.1`
  - `T07.2`
  - `T08.1`
  - `T08.2`
  - `T08.3`
  - `T07.3`
  - `T08.4`
  - `T07.4`
  - `T08.5`
  - `T07.5`
  - `T07.6`
  - `T08.6`
- Rationale:
  - do the minimum structural cleanup first so new investigation work does not land in unstable module boundaries
  - introduce deterministic investigation state before changing retrieval, review, or action selection
  - keep benchmark and audit gates until the end of each major capability block

## Task List

### T07 Structure Optimization And Second-Stage Decomposition

- Status: `in_progress`
- Goal: move from first-stage extraction into a stable subdomain package layout, remove residual compatibility cycles, and prevent `application/` from becoming a new monolith layer.
- Non-goals:
  - dependency risk cleanup
  - detached skills repository redesign
  - major runtime behavior changes unless required to complete the structural split safely
- Target end state:
  - `src/eco_council_runtime/application/` gains subdomain ownership instead of flat file accumulation:
    - `application/contract/`
    - `application/normalize/`
    - `application/reporting/`
    - `application/orchestration/`
    - `application/simulation/`
    - `application/supervisor/`
    - `application/archive/`
    - `application/investigation/`
  - `src/eco_council_runtime/domain/` grows explicit semantic homes where needed:
    - `domain/investigation/`
    - `domain/evidence/`
    - `domain/matching/`
    - `domain/mission/`
  - `src/eco_council_runtime/adapters/` becomes the permanent home for audit, OpenClaw, storage, and archive bridges rather than leaving those responsibilities under `controller/`
  - root modules become intentionally thin public facades or CLI entrypoints only
  - `controller/` shrinks toward compatibility shims and narrow workflow internals instead of long-lived ownership
- Acceptance criteria:
  - every remaining hotspot has an explicit destination and owner
  - targeted compatibility cycles are removed
  - no newly created extracted module should exceed roughly `1000` lines without an explicit exception note in this board
  - public entrypoints and benchmarked behavior remain stable

#### T07.1 Structural Target Map And Ownership Freeze

- Status: `completed`
- Scope:
  - define the second-stage package map under `application/`, `domain/`, and `adapters/`
  - assign each current hotspot file to concrete destination modules
  - define which root modules remain public facades and which imports are transitional only
  - define the controller exit map for `audit_chain`, OpenClaw helpers, archive import helpers, and supervisor internals
- Acceptance:
  - no hotspot remains without a target destination
  - import ownership is unambiguous enough to guide later slices without re-planning
- Outcome:
  - added `docs/architecture/second-stage-package-map.md` as the concrete `T07.1` ownership freeze for second-stage extraction, including hotspot-to-target mapping, public-facade policy, and controller exit mapping
  - updated `docs/architecture/standalone-controller-target.md` and `src/eco_council_runtime/controller/__init__.py` so the structural target is no longer described as a `T06`-only transitional state
  - added importable second-stage package skeletons under `application/`, `domain/`, and `adapters/`, giving later `T07` slices concrete destinations instead of ad hoc flat-file growth
  - extended package-topology regression coverage to lock the new second-stage package skeleton and keep `controller/` explicitly transitional, and targeted `unittest` regressions passed with `4` tests

#### T07.2 Compatibility-Cycle And Boundary Cleanup

- Status: `planned`
- Scope:
  - remove or isolate remaining compatibility back-import patterns, especially:
    - `application.contract_runtime -> contract`
    - `contract -> application.contract_runtime`
    - `signal_corpus -> supervisor`
    - `case_library -> supervisor`
  - clean stale README or architecture references that still describe transitional boundaries as final
  - ensure new structural work does not need lazy root-module imports to function
- Acceptance:
  - targeted modules import final package homes instead of bouncing through root facades
  - structural docs and runtime imports describe the same boundary model

#### T07.3 Reporting And Orchestration Hotspot Split

- Status: `planned`
- Scope:
  - split `application/reporting_drafts.py` into subdomain modules such as readiness, expert reports, investigation review, and council decision
  - split `application/reporting_artifacts.py` into packet building, prompt rendering, artifact persistence, and promotion flows
  - split `application/orchestration_planning.py` into governance checks, query builders, geometry or window helpers, and step synthesis
- Acceptance:
  - reporting and orchestration services stop depending on second-generation mega-modules
  - direct regression coverage moves with the extracted submodules

#### T07.4 Normalize, Simulation, And Archive Hotspot Split

- Status: `planned`
- Scope:
  - split `application/normalize_sources.py` into public-source, environment-source, and cache-wrapper or ingestion helper modules
  - split `application/simulation_workflow.py` into preset loading, raw artifact generation, synthetic payload builders, and workflow runners
  - move cross-run archive responsibilities toward `application/archive/` plus dedicated archive adapters so `signal_corpus.py` and `case_library.py` stop depending on `supervisor.py`
- Acceptance:
  - normalize, simulation, and archive flows have stable subdomain boundaries
  - archive import code no longer requires root `supervisor` module loading

#### T07.5 Root Facade Contraction And Controller Retirement Pass

- Status: `planned`
- Scope:
  - shrink root facades to intentional public API wrappers only
  - reduce `controller/` to documented compatibility shims or clearly bounded workflow internals
  - update README and architecture docs to reflect the final package layout after the second-stage split
- Acceptance:
  - root modules are visibly thin
  - `controller/` is no longer treated as a permanent home for new work
  - docs reflect the actual production boundaries

#### T07.6 Structural Regression Gate

- Status: `planned`
- Scope:
  - add topology or import-boundary regressions where useful
  - lock compatibility entrypoints that must remain stable
  - add guardrails against reintroducing the cleaned compatibility cycles
- Acceptance:
  - the second-stage structure is test-guarded rather than documented only
  - future refactors cannot easily drift back into flat mega-modules

### T08 Investigation Capability Upgrade Under Controlled Token And Auditable Flow

- Status: `planned`
- Goal: improve investigation quality from static heuristic routing toward evidence-state-guided planning, while keeping token use bounded and the full process replayable and auditable.
- Hard constraints:
  - deterministic state first, optional model assistance second
  - every new investigation artifact must be persisted to canonical run paths before it can influence later stages
  - every new planner or retrieval step must expose its inputs, selection reasons, and budget counters
  - source governance remains authoritative; the planner may recommend only mission-governed families, layers, or explicitly permitted skills
  - prompt-facing investigation context must be compact structured summaries rather than raw artifact dumps
- Initial budget targets:
  - max primary hypotheses materialized per round: `3`
  - max alternative hypotheses per primary hypothesis: `2`
  - max ranked next actions per planning pass: `6`
  - max historical cases surfaced to a role in one pass: `3`
  - max excerpt blocks per retrieved case: `2`
  - new retrieval or planning packets must emit a bounded token estimate or size counter alongside content
- Acceptance criteria:
  - planning no longer depends only on fixed profile and gap mappings
  - investigation review compares competing explanations rather than only aggregating leg statuses
  - retrieval becomes more useful without allowing uncontrolled context growth
  - all new state is auditable, replayable, and benchmarked

#### T08.1 Investigation State Artifact And Replay Model

- Status: `planned`
- Scope:
  - add a deterministic `shared/investigation_state.json`
  - derive it from canonical artifacts such as `investigation_plan`, matching outputs, evidence adjudication, curated evidence, and moderator review artifacts
  - track per hypothesis, per leg, and per alternative:
    - support
    - contradiction
    - coverage
    - remaining gaps
    - uncertainty
    - latest evidence refs
    - last update stage or round
- Acceptance:
  - investigation state can be rebuilt from canonical artifacts without hidden memory
  - later planners and reviews read this state instead of inferring everything from scratch each time

#### T08.2 Budgeted Next-Action Planner

- Status: `planned`
- Scope:
  - add a machine-readable `shared/investigation_actions.json`
  - derive candidate actions from unresolved required legs, contradiction signals, alternative hypotheses, and governed source options
  - score actions using explicit components such as:
    - expected evidence gain
    - contradiction resolution value
    - coverage gain
    - token or context cost penalty
    - audit clarity
  - keep the output bounded to the configured top actions only
- Acceptance:
  - the runtime can explain why one next action outranks another
  - action selection becomes evidence-state-aware instead of only gap-template-driven

#### T08.3 Retrieval V2 With Compact History Evidence

- Status: `planned`
- Scope:
  - upgrade historical retrieval from case-level overlap scoring toward compact artifact-level support
  - keep structured filters, but add bounded retrieval of report, decision, evidence-card, or curated-summary snippets
  - persist a machine-readable retrieval snapshot alongside the current human-readable history context so retrieval remains auditable
  - ensure weak lexical overlap cannot dominate retrieval when structured mismatch is strong
- Acceptance:
  - retrieved history is more directly useful for investigation planning
  - retrieval remains compact, explainable, and safe under the budget caps

#### T08.4 Competing-Hypothesis Review And Decision Gating

- Status: `planned`
- Scope:
  - upgrade investigation review so it compares primary and alternative explanations explicitly
  - surface contradiction paths, not just support paths
  - make review outputs explain whether another round is needed because of unresolved required legs, unresolved alternatives, or contradictory evidence
- Acceptance:
  - moderator review becomes a comparative causal assessment rather than mostly a leg-status rollup
  - decision drafting can consume clearer uncertainty and contradiction signals

#### T08.5 Governance-Aware Discovery Or Probe Mode

- Status: `planned`
- Scope:
  - allow limited exploratory investigation when current evidence is insufficient or atypical
  - constrain exploration by mission governance, explicit budgets, and auditable reason codes
  - keep discovery outputs as recommendations or probe requests rather than free-form uncontrolled source execution
- Acceptance:
  - recall improves on non-template missions without opening an unbounded fetch surface
  - every exploratory step remains reviewable and replayable

#### T08.6 Evaluation, Token-Budget, And Audit Regression Gate

- Status: `planned`
- Scope:
  - expand benchmarks for ambiguous attribution, false causal chains, contradictory evidence, low-evidence rounds, and atypical missions
  - add regression checks for token-budget envelopes, retrieval compactness, and deterministic replay of investigation artifacts
  - ensure any new investigation artifacts are included in validation and audit-chain coverage where appropriate
- Acceptance:
  - new investigation behavior has scenario-level regression coverage
  - budget drift and audit regressions fail deterministically

## Current Task Notes

- Active task: none
- Next planned task: `T07.2 Compatibility-Cycle And Boundary Cleanup`
- Working rule reaffirmed: after a sub-slice passes acceptance, persist its outcome here and then explicitly open the next sub-slice before coding continues.

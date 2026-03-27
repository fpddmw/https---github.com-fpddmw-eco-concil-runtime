# Runtime Task Board

## Working Rules

- Persist all execution planning in this file.
- Keep at most one task in `in_progress` at a time; if the next task has not started yet, leave it `planned`.
- A task is only `completed` when code, schema/contract updates, tests, and a short outcome note all land together.
- If a change is tightly coupled across `schema + runtime + tests`, treat it as one task and finish the whole slice before moving on.

## Status Legend

- `completed`: implemented and validated
- `in_progress`: the only active task
- `planned`: approved backlog, not yet active
- `blocked`: cannot proceed without a prior task or a design decision

## Task List

### T01 Claim-Local Scope And Public Claim Legging

- Status: `completed`
- Goal: separate `mission scope` from `claim-local scope` so public claims without signal-local anchors cannot be silently treated as direct local evidence.
- Outcome:
  - public claims now carry `claim_scope`
  - matching refuses direct use of mission-fallback public claims
  - `hypothesis_id` and `leg_id=public_interpretation` flow through claim-side artifacts
  - regression tests cover false-localized and localized public claims

### T02 Observation Leg Attribution And Physical-Leg Review Filtering

- Status: `completed`
- Goal: assign physical observations to `hypothesis_id` and `leg_id` when inferable, then make investigation review prefer those tags over raw metric-family fallback.
- Scope:
  - observation candidates
  - observation curation entries and submissions
  - shared observations and matching candidate views
  - investigation review of `source / mechanism / impact`
- Outcome:
  - atomic smoke-transport observations now infer stable `hypothesis_id` and `leg_id` for `source`, `mechanism`, and `impact`
  - curated observation submissions now preserve candidate-side tags conservatively and allow explicit top-level override without losing component attribution
  - investigation review now prefers explicit observation `hypothesis_id` and `leg_id` over metric-family fallback for physical legs
  - `normalize.py` and `reporting.py` now keep observation signature and hydration behavior aligned for tagged observations
  - regression tests cover observation legging and the repository test suite now passes with `31` tests
- Acceptance criteria:
  - atomic observations receive stable `hypothesis_id` and `leg_id` when the investigation plan makes the mapping unambiguous
  - curated observations preserve or intentionally override those tags
  - investigation review does not count a physical observation toward the wrong leg when explicit leg tags exist
  - smoke-transport tests cover `source`, `mechanism`, and `impact`
  - all repository tests pass

### T03 Benchmark Expansion For Investigation Quality

- Status: `completed`
- Goal: move from flow tests to scenario-level regression on smoke, flood, heat, policy, and negative-match cases.
- Outcome:
  - added a scenario-style benchmark matrix covering smoke, flood, heat, and policy cases across positive, contradiction, and no-match paths
  - benchmark expectations now lock `profile_id`, `claim_type`, `hypothesis_id`, `leg_id`, match verdicts, selected observation leg tags, and selected investigation-review statuses
  - benchmark rollout surfaced and fixed a latent flood-assessment bug in `normalize.py` where precipitation/hydrology metric sets were referenced but not defined
  - benchmark rollout also fixed evidence-card generation so unmatched claims without any linked observations no longer create misleading `supported` review paths
  - repository test suite now passes with `34` tests
- Acceptance criteria:
  - at least one positive and one false-match scenario per major profile
  - expected `hypothesis / leg / match` outcomes encoded in tests or fixtures

### T04 Investigation Planning And Retrieval Upgrade

- Status: `completed`
- Goal: replace shallow keyword routing with structured hypothesis-gap planning and stronger historical retrieval.
- Outcome:
  - investigation plans now emit explicit `fetch_intents`, leg-level `gap_types`, top-level `history_query`, and first-class `alternative_hypotheses` instead of only chain-leg scaffolding
  - role-prioritized `causal_focus` now carries gap-aware and alternative-aware planning context, so roles no longer start from a purely linear hypothesis view
  - case-library search now scores structured overlap across `profile_id`, `claim_type`, `metric_family`, `gap_type`, and `source_skill`, while preventing weak generic overlaps from dominating retrieval
  - history-context generation now reuses the structured investigation query rather than only `topic + region`, and renders planning-relevant overlap from matched historical cases
  - regression coverage now locks the new planner output, structured retrieval behavior, and history-context wiring, and the repository test suite passes with `37` tests
- Acceptance criteria:
  - planner emits gap-driven fetch intent
  - historical retrieval uses structured fields beyond lexical overlap
  - negative or alternative hypotheses become first-class planning inputs

### T05 Tamper-Evident Audit Chain

- Status: `completed`
- Goal: upgrade traceability into append-only, tamper-evident auditability.
- Scope for current slice:
  - add a dedicated audit-chain ledger with content-addressed artifact snapshots
  - record receipts at import, fetch, normalize, match, and decision phase boundaries
  - validate chain continuity, snapshot digests, and latest canonical artifact integrity deterministically
- Outcome:
  - added a dedicated append-only audit chain under `shared/evidence-library/audit-chain` with chained receipts and content-addressed snapshot blobs
  - import, fetch, normalize, match, and decision boundaries now emit immutable receipts, and import-driven fetch/data-plane flows now record `import` before phase receipts for clearer chronology
  - bundle validation now checks chain continuity, receipt digests, snapshot digests, and latest canonical artifact integrity, and malformed tampering now fails deterministically instead of crashing validation
  - regression coverage now locks full-phase receipt recording, import sequencing, latest-artifact tamper detection, and malformed-chain handling, and the repository test suite passes with `42` tests
- Acceptance criteria:
  - immutable receipts for import/fetch/normalize/match/decision
  - artifact digests and chained ledger entries
  - deterministic validation of chain integrity

### T06 Structural Decomposition After Semantics Stabilize

- Status: `planned`
- Goal: turn the current monolithic runtime into a layered, standalone-ready package structure with stable facades and clear ownership boundaries.
- Assessment:
  - do not attempt `T06` as a single cut
  - only `supervisor.py` has been materially decomposed so far; `normalize.py`, `reporting.py`, `orchestrate.py`, and `simulate.py` remain large shells, and `contract.py` is still structurally overloaded
  - current structural hotspot spans `normalize.py` (`7243` lines), `reporting.py` (`5484` lines), `orchestrate.py` (`3528` lines), `simulate.py` (`2217` lines), `supervisor.py` (`1326` lines), and `contract.py` (`3512` lines)
  - continuing to split code without first fixing the target directory topology would likely turn `controller/` into a second monolith directory
- Target structure for completion:
  - `domain/` for investigation, matching, evidence, and stage-policy semantics
  - `application/` for normalize/reporting/orchestration/simulation/supervisor use cases
  - `adapters/` for filesystem, OpenClaw, fetch bridges, caches, and archive integrations
  - `cli/` for thin command surfaces only
  - root-level legacy modules kept temporarily as facades during migration, then reduced or removed
- Refactor slices:
  - `T06.1 Target Package Topology And Migration Guardrails` (`completed`)
  - scope:
    - finalize the target package layout and migration rules in architecture docs
    - establish the rule that new long-lived refactor destinations are `domain/`, `application/`, `adapters/`, and `cli/`, not further expansion of `controller/`
    - define which legacy root modules remain as temporary compatibility facades during migration
  - acceptance:
    - target structure and migration rules are documented clearly enough to guide all later slices
    - next slices have explicit ownership and destination packages
  - outcome:
    - architecture docs now lock the target layered package layout, migration map, and the rule that `controller/` is transitional rather than permanent
    - importable package skeletons now exist for `domain/`, `application/`, `adapters/`, and `cli/`, so later slices have concrete destinations instead of only planned directories
    - `controller` package metadata now explicitly marks it as a transition zone, and topology regression coverage now locks these package boundaries
    - repository test suite passes with `44` tests
  - `T06.2 Shared Foundations And Adapter Base Extraction` (`completed`)
  - scope:
    - extract duplicated `json / hashing / round-id / mission / policy bridge / artifact / filesystem` helpers out of the large modules
    - move them into stable shared modules under the target layered structure
  - acceptance:
    - public entrypoints stay stable
    - duplicated helper families shrink materially
    - full repository tests pass
  - outcome:
    - added stable shared foundations under `domain/` and `adapters/` for text normalization, round-id handling, contract bridging, filesystem/json I/O, hashing, and mission/run-path access
    - `normalize.py`, `reporting.py`, `orchestrate.py`, and `contract.py` now consume those shared foundations instead of carrying their own duplicate low-level helper blocks
    - transitional controller shims now point at the new shared foundations for IO and contract-policy bridging, reducing pressure to keep growing `controller/` as a second architecture
    - regression coverage now locks shared foundation behavior and compatibility shims, and the repository test suite passes with `48` tests
  - `T06.3 Normalize Domain And Application Split` (`planned`)
  - sub-slices:
    - `T06.3a Normalize Domain Semantics Extraction` (`completed`)
    - scope:
      - move public-claim shaping, observation tagging, metric-family semantics, and shared normalization-domain helpers out of `normalize.py`
      - land them in `domain/` and make `normalize.py` consume them as a facade dependency
    - acceptance:
      - domain-level normalize semantics become directly importable outside `normalize.py`
      - existing normalization and benchmark regressions stay green
    - outcome:
      - added `domain/normalize_semantics.py` as the shared home for public-claim scope shaping, observation leg tagging, metric-family semantics, and claim-vs-observation assessment rules
      - `normalize.py` now consumes and compatibility-reexports those semantics instead of keeping the primary implementation inline, removing a large pure-domain block from the monolith without changing public entrypoints
      - added direct regression coverage for the extracted domain module, and the repository test suite now passes with `51` tests
    - `T06.3b Normalize Source Pipeline Extraction` (`planned`)
    - scope:
      - move public/environment source normalization pipelines and cached wrappers into `application/`
    - acceptance:
      - source normalization flows become application services rather than top-level monolith code
      - source normalization regressions stay green
    - `T06.3c Normalize Storage And Match-Prep Extraction` (`planned`)
    - scope:
      - move normalize cache/db/manifest support and match-prep builders into `adapters/` plus `application/`
    - acceptance:
      - storage/match-prep helpers stop living primarily in `normalize.py`
      - matching regressions stay green
    - `T06.3d Normalize Facade Cleanup` (`planned`)
    - scope:
      - reduce `normalize.py` to orchestration-level facade and CLI surface only
    - acceptance:
      - `normalize.py` is no longer the main implementation home
      - full repository tests pass
  - acceptance:
    - `normalize.py` becomes an orchestrating compatibility facade rather than the main implementation body
    - benchmark and normalization regressions stay green
  - `T06.4 Reporting And Decision Pipeline Split` (`planned`)
  - scope:
    - split `reporting.py` into packet rendering, round-state aggregation, report drafting, decision promotion, and bundle-validation concerns
  - acceptance:
    - packet rendering and decision lifecycle become separately testable
    - reporting and bundle-validation regressions stay green
  - `T06.5 Orchestration, Supervisor, And Simulation Application Split` (`planned`)
  - scope:
    - split fetch-plan/orchestration services away from adapters and CLI glue in `orchestrate.py`
    - continue shrinking `supervisor.py` so it becomes a thin lifecycle facade over extracted services
    - split `simulate.py` into application workflow plus thin command surface
  - acceptance:
    - orchestration, supervision, and simulation flows have clearer application-service boundaries
    - root modules become visibly thinner
    - full repository tests pass
  - `T06.6 Contract, CLI, And Legacy Facade Cleanup` (`planned`)
  - scope:
    - separate schema validation, scaffolding, and command-surface concerns in `contract.py`
    - finish CLI boundary cleanup and reduce root-level facades where safe
    - normalize import paths so the final package layout is coherent for standalone extraction
  - acceptance:
    - controller lifecycle logic is clearer and extraction toward standalone `domain / application / adapters / cli` is materially easier
    - remaining top-level modules are either thin facades or intentionally preserved public entrypoints
    - full repository tests pass
- Execution rule for `T06`:
  - activate only one slice at a time
  - each slice should first move boundaries, then perform only the minimum compatibility edits needed to keep behavior stable
  - each slice must land with code changes, tests, and a short outcome note together
  - full repository tests are the regression gate after every slice
- Acceptance criteria:
  - clearer boundaries across `domain / application / adapters / cli`
  - legacy monolith modules stop being the main implementation home
  - no behavior regressions against the benchmark suite

## Current Task Notes

- Active task: `none`
- Next planned task: `T06.3b Normalize Source Pipeline Extraction`
- Working rule reaffirmed: structure first, then extraction; do not start another refactor slice until the active one is fully closed.

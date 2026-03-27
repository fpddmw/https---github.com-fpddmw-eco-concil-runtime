# Runtime Task Board History (`T01` To `T06`)

This file keeps a compact record of the completed planning eras that were removed from the active task board on `2026-03-28`.

For the fully detailed per-slice execution notes, use repository history before the board reset that introduced the new forward-only `runtime-task-board.md`.

## Completed Themes

### T01 Claim-Local Scope And Public Claim Legging

- Separated mission scope from claim-local scope.
- Prevented mission-fallback public claims from being treated as direct local evidence.
- Propagated `hypothesis_id` and `leg_id` through claim-side artifacts.

### T02 Observation Leg Attribution And Physical-Leg Review Filtering

- Added stable physical-leg attribution when the investigation plan makes it unambiguous.
- Made investigation review prefer explicit observation leg tags over broad metric-family fallback.

### T03 Benchmark Expansion For Investigation Quality

- Added broader scenario coverage across smoke, flood, heat, policy, contradiction, and negative-match paths.
- Locked more hypothesis, leg, and match outcomes in regression expectations.

### T04 Investigation Planning And Retrieval Upgrade

- Added explicit `fetch_intents`, `gap_types`, `history_query`, and `alternative_hypotheses`.
- Upgraded case-library retrieval from shallow lexical matching toward structured overlap scoring.

### T05 Tamper-Evident Audit Chain

- Added append-only receipts, chained hashes, content-addressed snapshots, and deterministic validation.
- Covered import, fetch, normalize, match, and decision boundaries.

### T06 Structural Decomposition After Semantics Stabilize

- Landed the first-stage layered split into `domain/`, `application/`, `adapters/`, and `cli/`.
- Reduced major root modules into thinner facades and moved primary workflow ownership into extracted modules.
- Finished the phase with a recorded `110` passing tests baseline.

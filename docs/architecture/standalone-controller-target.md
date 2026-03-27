# Standalone Controller Target

`eco-council-runtime` is being prepared for extraction into a standalone council-controller repository that will sit parallel to the skills repository.

## Editing rule

When adding new control-plane logic, do not optimize for the current skills-repo layout.

Prefer structures that will survive extraction with minimal rewrite:

- `domain/`: stage machine, evidence semantics, investigation policy, matching policy
- `application/`: round lifecycle services and audited use cases
- `adapters/`: filesystem, OpenClaw, fetch-runner bridge, archive stores
- `cli/`: thin argument parsing and command dispatch

## Target package layout

The intended end state for controller-side code is:

- `src/eco_council_runtime/domain/`
  - mission/stage policy
  - investigation semantics
  - matching semantics
  - evidence/review semantics
- `src/eco_council_runtime/application/`
  - normalize use cases
  - reporting and decision lifecycle services
  - orchestration services
  - simulation services
  - supervisor round-lifecycle services
- `src/eco_council_runtime/adapters/`
  - filesystem paths and JSON stores
  - OpenClaw integration
  - fetch-runner bridges
  - sqlite/cache/archive helpers
- `src/eco_council_runtime/cli/`
  - thin command parsers and dispatchers only

Root-level legacy modules should eventually become thin compatibility facades or disappear:

- `normalize.py`
- `orchestrate.py`
- `reporting.py`
- `simulate.py`
- `supervisor.py`
- `contract.py`

## Migration map

During `T06`, treat the current large modules as migration shells, not the final home for new logic:

- `normalize.py`
  - move source normalization, claim shaping, observation tagging, cache/db helpers, and match-prep helpers into `domain/`, `application/`, and `adapters/`
  - keep `normalize.py` as a compatibility facade until the split stabilizes
- `reporting.py`
  - move report packet rendering, round-state aggregation, decision promotion, and bundle validation into `application/` plus supporting `domain/` helpers
  - keep `reporting.py` as a facade during migration
- `orchestrate.py`
  - move fetch-plan construction, execution services, and stateful round orchestration into `application/`
  - move command construction and runtime integrations into `adapters/`
- `simulate.py`
  - move simulation workflows into `application/`
  - keep CLI and argument plumbing out of the core implementation
- `supervisor.py`
  - continue shrinking toward a lifecycle/application facade plus CLI entrypoint
  - previously extracted `controller/` helpers should be migrated forward rather than expanded as the new permanent architecture
- `contract.py`
  - separate schema validation, scaffolding, and command-surface concerns
  - keep CLI behavior thin and explicit

## Transitional rule

`src/eco_council_runtime/controller/` is a useful intermediate extraction zone, but it is not the final architecture.

For the remainder of `T06`:

- do not keep growing `controller/` as a new catch-all directory
- new long-lived structural code should land in `domain/`, `application/`, `adapters/`, or `cli/`
- existing `controller/` modules may remain temporarily as compatibility shims or already-extracted internals until they are migrated into the target package layout

## Anti-patterns

Avoid reintroducing these patterns:

- packing OpenClaw adapter logic back into `supervisor.py`
- packing run-directory path rules back into command handlers
- coupling new control logic to the legacy in-repo eco-council control directories
- using skills-repo conventions as the main architectural constraint for controller code
- treating `controller/` as the permanent substitute for the intended layered package structure

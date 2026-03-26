# Standalone Controller Target

`eco-council-runtime` is being prepared for extraction into a standalone council-controller repository that will sit parallel to the skills repository.

## Editing rule

When adding new control-plane logic, do not optimize for the current skills-repo layout.

Prefer structures that will survive extraction with minimal rewrite:

- `domain/`: stage machine, evidence semantics, investigation policy, matching policy
- `application/`: round lifecycle services and audited use cases
- `adapters/`: filesystem, OpenClaw, fetch-runner bridge, archive stores
- `cli/`: thin argument parsing and command dispatch

## Current intermediate structure

Until the full extraction is finished, new controller internals should land in:

- `src/eco_council_runtime/controller/constants.py`
- `src/eco_council_runtime/controller/io.py`
- `src/eco_council_runtime/controller/paths.py`
- `src/eco_council_runtime/controller/state_config.py`
- `src/eco_council_runtime/controller/openclaw.py`
- `src/eco_council_runtime/controller/agent_turns.py`
- `src/eco_council_runtime/controller/cli.py`

`src/eco_council_runtime/supervisor.py` should continue shrinking toward orchestration-only behavior.

## Anti-patterns

Avoid reintroducing these patterns:

- packing OpenClaw adapter logic back into `supervisor.py`
- packing run-directory path rules back into command handlers
- coupling new control logic to the legacy in-repo eco-council control directories
- using skills-repo conventions as the main architectural constraint for controller code

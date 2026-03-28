# eco-council-runtime

Standalone runtime for eco-council investigation, control-plane orchestration, normalization, matching, and reporting.

## Scope

This package is the beginning of the eco-council control-plane migration out of the skills repository layout.

It currently contains:

- standalone contract, normalization, orchestration, reporting, supervisor, simulation, and archive CLIs
- packaged assets, examples, SQL schemas, and docs needed by those CLIs
- runtime path resolution that can work while the project still lives under the current workspace
- first-pass causal-chain scaffolding via `shared/investigation_plan.json`

## Current runtime shape

The runtime still calls fetch skills from the external skills repository, but its own control components no longer depend on the legacy in-repo eco-council control directories.

During OpenClaw provisioning, the runtime now projects the required detached eco-council skills into the isolated OpenClaw `managedSkillsDir`. That lets fixed role agents use repository skills after this runtime is extracted into its own standalone repository.

The causal-chain upgrade is started as an explicit investigation artifact:

- each scaffolded round now emits `shared/investigation_plan.json`
- source-selection packets include that investigation plan
- moderator matching, investigation-review, and decision packets include that investigation plan
- matching materialization now stops at a moderator-owned `investigation-review` stage before expert reports are generated

## Controller structure

This repository copy is being reorganized as a future standalone council-controller project, not as a long-term in-repo skill.

The current controller-oriented modules live under `src/eco_council_runtime/controller/`:

- `constants.py`: stable stage, role, and workflow constants
- `io.py`: atomic file IO and subprocess helpers
- `paths.py`: run-directory and artifact layout helpers
- `state_config.py`: historical-context and archive configuration
- `openclaw.py`: OpenClaw workspace/provisioning adapter
- `agent_turns.py`: turn resolution, prompt embedding, and OpenClaw turn execution
- `cli.py`: compatibility wrapper that re-exports the supervisor parser from `src/eco_council_runtime/cli/supervisor_cli.py`

When extending the control plane, prefer adding logic to these controller submodules or to future `domain/`, `application/`, and `adapters/` packages rather than expanding monolithic entry files.

## CLI entrypoints

From a source checkout, run CLIs with `PYTHONPATH=src python3 -m eco_council_runtime.<module> ...`.

After installation, prefer the console scripts exposed in `pyproject.toml`, for example `eco-council-supervisor` and `eco-council-reporting`.

## Useful local commands

```bash
PYTHONPATH=src python3 -m eco_council_runtime.contract scaffold-run-from-mission \
  --mission-input assets/contract/examples/mission.json \
  --run-dir /tmp/eco_runtime_demo \
  --pretty

PYTHONPATH=src python3 -m eco_council_runtime.supervisor init-run \
  --run-dir /tmp/eco_runtime_demo \
  --mission-input assets/contract/examples/mission.json \
  --skills-root /path/to/skills \
  --no-provision-openclaw \
  --pretty
```

## Next migration targets

- move remaining control logic away from monolithic copied modules into package submodules and then into domain/application/adapters boundaries
- deepen causal-leg reasoning so composite observations and cross-region links are less heuristic and more evidence-led
- reduce residual prompt wording and repo-coupling that still assumes the old workflow shape

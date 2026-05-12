---
name: link-source-acquisition-execution
description: Link an agent-authored source-acquisition proposal to fetch receipts, normalized signal refs, and execution artifacts while preserving the proposal as descriptive lineage rather than evidence acceptance.
---

# Link Source Acquisition Execution

## Core Goal
- Update one `source-acquisition-proposal` with execution lineage.
- Attach fetch receipt refs, normalization receipt refs, normalized signal refs, and artifact refs.
- Optionally move the proposal lifecycle status. If omitted, the skill derives a narrow status from linked refs: `normalized` when normalized signal refs are present, `receipt-only` when only normalization receipts are present, and `fetched` when only fetch receipts/artifacts are present.

## Boundaries
- Does not fetch, normalize, rank, or validate evidence.
- Does not decide that linked receipts or signals are accepted as council findings.
- The linked refs are audit/provenance material for agents to inspect and cite later.
- Status/ref shape is checked: `normalized` requires normalized signal refs; `receipt-only` cannot carry normalized signal refs; `fetched` cannot carry normalization refs.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- `object_id`
- `actor_role`

## Useful Optional Input
- `status`
- `status_rationale`
- `fetch_receipt_ref`
- `normalization_receipt_ref`
- `normalized_signal_ref`
- `artifact_ref`
- `evidence_ref`
- `lineage_id`
- `execution_link_json`
- `provenance_json`

## Output Contract
- Rewrites the canonical `source-acquisition-proposal` row with appended `execution_links`.
- Preserves existing evidence refs and lineage while appending supplied refs.
- Writes one runtime-local lineage artifact.
- `normalized` means signal-plane refs are linked; `fetched` and `receipt-only` remain audit lineage and should not be read as queryable normalized evidence.

## Scripts
- `scripts/link_source_acquisition_execution.py`

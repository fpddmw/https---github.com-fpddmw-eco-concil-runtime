---
name: materialize-claim-gap-action-cards
description: Optional-analysis helper that materializes non-ranking, non-scheduling claim-basis action cards from mission focus, council state, normalized signal counts, helper artifacts, source-attempt outcomes, open challenges, and readiness gaps.
---

# Eco Materialize Claim Gap Action Cards

## Core Goal
- Expose claim-basis gaps as actionable advisory cards.
- Keep cards subordinate to agent judgement and council objects.
- Make failed, zero, low-volume, and receipt-only acquisition attempts visible as recovery or source-limit prompts.
- Preserve the existing report path; this skill does not draft or validate a report.

## Triggering Conditions
- A moderator, investigator, challenger, or report-editor needs to see what basis would be needed before writing stronger public-policy situation-analysis claims.
- A source attempt is failed, zero-result, low-volume, or receipt-only and needs recovery or an explicit report boundary.
- A public semantic, formal comment, environment aggregate, interaction, or readiness claim might otherwise be written without the matching basis.

## Read/Write Contract
- Reads mission focus from `mission.json` and mission scaffold artifacts.
- Reads council objects from the deliberation DB.
- Reads normalized signal counts from `analytics/signal_plane.sqlite`.
- Reads helper artifacts from `analytics/` and reporting readiness artifacts when present.
- Reads round-liveness state for unresolved attempt and claim-strength context.
- Writes `run_dir/analytics/claim_gap_action_cards_<round_id>.json`.
- Syncs the artifact into the analysis plane as `claim-gap-action-card`.
- Does not write council objects, schedule skills, execute skills, rank sources, or change runtime state.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `output_path`
  - `low_volume_threshold`
  - `max_cards`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `analysis_sync`
- `board_handoff`
- The emitted artifact contains `action_cards`, each with:
  - `claim_gap`
  - `why_it_matters`
  - `candidate_skills`
  - `required_inputs`
  - `expected_artifacts`
  - `if_not_done_report_boundary`
  - `owner_role_suggestions`

## Agent Reasoning Guide
- Treat cards as claim-basis advisory prompts. They are not a queue, source ranking, score, gate, or automatic execution rule.
- Agent owners may adopt, reject, or rewrite a card through an evidence request, source-acquisition proposal, evidence-route assessment, finding, challenge disposition, readiness opinion, or round synthesis before downstream use.
- A zero-result fetch or zero-signal query is never evidence absence. It must become a recovery choice or a source-limit/report-boundary statement.
- GDELT tone can support media/document tone wording only; it must not become public sentiment basis.
- Report-editor use requires the card or its underlying gap to be carried by council/reporting basis.

## References
- `../../docs/openclaw-runtime-council-program-upgrade-plan.md`
- `../../docs/openclaw-project-overview.md`

## Scripts
- `scripts/materialize_claim_gap_action_cards.py`

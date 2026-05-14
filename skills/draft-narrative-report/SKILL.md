---
name: draft-narrative-report
description: Draft a narrative decision-support report from existing council, reporting, and frozen evidence-basis artifacts.
---

# Draft Narrative Report

## Core Goal
- Produce a reader-facing narrative decision-support report from existing council/reporting basis.
- Preserve claim boundaries, evidence refs, unresolved limitations, and audit trail.
- Keep report writing separate from investigation and source acquisition.
- Make the conclusion, reasoning path, limitations, and practical implications easy for a human reviewer to capture.

## Triggering Conditions
- A report-writing round has been opened, or moderator asks report-editor to draft a narrative from frozen/canonical basis.
- Final publication, council decision, expert reports, report-basis freeze, findings, bundles, or positions already exist.

## Read/Write Contract
- Reads `run_dir/reporting/final_publication_<basis_round_id>.json` when present.
- Reads `run_dir/reporting/council_decision_<basis_round_id>.json` and draft variant when present.
- Reads `run_dir/reporting/expert_report_*_<basis_round_id>.json` when present.
- Reads `run_dir/report_basis/frozen_report_basis_<basis_round_id>.json` when present.
- Reads `run_dir/analytics/public_discourse_sample_summary_<round_id>.json` when present or when supplied through `public_discourse_summary_path`.
- Reads DB-backed council objects for the basis round.
- Optionally reads `run_dir/analytics/public_discourse_sample_summary_<round_id>.json` or an explicit public discourse summary path as an advisory addendum source.
- Writes `run_dir/reporting/narrative_report_draft_<round_id>.json`.
- Writes `run_dir/reporting/narrative_report_draft_<round_id>.md`.

## Required Input
- `run_dir`
- `run_id`
- `round_id`
- Optional:
  - `basis_round_id`
  - `output_path`
  - `markdown_output_path`
  - `title`
  - `language` (`en`, `zh-Hans`, `zh`, `zh-CN`, `中文`)
  - `public_discourse_summary_path`
  - `max_items`

## Output Contract
- `status`
- `summary`
- `receipt_id`
- `batch_id`
- `artifact_refs`
- `canonical_ids`
- `warnings`
- `board_handoff`

## Agent Reasoning Guide
- This skill drafts prose only from recorded council/reporting objects. It must
  not introduce new facts, source claims, causal attributions, or unstated
  confidence upgrades.
- The report may be weak or bounded, but it must make limitations visible. Weak
  report drafting is not a substitute for moderator continuation when actionable
  investigation routes remain live.
- Evidence refs are an audit index, not evidence weights or source rankings.
- A good report is not a list of object summaries. It must transform recorded
  council objects into an explicit narrative chain:
  - the central judgment in one or two sentences;
  - the event or issue sequence in chronological or causal order;
  - the evidence lanes and the role each lane plays in the inference;
  - the point where the evidence stops being strong enough for a stronger claim;
  - the practical meaning for a human reviewer.
- Avoid repeated restatement. If the same object text supports multiple
  sections, summarize it once in the narrative account and then refer to its
  role in the evidence chain.
- Put audit refs at the end or after the relevant section in compact form. Do
  not let ids, runtime labels, or receipt lists dominate the main prose.
- Prefer connective prose over inventory prose: use language such as "This
  matters because", "That supports", "It still does not prove", and "The
  decision implication is". Do not merely enumerate sources.
- Avoid starting paragraphs with object-kind labels such as `council-decision`,
  `finding`, or `agent-position`. Use those ids in refs and audit trails, not as
  the main prose.
- Keep runtime details out of the main story unless they explain a material
  limitation. The reader should not need to understand the runtime schema to
  understand the report.
- When `language` is supplied, write generated headings, framing text, claim
  boundary, decision-use language, and known report-basis summaries in that
  language. Preserve source refs and raw source excerpts when translation would
  risk changing the recorded meaning.
- Recommendations must be report-boundary recommendations only: what the report
  can be used for, what it should not be used for, and what follow-up evidence
  would be needed for stronger claims. Do not invent policy advice that the
  council did not deliberate.
- When a public discourse summary is supplied, include it as a bounded
  sample-analysis addendum when a council object has carried it into report
  basis. It may refine public-discourse wording from visibility-only to
  sample-local issue/affect/source-narrative cues. If the summary supplies
  counts or fractions, report them as sample-local label structure, not as
  affected-population opinion, platform-wide sentiment, or representative
  public-opinion estimates. Make clear whether labels are non-exclusive and
  therefore should not be summed to 100%. Do not use public-discourse labels to
  strengthen physical source attribution or other conclusions beyond the
  recorded basis.

## Reader-Facing Section Requirements
- `Bottom Line` / `Executive Summary`: 2-4 sentences that state the most
  important conclusion, the evidence shape, and the claim boundary.
- `Key Takeaways`: short bullets that can be scanned without reading the full
  audit trail.
- `Narrative Account`: explain the event or issue as a sequence, not as a list
  of artifacts, using recorded basis only.
- `How The Evidence Fits`: connect evidence lanes to the conclusion without
  ranking or scoring sources. Explain what each lane contributes.
- `How The Council Closed`: explain why the moderator could close or why a weak
  report was still acceptable inside the stated boundary.
- `What Remains Unproven`: explain what is not proven and why in plain language.
- `Decision Use`: state how a human can responsibly use the report and what
  follow-up evidence is needed for stronger claims.
- `Audit Trail`: keep refs available without letting refs dominate the prose.

## Scripts
- `scripts/draft_narrative_report.py`

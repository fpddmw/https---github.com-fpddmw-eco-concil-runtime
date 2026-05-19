---
name: draft-narrative-report
description: Draft a narrative decision-support report from existing council, reporting, and frozen evidence-basis artifacts.
---

# Draft Narrative Report

## Core Goal
- Produce a reader-facing narrative decision-support report from existing council/reporting basis.
- Preserve claim boundaries, evidence refs, unresolved limitations, and audit trail.
- Keep report writing separate from investigation and source acquisition.
- Make the conclusion, evidence reasoning, limitations, and practical implications easy for a human reviewer to capture.
- Answer the user's mission request directly. The reader wants a professional reference report, not a runtime log.

## Triggering Conditions
- A report-writing round has been opened, or moderator asks report-editor to draft a narrative from frozen/canonical basis.
- Final publication, council decision, expert reports, report-basis freeze, findings, bundles, or positions already exist.

## Read/Write Contract
- Reads `run_dir/reporting/final_publication_<basis_round_id>.json` when present.
- Reads `run_dir/reporting/council_decision_<basis_round_id>.json` and draft variant when present.
- Reads `run_dir/reporting/expert_report_*_<basis_round_id>.json` when present.
- Reads `run_dir/report_basis/frozen_report_basis_<basis_round_id>.json` when present.
- Reads `run_dir/analytics/public_discourse_sample_summary_<round_id>.json` when present.
- If the report-writing round differs from the evidence basis round, also reads `run_dir/analytics/public_discourse_sample_summary_<basis_round_id>.json` when present.
- Reads DB-backed council objects for the basis round.
- Optionally reads an explicit public discourse summary path as an advisory addendum source.
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
- A good report is not a list of object summaries, a skill receipt summary, or
  a transcript of council procedure. It must read like an academic report or
  decision-support briefing:
  - the opening answer maps directly to the user's mission request;
  - before drafting prose, infer the report's argument map: central claim,
    chronological or causal development, supporting evidence, counter-boundary,
    and decision meaning;
  - write the main body as connected prose, not as visible template blocks that
    tell the reader "this section is for X";
  - foreground the real-world event or governance issue, then weave
    environmental/operational evidence, public-discourse semantics, formal
    records, and limits into one coherent line of reasoning;
  - show the council process briefly as method context only: what kinds of
    evidence were gathered, how they were checked, and why the report stops at
    the stated boundary;
  - preserve enough concrete evidence detail for professional review, including
    quantities, windows, source families, sample sizes, and known caveats when
    they are present in the recorded basis;
  - state the practical meaning for a human reviewer without inventing new
    policy advice.
- Avoid repeated restatement. If the same object text supports multiple
  sections, summarize it once in the narrative account and then refer to its
  role in the evidence chain.
- Put audit refs at the end or after the relevant section in compact form. Do
  not let ids, runtime labels, or receipt lists dominate the main prose.
- Prefer connective prose over inventory prose: use language such as "This
  matters because", "That supports", "It still does not prove", and "The
  decision implication is". Do not merely enumerate sources.
- Unless the user explicitly asks for a brief or one-page output, do not
  compress Chinese narrative reports into short executive summaries. A frozen
  case report should have enough length to develop the case: mission question,
  event development, concrete evidence detail, public-discourse semantics,
  reasoning links, council process summary, limitations, and follow-up evidence
  needs. The report may remain concise by academic standards, but it must not
  feel like an abstract.
- Avoid starting paragraphs with object-kind labels such as `council-decision`,
  `finding`, or `agent-position`. Use those ids in refs and audit trails, not as
  the main prose.
- Keep runtime details out of the main story unless they explain a material
  limitation. The reader should not need to understand the runtime schema to
  understand the report.
- Keep council/runtime procedure out of the main prose unless it is necessary
  to explain a material evidence limitation. The main report should answer:
  what happened, what the data show, what the governance record shows, what the
  public-discourse semantics show, what can be concluded, and what remains
  unsupported. Runtime receipts, round mechanics, and role descriptions belong
  in the audit trail or a compact source-basis note.
- Do not hard-code domain-specific story frames such as a particular city,
  pollutant, transport pathway, agency, data source, or disaster type into the
  generic template. Use such terms only when they appear in recorded council
  objects, report-basis artifacts, or a supplied public-discourse summary.
- When `language` is supplied, write generated headings, framing text, claim
  boundary, decision-use language, and known report-basis summaries in that
  language. In Chinese mode, do not pass through full English sentences in the
  reader-facing body. Keep stable identifiers, refs, acronyms, source names, and
  technical names such as `PM2.5`, `GDELT`, `USBR`, `FIRMS`, `YouTube`, or
  `Regulations.gov`, but render the surrounding prose in Chinese. If an English
  evidence excerpt cannot be safely translated, provide a bounded Chinese
  paraphrase or Chinese boundary note and leave the original traceable through
  audit refs.
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
- Treat formal comments and docket comments as institutional participation
  samples unless a representative sampling design is explicitly present.
- Treat source-narrative labels as cues for environmental review, not as proof
  of physical source, transport, or causal attribution.

## Reader-Facing Section Requirements
The JSON draft keeps stable internal sections for validation and publication,
but the reader-facing Markdown should not expose those internal sections as a
stack of disconnected blocks, especially in Chinese mode. Treat the internal
sections as source material for an article-like report:

- Start from the user's question and answer it directly.
- Develop the event or issue through time, causality, or thematic progression.
- Fold professional evidence detail into the prose instead of isolating it in
  inventory lists.
- Explain public-discourse semantics as part of the substantive case, not as an
  appendix detached from the argument.
- Briefly describe the council process only after the substantive evidence is
  clear, and only to explain how the evidence basis was formed and bounded.
- End with claim boundaries and responsible use.
- Keep audit refs available at the end without letting refs dominate the prose.

## Scripts
- `scripts/draft_narrative_report.py`

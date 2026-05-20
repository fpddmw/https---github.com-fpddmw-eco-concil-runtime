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
- A good report is a professional article with a visible argument. It is not a
  list of object summaries, a skill receipt summary, a transcript of council
  procedure, or a set of headings copied from the user's critique.
- Before drafting prose, infer the report's argument map:
  - mission question actually answerable from the frozen basis;
  - bounded central judgment;
  - evidence lanes and the specific role each lane plays;
  - interpretation that links those lanes into one line of reasoning;
  - counter-boundary: what stronger claims the evidence cannot support;
  - decision or research meaning.
- Reader-facing prose should normally develop that map as an academic article:
  - 摘要：研究问题、证据基础、主要发现、边界；
  - 关键词：case topic, source families, method terms;
  - 引言：议题背景、研究问题、本文贡献；
  - 材料与方法：frozen/reporting basis、source-family roles、sample scope、
    council validation method;
  - 结果：substantive findings only, not runtime chronology;
  - 讨论：interpretation, limits, alternative explanations, follow-up evidence;
  - 结论：bounded answer to the user mission;
  - 参考文献与审计索引：recorded source/audit refs, without inventing external
    bibliography.
- Show council process only as method context: what kinds of evidence were
  gathered, how they were bounded, and why the report stops where it does.
  Do not make "what the council did" the main report storyline unless the user
  explicitly asks for an operations audit.
- Preserve concrete evidence detail for professional review, including
  quantities, windows, source families, sample sizes, and known caveats when
  present in the recorded basis. Use those details as scope markers and
  reasoning supports; do not treat counts as representative conclusions unless
  the basis supplies a representative design.
- Avoid repeated restatement. If the same object text supports multiple
  sections, summarize it once in the narrative account and then refer to its
  role in the evidence chain.
- Put audit refs at the end or after the relevant section in compact form. Do
  not let ids, runtime labels, or receipt lists dominate the main prose.
- In reader-facing Markdown, keep raw object ids, receipts, signal ids, and
  `round-*` mechanics out of the body. The body should cite source families and
  data scopes in human terms; the full audit chain belongs in the final audit
  index or JSON artifact.
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
- Apply claim-sensitive soft obligations while writing. These are report-basis
  checks, not a fixed investigation agenda:
  - public emotion, opinion proportions, or main-concern claims need corpus,
    coverage audit, annotation, aggregation, explicit denominator, and
    sample-local representativeness limits;
  - formal comment issue or participation-structure claims need candidate
    audit, readable comment detail or attachment text, and formal issue
    classification or equivalent analysis;
  - environment trend, peak, or operating-status claims need
    `aggregate-environment-evidence` or explicit wording that the report is
    only using item-level examples;
  - source, causal, transport, or impact-chain claims need relation,
    fact-check, alternatives, or challenger-review basis; otherwise write
    compatibility cues or "still needs verification" language.

## Reader-Facing Section Requirements
The JSON draft keeps stable internal sections for validation and publication.
The reader-facing Markdown must not expose those internal sections as a stack
of disconnected blocks, especially in Chinese mode. Treat internal sections as
source material for an article-like report:

- Use academic sections such as "摘要", "引言", "材料与方法", "结果", "讨论",
  "结论", and "参考文献与审计索引" when they fit the case. Do not use mechanical
  labels like "总论点", "分论点", or "议会做了什么" as a substitute for
  argument.
- Start from the user's question and answer it directly in the abstract and
  conclusion, but do not quote the whole prompt as a separate block unless it
  clarifies scope.
- Develop the event or issue through thematic progression, chronology, or
  causal logic as supported by the basis.
- Fold professional evidence detail into prose. Short bullets are acceptable
  only for executive takeaways or explicit risk registers; they should not be
  the primary body of the report.
- Explain public-discourse semantics as part of the substantive case, not as an
  appendix detached from the argument.
- Briefly describe council process only after the substantive evidence is clear,
  and only to explain how the evidence basis was formed and bounded.
- End with responsible use and follow-up evidence needs.
- Keep audit refs available at the end without letting refs dominate the prose.
- For environmental-incident reports, the preferred article spine is:
  event chronology and intensity; contextual compatibility evidence; public and
  media semantics; discussion of risk communication and evidence boundaries;
  limitations; bounded conclusion. Do not write the report as repeated warnings
  about what cannot be claimed.

## Scripts
- `scripts/draft_narrative_report.py`

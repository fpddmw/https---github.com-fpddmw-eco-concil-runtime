# OpenClaw Source-Family Workflows

This document records the current fetch-skill orchestration semantics. It is a
capability map for agents, not a runtime-owned agenda.

## Boundary

Runtime exposes source-family workflows so agents can see common data-dependency
paths such as recon-to-table-pull and list-to-detail. These workflows do not rank
sources, assign weights, select evidence, or force a fixed round agenda.

The agent remains responsible for:

1. Choosing source families and queries.
2. Revising terms, windows, spatial bounds, or provider modes after weak results.
3. Deciding whether a follow-up skill is needed.
4. Recording source acquisition proposals, execution lineage, evidence positions,
   readiness opinions, and follow-up requests.

## Current Source-Family Workflows

| Family | Fetch skills | Multi-layer need |
| --- | --- | --- |
| GDELT public record | `fetch-gdelt-doc-search`, `fetch-gdelt-events`, `fetch-gdelt-mentions`, `fetch-gdelt-gkg` | DOC search is query-sensitive recon and article/timeline discovery; DOC also supports `tone` / `toneabs`, `timelinetone`, `tonechart`, and tone sorting for aggregate media/document tone. Events/Mentions/GKG are row-level follow-up surfaces for shared UTC windows. A failed or narrow DOC result should prompt query linting/rephrasing, DOC tone-mode checks, or table-window pulls, not abandonment. DOC tone, Events `AvgTone`, Mentions `MentionDocTone`, and GKG `V2Tone` are media/document tone cues, not public sentiment by themselves. |
| YouTube public discourse | `fetch-youtube-video-search`, `fetch-youtube-comments` | Video search discovers candidate videos. Comment fetch deepens public response evidence for selected video IDs. Search results without comment collection may be incomplete if discourse semantics, sample affect, or public-response issues are needed. |
| Regulations.gov policy comments | `fetch-regulationsgov-comments`, `audit-formal-comment-candidate-corpus`, `fetch-regulationsgov-comment-detail`, `fetch-regulationsgov-attachments`, `extract-document-text`, `normalize-regulationsgov-attachment-text`, `classify-formal-comment-issues` | Comment list fetch discovers IDs by docket/document/agency/time window. Candidate audit checks sample shape and drift cues. Detail fetch enriches selected comments. Attachment fetch/text extraction materializes readable comment text when inline text is absent or says “See Attached”. Formal issue annotation is sample-local advisory material, not stance distribution proof or public opinion. |
| Bluesky public discourse | `fetch-bluesky-cascade` | Search, author-feed, and thread/cascade modes are alternate paths inside the same skill. Agents should revise mode, handles, hashtags, or event terms before treating weak output as a source limit. |
| OpenAQ observations | `fetch-openaq` | Metadata discovery, API measurements, and S3 archive backfill are related paths. Empty measurement windows should prompt location/parameter/window review or archive backfill. |
| Environmental cross-check | `fetch-airnow-hourly-observations`, `fetch-open-meteo-air-quality`, `fetch-open-meteo-historical`, `fetch-open-meteo-flood`, `fetch-nasa-firms-fire`, `fetch-usgs-water-iv` | These are complementary evidence surfaces, not interchangeable proof channels. AirNow/OpenAQ expose station/provider observations; Open-Meteo exposes modeled weather, air-quality, or discharge context; FIRMS exposes active-fire detections; USGS IV exposes station hydrology in supported USGS coverage. Agents may cross-check receptor observations, modeled context, source-region fire activity, or hydrologic context. Runtime does not decide which source proves a claim. |
| USBR operational records | `fetch-usbr-rise`, `normalize-usbr-rise-environment-signals` | RISE operational data provides direct reservoir, release, storage, elevation, or related result rows. When item IDs are not known, use `fetch-usbr-rise discover-items` first to create a candidate item artifact, then fetch explicit item IDs with bounded windows and normalize the result rows. These records can ground operating-baseline observations, but they do not by themselves decide shortage severity, governance responsibility, operating compliance, or policy adequacy. API failures, wrong item IDs, missing metadata, or sparse windows are capability/source-surface limits, not evidence absence. |
| Official governance records | `fetch-usbr-project-records`, `fetch-federal-register-documents`, `fetch-epa-eis-records`, `normalize-official-governance-records` | Official project pages, Federal Register notices, EPA EIS metadata, Reclamation public-involvement pages, and attached PDFs/HTML records are direct governance-record surfaces. They should expose named documents, dates, agencies, comment periods, and artifact refs. They do not replace agent judgement about whether a document supports a governance claim. |

## Public Discourse Deepening

`public-discourse-sample-analysis` is an optional deepening lane, not a runtime
round type and not a source-selection rule. It may be recorded in a round brief,
evidence request, synthesis, or continuation focus when the council wants to
move beyond public visibility into sample-level issues, affect, media tone, or
source narratives.

Use these boundaries:

1. `social_sample_affect`
   - Use YouTube comments, Bluesky posts/replies, or formal public comments.
   - Item-level affect/stance labels should come from a bounded annotation
     worker such as `classify-public-discourse-affect`, not from fetch/normalize
     and not from unbounded social-investigator judgement.
   - Claims must stay inside the sampled platform/query/window.
   - Formal comments require a visible candidate corpus, readable text corpus,
     and annotation/aggregation basis before reporting issue or stance structure.
2. `gdelt_media_tone`
   - Use GDELT Events `AvgTone`, Mentions `MentionDocTone`, or GKG `V2Tone`.
   - Use `gdelt_doc_tone_aggregate` for DOC `timelinetone`, `tonechart`,
     `tone` / `toneabs`, or tone sort outputs.
   - These describe media/document tone, not public sentiment.
3. `source_narrative`
   - Use public/formal/media texts to record how sources are described or
     hypothesized.
   - Physical source attribution still requires environmental evidence.
4. `cross_source_comparison`
   - Compare media tone, public-response affect, formal comments, and source
     narratives as advisory cues.
   - Do not infer representativeness, causal truth, or source proof from overlap.

## Sample Proportion Semantics

The system may report sample-internal proportions when the denominator is explicit.
It must not call those proportions general public opinion unless the mission
provides a representative sampling design.

Allowed report forms:

1. “In this YouTube comment sample, `<label>` appears in X% of annotated comments.”
2. “In this formal public-participation sample, `<issue>` appears in X% of eligible
   submissions.”
3. “In this GDELT media/document sample, average tone is negative/positive within
   the sampled documents.”

Unsupported report forms without a representative design:

1. “X% of the public believes ...”
2. “Affected residents overall think ...”
3. “Public opinion is X% supportive / opposed.”
4. Mixing GDELT media rows, YouTube comments, and formal comments into one public
   denominator.

Required proportion metadata:

1. `sample_definition`
2. `source_family`
3. `source_skill`
4. `text_unit`
5. `eligible_signal_count`
6. `annotated_signal_count`
7. `label_family`
8. `labels_are_not_mutually_exclusive`
9. `representativeness_limits`

For formal comment samples, reports should additionally record:

1. candidate corpus/audit reference or explain why the sample is exploratory.
2. detail/attachment-text coverage, including `requires-attachment-text` or
   `text-extraction-limited` flags.
3. annotation basis, such as `classify-formal-comment-issues` or an approved
   aggregation artifact.
4. a boundary that formal participation samples are institutional records, not
   general public-opinion samples.

Desired optional-analysis surfaces for this lane include corpus materialization,
sample coverage audit, annotation-worker classification, annotation aggregation,
GDELT tone enrichment, cross-source comparison, and report handoff. Their outputs
remain advisory until a council agent cites them in a finding, evidence bundle,
challenge, readiness opinion, synthesis, or report-basis object.

## Acquisition Attempt Review

`failed`, `blocked`, `receipt-only`, `executed` without normalized signal refs,
and zero-signal fetch attempts are not terminal research conclusions. Before a
source family is abandoned, the source owner should record one of these paths:

1. Revise query syntax or terms.
2. Broaden or narrow time, space, parameter, or provider-mode constraints.
3. Use same-family follow-up skills.
4. Switch to a different source family.
5. Document an explicit source-limit rationale.

The preferred surfaces are `submit-agent-position`, `submit-readiness-opinion`,
`submit-evidence-request`, `submit-source-acquisition-proposal`, and moderator
`submit-round-synthesis`.

This should stay lightweight. Agents do not need to fill a long form for every
skill call. The required discipline appears only when a tool result is about to
support a negative or limiting claim:

> Under `<skill>` with `<query/window/bbox/provider-mode>`, this attempt returned
> `<zero/failed/receipt-only>`. This does not rule out `<untried routes>`; next I
> will `<revise/switch/ask moderator/bound the claim>`.

The goal is to protect agent autonomy by keeping attention on investigation
rather than compliance paperwork, while preventing a tool misuse from being
converted into a confident council conclusion.

## Signal Plane Reads

Normalize and query skills are also limited acquisition surfaces:

1. Normalize writes lineage, artifact refs, ingest receipts, and DB-backed signal
   rows. No-row normalization output should first be read as artifact/schema,
   mapping, allowlist, parser, or source-pairing scope, not as proof that source
   evidence is absent or useless.
2. Query reads only visible DB/archive/board state. Empty query output should
   first be read as run/round, `round_scope`, filter, normalization, archive
   import, or provenance visibility scope, not as proof that real-world evidence
   does not exist.
3. Query results become council evidence only when an agent carries the returned
   item-level `evidence_refs` and `evidence_basis` into a finding, evidence
   bundle, challenge, proposal, readiness opinion, synthesis, or report-basis
   object.

## Skill Use Cards

Generated role surfaces expose a `skill_use_card` for fetch skills. The card is
a compact reasoning guide, not an agenda. It records:

1. what the skill can observe;
2. what the skill cannot prove;
3. preflight checks such as linting, metadata, availability, or dry-run;
4. what zero or failed output can and cannot mean;
5. same-family follow-up skills where the source family has multiple layers.

Examples:

- GDELT DOC Search has both article reconnaissance and DOC tone aggregate
  modes. It is not the raw Events/Mentions/GKG layer, and `domainis:` is a URL
  filter rather than an official-record category.
- GDELT tone fields are media/document tone cues. They should be kept separate
  from YouTube/Bluesky/formal-comment sample affect.
- NASA FIRMS requires product/date compatibility checks. NRT products are not a
  safe default for historical cases; availability should be checked before zero
  rows affect source-attribution reasoning.
- YouTube and Regulations.gov are multi-step families. Search/list stages
  discover candidates; comment/detail stages provide deeper evidence when
  discourse or full-text semantics matter.
- AirNow/Open-Meteo/USGS environmental skills have different observation
  surfaces. Station observations, modeled grid fields, and hydrology discharge
  products should not be substituted for each other without stating the
  coverage limitation.
- Bluesky search is query-sensitive and not an exhaustive discourse universe.
  Weak output should trigger alternate terms, author/feed/list modes, or an
  explicit source-limit rationale.

## Closing Rule

The closing checklist may assist the moderator, but it must not sort evidence or
fix the agenda. If unresolved refs or nonproductive acquisition attempts remain,
the moderator must either open a continuation round for actionable follow-up or
record an explicit non-continuation rationale after source-owner reflection.

`no-actionable-path` is procedurally unsupported while failed/blocked/receipt-only,
executed-without-normalized-refs, or zero-signal attempts lack this reflection.

Weak or bounded reports remain allowed, but they require explicit claim strength,
limitations, unresolved refs, and non-continuation rationale. See
`docs/openclaw-claim-strength-obligations.md`.

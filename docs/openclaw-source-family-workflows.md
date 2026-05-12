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
| GDELT public record | `fetch-gdelt-doc-search`, `fetch-gdelt-events`, `fetch-gdelt-mentions`, `fetch-gdelt-gkg` | DOC search is query-sensitive recon and article/timeline discovery. Events/Mentions/GKG are row-level follow-up surfaces for shared UTC windows. A failed or narrow DOC result should prompt query linting/rephrasing or table-window pulls, not abandonment. |
| YouTube public discourse | `fetch-youtube-video-search`, `fetch-youtube-comments` | Video search discovers candidate videos. Comment fetch deepens public response evidence for selected video IDs. Search results without comment collection may be incomplete if discourse semantics are needed. |
| Regulations.gov policy comments | `fetch-regulationsgov-comments`, `fetch-regulationsgov-comment-detail` | Comment list fetch discovers IDs by docket/document/agency/time window. Detail fetch enriches selected comments and attachments when list rows are insufficient. |
| Bluesky public discourse | `fetch-bluesky-cascade` | Search, author-feed, and thread/cascade modes are alternate paths inside the same skill. Agents should revise mode, handles, hashtags, or event terms before treating weak output as a source limit. |
| OpenAQ observations | `fetch-openaq` | Metadata discovery, API measurements, and S3 archive backfill are related paths. Empty measurement windows should prompt location/parameter/window review or archive backfill. |
| Environmental cross-check | `fetch-airnow-hourly-observations`, `fetch-open-meteo-air-quality`, `fetch-open-meteo-historical`, `fetch-open-meteo-flood`, `fetch-nasa-firms-fire`, `fetch-usgs-water-iv` | These are complementary direct evidence surfaces. Agents may cross-check receptor observations, model/weather context, source-region fire activity, or hydrologic context. Runtime does not decide which source proves a claim. |

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

## Skill Use Cards

Generated role surfaces expose a `skill_use_card` for fetch skills. The card is
a compact reasoning guide, not an agenda. It records:

1. what the skill can observe;
2. what the skill cannot prove;
3. preflight checks such as linting, metadata, availability, or dry-run;
4. what zero or failed output can and cannot mean;
5. same-family follow-up skills where the source family has multiple layers.

Examples:

- GDELT DOC Search is article/timeline reconnaissance. It is not the raw
  Events/Mentions/GKG layer, and `domainis:` is a URL filter rather than an
  official-record category.
- NASA FIRMS requires product/date compatibility checks. NRT products are not a
  safe default for historical cases; availability should be checked before zero
  rows affect source-attribution reasoning.
- YouTube and Regulations.gov are multi-step families. Search/list stages
  discover candidates; comment/detail stages provide deeper evidence when
  discourse or full-text semantics matter.

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

# OpenClaw Claim-Strength Obligations

This document records the current closing semantics for report strength. It is a
thin governance surface, not a topic taxonomy or investigation script.

## Boundary

Runtime may require the council to make report boundaries explicit. Runtime must
not decide whether evidence is sufficient, assign source weights, sort evidence,
or require a fixed source family for a topic.

The council remains responsible for:

1. Choosing whether a statement is descriptive, relational, or causal/source-attribution.
2. Combining and accepting evidence.
3. Deciding whether unresolved refs still have actionable investigation paths.
4. Recording limitations when issuing a weak or bounded report.

Report-facing `claim_slots` are mission-driven questions to be answered, not a
fixed topic template and not a list of conclusions the system is expected to
prove. A sufficiency review may help the council decide which claim slots are
supportable or must be downgraded, but it is not a runtime truth mechanism and
does not replace council acceptance.

For complex public-policy situation analysis, claim-strength obligations should
be attached to a council investigation program and to the active themes of each
round. A round can close only after the council records whether its active theme
obligations are supported, downgraded, scoped out, or carried into a named
supplemental investigation round. This is still advisory governance: runtime
does not decide truth, source weight, or public-policy validity.

## Weak Reports

Weak or bounded reports are allowed. They are useful when the council can provide
an honest, limitation-aware answer without claiming more than the record supports.

They are not allowed as a shortcut. A weak report should not be used merely
because a query failed, a fetch returned zero signals, or a source family was
tried once. Failed, blocked, receipt-only, executed-without-normalized-refs, and
zero-signal acquisition attempts still require source-owner reflection before the
moderator treats `no-actionable-path` as procedurally supportable.

## Strong Claims

When a report asserts relational, causal, transport, origin, or source-attribution
claims, the council should cite explicit DB-backed refs such as hypotheses,
findings, evidence bundles, normalized signals, or receipts. Challenger review or
continuation should remain visible before final closure.

Runtime does not judge whether those refs are enough. It only exposes the
obligation so moderator, investigators, challenger, and report editor have a
shared closing boundary.

For policy evaluation wording, the basis must be synthesized from supported fact
records, official actions, governance records, public/media/formal semantics,
and challenger-visible limitations. `policy_evaluation_basis` is therefore a
report synthesis boundary, not an independent data lane or a special source
family.

## Public Discourse And Tone Claims

Public discourse claims require explicit sample and source-family boundaries.

Allowed bounded forms include:

1. “In this YouTube comments sample, health-risk concern appears frequently.”
2. “In this Bluesky query sample, Canada wildfire narratives recur.”
3. “In this GDELT media/document sample, tone indicators are mostly negative.”
4. “GDELT media tone and social sample affect point in similar/different
   directions, within their respective samples.”

Unsupported upgrades include:

1. Treating YouTube, Bluesky, GDELT, or Regulations.gov samples as general public
   opinion without an explicit representative design.
2. Treating GDELT `AvgTone`, `MentionDocTone`, `V2Tone`, DOC `timelinetone`,
   DOC `tonechart`, `tone` / `toneabs`, or GCAM cues as public sentiment.
3. Treating public source narratives as physical source attribution.
4. Treating zero or sparse public-signal output as proof that a concern or
   narrative is absent.

When a report needs stronger language, the council should keep three lanes
separate:

1. `gdelt_media_tone` / `gdelt_doc_tone_aggregate`: media/document tone.
2. `social_sample_affect`: sample-level public expression from comments/posts.
3. `physical_source_attribution`: environmental evidence for actual origin or
   transport claims.

## Closing Surface

The round liveness surface now includes:

1. `claim_strength_obligations`
2. `review-claim-strength-and-report-boundary` in `closing_checklist.items`

These surfaces ask the moderator to record claim strength, limitations,
unresolved refs, and non-continuation rationale before closing with a weak report.
If live actionable follow-up remains, the moderator should request or open a
continuation round rather than treating the report as complete.

Supplemental rounds should be issue-specific. Prefer names and metadata such as
`round-003-public-semantic-supplement-01` with `round_mode=supplemental-investigation`
and `primary_focus_refs` pointing to the unresolved theme or review object, rather
than a generic next round that repeats the same acquisition uncertainty.

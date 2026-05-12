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

## Closing Surface

The round liveness surface now includes:

1. `claim_strength_obligations`
2. `review-claim-strength-and-report-boundary` in `closing_checklist.items`

These surfaces ask the moderator to record claim strength, limitations,
unresolved refs, and non-continuation rationale before closing with a weak report.
If live actionable follow-up remains, the moderator should request or open a
continuation round rather than treating the report as complete.

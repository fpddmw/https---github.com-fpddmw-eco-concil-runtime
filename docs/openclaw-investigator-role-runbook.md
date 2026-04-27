# OpenClaw Investigator Role Runbook

## Purpose

This runbook defines the WP3 default investigator loop:

`fetch / normalize -> query / lookup -> finding -> evidence bundle -> optional proposal`

Investigators do not advance phases. The moderator may use submitted findings and evidence bundles to frame transition requests, and the runtime/operator approval chain remains responsible for committing state transitions.

## Default Loop

1. Query DB-backed signals with the appropriate read surface:
   - `query-public-signals`
   - `query-formal-signals`
   - `query-environment-signals`
   - `query-normalized-signal`
   - `query-raw-record`
2. Use the returned item-level `evidence_refs` and `evidence_basis` directly.
3. Submit a `finding` with `submit-finding-record`.
4. Submit an `evidence-bundle` with `submit-evidence-bundle`.
5. Submit a `proposal` only after it cites the finding or evidence bundle with `--response-to-id` / `--lineage-id`.
6. Use `post-discussion-message` for normal discussion and `post-review-comment` for challenger/moderator review comments.

## Query Evidence Basis Fixture

Every signal query result should expose a basis object shaped like this:

```json
{
  "signal_id": "signal-example-001",
  "evidence_refs": [
    {
      "ref_kind": "normalized-signal",
      "object_kind": "public-discourse-signal",
      "signal_id": "signal-example-001",
      "round_id": "round-example-001",
      "plane": "public",
      "source_skill": "fetch-youtube-video-search",
      "signal_kind": "youtube-video",
      "artifact_path": "/abs/run/raw/youtube.json",
      "record_locator": "$.records[0]",
      "artifact_sha256": "sha256...",
      "artifact_ref": "/abs/run/raw/youtube.json:$.records[0]"
    }
  ],
  "evidence_basis": {
    "basis_object_id": "signal-example-001",
    "basis_object_kind": "public-discourse-signal",
    "source_signal_id": "signal-example-001",
    "source_provenance": {
      "source_skill": "fetch-youtube-video-search",
      "signal_kind": "youtube-video",
      "canonical_object_kind": "public-discourse-signal"
    },
    "data_quality": {
      "quality_flags": ["provider-field-normalized"],
      "research_judgement": "none"
    },
    "temporal_scope": {
      "published_at_utc": "2023-06-07T13:00:00Z",
      "observed_at_utc": "",
      "window_start_utc": "",
      "window_end_utc": "",
      "captured_at_utc": ""
    },
    "spatial_scope": {
      "latitude": null,
      "longitude": null,
      "bbox": {}
    },
    "coverage_limitations": [
      "Public discourse rows reflect the queried platform or media source only and are not a representative sample of affected communities."
    ]
  }
}
```

## Minimal Command Chain

```bash
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py submit-finding-record \
  --actor-role public-discourse-investigator \
  --run-dir <run_dir> --run-id <run_id> --round-id <round_id> \
  --agent-role public-discourse-investigator \
  --finding-kind public-discourse-finding \
  --title <finding_title> \
  --summary <finding_summary> \
  --rationale <rationale> \
  --confidence <0_to_1> \
  --target-kind normalized-signal \
  --target-id <signal_id> \
  --basis-object-id <signal_id> \
  --source-signal-id <signal_id> \
  --evidence-ref <artifact_ref_from_query> \
  --provenance-json '{"source":"query-public-signals"}'

python3 eco-concil-runtime/scripts/eco_runtime_kernel.py submit-evidence-bundle \
  --actor-role public-discourse-investigator \
  --run-dir <run_dir> --run-id <run_id> --round-id <round_id> \
  --agent-role public-discourse-investigator \
  --bundle-kind public-discourse-evidence-bundle \
  --title <bundle_title> \
  --summary <bundle_summary> \
  --rationale <rationale> \
  --confidence <0_to_1> \
  --target-kind finding \
  --target-id <finding_id> \
  --basis-object-id <signal_id> \
  --source-signal-id <signal_id> \
  --finding-id <finding_id> \
  --evidence-ref <artifact_ref_from_query> \
  --provenance-json '{"source":"query-public-signals"}'
```

## Proposal Rule

`submit-council-proposal` is not the default investigation record. Use it only after the finding or evidence bundle exists, and cite those objects:

```bash
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py run-skill \
  --actor-role public-discourse-investigator \
  --run-dir <run_dir> --run-id <run_id> --round-id <round_id> \
  --skill-name submit-council-proposal -- \
  --agent-role public-discourse-investigator \
  --proposal-kind investigator-follow-up \
  --rationale <proposal_rationale> \
  --confidence <0_to_1> \
  --target-kind finding \
  --target-id <finding_id> \
  --response-to-id <finding_id> \
  --lineage-id <evidence_bundle_id> \
  --evidence-ref <artifact_ref_from_query> \
  --provenance-json '{"source":"finding-and-bundle"}'
```

## Challenger Review

Challengers should use review comments and challenge tickets to reference evidence bundles explicitly:

```bash
python3 eco-concil-runtime/scripts/eco_runtime_kernel.py post-review-comment \
  --actor-role challenger \
  --run-dir <run_dir> --run-id <run_id> --round-id <round_id> \
  --author-role challenger \
  --review-kind evidence-bundle-review \
  --comment-text <review_comment> \
  --target-kind evidence-bundle \
  --target-id <evidence_bundle_id> \
  --response-to-id <finding_id> \
  --evidence-ref evidence-bundle:<evidence_bundle_id> \
  --provenance-json '{"source":"challenger-review"}'
```

Optional analysis skills remain approval-gated and are not part of this default investigator loop.


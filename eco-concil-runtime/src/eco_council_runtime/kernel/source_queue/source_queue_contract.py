from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_SELECTION_ROLES = ("social-investigator", "environmental-investigator")
SUPPORTED_ARTIFACT_CAPTURE_MODES = ("stdout-json", "stdout-text", "direct-file")
KNOWN_FETCH_SIDE_EFFECTS = (
    "reads-artifacts",
    "writes-artifacts",
    "reads-shared-state",
    "writes-shared-state",
    "network-external",
    "destructive-write",
)

MISSION_INPUT_SEMANTICS: dict[str, Any] = {
    "schema_version": "mission-input-semantics-v1",
    "meaning": (
        "A mission is a user-facing request envelope for starting a council run. "
        "It is not the moderator's investigation plan, not an evidence bundle, "
        "not a report basis, and not a factual attribution."
    ),
    "required_fields": ["schema_version", "run_id", "topic", "objective"],
    "request_text_semantics": (
        "request_text preserves the user's natural-language request when present; "
        "objective may mirror it for legacy compatibility."
    ),
    "optional_seed_fields": [
        "window",
        "region",
        "artifact_imports",
        "source_requests",
        "hypotheses",
        "source_governance",
    ],
    "seed_field_boundary": (
        "Optional seed fields are user/operator-provided starting context only. "
        "They do not narrow investigator autonomy or decide evidence acceptance."
    ),
    "scoping_rule": (
        "If the mission lacks a complete window and region, runtime keeps the run "
        "in scoping mode. Moderator and agents must submit investigation-plan, "
        "investigation-scope, round-brief, or evidence-request objects before "
        "evidence collection is treated as scoped."
    ),
}


def mission_input_semantics() -> dict[str, Any]:
    return json.loads(json.dumps(MISSION_INPUT_SEMANTICS, ensure_ascii=True))


def _source(
    *,
    role: str,
    family_id: str,
    family_label: str,
    layer_id: str,
    layer_label: str,
    tier: str,
    normalizer_skill: str,
    default_suffix: str = ".json",
    artifact_capture: str = "stdout-json",
    runtime_output_mode: str = "none",
    runtime_output_arg: str = "",
    runtime_default_args: list[str] | None = None,
    requires_anchor: bool = False,
    anchor_argument: str = "",
    anchor_source_skills: list[str] | None = None,
    auto_selectable: bool | None = None,
) -> dict[str, Any]:
    return {
        "role": role,
        "family_id": family_id,
        "family_label": family_label,
        "layer_id": layer_id,
        "layer_label": layer_label,
        "tier": tier,
        "normalizer_skill": normalizer_skill,
        "default_suffix": default_suffix,
        "artifact_capture": artifact_capture,
        "runtime_output_mode": runtime_output_mode,
        "runtime_output_arg": runtime_output_arg,
        "runtime_default_args": list(runtime_default_args or []),
        "requires_anchor": requires_anchor,
        "anchor_argument": anchor_argument,
        "anchor_source_skills": list(anchor_source_skills or []),
        "auto_selectable": bool(auto_selectable) if auto_selectable is not None else tier == "l1",
    }


SOURCE_CATALOG: dict[str, dict[str, Any]] = {
    "fetch-bluesky-cascade": _source(
        role="social-investigator",
        family_id="bluesky",
        family_label="Bluesky",
        layer_id="posts",
        layer_label="Posts",
        tier="l1",
        normalizer_skill="normalize-bluesky-cascade-public-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-gdelt-doc-search": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="doc-search",
        layer_label="Doc Search",
        tier="l1",
        normalizer_skill="normalize-gdelt-doc-public-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-gdelt-events": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="events",
        layer_label="Events Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-events-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-gdelt-mentions": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="mentions",
        layer_label="Mentions Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-mentions-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-gdelt-gkg": _source(
        role="social-investigator",
        family_id="gdelt",
        family_label="GDELT",
        layer_id="gkg",
        layer_label="GKG Export",
        tier="l1",
        normalizer_skill="normalize-gdelt-gkg-public-signals",
        artifact_capture="stdout-json",
        runtime_output_mode="dir",
        runtime_output_arg="--output-dir",
    ),
    "fetch-youtube-video-search": _source(
        role="social-investigator",
        family_id="youtube",
        family_label="YouTube",
        layer_id="video-search",
        layer_label="Video Search",
        tier="l1",
        normalizer_skill="normalize-youtube-video-public-signals",
        runtime_default_args=["--include-records", "--no-save-records"],
    ),
    "fetch-youtube-comments": _source(
        role="social-investigator",
        family_id="youtube",
        family_label="YouTube",
        layer_id="comments",
        layer_label="Comments",
        tier="l2",
        normalizer_skill="normalize-youtube-comments-public-signals",
        runtime_default_args=["--include-records", "--no-save-records"],
        requires_anchor=True,
        anchor_argument="--video-ids-file",
        anchor_source_skills=["fetch-youtube-video-search"],
        auto_selectable=False,
    ),
    "fetch-regulationsgov-comments": _source(
        role="social-investigator",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="comments",
        layer_label="Comment List",
        tier="l1",
        normalizer_skill="normalize-regulationsgov-comments-public-signals",
        runtime_default_args=["--include-records", "--no-save-response"],
    ),
    "fetch-regulationsgov-comment-detail": _source(
        role="social-investigator",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="comment-detail",
        layer_label="Comment Detail",
        tier="l2",
        normalizer_skill="normalize-regulationsgov-comment-detail-public-signals",
        runtime_default_args=["--include-records", "--no-save-response"],
        requires_anchor=True,
        anchor_argument="--comment-ids-file",
        anchor_source_skills=["fetch-regulationsgov-comments"],
        auto_selectable=False,
    ),
    "fetch-regulationsgov-attachments": _source(
        role="social-investigator",
        family_id="regulationsgov",
        family_label="Regulations.gov",
        layer_id="attachments",
        layer_label="Attachments",
        tier="l3",
        normalizer_skill="normalize-regulationsgov-attachment-text",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--manifest-output",
        runtime_default_args=["--output-dir", "{artifact_dir}"],
        requires_anchor=True,
        anchor_argument="--input-artifact",
        anchor_source_skills=["fetch-regulationsgov-comment-detail"],
        auto_selectable=False,
    ),
    "fetch-epa-eis-records": _source(
        role="social-investigator",
        family_id="official-governance",
        family_label="Official Governance Records",
        layer_id="epa-eis-records",
        layer_label="EPA EIS Records",
        tier="l1",
        normalizer_skill="normalize-official-governance-records",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-federal-register-documents": _source(
        role="social-investigator",
        family_id="official-governance",
        family_label="Official Governance Records",
        layer_id="federal-register-documents",
        layer_label="Federal Register Documents",
        tier="l1",
        normalizer_skill="normalize-official-governance-records",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-usbr-project-records": _source(
        role="social-investigator",
        family_id="official-governance",
        family_label="Official Governance Records",
        layer_id="usbr-project-records",
        layer_label="USBR Project Records",
        tier="l1",
        normalizer_skill="normalize-official-governance-records",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-usbr-rise": _source(
        role="environmental-investigator",
        family_id="usbr-operational-records",
        family_label="USBR Operational Records",
        layer_id="rise-results",
        layer_label="RISE Results",
        tier="l1",
        normalizer_skill="normalize-usbr-rise-environment-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-airnow-hourly-observations": _source(
        role="environmental-investigator",
        family_id="airnow",
        family_label="AirNow",
        layer_id="hourly-observations",
        layer_label="Hourly Observations",
        tier="l1",
        normalizer_skill="normalize-airnow-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-openaq": _source(
        role="environmental-investigator",
        family_id="openaq",
        family_label="OpenAQ",
        layer_id="stations",
        layer_label="Stations",
        tier="l1",
        normalizer_skill="normalize-openaq-observation-signals",
    ),
    "fetch-open-meteo-historical": _source(
        role="environmental-investigator",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="historical",
        layer_label="Historical Weather",
        tier="l1",
        normalizer_skill="normalize-open-meteo-historical-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-open-meteo-air-quality": _source(
        role="environmental-investigator",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="air-quality",
        layer_label="Air Quality",
        tier="l1",
        normalizer_skill="normalize-open-meteo-air-quality-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-open-meteo-flood": _source(
        role="environmental-investigator",
        family_id="open-meteo",
        family_label="Open-Meteo",
        layer_id="flood",
        layer_label="Flood",
        tier="l1",
        normalizer_skill="normalize-open-meteo-flood-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-usgs-water-iv": _source(
        role="environmental-investigator",
        family_id="usgs-water",
        family_label="USGS Water",
        layer_id="instantaneous-values",
        layer_label="Instantaneous Values",
        tier="l1",
        normalizer_skill="normalize-usgs-water-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
    "fetch-nasa-firms-fire": _source(
        role="environmental-investigator",
        family_id="nasa-firms",
        family_label="NASA FIRMS",
        layer_id="active-fire",
        layer_label="Active Fire",
        tier="l1",
        normalizer_skill="normalize-nasa-firms-fire-observation-signals",
        artifact_capture="direct-file",
        runtime_output_mode="file",
        runtime_output_arg="--output",
    ),
}

DEFAULT_FETCH_SKILL_USE_CARD: dict[str, Any] = {
    "card_version": "skill-use-card-v1",
    "semantics": (
        "A fetch skill retrieves or materializes raw evidence. It does not decide "
        "claim truth, source sufficiency, report readiness, or evidence acceptance."
    ),
    "before_using": [
        "State the evidence need in plain language before choosing parameters.",
        "Check provider mode, time coverage, spatial coverage, and required identifiers.",
        "Use dry-run, lint, metadata, availability, or config probes when the skill offers them.",
    ],
    "zero_or_failed_result_discipline": [
        "Treat zero, failed, blocked, or receipt-only output as an attempt result, not proof that the real-world evidence is absent.",
        "Before abandoning the route, consider revised terms, window, bbox, provider mode, same-family follow-up skills, or a different source family.",
        "If stopping, record the source-limit rationale and the claim boundary that remains unresolved.",
    ],
    "autonomy_boundary": (
        "This card is guidance for agent reasoning. It is not source ranking, "
        "weighting, or a fixed agenda."
    ),
}

SOURCE_USE_CARDS: dict[str, dict[str, Any]] = {
    "fetch-gdelt-doc-search": {
        "what_this_skill_is": (
            "GDELT DOC API article/timeline reconnaissance and DOC-level media/document tone queries over indexed web documents."
        ),
        "what_this_skill_is_not": (
            "It is not the raw Events, Mentions, or GKG table export layer; it is "
            "not an official-record classifier; domain filters are URL filters, "
            "not proof that a source is official or unofficial."
        ),
        "before_using": [
            "Lint agent-authored queries when syntax is complex or provider-specific operators are used.",
            "Prefer compact topical searches before exact-domain searches when the source universe is uncertain.",
            "Use DOC tone/timelinetone/tonechart only as media/document tone, not as public sentiment.",
            "Use source/title/url/snippet metadata to reason about source type instead of relying only on domain filters.",
        ],
        "zero_or_failed_result_discipline": [
            "A zero DOC result may mean the query, date window, domain filter, or DOC index path was too narrow.",
            "It does not mean GDELT Events, Mentions, or GKG have no relevant rows.",
            "It does not mean official or public records do not exist outside this DOC query.",
        ],
        "same_family_followups": [
            "fetch-gdelt-events",
            "fetch-gdelt-mentions",
            "fetch-gdelt-gkg",
        ],
        "common_traps": [
            "Using search-engine `site:` syntax instead of GDELT `domain:` or `domainis:`.",
            "Treating exact-domain zero rows as absence of official evidence.",
            "Treating DOC tone, Events AvgTone, Mentions MentionDocTone, or GKG V2Tone as public sentiment.",
            "Using one highly compound query where several compact searches would be more robust.",
        ],
    },
    "fetch-gdelt-events": {
        "what_this_skill_is": "GDELT 2.0 Events export-file retrieval for a UTC snapshot range.",
        "what_this_skill_is_not": "It is not a topical article search and does not itself classify or summarize events.",
        "before_using": [
            "Dry-run the historical UTC range and keep max-files bounded.",
            "Plan downstream normalization/filtering before drawing item-level conclusions.",
        ],
        "zero_or_failed_result_discipline": [
            "A missing or failed export pull is an acquisition issue for that range or cap.",
            "It is not evidence that no public record or event signal exists.",
        ],
        "same_family_followups": ["fetch-gdelt-doc-search", "fetch-gdelt-mentions", "fetch-gdelt-gkg"],
    },
    "fetch-gdelt-mentions": {
        "what_this_skill_is": "GDELT 2.0 Global Mentions export-file retrieval for a UTC snapshot range.",
        "what_this_skill_is_not": "It is not a standalone article search and does not by itself settle salience.",
        "before_using": [
            "Dry-run the historical UTC range and keep max-files bounded.",
            "Use after DOC or Events context when mention/source context is needed.",
        ],
        "zero_or_failed_result_discipline": [
            "A failed range pull is an acquisition limitation for that range or cap.",
            "It is not evidence that no mention context exists.",
        ],
        "same_family_followups": ["fetch-gdelt-doc-search", "fetch-gdelt-events", "fetch-gdelt-gkg"],
    },
    "fetch-gdelt-gkg": {
        "what_this_skill_is": "GDELT 2.0 GKG export-file retrieval for knowledge-graph rows in a UTC range.",
        "what_this_skill_is_not": "It is not the DOC article API and does not itself produce issue conclusions.",
        "before_using": [
            "Dry-run the historical UTC range and keep max-files bounded.",
            "Use after a scoped time window exists or when DOC query sensitivity is limiting evidence discovery.",
        ],
        "zero_or_failed_result_discipline": [
            "A failed GKG pull is a table acquisition limitation for that range or cap.",
            "It is not evidence that no topical public signal exists.",
        ],
        "same_family_followups": ["fetch-gdelt-doc-search", "fetch-gdelt-events", "fetch-gdelt-mentions"],
    },
    "fetch-nasa-firms-fire": {
        "what_this_skill_is": "NASA FIRMS area/csv active-fire detection retrieval for one bbox, source, and date window.",
        "what_this_skill_is_not": "It does not geocode place names, run country-scale scans, classify severity, or prove smoke transport by itself.",
        "before_using": [
            "Run check-config with availability probing when the date window is historical or source coverage is uncertain.",
            "Choose NRT or SP sources based on provider availability, not on the command template alone.",
            "For historical windows, expect SP products to be the usual candidate when availability covers the requested dates.",
        ],
        "zero_or_failed_result_discipline": [
            "Zero rows can mean the selected product does not cover the requested date, the bbox is too narrow, or the window/source is mismatched.",
            "Zero rows from an unavailable product are not evidence that no fires existed.",
            "If source attribution depends on this lane, revise product, bbox, or window before abandoning the route.",
        ],
        "common_traps": [
            "Using NRT products for historical cases without checking availability.",
            "Interpreting fire detections as transport causation without weather/receptor corroboration.",
        ],
    },
    "fetch-youtube-video-search": {
        "what_this_skill_is": "YouTube video discovery for agent-authored queries and publication windows.",
        "what_this_skill_is_not": "It is not a public-discourse conclusion, does not fetch comment language, and does not by itself form a public-response corpus.",
        "zero_or_failed_result_discipline": [
            "Weak search results may reflect query wording, channel scope, language, or date filters.",
            "If discourse semantics matter, selected videos usually require fetch-youtube-comments follow-up.",
        ],
        "same_family_followups": ["fetch-youtube-comments"],
    },
    "fetch-youtube-comments": {
        "what_this_skill_is": "YouTube comment/reply retrieval for selected video IDs.",
        "what_this_skill_is_not": "It does not discover videos by itself and cannot represent platform-wide discourse.",
        "before_using": ["Ground video IDs in video-search artifacts or another explicit source."],
        "zero_or_failed_result_discipline": [
            "Zero comments may reflect disabled comments, selected videos, or filters.",
            "It is not evidence that no YouTube discourse exists unless discovery and selection limits are explicit.",
        ],
    },
    "fetch-regulationsgov-comments": {
        "what_this_skill_is": "Regulations.gov comment-list discovery by docket/document/agency/time filters.",
        "what_this_skill_is_not": "It is not full comment-detail enrichment, not readable attachment text, and does not derive issue, stance, or concern labels.",
        "zero_or_failed_result_discipline": [
            "Zero list rows may reflect docket/document/agency or date-field constraints.",
            "If list rows are found and full text matters, use fetch-regulationsgov-comment-detail, then attachments/text extraction when inline text is absent.",
        ],
        "same_family_followups": ["fetch-regulationsgov-comment-detail"],
    },
    "fetch-regulationsgov-comment-detail": {
        "what_this_skill_is": "Regulations.gov detail retrieval for selected comment IDs.",
        "what_this_skill_is_not": "It does not discover the relevant docket or comment universe by itself.",
        "before_using": ["Ground comment IDs in list artifacts or another explicit source."],
        "zero_or_failed_result_discipline": [
            "A failed detail fetch is a limitation for selected IDs, not proof that the docket lacks relevant comments.",
        ],
        "same_family_followups": ["fetch-regulationsgov-attachments"],
    },
    "fetch-regulationsgov-attachments": {
        "what_this_skill_is": "Regulations.gov attachment metadata and file download for selected comments or attachment IDs.",
        "what_this_skill_is_not": "It does not extract document text, classify comment stance, or prove attachment content is absent when downloads fail.",
        "before_using": ["Ground attachments in comment-detail artifacts, attachment IDs, or another explicit selection record."],
        "zero_or_failed_result_discipline": [
            "A missing file URL or failed attachment download is a route limitation for selected IDs, not proof that the comment has no readable content.",
            "If attachment text matters, revise metadata route or use a text/OCR extraction path before abandoning the source-limit rationale.",
        ],
    },
    "fetch-epa-eis-records": {
        "what_this_skill_is": "EPA EIS Database result-table retrieval for official NEPA/EIS metadata rows.",
        "what_this_skill_is_not": "It is not an EIS adequacy review and does not prove legal sufficiency or policy responsibility.",
        "before_using": [
            "Use common-search pages for current official surfaces or pass an explicit EPA EIS Database search URL.",
            "Normalize with normalize-official-governance-records before DB-backed formal review.",
        ],
        "zero_or_failed_result_discipline": [
            "Zero rows may reflect common-search choice, stale search URL, provider HTML changes, pagination, or the database result cap.",
            "It is not proof that EIS records are absent.",
        ],
    },
    "fetch-federal-register-documents": {
        "what_this_skill_is": "FederalRegister.gov published-document metadata retrieval by term, agency, type, and publication date.",
        "what_this_skill_is_not": "It is not a legal interpretation engine and does not prove official-record completeness.",
        "before_using": [
            "Use provider agency slugs and compact terms; keep date and page caps explicit.",
            "Normalize with normalize-official-governance-records before DB-backed formal review.",
        ],
        "zero_or_failed_result_discipline": [
            "Zero rows may reflect agency slug, term, document type, publication-date, or page-cap constraints.",
            "It is not proof that official governance records are absent.",
        ],
    },
    "fetch-usbr-project-records": {
        "what_this_skill_is": "Direct fetch of supplied Bureau of Reclamation project pages and same-domain linked record URLs.",
        "what_this_skill_is_not": "It is not a full-site USBR search and does not rank pages or decide document completeness.",
        "before_using": [
            "Ground each URL in an explicit source request or agent-selected official project surface.",
            "Normalize with normalize-official-governance-records before DB-backed formal review.",
        ],
        "zero_or_failed_result_discipline": [
            "Sparse links may reflect the supplied URL, page design, extraction limits, or same-domain filtering.",
            "It is not proof that USBR official records are absent.",
        ],
    },
    "fetch-usbr-rise": {
        "what_this_skill_is": "USBR RISE JSON:API catalog item discovery and result retrieval for explicit operational time-series item IDs.",
        "what_this_skill_is_not": "It does not decide shortage severity, operating compliance, governance responsibility, or whether a candidate item should be adopted as report evidence.",
        "before_using": [
            "When item IDs are unknown, run discover-items with agent-defined place/parameter terms and cite the candidate artifact.",
            "Before result fetch, ground item IDs in a RISE item detail page, operator source request, or discover-items catalog artifact.",
            "Keep date windows, page caps, and item metadata limitations explicit.",
        ],
        "zero_or_failed_result_discipline": [
            "Zero discovery candidates may reflect terms, page caps, or provider catalog shape; revise terms or record a route assessment before abandoning the family.",
            "An incomplete catalog scan is a route-grounding attempt, not evidence absence.",
            "Zero rows may reflect item ID, date filters, provider latency, metadata limits, or page caps.",
            "It is not proof that USBR operational records are absent.",
        ],
    },
    "fetch-openaq": {
        "what_this_skill_is": "OpenAQ metadata, API measurement, or archive-backfill retrieval.",
        "what_this_skill_is_not": "It is not an exposure conclusion and does not infer coverage sufficiency.",
        "before_using": [
            "Use metadata discovery before measurements when location or parameter IDs are uncertain.",
            "Consider archive backfill when API windows do not cover the needed period.",
        ],
        "zero_or_failed_result_discipline": [
            "Zero measurements may reflect wrong location IDs, parameter IDs, window, or API/archive mode.",
        ],
    },
}

_GDELT_EXPORT_HINT = {
    "provider_modes": [
        {
            "mode": "latest",
            "time_coverage": "latest public GDELT export snapshot",
            "time_args": [],
        },
        {
            "mode": "range",
            "time_coverage": "historical UTC export snapshots available from GDELT file lists",
            "time_args": ["--start-datetime", "--end-datetime"],
        },
    ],
    "fetch_argument_templates": [
        ["fetch", "--mode", "range", "--start-datetime", "<YYYYMMDDHHMMSS>", "--end-datetime", "<YYYYMMDDHHMMSS>", "--dry-run"],
        ["resolve-latest"],
    ],
}

SOURCE_CAPABILITY_HINTS: dict[str, dict[str, Any]] = {
    "fetch-bluesky-cascade": {
        "provider_modes": [
            {
                "mode": "search",
                "time_coverage": "provider-indexed recent or historical posts available through Bluesky search",
                "time_args": ["--start-datetime", "--end-datetime"],
            },
            {
                "mode": "author-feed",
                "time_coverage": "provider-visible author feed items within the requested UTC window",
                "time_args": ["--start-datetime", "--end-datetime"],
            },
        ],
        "fetch_argument_templates": [
            ["fetch", "--source-mode", "search", "--query", "<query>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>"],
            ["fetch", "--source-mode", "author-feed", "--actor", "<handle-or-did>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>"],
        ],
    },
    "fetch-gdelt-doc-search": {
        "provider_modes": [
            {
                "mode": "doc-search",
                "time_coverage": "GDELT DOC API indexed documents for a relative timespan or absolute UTC interval",
                "time_args": ["--timespan", "--start-datetime", "--end-datetime"],
            }
        ],
        "fetch_argument_templates": [
            ["search", "--query", "<query>", "--mode", "artlist", "--format", "json", "--start-datetime", "<YYYYMMDDHHMMSS>", "--end-datetime", "<YYYYMMDDHHMMSS>", "--max-records", "<1-250>"],
            ["lint-query", "--query", "<query>", "--domain-is", "<example.gov>"],
            ["search", "--query", "<compact_query>", "--domain-is", "<example.gov>", "--domain-is", "<another.gov>", "--mode", "artlist", "--format", "json", "--start-datetime", "<YYYYMMDDHHMMSS>", "--end-datetime", "<YYYYMMDDHHMMSS>", "--max-records", "<1-250>", "--continue-on-query-error"],
        ],
    },
    "fetch-gdelt-events": _GDELT_EXPORT_HINT,
    "fetch-gdelt-mentions": _GDELT_EXPORT_HINT,
    "fetch-gdelt-gkg": _GDELT_EXPORT_HINT,
    "fetch-youtube-video-search": {
        "provider_modes": [
            {
                "mode": "search",
                "time_coverage": "YouTube Data API search index constrained by published-after/published-before when supplied",
                "time_args": ["--published-after", "--published-before"],
            }
        ],
        "fetch_argument_templates": [
            ["search", "--query", "<query>", "--published-after", "<YYYY-MM-DDTHH:MM:SSZ>", "--published-before", "<YYYY-MM-DDTHH:MM:SSZ>", "--max-results", "<count>", "--include-records", "--no-save-records"],
        ],
    },
    "fetch-youtube-comments": {
        "provider_modes": [
            {
                "mode": "comments",
                "time_coverage": "comments visible for supplied video ids, optionally filtered by UTC comment timestamps after fetch",
                "time_args": ["--start-datetime", "--end-datetime"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--video-id", "<video_id>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--include-records", "--no-save-records"],
            ["fetch", "--video-ids-file", "<path>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--include-records", "--no-save-records"],
        ],
    },
    "fetch-regulationsgov-comments": {
        "provider_modes": [
            {
                "mode": "comments",
                "time_coverage": "Regulations.gov API comments filtered by posted date or modified date arguments",
                "time_args": ["--start-datetime", "--end-datetime", "--start-date", "--end-date"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--filter-mode", "last-modified", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--include-records", "--no-save-response"],
            ["fetch", "--filter-mode", "posted", "--start-date", "<YYYY-MM-DD>", "--end-date", "<YYYY-MM-DD>", "--include-records", "--no-save-response"],
            ["fetch", "--comment-on-id", "<document_id>", "--include-records", "--no-save-response"],
        ],
    },
    "fetch-regulationsgov-comment-detail": {
        "provider_modes": [
            {
                "mode": "comment-detail",
                "time_coverage": "details for explicitly supplied Regulations.gov comment ids",
                "time_args": [],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--comment-id", "<comment_id>", "--include-records", "--no-save-response"],
            ["fetch", "--comment-ids-file", "<path>", "--include-records", "--no-save-response"],
        ],
    },
    "fetch-epa-eis-records": {
        "provider_modes": [
            {
                "mode": "eis-database-html-results",
                "time_coverage": "EPA EIS Database official result surfaces selected by commonSearch or explicit search URL",
                "time_args": [],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--common-search", "openComment", "--max-records", "<N>", "--output", "<artifact.json>"],
            ["fetch", "--common-search", "last30Published", "--max-records", "<N>", "--output", "<artifact.json>"],
            ["fetch", "--search-url", "<official_epa_eis_search_url>", "--dry-run"],
        ],
    },
    "fetch-federal-register-documents": {
        "provider_modes": [
            {
                "mode": "documents",
                "time_coverage": "FederalRegister.gov published documents filtered by publication_date arguments",
                "time_args": ["--publication-date-gte", "--publication-date-lte"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--term", "<term>", "--agency", "<agency_slug>", "--publication-date-gte", "<YYYY-MM-DD>", "--publication-date-lte", "<YYYY-MM-DD>", "--max-pages", "<N>", "--output", "<artifact.json>"],
            ["fetch", "--document-type", "Notice", "--publication-date-gte", "<YYYY-MM-DD>", "--publication-date-lte", "<YYYY-MM-DD>", "--dry-run"],
        ],
    },
    "fetch-usbr-project-records": {
        "provider_modes": [
            {
                "mode": "direct-url-project-pages",
                "time_coverage": "Current official USBR project page content and linked same-domain records at supplied URLs",
                "time_args": [],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--url", "<https://www.usbr.gov/...>", "--max-linked-records", "<N>", "--output", "<artifact.json>"],
            ["fetch", "--url-file", "<urls.txt>", "--max-linked-records", "<N>", "--dry-run"],
        ],
    },
    "fetch-usbr-rise": {
        "provider_modes": [
            {
                "mode": "rise-catalog-discovery",
                "time_coverage": "current RISE catalog-item metadata scanned by page with client-side phrase/filter matching",
                "time_args": [],
            },
            {
                "mode": "rise-results",
                "time_coverage": "RISE time-series result rows for explicit item IDs and optional dateTime filters",
                "time_args": ["--after-utc", "--before-utc"],
            }
        ],
        "fetch_argument_templates": [
            ["discover-items", "--query", "<place parameter phrase>", "--max-pages", "<N>", "--max-records", "<N>", "--output", "<catalog_candidates.json>"],
            ["discover-items", "--query", "<broader_or_revised_phrase>", "--max-pages-per-run", "<approved_cap>", "--max-pages", "<approved_cap>", "--max-records", "<N>", "--output", "<catalog_candidates.json>"],
            ["fetch", "--item-id", "<rise_item_id>", "--after-utc", "<YYYY-MM-DDTHH:MM:SSZ>", "--before-utc", "<YYYY-MM-DDTHH:MM:SSZ>", "--max-pages", "<N>", "--output", "<artifact.json>"],
            ["fetch", "--item-id", "<rise_item_id>", "--include-item-metadata", "--dry-run"],
        ],
    },
    "fetch-airnow-hourly-observations": {
        "provider_modes": [
            {
                "mode": "hourly-file-archive",
                "time_coverage": "AirNow hourly AQ Obs file products for requested UTC hours when files are available",
                "time_args": ["--start-datetime", "--end-datetime"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--bbox", "<min_lon,min_lat,max_lon,max_lat>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--parameter", "PM25", "--dry-run"],
        ],
    },
    "fetch-openaq": {
        "provider_modes": [
            {
                "mode": "api",
                "time_coverage": "OpenAQ API v3 metadata or measurement windows exposed by API endpoints",
                "time_args": ["--datetime-from", "--datetime-to"],
            },
            {
                "mode": "s3",
                "time_coverage": "OpenAQ public S3 archive partitions for explicit location/time keys",
                "time_args": ["--year", "--month", "--day", "--hour"],
            },
        ],
        "fetch_argument_templates": [
            ["fetch-measurements", "--locations-id", "<location_id>", "--parameters-id", "<parameter_id>", "--datetime-from", "<ISO8601>", "--datetime-to", "<ISO8601>", "--dry-run"],
            ["fetch-archive-backfill", "--location-id", "<location_id>", "--year", "<YYYY>", "--month", "<1-12>", "--day", "<1-31>", "--dry-run"],
        ],
    },
    "fetch-open-meteo-historical": {
        "provider_modes": [
            {
                "mode": "historical-archive",
                "time_coverage": "Open-Meteo historical archive endpoint for explicit coordinates and inclusive date range",
                "time_args": ["--start-date", "--end-date"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--location", "<latitude,longitude>", "--start-date", "<YYYY-MM-DD>", "--end-date", "<YYYY-MM-DD>", "--hourly-var", "wind_speed_10m", "--dry-run"],
        ],
    },
    "fetch-open-meteo-air-quality": {
        "provider_modes": [
            {
                "mode": "air-quality-archive",
                "time_coverage": "Open-Meteo air-quality endpoint for explicit coordinates and inclusive date range",
                "time_args": ["--start-date", "--end-date"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--location", "<latitude,longitude>", "--start-date", "<YYYY-MM-DD>", "--end-date", "<YYYY-MM-DD>", "--hourly-var", "pm2_5", "--dry-run"],
        ],
    },
    "fetch-open-meteo-flood": {
        "provider_modes": [
            {
                "mode": "flood-archive",
                "time_coverage": "Open-Meteo flood endpoint for explicit coordinates and inclusive date range",
                "time_args": ["--start-date", "--end-date"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--location", "<latitude,longitude>", "--start-date", "<YYYY-MM-DD>", "--end-date", "<YYYY-MM-DD>", "--daily-var", "river_discharge", "--dry-run"],
        ],
    },
    "fetch-usgs-water-iv": {
        "provider_modes": [
            {
                "mode": "instantaneous-values",
                "time_coverage": "USGS Water Services IV records for requested sites or bbox and date-time interval",
                "time_args": ["--start-datetime", "--end-datetime"],
            }
        ],
        "fetch_argument_templates": [
            ["fetch", "--bbox", "<west,south,east,north>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--dry-run"],
            ["fetch", "--site", "<site_no>", "--start-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--end-datetime", "<YYYY-MM-DDTHH:MM:SSZ>", "--dry-run"],
        ],
    },
    "fetch-nasa-firms-fire": {
        "provider_modes": [
            {
                "mode": "nrt",
                "time_coverage": "NASA FIRMS near-real-time active fire source ids for recent inclusive date windows only when provider availability covers them",
                "time_args": ["--start-date", "--end-date"],
            },
            {
                "mode": "standard-processing",
                "time_coverage": "NASA FIRMS standard-processing source ids when the selected source is available",
                "time_args": ["--start-date", "--end-date"],
            },
        ],
        "fetch_argument_templates": [
            ["check-config", "--probe-map-key", "--probe-source", "ALL"],
            ["fetch", "--source", "VIIRS_SNPP_SP", "--bbox", "<west,south,east,north>", "--start-date", "<YYYY-MM-DD>", "--end-date", "<YYYY-MM-DD>", "--check-availability", "--dry-run"],
            ["fetch", "--source", "VIIRS_NOAA20_SP", "--bbox", "<west,south,east,north>", "--start-date", "<YYYY-MM-DD>", "--end-date", "<YYYY-MM-DD>", "--check-availability", "--dry-run"],
            ["fetch", "--source", "VIIRS_NOAA20_NRT", "--bbox", "<west,south,east,north>", "--start-date", "<recent-YYYY-MM-DD>", "--end-date", "<recent-YYYY-MM-DD>", "--check-availability", "--dry-run"],
        ],
    },
}

SMOKE_SOURCE_INTENT_TOKENS = (
    "wildfire",
    "wild fire",
    "smoke episode",
    "smoke transport",
    "plume",
    "haze",
)
SOURCE_ORIGIN_INTENT_TOKENS = (
    "source region",
    "origin",
    "source attribution",
)
TRANSPORT_INTENT_TOKENS = (
    "transport",
    "pathway",
    "trajectory",
    "spatiotemporal",
    "source attribution",
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object at {path}")
    return payload


def read_json_list(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError(f"Expected a JSON list of objects at {path}")
    return payload


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def file_snapshot(path: Path) -> dict[str, str]:
    return {
        "path": str(path.resolve()),
        "sha256": file_sha256(path),
    }


def source_selection_path(run_dir: Path, round_id: str, role: str) -> Path:
    return resolve_run_dir(run_dir) / "runtime" / f"source_selection_{role}_{round_id}.json"


def source_config(source_skill: str) -> dict[str, Any]:
    config = SOURCE_CATALOG.get(maybe_text(source_skill))
    if config is None:
        raise ValueError(f"Unsupported source_skill: {source_skill}")
    return config


def source_capability_hints(source_skill: str) -> dict[str, Any]:
    hints = SOURCE_CAPABILITY_HINTS.get(maybe_text(source_skill))
    if not isinstance(hints, dict):
        hints = {"provider_modes": [], "fetch_argument_templates": []}
    payload = json.loads(json.dumps(hints, ensure_ascii=True))
    skill_name = maybe_text(source_skill)
    card = dict(DEFAULT_FETCH_SKILL_USE_CARD)
    specific = SOURCE_USE_CARDS.get(skill_name)
    if isinstance(specific, dict):
        for key, value in specific.items():
            if key in {"before_using", "zero_or_failed_result_discipline"}:
                card[key] = unique_texts(
                    list(card.get(key, [])) + list(value if isinstance(value, list) else [value])
                )
            else:
                card[key] = value
    payload["skill_use_card"] = card
    return payload


def source_role(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("role"))


def source_normalizer_skill(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("normalizer_skill"))


def source_artifact_capture(source_skill: str) -> str:
    return normalize_artifact_capture(source_config(source_skill).get("artifact_capture"))


def source_runtime_output_mode(source_skill: str) -> str:
    mode = maybe_text(source_config(source_skill).get("runtime_output_mode")) or "none"
    if mode not in {"none", "file", "dir"}:
        raise ValueError(f"Unsupported runtime_output_mode for {source_skill}: {mode}")
    return mode


def source_runtime_output_arg(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("runtime_output_arg"))


def source_runtime_default_args(source_skill: str) -> list[str]:
    values = source_config(source_skill).get("runtime_default_args")
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_requires_anchor(source_skill: str) -> bool:
    return coerce_bool(source_config(source_skill).get("requires_anchor"))


def source_anchor_source_skills(source_skill: str) -> list[str]:
    values = source_config(source_skill).get("anchor_source_skills")
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_anchor_argument(source_skill: str) -> str:
    return maybe_text(source_config(source_skill).get("anchor_argument"))


def source_auto_selectable(source_skill: str) -> bool:
    return coerce_bool(source_config(source_skill).get("auto_selectable"))


def mission_intent_text(mission: dict[str, Any]) -> str:
    parts: list[str] = [
        maybe_text(mission.get("topic")),
        maybe_text(mission.get("objective")),
        maybe_text(mission.get("request_text")),
    ]
    for item in mission.get("hypotheses", []) if isinstance(mission.get("hypotheses"), list) else []:
        if isinstance(item, dict):
            parts.extend(
                [
                    maybe_text(item.get("title")),
                    maybe_text(item.get("statement")),
                    maybe_text(item.get("hypothesis")),
                ]
            )
        else:
            parts.append(maybe_text(item))
    return " ".join(part for part in parts if part).casefold()


def mission_requires_scoping(mission: dict[str, Any]) -> bool:
    status = mission.get("mission_scope_status")
    if not isinstance(status, dict):
        return False
    value = status.get("scoping_required")
    if isinstance(value, bool):
        return value
    return maybe_text(value).casefold() in {"1", "true", "yes"}


def derive_evidence_lanes(mission: dict[str, Any]) -> list[dict[str, str]]:
    text = mission_intent_text(mission)
    lanes: list[dict[str, str]] = []

    def add(lane_id: str, role: str, requirement_type: str, summary: str, priority: str = "high") -> None:
        if any(item["lane_id"] == lane_id for item in lanes):
            return
        lanes.append(
            {
                "lane_id": lane_id,
                "role": role,
                "requirement_type": requirement_type,
                "summary": summary,
                "priority": priority,
            }
        )

    smoke_context = "smoke" in text or any(token in text for token in SMOKE_SOURCE_INTENT_TOKENS)
    smoke_source_intent = any(token in text for token in SMOKE_SOURCE_INTENT_TOKENS) or (
        smoke_context and any(token in text for token in SOURCE_ORIGIN_INTENT_TOKENS)
    )
    transport_intent = any(token in text for token in TRANSPORT_INTENT_TOKENS)
    if smoke_source_intent:
        add(
            "receptor-air-quality",
            "environmental-investigator",
            "receptor-air-quality",
            "Record local receptor air-quality anomaly evidence as a candidate review lane.",
        )
        add(
            "fire-origin",
            "environmental-investigator",
            "fire-origin-candidate",
            "Record active-fire evidence sources for candidate wildfire source-region review.",
        )
        add(
            "public-discourse",
            "social-investigator",
            "public-discourse-signal",
            "Collect public/reporting signals about the smoke episode and affected communities.",
        )
    if smoke_source_intent or transport_intent:
        add(
            "local-weather-context",
            "environmental-investigator",
            "weather-transport-context",
            "Record local weather context for later council or agent transport review.",
        )
        add(
            "spatiotemporal-relation-review",
            "environmental-investigator",
            "spatiotemporal-relation-review",
            "Record a spatiotemporal relation review lane when source or transport questions are in scope.",
        )
    if any(token in text for token in ("health", "asthma", "community", "impact", "public health")):
        add(
            "community-impact",
            "social-investigator",
            "community-impact-signal",
            "Record public/community impact signals as a separate evidence lane.",
            priority="medium",
        )
    if any(token in text for token in ("recommendation", "response", "handling", "处理", "建议")):
        add(
            "response-recommendation-boundary",
            "social-investigator",
            "response-record-signal",
            "Record response or recommendation evidence when handling recommendations are in scope.",
            priority="medium",
        )
    return lanes


def derive_verification_scope(mission: dict[str, Any]) -> dict[str, Any]:
    window = mission.get("window") if isinstance(mission.get("window"), dict) else {}
    region = mission.get("region") if isinstance(mission.get("region"), dict) else {}
    geometry = region.get("geometry") if isinstance(region.get("geometry"), dict) else {}
    lanes = derive_evidence_lanes(mission)
    lane_ids = {maybe_text(lane.get("lane_id")) for lane in lanes}
    source_required = "fire-origin" in lane_ids
    transport_required = "spatiotemporal-relation-review" in lane_ids
    required_source_skills: list[str] = []
    candidate_source_skills: list[str] = []
    source_selections = (
        mission.get("source_selections")
        if isinstance(mission.get("source_selections"), dict)
        else {}
    )
    explicit_selected_sources: list[str] = []
    for selection in source_selections.values():
        if isinstance(selection, dict):
            explicit_selected_sources.extend(list_items(selection.get("selected_sources")))
    if explicit_selected_sources:
        required_source_skills.extend(unique_texts(explicit_selected_sources))
    for role in SOURCE_SELECTION_ROLES:
        candidate_source_skills.extend(intent_selected_sources(mission, role))
    return {
        "scope_id": "verification-scope-"
        + stable_hash(
            mission.get("run_id"),
            mission.get("topic"),
            mission.get("objective"),
            window.get("start_utc"),
            window.get("end_utc"),
            region.get("label"),
        )[:12],
        "receptor_region": {
            "label": maybe_text(region.get("label")),
            "geometry": geometry,
        },
        "study_window": {
            "start_utc": maybe_text(window.get("start_utc")),
            "end_utc": maybe_text(window.get("end_utc")),
        },
        "required_evidence_lanes": lanes,
        "candidate_source_region_policy": (
            "mission-derived-candidate-source-review" if source_required else "not-applicable"
        ),
        "transport_verification_policy": (
            "mission-derived-relation-review" if transport_required else "not-applicable"
        ),
        "lag_window": {
            "mode": "mission-derived",
            "minimum_hours": 0,
            "maximum_hours": 72 if source_required or transport_required else 0,
        },
        "required_source_skills": unique_texts(required_source_skills),
        "candidate_source_skills": unique_texts(candidate_source_skills),
    }


def intent_selected_sources(mission: dict[str, Any], role: str) -> list[str]:
    return []


def lane_evidence_requirements(mission: dict[str, Any], *, round_id: str, role: str) -> list[dict[str, str]]:
    requirements: list[dict[str, str]] = []
    for lane in derive_evidence_lanes(mission):
        if maybe_text(lane.get("role")) != role:
            continue
        lane_id = maybe_text(lane.get("lane_id"))
        if not lane_id:
            continue
        requirements.append(
            {
                "requirement_id": f"req-{role}-{round_id}-{lane_id}",
                "requirement_type": maybe_text(lane.get("requirement_type")),
                "summary": maybe_text(lane.get("summary")),
                "priority": maybe_text(lane.get("priority")) or "high",
                "evidence_lane": lane_id,
            }
        )
    return requirements


def normalize_text_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return unique_texts([maybe_text(value) for value in values if maybe_text(value)])


def normalize_artifact_capture(value: Any) -> str:
    capture_mode = maybe_text(value) or "stdout-json"
    if capture_mode not in SUPPORTED_ARTIFACT_CAPTURE_MODES:
        raise ValueError(f"Unsupported artifact_capture: {capture_mode}")
    return capture_mode


def normalize_fetch_execution_policy(payload: dict[str, Any]) -> dict[str, Any]:
    execution_policy = payload.get("fetch_execution_policy") if isinstance(payload.get("fetch_execution_policy"), dict) else {}
    timeout_seconds = coerce_float(execution_policy.get("timeout_seconds"))
    if timeout_seconds is None:
        timeout_seconds = coerce_float(payload.get("timeout_seconds"))
    retry_budget = coerce_int(execution_policy.get("retry_budget"))
    if retry_budget is None:
        retry_budget = coerce_int(payload.get("retry_budget"))
    retry_backoff_ms = coerce_int(execution_policy.get("retry_backoff_ms"))
    if retry_backoff_ms is None:
        retry_backoff_ms = coerce_int(payload.get("retry_backoff_ms"))
    return {
        "timeout_seconds": max(0.0, float(timeout_seconds if timeout_seconds is not None else 300.0)),
        "retry_budget": max(0, int(retry_budget if retry_budget is not None else 0)),
        "retry_backoff_ms": max(0, int(retry_backoff_ms if retry_backoff_ms is not None else 250)),
    }


def validate_fetch_side_effects(values: list[str], *, field_name: str) -> list[str]:
    invalid = [value for value in values if value not in KNOWN_FETCH_SIDE_EFFECTS]
    if invalid:
        raise ValueError(f"Unsupported fetch side effects in {field_name}: {', '.join(invalid)}")
    return unique_texts(values)


def normalize_fetch_declared_side_effects(payload: dict[str, Any]) -> list[str]:
    declared = normalize_text_list(payload.get("declared_side_effects"))
    validated = validate_fetch_side_effects(declared, field_name="declared_side_effects")
    return unique_texts(["writes-artifacts", *validated])


def normalize_fetch_requested_side_effect_approvals(payload: dict[str, Any], declared_side_effects: list[str]) -> list[str]:
    requested = validate_fetch_side_effects(
        normalize_text_list(payload.get("requested_side_effect_approvals")),
        field_name="requested_side_effect_approvals",
    )
    undeclared = [value for value in requested if value not in declared_side_effects]
    if undeclared:
        raise ValueError(
            "requested_side_effect_approvals must be a subset of declared_side_effects: "
            + ", ".join(undeclared)
        )
    return requested


def allowed_sources_for_role(mission: dict[str, Any], role: str) -> list[str]:
    base = [skill_name for skill_name, config in SOURCE_CATALOG.items() if maybe_text(config.get("role")) == role]
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    mission_allowlist = mission.get("allowed_sources_by_role") if isinstance(mission.get("allowed_sources_by_role"), dict) else {}
    configured = mission_allowlist.get(role)
    if configured is None and isinstance(governance.get("allowed_sources_by_role"), dict):
        configured = governance["allowed_sources_by_role"].get(role)
    if isinstance(configured, list):
        requested = {maybe_text(item) for item in configured if maybe_text(item)}
        return [skill_name for skill_name in base if skill_name in requested]
    return base


def effective_constraints(mission: dict[str, Any]) -> dict[str, int]:
    defaults = {
        "max_selected_sources_per_role": 4,
        "max_source_steps_per_round": 8,
    }
    constraints = mission.get("constraints") if isinstance(mission.get("constraints"), dict) else {}
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    for key in tuple(defaults):
        value = governance.get(key)
        if value in (None, ""):
            value = constraints.get(key)
        coerced = coerce_int(value)
        if coerced is not None:
            defaults[key] = coerced
    return defaults


def tier_sort_order(value: Any) -> int:
    text = maybe_text(value).lower()
    if text.startswith("l") and text[1:].isdigit():
        return int(text[1:])
    return 999


def role_source_governance(mission: dict[str, Any], role: str) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    approved_layers_payload = governance.get("approved_layers") if isinstance(governance.get("approved_layers"), list) else []
    families: dict[str, dict[str, Any]] = {}
    for source_skill, config in SOURCE_CATALOG.items():
        if maybe_text(config.get("role")) != role:
            continue
        family_id = maybe_text(config.get("family_id"))
        family = families.setdefault(
            family_id,
            {
                "family_id": family_id,
                "label": maybe_text(config.get("family_label")),
                "role": role,
                "skills": [],
                "_layers": {},
            },
        )
        family["skills"].append(source_skill)
        layer_id = maybe_text(config.get("layer_id"))
        tier = maybe_text(config.get("tier")) or "l1"
        layer_lookup = family.setdefault("_layers", {})
        if not isinstance(layer_lookup, dict):
            layer_lookup = {}
            family["_layers"] = layer_lookup
        layer = layer_lookup.setdefault(
            layer_id,
            {
                "layer_id": layer_id,
                "label": maybe_text(config.get("layer_label")),
                "tier": tier,
                "skills": [],
                "max_selected_skills": 0,
                "requires_anchor": coerce_bool(config.get("requires_anchor")),
                "anchor_source_skills": [],
                "auto_selectable": coerce_bool(config.get("auto_selectable")) if "auto_selectable" in config else tier == "l1",
            },
        )
        if isinstance(layer, dict):
            layer_skills = layer.setdefault("skills", [])
            if isinstance(layer_skills, list):
                layer_skills.append(source_skill)
            anchor_skills = layer.setdefault("anchor_source_skills", [])
            if isinstance(anchor_skills, list):
                anchor_skills.extend(source_anchor_source_skills(source_skill))
    for family in families.values():
        family["skills"] = unique_texts(family.get("skills", []))
        layer_lookup = family.pop("_layers", {})
        layers = layer_lookup.values() if isinstance(layer_lookup, dict) else []
        finalized_layers: list[dict[str, Any]] = []
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            layer["skills"] = unique_texts(layer.get("skills", []))
            layer["anchor_source_skills"] = unique_texts(layer.get("anchor_source_skills", []))
            layer["max_selected_skills"] = len(layer["skills"])
            finalized_layers.append(layer)
        family["layers"] = sorted(
            finalized_layers,
            key=lambda item: (tier_sort_order(item.get("tier")), maybe_text(item.get("layer_id"))),
        )
    family_ids = {maybe_text(item.get("family_id")) for item in families.values() if maybe_text(item.get("family_id"))}
    approved_layers = [
        item
        for item in approved_layers_payload
        if isinstance(item, dict)
        and maybe_text(item.get("family_id")) in family_ids
        and maybe_text(item.get("layer_id"))
    ]
    return {
        "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
        "allow_cross_round_anchors": coerce_bool(governance.get("allow_cross_round_anchors")),
        "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
        "max_active_families_per_role": coerce_int(governance.get("max_active_families_per_role")),
        "max_non_entry_layers_per_role": coerce_int(governance.get("max_non_entry_layers_per_role")),
        "approved_layers": approved_layers,
        "families": sorted(families.values(), key=lambda item: maybe_text(item.get("family_id"))),
    }


def policy_profile_summary(mission: dict[str, Any]) -> dict[str, Any]:
    governance = mission.get("source_governance") if isinstance(mission.get("source_governance"), dict) else {}
    return {
        "policy_profile": maybe_text(mission.get("policy_profile")) or "standard",
        "effective_constraints": effective_constraints(mission),
        "source_governance": {
            "approval_authority": maybe_text(governance.get("approval_authority")) or "runtime-operator",
            "allow_cross_round_anchors": coerce_bool(governance.get("allow_cross_round_anchors")),
            "max_selected_sources_per_role": effective_constraints(mission).get("max_selected_sources_per_role"),
        },
    }


def normalize_artifact_imports(mission: dict[str, Any]) -> list[dict[str, Any]]:
    imports = mission.get("artifact_imports") if isinstance(mission.get("artifact_imports"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in imports:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        config = source_config(source_skill)
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "artifact_path": maybe_text(item.get("artifact_path")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


def normalize_source_requests(mission: dict[str, Any]) -> list[dict[str, Any]]:
    requests = mission.get("source_requests") if isinstance(mission.get("source_requests"), list) else []
    normalized: list[dict[str, Any]] = []
    for item in requests:
        if not isinstance(item, dict):
            continue
        source_skill = maybe_text(item.get("source_skill"))
        if not source_skill:
            continue
        config = source_config(source_skill)
        fetch_argv = item.get("fetch_argv") if isinstance(item.get("fetch_argv"), list) else []
        declared_side_effects = normalize_fetch_declared_side_effects(item)
        normalized.append(
            {
                **item,
                "source_skill": source_skill,
                "role": maybe_text(config.get("role")),
                "query_text": maybe_text(item.get("query_text")),
                "source_mode": maybe_text(item.get("source_mode")),
                "artifact_capture": normalize_artifact_capture(item.get("artifact_capture") or config.get("artifact_capture")),
                "artifact_path": maybe_text(item.get("artifact_path")),
                "fetch_cwd": maybe_text(item.get("fetch_cwd")),
                "fetch_argv": [maybe_text(arg) for arg in fetch_argv if maybe_text(arg)],
                "fetch_execution_policy": normalize_fetch_execution_policy(item),
                "declared_side_effects": declared_side_effects,
                "requested_side_effect_approvals": normalize_fetch_requested_side_effect_approvals(item, declared_side_effects),
                "notes": [maybe_text(note) for note in item.get("notes", []) if maybe_text(note)] if isinstance(item.get("notes"), list) else [],
            }
        )
    return normalized


__all__ = [
    "coerce_bool",
    "coerce_float",
    "coerce_int",
    "KNOWN_FETCH_SIDE_EFFECTS",
    "SOURCE_CATALOG",
    "SOURCE_CAPABILITY_HINTS",
    "SOURCE_SELECTION_ROLES",
    "SUPPORTED_ARTIFACT_CAPTURE_MODES",
    "allowed_sources_for_role",
    "derive_evidence_lanes",
    "derive_verification_scope",
    "effective_constraints",
    "file_sha256",
    "file_snapshot",
    "maybe_text",
    "normalize_artifact_capture",
    "normalize_artifact_imports",
    "normalize_fetch_execution_policy",
    "normalize_fetch_declared_side_effects",
    "normalize_fetch_requested_side_effect_approvals",
    "normalize_source_requests",
    "normalize_text_list",
    "intent_selected_sources",
    "lane_evidence_requirements",
    "mission_intent_text",
    "mission_requires_scoping",
    "policy_profile_summary",
    "read_json_list",
    "read_json_object",
    "resolve_run_dir",
    "role_source_governance",
    "source_anchor_argument",
    "source_anchor_source_skills",
    "source_artifact_capture",
    "source_auto_selectable",
    "source_capability_hints",
    "source_config",
    "source_normalizer_skill",
    "source_role",
    "source_requires_anchor",
    "source_runtime_default_args",
    "source_runtime_output_arg",
    "source_runtime_output_mode",
    "source_selection_path",
    "stable_hash",
    "unique_texts",
    "utc_now_iso",
    "write_json_file",
]

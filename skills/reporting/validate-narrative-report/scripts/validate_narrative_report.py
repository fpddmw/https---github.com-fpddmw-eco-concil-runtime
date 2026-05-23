#!/usr/bin/env python3
"""Validate narrative report draft structure and traceability."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "validate-narrative-report"
REQUIRED_SECTIONS = {
    "executive-summary",
    "key-points",
    "what-happened",
    "evidence-basis",
    "council-reasoning",
    "limitations",
    "decision-implications",
    "audit-trail",
}
MACHINE_PROSE_PREFIXES = (
    "council-decision (",
    "council-decision-draft (",
    "expert-report-",
    "round-synthesis:",
    "agent-position:",
    "finding:",
    "environmental-investigator:",
    "social-investigator:",
)
SITUATION_ANALYSIS_CHAIN_FIELDS = (
    "event_stage_map",
    "fact_process_chain",
    "official_action_chain",
    "public_semantic_chain",
    "policy_semantic_chain",
    "interaction_claims",
)
SITUATION_ANALYSIS_CONSUMPTION_STOPWORDS = {
    "about",
    "action",
    "analysis",
    "answer",
    "basis",
    "bounded",
    "brief",
    "carried",
    "chain",
    "claim",
    "claims",
    "council",
    "current",
    "event",
    "fact",
    "from",
    "mission",
    "official",
    "policy",
    "process",
    "public",
    "question",
    "report",
    "semantic",
    "situation",
    "stage",
    "summary",
    "supports",
    "that",
    "the",
    "this",
    "with",
}
UNFINISHED_AUDIT_INDEX_TERMS = (
    "audit pending",
    "audit index pending",
    "audit index incomplete",
    "unfinished audit index",
    "unresolved audit index",
    "missing audit index",
    "not yet indexed",
    "todo audit",
    "tbd audit",
    "审计索引待补",
    "审计索引未完成",
    "审计索引缺失",
    "未完成审计索引",
)
RAW_AUDIT_ID_RE = re.compile(
    r"\b(?:finding|evidence-bundle|review-comment|readiness-opinion|round-synthesis|agent-position|"
    r"challenge-disposition|runtimeevt|runtime-receipt|normalize-receipt|source-acquisition-proposal|"
    r"report-basis|sig)-[0-9a-f]{6,}\b"
)
CHINESE_RUNTIME_JARGON = (
    "source attribution",
    "plume transport",
    "chemical causation",
    "public sentiment",
    "source narrative cues",
    "route diagnostic",
    "frozen basis",
    "canonical report basis",
)
PUBLIC_DISCOURSE_BASIS_MARKERS = (
    "public_discourse_sample_summary",
    "public-discourse-sample-summary",
    "summarize-public-discourse-sample",
    "audit-public-discourse-sample-coverage",
    "public-discourse-coverage-audit",
    "public-discourse-coverage-receipt",
    "public_discourse_annotation_aggregation",
    "public-discourse-annotation-aggregation",
    "aggregate-public-discourse-annotations",
    "classify-public-discourse-affect",
)
PUBLIC_DISCOURSE_QUANTIFICATION_CUES = (
    "affect",
    "emotion",
    "sentiment",
    "issue",
    "label",
    "narrative",
    "source narrative",
    "public discourse",
    "youtube comment",
    "bluesky",
    "formal comment",
    "gdelt tone",
    "health-risk",
    "concern",
    "sample fraction",
    "sample_fraction",
    "\u60c5\u7eea",
    "\u8bae\u9898",
    "\u6807\u7b7e",
    "\u53d9\u4e8b",
    "\u6765\u6e90\u53d9\u4e8b",
    "\u8206\u60c5",
    "\u8bc4\u8bba",
    "\u6837\u672c",
    "\u51fa\u73b0\u7387",
)
PUBLIC_DISCOURSE_SAMPLE_BOUNDARY_TERMS = (
    "sample-local",
    "sample local",
    "inside the sample",
    "within the sample",
    "in this sample",
    "sample-only",
    "not representative",
    "not a representative",
    "not population",
    "not affected population",
    "\u6837\u672c\u5185",
    "\u6837\u672c\u4e2d",
    "\u4ec5\u9650\u6837\u672c",
    "\u975e\u4ee3\u8868\u6027",
    "\u4e0d\u4ee3\u8868",
    "\u4e0d\u662f\u603b\u4f53",
    "\u4e0d\u662f\u53d7\u5f71\u54cd\u4eba\u7fa4\u603b\u4f53",
)
PUBLIC_DISCOURSE_NONEXCLUSIVE_TERMS = (
    "non-exclusive",
    "not mutually exclusive",
    "should not be summed",
    "do not sum",
    "not add up to 100",
    "not sum to 100",
    "\u975e\u4e92\u65a5",
    "\u4e0d\u4e92\u65a5",
    "\u4e0d\u5e94\u76f8\u52a0",
    "\u4e0d\u80fd\u76f8\u52a0",
    "\u4e0d\u5e94\u52a0\u603b",
    "\u4e0d\u7b49\u4e8e100%",
)
PUBLIC_OPINION_UPGRADE_PHRASES = (
    "overall public opinion",
    "general public opinion",
    "representative public opinion",
    "representative public sentiment",
    "platform-wide sentiment",
    "affected population opinion",
    "affected-population opinion",
    "population opinion",
    "public sentiment estimate",
    "overall public sentiment",
    "the public mostly",
    "the public generally",
    "most residents",
    "most affected residents",
    "affected residents mostly",
    "residents broadly",
    "platform sentiment overall",
    "\u603b\u4f53\u6c11\u610f",
    "\u6574\u4f53\u6c11\u610f",
    "\u4ee3\u8868\u6027\u6c11\u610f",
    "\u4ee3\u8868\u6027\u516c\u4f17\u60c5\u7eea",
    "\u5e73\u53f0\u6574\u4f53\u60c5\u7eea",
    "\u53d7\u5f71\u54cd\u4eba\u7fa4\u6574\u4f53\u89c2\u70b9",
    "\u53d7\u5f71\u54cd\u4eba\u7fa4\u603b\u4f53\u89c2\u70b9",
    "\u516c\u4f17\u666e\u904d",
    "\u5c45\u6c11\u666e\u904d",
    "\u5927\u591a\u6570\u516c\u4f17",
    "\u591a\u6570\u5c45\u6c11",
    "\u53d7\u5f71\u54cd\u5c45\u6c11\u666e\u904d",
)
PUBLIC_OPINION_DENOMINATOR_PHRASES = (
    "of the public",
    "among the public",
    "of affected residents",
    "among affected residents",
    "of residents",
    "among residents",
    "of the affected population",
    "among the affected population",
    "\u516c\u4f17\u4e2d",
    "\u53d7\u5f71\u54cd\u4eba\u7fa4\u4e2d",
    "\u53d7\u5f71\u54cd\u5c45\u6c11\u4e2d",
    "\u5c45\u6c11\u4e2d",
)
FORMAL_COMMENT_PUBLIC_OPINION_PHRASES = (
    "formal comments show public opinion",
    "formal comments represent public opinion",
    "formal comment distribution shows public opinion",
    "formal comment sample represents the public",
    "regulations.gov comments show public opinion",
    "regulations.gov comments represent public opinion",
    "docket comments show public opinion",
    "docket comments represent public opinion",
    "\u6b63\u5f0f\u610f\u89c1\u4ee3\u8868\u516c\u4f17\u610f\u89c1",
    "\u6b63\u5f0f\u8bc4\u8bba\u4ee3\u8868\u603b\u4f53\u6c11\u610f",
)
FORMAL_COMMENT_STRUCTURE_TERMS = (
    "formal comment",
    "formal comments",
    "formal participation",
    "regulations.gov comment",
    "regulations.gov comments",
    "docket comment",
    "docket comments",
    "\u6b63\u5f0f\u610f\u89c1",
    "\u6b63\u5f0f\u8bc4\u8bba",
)
FORMAL_COMMENT_STRUCTURE_CLAIM_TERMS = (
    "main issue",
    "major issue",
    "primary issue",
    "key issue",
    "issue distribution",
    "stance distribution",
    "concern distribution",
    "label distribution",
    "sample fraction",
    "\u4e3b\u8981\u4e89\u8bae",
    "\u4e3b\u8981\u8bae\u9898",
    "\u7acb\u573a\u5206\u5e03",
    "\u6807\u7b7e\u5206\u5e03",
)
FORMAL_COMMENT_STANCE_DISTRIBUTION_TERMS = (
    "stance distribution",
    "support distribution",
    "opposition distribution",
    "support/oppose",
    "support or oppose",
    "supportive or opposed",
    "formal_stance_hints",
    "\u7acb\u573a\u5206\u5e03",
    "\u652f\u6301/\u53cd\u5bf9",
)
FORMAL_ATTACHMENT_DISCUSSION_TERMS = (
    "attachment",
    "attached comment",
    "attachment text",
    "pdf attachment",
    "\u9644\u4ef6",
    "\u9644\u4ef6\u6587\u672c",
)
FORMAL_ATTACHMENT_LIMITATION_TERMS = (
    "attachment limitation",
    "attachment text limitation",
    "unreadable attachment",
    "text extraction limited",
    "text-extraction-limited",
    "requires attachment text",
    "requires-attachment-text",
    "unreadable pdf",
    "scanned pdf",
    "\u9644\u4ef6\u4e0d\u53ef\u8bfb",
    "\u9644\u4ef6\u6587\u672c\u9650\u5236",
)
SAMPLE_FRACTION_TOTALIZATION_PHRASES = (
    "sum to 100%",
    "add up to 100%",
    "total 100%",
    "complete opinion composition",
    "100% opinion composition",
    "full opinion composition",
    "\u76f8\u52a0\u4e3a100%",
    "\u52a0\u603b\u4e3a100%",
    "\u5b8c\u6574\u610f\u89c1\u6784\u6210",
)
SAMPLE_DENOMINATOR_TERMS = (
    "denominator",
    "sample denominator",
    "source-family denominator",
    "source family denominator",
    "discourse lane denominator",
    "eligible signal count",
    "eligible_signal_count",
    "annotated sample",
    "n=",
    "sample_count",
    "\u5206\u6bcd",
    "\u6837\u672c\u91cf",
)
SMALL_SAMPLE_BOUNDARY_TERMS = (
    "small sample",
    "limited sample",
    "sample size",
    "only ",
    "n=",
    "sample_count",
    "\u5c0f\u6837\u672c",
    "\u6837\u672c\u91cf\u6709\u9650",
    "\u6837\u672c\u4e0d\u8db3",
)
REPRESENTATIVENESS_LIMIT_TERMS = (
    "representativeness limit",
    "representativeness limits",
    "not representative",
    "not a representative",
    "not population",
    "not general public opinion",
    "not public opinion",
    "\u4ee3\u8868\u6027\u9650\u5236",
    "\u975e\u4ee3\u8868\u6027",
    "\u4e0d\u4ee3\u8868",
    "\u4e0d\u662f\u603b\u4f53",
    "\u4e0d\u80fd\u5916\u63a8",
    "\u4e0d\u662f\u968f\u673a\u62bd\u6837",
    "\u4e0d\u662f\u53d7\u5f71\u54cd\u4eba\u7fa4",
)
REPRESENTATIVE_SAMPLING_DESIGN_TERMS = (
    "representative sampling design",
    "representative sample design",
    "representative survey design",
    "probability sample",
    "probability sampling",
    "stratified random sample",
    "weighted survey",
    "survey weights",
    "population-weighted",
    "\u4ee3\u8868\u6027\u62bd\u6837\u8bbe\u8ba1",
    "\u6982\u7387\u62bd\u6837",
    "\u5206\u5c42\u968f\u673a\u62bd\u6837",
    "\u52a0\u6743\u8c03\u67e5",
    "\u6c11\u610f\u8c03\u67e5\u8bbe\u8ba1",
)
GDELT_TONE_PUBLIC_SENTIMENT_PHRASES = (
    "gdelt tone proves public sentiment",
    "gdelt tone shows public sentiment",
    "gdelt v2tone proves public sentiment",
    "gdelt v2tone shows public sentiment",
    "gdelt tone represents public sentiment",
    "gdelt tone represents public emotion",
    "gdelt tone is public sentiment",
    "gdelt tone \u662f\u516c\u4f17\u60c5\u7eea",
    "gdelt tone \u4ee3\u8868\u516c\u4f17\u60c5\u7eea",
    "gdelt tone \u8bc1\u660e\u516c\u4f17\u60c5\u7eea",
    "gdelt v2tone \u8bc1\u660e\u516c\u4f17\u60c5\u7eea",
)
GDELT_TONE_TERMS = (
    "gdelt tone",
    "gdelt v2tone",
    "v2tone",
    "avgtone",
    "mentiondoctone",
    "mention doc tone",
    "tonechart",
    "timeline tone",
    "timelinetone",
    "gkg tone",
    "doc tone",
    "gdelt \u8bed\u6c14",
    "gdelt tone",
)
PUBLIC_SENTIMENT_TERMS = (
    "public sentiment",
    "public emotion",
    "public mood",
    "public feelings",
    "public affect",
    "public opinion",
    "\u516c\u4f17\u60c5\u7eea",
    "\u516c\u4f17\u60c5\u611f",
    "\u516c\u4f17\u5fc3\u6001",
    "\u603b\u4f53\u6c11\u610f",
    "\u6c11\u610f",
)
PLATFORM_OR_DOCKET_SAMPLE_TERMS = (
    "youtube",
    "bluesky",
    "regulations.gov",
    "regulationsgov",
    "formal comment",
    "formal comments",
    "docket comment",
    "docket comments",
    "\u6b63\u5f0f\u610f\u89c1",
    "\u6b63\u5f0f\u8bc4\u8bba",
)
SAMPLE_GENERALIZATION_TERMS = (
    "shows the public",
    "shows residents",
    "shows affected residents",
    "shows people",
    "the public believes",
    "the public thinks",
    "residents believe",
    "residents think",
    "people believe",
    "people think",
    "overall public",
    "general public",
    "representative public",
    "platform-wide",
    "population-wide",
    "\u516c\u4f17\u666e\u904d",
    "\u5c45\u6c11\u666e\u904d",
    "\u5927\u591a\u6570\u516c\u4f17",
    "\u591a\u6570\u5c45\u6c11",
    "\u603b\u4f53\u6c11\u610f",
    "\u6574\u4f53\u6c11\u610f",
    "\u4ee3\u8868\u6027\u6c11\u610f",
)
SOURCE_NARRATIVE_ATTRIBUTION_PHRASES = (
    "source narrative proves physical source attribution",
    "source narrative establishes physical source attribution",
    "public source narrative proves physical source",
    "public narrative proves physical source attribution",
    "source narrative proves origin",
    "public narratives show the physical source",
    "public narratives show the source",
    "comments show the physical source",
    "comments show the source",
    "media narrative proves source attribution",
    "media narratives prove source attribution",
    "\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u7269\u7406\u6765\u6e90",
    "\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u6765\u6e90\u5f52\u56e0",
    "\u516c\u5171\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u7269\u7406\u5f52\u56e0",
    "\u516c\u5171\u53d9\u4e8b\u8bc1\u660e\u5177\u4f53\u6765\u6e90",
    "\u516c\u5171\u53d9\u4e8b\u8868\u660e\u7269\u7406\u6765\u6e90",
    "\u8bc4\u8bba\u8bc1\u660e\u7269\u7406\u6765\u6e90",
)
SOURCE_NARRATIVE_DISCUSSION_TERMS = (
    "source narrative",
    "source-narrative",
    "source narrative distribution",
    "source narrative label",
    "source-narrative label",
    "public source narrative",
    "\u6765\u6e90\u53d9\u4e8b",
)
SOURCE_NARRATIVE_BOUNDARY_TERMS = (
    "source narrative cue",
    "source-narrative cue",
    "public source narrative cue",
    "not physical source attribution",
    "not physical attribution",
    "not source attribution",
    "cannot substitute for physical source attribution",
    "environmental verification",
    "\u6765\u6e90\u53d9\u4e8b\u7ebf\u7d22",
    "\u4e0d\u662f\u7269\u7406\u6765\u6e90\u5f52\u56e0",
    "\u4e0d\u80fd\u66ff\u4ee3\u7269\u7406\u6765\u6e90\u5f52\u56e0",
    "\u7269\u7406\u6765\u6e90\u5f52\u56e0\u9a8c\u8bc1",
    "\u7269\u7406\u6765\u6e90\u5224\u5b9a",
    "\u4e0d\u80fd\u66ff\u4ee3\u73af\u5883\u7ebf\u7684\u7269\u7406\u6765\u6e90\u5224\u5b9a",
    "\u4e0d\u80fd\u66ff\u4ee3\u73af\u5883\u3001\u8fd0\u884c\u3001\u6cd5\u5f8b\u6216\u653f\u7b56\u56e0\u679c\u5224\u5b9a",
    "\u4e0d\u80fd\u7528\u516c\u5171\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u5177\u4f53\u6765\u6e90\u3001\u8fd0\u884c\u56e0\u679c\u6216\u8d23\u4efb\u5224\u65ad",
)
OPTIONAL_HELPER_MARKERS = (
    "aggregate-environment-evidence",
    "environment_evidence_aggregation",
    "envagg-",
    "build-fact-policy-public-interaction-timeline",
    "fact_policy_public_interaction_timeline",
    "fact-policy-public-interaction",
    "fact-policy-public-interaction-node",
    "section-brief-fpp-",
    "interaction_timeline",
    "summarize-public-discourse-sample",
    "public_discourse_sample_summary",
    "aggregate-public-discourse-annotations",
    "public_discourse_annotation_aggregation",
    "compare-public-media-narratives",
    "public_media_narrative_comparison",
    "materialize-public-discourse-corpus",
    "public_discourse_corpus",
)
INTERACTION_TIMELINE_BASIS_MARKERS = (
    "build-fact-policy-public-interaction-timeline",
    "fact_policy_public_interaction_timeline",
    "fact-policy-public-interaction",
    "fact-policy-public-interaction-node",
    "lane_episode_cards",
    "lane episode card",
    "section-brief-fpp-",
    "interaction_timeline_nodes",
    "interaction timeline",
    "\u4e8b\u5b9e-\u653f\u7b56-\u516c\u5171\u4e92\u52a8",
    "\u4e92\u52a8\u65f6\u95f4\u7ebf",
)
INTERACTION_JUDGEMENT_PHRASES = (
    "public response to",
    "public reaction to",
    "public discourse shifted after",
    "public concern shifted after",
    "public media response to",
    "same-day public response",
    "same-day interaction",
    "interaction between official",
    "interaction between policy",
    "fact-policy-public interaction",
    "policy response caused public concern",
    "official action drove public concern",
    "media and public response to",
    "\u516c\u4f17\u5bf9",
    "\u516c\u5171\u8ba8\u8bba\u56e0",
    "\u8206\u60c5\u968f",
    "\u540c\u65e5\u4e92\u52a8",
    "\u653f\u7b56\u56de\u5e94\u5f15\u53d1",
)
INTERACTION_SIDE_TERMS = (
    "policy response",
    "official action",
    "agency notice",
    "formal record",
    "fact/policy",
    "public/media",
    "\u653f\u7b56\u56de\u5e94",
    "\u5b98\u65b9\u884c\u52a8",
    "\u6b63\u5f0f\u8bb0\u5f55",
)
INTERACTION_CLAIM_TERMS = (
    "caused public",
    "drove public",
    "triggered public",
    "public response",
    "public reaction",
    "interaction",
    "semantic shift",
    "communication gap",
    "\u5f15\u53d1\u516c\u4f17",
    "\u63a8\u52a8\u8206\u60c5",
    "\u516c\u4f17\u53cd\u5e94",
    "\u8bed\u4e49\u53d8\u5316",
    "\u6c9f\u901a\u7f3a\u53e3",
)
INTERACTION_BOUNDARY_TERMS = (
    "not causality",
    "does not prove causality",
    "not policy impact",
    "not public response attribution",
    "descriptive chronology",
    "co-visible",
    "same timeline window",
    "bounded-descriptive-context-only",
    "\u4e0d\u662f\u56e0\u679c",
    "\u4e0d\u8bc1\u660e\u56e0\u679c",
    "\u4e0d\u662f\u653f\u7b56\u6548\u679c",
    "\u4e0d\u662f\u516c\u4f17\u53cd\u5e94\u5f52\u56e0",
    "\u63cf\u8ff0\u6027\u65f6\u95f4\u7ebf",
)
POLICY_EVALUATION_CLAIM_PHRASES = (
    "policy was effective",
    "policy is effective",
    "policy was ineffective",
    "policy is ineffective",
    "policy succeeded",
    "policy failed",
    "policy solved",
    "policy reduced",
    "policy improved",
    "policy caused improvement",
    "policy caused harm",
    "policy response was effective",
    "agency response was effective",
    "\u653f\u7b56\u6709\u6548",
    "\u653f\u7b56\u65e0\u6548",
    "\u653f\u7b56\u5931\u8d25",
    "\u653f\u7b56\u6210\u529f",
    "\u653f\u7b56\u6539\u5584",
    "\u653f\u7b56\u5bfc\u81f4\u6539\u5584",
)
RESPONSIBILITY_CLAIM_PHRASES = (
    "agency is responsible for",
    "agency was responsible for",
    "government is responsible for",
    "government was responsible for",
    "policy responsibility",
    "regulatory responsibility",
    "accountable for the harm",
    "responsible for the health outcome",
    "responsible for exposure",
    "failed to protect",
    "\u673a\u6784\u5bf9",
    "\u653f\u5e9c\u5bf9",
    "\u653f\u7b56\u8d23\u4efb",
    "\u76d1\u7ba1\u8d23\u4efb",
    "\u5bf9\u5065\u5eb7\u7ed3\u679c\u8d1f\u8d23",
    "\u5bf9\u66b4\u9732\u8d1f\u8d23",
)
POLICY_EVALUATION_BASIS_MARKERS = (
    "policy evaluation basis",
    "policy_recommendations",
    "proposal",
    "evidence-bundle",
    "finding-record",
    "policy option",
    "options-and-tradeoffs",
    "decision_packet",
    "\u653f\u7b56\u8bc4\u4f30\u4f9d\u636e",
    "\u653f\u7b56\u9009\u9879",
    "\u6743\u8861",
)
OFFICIAL_ACTION_GOVERNANCE_BASIS_MARKERS = (
    "official action",
    "official-action",
    "agency action",
    "agency response",
    "governance record",
    "governance-record",
    "formal record",
    "formal-record",
    "federal register",
    "regulations.gov",
    "docket",
    "notice of",
    "rulemaking",
    "policy record",
    "policy action",
    "\u5b98\u65b9\u884c\u52a8",
    "\u673a\u6784\u884c\u52a8",
    "\u6cbb\u7406\u8bb0\u5f55",
    "\u6b63\u5f0f\u8bb0\u5f55",
    "\u653f\u7b56\u8bb0\u5f55",
)
INSUFFICIENT_BASIS_MARKERS = (
    "insufficient",
    "missing",
    "absent",
    "blocked",
    "unsupported",
    "downgrade",
    "no official",
    "no governance",
    "without official",
    "\u7f3a\u5c11",
    "\u4e0d\u8db3",
    "\u964d\u7ea7",
    "\u672a\u652f\u6491",
)
POLICY_EVALUATION_ACQUISITION_FIELDS = (
    "theme_id",
    "theme_question",
    "source_family",
    "source_families",
    "source_family_candidates",
    "source_skill",
    "query_variant_plan",
    "evidence_obligations",
    "success_criteria",
    "denominator_obligations",
    "expected_artifacts",
    "acquisition_lane",
    "lane_key",
    "object_kind",
    "target_kind",
)
ENVIRONMENT_ATTRIBUTION_PHRASES = (
    "caused by",
    "causal attribution",
    "source attribution",
    "transport attribution",
    "specific source",
    "specific origin",
    "specific fire",
    "proved the source",
    "proves the source",
    "\u6765\u6e90\u5f52\u56e0",
    "\u56e0\u679c\u5f52\u56e0",
    "\u8f93\u9001\u5f52\u56e0",
    "\u5177\u4f53\u6e90\u5934",
    "\u5177\u4f53\u6765\u6e90",
    "\u5177\u4f53\u706b\u573a",
    "\u8bc1\u660e\u6765\u6e90",
)
STRONG_ENVIRONMENT_ATTRIBUTION_PHRASES = (
    "proves source attribution",
    "proved source attribution",
    "proves physical source attribution",
    "proved physical source attribution",
    "proves transport attribution",
    "proved transport attribution",
    "proves causal attribution",
    "proved causal attribution",
    "establishes source attribution",
    "established source attribution",
    "confirms source attribution",
    "confirmed source attribution",
    "definitively caused by",
    "definitively proves",
    "\u8bc1\u660e\u6765\u6e90\u5f52\u56e0",
    "\u8bc1\u660e\u7269\u7406\u6765\u6e90\u5f52\u56e0",
    "\u8bc1\u660e\u8f93\u9001\u5f52\u56e0",
    "\u786e\u8ba4\u6765\u6e90\u5f52\u56e0",
)
BOUNDED_ATTRIBUTION_TERMS = (
    "compatible with",
    "compatibility",
    "consistent with",
    "descriptive relationship",
    "descriptive relation",
    "not attribution",
    "not proof of source",
    "not proof of attribution",
    "does not prove source",
    "does not prove attribution",
    "\u76f8\u5bb9",
    "\u63cf\u8ff0\u6027\u5173\u7cfb",
    "\u4e0d\u662f\u5f52\u56e0",
    "\u4e0d\u8bc1\u660e\u6765\u6e90",
)
ATTRIBUTION_MODEL_MARKERS = (
    "trajectory",
    "back trajectory",
    "plume",
    "chemistry",
    "chemical",
    "attribution model",
    "smoke model",
    "\u53cd\u5411\u8f68\u8ff9",
    "\u8f68\u8ff9",
    "\u70df\u7fbd",
    "\u5316\u5b66",
    "\u5f52\u56e0\u6a21\u578b",
)
ACQUISITION_ATTEMPT_TERMS = (
    "zero-signal",
    "zero signal",
    "receipt-only",
    "failed acquisition",
    "blocked acquisition",
    "executed-without-normalized-refs",
    "no normalized refs",
)
ACTIONABLE_PATH_TERMS = (
    "actionable path",
    "actionable route",
    "non-continuation rationale",
    "continue investigation",
    "continuation round",
    "no-actionable-path",
    "\u53ef\u884c\u52a8\u8c03\u67e5\u8def\u5f84",
    "\u7ee7\u7eed\u8c03\u67e5",
    "\u4e0d\u7ee7\u7eed\u8c03\u67e5\u7406\u7531",
)
ENVIRONMENT_STATE_SUBJECT_TERMS = (
    "environmental evidence",
    "environment data",
    "environmental data",
    "airnow",
    "openaq",
    "open-meteo",
    "open meteo",
    "usgs",
    "usbr rise",
    "firms",
    "pm2.5",
    "aqi",
    "water level",
    "reservoir level",
    "storage",
    "inflow",
    "release",
    "discharge",
    "elevation",
    "operating status",
    "operational status",
    "\u73af\u5883\u6570\u636e",
    "\u73af\u5883\u8bc1\u636e",
    "\u8fd0\u884c\u8bb0\u5f55",
    "\u8fd0\u884c\u6570\u636e",
    "\u8fd0\u884c\u72b6\u6001",
    "\u6c34\u4f4d",
    "\u5e93\u5bb9",
    "\u5165\u6d41",
    "\u4e0b\u6cc4",
    "\u6392\u653e",
    "\u6d41\u91cf",
    "\u706b\u70b9",
)
ENVIRONMENT_STATE_CLAIM_CUES = (
    "trend",
    "peaked",
    "peak",
    "maximum",
    "minimum",
    "highest",
    "lowest",
    "mean",
    "average",
    "range",
    "rose",
    "fell",
    "declined",
    "increased",
    "decreased",
    "recovered",
    "operating status",
    "operational status",
    "state of operations",
    "\u8d8b\u52bf",
    "\u5cf0\u503c",
    "\u9ad8\u503c",
    "\u4f4e\u503c",
    "\u6700\u9ad8",
    "\u6700\u4f4e",
    "\u5747\u503c",
    "\u5e73\u5747",
    "\u8303\u56f4",
    "\u4e0a\u5347",
    "\u4e0b\u964d",
    "\u56de\u843d",
    "\u6062\u590d",
    "\u8fd0\u884c\u72b6\u6001",
)
UNSUPPORTED_ENVIRONMENT_REPORT_PHRASES = (
    "receptor observation",
    "receptor observations",
    "environmental observation",
    "environmental pressure signal",
    "environment trend",
    "environmental trend",
    "exposure causality",
    "health outcome",
    "policy responsibility",
    "source attribution",
    "pollution source attribution",
    "\u53d7\u4f53\u4fa7\u73af\u5883\u89c2\u6d4b",
    "\u73af\u5883\u89c2\u6d4b",
    "\u73af\u5883\u538b\u529b\u4fe1\u53f7",
    "\u73af\u5883\u8d8b\u52bf",
    "\u66b4\u9732\u56e0\u679c",
    "\u66b4\u9732\u4f30\u8ba1",
    "\u5065\u5eb7\u7ed3\u679c",
    "\u653f\u7b56\u8d23\u4efb",
    "\u6765\u6e90\u5f52\u56e0",
    "\u6c61\u67d3\u6765\u6e90\u5f52\u56e0",
    "\u7269\u7406\u6765\u6e90",
)
REPORTABLE_ENVIRONMENT_BASIS_MARKERS = (
    "aggregate-environment-evidence",
    "environment_evidence_aggregation",
    "envagg-",
    "airnow",
    "openaq",
    "open-meteo",
    "open meteo",
    "usgs",
    "usbr rise",
    "firms",
    "monitoring station",
    "station observations",
    "water level",
    "storage",
    "inflow",
    "release",
    "discharge",
    "\u76d1\u6d4b\u70b9",
    "\u89c2\u6d4b\u7ad9",
    "\u706b\u70b9",
    "\u6c34\u4f4d",
    "\u5e93\u5bb9",
    "\u5165\u6d41",
    "\u4e0b\u6cc4",
    "\u6d41\u91cf",
)
ENVIRONMENT_AGGREGATION_MARKERS = (
    "aggregate-environment-evidence",
    "environment_evidence_aggregation",
    "envagg-",
    "environment aggregation",
    "environment evidence aggregation",
)
ITEM_LEVEL_ENVIRONMENT_CAVEAT_TERMS = (
    "item-level example",
    "item level example",
    "selected item",
    "selected items",
    "illustrative item",
    "illustrative example",
    "single observation",
    "single row",
    "not an aggregate",
    "not a trend",
    "\u5355\u6761\u8bc1\u636e",
    "\u5355\u6761\u8bb0\u5f55",
    "\u6761\u76ee\u7ea7",
    "\u793a\u4f8b",
    "\u4e0d\u662f\u805a\u5408",
    "\u4e0d\u6784\u6210\u8d8b\u52bf",
)
RELATION_REVIEW_BASIS_MARKERS = (
    "materialize-spatiotemporal-relation-evidence-packet",
    "spatiotemporal relation evidence packet",
    "detect-temporal-cooccurrence-cues",
    "temporal co-occurrence",
    "review-spatiotemporal-relation-alternatives",
    "review-fact-check-evidence-scope",
    "fact-check evidence scope",
    "review-evidence-sufficiency",
    "challenger",
    "review-comment",
    "challenge",
    "alternative explanation",
    "alternatives",
    "\u8d28\u8be2",
    "\u6311\u6218",
    "\u66ff\u4ee3\u89e3\u91ca",
    "\u4e8b\u5b9e\u6838\u67e5",
    "\u5173\u7cfb\u8bc1\u636e",
)
NEGATION_CUES = (
    "not ",
    "not a ",
    "not as ",
    "not be ",
    "must not ",
    "do not ",
    "cannot ",
    "can't ",
    "without ",
    "does not ",
    "should not ",
    "requires ",
    "require ",
    "would require ",
    "stronger ",
    "\u4e0d\u5f97",
    "\u4e0d\u80fd",
    "\u4e0d\u5e94",
    "\u4e0d\u662f",
    "\u5e76\u975e",
    "\u800c\u975e",
    "\u4e0d\u53ef",
    "\u672a\u80fd",
    "\u672a\u88ab\u8bb0\u5f55\u652f\u6491",
    "\u7f3a\u5c11",
    "\u6ca1\u6709",
    "\u65e0",
    "\u4e0d\u652f\u6301",
    "\u4e0d\u80fd\u652f\u6301",
    "\u6392\u9664",
    "\u5fc5\u987b\u6392\u9664",
    "\u82e5\u8981",
    "\u9700\u8981\u8865\u5145",
    "\u9700\u8865\u5145",
)
POST_PHRASE_BOUNDARY_CUES = (
    " exclusion boundary",
    " excluded",
    " exclusion",
    " must be excluded",
    " must not",
    " cannot",
    " should not",
    " basis gap",
    " downgraded",
    " downgrade",
    "\u6392\u9664\u8fb9\u754c",
    "\u5f3a\u5236\u6392\u9664",
    "\u4e0d\u5f97",
    "\u4e0d\u80fd",
    "\u4e0d\u5e94",
    "\u4e0d\u662f",
    "\u5e76\u975e",
    "\u800c\u975e",
    "\u4e0d\u4ee3\u8868",
    "\u4e0d\u9002\u5408",
    "\u964d\u7ea7",
    "\u5199\u6210\u7f3a\u53e3",
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}.")
    return payload


def load_json_file_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return load_json_file(path)


def load_text_file_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def issue(code: str, message: str, severity: str = "warning") -> dict[str, str]:
    return {"code": code, "severity": severity, "message": message}


def strings_from(value: Any) -> list[str]:
    results: list[str] = []
    if isinstance(value, dict):
        for child in value.values():
            results.extend(strings_from(child))
    elif isinstance(value, list):
        for child in value:
            results.extend(strings_from(child))
    else:
        text = maybe_text(value)
        if text:
            results.append(text)
    return results


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def report_prose_text(draft: dict[str, Any]) -> str:
    parts: list[Any] = [draft.get("title")]
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    parts.append(boundary.get("summary"))
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for section in sections:
        if not isinstance(section, dict):
            continue
        parts.append(section.get("title"))
        paragraphs = section.get("paragraphs") if isinstance(section.get("paragraphs"), list) else []
        parts.extend(paragraphs)
    return "\n".join(unique_texts(parts))


def normalized_text(text: str) -> str:
    return maybe_text(text).casefold()


def significant_terms(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{3,}", normalized_text(text))
        if token not in SITUATION_ANALYSIS_CONSUMPTION_STOPWORDS
    }


def phrase_is_negated(text: str, start: int) -> bool:
    prefix = text[max(0, start - 120) : start]
    suffix = text[start : min(len(text), start + 120)]
    return any(cue in prefix for cue in NEGATION_CUES) or any(
        cue in suffix for cue in POST_PHRASE_BOUNDARY_CUES
    )


def contains_unnegated_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    lowered = normalized_text(text)
    for phrase in phrases:
        lowered_phrase = phrase.casefold()
        search_from = 0
        while True:
            index = lowered.find(lowered_phrase, search_from)
            if index < 0:
                break
            if not phrase_is_negated(lowered, index):
                return True
            search_from = index + len(lowered_phrase)
    return False


def contains_any_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    lowered = normalized_text(text)
    return any(phrase.casefold() in lowered for phrase in phrases)


def public_opinion_percentage_present(text: str) -> bool:
    lowered = normalized_text(text)
    if not contains_any_phrase(lowered, PUBLIC_OPINION_DENOMINATOR_PHRASES):
        return False
    percent_patterns = (
        r"\b\d+(?:\.\d+)?\s*%\s+(?:of|among)\s+(?:the\s+)?(?:public|residents|affected residents|affected population)\b",
        r"(?:of|among)\s+(?:the\s+)?(?:public|residents|affected residents|affected population)\s*,?\s+\d+(?:\.\d+)?\s*%",
        r"[\u516c\u4f17\u5c45\u6c11\u4eba\u7fa4]{2,12}\u4e2d\s*\d+(?:\.\d+)?\s*%",
    )
    return any(re.search(pattern, lowered) for pattern in percent_patterns)


def formal_comment_public_opinion_upgrade_present(text: str) -> bool:
    lowered = normalized_text(text)
    if contains_unnegated_phrase(lowered, FORMAL_COMMENT_PUBLIC_OPINION_PHRASES):
        return True
    has_formal_sample = contains_any_phrase(
        lowered,
        (
            "formal comment",
            "formal comments",
            "formal participation",
            "regulations.gov",
            "docket comment",
            "docket comments",
            "\u6b63\u5f0f\u610f\u89c1",
            "\u6b63\u5f0f\u8bc4\u8bba",
        ),
    )
    return has_formal_sample and (
        contains_unnegated_phrase(lowered, PUBLIC_OPINION_UPGRADE_PHRASES)
        or public_opinion_percentage_present(lowered)
    )


def formal_comment_structure_claim_present(text: str) -> bool:
    lowered = normalized_text(text)
    return contains_any_phrase(lowered, FORMAL_COMMENT_STRUCTURE_TERMS) and contains_any_phrase(
        lowered,
        FORMAL_COMMENT_STRUCTURE_CLAIM_TERMS,
    )


def formal_comment_stance_distribution_present(text: str) -> bool:
    if text_window_contains(
        text,
        FORMAL_COMMENT_STRUCTURE_TERMS,
        FORMAL_COMMENT_STANCE_DISTRIBUTION_TERMS,
        window=220,
    ):
        return True
    return (
        text_window_contains(
            text,
            FORMAL_COMMENT_STRUCTURE_TERMS,
            ("support", "oppose", "opposed", "stance", "\u652f\u6301", "\u53cd\u5bf9"),
            window=220,
        )
        and text_window_contains(
            text,
            FORMAL_COMMENT_STRUCTURE_TERMS,
            ("distribution", "proportion", "share", "most", "majority", "%", "\u5206\u5e03", "\u6bd4\u4f8b"),
            window=220,
        )
    )


def formal_attachment_discussion_present(text: str) -> bool:
    return contains_any_phrase(text, FORMAL_ATTACHMENT_DISCUSSION_TERMS)


def formal_attachment_limitation_visible(text: str) -> bool:
    return contains_any_phrase(text, FORMAL_ATTACHMENT_LIMITATION_TERMS)


def source_narrative_discussion_present(text: str) -> bool:
    return contains_any_phrase(text, SOURCE_NARRATIVE_DISCUSSION_TERMS)


def source_narrative_boundary_visible(text: str) -> bool:
    return contains_any_phrase(text, SOURCE_NARRATIVE_BOUNDARY_TERMS)


def sample_fraction_totalized_present(text: str) -> bool:
    return contains_unnegated_phrase(text, SAMPLE_FRACTION_TOTALIZATION_PHRASES)


def mission_text(run_dir: Path | None) -> str:
    if run_dir is None:
        return ""
    candidates = [
        run_dir / "mission.json",
        run_dir / "input" / "mission.json",
        run_dir / "inputs" / "mission.json",
    ]
    parts: list[str] = []
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = load_json_file(path)
        except (OSError, ValueError, json.JSONDecodeError):
            parts.append(load_text_file_if_exists(path))
            continue
        parts.extend(strings_from(payload))
    return "\n".join(unique_texts(parts))


def mission_has_representative_sampling_design(run_dir: Path | None) -> bool:
    return contains_any_phrase(mission_text(run_dir), REPRESENTATIVE_SAMPLING_DESIGN_TERMS)


def section_by_id(draft: dict[str, Any], section_id: str) -> dict[str, Any]:
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for section in sections:
        if isinstance(section, dict) and maybe_text(section.get("section_id")) == section_id:
            return section
    return {}


def all_evidence_refs(draft: dict[str, Any]) -> list[str]:
    refs = list(draft.get("evidence_refs")) if isinstance(draft.get("evidence_refs"), list) else []
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for section in sections:
        if isinstance(section, dict) and isinstance(section.get("evidence_refs"), list):
            refs.extend(section["evidence_refs"])
    return unique_texts(refs)


def council_object_counts(draft: dict[str, Any]) -> dict[str, int]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    counts = (
        source_material.get("council_object_counts")
        if isinstance(source_material.get("council_object_counts"), dict)
        else {}
    )
    normalized: dict[str, int] = {}
    for key, value in counts.items():
        if isinstance(key, str):
            try:
                normalized[key] = int(value)
            except (TypeError, ValueError):
                normalized[key] = 0
    return normalized


def source_material_dict(draft: dict[str, Any]) -> dict[str, Any]:
    source_material = draft.get("source_material")
    return source_material if isinstance(source_material, dict) else {}


def source_material_list(draft: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = source_material_dict(draft).get(key)
    return [item for item in list_items(value) if isinstance(item, dict)]


def reporting_artifact_rows(draft: dict[str, Any]) -> list[dict[str, Any]]:
    return source_material_list(draft, "reporting_artifacts")


def section_brief_rows(draft: dict[str, Any]) -> list[dict[str, Any]]:
    return source_material_list(draft, "section_briefs")


def theme_sufficiency_review_rows(draft: dict[str, Any]) -> list[dict[str, Any]]:
    return source_material_list(draft, "theme_sufficiency_reviews")


def theme_progress_review_rows(draft: dict[str, Any]) -> list[dict[str, Any]]:
    return source_material_list(draft, "theme_progress_reviews")


def situation_analysis_brief_rows(draft: dict[str, Any]) -> list[dict[str, Any]]:
    return source_material_list(draft, "situation_analysis_briefs")


def situation_analysis_anchor_phrases(brief: dict[str, Any]) -> list[str]:
    anchors: list[str] = [
        maybe_text(brief.get("central_bounded_judgement")),
        maybe_text(brief.get("mission_answerable_question")),
    ]
    for item in list_items(brief.get("recommended_report_spine")):
        anchors.extend(strings_from(item))
    for field_name in SITUATION_ANALYSIS_CHAIN_FIELDS:
        for item in list_items(brief.get(field_name)):
            anchors.extend(strings_from(item))
    return [
        text
        for text in unique_texts(anchors)
        if len(normalized_text(text)) >= 24
    ]


def situation_analysis_phrase_visible_in_prose(phrase: str, prose: str) -> bool:
    phrase_text = normalized_text(phrase)
    prose_text = normalized_text(prose)
    if phrase_text and phrase_text in prose_text:
        return True
    phrase_terms = significant_terms(phrase_text)
    if len(phrase_terms) < 4:
        return False
    prose_terms = significant_terms(prose_text)
    overlap = phrase_terms & prose_terms
    required = max(4, min(6, int(len(phrase_terms) * 0.6)))
    return len(overlap) >= required


def situation_analysis_brief_consumption_issues(draft: dict[str, Any]) -> list[dict[str, str]]:
    briefs = situation_analysis_brief_rows(draft)
    if not briefs:
        return []
    source_material = source_material_dict(draft)
    reader_guidance = draft.get("reader_guidance") if isinstance(draft.get("reader_guidance"), dict) else {}
    prefers_situation_brief = bool(source_material.get("situation_analysis_preferred")) or bool(
        reader_guidance.get("situation_analysis_brief_first")
    )
    anchors = [
        phrase
        for brief in briefs
        for phrase in situation_analysis_anchor_phrases(brief)
    ]
    if not anchors:
        return []
    prose = report_prose_text(draft)
    carries_anchor = any(situation_analysis_phrase_visible_in_prose(phrase, prose) for phrase in anchors)
    if prefers_situation_brief and carries_anchor:
        return []
    return [
        issue(
            "situation-analysis-brief-not-consumed",
            (
                "Situation-analysis brief is present, but reader-facing prose does not visibly carry "
                "its mission answer or recommended report spine."
            ),
            "warning",
        )
    ]


def situation_analysis_brief_quality_issues(draft: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    briefs = situation_analysis_brief_rows(draft)
    if not briefs:
        return issues
    if not any(maybe_text(brief.get("mission_answerable_question")) for brief in briefs):
        issues.append(
            issue(
                "missing-situation-analysis-mission-question",
                "Situation-analysis brief should preserve the mission-answerable question it is organizing the report around.",
                "warning",
            )
        )
    if not any(maybe_text(brief.get("central_bounded_judgement")) for brief in briefs):
        issues.append(
            issue(
                "missing-mission-answer",
                "Situation-analysis brief is present but does not state a central bounded judgement.",
                "warning",
            )
        )
    if not any(list_items(brief.get("recommended_report_spine")) for brief in briefs):
        issues.append(
            issue(
                "weak-report-mainline",
                "Situation-analysis brief is present but does not provide a recommended report spine.",
                "warning",
            )
        )
    missing_chain_fields = [
        field_name
        for field_name in SITUATION_ANALYSIS_CHAIN_FIELDS
        if not any(list_items(brief.get(field_name)) for brief in briefs)
    ]
    if missing_chain_fields:
        issues.append(
            issue(
                "situation-analysis-brief-missing-chain",
                (
                    "Situation-analysis brief should index fact, official-action, public-semantic, "
                    "policy-semantic, interaction, and stage chains before narrative writing. "
                    "Missing: "
                    + ", ".join(missing_chain_fields)
                    + "."
                ),
                "warning",
            )
        )
    if not any(
        list_items(brief.get("unresolved_claim_needs"))
        or list_items(brief.get("downgraded_claims"))
        or list_items(brief.get("forbidden_writing_upgrades"))
        for brief in briefs
    ):
        issues.append(
            issue(
                "situation-analysis-unresolved-index-missing",
                (
                    "Situation-analysis brief should carry unresolved claim needs, downgraded claims, "
                    "or forbidden writing upgrades so report validation can audit unfinished claim boundaries."
                ),
                "warning",
            )
        )
    return issues


def review_carried_by_council(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    uptake_status = maybe_text(
        row.get("uptake_status")
        or row.get("council_uptake_status")
        or row.get("report_basis_status")
    ).casefold()
    if uptake_status in {
        "accepted",
        "adopted",
        "carried",
        "council-carried",
        "report-basis-carried",
        "accepted-for-reporting",
    }:
        return True
    return any(
        maybe_text(row.get(field_name))
        for field_name in (
            "accepted_by_object_id",
            "adopted_by_object_id",
            "carried_by_object_id",
            "moderator_synthesis_id",
            "readiness_opinion_id",
            "report_basis_freeze_id",
            "transition_approval_id",
        )
    )


def carried_theme_review_rows(draft: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        row
        for row in [
            *theme_sufficiency_review_rows(draft),
            *theme_progress_review_rows(draft),
        ]
        if review_carried_by_council(row)
    ]


def challenger_boundary_review_visible(draft: dict[str, Any]) -> bool:
    if any(
        maybe_text(row.get("agent_role")) == "challenger"
        or "challenger" in maybe_text(row.get("section_key")).casefold()
        or "challenger" in maybe_text(row.get("section_role")).casefold()
        for row in section_brief_rows(draft)
    ):
        return True
    source_material = source_material_dict(draft)
    role_counts = (
        source_material.get("agent_role_counts")
        if isinstance(source_material.get("agent_role_counts"), dict)
        else {}
    )
    if int_value(role_counts.get("challenger")) > 0:
        return True
    counts = council_object_counts(draft)
    return counts.get("review-comment", 0) > 0 or counts.get("challenge", 0) > 0


def frozen_or_reporting_basis_visible(draft: dict[str, Any]) -> bool:
    if any(
        isinstance(row, dict) and maybe_text(row.get("kind")) == "report-basis-freeze"
        for row in reporting_artifact_rows(draft)
    ):
        return True
    counts = council_object_counts(draft)
    return any(
        counts.get(kind, 0) > 0
        for kind in (
            "report-basis-freeze",
            "finding",
            "evidence-bundle",
            "round-synthesis",
            "agent-position",
        )
    )


def claim_chain_basis_visible(draft: dict[str, Any]) -> bool:
    return (
        bool(section_brief_rows(draft))
        or bool(carried_theme_review_rows(draft))
        or frozen_or_reporting_basis_visible(draft)
    )


def has_public_discourse_basis(draft: dict[str, Any], text: str) -> bool:
    source_material = source_material_dict(draft)
    public_summary = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    if any(maybe_text(public_summary.get(key)) for key in ("path", "summary_id", "status")):
        return True
    formal_helper = (
        source_material.get("formal_policy_helper_summary")
        if isinstance(source_material.get("formal_policy_helper_summary"), dict)
        else {}
    )
    if any(maybe_text(formal_helper.get(key)) for key in ("coverage_audit_id", "annotation_set_id")):
        return True
    refs_text = "\n".join(all_evidence_refs(draft))
    return contains_any_phrase("\n".join([text, refs_text]), PUBLIC_DISCOURSE_BASIS_MARKERS)


def public_discourse_summary_contract_issues(
    draft: dict[str, Any],
    *,
    run_dir: Path,
) -> list[dict[str, str]]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    public_summary_meta = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    public_section = section_by_id(draft, "public-discourse-deepening")
    if not public_section and not any(maybe_text(public_summary_meta.get(key)) for key in ("path", "summary_id", "status")):
        return []

    issues: list[dict[str, str]] = []
    summary_path_text = maybe_text(public_summary_meta.get("path"))
    if not summary_path_text:
        issues.append(
            issue(
                "public-summary-path-missing",
                "Public discourse addendum metadata should include the helper artifact path.",
                "warning",
            )
        )
        return issues

    summary_path = resolve_path(run_dir, summary_path_text, summary_path_text)
    summary_payload = load_json_file_if_exists(summary_path)
    if not summary_payload:
        issues.append(
            issue(
                "public-summary-artifact-missing",
                "Public discourse addendum metadata points to a helper artifact that was not found.",
                "warning",
            )
        )
        return issues

    if maybe_text(summary_payload.get("schema_version")) != "optional-analysis-public-discourse-sample-summary-v1":
        issues.append(
            issue(
                "public-summary-unexpected-schema",
                "Public discourse summary should use optional-analysis-public-discourse-sample-summary-v1.",
                "warning",
            )
        )
    if maybe_text(summary_payload.get("skill")) != "summarize-public-discourse-sample":
        issues.append(
            issue(
                "public-summary-unexpected-skill",
                "Public discourse summary should come from summarize-public-discourse-sample or equivalent approved helper basis.",
                "warning",
            )
        )
    required_fields = (
        "sample_definition",
        "source_family_counts",
        "discourse_lane_counts",
        "warnings",
        "evidence_refs",
        "distribution_use_policy",
    )
    for field_name in required_fields:
        value = summary_payload.get(field_name)
        if field_name in {"sample_definition", "distribution_use_policy"}:
            present = isinstance(value, dict) and bool(value)
        else:
            present = isinstance(value, list)
        if not present:
            issues.append(
                issue(
                    "public-summary-contract-incomplete",
                    f"Public discourse summary is missing or has an invalid `{field_name}` field.",
                    "warning",
                )
            )
    has_distribution = any(
        isinstance(summary_payload.get(field_name), list) and bool(summary_payload.get(field_name))
        for field_name in (
            "issue_distribution",
            "social_affect_distribution",
            "source_narrative_distribution",
            "actor_responsibility_distribution",
            "action_orientation_distribution",
        )
    )
    if not has_distribution:
        issues.append(
            issue(
                "public-summary-no-label-distribution",
                "Public discourse summary carries no label distributions; report prose should stay at visibility/source-family boundary.",
                "warning",
            )
        )
    policy = summary_payload.get("distribution_use_policy") if isinstance(summary_payload.get("distribution_use_policy"), dict) else {}
    expected_policy = {
        "label_sets_are_non_exclusive": True,
        "sample_fractions_are_sample_local": True,
        "do_not_sum_to_population_opinion": True,
        "requires_council_uptake_before_reporting": True,
    }
    for key, expected in expected_policy.items():
        if policy.get(key) is not expected:
            issues.append(
                issue(
                    "public-summary-policy-boundary-missing",
                    f"Public discourse summary distribution_use_policy should set `{key}` to true.",
                    "warning",
                )
            )
    if maybe_text(policy.get("gdelt_tone_boundary")) not in {"media_or_document_tone_not_public_sentiment", ""}:
        issues.append(
            issue(
                "public-summary-gdelt-boundary-unexpected",
                "Public discourse summary has an unexpected GDELT tone boundary marker.",
                "warning",
            )
        )
    if maybe_text(policy.get("source_narrative_boundary")) not in {"public_source_narrative_cue_not_physical_source_attribution", ""}:
        issues.append(
            issue(
                "public-summary-source-narrative-boundary-unexpected",
                "Public discourse summary has an unexpected source narrative boundary marker.",
                "warning",
            )
        )
    return issues


def artifact_candidate_paths(draft: dict[str, Any], *, run_dir: Path) -> list[Path]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    values = [*strings_from(source_material), *all_evidence_refs(draft)]
    paths: list[Path] = []
    seen: set[str] = set()
    for value in values:
        for token in re.split(r"[\s,;]+", maybe_text(value)):
            if ".json" not in token:
                continue
            cleaned = token.strip("`'\"()[]{}<>")
            json_index = cleaned.find(".json")
            if json_index < 0:
                continue
            candidate_text = cleaned[: json_index + len(".json")]
            candidate = resolve_path(run_dir, candidate_text, candidate_text)
            key = str(candidate)
            if key in seen or not candidate.exists():
                continue
            seen.add(key)
            paths.append(candidate)
    return paths


def helper_artifacts_from_draft(draft: dict[str, Any], *, run_dir: Path | None) -> list[dict[str, Any]]:
    if run_dir is None:
        return []
    artifacts: list[dict[str, Any]] = []
    for path in artifact_candidate_paths(draft, run_dir=run_dir):
        payload = load_json_file_if_exists(path)
        if payload:
            payload = dict(payload)
            payload["_artifact_path"] = str(path)
            artifacts.append(payload)
    return artifacts


def int_value(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = maybe_text(value)
    if not text:
        return 0
    try:
        return max(0, int(float(text)))
    except ValueError:
        return 0


def count_from_rows(rows: Any, *, name_field: str, names: set[str], count_field: str) -> int:
    if not isinstance(rows, list):
        return 0
    total = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = maybe_text(row.get(name_field))
        if name in names:
            total += int_value(row.get(count_field))
    return total


def formal_distribution_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field_name in ("annotation_distributions", "formal_issue_distribution", "formal_stance_distribution"):
        value = payload.get(field_name)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    sample_internal = payload.get("sample_internal_distribution")
    if isinstance(sample_internal, dict):
        rows.extend(formal_distribution_rows(sample_internal))
    return rows


def formal_comment_basis_summary(draft: dict[str, Any], *, run_dir: Path | None) -> dict[str, Any]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    formal_meta: dict[str, Any] = {}
    for key in (
        "formal_comment_basis",
        "formal_comment_sample",
        "formal_comment_annotations",
        "formal_policy_helper_summary",
    ):
        value = source_material.get(key)
        if isinstance(value, dict):
            formal_meta.update(value)
    source_skill_counts = (
        formal_meta.get("source_skill_counts")
        if isinstance(formal_meta.get("source_skill_counts"), dict)
        else {}
    )

    readable_count = max(
        int_value(formal_meta.get("readable_formal_signal_count")),
        int_value(formal_meta.get("formal_comment_text_signal_count")),
        int_value(formal_meta.get("formal_signal_count")),
        int_value(formal_meta.get("comment_detail_count")),
        int_value(source_skill_counts.get("fetch-regulationsgov-comment-detail")),
        int_value(formal_meta.get("attachment_text_signal_count")),
        int_value(formal_meta.get("sample_count")),
        int_value(formal_meta.get("annotation_sample_count")),
    )
    annotation_count = max(
        int_value(formal_meta.get("formal_annotation_count")),
        int_value(formal_meta.get("annotation_count")),
    )
    candidate_audit_count = max(
        int_value(formal_meta.get("candidate_audit_count")),
        int_value(formal_meta.get("formal_candidate_audit_count")),
        int_value(formal_meta.get("coverage_sample_count")),
    )
    attachment_text_count = int_value(formal_meta.get("attachment_text_signal_count"))
    listing_count = max(
        int_value(formal_meta.get("comment_listing_count")),
        int_value(source_skill_counts.get("fetch-regulationsgov-comments")),
    )
    detail_count = max(
        int_value(formal_meta.get("comment_detail_count")),
        int_value(source_skill_counts.get("fetch-regulationsgov-comment-detail")),
    )
    has_formal_annotation_distribution = annotation_count > 0
    has_candidate_audit = candidate_audit_count > 0 or any(
        maybe_text(formal_meta.get(key))
        for key in (
            "candidate_audit_ref",
            "candidate_audit_path",
            "formal_candidate_audit_ref",
            "formal_candidate_audit_path",
            "coverage_audit_id",
        )
    )

    for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir):
        skill = maybe_text(artifact.get("skill"))
        if skill == "audit-formal-comment-candidate-corpus":
            has_candidate_audit = True
            candidate_audit_count = max(
                candidate_audit_count,
                int_value(artifact.get("candidate_signal_count")),
                int_value(artifact.get("candidate_count")),
                int_value(artifact.get("sample_count")),
            )
        elif skill == "classify-formal-comment-issues":
            readable_count = max(readable_count, int_value(artifact.get("sample_count")))
            annotation_count = max(annotation_count, int_value(artifact.get("annotation_count")))
            for row in artifact.get("annotations") if isinstance(artifact.get("annotations"), list) else []:
                if isinstance(row, dict) and maybe_text(row.get("source_skill")) == "fetch-regulationsgov-attachments":
                    attachment_text_count += 1
            has_formal_annotation_distribution = has_formal_annotation_distribution or annotation_count > 0
        elif skill in {"materialize-public-discourse-corpus", "summarize-public-discourse-sample"}:
            formal_lane_count = count_from_rows(
                artifact.get("discourse_lane_counts"),
                name_field="discourse_lane",
                names={"formal_public_comment_sample", "formal_record_text"},
                count_field="signal_count",
            )
            readable_count = max(readable_count, formal_lane_count)
            formal_source_count = count_from_rows(
                artifact.get("source_family_counts"),
                name_field="source_family",
                names={"regulationsgov-formal-comments", "formal-record"},
                count_field="signal_count",
            )
            readable_count = max(readable_count, formal_source_count)
            if skill == "summarize-public-discourse-sample" and max(formal_lane_count, formal_source_count) > 0:
                label_rows: list[Any] = []
                for field_name in (
                    "issue_distribution",
                    "social_affect_distribution",
                    "source_narrative_distribution",
                    "actor_responsibility_distribution",
                    "action_orientation_distribution",
                    "annotation_distributions",
                ):
                    if isinstance(artifact.get(field_name), list):
                        label_rows.extend(artifact[field_name])
                if any(isinstance(row, dict) for row in label_rows):
                    has_formal_annotation_distribution = True
                    annotation_count = max(
                        annotation_count,
                        sum(
                            int_value(row.get("annotated_signal_count"))
                            for row in label_rows
                            if isinstance(row, dict)
                        ),
                    )
        elif skill == "aggregate-public-discourse-annotations":
            formal_rows = [
                row
                for row in formal_distribution_rows(artifact)
                if maybe_text(row.get("label_family")).startswith("formal_")
            ]
            if formal_rows:
                has_formal_annotation_distribution = True
                annotation_count = max(
                    annotation_count,
                    sum(int_value(row.get("annotated_signal_count")) for row in formal_rows),
                )

    source_text = "\n".join(strings_from(source_material))
    attachment_limited = contains_any_phrase(
        source_text,
        (
            "requires-attachment-text",
            "requires attachment text",
            "text-extraction-limited",
            "pdf-reader-unavailable",
            "unreadable attachment",
            "scanned pdf",
        ),
    )
    return {
        "readable_count": readable_count,
        "annotation_count": annotation_count,
        "attachment_text_count": attachment_text_count,
        "listing_count": listing_count,
        "detail_count": detail_count,
        "candidate_audit_count": candidate_audit_count,
        "has_candidate_audit": has_candidate_audit,
        "has_readable_formal_text": readable_count > 0,
        "has_annotation_basis": annotation_count > 0 or has_formal_annotation_distribution,
        "attachment_limited": attachment_limited,
    }


def public_discourse_summary_payload(draft: dict[str, Any], *, run_dir: Path | None) -> dict[str, Any]:
    if run_dir is None:
        return {}
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    public_summary_meta = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    summary_path_text = maybe_text(public_summary_meta.get("path"))
    if summary_path_text:
        payload = load_json_file_if_exists(resolve_path(run_dir, summary_path_text, summary_path_text))
        if payload:
            return payload
    for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir):
        if maybe_text(artifact.get("skill")) == "summarize-public-discourse-sample":
            return artifact
    return {}


def distribution_rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field_name in (
        "issue_distribution",
        "social_affect_distribution",
        "source_narrative_distribution",
        "actor_responsibility_distribution",
        "action_orientation_distribution",
        "annotation_distributions",
    ):
        value = payload.get(field_name)
        if isinstance(value, list):
            rows.extend(row for row in value if isinstance(row, dict))
    sample_internal = payload.get("sample_internal_distribution")
    if isinstance(sample_internal, dict):
        rows.extend(distribution_rows_from_payload(sample_internal))
    return rows


def public_discourse_basis_summary(draft: dict[str, Any], *, run_dir: Path | None) -> dict[str, bool]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    basis_meta: dict[str, Any] = {}
    for key in (
        "public_discourse_basis",
        "public_discourse_sample",
        "public_discourse_annotations",
        "formal_policy_helper_summary",
    ):
        value = source_material.get(key)
        if isinstance(value, dict):
            basis_meta.update(value)
    summary_payload = public_discourse_summary_payload(draft, run_dir=run_dir)
    observed_inputs = (
        summary_payload.get("observed_inputs")
        if isinstance(summary_payload.get("observed_inputs"), dict)
        else {}
    )
    coverage_summary = (
        summary_payload.get("coverage_audit_summary")
        if isinstance(summary_payload.get("coverage_audit_summary"), dict)
        else {}
    )
    rows = distribution_rows_from_payload(summary_payload)
    source_text = "\n".join(strings_from(source_material))
    refs_text = "\n".join(all_evidence_refs(draft))
    artifact_text = "\n".join(
        "\n".join(strings_from(artifact))
        for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir)
    )
    combined_text = "\n".join([source_text, refs_text, artifact_text])
    has_corpus = any(
        maybe_text(value)
        for value in (
            basis_meta.get("corpus_path"),
            basis_meta.get("corpus_ref"),
            observed_inputs.get("corpus_path"),
            summary_payload.get("corpus_id"),
        )
    ) or int_value(basis_meta.get("coverage_sample_count")) > 0 or contains_any_phrase(
        combined_text, ("materialize-public-discourse-corpus", "public_discourse_corpus")
    )
    has_coverage_audit = any(
        maybe_text(value)
        for value in (
            basis_meta.get("coverage_audit_path"),
            basis_meta.get("coverage_audit_ref"),
            basis_meta.get("coverage_audit_id"),
            observed_inputs.get("coverage_audit_path"),
            coverage_summary.get("coverage_audit_id"),
        )
    ) or contains_any_phrase(combined_text, ("audit-public-discourse-sample-coverage", "public_discourse_coverage_audit"))
    has_annotation = bool(rows) or any(
        int_value(basis_meta.get(key)) > 0
        for key in ("annotation_count", "annotated_signal_count", "public_annotation_count")
    ) or contains_any_phrase(combined_text, ("classify-public-discourse-affect", "public_discourse_affect_annotations"))
    has_aggregation = any(
        maybe_text(value)
        for value in (
            basis_meta.get("aggregation_path"),
            basis_meta.get("aggregation_ref"),
            observed_inputs.get("aggregation_path"),
            summary_payload.get("aggregation_id"),
        )
    ) or bool(basis_meta.get("label_counts")) or contains_any_phrase(
        combined_text, ("aggregate-public-discourse-annotations", "public_discourse_annotation_aggregation")
    )
    has_denominator = any(
        int_value(row.get("label_family_denominator")) > 0
        or int_value(row.get("eligible_signal_count")) > 0
        for row in rows
    ) or any(
        int_value(basis_meta.get(key)) > 0
        for key in (
            "denominator",
            "sample_denominator",
            "eligible_signal_count",
            "annotation_sample_count",
        )
    )
    distribution_denominators = summary_payload.get("distribution_denominators")
    if isinstance(distribution_denominators, dict) and distribution_denominators:
        has_denominator = True
    sample_internal = summary_payload.get("sample_internal_distribution")
    if isinstance(sample_internal, dict) and isinstance(sample_internal.get("distribution_denominators"), dict):
        has_denominator = True
    return {
        "has_corpus": has_corpus,
        "has_coverage_audit": has_coverage_audit,
        "has_annotation": has_annotation,
        "has_aggregation": has_aggregation,
        "has_denominator": has_denominator,
    }


def source_family_count(payload: dict[str, Any]) -> int:
    rows = payload.get("source_family_counts")
    if not isinstance(rows, list):
        return 0
    return sum(
        1
        for row in rows
        if isinstance(row, dict) and maybe_text(row.get("source_family")) and int_value(row.get("signal_count")) > 0
    )


def public_semantic_source_family_denominator_visible(draft: dict[str, Any], *, run_dir: Path | None) -> bool:
    payload = public_discourse_summary_payload(draft, run_dir=run_dir)
    if source_family_count(payload) > 0:
        return True
    for key in ("source_family_denominators", "distribution_denominators"):
        value = payload.get(key)
        if isinstance(value, list) and any(
            isinstance(row, dict)
            and maybe_text(row.get("source_family"))
            and (
                int_value(row.get("denominator")) > 0
                or int_value(row.get("sample_count")) > 0
                or int_value(row.get("eligible_signal_count")) > 0
                or int_value(row.get("signal_count")) > 0
            )
            for row in value
        ):
            return True
        if isinstance(value, dict) and value:
            return True
    source_material = source_material_dict(draft)
    public_basis = (
        source_material.get("public_discourse_basis")
        if isinstance(source_material.get("public_discourse_basis"), dict)
        else {}
    )
    if any(
        isinstance(row, dict)
        and maybe_text(row.get("source_family"))
        and (
            int_value(row.get("denominator")) > 0
            or int_value(row.get("sample_count")) > 0
            or int_value(row.get("eligible_signal_count")) > 0
            or int_value(row.get("signal_count")) > 0
        )
        for row in list_items(public_basis.get("source_family_denominators"))
    ):
        return True
    formal_helper = (
        source_material.get("formal_policy_helper_summary")
        if isinstance(source_material.get("formal_policy_helper_summary"), dict)
        else {}
    )
    helper_family_counts = formal_helper.get("source_family_counts")
    if isinstance(helper_family_counts, dict) and any(int_value(value) > 0 for value in helper_family_counts.values()):
        return True
    if isinstance(helper_family_counts, list) and any(
        isinstance(row, dict)
        and maybe_text(row.get("source_family"))
        and int_value(row.get("signal_count")) > 0
        for row in helper_family_counts
    ):
        return True
    if any(
        isinstance(row, dict)
        and maybe_text(row.get("source_family"))
        and int_value(row.get("observed_signal_count")) > 0
        for row in list_items(formal_helper.get("coverage_cues"))
    ):
        return True
    for brief in section_brief_rows(draft):
        source_families = [maybe_text(value) for value in list_items(brief.get("source_families"))]
        denominators = brief.get("denominators") if isinstance(brief.get("denominators"), dict) else {}
        legacy_denominator = brief.get("denominator") if isinstance(brief.get("denominator"), dict) else {}
        denominator_text = "\n".join(strings_from([denominators, legacy_denominator]))
        if source_families and contains_any_phrase(denominator_text, ("denominator", "sample_count", "eligible_signal_count", "signal_count")):
            return True
    return False


def has_optional_analysis_carrier(draft: dict[str, Any], helper_id: str) -> bool:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    reporting_artifacts = (
        source_material.get("reporting_artifacts")
        if isinstance(source_material.get("reporting_artifacts"), list)
        else []
    )
    counts = council_object_counts(draft)
    if any(
        counts.get(kind, 0) > 0
        for kind in (
            "finding",
            "evidence-bundle",
            "agent-position",
            "readiness-opinion",
            "round-synthesis",
            "proposal",
        )
    ):
        return True
    reporting_artifact_text = "\n".join(strings_from(reporting_artifacts))
    if helper_id and helper_id.casefold() in reporting_artifact_text.casefold():
        return True
    if contains_any_phrase(reporting_artifact_text, PUBLIC_DISCOURSE_BASIS_MARKERS):
        return True
    return any(
        isinstance(row, dict) and maybe_text(row.get("kind")) == "report-basis-freeze"
        for row in reporting_artifacts
    )


def sample_distribution_language_present(text: str) -> bool:
    lowered = normalized_text(text)
    has_sample_language = any(
        marker in lowered
        for marker in (
            "sample-local",
            "sample local",
            "sample fraction",
            "sample_fraction",
            "sample distribution",
            "sample-level",
            "\u6837\u672c\u5185",
            "\u6837\u672c\u6bd4\u4f8b",
            "\u6837\u672c\u5206\u5e03",
        )
    )
    has_ratio = bool(re.search(r"\b\d+(?:\.\d+)?\s*%", lowered)) or "\u51fa\u73b0\u7387" in lowered
    return has_sample_language and has_ratio


def public_discourse_quantification_present(text: str) -> bool:
    lowered = normalized_text(text)
    has_quantity = (
        bool(re.search(r"\b\d+(?:\.\d+)?\s*%", lowered))
        or "sample_fraction" in lowered
        or "sample fraction" in lowered
        or "distribution" in lowered
        or "proportion" in lowered
        or "share" in lowered
        or "\u51fa\u73b0\u7387" in lowered
        or "\u6bd4\u4f8b" in lowered
        or "\u5206\u5e03" in lowered
    )
    return has_quantity and contains_any_phrase(lowered, PUBLIC_DISCOURSE_QUANTIFICATION_CUES)


def public_discourse_sample_boundary_visible(text: str) -> bool:
    return contains_any_phrase(text, PUBLIC_DISCOURSE_SAMPLE_BOUNDARY_TERMS)


def public_discourse_nonexclusive_boundary_visible(text: str) -> bool:
    return contains_any_phrase(text, PUBLIC_DISCOURSE_NONEXCLUSIVE_TERMS)


def sample_denominator_visible(text: str) -> bool:
    return contains_any_phrase(text, SAMPLE_DENOMINATOR_TERMS)


def small_sample_boundary_visible(text: str) -> bool:
    return public_discourse_sample_boundary_visible(text) or contains_any_phrase(text, SMALL_SAMPLE_BOUNDARY_TERMS)


def representativeness_limit_visible(text: str) -> bool:
    return contains_any_phrase(text, REPRESENTATIVENESS_LIMIT_TERMS)


def helper_marker_mentions(text: str) -> list[str]:
    lowered = normalized_text(text)
    return [
        marker
        for marker in OPTIONAL_HELPER_MARKERS
        if marker.casefold() in lowered
    ]


def text_window_contains(text: str, terms_a: tuple[str, ...], terms_b: tuple[str, ...], *, window: int = 160) -> bool:
    lowered = normalized_text(text)
    for term_a in terms_a:
        search_from = 0
        lowered_a = term_a.casefold()
        while True:
            index = lowered.find(lowered_a, search_from)
            if index < 0:
                break
            if phrase_is_negated(lowered, index):
                search_from = index + len(lowered_a)
                continue
            start = max(0, index - window)
            end = min(len(lowered), index + len(lowered_a) + window)
            for term_b in terms_b:
                lowered_b = term_b.casefold()
                search_b_from = start
                while True:
                    index_b = lowered.find(lowered_b, search_b_from, end)
                    if index_b < 0:
                        break
                    if not phrase_is_negated(lowered, index_b):
                        return True
                    search_b_from = index_b + len(lowered_b)
            search_from = index + len(lowered_a)
    return False


def gdelt_tone_as_public_sentiment_present(text: str) -> bool:
    if contains_unnegated_phrase(text, GDELT_TONE_PUBLIC_SENTIMENT_PHRASES):
        return True
    return text_window_contains(text, GDELT_TONE_TERMS, PUBLIC_SENTIMENT_TERMS)


def platform_or_docket_sample_generalization_present(text: str) -> bool:
    return text_window_contains(text, PLATFORM_OR_DOCKET_SAMPLE_TERMS, SAMPLE_GENERALIZATION_TERMS)


def environment_state_claim_present(text: str) -> bool:
    return text_window_contains(
        text,
        ENVIRONMENT_STATE_SUBJECT_TERMS,
        ENVIRONMENT_STATE_CLAIM_CUES,
        window=180,
    )


def environment_aggregation_basis_visible(draft: dict[str, Any], *, run_dir: Path | None) -> bool:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    env_meta = (
        source_material.get("environment_aggregation")
        if isinstance(source_material.get("environment_aggregation"), dict)
        else {}
    )
    if any(maybe_text(value) for value in env_meta.values()):
        return True
    text = "\n".join(
        [
            "\n".join(strings_from(source_material)),
            "\n".join(all_evidence_refs(draft)),
            "\n".join(
                "\n".join(strings_from(artifact))
                for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir)
            ),
        ]
    )
    return contains_any_phrase(text, ENVIRONMENT_AGGREGATION_MARKERS)


def reportable_environment_basis_visible(draft: dict[str, Any], *, run_dir: Path | None) -> bool:
    if environment_aggregation_basis_visible(draft, run_dir=run_dir):
        return True
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    combined = "\n".join(
        [
            "\n".join(strings_from(source_material)),
            "\n".join(all_evidence_refs(draft)),
            "\n".join(
                "\n".join(strings_from(artifact))
                for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir)
            ),
        ]
    )
    return contains_unnegated_phrase(combined, REPORTABLE_ENVIRONMENT_BASIS_MARKERS)


def unsupported_environment_claim_present(text: str) -> bool:
    return contains_unnegated_phrase(text, UNSUPPORTED_ENVIRONMENT_REPORT_PHRASES)


def item_level_environment_boundary_visible(text: str) -> bool:
    return contains_any_phrase(text, ITEM_LEVEL_ENVIRONMENT_CAVEAT_TERMS)


def relation_review_basis_visible(draft: dict[str, Any], text: str) -> bool:
    counts = council_object_counts(draft)
    if counts.get("review-comment", 0) > 0 or counts.get("challenge", 0) > 0:
        return True
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    combined = "\n".join(
        [
            text,
            "\n".join(strings_from(source_material)),
            "\n".join(all_evidence_refs(draft)),
        ]
    )
    return contains_any_phrase(combined, RELATION_REVIEW_BASIS_MARKERS)


def interaction_judgement_present(text: str) -> bool:
    if contains_unnegated_phrase(text, INTERACTION_JUDGEMENT_PHRASES):
        return True
    return text_window_contains(text, INTERACTION_SIDE_TERMS, INTERACTION_CLAIM_TERMS, window=220)


def interaction_timeline_basis_visible(draft: dict[str, Any], *, run_dir: Path | None) -> bool:
    source_material = source_material_dict(draft)
    timeline_meta = (
        source_material.get("interaction_timeline")
        if isinstance(source_material.get("interaction_timeline"), dict)
        else {}
    )
    if (
        int_value(timeline_meta.get("section_brief_count")) > 0
        and int_value(timeline_meta.get("interaction_node_count")) > 0
        and int_value(timeline_meta.get("lane_episode_card_count")) > 0
    ):
        return True
    lane_episode_cards = source_material.get("lane_episode_cards")
    if (
        isinstance(lane_episode_cards, list)
        and lane_episode_cards
        and int_value(timeline_meta.get("interaction_node_count")) > 0
    ):
        return True
    section_briefs = source_material.get("section_briefs")
    if isinstance(section_briefs, list) and any(
        isinstance(brief, dict)
        and isinstance(brief.get("denominator"), dict)
        and int_value(brief["denominator"].get("interaction_node_count")) > 0
        and int_value(brief["denominator"].get("lane_episode_card_count")) > 0
        for brief in section_briefs
    ):
        return True
    combined = "\n".join(
        [
            "\n".join(strings_from(source_material)),
            "\n".join(all_evidence_refs(draft)),
            "\n".join(
                "\n".join(strings_from(artifact))
                for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir)
            ),
        ]
    )
    return (
        contains_any_phrase(combined, ("lane_episode_cards", "lane episode card"))
        and contains_any_phrase(combined, INTERACTION_TIMELINE_BASIS_MARKERS)
    )


def interaction_node_ref_classes(node: dict[str, Any]) -> set[str]:
    classes: set[str] = set()
    fact_refs = (
        list_items(node.get("fact_or_policy_evidence_refs"))
        or list_items(node.get("fact_policy_refs"))
        or list_items(node.get("fact_or_policy_episode_refs"))
    )
    public_refs = (
        list_items(node.get("public_or_media_evidence_refs"))
        or list_items(node.get("public_media_refs"))
        or list_items(node.get("public_or_media_episode_refs"))
    )
    if fact_refs:
        classes.add("fact-or-policy")
    if public_refs:
        classes.add("public-or-media")
    return classes


def interaction_timeline_quality_issues(draft: dict[str, Any]) -> list[dict[str, str]]:
    source_material = source_material_dict(draft)
    nodes = source_material_list(draft, "interaction_timeline_nodes")
    lane_episode_cards = source_material_list(draft, "lane_episode_cards")
    if not nodes:
        return []
    issues: list[dict[str, str]] = []
    missing_summary_count = sum(1 for node in nodes if not maybe_text(node.get("node_summary")))
    if missing_summary_count:
        issues.append(
            issue(
                "interaction-node-summary-missing",
                (
                    "Interaction timeline claims require readable node_summary values "
                    f"on every carried node; {missing_summary_count} node(s) lack one."
                ),
                "error",
            )
        )
    if not lane_episode_cards:
        issues.append(
            issue(
                "interaction-claim-without-lane-episode-cards",
                "Interaction claims require lane episode cards before timeline synthesis.",
                "error",
            )
        )
    if not any({"fact-or-policy", "public-or-media"}.issubset(interaction_node_ref_classes(node)) for node in nodes):
        issues.append(
            issue(
                "interaction-node-two-ref-classes-missing",
                (
                    "Interaction timeline claims require at least one carried node with "
                    "fact/policy refs and public/media refs."
                ),
                "error",
            )
        )
    timeline_meta = (
        source_material.get("interaction_timeline")
        if isinstance(source_material.get("interaction_timeline"), dict)
        else {}
    )
    if int_value(timeline_meta.get("lane_episode_card_count")) <= 0 and not lane_episode_cards:
        issues.append(
            issue(
                "interaction-denominator-lane-episode-count-missing",
                "Interaction timeline basis should carry a lane episode card denominator.",
                "warning",
            )
        )
    return issues


def strong_report_claim_present(text: str) -> bool:
    return (
        policy_evaluation_claim_present(text)
        or responsibility_claim_present(text)
        or interaction_judgement_present(text)
        or contains_unnegated_phrase(text, STRONG_ENVIRONMENT_ATTRIBUTION_PHRASES)
    )


def policy_evaluation_basis_acquisition_lane_misuse(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("not_acquisition_theme") is True or value.get("policy_evaluation_basis_is_not_acquisition_lane") is True:
            return False
        selected_text = " ".join(
            "\n".join(strings_from(value.get(field_name)))
            for field_name in POLICY_EVALUATION_ACQUISITION_FIELDS
        )
        mentions_policy_basis = contains_any_phrase(
            selected_text,
            ("policy_evaluation_basis", "policy evaluation basis"),
        )
        acquisition_shaped = any(
            key in value
            for key in (
                "source_family",
                "source_families",
                "source_family_candidates",
                "source_skill",
                "query_variant_plan",
                "evidence_obligations",
                "success_criteria",
                "denominator_obligations",
                "acquisition_lane",
                "theme_question",
            )
        ) or maybe_text(value.get("object_kind")) in {"investigation-theme", "theme-evidence-boundary-plan"}
        if mentions_policy_basis and acquisition_shaped:
            return True
        return any(policy_evaluation_basis_acquisition_lane_misuse(child) for child in value.values())
    if isinstance(value, list):
        return any(policy_evaluation_basis_acquisition_lane_misuse(child) for child in value)
    return False


def interaction_boundary_visible(text: str) -> bool:
    return contains_any_phrase(text, INTERACTION_BOUNDARY_TERMS)


def policy_evaluation_claim_present(text: str) -> bool:
    return contains_unnegated_phrase(text, POLICY_EVALUATION_CLAIM_PHRASES)


def policy_lane_declares_absence(draft: dict[str, Any]) -> bool:
    policy_lane = source_material_dict(draft).get("policy_lane")
    if not isinstance(policy_lane, dict):
        return False
    value = policy_lane.get("official_action_or_governance_record_basis_visible")
    return value is False or maybe_text(value).casefold() in {"false", "0", "no"}


def official_action_or_governance_record_basis_visible(draft: dict[str, Any], *, run_dir: Path | None) -> bool:
    source_material = source_material_dict(draft)
    policy_lane = source_material.get("policy_lane")
    if isinstance(policy_lane, dict):
        visible = policy_lane.get("official_action_or_governance_record_basis_visible")
        if visible is True or maybe_text(visible).casefold() in {"true", "1", "yes"}:
            return True
        if visible is False or maybe_text(visible).casefold() in {"false", "0", "no"}:
            return False
    for brief in section_brief_rows(draft):
        refs = [*list_items(brief.get("evidence_refs")), *list_items(brief.get("refs"))]
        if not refs:
            continue
        text = "\n".join(
            [
                maybe_text(brief.get("section_key")),
                maybe_text(brief.get("section_role")),
                maybe_text(brief.get("claim_strength")),
                "\n".join(strings_from(brief.get("source_families"))),
                "\n".join(strings_from(brief.get("main_claims"))),
            ]
        )
        if contains_any_phrase(text, OFFICIAL_ACTION_GOVERNANCE_BASIS_MARKERS) and not contains_any_phrase(
            text,
            INSUFFICIENT_BASIS_MARKERS,
        ):
            return True
    combined = "\n".join(
        [
            "\n".join(strings_from(source_material)),
            "\n".join(all_evidence_refs(draft)),
            "\n".join(
                "\n".join(strings_from(artifact))
                for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir)
            ),
        ]
    )
    return contains_any_phrase(combined, OFFICIAL_ACTION_GOVERNANCE_BASIS_MARKERS) and not (
        policy_lane_declares_absence(draft)
    )


def explicit_policy_evaluation_basis_visible(
    draft: dict[str, Any],
    *,
    run_dir: Path | None,
) -> bool:
    def has_policy_basis_marker(row: dict[str, Any]) -> bool:
        text = "\n".join(strings_from(row))
        return contains_any_phrase(text, POLICY_EVALUATION_BASIS_MARKERS) and not contains_any_phrase(
            text,
            INSUFFICIENT_BASIS_MARKERS,
        )

    for brief in situation_analysis_brief_rows(draft):
        basis_rows = [
            item
            for item in list_items(brief.get("policy_evaluation_basis"))
            if isinstance(item, dict)
        ]
        if any(has_policy_basis_marker(row) for row in basis_rows):
            return True

    for brief in section_brief_rows(draft):
        refs = [
            *list_items(brief.get("evidence_refs")),
            *list_items(brief.get("basis_object_ids")),
            *list_items(brief.get("refs")),
        ]
        if refs and has_policy_basis_marker(brief):
            return True

    for review in carried_theme_review_rows(draft):
        if has_policy_basis_marker(review):
            return True

    for artifact in helper_artifacts_from_draft(draft, run_dir=run_dir):
        if has_policy_basis_marker(artifact):
            return True
    return False


def policy_evaluation_basis_visible(draft: dict[str, Any], *, run_dir: Path | None) -> bool:
    if not official_action_or_governance_record_basis_visible(draft, run_dir=run_dir):
        return False
    counts = council_object_counts(draft)
    if any(
        counts.get(kind, 0) > 0
        for kind in (
            "finding",
            "evidence-bundle",
            "round-synthesis",
            "readiness-opinion",
            "proposal",
        )
    ) or explicit_policy_evaluation_basis_visible(draft, run_dir=run_dir):
        return True
    return False


def responsibility_claim_present(text: str) -> bool:
    return contains_unnegated_phrase(text, RESPONSIBILITY_CLAIM_PHRASES)


def optional_helper_carrier_issues(draft: dict[str, Any]) -> list[dict[str, str]]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    reporting_artifacts = (
        source_material.get("reporting_artifacts")
        if isinstance(source_material.get("reporting_artifacts"), list)
        else []
    )
    counts = council_object_counts(draft)
    carrier_visible = any(
        isinstance(row, dict) and maybe_text(row.get("kind")) == "report-basis-freeze"
        for row in reporting_artifacts
    ) or any(
        counts.get(kind, 0) > 0
        for kind in (
            "finding",
            "evidence-bundle",
            "agent-position",
            "readiness-opinion",
            "round-synthesis",
            "proposal",
        )
    )
    if carrier_visible:
        return []
    helper_text = "\n".join(
        [
            report_prose_text(draft),
            "\n".join(all_evidence_refs(draft)),
            "\n".join(strings_from(source_material)),
        ]
    )
    markers = helper_marker_mentions(helper_text)
    if not markers:
        return []
    return [
        issue(
            "optional-analysis-helper-not-carried",
            (
                "The draft appears to cite optional-analysis helper output "
                f"({', '.join(unique_texts(markers)[:4])}). Helper artifacts must be carried "
                "by a finding, evidence bundle, agent position, readiness, synthesis, or report-basis object before report use."
            ),
            "warning",
        )
    ]


def validate_claim_boundary_semantics(
    draft: dict[str, Any],
    *,
    run_dir: Path | None = None,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    text = report_prose_text(draft)
    if not text:
        return issues

    if strong_report_claim_present(text) and not claim_chain_basis_visible(draft):
        issues.append(
            issue(
                "strong-claim-without-brief-review-or-frozen-basis",
                (
                    "Strong policy, responsibility, interaction, public semantic, or "
                    "attribution claims require an agent section brief, theme "
                    "sufficiency/progress review carried by council uptake, or "
                    "frozen/reporting basis."
                ),
                "error",
            )
        )

    if (
        strong_report_claim_present(text)
        or policy_evaluation_claim_present(text)
        or public_opinion_percentage_present(text)
        or contains_unnegated_phrase(text, ENVIRONMENT_ATTRIBUTION_PHRASES)
        or interaction_judgement_present(text)
    ) and not challenger_boundary_review_visible(draft):
        issues.append(
            issue(
                "challenger-boundary-review-missing",
                (
                    "Strong, policy-evaluation, public-proportion, causal, "
                    "interaction, or attribution wording should show challenger "
                    "boundary review through a challenger section brief, challenge, "
                    "or review comment before report use."
                ),
                "warning",
            )
        )

    if policy_evaluation_basis_acquisition_lane_misuse(source_material_dict(draft)):
        issues.append(
            issue(
                "policy-evaluation-basis-as-acquisition-lane",
                (
                    "policy_evaluation_basis must remain a report synthesis layer. "
                    "It must not appear as an investigation theme, source family, "
                    "query lane, or theme evidence boundary plan target."
                ),
                "error",
            )
        )

    has_representative_design = mission_has_representative_sampling_design(run_dir)
    if (
        contains_unnegated_phrase(text, PUBLIC_OPINION_UPGRADE_PHRASES)
        or public_opinion_percentage_present(text)
    ) and not has_representative_design:
        issues.append(
            issue(
                "unsupported-public-opinion-claim",
                (
                    "Report text appears to make a representative or platform-wide "
                    "public-opinion claim. Keep public discourse language sample-local "
                    "unless the mission records a representative sampling design."
                ),
                "error",
            )
        )

    if formal_comment_public_opinion_upgrade_present(text) and not has_representative_design:
        issues.append(
            issue(
                "formal-comment-distribution-as-public-opinion",
                (
                    "Formal comment or docket samples are institutional participation records. "
                    "They must not be written as general public-opinion distributions without "
                    "a representative sampling design."
                ),
                "error",
            )
        )

    formal_basis = formal_comment_basis_summary(draft, run_dir=run_dir)
    if formal_comment_structure_claim_present(text):
        if not formal_basis["has_candidate_audit"]:
            issues.append(
                issue(
                    "formal-comment-structure-without-candidate-audit",
                    (
                        "Formal comment issue, stance, or concern structure requires "
                        "a candidate-corpus audit or equivalent sample-shape basis before report use."
                    ),
                    "error",
                )
            )
        if not formal_basis["has_readable_formal_text"]:
            issues.append(
                issue(
                    "formal-comment-structure-without-readable-corpus",
                    (
                        "Formal comment issue, stance, or concern structure requires "
                        "a readable formal comment corpus, not only source discovery or receipt metadata."
                    ),
                    "error",
                )
            )
        if not formal_basis["has_annotation_basis"]:
            issues.append(
                issue(
                    "formal-comment-structure-without-annotation-basis",
                    (
                        "Formal comment issue, stance, or concern structure requires "
                        "a formal comment annotation or aggregation basis before report use."
                    ),
                    "error",
                )
            )

    if formal_comment_stance_distribution_present(text) and (
        int_value(formal_basis.get("readable_count")) <= 1 or not formal_basis["has_annotation_basis"]
    ):
        issues.append(
            issue(
                "formal-comment-stance-distribution-insufficient-basis",
                (
                    "Formal comment stance distributions require more than a single readable "
                    "comment/detail seed and must cite annotation or aggregation basis."
                ),
                "error",
            )
        )

    if (
        formal_attachment_discussion_present(text)
        and formal_basis["attachment_limited"]
        and not formal_attachment_limitation_visible(text)
    ):
        issues.append(
            issue(
                "formal-attachment-text-limitation-missing",
                (
                    "The draft discusses formal comment attachments while source material "
                    "records attachment text limits. State the attachment readability or extraction limitation."
                ),
                "warning",
            )
        )

    public_summary_payload = public_discourse_summary_payload(draft, run_dir=run_dir)
    if (
        sample_distribution_language_present(text)
        or public_discourse_quantification_present(text)
    ) and not has_public_discourse_basis(draft, text):
        issues.append(
            issue(
                "sample-distribution-without-public-discourse-basis",
                (
                    "Sample-local percentages or label distributions require a public "
                    "discourse summary, annotation aggregation, or equivalent DB-backed helper basis."
                ),
                "error",
            )
        )
    elif public_discourse_quantification_present(text):
        public_basis = public_discourse_basis_summary(draft, run_dir=run_dir)
        required_basis_messages = {
            "has_corpus": (
                "public-discourse-corpus-basis-missing",
                "Public discourse emotion, issue, concern, or proportion claims require a materialized corpus or equivalent sample-definition basis.",
            ),
            "has_coverage_audit": (
                "public-discourse-coverage-audit-basis-missing",
                "Public discourse emotion, issue, concern, or proportion claims require a coverage-audit basis before report use.",
            ),
            "has_annotation": (
                "public-discourse-annotation-basis-missing",
                "Public discourse emotion, issue, concern, or proportion claims require annotation basis, not only raw fetch or visibility records.",
            ),
            "has_aggregation": (
                "public-discourse-aggregation-basis-missing",
                "Public discourse emotion, issue, concern, or proportion claims require annotation aggregation or equivalent summarized basis.",
            ),
            "has_denominator": (
                "public-discourse-denominator-basis-missing",
                "Public discourse percentages, main-concern, or issue-distribution claims require an explicit denominator in the basis.",
            ),
        }
        for key, (code, message) in required_basis_messages.items():
            if not public_basis[key]:
                issues.append(issue(code, message, "error"))
        if not public_semantic_source_family_denominator_visible(draft, run_dir=run_dir):
            issues.append(
                issue(
                    "public-semantic-source-family-denominator-missing",
                    (
                        "Public semantic percentages or label distributions require "
                        "a visible source family and denominator in the carried basis."
                    ),
                    "error",
                )
            )
        if not public_discourse_sample_boundary_visible(text):
            issues.append(
                issue(
                    "public-discourse-quantification-sample-boundary-missing",
                    (
                        "Public discourse counts, percentages, shares, or label distributions "
                        "should state that they are sample-local and not population or platform-wide estimates."
                    ),
                    "warning",
                )
            )
        if not public_discourse_nonexclusive_boundary_visible(text):
            issues.append(
                issue(
                    "public-discourse-label-nonexclusive-boundary-missing",
                    (
                        "Public discourse label percentages should state whether labels are non-exclusive "
                        "and should not be summed into a 100% opinion composition."
                    ),
                    "warning",
                )
            )
        if sample_fraction_totalized_present(text):
            issues.append(
                issue(
                    "sample-fractions-totalized-as-opinion-composition",
                    (
                    "Sample fractions from public-discourse labels must not be "
                    "summed or presented as a complete 100% opinion composition "
                    "when labels can be non-exclusive."
                ),
                "error",
            )
        )
        sample_count = int_value(public_summary_payload.get("sample_count"))
        if 0 < sample_count < 10 and not small_sample_boundary_visible(text):
            issues.append(
                issue(
                    "small-public-discourse-sample-boundary-missing",
                    (
                        "Public discourse sample counts below 10 should be paired with "
                        "visible small-sample or sample-boundary wording."
                    ),
                    "warning",
                )
            )
        if source_family_count(public_summary_payload) > 1 and not sample_denominator_visible(text):
            issues.append(
                issue(
                    "mixed-source-family-denominator-missing",
                    (
                        "Public discourse statistics spanning multiple source families "
                        "should state source-family or discourse-lane denominators."
                    ),
                    "warning",
                )
            )
        if not sample_denominator_visible(text):
            issues.append(
                issue(
                    "public-discourse-denominator-missing",
                    "Public discourse percentages or label distributions should state an explicit sample denominator.",
                    "warning",
                )
            )
        if not representativeness_limit_visible(text):
            issues.append(
                issue(
                    "public-discourse-representativeness-limit-missing",
                    (
                        "Public discourse percentages or label distributions should state "
                        "representativeness limits such as not representative or not public opinion."
                    ),
                    "warning",
                )
            )

    if contains_unnegated_phrase(text, GDELT_TONE_PUBLIC_SENTIMENT_PHRASES):
        issues.append(
            issue(
                "gdelt-tone-public-sentiment",
                "GDELT tone may describe media/document tone, not public sentiment or public emotion.",
                "error",
            )
        )
    elif gdelt_tone_as_public_sentiment_present(text):
        issues.append(
            issue(
                "gdelt-tone-public-sentiment",
                "GDELT tone may describe media/document tone, not public sentiment or public emotion.",
                "error",
            )
        )

    if platform_or_docket_sample_generalization_present(text) and not has_representative_design:
        issues.append(
            issue(
                "platform-or-docket-sample-generalized",
                (
                    "YouTube, Bluesky, and Regulations.gov samples must not be "
                    "written as overall public or resident opinion unless the "
                    "mission records a representative sampling design."
                ),
                "error",
            )
        )

    if interaction_judgement_present(text):
        if not interaction_timeline_basis_visible(draft, run_dir=run_dir):
            issues.append(
                issue(
                    "interaction-claim-without-timeline-basis",
                    (
                        "Fact-policy-public interaction, public-response, semantic-shift, "
                        "or same-day interaction wording requires an interaction timeline "
                        "section brief or equivalent carried basis."
                    ),
                    "error",
                )
            )
        elif not interaction_boundary_visible(text):
            issues.append(
                issue(
                    "interaction-claim-boundary-missing",
                    (
                        "Interaction timeline wording should state its descriptive boundary: "
                        "co-visible chronology is not causality, policy effect, or public-response attribution."
                    ),
                    "warning",
                )
            )
        issues.extend(interaction_timeline_quality_issues(draft))

    if policy_evaluation_claim_present(text) and not policy_evaluation_basis_visible(draft, run_dir=run_dir):
        issues.append(
            issue(
                "policy-evaluation-claim-without-basis",
                (
                    "Policy success, failure, effectiveness, harm, or improvement claims "
                    "require official action or governance record basis plus a "
                    "council-carried/reporting basis. policy_evaluation_basis is a "
                    "synthesis layer, not an acquisition lane."
                ),
                "error",
            )
        )

    if (policy_evaluation_claim_present(text) or responsibility_claim_present(text)) and policy_lane_declares_absence(draft):
        issues.append(
            issue(
                "policy-lane-absence-claim-downgrade-required",
                (
                    "The draft source_material records no official action or governance "
                    "record basis. Policy effectiveness, policy response, or "
                    "responsibility conclusions must be downgraded to a basis gap."
                ),
                "error",
            )
        )

    if responsibility_claim_present(text) and not (
        policy_evaluation_basis_visible(draft, run_dir=run_dir)
        or relation_review_basis_visible(draft, text)
    ):
        issues.append(
            issue(
                "responsibility-claim-without-basis",
                (
                    "Responsibility or accountability claims require a policy-evaluation "
                    "basis and/or relation-review/challenger basis before report use."
                ),
                "error",
            )
        )

    if (
        environment_state_claim_present(text)
        and not environment_aggregation_basis_visible(draft, run_dir=run_dir)
        and not item_level_environment_boundary_visible(text)
    ):
        issues.append(
            issue(
                "environment-state-claim-without-aggregation",
                (
                    "Environment trend, peak, range, or operating-status claims "
                    "should cite aggregate-environment-evidence or explicitly state "
                    "that the text is only an item-level example."
                ),
                "error",
            )
        )

    if unsupported_environment_claim_present(text) and not reportable_environment_basis_visible(draft, run_dir=run_dir):
        issues.append(
            issue(
                "environment-or-policy-claim-without-visible-basis",
                (
                    "Environmental observation, environmental trend, exposure, health-outcome, "
                    "policy-responsibility, or source-attribution language requires a visible "
                    "environmental/attribution basis. If the council basis only records a boundary, "
                    "write the point as an exclusion rather than a substantive claim."
                ),
                "error",
            )
        )

    if source_narrative_discussion_present(text) and not source_narrative_boundary_visible(text):
        issues.append(
            issue(
                "source-narrative-boundary-missing",
                (
                    "Public source-narrative wording should state that it is a "
                    "source-narrative cue and cannot substitute for physical "
                    "source attribution or other environment/operation/legal/policy "
                    "causal judgment."
                ),
                "warning",
            )
        )

    if contains_unnegated_phrase(text, SOURCE_NARRATIVE_ATTRIBUTION_PHRASES):
        issues.append(
            issue(
                "source-narrative-as-physical-attribution",
                (
                    "Public source narratives must remain source-narrative cues and "
                    "must not substitute for physical source attribution."
                ),
                "error",
            )
        )

    if contains_unnegated_phrase(text, ENVIRONMENT_ATTRIBUTION_PHRASES):
        counts = council_object_counts(draft)
        has_environment_basis = any(
            counts.get(kind, 0) > 0
            for kind in ("finding", "evidence-bundle", "hypothesis", "hypothesis-status")
        )
        has_challenger_review = challenger_boundary_review_visible(draft)
        if not has_environment_basis:
            issues.append(
                issue(
                    "attribution-claim-needs-environment-basis",
                    (
                        "Environmental source, transport, causal, or origin claims "
                        "should cite environment findings, evidence bundles, hypotheses, "
                        "or equivalent DB-backed basis."
                    ),
                    "warning",
                )
            )
        if not has_challenger_review:
            issues.append(
                issue(
                    "attribution-claim-needs-challenger-visibility",
                    (
                        "Strong attribution language should keep challenger review, "
                        "alternative explanations, or explicit limitation handling visible."
                    ),
                    "warning",
                )
            )
        if not relation_review_basis_visible(draft, text):
            issues.append(
                issue(
                    "causal-or-attribution-claim-without-relation-review-basis",
                    (
                        "Source, causal, transport, or impact-chain wording should "
                        "show relation, fact-check, alternatives, or challenger-review "
                        "basis. Otherwise downgrade the wording to compatible cues or "
                        "still-needs-verification language."
                    ),
                    "warning",
                )
            )
        if (
            contains_unnegated_phrase(text, STRONG_ENVIRONMENT_ATTRIBUTION_PHRASES)
            and not contains_any_phrase(text, ATTRIBUTION_MODEL_MARKERS)
        ):
            issues.append(
                issue(
                    "strong-attribution-without-attribution-model",
                    (
                        "Strong source, transport, or causal attribution wording "
                        "requires trajectory, plume, chemistry, or comparable "
                        "professional attribution-model basis. Without that basis, "
                        "the report should use compatibility or descriptive language."
                    ),
                    "error",
                )
            )
        if not contains_any_phrase(text, ATTRIBUTION_MODEL_MARKERS):
            issues.append(
                issue(
                    "attribution-model-limitation-not-visible",
                    (
                        "If the report discusses source, transport, or causal attribution, "
                        "it should state whether trajectory, plume, chemistry, or comparable "
                        "attribution evidence is present or absent."
                    ),
                    "warning",
                )
            )
        elif not contains_any_phrase(text, BOUNDED_ATTRIBUTION_TERMS):
            issues.append(
                issue(
                    "attribution-boundary-language-missing",
                    (
                        "Attribution discussion should keep compatibility, "
                        "descriptive relation, or explicit non-proof language visible "
                        "unless the cited basis supports stronger attribution."
                    ),
                    "warning",
                )
            )

    public_section = section_by_id(draft, "public-discourse-deepening")
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    public_summary = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    public_summary_id = maybe_text(public_summary.get("summary_id"))
    if public_section and public_summary and not has_optional_analysis_carrier(draft, public_summary_id):
        issues.append(
            issue(
                "optional-analysis-not-carried",
                (
                    "The public discourse addendum uses an advisory helper output. "
                    "Confirm it is carried by a finding, bundle, position, readiness, "
                    "synthesis, or report-basis object before treating it as report basis."
                ),
                "warning",
            )
        )

    issues.extend(optional_helper_carrier_issues(draft))

    if contains_any_phrase(text, ACQUISITION_ATTEMPT_TERMS) and not contains_any_phrase(text, ACTIONABLE_PATH_TERMS):
        issues.append(
            issue(
                "acquisition-attempt-without-actionable-path-rationale",
                (
                    "Failed, blocked, receipt-only, zero-signal, or no-normalized-ref "
                    "acquisition attempts should be paired with an explicit actionable-path "
                    "or non-continuation rationale before report closure."
                ),
                "warning",
            )
        )

    return issues


def validate_draft(draft: dict[str, Any], *, run_dir: Path | None = None) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if maybe_text(draft.get("schema_version")) != "narrative-report-draft-v1":
        issues.append(issue("unexpected-schema", "Draft schema_version is not narrative-report-draft-v1.", "error"))
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    if not maybe_text(boundary.get("summary")):
        issues.append(issue("missing-claim-boundary", "Draft must include a visible claim boundary.", "error"))
    if not isinstance(boundary.get("forbidden_claims"), list) or not boundary["forbidden_claims"]:
        issues.append(issue("missing-forbidden-claims", "Draft must state forbidden claim upgrades.", "warning"))
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    section_ids = {
        maybe_text(section.get("section_id"))
        for section in sections
        if isinstance(section, dict)
    }
    missing = sorted(REQUIRED_SECTIONS - section_ids)
    for section_id in missing:
        issues.append(issue("missing-section", f"Missing required section: {section_id}.", "error"))
    allowed_ref_optional_statuses = {
        "limitations-only",
        "limitations-visible",
        "boundary-only",
        "traceability-index",
    }
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_id = maybe_text(section.get("section_id")) or "unknown-section"
        paragraphs = section.get("paragraphs") if isinstance(section.get("paragraphs"), list) else []
        if not any(maybe_text(paragraph) for paragraph in paragraphs):
            issues.append(issue("empty-section", f"Section {section_id} has no paragraph text.", "error"))
        for paragraph in paragraphs:
            text = maybe_text(paragraph)
            lowered = text.lower()
            if any(lowered.startswith(prefix) for prefix in MACHINE_PROSE_PREFIXES):
                issues.append(
                    issue(
                        "machine-object-prose",
                        f"Section {section_id} appears to lead with object labels instead of reader-facing prose.",
                        "warning",
                    )
                )
                break
        refs = section.get("evidence_refs") if isinstance(section.get("evidence_refs"), list) else []
        status = maybe_text(section.get("status"))
        if not refs and status not in allowed_ref_optional_statuses:
            issues.append(issue("section-without-refs", f"Section {section_id} has no evidence refs or limitation status.", "warning"))
    all_paragraphs = [
        maybe_text(paragraph)
        for section in sections
        if isinstance(section, dict)
        for paragraph in (section.get("paragraphs") if isinstance(section.get("paragraphs"), list) else [])
        if maybe_text(paragraph)
    ]
    duplicate_count = len(all_paragraphs) - len(set(all_paragraphs))
    if duplicate_count:
        issues.append(
            issue(
                "duplicate-prose",
                f"Draft repeats {duplicate_count} paragraph(s); narrative reports should explain object roles instead of restating the same artifact text.",
                "warning",
            )
        )
    if any(text.startswith("- ") for text in all_paragraphs):
        issues.append(
            issue(
                "embedded-markdown-bullets",
                "Draft stores Markdown bullet prefixes inside paragraph text; use presentation metadata or plain paragraph strings instead.",
                "warning",
            )
        )
    title = maybe_text(draft.get("title")).lower()
    if title.startswith("narrative report draft for") or title.startswith("narrative report for"):
        issues.append(
            issue(
                "weak-report-title",
                "Report title should identify the subject or basis in reader-facing terms, not only the round id.",
                "warning",
            )
        )
    if not isinstance(draft.get("reader_guidance"), dict):
        issues.append(issue("missing-reader-guidance", "Draft should include reader_guidance describing intended audience and style.", "warning"))
    if not isinstance(draft.get("evidence_refs"), list) or not draft["evidence_refs"]:
        issues.append(issue("missing-evidence-index", "Draft has no top-level evidence_refs index.", "warning"))
    if not isinstance(draft.get("audit_refs"), list) or not draft["audit_refs"]:
        issues.append(issue("missing-audit-refs", "Draft has no audit_refs index.", "warning"))
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    situation_briefs = situation_analysis_brief_rows(draft)
    if not situation_briefs and not bool(source_material.get("not_situation_analysis_report")):
        issues.append(
            issue(
                "missing-situation-analysis-brief",
                "Situation-analysis reports should carry a situation-analysis brief before narrative report writing.",
                "warning",
            )
        )
        issues.append(
            issue(
                "weak-report-mainline",
                "Validator cannot see a situation-analysis report spine; draft may be assembling sections from scattered objects rather than a mission-answering analysis line.",
                "warning",
            )
        )
    elif situation_briefs:
        issues.extend(situation_analysis_brief_quality_issues(draft))
        issues.extend(situation_analysis_brief_consumption_issues(draft))
    instruction_residue_phrases = (
        "本节应",
        "报告应",
        "应当说明",
        "should describe",
        "should explain",
        "should state",
        "TODO",
        "see JSON",
    )
    if any(
        phrase in maybe_text(paragraph)
        for paragraph in all_paragraphs
        for phrase in instruction_residue_phrases
    ):
        issues.append(
            issue(
                "instruction-residue",
                "Draft appears to retain writing instructions or unfinished audit handoff text.",
                "warning",
            )
        )
    runtime_meta_terms = ("runtime", "ledger", "object_id", "round_id", "schema_version", "handoff_id")
    runtime_meta_hits = sum(
        maybe_text(paragraph).casefold().count(term)
        for paragraph in all_paragraphs
        for term in runtime_meta_terms
    )
    if runtime_meta_hits > 8:
        issues.append(
            issue(
                "runtime-meta-language-too-dense",
                "Draft body contains dense runtime or object metadata language; reader-facing prose should foreground the situation analysis.",
                "warning",
            )
        )
    audit_trail = section_by_id(draft, "audit-trail")
    audit_index_text = "\n".join(
        [
            "\n".join(maybe_text(ref) for ref in list_items(draft.get("audit_refs"))),
            "\n".join(
                maybe_text(paragraph)
                for paragraph in list_items(audit_trail.get("paragraphs"))
            ),
            "\n".join(strings_from(source_material.get("audit_index"))),
        ]
    )
    if contains_any_phrase(audit_index_text, UNFINISHED_AUDIT_INDEX_TERMS):
        issues.append(
            issue(
                "unfinished-audit-index",
                "Draft appears to carry an unfinished audit index marker; resolve or scope it before final publication.",
                "warning",
            )
        )
    issues.extend(validate_claim_boundary_semantics(draft, run_dir=run_dir))
    if run_dir is not None:
        issues.extend(public_discourse_summary_contract_issues(draft, run_dir=run_dir))
    return issues


def markdown_reader_quality_issues(markdown_text: str) -> list[dict[str, str]]:
    if not maybe_text(markdown_text):
        return []
    issues: list[dict[str, str]] = []
    audit_heading_match = re.search(r"^## (?:参考文献与审计索引|审计索引|Audit Trail)\b", markdown_text, re.MULTILINE)
    body_text = markdown_text[: audit_heading_match.start()] if audit_heading_match else markdown_text
    if "审计引用（节选）" in body_text:
        issues.append(
            issue(
                "reader-body-inline-audit-snippets",
                "Reader-facing Markdown inserts audit-ref snippets inside substantive sections; keep audit refs in the final audit/index section.",
                "error",
            )
        )
    raw_id_count = len(RAW_AUDIT_ID_RE.findall(body_text))
    if raw_id_count > 4:
        issues.append(
            issue(
                "reader-body-audit-id-density",
                f"Reader-facing Markdown body contains {raw_id_count} raw audit/object ids before the audit section.",
                "error",
            )
        )
    lowered_body = body_text.lower()
    jargon_hits = [term for term in CHINESE_RUNTIME_JARGON if term in lowered_body]
    if jargon_hits and re.search(r"[\u4e00-\u9fff]", body_text):
        issues.append(
            issue(
                "reader-body-untranslated-runtime-jargon",
                "Chinese reader-facing Markdown still contains untranslated runtime/audit jargon: "
                + ", ".join(jargon_hits[:6]),
                "warning",
            )
        )
    json_tail_mentions = len(re.findall(r"另有\s*\d+\s*条.*JSON", body_text))
    if json_tail_mentions:
        issues.append(
            issue(
                "reader-body-json-tail-refs",
                "Reader-facing Markdown body points readers to additional JSON refs inside substantive prose; reserve that for the audit/index section.",
                "warning",
            )
        )
    cannot_count = body_text.count("不能") + body_text.count("不得")
    body_length_units = max(1, len(body_text) // 1000)
    if cannot_count / body_length_units > 6:
        issues.append(
            issue(
                "over-defensive-reader-body",
                "Reader-facing Markdown uses unusually dense negative boundary language; consolidate limits into discussion/limitations instead of repeating them throughout the body.",
                "warning",
            )
        )
    return issues


def validate_narrative_report(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    draft_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    draft_file = resolve_path(run_dir_path, draft_path, f"reporting/narrative_report_draft_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/narrative_report_validation_{round_id}.json")
    draft = load_json_file(draft_file)
    issues = validate_draft(draft, run_dir=run_dir_path)
    markdown_file = draft_file.with_suffix(".md")
    if markdown_file.exists():
        issues.extend(markdown_reader_quality_issues(markdown_file.read_text(encoding="utf-8")))
    error_count = sum(1 for item in issues if item.get("severity") == "error")
    warning_count = sum(1 for item in issues if item.get("severity") != "error")
    validation_id = "narrative-report-validation-" + stable_hash(run_id, round_id, draft.get("draft_id"), issues)[:12]
    validation = {
        "schema_version": "narrative-report-validation-v1",
        "validation_id": validation_id,
        "run_id": run_id,
        "round_id": round_id,
        "draft_id": maybe_text(draft.get("draft_id")),
        "basis_round_id": maybe_text(draft.get("basis_round_id")),
        "generated_at_utc": utc_now_iso(),
        "status": "valid" if error_count == 0 else "invalid",
        "validation_scope": "structure-traceability-and-claim-boundary",
        "does_not_decide": [
            "truth",
            "evidence sufficiency",
            "source ranking",
            "claim confidence",
        ],
        "issue_count": len(issues),
        "error_count": error_count,
        "warning_count": warning_count,
        "issues": issues,
        "draft_path": str(draft_file),
        "publish_allowed": error_count == 0,
    }
    write_json_file(output_file, validation)
    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
    ]
    return {
        "status": "completed" if error_count == 0 else "blocked",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "validation_id": validation_id,
            "validation_status": validation["status"],
            "error_count": error_count,
            "warning_count": warning_count,
            "output_path": str(output_file),
        },
        "receipt_id": "report-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, validation_id)[:20],
        "batch_id": "reportbatch-" + stable_hash(SKILL_NAME, run_id, round_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [validation_id],
        "warnings": [item for item in issues if item.get("severity") != "error"],
        "board_handoff": {
            "candidate_ids": [validation_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in issues],
            "challenge_hints": [],
            "suggested_next_skills": ["publish-narrative-report"] if error_count == 0 else ["draft-narrative-report"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a narrative report draft.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--draft-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = validate_narrative_report(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        draft_path=args.draft_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

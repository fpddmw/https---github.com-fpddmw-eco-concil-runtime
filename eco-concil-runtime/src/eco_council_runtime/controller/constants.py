"""Stable constants for the eco-council controller."""

from __future__ import annotations

import re

DEFAULT_SCHEMA_VERSION = "1.0.0"

ROUND_ID_PATTERN = re.compile(r"^round-\d{3}$")
ROUND_ID_INPUT_PATTERN = re.compile(r"^round[-_](\d{3})$")
ROUND_DIR_PATTERN = re.compile(r"^round_(\d{3})$")
AGENT_ID_SAFE = re.compile(r"[^a-z0-9-]+")

ROLES = ("moderator", "sociologist", "environmentalist")
SOURCE_SELECTION_ROLES = ("sociologist", "environmentalist")
CURATION_ROLES = ("sociologist", "environmentalist")
READINESS_ROLES = ("sociologist", "environmentalist")
REPORT_ROLES = ("sociologist", "environmentalist")

OPENCLAW_AGENT_GUIDE_FILENAME = "OPENCLAW_AGENT_GUIDE.md"

STAGE_AWAITING_TASK_REVIEW = "awaiting-moderator-task-review"
STAGE_AWAITING_SOURCE_SELECTION = "awaiting-source-selection"
STAGE_READY_PREPARE = "ready-to-prepare-round"
STAGE_READY_FETCH = "ready-to-execute-fetch-plan"
STAGE_READY_DATA_PLANE = "ready-to-run-data-plane"
STAGE_AWAITING_EVIDENCE_CURATION = "awaiting-evidence-curation"
STAGE_AWAITING_DATA_READINESS = "awaiting-data-readiness"
STAGE_AWAITING_MATCHING_AUTHORIZATION = "awaiting-matching-authorization"
STAGE_AWAITING_MATCHING_ADJUDICATION = "awaiting-matching-adjudication"
STAGE_AWAITING_INVESTIGATION_REVIEW = "awaiting-investigation-review"
STAGE_READY_MATCHING_ADJUDICATION = "ready-to-run-matching-adjudication"
STAGE_AWAITING_REPORTS = "awaiting-expert-reports"
STAGE_AWAITING_DECISION = "awaiting-moderator-decision"
STAGE_READY_PROMOTE = "ready-to-promote"
STAGE_READY_ADVANCE = "ready-to-advance-round"
STAGE_COMPLETED = "completed"

DEFAULT_HISTORY_TOP_K = 3
MAX_HISTORY_TOP_K = 5
LAST_FAILURE_ERROR_LIMIT = 4000
MAX_OPENCLAW_INLINE_MESSAGE_CHARS = 100000

DATA_PLANE_STEP_IDS = (
    "normalize-init-run",
    "normalize-public",
    "normalize-environment",
    "build-round-context",
    "reporting-build-curation-packets",
    "render-openclaw-prompts",
    "validate-bundle",
    "build-reporting-handoff",
)

MATCHING_ADJUDICATION_STEP_IDS = (
    "apply-matching-adjudication",
    "build-round-context",
    "reporting-build-investigation-review-packet",
    "reporting-promote-investigation-review-draft",
    "reporting-build-report-packets",
    "render-openclaw-prompts",
    "validate-bundle",
    "build-reporting-handoff",
)

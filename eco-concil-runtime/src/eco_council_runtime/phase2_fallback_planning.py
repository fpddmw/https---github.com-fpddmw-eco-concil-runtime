from __future__ import annotations

# Compatibility-only facade. Canonical phase-2 fallback logic now lives in the
# narrower modules below so callers can depend on explicit policy surfaces
# instead of one oversized planning module.
from .phase2_fallback_agenda import (  # noqa: F401
    action_from_claim_assessment,
    action_from_coverage,
    action_from_diffusion_edge,
    action_from_formal_public_link,
    action_from_hypothesis,
    action_from_issue_cluster,
    action_from_open_challenge,
    action_from_open_task,
    action_from_representation_gap,
    action_from_verification_route,
    agenda_action,
    board_counts_from_round_state,
    board_snapshot,
    build_fallback_agenda_context,
    build_actions,
    controversy_context_counts,
    default_fallback_agenda_profile,
    fallback_agenda_source,
    prepare_promotion_action,
    role_from_coverage,
    score_action,
)
from .phase2_fallback_common import (  # noqa: F401
    excerpt_text,
    grouped_by_issue_label,
    indexed_by_claim_id,
    issue_label_for_item,
    list_field,
    load_json_if_exists,
    load_text_if_exists,
    maybe_number,
    maybe_text,
    normalize_space,
    optional_context_count,
    optional_context_present,
    optional_context_source,
    optional_context_warnings,
    priority_from_score,
    priority_score,
    resolve_path,
    role_from_lane,
    source_available,
    stable_hash,
    unique_texts,
    weakest_coverage_for_claim_ids,
)
from .phase2_fallback_context import (  # noqa: F401
    load_d1_shared_context,
    load_ranked_actions_context,
    primary_analysis_sync,
)
from .phase2_fallback_contracts import (  # noqa: F401
    d1_contract_fields,
    d1_contract_fields_from_payload,
    normalize_d1_observed_inputs,
)
from .phase2_fallback_policy import (  # noqa: F401
    DEFAULT_FALLBACK_POLICY_OWNER,
    DEFAULT_FALLBACK_POLICY_PROFILE,
    DEFAULT_FALLBACK_POLICY_SOURCE,
    fallback_policy_annotation,
)

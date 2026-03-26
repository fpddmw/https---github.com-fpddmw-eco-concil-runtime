"""Agent payload normalization, validation, and import-adjacent helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.controller.common import maybe_int, unique_strings
from eco_council_runtime.controller.io import (
    cloned_json,
    load_json_if_exists,
    maybe_text,
    run_json_command,
    write_json,
)
from eco_council_runtime.controller.paths import (
    override_requests_path,
    require_round_id,
    source_selection_packet_path,
)
from eco_council_runtime.controller.policy import contract_call
from eco_council_runtime.layout import CONTRACT_SCRIPT_PATH, PROJECT_DIR


def validate_input_file(kind: str, input_path: Path) -> None:
    payload = run_json_command(
        [
            "python3",
            str(CONTRACT_SCRIPT_PATH),
            "validate",
            "--kind",
            kind,
            "--input",
            str(input_path),
            "--pretty",
        ],
        cwd=PROJECT_DIR,
    )
    validation_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    validation = validation_payload.get("validation") if isinstance(validation_payload, dict) else None
    if not isinstance(validation, dict):
        raise RuntimeError(f"Schema validation returned an unexpected payload for {input_path}")
    if validation.get("ok"):
        return
    issues = validation.get("issues") if isinstance(validation.get("issues"), list) else []
    snippets: list[str] = []
    for issue in issues[:5]:
        if not isinstance(issue, dict):
            continue
        path = maybe_text(issue.get("path")) or "<root>"
        message = maybe_text(issue.get("message")) or "Validation failed."
        snippets.append(f"{path}: {message}")
    detail = "; ".join(snippets) if snippets else "Validation failed without issue details."
    raise ValueError(f"Invalid {kind}: {detail}")


def normalize_source_selection_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    normalized = cloned_json(payload)
    status = maybe_text(normalized.get("status")).casefold()
    status_aliases = {
        "completed": "complete",
        "complete": "complete",
        "done": "complete",
        "finished": "complete",
        "in_progress": "pending",
        "in-progress": "pending",
        "pending": "pending",
        "blocked": "blocked",
    }
    if status in status_aliases:
        normalized["status"] = status_aliases[status]

    decisions = normalized.get("source_decisions")
    if isinstance(decisions, list):
        fixed_decisions: list[Any] = []
        for item in decisions:
            if not isinstance(item, dict):
                fixed_decisions.append(item)
                continue
            decision = dict(item)
            if "source_skill" not in decision and "source" in decision:
                decision["source_skill"] = decision.pop("source")
            fixed_decisions.append(decision)
        normalized["source_decisions"] = fixed_decisions

    top_level_layers = normalized.get("layer_plans")
    grouped_top_level_layers: dict[str, list[dict[str, Any]]] = {}
    if isinstance(top_level_layers, list):
        for layer in top_level_layers:
            if not isinstance(layer, dict):
                continue
            family_id = maybe_text(layer.get("family_id"))
            if not family_id:
                continue
            grouped_top_level_layers.setdefault(family_id, []).append(dict(layer))

    family_plans = normalized.get("family_plans")
    if isinstance(family_plans, list):
        fixed_families: list[Any] = []
        for family in family_plans:
            if not isinstance(family, dict):
                fixed_families.append(family)
                continue
            family_plan = dict(family)
            if "reason" not in family_plan and "justification" in family_plan:
                family_plan["reason"] = family_plan.pop("justification")
            family_id = maybe_text(family_plan.get("family_id"))
            layer_plans = family_plan.get("layer_plans")
            if (not isinstance(layer_plans, list) or not layer_plans) and family_id in grouped_top_level_layers:
                layer_plans = grouped_top_level_layers.get(family_id, [])
                family_plan["layer_plans"] = layer_plans
            if isinstance(layer_plans, list):
                existing_keys = {
                    (family_id, maybe_text(layer.get("layer_id")))
                    for layer in layer_plans
                    if isinstance(layer, dict) and maybe_text(layer.get("layer_id"))
                }
                for lifted_layer in grouped_top_level_layers.get(family_id, []):
                    layer_key = (family_id, maybe_text(lifted_layer.get("layer_id")))
                    if not layer_key[1] or layer_key in existing_keys:
                        continue
                    layer_plans.append(dict(lifted_layer))
                    existing_keys.add(layer_key)
                fixed_layers: list[Any] = []
                for layer in layer_plans:
                    if not isinstance(layer, dict):
                        fixed_layers.append(layer)
                        continue
                    layer_plan = dict(layer)
                    if "source_skills" not in layer_plan and "selected_skills" in layer_plan:
                        layer_plan["source_skills"] = layer_plan.pop("selected_skills")
                    if "reason" not in layer_plan and "justification" in layer_plan:
                        layer_plan["reason"] = layer_plan.pop("justification")
                    layer_id = maybe_text(layer_plan.get("layer_id"))
                    if not maybe_text(layer_plan.get("reason")):
                        selected = layer_plan.get("selected") is True
                        if family_id and layer_id:
                            layer_plan["reason"] = (
                                f"Selected {family_id}.{layer_id} for the current round."
                                if selected
                                else f"Did not select {family_id}.{layer_id} for the current round."
                            )
                        elif layer_id:
                            layer_plan["reason"] = (
                                f"Selected {layer_id} for the current round."
                                if selected
                                else f"Did not select {layer_id} for the current round."
                            )
                    anchor_mode = maybe_text(layer_plan.get("anchor_mode")).casefold()
                    anchor_mode_aliases = {
                        "": "none",
                        "not-required": "none",
                        "not_required": "none",
                        "no-anchor": "none",
                        "no_anchor": "none",
                    }
                    if anchor_mode in anchor_mode_aliases:
                        layer_plan["anchor_mode"] = anchor_mode_aliases[anchor_mode]
                    fixed_layers.append(layer_plan)
                family_plan["layer_plans"] = fixed_layers
            if not maybe_text(family_plan.get("reason")):
                selected = family_plan.get("selected") is True
                if family_id:
                    family_plan["reason"] = (
                        f"Selected the {family_id} family for the current round."
                        if selected
                        else f"Did not select the {family_id} family for the current round."
                    )
            fixed_families.append(family_plan)
        normalized["family_plans"] = fixed_families
    if "layer_plans" in normalized:
        normalized.pop("layer_plans", None)

    return normalized


def synthesize_claim_meaning(record: dict[str, Any]) -> str:
    summary = maybe_text(record.get("summary"))
    statement = maybe_text(record.get("statement"))
    claim_type = maybe_text(record.get("claim_type")) or "public"
    if summary:
        return f"Preserves this {claim_type} claim as auditable evidence for later cross-domain matching: {summary}"
    if statement:
        return f"Preserves this {claim_type} claim as auditable evidence for later cross-domain matching: {statement}"
    return f"Preserves this {claim_type} claim as auditable evidence for later cross-domain matching."


def normalize_claim_curation_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    normalized = cloned_json(payload)
    status = maybe_text(normalized.get("status")).casefold()
    status_aliases = {
        "completed": "complete",
        "complete": "complete",
        "done": "complete",
        "finished": "complete",
        "in_progress": "pending",
        "in-progress": "pending",
        "pending": "pending",
        "blocked": "blocked",
    }
    if status in status_aliases:
        normalized["status"] = status_aliases[status]

    curated_claims = normalized.get("curated_claims")
    if isinstance(curated_claims, list):
        fixed_claims: list[Any] = []
        for index, claim in enumerate(curated_claims, start=1):
            if not isinstance(claim, dict):
                fixed_claims.append(claim)
                continue
            fixed_claim = dict(claim)
            if not maybe_text(fixed_claim.get("meaning")):
                fixed_claim["meaning"] = synthesize_claim_meaning(fixed_claim)
            priority = fixed_claim.get("priority")
            if isinstance(priority, str) and maybe_text(priority).isdigit():
                priority = int(maybe_text(priority))
            if not isinstance(priority, int):
                priority = index
            fixed_claim["priority"] = max(1, min(5, priority))
            fixed_claims.append(fixed_claim)
        normalized["curated_claims"] = fixed_claims

    return normalized


def normalize_matching_authorization_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    normalized = cloned_json(payload)

    status_aliases = {
        "approved": "authorized",
        "authorize": "authorized",
        "authorized": "authorized",
        "defer": "deferred",
        "deferred": "deferred",
        "not_authorized": "not-authorized",
        "not-authorized": "not-authorized",
        "denied": "not-authorized",
        "rejected": "not-authorized",
    }

    status = maybe_text(normalized.get("authorization_status")).casefold()
    if status in status_aliases:
        normalized["authorization_status"] = status_aliases[status]

    requested_status = maybe_text(normalized.get("moderator_requested_status")).casefold()
    if requested_status in status_aliases:
        normalized["moderator_requested_status"] = status_aliases[requested_status]

    basis_aliases = {
        "readiness_ready": "readiness-ready",
        "readiness-blocked": "readiness-blocked",
        "readiness_blocked": "readiness-blocked",
        "readiness-deferred": "readiness-deferred",
        "readiness_deferred": "readiness-deferred",
        "final_round_forced": "final-round-forced",
        "final-round-forced": "final-round-forced",
    }
    basis = maybe_text(normalized.get("authorization_basis")).casefold()
    if basis in basis_aliases:
        normalized["authorization_basis"] = basis_aliases[basis]

    if "rationale" not in normalized and "reason" in normalized:
        normalized["rationale"] = normalized.pop("reason")

    for field_name in ("referenced_readiness_ids", "claim_ids", "observation_ids", "open_questions"):
        values = normalized.get(field_name)
        if isinstance(values, list):
            normalized[field_name] = unique_strings([maybe_text(item) for item in values if maybe_text(item)])

    for field_name in ("allow_isolated_evidence", "moderator_override"):
        value = normalized.get(field_name)
        if isinstance(value, str):
            text = maybe_text(value).casefold()
            if text in {"true", "yes", "y", "1"}:
                normalized[field_name] = True
            elif text in {"false", "no", "n", "0"}:
                normalized[field_name] = False

    return normalized


def infer_matching_result_status(result: dict[str, Any]) -> str:
    matched_pairs = result.get("matched_pairs")
    matched_claim_ids = result.get("matched_claim_ids")
    matched_observation_ids = result.get("matched_observation_ids")
    unmatched_claim_ids = result.get("unmatched_claim_ids")
    unmatched_observation_ids = result.get("unmatched_observation_ids")

    has_matches = any(
        isinstance(values, list) and bool(values)
        for values in (matched_pairs, matched_claim_ids, matched_observation_ids)
    )
    has_unmatched = any(
        isinstance(values, list) and bool(values)
        for values in (unmatched_claim_ids, unmatched_observation_ids)
    )
    if has_matches and not has_unmatched:
        return "matched"
    if has_matches and has_unmatched:
        return "partial"
    return "unmatched"


def normalize_matching_adjudication_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    normalized = cloned_json(payload)
    matching_result = normalized.get("matching_result")
    if isinstance(matching_result, dict):
        fixed_result = dict(matching_result)
        status = maybe_text(fixed_result.get("result_status")).casefold()
        alias_map = {
            "matched": "matched",
            "match": "matched",
            "complete": infer_matching_result_status(fixed_result),
            "completed": infer_matching_result_status(fixed_result),
            "done": infer_matching_result_status(fixed_result),
            "finished": infer_matching_result_status(fixed_result),
            "partial": "partial",
            "partially-matched": "partial",
            "partially_matched": "partial",
            "unmatched": "unmatched",
            "none": "unmatched",
        }
        if status in alias_map:
            fixed_result["result_status"] = alias_map[status]
        elif not status:
            fixed_result["result_status"] = infer_matching_result_status(fixed_result)
        normalized["matching_result"] = fixed_result
    return normalized


def hydrate_source_selection_layer_skills(*, run_dir: Path, round_id: str, role: str, payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    family_plans = payload.get("family_plans")
    if not isinstance(family_plans, list):
        return payload

    packet = load_json_if_exists(source_selection_packet_path(run_dir, round_id, role))
    if not isinstance(packet, dict):
        return payload
    governance = packet.get("governance") if isinstance(packet.get("governance"), dict) else {}
    families = governance.get("families") if isinstance(governance.get("families"), list) else []
    layer_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        if not family_id:
            continue
        for layer in family.get("layers", []):
            if not isinstance(layer, dict):
                continue
            layer_id = maybe_text(layer.get("layer_id"))
            if not layer_id:
                continue
            layer_lookup[(family_id, layer_id)] = {
                "skills": [maybe_text(skill) for skill in layer.get("skills", []) if maybe_text(skill)],
                "tier": maybe_text(layer.get("tier")),
            }

    normalized = cloned_json(payload)
    fixed_families: list[Any] = []
    for family in family_plans:
        if not isinstance(family, dict):
            fixed_families.append(family)
            continue
        family_plan = dict(family)
        family_id = maybe_text(family_plan.get("family_id"))
        layer_plans = family_plan.get("layer_plans")
        if isinstance(layer_plans, list):
            fixed_layers: list[Any] = []
            for layer in layer_plans:
                if not isinstance(layer, dict):
                    fixed_layers.append(layer)
                    continue
                layer_plan = dict(layer)
                layer_id = maybe_text(layer_plan.get("layer_id"))
                layer_meta = layer_lookup.get((family_id, layer_id)) if family_id and layer_id else {}
                skills = layer_plan.get("source_skills")
                if (not isinstance(skills, list) or not [maybe_text(skill) for skill in skills if maybe_text(skill)]):
                    fallback_skills = layer_meta.get("skills") if isinstance(layer_meta, dict) else []
                    if fallback_skills:
                        layer_plan["source_skills"] = fallback_skills
                if not maybe_text(layer_plan.get("tier")):
                    fallback_tier = layer_meta.get("tier") if isinstance(layer_meta, dict) else ""
                    if fallback_tier:
                        layer_plan["tier"] = fallback_tier
                fixed_layers.append(layer_plan)
            family_plan["layer_plans"] = fixed_layers
        fixed_families.append(family_plan)
    normalized["family_plans"] = fixed_families
    return normalized


def normalize_agent_payload_for_schema(
    *,
    schema_kind: str,
    payload: Any,
    run_dir: Path,
    round_id: str,
    role: str,
) -> Any:
    if schema_kind == "source-selection":
        normalized = normalize_source_selection_payload(payload)
        return hydrate_source_selection_layer_skills(run_dir=run_dir, round_id=round_id, role=role, payload=normalized)
    if schema_kind == "claim-curation":
        return normalize_claim_curation_payload(payload)
    if schema_kind == "matching-authorization":
        return normalize_matching_authorization_payload(payload)
    if schema_kind == "matching-adjudication":
        return normalize_matching_adjudication_payload(payload)
    return payload


def ensure_task_review_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, list):
        raise ValueError("Task review payload must be a JSON list.")
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("Each round task must be a JSON object.")
        item_round_id = maybe_text(item.get("round_id"))
        if item_round_id and item_round_id != round_id:
            raise ValueError(f"Task round_id mismatch: expected {round_id}, got {item_round_id}")


def ensure_source_selection_matches(payload: Any, *, round_id: str, role: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Source-selection payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    payload_status = maybe_text(payload.get("status"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Source-selection round_id mismatch: expected {round_id}, got {payload_round_id}")
    if payload_role and payload_role != role:
        raise ValueError(f"Source-selection agent_role mismatch: expected {role}, got {payload_role}")
    if payload_status == "pending":
        raise ValueError("Source-selection payload must not remain pending when imported into the supervisor.")


def ensure_source_selection_respects_packet(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    payload: dict[str, Any],
    build_packet: Callable[[Path, str, str], Path] | None = None,
) -> None:
    packet = load_json_if_exists(source_selection_packet_path(run_dir, round_id, role))
    if not isinstance(packet, dict) and build_packet is not None:
        packet_path = build_packet(run_dir, round_id, role)
        packet = load_json_if_exists(packet_path)
    if not isinstance(packet, dict):
        raise ValueError("Source-selection packet is missing or invalid.")

    governance = packet.get("governance") if isinstance(packet.get("governance"), dict) else {}
    families = governance.get("families") if isinstance(governance.get("families"), list) else []
    approved_layers = governance.get("approved_layers") if isinstance(governance.get("approved_layers"), list) else []
    family_lookup: dict[str, dict[str, Any]] = {}
    for family in families:
        if not isinstance(family, dict):
            continue
        family_id = maybe_text(family.get("family_id"))
        if family_id:
            family_lookup[family_id] = family
    approved_lookup = {
        (maybe_text(item.get("family_id")), maybe_text(item.get("layer_id"))): item
        for item in approved_layers
        if isinstance(item, dict) and maybe_text(item.get("family_id")) and maybe_text(item.get("layer_id"))
    }

    family_plans = payload.get("family_plans")
    if not isinstance(family_plans, list):
        raise ValueError("Source-selection payload must include family_plans.")
    payload_family_lookup: dict[str, dict[str, Any]] = {}
    for family_plan in family_plans:
        if not isinstance(family_plan, dict):
            continue
        family_id = maybe_text(family_plan.get("family_id"))
        if family_id:
            payload_family_lookup[family_id] = family_plan

    expected_family_ids = set(family_lookup)
    payload_family_ids = set(payload_family_lookup)
    if expected_family_ids and payload_family_ids != expected_family_ids:
        missing = sorted(expected_family_ids - payload_family_ids)
        extra = sorted(payload_family_ids - expected_family_ids)
        raise ValueError(f"Source-selection family_plans must match packet governance families. Missing={missing}, extra={extra}")

    max_sources = governance.get("max_selected_sources_per_role")
    selected_sources = contract_call("selected_sources_from_family_plans", payload)
    if not isinstance(selected_sources, list):
        selected_sources = payload.get("selected_sources") if isinstance(payload.get("selected_sources"), list) else []
    if isinstance(max_sources, int) and max_sources > 0 and len(selected_sources) > max_sources:
        raise ValueError(f"Selected sources {selected_sources} exceed governance max_selected_sources_per_role={max_sources}.")

    selected_family_count = 0
    selected_non_entry_layers = 0
    allow_cross_round_anchors = bool(governance.get("allow_cross_round_anchors"))

    for family_id, family_plan in payload_family_lookup.items():
        family_policy = family_lookup.get(family_id)
        if not isinstance(family_policy, dict):
            raise ValueError(f"family_plans references unknown family_id: {family_id}")
        layer_lookup = {
            maybe_text(layer.get("layer_id")): layer
            for layer in family_policy.get("layers", [])
            if isinstance(layer, dict) and maybe_text(layer.get("layer_id"))
        }
        layer_plans = family_plan.get("layer_plans")
        if not isinstance(layer_plans, list):
            raise ValueError(f"family_plans.{family_id}.layer_plans must be a list.")
        payload_layer_ids = {
            maybe_text(layer.get("layer_id"))
            for layer in layer_plans
            if isinstance(layer, dict) and maybe_text(layer.get("layer_id"))
        }
        if set(layer_lookup) != payload_layer_ids:
            missing = sorted(set(layer_lookup) - payload_layer_ids)
            extra = sorted(payload_layer_ids - set(layer_lookup))
            raise ValueError(f"family_plans.{family_id}.layer_plans must match governance layers. Missing={missing}, extra={extra}")

        if family_plan.get("selected") is True:
            selected_family_count += 1

        for layer_plan in layer_plans:
            if not isinstance(layer_plan, dict):
                continue
            layer_id = maybe_text(layer_plan.get("layer_id"))
            layer_policy = layer_lookup.get(layer_id)
            if not isinstance(layer_policy, dict):
                continue
            if maybe_text(layer_plan.get("tier")) != maybe_text(layer_policy.get("tier")):
                raise ValueError(f"{family_id}.{layer_id} tier must match governance tier.")
            skills = layer_plan.get("source_skills")
            if not isinstance(skills, list):
                raise ValueError(f"{family_id}.{layer_id} source_skills must be a list.")
            selected_skill_set = {maybe_text(skill) for skill in skills if maybe_text(skill)}
            allowed_skill_set = {
                maybe_text(skill)
                for skill in layer_policy.get("skills", [])
                if maybe_text(skill)
            }
            if not selected_skill_set <= allowed_skill_set:
                invalid = sorted(selected_skill_set - allowed_skill_set)
                raise ValueError(f"{family_id}.{layer_id} selected unsupported skills: {invalid}")
            max_selected = layer_policy.get("max_selected_skills")
            if isinstance(max_selected, int) and max_selected > 0 and len(selected_skill_set) > max_selected:
                raise ValueError(f"{family_id}.{layer_id} selected {len(selected_skill_set)} skills but max_selected_skills={max_selected}.")

            if layer_plan.get("selected") is not True:
                continue

            tier = maybe_text(layer_policy.get("tier"))
            anchor_mode = maybe_text(layer_plan.get("anchor_mode"))
            anchor_refs = layer_plan.get("anchor_refs") if isinstance(layer_plan.get("anchor_refs"), list) else []
            authorization_basis = maybe_text(layer_plan.get("authorization_basis"))
            auto_selectable = layer_policy.get("auto_selectable") is True
            requires_anchor = layer_policy.get("requires_anchor") is True

            if tier == "l2":
                selected_non_entry_layers += 1
            if requires_anchor and (anchor_mode == "none" or not anchor_refs):
                raise ValueError(f"{family_id}.{layer_id} requires a non-empty anchor_refs list and a non-none anchor_mode.")
            if anchor_mode == "prior_round_l1" and not allow_cross_round_anchors:
                raise ValueError(f"{family_id}.{layer_id} cannot use prior_round_l1 because governance disallows cross-round anchors.")

            approval_key = (family_id, layer_id)
            if tier == "l1":
                if authorization_basis != "entry-layer":
                    raise ValueError(f"{family_id}.{layer_id} must use authorization_basis=entry-layer for L1 selection.")
            else:
                if approval_key in approved_lookup:
                    if authorization_basis != "upstream-approval":
                        raise ValueError(f"{family_id}.{layer_id} must use authorization_basis=upstream-approval.")
                elif auto_selectable:
                    if authorization_basis != "policy-auto":
                        raise ValueError(f"{family_id}.{layer_id} is auto-selectable and must use authorization_basis=policy-auto.")
                else:
                    raise ValueError(f"{family_id}.{layer_id} is not upstream-approved and not policy-auto, so it cannot be selected.")

    max_families = governance.get("max_active_families_per_role")
    if isinstance(max_families, int) and max_families > 0 and selected_family_count > max_families:
        raise ValueError(
            f"Selected families {selected_family_count} exceed governance max_active_families_per_role={max_families}."
        )
    max_l2_layers = governance.get("max_non_entry_layers_per_role")
    if isinstance(max_l2_layers, int) and max_l2_layers >= 0 and selected_non_entry_layers > max_l2_layers:
        raise ValueError(
            f"Selected non-entry layers {selected_non_entry_layers} exceed governance max_non_entry_layers_per_role={max_l2_layers}."
        )


def ensure_claim_curation_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Claim-curation payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    payload_status = maybe_text(payload.get("status"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Claim-curation round_id mismatch: expected {round_id}, got {payload_round_id}")
    if payload_role and payload_role != "sociologist":
        raise ValueError(f"Claim-curation agent_role mismatch: expected sociologist, got {payload_role}")
    if payload_status == "pending":
        raise ValueError("Claim-curation payload must not remain pending when imported into the supervisor.")


def ensure_observation_curation_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Observation-curation payload must be a JSON object.")
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    payload_status = maybe_text(payload.get("status"))
    if payload_round_id and payload_round_id != round_id:
        raise ValueError(f"Observation-curation round_id mismatch: expected {round_id}, got {payload_round_id}")
    if payload_role and payload_role != "environmentalist":
        raise ValueError(f"Observation-curation agent_role mismatch: expected environmentalist, got {payload_role}")
    if payload_status == "pending":
        raise ValueError("Observation-curation payload must not remain pending when imported into the supervisor.")


def ensure_data_readiness_matches(payload: Any, *, round_id: str, role: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Data-readiness payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_round_id and require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Data-readiness round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    if payload_role and payload_role != role:
        raise ValueError(f"Data-readiness agent_role mismatch: expected {role}, got {payload_role}")


def ensure_matching_authorization_matches(payload: Any, *, round_id: str, expected_run_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Matching-authorization payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_run_id = maybe_text(payload.get("run_id"))
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_run_id and payload_run_id != expected_run_id:
        raise ValueError(f"Matching-authorization run_id mismatch: expected {expected_run_id}, got {payload_run_id}")
    if payload_round_id and require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Matching-authorization round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    if payload_role and payload_role != "moderator":
        raise ValueError(f"Matching-authorization agent_role mismatch: expected moderator, got {payload_role}")


def ensure_matching_adjudication_matches(payload: Any, *, round_id: str, expected_run_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Matching-adjudication payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_run_id = maybe_text(payload.get("run_id"))
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_run_id and payload_run_id != expected_run_id:
        raise ValueError(f"Matching-adjudication run_id mismatch: expected {expected_run_id}, got {payload_run_id}")
    if payload_round_id and require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Matching-adjudication round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    if payload_role and payload_role != "moderator":
        raise ValueError(f"Matching-adjudication agent_role mismatch: expected moderator, got {payload_role}")


def ensure_investigation_review_matches(payload: Any, *, round_id: str, expected_run_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Investigation-review payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_run_id = maybe_text(payload.get("run_id"))
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_run_id and payload_run_id != expected_run_id:
        raise ValueError(f"Investigation-review run_id mismatch: expected {expected_run_id}, got {payload_run_id}")
    if payload_round_id and require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Investigation-review round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    if payload_role and payload_role != "moderator":
        raise ValueError(f"Investigation-review agent_role mismatch: expected moderator, got {payload_role}")


def ensure_report_matches(payload: Any, *, round_id: str, role: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Report payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_round_id = maybe_text(payload.get("round_id"))
    payload_role = maybe_text(payload.get("agent_role"))
    if payload_round_id and require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Report round_id mismatch: expected {expected_round_id}, got {payload_round_id}")
    if payload_role and payload_role != role:
        raise ValueError(f"Report agent_role mismatch: expected {role}, got {payload_role}")


def ensure_decision_matches(payload: Any, *, round_id: str) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Decision payload must be a JSON object.")
    expected_round_id = require_round_id(round_id)
    payload_round_id = maybe_text(payload.get("round_id"))
    if payload_round_id and require_round_id(payload_round_id) != expected_round_id:
        raise ValueError(f"Decision round_id mismatch: expected {expected_round_id}, got {payload_round_id}")


def extract_override_requests(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    value = payload.get("override_requests")
    if not isinstance(value, list):
        return []
    return [cloned_json(item) for item in value if isinstance(item, dict)]


def persist_override_requests_for_role(
    *,
    run_dir: Path,
    round_id: str,
    role: str,
    origin_kind: str,
    payload: Any,
) -> None:
    target = override_requests_path(run_dir, round_id, role)
    current_payload = load_json_if_exists(target)
    current_items = current_payload if isinstance(current_payload, list) else []
    merged: dict[str, dict[str, Any]] = {}
    for item in current_items:
        if not isinstance(item, dict):
            continue
        if maybe_text(item.get("request_origin_kind")) == origin_kind:
            continue
        request_id = maybe_text(item.get("request_id"))
        if request_id:
            merged[request_id] = cloned_json(item)
    for item in extract_override_requests(payload):
        request_id = maybe_text(item.get("request_id"))
        if request_id:
            merged[request_id] = item
    write_json(target, list(sorted(merged.values(), key=lambda item: maybe_text(item.get("request_id")))), pretty=True)

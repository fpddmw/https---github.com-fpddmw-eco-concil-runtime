from __future__ import annotations

import sys
import unittest

from _workflow_support import runtime_src_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.contracts import (  # noqa: E402
    FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE,
    FIELD_GROUP_EVIDENCE_LINEAGE,
    FIELD_GROUP_REPORT_CLAIM_LINKAGE,
    PLANE_ANALYSIS,
    PLANE_DELIBERATION,
    PLANE_REPORTING,
    PLANE_RUNTIME,
    canonical_contract,
    canonical_contract_kinds,
    contract_field_group,
    contract_field_groups,
    validate_canonical_payload,
)


class CanonicalContractTests(unittest.TestCase):
    def test_registry_exposes_batch1_target_contracts(self) -> None:
        expected_kinds = {
            "formal-comment-signal",
            "public-discourse-signal",
            "environment-observation-signal",
            "issue-cluster",
            "stance-group",
            "concern-facet",
            "actor-profile",
            "evidence-citation-type",
            "claim-candidate",
            "claim-cluster",
            "claim-scope",
            "verifiability-assessment",
            "verification-route",
            "acquisition-checkpoint",
            "theme-sufficiency-review",
            "report-blueprint",
            "investigation-theme",
            "theme-acquisition-plan",
            "formal-public-link",
            "representation-gap",
            "spatiotemporal-relation-cue",
            "diffusion-edge",
            "controversy-map",
            "proposal",
            "hypothesis",
            "challenge",
            "discussion-message",
            "evidence-bundle",
            "finding",
            "board-task",
            "next-action",
            "probe",
            "readiness-opinion",
            "readiness-assessment",
            "agent-section-brief",
            "report-section-draft",
            "review-comment",
            "investigation-plan",
            "subissue",
            "investigation-scope",
            "round-brief",
            "round-synthesis",
            "evidence-request",
            "source-acquisition-proposal",
            "evidence-route-assessment",
            "agent-position",
            "context-packet",
            "challenge-disposition",
            "report-basis-freeze",
            "decision-trace",
            "runtime-control-freeze",
            "controller-state",
            "gate-state",
            "supervisor-state",
            "orchestration-plan",
            "orchestration-plan-step",
            "skill-approval",
            "skill-approval-consumption",
            "skill-approval-rejection",
            "skill-approval-request",
            "transition-approval",
            "transition-rejection",
            "transition-request",
            "reporting-handoff",
            "council-decision",
            "expert-report",
            "final-publication",
        }
        self.assertSetEqual(expected_kinds, set(canonical_contract_kinds()))
        self.assertSetEqual(
            {
                "actor-profile",
                "claim-candidate",
                "claim-cluster",
                "claim-scope",
                "concern-facet",
                "controversy-map",
                "diffusion-edge",
                "evidence-citation-type",
                "formal-public-link",
                "issue-cluster",
                "representation-gap",
                "spatiotemporal-relation-cue",
                "stance-group",
                "verifiability-assessment",
                "verification-route",
                "acquisition-checkpoint",
                "theme-sufficiency-review",
            },
            set(canonical_contract_kinds(plane=PLANE_ANALYSIS)),
        )
        self.assertSetEqual(
            {
                "proposal",
                "hypothesis",
                "challenge",
                "discussion-message",
                "evidence-bundle",
                "finding",
                "board-task",
                "next-action",
                "probe",
                "readiness-opinion",
                "readiness-assessment",
                "review-comment",
                "investigation-plan",
                "subissue",
                "investigation-scope",
                "round-brief",
                "round-synthesis",
                "evidence-request",
                "source-acquisition-proposal",
                "evidence-route-assessment",
                "agent-position",
                "context-packet",
                "challenge-disposition",
                "report-basis-freeze",
                "decision-trace",
                "report-blueprint",
                "investigation-theme",
                "theme-acquisition-plan",
            },
            set(canonical_contract_kinds(plane=PLANE_DELIBERATION)),
        )
        self.assertSetEqual(
            {
                "reporting-handoff",
                "council-decision",
                "expert-report",
                "final-publication",
                "agent-section-brief",
                "report-section-draft",
            },
            set(canonical_contract_kinds(plane=PLANE_REPORTING)),
        )
        self.assertSetEqual(
            {
                "controller-state",
                "gate-state",
                "orchestration-plan",
                "orchestration-plan-step",
                "runtime-control-freeze",
                "skill-approval",
                "skill-approval-consumption",
                "skill-approval-rejection",
                "skill-approval-request",
                "supervisor-state",
                "transition-approval",
                "transition-rejection",
                "transition-request",
            },
            set(canonical_contract_kinds(plane=PLANE_RUNTIME)),
        )

    def test_contracts_expose_shared_minimal_field_groups(self) -> None:
        groups = {group["group_id"] for group in contract_field_groups()}
        self.assertIn(FIELD_GROUP_EVIDENCE_LINEAGE, groups)
        self.assertIn(FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE, groups)
        self.assertIn(FIELD_GROUP_REPORT_CLAIM_LINKAGE, groups)

        evidence_group = contract_field_group(FIELD_GROUP_EVIDENCE_LINEAGE)
        self.assertEqual(("evidence_refs", "lineage"), evidence_group.list_fields)

        section_contract = canonical_contract("report-section-draft")
        self.assertIn(FIELD_GROUP_REPORT_CLAIM_LINKAGE, section_contract.field_groups)
        self.assertIn("claim_text", section_contract.optional_text_fields)
        self.assertIn("claim_constraint_ids", section_contract.optional_list_fields)
        self.assertIn("lead_basis", section_contract.optional_bool_fields)

        readiness_contract = canonical_contract("readiness-assessment")
        self.assertIn(
            FIELD_GROUP_CHALLENGER_CONSTRAINT_STATE,
            readiness_contract.field_groups,
        )

    def test_validate_canonical_payload_rejects_missing_structural_fields(self) -> None:
        with self.assertRaises(ValueError):
            validate_canonical_payload(
                "proposal",
                {
                    "run_id": "run-001",
                    "round_id": "round-001",
                    "proposal_id": "proposal-001",
                    "proposal_kind": "open-investigation",
                    "agent_role": "moderator",
                    "status": "open",
                    "target_kind": "issue-cluster",
                    "target_id": "issue-001",
                    "rationale": "Need more analysis.",
                    "decision_source": "agent-council",
                    "evidence_refs": [],
                },
            )

    def test_validate_canonical_payload_accepts_well_formed_proposal(self) -> None:
        payload = validate_canonical_payload(
            "proposal",
            {
                "run_id": "run-001",
                "round_id": "round-001",
                "proposal_id": "proposal-001",
                "proposal_kind": "open-investigation",
                "agent_role": "moderator",
                "status": "open",
                "target_kind": "issue-cluster",
                "target_id": "issue-001",
                "rationale": "Need more analysis.",
                "confidence": 0.72,
                "decision_source": "agent-council",
                "evidence_refs": ["evidence://issue-001"],
                "lineage": [],
                "response_to_ids": [],
                "provenance": {"source": "unit-test"},
                "target": {
                    "object_kind": "issue-cluster",
                    "object_id": "issue-001",
                },
            },
        )
        contract = canonical_contract("proposal")
        self.assertEqual(contract.schema_version, payload["schema_version"])
        self.assertEqual("proposal-001", payload["proposal_id"])

    def test_validate_canonical_payload_rejects_malformed_optional_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "optional list fields"):
            validate_canonical_payload(
                "report-section-draft",
                {
                    "run_id": "run-001",
                    "round_id": "round-001",
                    "section_id": "section-001",
                    "decision_source": "report-editor",
                    "agent_role": "report-editor",
                    "status": "draft",
                    "report_id": "round-001",
                    "section_key": "summary",
                    "section_title": "Summary",
                    "section_text": "Draft text.",
                    "evidence_refs": ["evidence://issue-001"],
                    "lineage": [],
                    "basis_object_ids": [],
                    "bundle_ids": [],
                    "finding_ids": [],
                    "provenance": {"source": "unit-test"},
                    "claim_text": "Claim supplied by report editor.",
                    "claim_constraint_ids": "challenger-constraint-001",
                    "lead_basis": False,
                },
            )

    def test_validate_canonical_payload_accepts_well_formed_claim_candidate(self) -> None:
        payload = validate_canonical_payload(
            "claim-candidate",
            {
                "run_id": "run-001",
                "round_id": "round-001",
                "claim_id": "claim-001",
                "agent_role": "social-investigator",
                "claim_type": "hazard-impact",
                "status": "candidate",
                "summary": "Smoke is worsening.",
                "statement": "Residents reported worsening smoke conditions across the city.",
                "issue_hint": "air-quality-smoke",
                "issue_terms": ["smoke", "air", "quality"],
                "stance_hint": "report-impact",
                "concern_facets": ["health-safety"],
                "actor_hints": ["resident"],
                "evidence_citation_types": ["news-report"],
                "verifiability_hint": "empirical-observable",
                "dispute_type": "impact-severity",
                "decision_source": "runtime-fallback",
                "confidence": 0.72,
                "rationale": "Grouped repeated smoke-impact reports into one candidate.",
                "source_signal_count": 2,
                "source_signal_ids": ["signal-001", "signal-002"],
                "evidence_refs": [],
                "lineage": ["signal-001", "signal-002"],
                "provenance": {"source": "unit-test"},
                "controversy_seed": {"issue_hint": "air-quality-smoke"},
                "time_window": {"start_utc": "", "end_utc": ""},
                "place_scope": {"label": "Public evidence footprint", "geometry": {}},
                "claim_scope": {"label": "Public evidence footprint", "geometry": {}, "usable_for_matching": False},
                "compact_audit": {"representative": False},
            },
        )
        self.assertEqual("claim-candidate-v1", payload["schema_version"])
        self.assertEqual("claim-001", payload["claim_id"])

    def test_validate_canonical_payload_accepts_well_formed_issue_cluster(self) -> None:
        payload = validate_canonical_payload(
            "issue-cluster",
            {
                "run_id": "run-001",
                "round_id": "round-001",
                "cluster_id": "issuemap-001",
                "map_issue_id": "issuemap-001",
                "claim_cluster_id": "claimcluster-001",
                "issue_label": "air-quality-smoke",
                "claim_type": "hazard-impact",
                "dominant_stance": "report-impact",
                "verifiability_kind": "empirical-observable",
                "dispute_type": "impact-severity",
                "recommended_lane": "environmental-observation",
                "route_status": "route-to-verification-lane",
                "controversy_posture": "empirical-issue",
                "issue_summary": "Smoke issue is routed for empirical verification.",
                "decision_source": "runtime-fallback",
                "member_count": 2,
                "aggregate_source_signal_count": 3,
                "confidence": 0.81,
                "claim_ids": ["claim-001", "claim-002"],
                "source_signal_ids": ["signal-001", "signal-002"],
                "stance_distribution": [{"stance": "report-impact", "count": 2}],
                "stance_group_ids": ["stancegroup-001"],
                "concern_ids": ["concern-001"],
                "actor_ids": ["actor-001"],
                "citation_type_ids": ["citationtype-001"],
                "concern_facets": ["health-safety"],
                "actor_hints": ["resident"],
                "evidence_citation_types": ["news-report"],
                "evidence_refs": [],
                "lineage": ["issuemap-001", "claimcluster-001", "signal-001"],
                "rationale": "Projected the controversy map issue into a typed issue cluster.",
                "provenance": {"source": "unit-test"},
            },
        )
        self.assertEqual("issue-cluster-v1", payload["schema_version"])
        self.assertEqual("issuemap-001", payload["cluster_id"])

    def test_validate_canonical_payload_accepts_well_formed_actor_profile(self) -> None:
        payload = validate_canonical_payload(
            "actor-profile",
            {
                "run_id": "run-001",
                "round_id": "round-001",
                "actor_id": "actor-001",
                "cluster_id": "issuemap-001",
                "map_issue_id": "issuemap-001",
                "claim_cluster_id": "claimcluster-001",
                "issue_label": "air-quality-smoke",
                "claim_type": "hazard-impact",
                "display_name": "resident",
                "actor_label": "resident",
                "dominant_stance": "report-impact",
                "recommended_lane": "environmental-observation",
                "route_status": "route-to-verification-lane",
                "profile_summary": "Residents dominate the smoke issue cluster.",
                "decision_source": "runtime-fallback",
                "claim_count": 2,
                "source_signal_count": 3,
                "confidence": 0.74,
                "claim_ids": ["claim-001", "claim-002"],
                "source_signal_ids": ["signal-001", "signal-002"],
                "concern_facets": ["health-safety"],
                "evidence_citation_types": ["news-report"],
                "evidence_refs": [],
                "lineage": ["issuemap-001", "claimcluster-001", "signal-001"],
                "rationale": "Projected actor hints into a typed actor profile.",
                "provenance": {"source": "unit-test"},
            },
        )
        self.assertEqual("actor-profile-v1", payload["schema_version"])
        self.assertEqual("actor-001", payload["actor_id"])

    def test_validate_canonical_payload_accepts_well_formed_formal_public_link(self) -> None:
        payload = validate_canonical_payload(
            "formal-public-link",
            {
                "run_id": "run-001",
                "round_id": "round-001",
                "linkage_id": "fplink-001",
                "issue_label": "air-quality-smoke",
                "issue_terms": ["smoke", "air quality"],
                "concern_facets": ["health-safety"],
                "actor_hints": ["resident"],
                "cluster_ids": ["claimcluster-001"],
                "claim_ids": ["claim-001"],
                "claim_scope_ids": ["scope-001"],
                "assessment_ids": ["assessment-001"],
                "route_ids": ["route-001"],
                "formal_signal_ids": ["signal-formal-001"],
                "public_signal_ids": ["signal-public-001"],
                "formal_signal_count": 1,
                "public_signal_count": 1,
                "formal_source_skills": ["fetch-regulationsgov-comments"],
                "public_source_skills": ["fetch-youtube-video-search"],
                "formal_examples": ["Formal smoke comment"],
                "public_examples": ["Public smoke video"],
                "alignment_score": 0.86,
                "link_status": "aligned",
                "recommended_lane": "environmental-observation",
                "route_status": "route-to-verification-lane",
                "linkage_summary": "Linked formal comments and public discourse for smoke.",
                "decision_source": "runtime-fallback",
                "evidence_refs": [],
                "lineage": ["claimcluster-001", "claim-001", "route-001"],
                "rationale": "Unified formal and public footprints for one issue.",
                "provenance": {"source": "unit-test"},
            },
        )
        self.assertEqual("formal-public-link-v1", payload["schema_version"])
        self.assertEqual("fplink-001", payload["linkage_id"])


if __name__ == "__main__":
    unittest.main()

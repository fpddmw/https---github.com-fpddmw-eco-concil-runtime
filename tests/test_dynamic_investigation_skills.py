from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from _workflow_support import run_kernel, run_script, runtime_src_path, script_path

RUNTIME_SRC = runtime_src_path()
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.governance.role_contracts import role_capabilities  # noqa: E402
from eco_council_runtime.kernel.governance.skill_registry import resolve_skill_policy  # noqa: E402

RUN_ID = "run-dynamic-investigation-skills-001"
ROUND_ID = "round-dynamic-investigation-skills-001"


class DynamicInvestigationSkillTests(unittest.TestCase):
    def test_submit_dynamic_coordination_skills_write_queryable_objects(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            plan = run_script(
                script_path("submit-investigation-plan"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--mission-ref",
                "mission://broad-river-pollution",
                "--rationale",
                "Frame broad investigation without locking a single river or metric.",
                "--proposed-subissue-ref",
                "subissue://candidate-water-quality",
                "--open-question",
                "Which river systems should agents inspect first?",
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            scope = run_script(
                script_path("submit-investigation-scope"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "environmental-investigator",
                "--scope-kind",
                "candidate",
                "--spatial-scope",
                "multi-region, unresolved",
                "--rationale",
                "Keep spatial scope revisable until source availability is known.",
                "--target-kind",
                "investigation-plan",
                "--target-id",
                plan["canonical_ids"][0],
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            request = run_script(
                script_path("submit-evidence-request"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "challenger",
                "--question",
                "What contradicts the initial pollution framing?",
                "--desired-evidence-type",
                "counterexample records",
                "--rationale",
                "Ask for contradiction evidence without ranking sources.",
                "--target-kind",
                "investigation-scope",
                "--target-id",
                scope["canonical_ids"][0],
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            position = run_script(
                script_path("submit-agent-position"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "environmental-investigator",
                "--target-kind",
                "evidence-request",
                "--target-id",
                request["canonical_ids"][0],
                "--claim-summary",
                "The request is investigable but requires source-specific follow-up.",
                "--rationale",
                "No conclusion is adopted until agents submit evidence objects.",
                "--limitation",
                "No source family has been accepted yet.",
                "--evidence-ref",
                "evidence://request-context/001",
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )
            disposition = run_script(
                script_path("submit-challenge-disposition"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--author-role",
                "moderator",
                "--target-kind",
                "review-comment",
                "--target-id",
                "review-comment-dynamic-001",
                "--response-to-id",
                "review-comment-dynamic-001",
                "--disposition-status",
                "accepted-as-limitation",
                "--decided-by-role",
                "moderator",
                "--rationale",
                "Record a bounded report-use disposition without judging the challenge truth.",
                "--evidence-ref",
                "evidence://request-context/001",
                "--provenance-json",
                json.dumps({"source": "unit-test"}, ensure_ascii=True, sort_keys=True),
            )

            query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "agent-position",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--agent-role",
                "environmental-investigator",
                "--target-kind",
                "evidence-request",
                "--target-id",
                request["canonical_ids"][0],
            )

            self.assertEqual(1, query["summary"]["returned_object_count"])
            self.assertEqual(position["canonical_ids"][0], query["objects"][0]["object_id"])
            self.assertEqual("agent-position", query["objects"][0]["object_kind"])
            self.assertNotIn("score", query["objects"][0])
            self.assertNotIn("priority", query["objects"][0])

            disposition_query = run_kernel(
                "query-council-objects",
                "--run-dir",
                str(run_dir),
                "--object-kind",
                "challenge-disposition",
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--target-kind",
                "review-comment",
                "--target-id",
                "review-comment-dynamic-001",
            )

            self.assertEqual(1, disposition_query["summary"]["returned_object_count"])
            self.assertEqual(
                disposition["canonical_ids"][0],
                disposition_query["objects"][0]["object_id"],
            )
            self.assertEqual(
                "accepted-as-limitation",
                disposition_query["objects"][0]["disposition_status"],
            )

    def test_dynamic_coordination_skill_rejects_heuristic_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir) / "run"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script_path("submit-round-brief")),
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    RUN_ID,
                    "--round-id",
                    ROUND_ID,
                    "--rationale",
                    "Brief agents without ordering their work.",
                    "--payload-json",
                    json.dumps({"priority": "high"}, ensure_ascii=True, sort_keys=True),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(0, completed.returncode)
            payload = json.loads(completed.stdout)
            self.assertEqual("failed", payload["status"])
            self.assertIn("heuristic/control field `priority`", payload["message"])

    def test_dynamic_coordination_skill_policies_match_role_capabilities(self) -> None:
        for skill_name in (
            "submit-investigation-plan",
            "submit-investigation-scope",
            "submit-round-brief",
            "materialize-context-packet",
            "submit-evidence-request",
            "submit-agent-position",
            "submit-challenge-disposition",
        ):
            policy = resolve_skill_policy(skill_name)
            required = set(policy["required_capabilities"])
            for role in policy["allowed_roles"]:
                self.assertTrue(required.issubset(role_capabilities(role)), skill_name)
            self.assertNotIn("runtime-operator", policy["allowed_roles"])
            self.assertEqual("deliberation-write", policy["skill_layer"])


if __name__ == "__main__":
    unittest.main()

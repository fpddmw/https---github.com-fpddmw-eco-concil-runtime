from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import unittest

from _workflow_support import script_path


def load_gdelt_doc_module():
    path = script_path("fetch-gdelt-doc-search")
    spec = importlib.util.spec_from_file_location("fetch_gdelt_doc_search_under_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class GdeltDocQuerySafetyTests(unittest.TestCase):
    def test_lint_rejects_search_engine_site_operator(self) -> None:
        module = load_gdelt_doc_module()

        result = module.lint_doc_query("site:airnow.gov smoke")

        self.assertFalse(result["ok"])
        self.assertEqual("unsupported-operator-site", result["errors"][0]["code"])
        self.assertIn("domainis:example.gov", result["errors"][0]["message"])

    def test_domain_is_splits_into_compact_queries(self) -> None:
        module = load_gdelt_doc_module()

        queries = [
            module.compose_query("smoke wildfire \"New York City\"", item)
            for item in ["domainis:airnow.gov", "domainis:epa.gov"]
        ]

        self.assertEqual(
            [
                'domainis:airnow.gov smoke wildfire "New York City"',
                'domainis:epa.gov smoke wildfire "New York City"',
            ],
            queries,
        )
        self.assertTrue(all(module.lint_doc_query(query)["ok"] for query in queries))

    def test_lint_rejects_unparenthesized_or(self) -> None:
        module = load_gdelt_doc_module()

        result = module.lint_doc_query('smoke OR "air quality" OR advisory')

        self.assertFalse(result["ok"])
        self.assertEqual("unparenthesized-or", result["errors"][0]["code"])

    def test_domain_filter_auto_groups_top_level_or_query(self) -> None:
        module = load_gdelt_doc_module()

        query = module.compose_query(
            'smoke OR "air quality" OR advisory',
            "domainis:nyc.gov",
        )

        self.assertEqual(
            'domainis:nyc.gov (smoke OR "air quality" OR advisory)',
            query,
        )
        self.assertTrue(module.lint_doc_query(query)["ok"])

    def test_parenthesized_or_query_passes_lint(self) -> None:
        module = load_gdelt_doc_module()

        result = module.lint_doc_query(
            'domainis:nyc.gov (smoke OR "air quality" OR advisory) "New York City"'
        )

        self.assertTrue(result["ok"])

    def test_lint_query_cli_returns_structured_guidance(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(script_path("fetch-gdelt-doc-search")),
                "lint-query",
                "--query",
                "site:airnow.gov smoke",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(2, completed.returncode)
        payload = json.loads(completed.stdout)
        self.assertFalse(payload["ok"])
        self.assertEqual("unsupported-operator-site", payload["queries"][0]["errors"][0]["code"])

    def test_lint_query_cli_auto_groups_domain_or_query(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(script_path("fetch-gdelt-doc-search")),
                "lint-query",
                "--query",
                'smoke OR "air quality" OR advisory',
                "--domain-is",
                "nyc.gov",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(0, completed.returncode, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(
            'domainis:nyc.gov (smoke OR "air quality" OR advisory)',
            payload["queries"][0]["query"],
        )

    def test_merge_json_payloads_dedupes_articles(self) -> None:
        module = load_gdelt_doc_module()

        merged = module.merge_json_payloads(
            [
                {
                    "query": "domainis:a.gov smoke",
                    "data": {"articles": [{"url": "https://example.com/a", "title": "A"}]},
                },
                {
                    "query": "domainis:b.gov smoke",
                    "data": {
                        "articles": [
                            {"url": "https://example.com/a", "title": "A"},
                            {"url": "https://example.com/b", "title": "B"},
                        ]
                    },
                },
            ]
        )

        self.assertEqual(2, len(merged["articles"]))
        self.assertEqual(2, len(merged["batches"]))


if __name__ == "__main__":
    unittest.main()

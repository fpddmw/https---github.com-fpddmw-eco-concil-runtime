from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

from _workflow_support import analytics_path, load_json, run_script, script_path

RUN_ID = "run-usbr-rise-001"
ROUND_ID = "round-usbr-rise-001"


@contextmanager
def fixture_server(routes: dict[str, tuple[str, str]]) -> Iterator[str]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path not in routes:
                self.send_response(404)
                self.end_headers()
                return
            content_type, body = routes[path]
            raw = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def normalized_rows(run_dir: Path) -> list[dict[str, object]]:
    with sqlite3.connect(analytics_path(run_dir, "signal_plane.sqlite")) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT source_skill, plane, signal_kind, canonical_object_kind,
                   metric, numeric_value, unit, observed_at_utc,
                   latitude, longitude, metadata_json, quality_flags_json
            FROM normalized_signals
            WHERE source_skill = 'fetch-usbr-rise'
            ORDER BY observed_at_utc
            """
        ).fetchall()
    return [dict(row) for row in rows]


class UsbrRiseWorkflowTests(unittest.TestCase):
    def test_discover_usbr_rise_items_outputs_candidate_item_ids(self) -> None:
        catalog_payload = {
            "@context": "/rise/api/contexts/CatalogItem",
            "@id": "/rise/api/catalog-item",
            "@type": "Collection",
            "totalItems": 2,
            "member": [
                {
                    "@id": "/rise/api/catalog-item/10835",
                    "@type": ["CatalogItem", "dcat:Dataset"],
                    "id": 10835,
                    "itemTitle": "Glen Canyon Dam Daily Release Time Series Data",
                    "itemDescription": "Lake Powell operations data.",
                    "locationName": "Glen Canyon Dam",
                    "parameterId": 15,
                    "parameterName": "Lake/Reservoir Release - Total",
                    "parameterUnit": "cfs",
                    "parameterGroup": "Lake/Reservoir Outflow",
                    "sourceCode": "HAR",
                },
                {
                    "@id": "/rise/api/catalog-item/99999",
                    "@type": ["CatalogItem", "dcat:Dataset"],
                    "id": 99999,
                    "itemTitle": "Yakima River Water Temperature Time Series Data",
                    "locationName": "Yakima River",
                    "parameterId": 24,
                    "parameterName": "Water Temperature",
                    "parameterUnit": "DegF",
                },
            ],
            "view": {
                "@id": "/rise/api/catalog-item?page=1",
                "@type": "PartialCollectionView",
                "first": "/rise/api/catalog-item?page=1",
                "last": "/rise/api/catalog-item?page=1",
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir, fixture_server(
            {"/rise/api/catalog-item": ("application/ld+json", json.dumps(catalog_payload))}
        ) as base_url:
            output_path = Path(tmpdir) / "rise-candidates.json"

            payload = run_script(
                script_path("fetch-usbr-rise"),
                "discover-items",
                "--base-url",
                f"{base_url}/rise/api",
                "--query",
                "Glen Canyon release",
                "--max-pages",
                "1",
                "--max-records",
                "10",
                "--output",
                str(output_path),
            )
            artifact = load_json(output_path)

            self.assertEqual("fetch-usbr-rise-v1", payload["schema_version"])
            self.assertEqual("usbr-rise-catalog-items", artifact["source"])
            self.assertEqual(["10835"], artifact["candidate_item_ids"])
            self.assertEqual("10835", artifact["records"][0]["item_id"])
            self.assertEqual("Glen Canyon Dam", artifact["records"][0]["location_name"])
            self.assertEqual("catalog-page-scan-client-filter", artifact["discovery_mode"])
            self.assertIn("not source ranking", artifact["list_semantics"])

    def test_fetch_usbr_rise_results_and_normalize_environment_signals(self) -> None:
        result_payload = {
            "@context": "/rise/api/contexts/Result",
            "@id": "/rise/api/result",
            "@type": "Collection",
            "totalItems": 2,
            "member": [
                {
                    "@id": "/rise/api/result/1001",
                    "@type": "Result",
                    "id": 1001,
                    "itemId": 10835,
                    "locationId": 509,
                    "sourceCode": "har",
                    "dateTime": "2023-06-07T08:00:00+00:00",
                    "result": 8470,
                    "status": None,
                    "parameterId": 15,
                    "createDate": "2023-06-08T01:00:00+00:00",
                },
                {
                    "@id": "/rise/api/result/1002",
                    "@type": "Result",
                    "id": 1002,
                    "itemId": 10835,
                    "locationId": 509,
                    "sourceCode": "har",
                    "dateTime": "2023-06-08T08:00:00+00:00",
                    "result": 8425,
                    "status": None,
                    "parameterId": 15,
                    "createDate": "2023-06-09T01:00:00+00:00",
                },
            ],
            "view": {
                "@id": "/rise/api/result?itemId=10835&page=1",
                "@type": "PartialCollectionView",
                "first": "/rise/api/result?itemId=10835&page=1",
                "last": "/rise/api/result?itemId=10835&page=1",
            },
        }
        item_payload = {
            "@context": "/rise/api/contexts/CatalogItem",
            "@id": "/rise/api/catalog-item/10835",
            "@type": ["CatalogItem", "dcat:Dataset"],
            "id": 10835,
            "itemTitle": "Glen Canyon Dam Daily Release Time Series Data",
            "itemDescription": "Upper Colorado Basin Region water operations data.",
            "sourceCode": "HAR",
            "locationSourceCode": "GLN",
            "parameterId": 15,
            "parameterName": "Lake/Reservoir Release - Total",
            "parameterUnit": "cfs",
            "parameterTimestep": "daily",
            "parameterTransformation": "instant",
            "parameterGroup": "Lake/Reservoir Outflow",
            "disclaimer": "Data are provisional and subject to revision unless otherwise noted.",
            "dcat:landingPage": "https://data.usbr.gov/catalog/fixture/item/10835",
            "dcat:spatial": {"type": "Point", "coordinates": [-111.483, 36.936]},
        }

        with tempfile.TemporaryDirectory() as tmpdir, fixture_server(
            {
                "/rise/api/result": ("application/ld+json", json.dumps(result_payload)),
                "/rise/api/catalog-item/10835": ("application/ld+json", json.dumps(item_payload)),
            }
        ) as base_url:
            root = Path(tmpdir)
            run_dir = root / "run"
            output_path = root / "usbr-rise.json"

            fetch_payload = run_script(
                script_path("fetch-usbr-rise"),
                "fetch",
                "--base-url",
                f"{base_url}/rise/api",
                "--item-id",
                "10835",
                "--after-utc",
                "2023-06-01T00:00:00Z",
                "--before-utc",
                "2023-06-30T23:59:59Z",
                "--include-item-metadata",
                "--max-pages",
                "1",
                "--max-records",
                "10",
                "--output",
                str(output_path),
            )
            artifact = load_json(output_path)

            self.assertEqual("fetch-usbr-rise-v1", fetch_payload["schema_version"])
            self.assertEqual("fetch-usbr-rise", artifact["source_skill"])
            self.assertEqual(2, artifact["records_fetched"])
            self.assertEqual("Lake/Reservoir Release - Total", artifact["records"][0]["parameter_name"])
            self.assertEqual("cfs", artifact["records"][0]["parameter_unit"])
            self.assertEqual(36.936, artifact["records"][0]["latitude"])
            self.assertEqual(-111.483, artifact["records"][0]["longitude"])

            normalize_payload = run_script(
                script_path("normalize-usbr-rise-environment-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(output_path),
            )
            rows = normalized_rows(run_dir)

            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(2, normalize_payload["summary"]["signal_count"])
            self.assertEqual({"environment"}, {str(row["plane"]) for row in rows})
            self.assertEqual({"environment-observation-signal"}, {str(row["canonical_object_kind"]) for row in rows})
            self.assertEqual({"usbr-rise-result"}, {str(row["signal_kind"]) for row in rows})
            self.assertEqual("lake_reservoir_release_total", rows[0]["metric"])
            self.assertEqual("cfs", rows[0]["unit"])
            self.assertEqual(8470.0, rows[0]["numeric_value"])
            metadata = json.loads(str(rows[0]["metadata_json"]))
            self.assertEqual("hydrology", metadata["environment_signal_class"])
            self.assertEqual("context-observation", metadata["signal_role"])
            self.assertEqual("Bureau of Reclamation RISE", metadata["provider"])
            self.assertEqual("fetch-usbr-rise", metadata["source_provenance"]["source_skill"])
            self.assertIn("provider-provisional-disclaimer", json.loads(str(rows[0]["quality_flags_json"])))


if __name__ == "__main__":
    unittest.main()

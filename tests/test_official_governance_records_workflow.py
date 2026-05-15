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

RUN_ID = "run-official-governance-001"
ROUND_ID = "round-official-governance-001"


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


def normalized_rows(run_dir: Path, source_skill: str) -> list[dict[str, object]]:
    with sqlite3.connect(analytics_path(run_dir, "signal_plane.sqlite")) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            """
            SELECT source_skill, plane, signal_kind, canonical_object_kind,
                   external_id, title, url, metadata_json, quality_flags_json
            FROM normalized_signals
            WHERE source_skill = ?
            ORDER BY external_id
            """,
            (source_skill,),
        ).fetchall()
    return [dict(row) for row in rows]


class OfficialGovernanceRecordsWorkflowTests(unittest.TestCase):
    def test_epa_eis_fetch_artifact_normalizes_to_formal_signals(self) -> None:
        html = """
        <html>
          <body>
            <span class="pagebanner">1 items found, displaying all items.</span>
            <table id="submissionsTable" class="responsive-table">
              <thead>
                <tr>
                  <th>Title</th><th>CEQ Number</th><th>Document</th>
                  <th>EPA Comment Letter Date</th><th>Federal Register Date</th>
                  <th>Unique Identification Number</th><th>Lead Agency</th>
                  <th>Federal Cooperating Agency(ies)</th><th>State</th>
                  <th>Download Documents</th>
                </tr>
              </thead>
              <tbody>
                <tr class="odd">
                  <td><a href="/cdx-enepa-II/public/action/eis/details?eisId=123456">Colorado River Operations Supplemental EIS</a></td>
                  <td>20230123</td>
                  <td>Draft Supplement</td>
                  <td>06/15/2023</td>
                  <td>04/21/2023</td>
                  <td>DOE/EIS-0001-S1</td>
                  <td>Bureau of Reclamation</td>
                  <td>EPA, USFWS</td>
                  <td>AZ</td>
                  <td><a href="javascript:void(0);" onclick="startDownload('downloadEisDocuments', '123456', '111;222;')">Download EIS</a></td>
                </tr>
              </tbody>
            </table>
          </body>
        </html>
        """
        with tempfile.TemporaryDirectory() as tmpdir, fixture_server(
            {"/eis/search": ("text/html", html)}
        ) as base_url:
            root = Path(tmpdir)
            run_dir = root / "run"
            output_path = root / "epa-eis.json"

            fetch_payload = run_script(
                script_path("fetch-epa-eis-records"),
                "fetch",
                "--search-url",
                f"{base_url}/eis/search",
                "--output",
                str(output_path),
            )
            artifact = load_json(output_path)

            self.assertEqual("official-governance-record-fetch-v1", fetch_payload["schema_version"])
            self.assertEqual("fetch-epa-eis-records", artifact["source_skill"])
            self.assertEqual(1, artifact["records_fetched"])
            record = artifact["records"][0]
            self.assertEqual("20230123", record["ceq_number"])
            self.assertEqual("DOE/EIS-0001-S1", record["unique_identification_number"])
            self.assertEqual(["111", "222"], record["download_document_ids"])
            self.assertEqual("Bureau of Reclamation", record["lead_agency"])

            normalize_payload = run_script(
                script_path("normalize-official-governance-records"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(output_path),
            )
            rows = normalized_rows(run_dir, "fetch-epa-eis-records")

            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, normalize_payload["summary"]["signal_count"])
            self.assertEqual({"formal"}, {str(row["plane"]) for row in rows})
            self.assertEqual({"formal-comment-signal"}, {str(row["canonical_object_kind"]) for row in rows})
            metadata = json.loads(str(rows[0]["metadata_json"]))
            self.assertEqual("EPA EIS Database", metadata["provider"])
            self.assertEqual("20230123", metadata["ceq_number"])
            self.assertEqual("Bureau of Reclamation", metadata["lead_agency"])

    def test_federal_register_fetch_artifact_normalizes_to_formal_signals(self) -> None:
        provider_payload = {
            "count": 1,
            "total_pages": 1,
            "results": [
                {
                    "document_number": "2023-12345",
                    "title": "Colorado River Basin notice",
                    "type": "Notice",
                    "abstract": "Notice of public process for Colorado River operations.",
                    "publication_date": "2023-06-01",
                    "html_url": "https://www.federalregister.gov/documents/2023/06/01/2023-12345/colorado-river-basin-notice",
                    "pdf_url": "https://www.govinfo.gov/example.pdf",
                    "docket_ids": ["BOR-2023-0001"],
                    "agencies": [{"name": "Bureau of Reclamation"}],
                    "comments_close_on": "2023-07-01",
                    "citation": "88 FR 12345",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir, fixture_server(
            {"/api/v1/documents.json": ("application/json", json.dumps(provider_payload))}
        ) as base_url:
            root = Path(tmpdir)
            run_dir = root / "run"
            output_path = root / "federal-register.json"

            fetch_payload = run_script(
                script_path("fetch-federal-register-documents"),
                "fetch",
                "--base-url",
                f"{base_url}/api/v1",
                "--term",
                "Colorado River",
                "--agency",
                "reclamation-bureau",
                "--publication-date-gte",
                "2023-01-01",
                "--publication-date-lte",
                "2023-12-31",
                "--max-pages",
                "1",
                "--max-records",
                "10",
                "--output",
                str(output_path),
            )
            artifact = load_json(output_path)

            self.assertEqual("official-governance-record-fetch-v1", fetch_payload["schema_version"])
            self.assertEqual("fetch-federal-register-documents", artifact["source_skill"])
            self.assertEqual(1, artifact["records_fetched"])
            self.assertEqual("2023-12345", artifact["records"][0]["record_id"])
            self.assertEqual(["Bureau of Reclamation"], artifact["records"][0]["agency_names"])

            normalize_payload = run_script(
                script_path("normalize-official-governance-records"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(output_path),
            )
            rows = normalized_rows(run_dir, "fetch-federal-register-documents")

            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(1, normalize_payload["summary"]["signal_count"])
            self.assertEqual({"formal"}, {str(row["plane"]) for row in rows})
            self.assertEqual({"formal-comment-signal"}, {str(row["canonical_object_kind"]) for row in rows})
            metadata = json.loads(str(rows[0]["metadata_json"]))
            self.assertEqual("BOR-2023-0001", metadata["docket_id"])
            self.assertEqual("FederalRegister.gov", metadata["provider"])
            self.assertEqual("fetch-federal-register-documents", metadata["source_provenance"]["source_skill"])

    def test_usbr_project_page_fetches_link_inventory_and_normalizes(self) -> None:
        html = """
        <html>
          <head>
            <title>Colorado River Interim Guidelines SEIS</title>
            <meta name="description" content="Public involvement materials for the SEIS process.">
          </head>
          <body>
            <a href="/records/final-seis.pdf">Final SEIS PDF</a>
            <a href="/records/public-involvement.html">Public involvement page</a>
            <a href="https://example.com/external.pdf">External mirror</a>
          </body>
        </html>
        """
        with tempfile.TemporaryDirectory() as tmpdir, fixture_server(
            {"/project.html": ("text/html", html)}
        ) as base_url:
            root = Path(tmpdir)
            run_dir = root / "run"
            output_path = root / "usbr-project-records.json"

            fetch_payload = run_script(
                script_path("fetch-usbr-project-records"),
                "fetch",
                "--url",
                f"{base_url}/project.html",
                "--max-linked-records",
                "10",
                "--output",
                str(output_path),
            )
            artifact = load_json(output_path)

            self.assertEqual("official-governance-record-fetch-v1", fetch_payload["schema_version"])
            self.assertEqual("fetch-usbr-project-records", artifact["source_skill"])
            self.assertEqual(3, artifact["records_fetched"])
            self.assertEqual("usbr_project_page", artifact["records"][0]["record_type"])
            self.assertEqual(
                ["Final SEIS PDF", "Public involvement page"],
                [link["text"] for link in artifact["records"][0]["links"]],
            )
            self.assertTrue(any(warning["code"] == "non-usbr-domain" for warning in artifact["warnings"]))

            normalize_payload = run_script(
                script_path("normalize-official-governance-records"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(output_path),
            )
            rows = normalized_rows(run_dir, "fetch-usbr-project-records")

            self.assertEqual("completed", normalize_payload["status"])
            self.assertEqual(3, normalize_payload["summary"]["signal_count"])
            self.assertEqual({"formal"}, {str(row["plane"]) for row in rows})
            self.assertEqual({"official-governance-record"}, {str(row["signal_kind"]) for row in rows})
            linked_flags = [
                json.loads(str(row["quality_flags_json"]))
                for row in rows
                if str(row["external_id"]).endswith(".pdf")
            ][0]
            self.assertIn("linked-record-not-fetched", linked_flags)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

from _workflow_support import analytics_path, run_script, script_path, write_json

RUN_ID = "run-gdelt-tone-enrichment"
ROUND_ID = "round-gdelt-tone-enrichment"


def write_gkg_fixture(root: Path) -> Path:
    zip_path = root / "gkg_fixture.zip"
    row = [
        "20230607120000-123",
        "20230607120000",
        "1",
        "example.com",
        "https://example.com/nyc-smoke",
        "",
        "",
        "ENV_WILDFIRE",
        "ENV_WILDFIRE;TAX_FNCACT_HEALTH",
        "",
        "",
        "",
        "",
        "",
        "",
        "-3.5,1.2,4.7,-5.9,2.1,0.3,90",
        "",
        "wc:90,c1.2:1,c2.3:-0.25,malformed,c3.4:not-number",
        "",
        "",
        "",
        "",
        "",
        "New York;wildfire smoke",
        "",
        "",
        "",
    ]
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("gkg_fixture.tsv", "\t".join(row) + "\n")
    manifest_path = root / "gkg_manifest.json"
    write_json(
        manifest_path,
        {
            "downloads": [
                {
                    "entry": {
                        "timestamp_utc": "2023-06-07T12:00:00Z",
                        "url": "http://data.gdeltproject.org/gdeltv2/gkg_fixture.zip",
                        "size_bytes": zip_path.stat().st_size,
                    },
                    "output_path": str(zip_path),
                    "request_url": "http://data.gdeltproject.org/gdeltv2/gkg_fixture.zip",
                    "validation": {"status": "fixture"},
                }
            ]
        },
    )
    return manifest_path


class GdeltToneEnrichmentTests(unittest.TestCase):
    def test_gkg_normalizer_preserves_v2_tone_parts_as_media_tone(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "run"
            manifest_path = write_gkg_fixture(root)

            payload = run_script(
                script_path("normalize-gdelt-gkg-public-signals"),
                "--run-dir",
                str(run_dir),
                "--run-id",
                RUN_ID,
                "--round-id",
                ROUND_ID,
                "--artifact-path",
                str(manifest_path),
            )

            self.assertEqual("completed", payload["status"])
            self.assertEqual(1, payload["summary"]["signal_count"])

            with sqlite3.connect(analytics_path(run_dir, "signal_plane.sqlite")) as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    """
                    SELECT metric, numeric_value, metadata_json
                    FROM normalized_signals
                    WHERE run_id = ? AND round_id = ? AND source_skill = 'fetch-gdelt-gkg'
                    """,
                    (RUN_ID, ROUND_ID),
                ).fetchone()

            self.assertIsNotNone(row)
            assert row is not None
            metadata = json.loads(row["metadata_json"])
            self.assertEqual("v2_tone", row["metric"])
            self.assertEqual(-3.5, row["numeric_value"])
            self.assertEqual("gdelt_media_tone", metadata["gdelt_tone_kind"])
            self.assertEqual(
                "media_or_document_tone_not_public_response_sentiment",
                metadata["tone_semantics"],
            )
            self.assertEqual("-3.5,1.2,4.7,-5.9,2.1,0.3,90", metadata["v2_tone_raw"])
            self.assertEqual(
                {
                    "activity_reference_density": 2.1,
                    "field_count": 7,
                    "negative_score": 4.7,
                    "polarity": -5.9,
                    "positive_score": 1.2,
                    "raw": "-3.5,1.2,4.7,-5.9,2.1,0.3,90",
                    "self_group_reference_density": 0.3,
                    "tone": -3.5,
                    "word_count": 90,
                },
                metadata["v2_tone_parts"],
            )
            self.assertEqual(
                "provider_emotion_psychology_cues_for_audit_not_public_response_sentiment",
                metadata["gcam_semantics"],
            )
            self.assertEqual(
                "wc:90,c1.2:1,c2.3:-0.25,malformed,c3.4:not-number",
                metadata["gcam_raw"],
            )
            self.assertEqual(
                {
                    "dimension_count": 2,
                    "dimensions": [
                        {"dimension": "c1.2", "value": 1.0},
                        {"dimension": "c2.3", "value": -0.25},
                    ],
                    "entry_count": 5,
                    "raw": "wc:90,c1.2:1,c2.3:-0.25,malformed,c3.4:not-number",
                    "unparsed_count": 2,
                    "unparsed_entries": ["malformed", "c3.4:not-number"],
                    "word_count": 90,
                },
                metadata["gcam_cues"],
            )


if __name__ == "__main__":
    unittest.main()

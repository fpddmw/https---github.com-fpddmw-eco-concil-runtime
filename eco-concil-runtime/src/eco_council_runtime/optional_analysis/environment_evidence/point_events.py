from __future__ import annotations

from collections import Counter
from typing import Any

from ..support import maybe_text
from .common import date_bucket, is_point_event_signal, metadata_float, metadata_text, signal_timestamp


class MetadataNumericStats:
    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        self.numeric_count = 0
        self.missing_count = 0
        self._sum = 0.0
        self.min_value: float | None = None
        self.max_value: float | None = None

    def add(self, value: float | None) -> None:
        if value is None:
            self.missing_count += 1
            return
        self.numeric_count += 1
        self._sum += value
        self.min_value = value if self.min_value is None else min(self.min_value, value)
        self.max_value = value if self.max_value is None else max(self.max_value, value)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "field": self.field_name,
            "numeric_count": self.numeric_count,
            "missing_count": self.missing_count,
        }
        if self.numeric_count:
            payload.update(
                {
                    "min": self.min_value,
                    "max": self.max_value,
                    "mean": round(self._sum / self.numeric_count, 6),
                }
            )
        return payload


class PointEventAccumulator:
    def __init__(self, *, group_limit: int) -> None:
        self.group_limit = group_limit
        self.point_event_count = 0
        self.date_counts: Counter[str] = Counter()
        self.satellite_counts: Counter[str] = Counter()
        self.instrument_counts: Counter[str] = Counter()
        self.provider_counts: Counter[str] = Counter()
        self.metadata_numeric: dict[str, MetadataNumericStats] = {
            "frp": MetadataNumericStats("frp"),
            "confidence": MetadataNumericStats("confidence"),
            "brightness": MetadataNumericStats("brightness"),
            "bright_ti4": MetadataNumericStats("bright_ti4"),
            "bright_ti5": MetadataNumericStats("bright_ti5"),
        }
        self.min_latitude: float | None = None
        self.max_latitude: float | None = None
        self.min_longitude: float | None = None
        self.max_longitude: float | None = None
        self.missing_coordinates = 0

    def add(self, signal: dict[str, Any]) -> None:
        if not is_point_event_signal(signal):
            return
        self.point_event_count += 1
        timestamp = signal_timestamp(signal)
        self.date_counts[date_bucket(timestamp) or "missing-timestamp"] += 1
        self._add_coordinates(signal)
        self._add_distribution(self.satellite_counts, metadata_text(signal, "satellite"))
        self._add_distribution(self.instrument_counts, metadata_text(signal, "instrument"))
        provider = metadata_text(signal, "provider", "provider_name") or maybe_text(signal.get("channel_name"))
        self._add_distribution(self.provider_counts, provider)
        self.metadata_numeric["frp"].add(metadata_float(signal, "frp"))
        self.metadata_numeric["confidence"].add(metadata_float(signal, "confidence"))
        brightness = (
            metadata_float(signal, "brightness")
            or metadata_float(signal, "bright_ti4")
            or metadata_float(signal, "bright_ti5")
        )
        self.metadata_numeric["brightness"].add(brightness)
        self.metadata_numeric["bright_ti4"].add(metadata_float(signal, "bright_ti4"))
        self.metadata_numeric["bright_ti5"].add(metadata_float(signal, "bright_ti5"))

    def _add_coordinates(self, signal: dict[str, Any]) -> None:
        latitude = signal.get("latitude")
        longitude = signal.get("longitude")
        if not isinstance(latitude, (int, float)) or not isinstance(longitude, (int, float)):
            self.missing_coordinates += 1
            return
        self.min_latitude = float(latitude) if self.min_latitude is None else min(self.min_latitude, float(latitude))
        self.max_latitude = float(latitude) if self.max_latitude is None else max(self.max_latitude, float(latitude))
        self.min_longitude = float(longitude) if self.min_longitude is None else min(self.min_longitude, float(longitude))
        self.max_longitude = float(longitude) if self.max_longitude is None else max(self.max_longitude, float(longitude))

    @staticmethod
    def _add_distribution(counter: Counter[str], value: str) -> None:
        text = maybe_text(value)
        if text:
            counter[text] += 1

    def has_output(self) -> bool:
        return self.point_event_count > 0

    def _counter_rows(self, counts: Counter[str], key_name: str) -> list[dict[str, Any]]:
        return [
            {key_name: key, "signal_count": count}
            for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        ][: self.group_limit]

    def spatial_envelope(self) -> dict[str, Any]:
        bbox = None
        with_coordinates = self.point_event_count - self.missing_coordinates
        if with_coordinates:
            bbox = {
                "west": self.min_longitude,
                "south": self.min_latitude,
                "east": self.max_longitude,
                "north": self.max_latitude,
            }
        return {
            "with_coordinates": with_coordinates,
            "missing_coordinates": self.missing_coordinates,
            "bbox": bbox,
        }

    def to_payload(self) -> dict[str, Any]:
        return {
            "point_event_signal_count": self.point_event_count,
            "date_buckets": [
                {"date": key, "signal_count": count}
                for key, count in sorted(self.date_counts.items())
            ][: self.group_limit],
            "spatial_envelope": self.spatial_envelope(),
            "metadata_numeric_summary": [
                stats.to_payload()
                for stats in self.metadata_numeric.values()
                if stats.numeric_count or stats.missing_count
            ],
            "satellite_distribution": self._counter_rows(self.satellite_counts, "satellite"),
            "instrument_distribution": self._counter_rows(self.instrument_counts, "instrument"),
            "provider_distribution": self._counter_rows(self.provider_counts, "provider"),
            "semantics": "descriptive point-event record counts and metadata statistics only; no severity, exposure, sufficiency, transport, or attribution judgement",
        }

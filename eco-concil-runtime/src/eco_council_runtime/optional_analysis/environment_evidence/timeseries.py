from __future__ import annotations

from collections import Counter
from typing import Any

from ..support import maybe_text
from .common import date_bucket, numeric_value, signal_location_payload, signal_timestamp


class SeriesStats:
    def __init__(
        self,
        *,
        source_skill: str,
        metric: str,
        unit: str,
        location: dict[str, Any],
    ) -> None:
        self.source_skill = source_skill
        self.metric = metric
        self.unit = unit
        self.location = location
        self.count = 0
        self.numeric_count = 0
        self.missing_numeric_count = 0
        self._sum = 0.0
        self.min_value: float | None = None
        self.max_value: float | None = None
        self.first_observed_at = ""
        self.last_observed_at = ""
        self.date_counts: Counter[str] = Counter()

    def add(self, signal: dict[str, Any]) -> None:
        self.count += 1
        timestamp = signal_timestamp(signal)
        if timestamp:
            if not self.first_observed_at or timestamp < self.first_observed_at:
                self.first_observed_at = timestamp
            if not self.last_observed_at or timestamp > self.last_observed_at:
                self.last_observed_at = timestamp
            self.date_counts[date_bucket(timestamp) or "undated"] += 1
        else:
            self.date_counts["missing-timestamp"] += 1
        value = numeric_value(signal)
        if value is None:
            self.missing_numeric_count += 1
            return
        self.numeric_count += 1
        self._sum += value
        self.min_value = value if self.min_value is None else min(self.min_value, value)
        self.max_value = value if self.max_value is None else max(self.max_value, value)

    def to_payload(self, *, bucket_limit: int) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source_skill": self.source_skill,
            "location": self.location,
            "metric": self.metric,
            "unit": self.unit,
            "count": self.count,
            "numeric_count": self.numeric_count,
            "missing_numeric_count": self.missing_numeric_count,
            "first_observed_at": self.first_observed_at,
            "last_observed_at": self.last_observed_at,
            "date_buckets": [
                {"date": key, "count": count}
                for key, count in sorted(self.date_counts.items())
            ][:bucket_limit],
        }
        if self.numeric_count:
            payload.update(
                {
                    "min": self.min_value,
                    "max": self.max_value,
                    "mean": round(self._sum / self.numeric_count, 6),
                    "descriptive_extrema": {
                        "min_value": self.min_value,
                        "max_value": self.max_value,
                        "semantics": "descriptive extrema only",
                    },
                }
            )
        return payload


class TimeSeriesAccumulator:
    def __init__(self, *, group_limit: int) -> None:
        self.group_limit = group_limit
        self.groups: dict[tuple[str, str, str, str], SeriesStats] = {}
        self.candidate_count = 0

    def add(self, signal: dict[str, Any]) -> None:
        if not signal_timestamp(signal) or numeric_value(signal) is None:
            return
        self.candidate_count += 1
        source_skill = maybe_text(signal.get("source_skill")) or "unspecified"
        metric = maybe_text(signal.get("metric")) or "unspecified"
        unit = maybe_text(signal.get("unit"))
        location = signal_location_payload(signal)
        location_key = maybe_text(location.get("location_key")) or "unspecified-location"
        key = (source_skill, location_key, metric, unit)
        group = self.groups.get(key)
        if group is None:
            group = SeriesStats(
                source_skill=source_skill,
                metric=metric,
                unit=unit,
                location=location,
            )
            self.groups[key] = group
        group.add(signal)

    def has_output(self) -> bool:
        return bool(self.groups)

    def to_payload(self) -> dict[str, Any]:
        groups = sorted(
            self.groups.values(),
            key=lambda item: (-item.count, item.source_skill, item.metric, maybe_text(item.location.get("location_key"))),
        )
        return {
            "candidate_signal_count": self.candidate_count,
            "group_count": len(groups),
            "groups": [
                group.to_payload(bucket_limit=self.group_limit)
                for group in groups[: self.group_limit]
            ],
            "semantics": "descriptive time-series statistics only; no exposure, severity, sufficiency, transport, or attribution judgement",
        }

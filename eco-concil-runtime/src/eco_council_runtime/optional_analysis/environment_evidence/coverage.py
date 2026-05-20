from __future__ import annotations

from collections import Counter
from typing import Any

from ..support import list_items, maybe_text, unique_values
from .common import date_bucket, limited_unique_texts, numeric_value, signal_timestamp, sorted_counter_rows


class MetricStats:
    def __init__(self, metric: str, unit: str) -> None:
        self.metric = metric or "unspecified"
        self.unit = unit
        self.signal_count = 0
        self.numeric_count = 0
        self.missing_numeric_count = 0
        self._sum = 0.0
        self.min_value: float | None = None
        self.max_value: float | None = None

    def add(self, value: float | None) -> None:
        self.signal_count += 1
        if value is None:
            self.missing_numeric_count += 1
            return
        self.numeric_count += 1
        self._sum += value
        self.min_value = value if self.min_value is None else min(self.min_value, value)
        self.max_value = value if self.max_value is None else max(self.max_value, value)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "metric": self.metric,
            "unit": self.unit,
            "signal_count": self.signal_count,
            "numeric_count": self.numeric_count,
            "missing_numeric_count": self.missing_numeric_count,
        }
        if self.numeric_count:
            payload.update(
                {
                    "min_value": self.min_value,
                    "max_value": self.max_value,
                    "mean_value": round(self._sum / self.numeric_count, 6),
                }
            )
        return payload


class CoverageAccumulator:
    def __init__(self, *, group_limit: int, sample_ref_limit: int, sample_limit: int) -> None:
        self.group_limit = group_limit
        self.sample_ref_limit = sample_ref_limit
        self.sample_limit = sample_limit
        self.signal_count = 0
        self.numeric_signal_count = 0
        self.source_counts: Counter[str] = Counter()
        self.metric_stats: dict[str, MetricStats] = {}
        self.date_counts: Counter[str] = Counter()
        self.round_counts: Counter[str] = Counter()
        self.quality_flag_counts: Counter[str] = Counter()
        self.coverage_limitation_counts: Counter[str] = Counter()
        self.timestamp_missing_count = 0
        self.coordinate_missing_count = 0
        self.with_coordinates = 0
        self.first_observed_at = ""
        self.last_observed_at = ""
        self.min_latitude: float | None = None
        self.max_latitude: float | None = None
        self.min_longitude: float | None = None
        self.max_longitude: float | None = None
        self.source_signal_ref_samples: list[dict[str, Any]] = []
        self.evidence_ref_samples: list[Any] = []
        self.source_signal_ids: list[str] = []

    def add(self, signal: dict[str, Any]) -> None:
        self.signal_count += 1
        source_skill = maybe_text(signal.get("source_skill")) or "unspecified"
        metric = maybe_text(signal.get("metric")) or "unspecified"
        unit = maybe_text(signal.get("unit"))
        self.source_counts[source_skill] += 1
        self.round_counts[maybe_text(signal.get("round_id")) or "unspecified"] += 1
        timestamp = signal_timestamp(signal)
        if timestamp:
            if not self.first_observed_at or timestamp < self.first_observed_at:
                self.first_observed_at = timestamp
            if not self.last_observed_at or timestamp > self.last_observed_at:
                self.last_observed_at = timestamp
            self.date_counts[date_bucket(timestamp) or "undated"] += 1
        else:
            self.timestamp_missing_count += 1
            self.date_counts["missing-timestamp"] += 1

        value = numeric_value(signal)
        if value is not None:
            self.numeric_signal_count += 1
        metric_key = f"{metric}\x1f{unit}"
        stats = self.metric_stats.setdefault(metric_key, MetricStats(metric, unit))
        stats.add(value)

        latitude = signal.get("latitude")
        longitude = signal.get("longitude")
        if isinstance(latitude, (int, float)) and isinstance(longitude, (int, float)):
            self.with_coordinates += 1
            self.min_latitude = float(latitude) if self.min_latitude is None else min(self.min_latitude, float(latitude))
            self.max_latitude = float(latitude) if self.max_latitude is None else max(self.max_latitude, float(latitude))
            self.min_longitude = float(longitude) if self.min_longitude is None else min(self.min_longitude, float(longitude))
            self.max_longitude = float(longitude) if self.max_longitude is None else max(self.max_longitude, float(longitude))
        else:
            self.coordinate_missing_count += 1

        for flag in list_items(signal.get("quality_flags")):
            text = maybe_text(flag)
            if text:
                self.quality_flag_counts[text] += 1
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        limitations = metadata.get("coverage_limitations") if isinstance(metadata, dict) else []
        if isinstance(limitations, str):
            limitations = [limitations]
        if isinstance(limitations, list):
            for limitation in limitations:
                text = maybe_text(limitation)
                if text:
                    self.coverage_limitation_counts[text] += 1

        self._append_samples(signal, timestamp=timestamp, source_skill=source_skill, metric=metric)

    def _append_samples(self, signal: dict[str, Any], *, timestamp: str, source_skill: str, metric: str) -> None:
        signal_id = maybe_text(signal.get("signal_id"))
        if signal_id and len(self.source_signal_ids) < self.sample_limit:
            self.source_signal_ids.append(signal_id)
        refs = list_items(signal.get("evidence_refs"))
        if refs and len(self.evidence_ref_samples) < self.sample_ref_limit:
            for ref in refs:
                if len(self.evidence_ref_samples) >= self.sample_ref_limit:
                    break
                self.evidence_ref_samples = unique_values([*self.evidence_ref_samples, ref])
        if len(self.source_signal_ref_samples) >= self.sample_ref_limit:
            return
        ref = refs[0] if refs else {}
        self.source_signal_ref_samples.append(
            {
                "signal_id": signal_id,
                "source_skill": source_skill,
                "metric": metric,
                "observed_at_utc": timestamp,
                "evidence_ref": ref,
            }
        )

    def source_distribution(self) -> list[dict[str, Any]]:
        return sorted_counter_rows(self.source_counts, key_name="source_skill", limit=self.group_limit)

    def metric_distribution(self) -> list[dict[str, Any]]:
        rows = [stats.to_payload() for _, stats in sorted(self.metric_stats.items())]
        return sorted(rows, key=lambda item: (-int(item["signal_count"]), item["metric"], item["unit"]))[
            : self.group_limit
        ]

    def date_buckets(self) -> list[dict[str, Any]]:
        return [
            {"date": key, "signal_count": count}
            for key, count in sorted(self.date_counts.items())
        ][: self.group_limit]

    def spatial_coverage(self) -> dict[str, Any]:
        bbox = None
        if self.with_coordinates:
            bbox = {
                "west": self.min_longitude,
                "south": self.min_latitude,
                "east": self.max_longitude,
                "north": self.max_latitude,
            }
        return {
            "with_coordinates": self.with_coordinates,
            "missing_coordinates": self.coordinate_missing_count,
            "bbox": bbox,
        }

    def time_coverage(self) -> dict[str, Any]:
        return {
            "first_observed_at": self.first_observed_at,
            "last_observed_at": self.last_observed_at,
            "missing_timestamp_count": self.timestamp_missing_count,
            "date_buckets": self.date_buckets(),
        }

    def quality_or_metadata_limitations(self) -> list[str]:
        limitations: list[Any] = [
            "No environment signals found." if not self.signal_count else "",
            f"{self.coordinate_missing_count} environment signals lack coordinates." if self.coordinate_missing_count else "",
            f"{self.timestamp_missing_count} environment signals lack usable timestamps." if self.timestamp_missing_count else "",
        ]
        limitations.extend(
            key
            for key, _ in sorted(
                self.coverage_limitation_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
        )
        return limited_unique_texts(limitations, limit=self.group_limit)

    def statistics_summary(self) -> dict[str, Any]:
        return {
            "signal_count": self.signal_count,
            "numeric_signal_count": self.numeric_signal_count,
            "source_family_count": len([key for key in self.source_counts if key and key != "unspecified"]),
            "metric_count": len([key for key in self.metric_stats if key and key != "unspecified"]),
        }

    def coverage_summary(self) -> dict[str, Any]:
        return {
            "signal_count": self.signal_count,
            "source_distribution": self.source_distribution(),
            "metric_distribution": self.metric_distribution(),
            "time_coverage": self.time_coverage(),
            "spatial_coverage": self.spatial_coverage(),
            "quality_flags": sorted_counter_rows(self.quality_flag_counts, key_name="quality_flag", limit=self.group_limit),
            "quality_or_metadata_limitations": self.quality_or_metadata_limitations(),
            "evidence_ref_samples": self.evidence_ref_samples,
            "source_signal_ref_samples": self.source_signal_ref_samples,
        }


def sample_status(*, signal_count: int, sample_limit: int) -> str:
    if signal_count == 0:
        return "no-matched-signals"
    if sample_limit <= 0:
        return "full-statistics-without-output-row-samples"
    if signal_count > sample_limit:
        return "full-statistics-with-limited-output-samples"
    return "full-statistics-with-complete-output-samples"

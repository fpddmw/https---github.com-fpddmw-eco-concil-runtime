"""Shared domain semantics for normalize-time claim and observation reasoning."""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timezone
from typing import Any

from eco_council_runtime.domain.text import maybe_text, normalize_space, truncate_text, unique_strings

POINT_MATCH_EPSILON_DEGREES = 0.05

PHYSICAL_CLAIM_TYPES = {
    "wildfire",
    "smoke",
    "flood",
    "heat",
    "drought",
    "air-pollution",
    "water-pollution",
}
NON_CLAIM_PUBLIC_SIGNAL_KINDS = {
    "artifact-manifest",
    "table-coverage",
    "timeline-bin",
}
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "were",
    "with",
}
GENERIC_REGION_TOKENS = {
    "area",
    "city",
    "country",
    "county",
    "district",
    "metro",
    "province",
    "region",
    "state",
    "town",
    "urban",
    "usa",
    "us",
}
PHYSICAL_LEG_ORDER = ("source", "mechanism", "impact")
CLAIM_KEYWORDS = {
    "wildfire": ("wildfire", "fire", "burning", "burn", "forest fire", "bushfire"),
    "smoke": ("smoke", "haze", "smog", "ash"),
    "flood": ("flood", "flooding", "overflow", "inundation"),
    "heat": ("heat", "heatwave", "hot weather", "extreme heat"),
    "drought": ("drought", "dry spell", "water shortage", "dryness"),
    "air-pollution": ("air quality", "pm2.5", "pm10", "pollution", "dirty air", "aqi"),
    "water-pollution": ("water pollution", "contaminated water", "sewage", "toxic spill"),
    "policy-reaction": ("policy", "regulation", "rulemaking", "public comment", "epa", "agency"),
}
CLAIM_METRIC_RULES = {
    "smoke": {
        "support": {
            "pm2_5": 35.0,
            "pm10": 50.0,
            "us_aqi": 100.0,
            "fire_detection_count": 1.0,
        },
        "contradict": {
            "pm2_5": 12.0,
            "pm10": 20.0,
            "us_aqi": 50.0,
        },
    },
    "air-pollution": {
        "support": {
            "pm2_5": 35.0,
            "pm10": 50.0,
            "us_aqi": 100.0,
            "nitrogen_dioxide": 40.0,
            "ozone": 100.0,
        },
        "contradict": {
            "pm2_5": 12.0,
            "pm10": 20.0,
            "us_aqi": 50.0,
        },
    },
    "wildfire": {
        "support": {
            "fire_detection_count": 1.0,
            "temperature_2m": 30.0,
            "wind_speed_10m": 5.0,
        },
        "contradict": {
            "fire_detection_count": 0.0,
            "precipitation_sum": 20.0,
            "relative_humidity_2m": 70.0,
        },
    },
    "flood": {
        "support": {
            "precipitation_sum": 20.0,
            "precipitation": 10.0,
            "river_discharge": 100.0,
            "river_discharge_mean": 100.0,
            "river_discharge_max": 120.0,
            "river_discharge_p75": 100.0,
        },
        "contradict": {
            "precipitation_sum": 1.0,
            "river_discharge": 20.0,
            "river_discharge_mean": 20.0,
            "river_discharge_max": 25.0,
            "river_discharge_p75": 20.0,
        },
    },
    "heat": {
        "support": {
            "temperature_2m": 32.0,
        },
        "contradict": {
            "temperature_2m": 22.0,
        },
    },
    "drought": {
        "support": {
            "precipitation_sum": 2.0,
            "soil_moisture_0_to_7cm": 0.12,
        },
        "contradict": {
            "precipitation_sum": 10.0,
            "soil_moisture_0_to_7cm": 0.25,
        },
    },
}
METEOROLOGY_METRICS = {"temperature_2m", "wind_speed_10m", "relative_humidity_2m", "precipitation_sum", "precipitation"}
PRECIPITATION_METRICS = {
    "precipitation",
    "precipitation_sum",
    "soil_moisture_0_to_7cm",
}
HYDROLOGY_METRICS = {
    "river_discharge",
    "river_discharge_mean",
    "river_discharge_max",
    "river_discharge_min",
    "river_discharge_p25",
    "river_discharge_p75",
    "gage_height",
}
METRIC_FAMILY_GROUPS = {
    "air-quality": {
        "pm2_5",
        "pm2_5_aqi",
        "pm10",
        "pm10_aqi",
        "us_aqi",
        "nitrogen_dioxide",
        "nitrogen_dioxide_aqi",
        "ozone",
        "ozone_aqi",
        "sulfur_dioxide",
        "sulfur_dioxide_aqi",
        "carbon_monoxide",
        "carbon_monoxide_aqi",
    },
    "fire-detection": {
        "fire_detection",
        "fire_detection_count",
    },
    "meteorology": {
        "temperature_2m",
        "wind_speed_10m",
        "relative_humidity_2m",
        "precipitation",
        "precipitation_sum",
    },
    "hydrology": {
        "river_discharge",
        "river_discharge_mean",
        "river_discharge_max",
        "river_discharge_min",
        "river_discharge_p25",
        "river_discharge_p75",
        "gage_height",
    },
    "soil": {
        "soil_moisture_0_to_7cm",
    },
}
DEFAULT_OBSERVATION_FAMILY_ORDER = ("air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other")
ENVIRONMENT_METRIC_ALIASES = {
    "pm25": "pm2_5",
    "pm2.5": "pm2_5",
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "o3": "ozone",
    "ozone": "ozone",
    "no2": "nitrogen_dioxide",
    "nitrogen_dioxide": "nitrogen_dioxide",
    "so2": "sulphur_dioxide",
    "sulphur_dioxide": "sulphur_dioxide",
    "co": "carbon_monoxide",
    "carbon_monoxide": "carbon_monoxide",
    "us_aqi": "us_aqi",
    "gage_height": "gage_height",
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def maybe_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def canonical_environment_metric(value: Any) -> str:
    text = maybe_text(value)
    if not text:
        return ""
    lowered = text.casefold()
    if lowered.endswith("_aqi"):
        base_metric = ENVIRONMENT_METRIC_ALIASES.get(lowered[:-4], lowered[:-4])
        return f"{base_metric}_aqi"
    return ENVIRONMENT_METRIC_ALIASES.get(lowered, text)


def parse_loose_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    text = normalize_space(str(value))
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for pattern in ("%Y%m%d%H%M%S", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, pattern)
            parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    return None


def to_rfc3339_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def text_tokens(value: Any, *, minimum_length: int = 4) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", maybe_text(value).casefold())
    output: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if len(token) < minimum_length or token in STOPWORDS or token in seen:
            continue
        seen.add(token)
        output.append(token)
    return output


def geometry_to_bbox(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    kind = maybe_text(geometry.get("type"))
    if kind == "Point":
        lat = maybe_number(geometry.get("latitude"))
        lon = maybe_number(geometry.get("longitude"))
        if lat is None or lon is None:
            return None
        return (lon, lat, lon, lat)
    if kind == "BBox":
        west = maybe_number(geometry.get("west"))
        south = maybe_number(geometry.get("south"))
        east = maybe_number(geometry.get("east"))
        north = maybe_number(geometry.get("north"))
        if None in {west, south, east, north}:
            return None
        return (float(west), float(south), float(east), float(north))
    return None


def geometry_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_type = maybe_text(left.get("type"))
    right_type = maybe_text(right.get("type"))
    if left_type == "Point" and right_type == "Point":
        left_lat = maybe_number(left.get("latitude"))
        left_lon = maybe_number(left.get("longitude"))
        right_lat = maybe_number(right.get("latitude"))
        right_lon = maybe_number(right.get("longitude"))
        if None in {left_lat, left_lon, right_lat, right_lon}:
            return False
        assert left_lat is not None
        assert left_lon is not None
        assert right_lat is not None
        assert right_lon is not None
        return (
            abs(left_lat - right_lat) <= POINT_MATCH_EPSILON_DEGREES
            and abs(left_lon - right_lon) <= POINT_MATCH_EPSILON_DEGREES
        )
    if left_type == "Point" and right_type == "BBox":
        left_lat = maybe_number(left.get("latitude"))
        left_lon = maybe_number(left.get("longitude"))
        bbox = geometry_to_bbox(right)
        if None in {left_lat, left_lon} or bbox is None:
            return False
        assert left_lat is not None
        assert left_lon is not None
        west, south, east, north = bbox
        return west <= left_lon <= east and south <= left_lat <= north
    if left_type == "BBox" and right_type == "Point":
        return geometry_overlap(right, left)
    left_bbox = geometry_to_bbox(left)
    right_bbox = geometry_to_bbox(right)
    if left_bbox is None or right_bbox is None:
        return False
    left_west, left_south, left_east, left_north = left_bbox
    right_west, right_south, right_east, right_north = right_bbox
    return not (
        left_east < right_west
        or right_east < left_west
        or left_north < right_south
        or right_north < left_south
    )


def point_matches_geometry(latitude: float | None, longitude: float | None, geometry: dict[str, Any]) -> bool:
    if latitude is None or longitude is None:
        return False
    return geometry_overlap(
        {"type": "Point", "latitude": latitude, "longitude": longitude},
        geometry,
    )


def region_core_tokens(label: Any) -> list[str]:
    tokens = text_tokens(label, minimum_length=3)
    core = [token for token in tokens if token not in GENERIC_REGION_TOKENS]
    return core or tokens[:3]


def row_token_set(*parts: Any, minimum_length: int = 3) -> set[str]:
    tokens: set[str] = set()
    for part in parts:
        tokens.update(text_tokens(part, minimum_length=minimum_length))
    return tokens


def time_windows_overlap(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_start = parse_loose_datetime(left.get("start_utc"))
    left_end = parse_loose_datetime(left.get("end_utc"))
    right_start = parse_loose_datetime(right.get("start_utc"))
    right_end = parse_loose_datetime(right.get("end_utc"))
    if None in {left_start, left_end, right_start, right_end}:
        return False
    assert left_start is not None
    assert left_end is not None
    assert right_start is not None
    assert right_end is not None
    return max(left_start, right_start) <= min(left_end, right_end)


def public_signal_channel(source_skill: str) -> str:
    source = maybe_text(source_skill)
    if source.startswith("gdelt"):
        return "news"
    if source.startswith("youtube"):
        return "video"
    if source.startswith("bluesky"):
        return "social"
    if source.startswith("federal-register") or source.startswith("regulationsgov"):
        return "rulemaking"
    return source or "unknown"


def observation_metric_family(metric: Any) -> str:
    canonical = canonical_environment_metric(metric)
    for family, metrics in METRIC_FAMILY_GROUPS.items():
        if canonical in metrics:
            return family
    return "other"


def claim_priority_metric_families(claims: list[dict[str, Any]]) -> list[str]:
    claim_types = {
        maybe_text(item.get("claim_type"))
        for item in claims
        if isinstance(item, dict) and bool(item.get("needs_physical_validation"))
    }
    ordered: list[str] = []
    for claim_type in sorted(claim_types):
        if claim_type in {"smoke", "air-pollution"}:
            ordered.extend(["air-quality", "fire-detection", "meteorology"])
        elif claim_type == "wildfire":
            ordered.extend(["fire-detection", "meteorology", "air-quality"])
        elif claim_type == "flood":
            ordered.extend(["hydrology", "meteorology"])
        elif claim_type == "heat":
            ordered.extend(["meteorology"])
        elif claim_type == "drought":
            ordered.extend(["soil", "meteorology"])
    ordered.extend(DEFAULT_OBSERVATION_FAMILY_ORDER)
    return unique_strings(ordered)


def semantic_fingerprint(text: str) -> str:
    cleaned = []
    token = []
    for char in text.lower():
        if char.isalnum():
            token.append(char)
            continue
        if token:
            cleaned.append("".join(token))
            token = []
    if token:
        cleaned.append("".join(token))
    filtered = [item for item in cleaned if item and item not in STOPWORDS]
    return "-".join(filtered[:12])


def claim_type_from_text(text: str) -> str:
    lowered = text.lower()
    for claim_type, keywords in CLAIM_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return claim_type
    return "other"


def candidate_statement(title: str, text: str) -> str:
    if text:
        return truncate_text(text, 420)
    return truncate_text(title, 420)


def best_public_claim_hypothesis_id(plan: dict[str, Any], claim_type: str, statement: str) -> str:
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    if not hypotheses:
        return ""
    if len(hypotheses) == 1:
        return maybe_text(hypotheses[0].get("hypothesis_id"))
    claim_tokens = row_token_set(statement, claim_type, minimum_length=4)
    best_hypothesis_id = ""
    best_score = -1
    for hypothesis in hypotheses:
        if not isinstance(hypothesis, dict):
            continue
        hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
        if not hypothesis_id:
            continue
        hypothesis_tokens = row_token_set(
            hypothesis.get("statement"),
            hypothesis.get("summary"),
            minimum_length=4,
        )
        score = len(claim_tokens & hypothesis_tokens)
        for leg in hypothesis.get("chain_legs", []):
            if not isinstance(leg, dict):
                continue
            if maybe_text(leg.get("leg_id")) != "public_interpretation":
                continue
            claim_types = {
                maybe_text(item)
                for item in (leg.get("claim_types") if isinstance(leg.get("claim_types"), list) else [])
                if maybe_text(item)
            }
            if claim_type in claim_types:
                score += 1
        if score > best_score:
            best_score = score
            best_hypothesis_id = hypothesis_id
    return best_hypothesis_id if best_score > 0 else ""


def public_signal_location_candidates(signal: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    raw_json = signal.get("raw_json") if isinstance(signal.get("raw_json"), dict) else {}

    def append_candidate(label: Any, latitude: Any, longitude: Any) -> None:
        name = maybe_text(label)
        lat = maybe_number(latitude)
        lon = maybe_number(longitude)
        if not name and lat is None and lon is None:
            return
        candidates.append(
            {
                "label": name,
                "latitude": lat,
                "longitude": lon,
            }
        )

    append_candidate(signal.get("location_name"), signal.get("latitude"), signal.get("longitude"))
    append_candidate(raw_json.get("action_geo_name"), raw_json.get("action_geo_lat"), raw_json.get("action_geo_lon"))
    append_candidate(metadata.get("action_geo_name"), metadata.get("action_geo_lat"), metadata.get("action_geo_lon"))

    for collection in (raw_json.get("locations"), metadata.get("locations")):
        if not isinstance(collection, list):
            continue
        for item in collection:
            if isinstance(item, dict):
                append_candidate(
                    item.get("name") or item.get("label") or item.get("location"),
                    item.get("latitude") or item.get("lat"),
                    item.get("longitude") or item.get("lon") or item.get("lng"),
                )
            else:
                append_candidate(item, None, None)

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = _stable_json(
            {
                "label": maybe_text(candidate.get("label")).casefold(),
                "latitude": candidate.get("latitude"),
                "longitude": candidate.get("longitude"),
            }
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def public_signal_mentions_mission_region(signals: list[dict[str, Any]], mission_scope: dict[str, Any]) -> bool:
    required_tokens = region_core_tokens(mission_scope.get("label"))[:2]
    if not required_tokens:
        return False
    token_set: set[str] = set()
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
        raw_json = signal.get("raw_json") if isinstance(signal.get("raw_json"), dict) else {}
        token_set.update(
            row_token_set(
                signal.get("title"),
                signal.get("text"),
                signal.get("query_text"),
                signal.get("channel_name"),
                metadata.get("locations"),
                raw_json.get("locations"),
                raw_json.get("action_geo_name"),
                minimum_length=3,
            )
        )
    return all(token in token_set for token in required_tokens)


def bbox_scope_from_location_candidates(
    location_candidates: list[dict[str, Any]],
    *,
    label: str,
) -> dict[str, Any] | None:
    points = [
        (float(candidate["latitude"]), float(candidate["longitude"]))
        for candidate in location_candidates
        if maybe_number(candidate.get("latitude")) is not None and maybe_number(candidate.get("longitude")) is not None
    ]
    if not points:
        return None
    latitudes = [point[0] for point in points]
    longitudes = [point[1] for point in points]
    if len(points) == 1:
        latitude, longitude = points[0]
        return {
            "label": label,
            "geometry": {"type": "Point", "latitude": latitude, "longitude": longitude},
        }
    return {
        "label": label,
        "geometry": {
            "type": "BBox",
            "west": min(longitudes),
            "south": min(latitudes),
            "east": max(longitudes),
            "north": max(latitudes),
        },
    }


def derive_public_claim_place_scope(
    signals: list[dict[str, Any]],
    *,
    mission_scope: dict[str, Any],
) -> tuple[dict[str, Any], str, list[str]]:
    scope_fallback = copy.deepcopy(mission_scope)
    location_candidates = [
        candidate
        for signal in signals
        if isinstance(signal, dict)
        for candidate in public_signal_location_candidates(signal)
    ]
    point_candidates = [
        candidate
        for candidate in location_candidates
        if maybe_number(candidate.get("latitude")) is not None and maybe_number(candidate.get("longitude")) is not None
    ]
    if point_candidates:
        mission_geometry = mission_scope.get("geometry") if isinstance(mission_scope.get("geometry"), dict) else {}
        if mission_geometry and all(
            point_matches_geometry(
                maybe_number(candidate.get("latitude")),
                maybe_number(candidate.get("longitude")),
                mission_geometry,
            )
            for candidate in point_candidates
        ):
            return (
                scope_fallback,
                "signal-derived",
                ["Public evidence locations fall inside the mission region."],
            )
        labels = unique_strings(candidate.get("label") for candidate in point_candidates if maybe_text(candidate.get("label")))
        derived_scope = bbox_scope_from_location_candidates(
            point_candidates,
            label=labels[0] if labels else "Derived public-signal footprint",
        )
        if derived_scope is not None:
            notes: list[str] = []
            if maybe_text(derived_scope.get("geometry", {}).get("type")) == "BBox":
                notes.append("Claim place scope comes from multiple public-signal locations.")
            return derived_scope, "signal-derived", notes
    if public_signal_mentions_mission_region(signals, mission_scope):
        return (
            scope_fallback,
            "signal-text-mention",
            ["Public evidence explicitly mentions the mission region."],
        )
    return (
        scope_fallback,
        "mission-fallback",
        ["Public evidence lacked a signal-local geographic anchor; mission scope is retained only as a retrieval boundary."],
    )


def derive_public_claim_time_window(
    signals: list[dict[str, Any]],
    *,
    mission_time_window: dict[str, Any],
) -> tuple[dict[str, Any], str, list[str]]:
    published = sorted(
        parsed
        for signal in signals
        if isinstance(signal, dict)
        for parsed in [parse_loose_datetime(signal.get("published_at_utc"))]
        if parsed is not None
    )
    if not published:
        return (
            copy.deepcopy(mission_time_window),
            "mission-fallback",
            ["Public evidence lacked auditable publish timestamps; mission time is retained only as a retrieval boundary."],
        )
    return (
        {
            "start_utc": to_rfc3339_z(published[0]),
            "end_utc": to_rfc3339_z(published[-1]),
        },
        "signal-derived",
        [],
    )


def build_public_claim_scope(
    *,
    signals: list[dict[str, Any]],
    mission_scope: dict[str, Any],
    mission_time_window: dict[str, Any],
) -> dict[str, Any]:
    time_window, time_source, time_notes = derive_public_claim_time_window(
        signals,
        mission_time_window=mission_time_window,
    )
    place_scope, place_source, place_notes = derive_public_claim_place_scope(
        signals,
        mission_scope=mission_scope,
    )
    return {
        "time_window": time_window,
        "place_scope": place_scope,
        "time_source": time_source,
        "place_source": place_source,
        "usable_for_matching": time_source != "mission-fallback" and place_source != "mission-fallback",
        "notes": unique_strings(time_notes + place_notes),
    }


def compact_claim_scope(scope: Any) -> dict[str, Any]:
    if not isinstance(scope, dict):
        return {}
    payload: dict[str, Any] = {
        "time_source": maybe_text(scope.get("time_source")),
        "place_source": maybe_text(scope.get("place_source")),
        "usable_for_matching": bool(scope.get("usable_for_matching")),
    }
    time_window = scope.get("time_window")
    if isinstance(time_window, dict):
        payload["time_window"] = {
            "start_utc": maybe_text(time_window.get("start_utc")),
            "end_utc": maybe_text(time_window.get("end_utc")),
        }
    place_scope = scope.get("place_scope")
    if isinstance(place_scope, dict):
        payload["place_scope"] = {
            "label": maybe_text(place_scope.get("label")),
            "geometry": copy.deepcopy(place_scope.get("geometry")) if isinstance(place_scope.get("geometry"), dict) else {},
        }
    notes = [
        maybe_text(item)
        for item in (scope.get("notes") if isinstance(scope.get("notes"), list) else [])
        if maybe_text(item)
    ]
    if notes:
        payload["notes"] = notes[:3]
    return payload


def claim_matching_scope(claim: dict[str, Any]) -> dict[str, Any] | None:
    claim_scope = claim.get("claim_scope")
    if isinstance(claim_scope, dict):
        if not bool(claim_scope.get("usable_for_matching")):
            return None
        time_window = claim_scope.get("time_window")
        place_scope = claim_scope.get("place_scope")
        if isinstance(time_window, dict) and isinstance(place_scope, dict):
            return compact_claim_scope(claim_scope)
        return None
    time_window = claim.get("time_window")
    place_scope = claim.get("place_scope")
    if isinstance(time_window, dict) and isinstance(place_scope, dict):
        return {
            "time_window": copy.deepcopy(time_window),
            "place_scope": copy.deepcopy(place_scope),
            "time_source": "legacy-inline",
            "place_source": "legacy-inline",
            "usable_for_matching": True,
        }
    return None


def direct_matching_gap_for_claim(claim: dict[str, Any]) -> str:
    claim_scope = claim.get("claim_scope")
    if not isinstance(claim_scope, dict):
        return ""
    if bool(claim_scope.get("usable_for_matching")):
        return ""
    place_source = maybe_text(claim_scope.get("place_source"))
    time_source = maybe_text(claim_scope.get("time_source"))
    if place_source == "mission-fallback" and time_source == "mission-fallback":
        return "Claim lacks signal-local time and place scope and cannot be treated as direct mission evidence yet."
    if place_source == "mission-fallback":
        return "Claim lacks signal-local place scope and cannot be treated as direct mission evidence yet."
    if time_source == "mission-fallback":
        return "Claim lacks signal-local timing and cannot be treated as direct mission evidence yet."
    return ""


def physical_investigation_leg_lookup(plan: dict[str, Any]) -> dict[str, dict[str, Any]]:
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    by_leg: dict[str, dict[str, Any]] = {}
    for hypothesis in hypotheses:
        if not isinstance(hypothesis, dict):
            continue
        for leg in hypothesis.get("chain_legs", []):
            if not isinstance(leg, dict):
                continue
            leg_id = maybe_text(leg.get("leg_id"))
            if not leg_id or leg_id == "public_interpretation" or leg_id in by_leg:
                continue
            by_leg[leg_id] = leg
    return {leg_id: by_leg[leg_id] for leg_id in PHYSICAL_LEG_ORDER if leg_id in by_leg}


def observation_overlaps_mission_scope(observation: dict[str, Any], mission_scope: dict[str, Any]) -> bool:
    observation_scope = observation.get("place_scope") if isinstance(observation.get("place_scope"), dict) else {}
    observation_geometry = observation_scope.get("geometry") if isinstance(observation_scope.get("geometry"), dict) else {}
    mission_geometry = mission_scope.get("geometry") if isinstance(mission_scope.get("geometry"), dict) else {}
    if not observation_geometry or not mission_geometry:
        return False
    return geometry_overlap(observation_geometry, mission_geometry)


def score_observation_for_investigation_leg(
    observation: dict[str, Any],
    leg: dict[str, Any],
    *,
    mission_scope: dict[str, Any],
) -> int:
    metric = canonical_environment_metric(observation.get("metric"))
    family = observation_metric_family(metric)
    relevant_families = {
        maybe_text(item)
        for item in (leg.get("metric_families") if isinstance(leg.get("metric_families"), list) else [])
        if maybe_text(item)
    }
    if family not in relevant_families:
        return -1
    overlaps_mission = observation_overlaps_mission_scope(observation, mission_scope)
    leg_id = maybe_text(leg.get("leg_id"))
    score = 3
    scope_mode = maybe_text(leg.get("scope_mode"))
    if scope_mode == "mission":
        score += 2 if overlaps_mission else -1
    elif scope_mode == "derived-region":
        score += 2 if not overlaps_mission else 0

    if leg_id == "source":
        if family == "fire-detection":
            score += 4
        elif metric in {"precipitation", "precipitation_sum", "soil_moisture_0_to_7cm"}:
            score += 3
        elif family == "hydrology":
            score += 1
    elif leg_id == "mechanism":
        if metric in {"wind_speed_10m", "relative_humidity_2m"}:
            score += 4
        elif family == "hydrology":
            score += 3
        elif family == "meteorology":
            score += 1
    elif leg_id == "impact":
        if overlaps_mission:
            score += 3
        if family in {"air-quality", "hydrology", "soil"}:
            score += 2
        elif metric == "temperature_2m":
            score += 2
    return score


def best_physical_observation_hypothesis_id(
    plan: dict[str, Any],
    *,
    observation: dict[str, Any],
    leg_id: str,
) -> str:
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    if not hypotheses:
        return ""
    if len(hypotheses) == 1:
        return maybe_text(hypotheses[0].get("hypothesis_id"))

    metric = canonical_environment_metric(observation.get("metric"))
    tokens = row_token_set(
        metric,
        observation_metric_family(metric),
        maybe_text(observation.get("source_skill")),
        maybe_text(((observation.get("place_scope") or {}).get("label"))),
        leg_id.replace("_", " "),
        minimum_length=3,
    )
    best_hypothesis_id = ""
    best_score = -1
    tied = False
    for hypothesis in hypotheses:
        if not isinstance(hypothesis, dict):
            continue
        hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
        if not hypothesis_id:
            continue
        hypothesis_tokens = row_token_set(
            hypothesis.get("statement"),
            hypothesis.get("summary"),
            minimum_length=3,
        )
        score = len(tokens & hypothesis_tokens)
        if score > best_score:
            best_score = score
            best_hypothesis_id = hypothesis_id
            tied = False
        elif score == best_score:
            tied = True
    if best_score <= 0 or tied:
        return ""
    return best_hypothesis_id


def infer_observation_investigation_tags(
    observation: dict[str, Any],
    *,
    plan: dict[str, Any],
    mission_scope: dict[str, Any],
) -> dict[str, str]:
    legs_by_id = physical_investigation_leg_lookup(plan)
    if not legs_by_id:
        return {}
    scored_legs: list[tuple[int, str]] = []
    for leg_id, leg in legs_by_id.items():
        score = score_observation_for_investigation_leg(
            observation,
            leg,
            mission_scope=mission_scope,
        )
        if score > 0:
            scored_legs.append((score, leg_id))
    if not scored_legs:
        return {}
    scored_legs.sort(key=lambda item: (-item[0], PHYSICAL_LEG_ORDER.index(item[1]) if item[1] in PHYSICAL_LEG_ORDER else 99))
    best_score = scored_legs[0][0]
    best_leg_ids = [leg_id for score, leg_id in scored_legs if score == best_score]
    payload: dict[str, str] = {}
    if len(best_leg_ids) == 1:
        payload["leg_id"] = best_leg_ids[0]
        hypothesis_id = best_physical_observation_hypothesis_id(
            plan,
            observation=observation,
            leg_id=best_leg_ids[0],
        )
        if hypothesis_id:
            payload["hypothesis_id"] = hypothesis_id
    return payload


def extract_value_for_metric(observation: dict[str, Any]) -> float | None:
    statistics_obj = observation.get("statistics")
    if isinstance(statistics_obj, dict):
        for key in ("mean", "max", "p95", "min"):
            value = maybe_number(statistics_obj.get(key))
            if value is not None:
                return value
    return maybe_number(observation.get("value"))


def metric_relevant(claim_type: str, metric: str) -> bool:
    metric = canonical_environment_metric(metric)
    if claim_type not in CLAIM_METRIC_RULES:
        return True
    support_metrics = set(CLAIM_METRIC_RULES[claim_type]["support"].keys())
    contradict_metrics = set(CLAIM_METRIC_RULES[claim_type]["contradict"].keys())
    return metric in support_metrics or metric in contradict_metrics


def default_evidence_role_for_claim_metric(claim_type: str, metric: str) -> str:
    metric = canonical_environment_metric(metric)
    if claim_type == "wildfire":
        if metric == "fire_detection_count":
            return "primary"
        if metric in {"temperature_2m", "wind_speed_10m"}:
            return "contextual"
        if metric in {"precipitation_sum", "relative_humidity_2m"}:
            return "contradictory"
    if claim_type in {"smoke", "air-pollution"}:
        family = observation_metric_family(metric)
        if family == "air-quality":
            return "primary"
        if metric == "fire_detection_count":
            return "contextual"
        return "contextual"
    if claim_type == "flood":
        if metric in PRECIPITATION_METRICS or metric in HYDROLOGY_METRICS:
            return "primary"
    if claim_type == "heat":
        if metric == "temperature_2m":
            return "primary"
        return "contextual"
    if claim_type == "drought":
        if metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
            return "primary"
    rules = CLAIM_METRIC_RULES.get(claim_type, {})
    if metric in rules.get("contradict", {}):
        return "contradictory"
    if metric in rules.get("support", {}):
        return "primary"
    return "contextual"


def effective_component_role_for_claim(claim_type: str, metric: str, declared_role: str) -> str:
    normalized_declared = maybe_text(declared_role)
    default_role = default_evidence_role_for_claim_metric(claim_type, metric)
    if normalized_declared in {"contradictory", "mixed"}:
        return normalized_declared
    if normalized_declared == "contextual":
        return "contextual"
    if normalized_declared == "primary":
        return "primary" if default_role == "primary" else default_role
    return default_role


def iter_observation_assessment_components(observation: dict[str, Any]) -> list[dict[str, Any]]:
    component_roles = observation.get("component_roles")
    if isinstance(component_roles, list) and component_roles:
        components: list[dict[str, Any]] = []
        for component in component_roles:
            if not isinstance(component, dict):
                continue
            metric = canonical_environment_metric(component.get("metric") or observation.get("metric"))
            value = maybe_number(component.get("value"))
            if not metric or value is None:
                continue
            components.append(
                {
                    "metric": metric,
                    "value": float(value),
                    "role": maybe_text(component.get("role")) or maybe_text(observation.get("evidence_role")),
                    "unit": maybe_text(component.get("unit")) or maybe_text(observation.get("unit")),
                    "rationale": maybe_text(component.get("rationale")),
                }
            )
        if components:
            return components
    metric = canonical_environment_metric(observation.get("metric"))
    value = extract_value_for_metric(observation)
    if not metric or value is None:
        return []
    return [
        {
            "metric": metric,
            "value": float(value),
            "role": maybe_text(observation.get("evidence_role")),
            "unit": maybe_text(observation.get("unit")),
            "rationale": "",
        }
    ]


def assess_claim_metric_value(claim_type: str, metric: str, metric_value: float) -> tuple[bool, bool]:
    rules = CLAIM_METRIC_RULES.get(claim_type)
    if rules is None:
        return False, False
    support_threshold = rules["support"].get(metric)
    contradict_threshold = rules["contradict"].get(metric)
    support_hit = False
    contradict_hit = False
    if support_threshold is not None:
        if metric == "fire_detection_count":
            support_hit = metric_value >= support_threshold
        elif claim_type == "drought" and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
            support_hit = metric_value <= support_threshold
        else:
            support_hit = metric_value >= support_threshold
    if contradict_threshold is not None:
        if metric == "fire_detection_count":
            contradict_hit = metric_value <= contradict_threshold
        elif claim_type == "wildfire" and metric in {"precipitation_sum", "relative_humidity_2m"}:
            contradict_hit = metric_value >= contradict_threshold
        elif claim_type == "drought" and metric in {"precipitation_sum", "soil_moisture_0_to_7cm"}:
            contradict_hit = metric_value >= contradict_threshold
        else:
            contradict_hit = metric_value <= contradict_threshold
    return support_hit, contradict_hit


def assess_observation_against_claim(claim_type: str, observation: dict[str, Any]) -> dict[str, Any]:
    components = iter_observation_assessment_components(observation)
    if not components:
        return {
            "support_score": 0,
            "contradict_score": 0,
            "notes": [],
            "primary_support_hits": 0,
            "contradict_hits": 0,
            "contextual_hits": 0,
        }
    support_score = 0
    contradict_score = 0
    primary_support_hits = 0
    contradict_hits = 0
    contextual_hits = 0
    notes: list[str] = []
    for component in components:
        metric = canonical_environment_metric(component.get("metric"))
        metric_value = maybe_number(component.get("value"))
        if not metric or metric_value is None:
            continue
        support_hit, contradict_hit = assess_claim_metric_value(claim_type, metric, float(metric_value))
        effective_role = effective_component_role_for_claim(
            claim_type,
            metric,
            maybe_text(component.get("role")),
        )
        label = f"{metric}={metric_value:g}"
        rationale = maybe_text(component.get("rationale"))
        if rationale:
            label = f"{label} ({rationale})"
        if support_hit:
            if effective_role == "primary":
                support_score += 2
                primary_support_hits += 1
                notes.append(label)
            elif effective_role == "mixed":
                support_score += 1
                primary_support_hits += 1
                notes.append(f"{label} [mixed]")
            else:
                contextual_hits += 1
                notes.append(f"{label} [contextual]")
        if contradict_hit:
            if effective_role in {"contradictory", "mixed"}:
                contradict_score += 2 if effective_role == "contradictory" else 1
                contradict_hits += 1
                notes.append(f"{label} [contradictory]")
            elif effective_role == "primary":
                contradict_score += 1
                contradict_hits += 1
                notes.append(f"{label} [primary-contradiction]")
            else:
                contextual_hits += 1
                notes.append(f"{label} [contextual-contradiction]")
    return {
        "support_score": support_score,
        "contradict_score": contradict_score,
        "notes": notes,
        "primary_support_hits": primary_support_hits,
        "contradict_hits": contradict_hits,
        "contextual_hits": contextual_hits,
    }


def build_evidence_summary(claim: dict[str, Any], observation_notes: list[str], verdict: str, gaps: list[str]) -> str:
    lead = claim.get("summary") or claim.get("statement") or "Claim"
    base = truncate_text(maybe_text(lead), 140)
    if observation_notes:
        return f"{base}. Matched metrics: {', '.join(observation_notes[:4])}."
    if gaps:
        return f"{base}. Evidence remains limited: {'; '.join(gaps[:2])}."
    return f"{base}. Current evidence verdict: {verdict}."


__all__ = [
    "CLAIM_KEYWORDS",
    "CLAIM_METRIC_RULES",
    "DEFAULT_OBSERVATION_FAMILY_ORDER",
    "ENVIRONMENT_METRIC_ALIASES",
    "GENERIC_REGION_TOKENS",
    "HYDROLOGY_METRICS",
    "METEOROLOGY_METRICS",
    "METRIC_FAMILY_GROUPS",
    "NON_CLAIM_PUBLIC_SIGNAL_KINDS",
    "PHYSICAL_CLAIM_TYPES",
    "PHYSICAL_LEG_ORDER",
    "POINT_MATCH_EPSILON_DEGREES",
    "PRECIPITATION_METRICS",
    "STOPWORDS",
    "assess_claim_metric_value",
    "assess_observation_against_claim",
    "best_physical_observation_hypothesis_id",
    "best_public_claim_hypothesis_id",
    "bbox_scope_from_location_candidates",
    "build_evidence_summary",
    "build_public_claim_scope",
    "candidate_statement",
    "canonical_environment_metric",
    "claim_matching_scope",
    "claim_priority_metric_families",
    "claim_type_from_text",
    "compact_claim_scope",
    "default_evidence_role_for_claim_metric",
    "derive_public_claim_place_scope",
    "derive_public_claim_time_window",
    "direct_matching_gap_for_claim",
    "effective_component_role_for_claim",
    "extract_value_for_metric",
    "geometry_overlap",
    "geometry_to_bbox",
    "infer_observation_investigation_tags",
    "iter_observation_assessment_components",
    "maybe_number",
    "metric_relevant",
    "observation_metric_family",
    "observation_overlaps_mission_scope",
    "parse_loose_datetime",
    "physical_investigation_leg_lookup",
    "point_matches_geometry",
    "public_signal_channel",
    "public_signal_location_candidates",
    "public_signal_mentions_mission_region",
    "region_core_tokens",
    "row_token_set",
    "score_observation_for_investigation_leg",
    "semantic_fingerprint",
    "text_tokens",
    "time_windows_overlap",
    "to_rfc3339_z",
]

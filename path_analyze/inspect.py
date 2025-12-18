"""Inspect Path.csv and export readable time series."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from path_analyze.models import TrackPoint
from path_analyze.timeutils import DeltaStats, delta_stats, dt_from_epoch_ms


@dataclass(frozen=True, slots=True)
class InspectResult:
    """High-level CSV inspection result."""

    fieldnames: Sequence[str]
    rows_total: int
    rows_parsed: int
    rows_skipped: int
    min_time_ms: int | None
    max_time_ms: int | None
    delta: DeltaStats | None
    min_lat: float | None
    max_lat: float | None
    min_lon: float | None
    max_lon: float | None
    duplicates_geo_time: int


def inspect_points(points: Sequence[TrackPoint]) -> InspectResult:
    """Inspect already-loaded points."""

    if not points:
        return InspectResult(
            fieldnames=(),
            rows_total=0,
            rows_parsed=0,
            rows_skipped=0,
            min_time_ms=None,
            max_time_ms=None,
            delta=None,
            min_lat=None,
            max_lat=None,
            min_lon=None,
            max_lon=None,
            duplicates_geo_time=0,
        )

    times = sorted(p.geo_time_ms for p in points)
    dupe = 0
    for i in range(1, len(times)):
        if times[i] == times[i - 1]:
            dupe += 1

    lats = [p.latitude for p in points]
    lons = [p.longitude for p in points]
    return InspectResult(
        fieldnames=(),
        rows_total=len(points),
        rows_parsed=len(points),
        rows_skipped=0,
        min_time_ms=times[0],
        max_time_ms=times[-1],
        delta=delta_stats(times),
        min_lat=min(lats),
        max_lat=max(lats),
        min_lon=min(lons),
        max_lon=max(lons),
        duplicates_geo_time=dupe,
    )


def export_readable_csv(
    points: Iterable[TrackPoint],
    out_path: str | Path,
    tz_name: str,
    place_name_by_key: dict[str, str] | None = None,
    coord_precision: int = 4,
) -> None:
    """Export points to a human-readable CSV.

    Output columns:
        - time_local: ISO datetime (local timezone)
        - epoch_ms, lat, lon, altitude_m, speed_mps, horizontal_accuracy_m, location_type
        - place_name (optional): reverse geocoded name if provided
    """

    p = Path(out_path)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "time_local",
                "epoch_ms",
                "latitude",
                "longitude",
                "altitude_m",
                "speed_mps",
                "horizontal_accuracy_m",
                "location_type",
                "place_name",
            ],
        )
        w.writeheader()
        for pt in points:
            place_name = ""
            if place_name_by_key is not None:
                from path_analyze.geocode import coord_key

                place_name = place_name_by_key.get(coord_key(pt.latitude, pt.longitude, coord_precision), "")
            w.writerow(
                {
                    "time_local": dt_from_epoch_ms(pt.geo_time_ms, tz_name).isoformat(sep=" "),
                    "epoch_ms": pt.geo_time_ms,
                    "latitude": pt.latitude,
                    "longitude": pt.longitude,
                    "altitude_m": pt.altitude_m,
                    "speed_mps": pt.speed_mps,
                    "horizontal_accuracy_m": pt.horizontal_accuracy_m,
                    "location_type": pt.location_type,
                    "place_name": place_name,
                }
            )



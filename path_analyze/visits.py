"""Geofence visit detection and reporting."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from path_analyze.geo import is_inside_circle
from path_analyze.models import TrackPoint, Visit
from path_analyze.timeutils import dt_from_epoch_ms, parse_dt


@dataclass(frozen=True, slots=True)
class GeofenceCircle:
    """A circle geofence (center + radius)."""

    center_lat: float
    center_lon: float
    radius_m: float


@dataclass(frozen=True, slots=True)
class VisitParams:
    """Parameters controlling visit segmentation."""

    tz_name: str
    # If consecutive "inside" samples are separated by a very large gap, we may treat it as
    # discontinuous to avoid counting days. Default is generous to support sparse sampling.
    max_gap_seconds: float = 12 * 60 * 60.0
    # Exit confirmation: require being outside for at least this long before closing a visit.
    # This helps against GPS jitter around the boundary.
    exit_grace_seconds: float = 5 * 60.0
    # For entry/exit boundary midpoint estimation only. If gaps exceed this threshold,
    # we stop using the midpoint and fall back to one side.
    transition_gap_seconds: float = 10 * 60.0
    min_dwell_seconds: float = 60.0


def _boundary_ms(prev_ms: int, cur_ms: int, max_gap_s: float, prefer: str) -> int:
    """Estimate entry/exit boundary timestamp.

    If two samples are close enough, use midpoint; otherwise fall back to one side.

    Args:
        prev_ms: Previous sample epoch ms.
        cur_ms: Current sample epoch ms.
        max_gap_s: If gap exceeds this, midpoint becomes unreliable.
        prefer: "prev" or "cur".
    """

    if cur_ms < prev_ms:
        return prev_ms
    gap_s = (cur_ms - prev_ms) / 1000.0
    if gap_s <= max_gap_s:
        return prev_ms + (cur_ms - prev_ms) // 2
    return prev_ms if prefer == "prev" else cur_ms


def find_visits(
    points: Sequence[TrackPoint],
    geofence: GeofenceCircle,
    params: VisitParams,
) -> list[Visit]:
    """Detect visits (time intervals) spent inside the geofence.

    Args:
        points: Track points (can be unsorted).
        geofence: Circle geofence definition.
        params: Segmentation parameters.

    Returns:
        List of visits.
    """

    if not points:
        return []

    pts = sorted(points, key=lambda p: p.geo_time_ms)

    visits: list[Visit] = []
    in_visit = False
    visit_start_ms = 0
    visit_end_ms = 0
    last_inside_ms = 0
    outside_started_ms: int | None = None
    visit_points = 0
    visit_id = 1
    method = "midpoint"

    prev = pts[0]
    prev_inside = is_inside_circle(
        prev.latitude, prev.longitude, geofence.center_lat, geofence.center_lon, geofence.radius_m
    )
    if prev_inside:
        in_visit = True
        visit_start_ms = prev.geo_time_ms
        visit_end_ms = prev.geo_time_ms
        last_inside_ms = prev.geo_time_ms
        visit_points = 1
        method = "start_at_first_inside"

    for cur in pts[1:]:
        cur_inside = is_inside_circle(
            cur.latitude, cur.longitude, geofence.center_lat, geofence.center_lon, geofence.radius_m
        )

        gap_s = (cur.geo_time_ms - prev.geo_time_ms) / 1000.0 if cur.geo_time_ms >= prev.geo_time_ms else 0.0
        if not in_visit:
            if (not prev_inside) and cur_inside:
                # entry transition: estimate boundary
                visit_start_ms = _boundary_ms(
                    prev.geo_time_ms,
                    cur.geo_time_ms,
                    params.transition_gap_seconds,
                    "cur",
                )
                visit_end_ms = cur.geo_time_ms
                last_inside_ms = cur.geo_time_ms
                visit_points = 1
                in_visit = True
                outside_started_ms = None
                method = "midpoint_entry"
            elif cur_inside:
                # two consecutive inside with no explicit transition (after discontinuity, etc.)
                visit_start_ms = cur.geo_time_ms
                visit_end_ms = cur.geo_time_ms
                last_inside_ms = cur.geo_time_ms
                visit_points = 1
                in_visit = True
                outside_started_ms = None
                method = "start_at_inside"
        else:
            if cur_inside:
                # If we are inside again, clear any pending exit.
                outside_started_ms = None
                # If the gap between inside samples is extremely large, treat it as discontinuity.
                # This prevents a single visit from spanning days due to sparse sampling.
                if last_inside_ms > 0 and (cur.geo_time_ms - last_inside_ms) / 1000.0 > params.max_gap_seconds:
                    _append_visit(
                        visits=visits,
                        visit_id=visit_id,
                        start_ms=visit_start_ms,
                        end_ms=last_inside_ms,
                        points=visit_points,
                        method="split_on_inside_gap",
                        tz_name=params.tz_name,
                        min_dwell_s=params.min_dwell_seconds,
                    )
                    visit_id += 1
                    # start a new visit at current inside point
                    visit_start_ms = cur.geo_time_ms
                    visit_end_ms = cur.geo_time_ms
                    last_inside_ms = cur.geo_time_ms
                    visit_points = 1
                    method = "restart_after_gap"
                else:
                    visit_end_ms = cur.geo_time_ms
                    last_inside_ms = cur.geo_time_ms
                    visit_points += 1
            else:
                # Exit confirmation: only close if we stayed outside long enough.
                if outside_started_ms is None:
                    outside_started_ms = cur.geo_time_ms
                outside_s = (cur.geo_time_ms - outside_started_ms) / 1000.0 if cur.geo_time_ms >= outside_started_ms else 0.0
                if outside_s >= params.exit_grace_seconds:
                    # exit boundary: between last inside and the first confirmed outside moment
                    exit_ms = _boundary_ms(
                        last_inside_ms,
                        outside_started_ms,
                        params.transition_gap_seconds,
                        "prev",
                    )
                    visit_end_ms = max(visit_end_ms, exit_ms)
                    _append_visit(
                        visits=visits,
                        visit_id=visit_id,
                        start_ms=visit_start_ms,
                        end_ms=visit_end_ms,
                        points=visit_points,
                        method="exit_grace",
                        tz_name=params.tz_name,
                        min_dwell_s=params.min_dwell_seconds,
                    )
                    visit_id += 1
                    in_visit = False
                    visit_points = 0
                    outside_started_ms = None

        prev = cur
        prev_inside = cur_inside

    if in_visit:
        # Close at last known inside time (more stable than 'prev', which might be outside due to jitter).
        visit_end_ms = last_inside_ms if last_inside_ms > 0 else prev.geo_time_ms
        _append_visit(
            visits=visits,
            visit_id=visit_id,
            start_ms=visit_start_ms,
            end_ms=visit_end_ms,
            points=visit_points,
            method=method,
            tz_name=params.tz_name,
            min_dwell_s=params.min_dwell_seconds,
        )

    return visits


def _append_visit(
    *,
    visits: list[Visit],
    visit_id: int,
    start_ms: int,
    end_ms: int,
    points: int,
    method: str,
    tz_name: str,
    min_dwell_s: float,
) -> None:
    if end_ms <= start_ms:
        return
    if (end_ms - start_ms) / 1000.0 < min_dwell_s:
        return
    visits.append(
        Visit(
            visit_id=visit_id,
            start_dt=dt_from_epoch_ms(start_ms, tz_name),
            end_dt=dt_from_epoch_ms(end_ms, tz_name),
            start_ms=start_ms,
            end_ms=end_ms,
            points=points,
            method=method,
        )
    )


def _format_hhmmss(seconds: float) -> str:
    s = int(round(seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


def write_visits_csv(visits: Sequence[Visit], out_path: str | Path) -> None:
    """Write visits to CSV for manual editing."""

    p = Path(out_path)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "visit_id",
                "start_time",
                "end_time",
                "duration_seconds",
                "duration_hhmmss",
                "points",
                "start_epoch_ms",
                "end_epoch_ms",
                "method",
            ],
        )
        w.writeheader()
        for v in visits:
            w.writerow(
                {
                    "visit_id": v.visit_id,
                    "start_time": v.start_dt.isoformat(sep=" "),
                    "end_time": v.end_dt.isoformat(sep=" "),
                    "duration_seconds": f"{v.duration_seconds:.3f}",
                    "duration_hhmmss": _format_hhmmss(v.duration_seconds),
                    "points": v.points,
                    "start_epoch_ms": v.start_ms,
                    "end_epoch_ms": v.end_ms,
                    "method": v.method,
                }
            )


@dataclass(frozen=True, slots=True)
class VisitsTotal:
    """Total duration summary."""

    visits: int
    total_seconds: float

    @property
    def total_hhmmss(self) -> str:
        return _format_hhmmss(self.total_seconds)


def sum_visits(visits: Iterable[Visit]) -> VisitsTotal:
    """Sum visit durations."""

    total = 0.0
    count = 0
    for v in visits:
        total += v.duration_seconds
        count += 1
    return VisitsTotal(visits=count, total_seconds=total)


def iter_visits_from_csv(csv_path: str | Path, tz_name: str) -> Iterator[Visit]:
    """Read visits.csv (possibly manually edited) and yield Visit objects.

    Manual editing guidance:
        - You may edit start_time/end_time columns directly.
        - If you do, start_epoch_ms/end_epoch_ms will be ignored and recomputed.
    """

    p = Path(csv_path)
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            start_dt = parse_dt(row["start_time"], tz_name)
            end_dt = parse_dt(row["end_time"], tz_name)
            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
            yield Visit(
                visit_id=int(row.get("visit_id", "0") or "0"),
                start_dt=start_dt,
                end_dt=end_dt,
                start_ms=start_ms,
                end_ms=end_ms,
                points=int(row.get("points", "0") or "0"),
                method=row.get("method", "manual_or_imported") or "manual_or_imported",
            )




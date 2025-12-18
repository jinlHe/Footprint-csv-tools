"""CSV input/output utilities for the exported track file."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

from path_analyze.models import TrackPoint

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CsvSummary:
    """Quick summary of CSV parsing."""

    rows_total: int
    rows_parsed: int
    rows_skipped: int
    fieldnames: Sequence[str]


def _parse_int(value: str) -> int:
    return int(value.strip())


def _parse_float(value: str) -> float:
    return float(value.strip())


def iter_track_points(csv_path: str | Path) -> Iterator[TrackPoint]:
    """Yield TrackPoint objects from Path.csv.

    Args:
        csv_path: Path to the exported CSV.

    Yields:
        TrackPoint rows parsed successfully.

    Notes:
        The export uses these columns (observed):
          - geoTime: epoch milliseconds
          - latitude/longitude: decimal degrees
          - altitude/speed/horizontalAccuracy/locationType, etc.
    """

    p = Path(csv_path)
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return

        for row in reader:
            try:
                yield TrackPoint(
                    geo_time_ms=_parse_int(row["geoTime"]),
                    latitude=_parse_float(row["latitude"]),
                    longitude=_parse_float(row["longitude"]),
                    altitude_m=_parse_float(row.get("altitude", "0") or "0"),
                    speed_mps=_parse_float(row.get("speed", "0") or "0"),
                    horizontal_accuracy_m=_parse_float(row.get("horizontalAccuracy", "-1") or "-1"),
                    location_type=_parse_int(row.get("locationType", "0") or "0"),
                )
            except KeyError as exc:
                raise KeyError(f"CSV缺少必要字段：{exc}. 实际字段：{reader.fieldnames}") from exc
            except (ValueError, TypeError):
                # 某些行可能损坏/空行，直接跳过
                continue


def load_track_points(csv_path: str | Path) -> tuple[list[TrackPoint], CsvSummary]:
    """Load all points into memory.

    Args:
        csv_path: Path to the exported CSV.

    Returns:
        (points, summary)
    """

    p = Path(csv_path)
    rows_total = 0
    parsed: list[TrackPoint] = []
    fieldnames: Sequence[str] = ()

    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or ()
        for row in reader:
            rows_total += 1
            try:
                parsed.append(
                    TrackPoint(
                        geo_time_ms=_parse_int(row["geoTime"]),
                        latitude=_parse_float(row["latitude"]),
                        longitude=_parse_float(row["longitude"]),
                        altitude_m=_parse_float(row.get("altitude", "0") or "0"),
                        speed_mps=_parse_float(row.get("speed", "0") or "0"),
                        horizontal_accuracy_m=_parse_float(row.get("horizontalAccuracy", "-1") or "-1"),
                        location_type=_parse_int(row.get("locationType", "0") or "0"),
                    )
                )
            except (KeyError, ValueError, TypeError):
                continue

    summary = CsvSummary(
        rows_total=rows_total,
        rows_parsed=len(parsed),
        rows_skipped=rows_total - len(parsed),
        fieldnames=fieldnames,
    )
    if summary.rows_skipped > 0:
        logger.warning("CSV中有 %s 行解析失败已跳过", summary.rows_skipped)
    return parsed, summary



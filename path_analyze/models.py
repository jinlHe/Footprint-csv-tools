"""Data models for track points and visits."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Final


@dataclass(frozen=True, slots=True)
class TrackPoint:
    """A single location sample.

    Attributes:
        geo_time_ms: Unix epoch milliseconds.
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.
        altitude_m: Altitude in meters. May be 0.0 depending on device/app.
        speed_mps: Speed in meters/second. Some rows may use -1.0 as sentinel.
        horizontal_accuracy_m: Horizontal accuracy in meters. Some rows use -1.0.
        location_type: App-specific integer describing the positioning source.
    """

    geo_time_ms: int
    latitude: float
    longitude: float
    altitude_m: float
    speed_mps: float
    horizontal_accuracy_m: float
    location_type: int

    @property
    def geo_time_s(self) -> float:
        """Unix epoch seconds as float."""

        return self.geo_time_ms / 1000.0


@dataclass(frozen=True, slots=True)
class Visit:
    """A continuous time interval spent within a geofence.

    Note:
        Start/end are stored as timezone-aware datetimes for readability.
        The epoch fields are kept for stable numeric computations.
    """

    visit_id: int
    start_dt: datetime
    end_dt: datetime
    start_ms: int
    end_ms: int
    points: int
    method: str

    @property
    def duration_seconds(self) -> float:
        """Visit duration in seconds."""

        return max(0.0, (self.end_ms - self.start_ms) / 1000.0)


DEFAULT_TZ: Final[str] = "Asia/Shanghai"



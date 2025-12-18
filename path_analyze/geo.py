"""Geospatial utilities (no external dependencies)."""

from __future__ import annotations

import math


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute Haversine distance in meters between two lat/lon points.

    Args:
        lat1: Latitude 1 in degrees.
        lon1: Longitude 1 in degrees.
        lat2: Latitude 2 in degrees.
        lon2: Longitude 2 in degrees.

    Returns:
        Distance in meters.
    """

    r = 6_371_000.0  # mean Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def is_inside_circle(
    lat: float,
    lon: float,
    center_lat: float,
    center_lon: float,
    radius_m: float,
) -> bool:
    """Check whether a point is inside or on the boundary of a circle geofence."""

    return haversine_m(lat, lon, center_lat, center_lon) <= radius_m



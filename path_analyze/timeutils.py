"""Time parsing and formatting utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timezone
from typing import Iterable

from zoneinfo import ZoneInfo


def tzinfo_from_name(tz_name: str) -> timezone:
    """Create tzinfo from an IANA timezone name.

    Args:
        tz_name: Timezone name like "Asia/Shanghai".

    Returns:
        tzinfo instance.

    Raises:
        ValueError: If timezone name is invalid on this system.
    """

    try:
        return ZoneInfo(tz_name)
    except Exception as exc:  # ZoneInfo raises KeyError / ZoneInfoNotFoundError (platform dependent)
        raise ValueError(f"无效时区：{tz_name!r}。例如可用：Asia/Shanghai") from exc


def dt_from_epoch_ms(epoch_ms: int, tz_name: str) -> datetime:
    """Convert epoch milliseconds to timezone-aware datetime.

    Args:
        epoch_ms: Unix epoch milliseconds.
        tz_name: IANA timezone name.

    Returns:
        Timezone-aware datetime.
    """

    tz = tzinfo_from_name(tz_name)
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=tz)


def epoch_ms_from_dt(dt: datetime) -> int:
    """Convert a datetime to epoch milliseconds.

    Args:
        dt: Datetime. If naive, will be treated as UTC (discouraged).

    Returns:
        Epoch milliseconds.
    """

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def parse_dt(text: str, tz_name: str) -> datetime:
    """Parse user-provided datetime text to a timezone-aware datetime.

    Supported formats:
      - "YYYY-MM-DD HH:MM:SS"
      - "YYYY-MM-DDTHH:MM:SS"
      - with optional timezone offset, e.g. "+08:00"

    If timezone is missing, it will be assumed to be tz_name.

    Args:
        text: Datetime string.
        tz_name: IANA timezone name for naive strings.

    Returns:
        Timezone-aware datetime.

    Raises:
        ValueError: If cannot parse.
    """

    s = text.strip().replace("T", " ")
    tz = tzinfo_from_name(tz_name)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        raise ValueError(f"无法解析时间：{text!r}。建议格式：2025-12-18 09:30:00") from exc

    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


@dataclass(frozen=True, slots=True)
class DeltaStats:
    """Sampling interval stats (seconds)."""

    count: int
    min_s: float
    median_s: float
    p95_s: float
    max_s: float


def delta_stats(epoch_ms_sorted: Iterable[int]) -> DeltaStats | None:
    """Compute basic sampling-interval statistics.

    Args:
        epoch_ms_sorted: Epoch ms sorted ascending.

    Returns:
        DeltaStats or None if less than 2 points.
    """

    ms = list(epoch_ms_sorted)
    if len(ms) < 2:
        return None
    deltas = [(ms[i] - ms[i - 1]) / 1000.0 for i in range(1, len(ms)) if ms[i] >= ms[i - 1]]
    if not deltas:
        return None
    deltas.sort()
    n = len(deltas)
    median = deltas[n // 2] if n % 2 == 1 else 0.5 * (deltas[n // 2 - 1] + deltas[n // 2])
    p95 = deltas[int(0.95 * (n - 1))]
    return DeltaStats(
        count=n,
        min_s=deltas[0],
        median_s=median,
        p95_s=p95,
        max_s=deltas[-1],
    )



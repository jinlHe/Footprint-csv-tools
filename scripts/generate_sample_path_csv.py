from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final

from zoneinfo import ZoneInfo


TZ: Final[str] = "Asia/Shanghai"


@dataclass(frozen=True, slots=True)
class Cluster:
    name: str
    lat: float
    lon: float


def _epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _day_start_epoch_s(dt: datetime) -> int:
    """Return epoch seconds for local midnight in TZ, encoded as Unix seconds."""

    tz = ZoneInfo(TZ)
    local = dt.astimezone(tz)
    midnight = datetime(local.year, local.month, local.day, tzinfo=tz)
    return int(midnight.timestamp())


def generate_points(
    *,
    rows: int,
    seed: int,
    start_local: datetime,
    clusters: list[Cluster],
) -> list[dict[str, str]]:
    """Generate fake Path.csv rows with realistic-ish movement/stays."""

    rng = random.Random(seed)
    tz = ZoneInfo(TZ)
    cur = start_local.replace(tzinfo=tz)

    out: list[dict[str, str]] = []
    cluster = rng.choice(clusters)

    for _ in range(rows):
        # Occasionally "teleport" to another city to simulate travel
        if rng.random() < 0.03:
            cluster = rng.choice(clusters)

        # Mostly stay around current cluster, small jitter
        lat = cluster.lat + rng.uniform(-0.0015, 0.0015)
        lon = cluster.lon + rng.uniform(-0.0015, 0.0015)

        # Time step: usually 1-10 minutes, sometimes 30-90 minutes gap
        if rng.random() < 0.08:
            cur = cur + timedelta(minutes=rng.uniform(30, 90))
        else:
            cur = cur + timedelta(seconds=rng.uniform(60, 600))

        geo_ms = _epoch_ms(cur)
        group_time = int(geo_ms / 1000)
        day_time = _day_start_epoch_s(cur)

        altitude = rng.uniform(0, 600)
        course = rng.uniform(0, 360)
        hacc = rng.choice([3.0, 5.0, 8.0, 12.0, 20.0, 35.0])
        vacc = rng.choice([3.0, 5.0, 8.0, 12.0, 20.0, 30.0])
        speed = rng.choice([0.0, 0.0, 0.0, rng.uniform(0.5, 2.5), rng.uniform(3.0, 12.0), -1.0])

        out.append(
            {
                "geoTime": str(geo_ms),
                "latitude": f"{lat:.7f}",
                "longitude": f"{lon:.7f}",
                "altitude": f"{altitude:.1f}",
                "course": f"{course:.1f}",
                "horizontalAccuracy": f"{hacc:.1f}",
                "verticalAccuracy": f"{vacc:.1f}",
                "speed": f"{speed:.1f}",
                "dayTime": str(day_time),
                "groupTime": str(group_time),
                "isSplit": "0",
                "isMerge": "0",
                "isAdd": "0",
                "network": str(rng.choice([0, 1, 2])),
                "networkName": "",
                "locationType": str(rng.choice([0, 1])),
            }
        )

    # Ensure stable order by time
    out.sort(key=lambda r: int(r["geoTime"]))
    return out


def main() -> int:
    p = argparse.ArgumentParser(description="Generate a fake Path.csv for demo/testing (privacy-safe).")
    p.add_argument("--out", type=str, default="sample_data/Path.csv", help="Output CSV path")
    p.add_argument("--rows", type=int, default=500, help="Number of rows")
    p.add_argument("--seed", type=int, default=42, help="Random seed (reproducible)")
    p.add_argument(
        "--start",
        type=str,
        default="2025-01-01 08:00:00",
        help="Start local time in Asia/Shanghai, e.g. '2025-01-01 08:00:00'",
    )
    args = p.parse_args()

    start_local = datetime.fromisoformat(args.start)
    clusters = [
        Cluster("shanghai_lab", 31.2304000, 121.4737000),
        Cluster("shanghai_home", 31.2222000, 121.4588000),
        Cluster("beijing_trip", 39.9042000, 116.4074000),
        Cluster("shenzhen_trip", 22.5431000, 114.0579000),
    ]

    rows = generate_points(rows=args.rows, seed=args.seed, start_local=start_local, clusters=clusters)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "geoTime",
        "latitude",
        "longitude",
        "altitude",
        "course",
        "horizontalAccuracy",
        "verticalAccuracy",
        "speed",
        "dayTime",
        "groupTime",
        "isSplit",
        "isMerge",
        "isAdd",
        "network",
        "networkName",
        "locationType",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Generated: {out_path} (rows={len(rows)}, seed={args.seed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



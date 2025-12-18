"""Reverse geocoding utilities (lat/lon -> place name).

This module intentionally uses only Python standard library to keep the project lightweight.

Important:
    - Public reverse-geocoding services are rate-limited.
    - For Nominatim (OpenStreetMap), please respect their usage policy and set a reasonable
      request interval and a descriptive User-Agent.
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class GeocodeResult:
    """A minimal reverse geocoding result."""

    place_name: str
    raw: dict[str, Any]


def coord_key(lat: float, lon: float, precision: int) -> str:
    """Build a stable cache key by rounding coordinates.

    Notes:
        This is used as the cache key: "lat,lon" with fixed decimals.
        Precision=4 is often a good default (lat ~ 11m resolution).
    """

    return f"{round(lat, precision):.{precision}f},{round(lon, precision):.{precision}f}"


class JsonDiskCache:
    """A tiny JSON cache persisted on disk (key -> result dict)."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        # Write-ahead journal for crash-safe incremental persistence.
        # Example: geocode_cache.json -> geocode_cache.journal.jsonl
        self._journal_path = self._path.with_name(f"{self._path.stem}.journal.jsonl")
        self._data: dict[str, dict[str, Any]] = {}
        self._loaded = False

    def ensure_persistent_files(self) -> None:
        """Ensure cache and journal files exist on disk.

        This improves UX: during long runs you can immediately see the cache artifacts.
        It does NOT clear existing content.
        """

        # Ensure snapshot file exists (even if empty); do not overwrite existing.
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text("{}", encoding="utf-8")

        # Ensure journal file exists; do not overwrite existing.
        if not self._journal_path.exists():
            self._journal_path.parent.mkdir(parents=True, exist_ok=True)
            self._journal_path.write_text("", encoding="utf-8")

    def load(self) -> None:
        """Load cache from disk (no-op if file not exists)."""

        if self._loaded:
            return
        if not self._path.exists():
            self._data = {}
            self._loaded = True
            return
        text = self._path.read_text(encoding="utf-8").strip()
        if not text:
            self._data = {}
            self._loaded = True
            return
        try:
            self._data = json.loads(text)
        except json.JSONDecodeError:
            # Cache file corrupted: keep a backup and start fresh
            backup = self._path.with_suffix(self._path.suffix + ".broken")
            backup.write_text(text, encoding="utf-8")
            self._data = {}

        # Replay journal (if any) so that even if the program crashed, we keep the latest results.
        self._replay_journal()
        self._loaded = True

    def get(self, key: str) -> dict[str, Any] | None:
        self.load()
        return self._data.get(key)

    def set(self, key: str, value: dict[str, Any]) -> None:
        self.load()
        self._data[key] = value
        self._append_journal(key, value)

    def place_name_map(self) -> dict[str, str]:
        """Return key -> place_name mapping for all cached entries."""

        self.load()
        out: dict[str, str] = {}
        for k, v in self._data.items():
            out[k] = str(v.get("place_name", "") or "")
        return out

    def flush(self) -> None:
        """Persist cache to disk (atomic-ish)."""

        self.load()
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self._path)
        # After we persisted the full snapshot, it's safe to clear the journal.
        self._clear_journal()

    def _append_journal(self, key: str, value: dict[str, Any]) -> None:
        """Append a single update to the journal for crash-safe persistence."""

        self._journal_path.parent.mkdir(parents=True, exist_ok=True)
        record = {"k": key, "v": value}
        with self._journal_path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _replay_journal(self) -> None:
        """Replay journal entries into memory (best-effort)."""

        if not self._journal_path.exists():
            return
        try:
            with self._journal_path.open("r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        rec = json.loads(s)
                    except json.JSONDecodeError:
                        # ignore broken tail lines
                        continue
                    k = rec.get("k")
                    v = rec.get("v")
                    if isinstance(k, str) and isinstance(v, dict):
                        self._data[k] = v
        except OSError:
            # If journal cannot be read, do not fail the whole run.
            return

    def _clear_journal(self) -> None:
        """Clear journal file if exists (best-effort)."""

        try:
            if self._journal_path.exists():
                self._journal_path.unlink()
        except OSError:
            return


@dataclass(frozen=True, slots=True)
class NominatimConfig:
    """Configuration for Nominatim reverse API."""

    base_url: str = "https://nominatim.openstreetmap.org/reverse"
    accept_language: str = "zh-CN"
    zoom: int = 18
    addressdetails: int = 1
    timeout_seconds: float = 20.0
    min_interval_seconds: float = 1.0
    user_agent: str = "path-analyze/0.1.0 (reverse-geocode; please set your own UA)"


def nominatim_reverse_raw(lat: float, lon: float, cfg: NominatimConfig) -> dict[str, Any] | None:
    """Call Nominatim reverse API and return raw JSON dict.

    This is a pure function (no cache, no throttling state). It is designed to be
    picklable-friendly for multiprocessing usage.

    Args:
        lat: Latitude.
        lon: Longitude.
        cfg: NominatimConfig.

    Returns:
        Parsed JSON dict on success, otherwise None.
    """

    params = {
        "format": "jsonv2",
        "lat": f"{lat:.8f}",
        "lon": f"{lon:.8f}",
        "zoom": str(cfg.zoom),
        "addressdetails": str(cfg.addressdetails),
        "accept-language": cfg.accept_language,
    }
    url = f"{cfg.base_url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": cfg.user_agent,
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=cfg.timeout_seconds) as resp:  # noqa: S310
            body = resp.read().decode("utf-8", errors="replace")
        raw: dict[str, Any] = json.loads(body)
    except Exception:
        return None
    return raw


class NominatimReverseGeocoder:
    """Reverse geocoder using OpenStreetMap Nominatim."""

    def __init__(self, config: NominatimConfig, cache: JsonDiskCache | None = None) -> None:
        self._cfg = config
        self._cache = cache
        self._last_request_at = 0.0

    def reverse(
        self,
        *,
        lat: float,
        lon: float,
        precision: int = 4,
        max_new_requests: int | None = None,
        _budget_state: dict[str, int] | None = None,
    ) -> GeocodeResult | None:
        """Reverse geocode one coordinate.

        Args:
            lat: Latitude.
            lon: Longitude.
            precision: Rounding precision for caching (default 4 ~ ~11m lat).
            max_new_requests: Max number of *new* API requests allowed (None means unlimited).
            _budget_state: Internal mutable state for counting new requests across calls.

        Returns:
            GeocodeResult or None if budget exceeded or request failed.
        """

        key = coord_key(lat, lon, precision)
        if self._cache is not None:
            cached = self._cache.get(key)
            if cached is not None:
                return GeocodeResult(place_name=str(cached.get("place_name", "")), raw=cached)

        if max_new_requests is not None:
            if _budget_state is None:
                _budget_state = {"used": 0}
            if _budget_state["used"] >= max_new_requests:
                return None
            _budget_state["used"] += 1

        self._sleep_if_needed()
        raw = nominatim_reverse_raw(lat, lon, self._cfg)
        if raw is None:
            return None

        place = str(raw.get("display_name", "") or "")
        result = GeocodeResult(place_name=place, raw=raw)
        if self._cache is not None:
            self._cache.set(key, {"place_name": place, **raw})
        return result

    def _sleep_if_needed(self) -> None:
        now = time.time()
        wait = self._cfg.min_interval_seconds - (now - self._last_request_at)
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.time()



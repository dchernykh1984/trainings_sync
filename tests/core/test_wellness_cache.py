from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.connectors.wellness_base import WellnessDataType
from app.core.wellness_cache import WellnessCache


@pytest.fixture
def cache(tmp_path: Path) -> WellnessCache:
    return WellnessCache(tmp_path / "cache")


class TestHas:
    def test_returns_false_when_no_file(self, cache: WellnessCache) -> None:
        assert not cache.has("garmin", WellnessDataType.SLEEP, "2026-01-01")

    def test_returns_true_after_write(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {"v": 1})
        assert cache.has("garmin", WellnessDataType.SLEEP, "2026-01-01")

    def test_different_connector_returns_false(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {"v": 1})
        assert not cache.has("strava", WellnessDataType.SLEEP, "2026-01-01")

    def test_different_data_type_returns_false(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {"v": 1})
        assert not cache.has("garmin", WellnessDataType.HRV, "2026-01-01")

    def test_different_date_key_returns_false(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {"v": 1})
        assert not cache.has("garmin", WellnessDataType.SLEEP, "2026-01-02")


class TestRead:
    def test_returns_none_when_no_file(self, cache: WellnessCache) -> None:
        assert cache.read("garmin", WellnessDataType.SLEEP, "2026-01-01") is None

    def test_returns_written_data(self, cache: WellnessCache) -> None:
        data = {"sleepData": [1, 2, 3]}
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", data)
        result = cache.read("garmin", WellnessDataType.SLEEP, "2026-01-01")
        assert result == data

    def test_preserves_unicode(self, cache: WellnessCache) -> None:
        data = {"name": "h\u00e9ros"}
        cache.write("garmin", WellnessDataType.SLEEP, "key", data)
        result = cache.read("garmin", WellnessDataType.SLEEP, "key")
        assert result == data


class TestWrite:
    def test_creates_parent_directories(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {})
        p = cache._dir / "garmin" / "sleep" / "2026-01-01.json"
        assert p.is_file()

    def test_overwrites_existing(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {"v": 1})
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {"v": 2})
        result = cache.read("garmin", WellnessDataType.SLEEP, "2026-01-01")
        assert result == {"v": 2}

    def test_atomic_no_tmp_left_over(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {})
        tmp = cache._dir / "garmin" / "sleep" / "2026-01-01.tmp"
        assert not tmp.exists()

    def test_valid_json_written(self, cache: WellnessCache) -> None:
        data = {"key": [1, 2, 3], "nested": {"a": True}}
        cache.write("garmin", WellnessDataType.HRV, "key", data)
        p = cache._dir / "garmin" / "hrv" / "key.json"
        loaded = json.loads(p.read_text(encoding="utf-8"))
        assert loaded == data


class TestInvalidate:
    def test_removes_connector_directory(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {})
        cache.write("garmin", WellnessDataType.HRV, "2026-01-01", {})
        cache.invalidate("garmin")
        assert not (cache._dir / "garmin").exists()

    def test_does_not_affect_other_connectors(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {})
        cache.write("strava", WellnessDataType.ATHLETE_STATS, "snapshot", {})
        cache.invalidate("garmin")
        assert cache.has("strava", WellnessDataType.ATHLETE_STATS, "snapshot")

    def test_no_error_when_connector_not_cached(self, cache: WellnessCache) -> None:
        cache.invalidate("garmin")  # Should not raise

    def test_has_returns_false_after_invalidate(self, cache: WellnessCache) -> None:
        cache.write("garmin", WellnessDataType.SLEEP, "2026-01-01", {})
        cache.invalidate("garmin")
        assert not cache.has("garmin", WellnessDataType.SLEEP, "2026-01-01")


class TestKeyHelpers:
    def test_range_key(self) -> None:
        start = date(2026, 1, 1)
        end = date(2026, 12, 31)
        assert WellnessCache.range_key(start, end) == "2026-01-01_2026-12-31"

    def test_daily_key(self) -> None:
        d = date(2026, 6, 15)
        assert WellnessCache.daily_key(d) == "2026-06-15"

    def test_snapshot_key(self) -> None:
        assert WellnessCache.SNAPSHOT_KEY == "snapshot"

    def test_range_key_same_start_end(self) -> None:
        d = date(2026, 1, 1)
        key = WellnessCache.range_key(d, d)
        assert key == "2026-01-01_2026-01-01"

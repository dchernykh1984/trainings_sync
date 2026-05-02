from __future__ import annotations

import dataclasses
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.connectors.base import ActivityMeta
from app.core.cache import ActivityCache, CacheEntry

_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
_DT2 = datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)


def _make_entry(
    external_id: str = "123",
    source_id: str = "garmin-main",
    format: str = "fit",
    start_time: datetime = _DT,
    elapsed_s: int | None = 3600,
) -> CacheEntry:
    return CacheEntry(
        external_id=external_id,
        source_id=source_id,
        format=format,
        start_time=start_time,
        elapsed_s=elapsed_s,
    )


@pytest.fixture
def cache(tmp_path: Path) -> ActivityCache:
    c = ActivityCache(tmp_path)
    c.load()
    return c


class TestCacheEntry:
    def test_filename_defaults_to_empty(self) -> None:
        assert _make_entry().filename == ""

    def test_needs_refresh_defaults_to_false(self) -> None:
        assert _make_entry().needs_refresh is False

    def test_uploaded_to_defaults_to_empty_tuple(self) -> None:
        assert _make_entry().uploaded_to == ()

    def test_is_immutable(self) -> None:
        entry = _make_entry()
        with pytest.raises(dataclasses.FrozenInstanceError):
            entry.needs_refresh = True  # type: ignore[misc]

    def test_raises_on_naive_start_time(self) -> None:
        with pytest.raises(ValueError, match="UTC"):
            _make_entry(start_time=datetime(2026, 1, 1, 8, 0))

    def test_raises_on_non_utc_start_time(self) -> None:
        tz_plus3 = timezone(timedelta(hours=3))
        with pytest.raises(ValueError, match="UTC"):
            _make_entry(start_time=datetime(2026, 1, 1, 8, 0, tzinfo=tz_plus3))

    def test_raises_on_invalid_format(self) -> None:
        with pytest.raises(ValueError, match="format"):
            _make_entry(format="zip")

    def test_raises_on_negative_elapsed_s(self) -> None:
        with pytest.raises(ValueError, match="elapsed_s"):
            _make_entry(elapsed_s=-1)

    def test_normalizes_list_uploaded_to_to_tuple(self) -> None:
        entry = CacheEntry(
            external_id="1",
            source_id="garmin-main",
            format="fit",
            start_time=_DT,
            elapsed_s=None,
            uploaded_to=["strava"],  # type: ignore[arg-type]
        )
        assert isinstance(entry.uploaded_to, tuple)
        assert entry.uploaded_to == ("strava",)


class TestLoad:
    def test_empty_when_no_index(self, tmp_path: Path) -> None:
        cache = ActivityCache(tmp_path)
        cache.load()

        assert cache.all_entries() == []

    def test_restores_entries_from_index(self, tmp_path: Path) -> None:
        cache = ActivityCache(tmp_path)
        cache.load()
        cache.put(_make_entry(), b"content")

        reloaded = ActivityCache(tmp_path)
        reloaded.load()

        assert len(reloaded.all_entries()) == 1
        e = reloaded.all_entries()[0]
        assert e.external_id == "123"
        assert e.source_id == "garmin-main"
        assert e.elapsed_s == 3600

    def test_restores_start_time_with_timezone(self, tmp_path: Path) -> None:
        cache = ActivityCache(tmp_path)
        cache.load()
        cache.put(_make_entry(), b"x")

        reloaded = ActivityCache(tmp_path)
        reloaded.load()

        assert reloaded.all_entries()[0].start_time.tzinfo is not None

    def test_naive_start_time_in_index_gets_utc(self, tmp_path: Path) -> None:
        import json

        (tmp_path / "index.json").write_text(
            json.dumps(
                {
                    "entries": [
                        {
                            "external_id": "1",
                            "source_id": "g",
                            "format": "fit",
                            "start_time": "2026-01-01T08:00:00",  # naive — no +00:00
                            "elapsed_s": None,
                            "filename": "g/x.fit",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        cache = ActivityCache(tmp_path)
        cache.load()

        assert cache.all_entries()[0].start_time.tzinfo is not None

    def _write_index(self, tmp_path: Path, filename: str) -> None:
        (tmp_path / "index.json").write_text(
            json.dumps(
                {
                    "entries": [
                        {
                            "external_id": "1",
                            "source_id": "g",
                            "format": "fit",
                            "start_time": "2026-01-01T08:00:00+00:00",
                            "elapsed_s": None,
                            "filename": filename,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

    def test_raises_on_absolute_filename_in_index(self, tmp_path: Path) -> None:
        self._write_index(tmp_path, "/etc/hosts")
        cache = ActivityCache(tmp_path)
        with pytest.raises(ValueError, match="relative"):
            cache.load()

    def test_raises_on_traversal_filename_in_index(self, tmp_path: Path) -> None:
        self._write_index(tmp_path, "../../etc/passwd")
        cache = ActivityCache(tmp_path)
        with pytest.raises(ValueError, match="escapes"):
            cache.load()

    def test_raises_on_empty_filename_in_index(self, tmp_path: Path) -> None:
        self._write_index(tmp_path, "")
        cache = ActivityCache(tmp_path)
        with pytest.raises(ValueError, match="empty filename"):
            cache.load()

    def test_deduplicates_entries_on_load(self, tmp_path: Path) -> None:
        (tmp_path / "index.json").write_text(
            json.dumps(
                {
                    "entries": [
                        {
                            "external_id": "1",
                            "source_id": "g",
                            "format": "fit",
                            "start_time": "2026-01-01T08:00:00+00:00",
                            "elapsed_s": None,
                            "filename": "g/a.fit",
                        },
                        {
                            "external_id": "1",
                            "source_id": "g",
                            "format": "fit",
                            "start_time": "2026-01-01T08:00:00+00:00",
                            "elapsed_s": None,
                            "filename": "g/b.fit",
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )
        cache = ActivityCache(tmp_path)
        cache.load()

        assert len(cache.all_entries()) == 1
        assert cache.all_entries()[0].filename == "g/a.fit"


class TestSave:
    def test_creates_cache_dir_if_missing(self, tmp_path: Path) -> None:
        nested = tmp_path / "a" / "b"
        cache = ActivityCache(nested)
        cache.load()
        cache.save()

        assert (nested / "index.json").exists()

    def test_writes_valid_json(self, cache: ActivityCache, tmp_path: Path) -> None:
        cache.put(_make_entry(), b"x")

        data = json.loads((tmp_path / "index.json").read_text())
        assert "entries" in data
        assert len(data["entries"]) == 1

    def test_atomic_write_uses_tmp_then_replaces(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        cache.put(_make_entry(), b"x")

        assert (tmp_path / "index.json").exists()
        assert not (tmp_path / "index.tmp").exists()


class TestPut:
    def test_stores_content_on_disk(self, cache: ActivityCache, tmp_path: Path) -> None:
        entry = cache.put(_make_entry(), b"fit-bytes")

        assert (tmp_path / entry.filename).read_bytes() == b"fit-bytes"

    def test_returns_entry_with_filename_set(self, cache: ActivityCache) -> None:
        entry = cache.put(_make_entry(), b"x")

        assert entry.filename != ""
        assert "garmin-main" in entry.filename
        assert "123" in entry.filename
        assert entry.filename.endswith(".fit")

    def test_filename_contains_timestamp(self, cache: ActivityCache) -> None:
        entry = cache.put(_make_entry(), b"x")

        assert "20260101T080000" in entry.filename

    def test_replaces_existing_entry(self, cache: ActivityCache) -> None:
        cache.put(_make_entry(), b"old")
        cache.put(_make_entry(), b"new")

        assert len(cache.all_entries()) == 1

    def test_updates_content_on_replace(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        cache.put(_make_entry(), b"old")
        entry = cache.put(_make_entry(), b"new")

        assert (tmp_path / entry.filename).read_bytes() == b"new"

    def test_content_file_tmp_is_cleaned_up(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = cache.put(_make_entry(), b"x")

        assert not Path(str(tmp_path / entry.filename) + ".tmp").exists()

    def test_preserves_needs_refresh_flag(self, cache: ActivityCache) -> None:
        raw = dataclasses.replace(_make_entry(), needs_refresh=True)
        entry = cache.put(raw, b"x")

        assert entry.needs_refresh is True

    def test_saves_index_after_put(self, cache: ActivityCache, tmp_path: Path) -> None:
        cache.put(_make_entry(), b"x")

        assert (tmp_path / "index.json").exists()

    def test_sanitizes_unsafe_chars_in_ids(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = _make_entry(external_id="/abs/path/123", source_id="my/source")
        result = cache.put(entry, b"x")

        dir_part, file_part = result.filename.split("/", 1)
        assert "/" not in dir_part  # source_id sanitized
        assert "/" not in file_part  # external_id sanitized
        assert (tmp_path / result.filename).exists()

    def test_raises_on_invalid_format(self, cache: ActivityCache) -> None:
        with pytest.raises(ValueError, match="format"):
            _make_entry(format="../../../../outside")

    def test_no_collision_when_ids_sanitize_to_same_name(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        e1 = cache.put(_make_entry(external_id="a/b"), b"first")
        e2 = cache.put(_make_entry(external_id="a_b"), b"second")

        assert e1.filename != e2.filename
        assert (tmp_path / e1.filename).read_bytes() == b"first"
        assert (tmp_path / e2.filename).read_bytes() == b"second"

    def test_raises_on_empty_source_id(self, cache: ActivityCache) -> None:
        entry = _make_entry(source_id="")
        with pytest.raises(ValueError, match="source_id"):
            cache.put(entry, b"x")

    def test_long_external_id_does_not_raise(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = _make_entry(external_id="x" * 280)
        result = cache.put(entry, b"x")
        assert (tmp_path / result.filename).exists()

    def test_long_source_id_does_not_raise(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = _make_entry(source_id="s" * 280)
        result = cache.put(entry, b"x")
        assert (tmp_path / result.filename).exists()

    def test_deletes_old_file_when_filename_changes(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        e1 = cache.put(_make_entry(), b"old")
        old_path = tmp_path / e1.filename
        e2 = cache.put(
            _make_entry(start_time=datetime(2026, 1, 2, 8, 0, tzinfo=timezone.utc)),
            b"new",
        )

        assert not old_path.exists()
        assert (tmp_path / e2.filename).exists()


class TestHas:
    def test_true_when_file_exists_and_not_needs_refresh(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_make_entry(), b"x")

        assert cache.has("123", "garmin-main") is True

    def test_false_when_not_in_cache(self, cache: ActivityCache) -> None:
        assert cache.has("999", "garmin-main") is False

    def test_false_when_needs_refresh(self, cache: ActivityCache) -> None:
        cache.put(_make_entry(), b"x")
        cache.mark_refresh("garmin-main")

        assert cache.has("123", "garmin-main") is False

    def test_false_when_file_deleted(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = cache.put(_make_entry(), b"x")
        (tmp_path / entry.filename).unlink()

        assert cache.has("123", "garmin-main") is False

    def test_false_when_path_is_directory(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = cache.put(_make_entry(), b"x")
        file_path = tmp_path / entry.filename
        file_path.unlink()
        file_path.mkdir()

        assert cache.has("123", "garmin-main") is False


class TestGetEntry:
    def test_returns_entry_when_found(self, cache: ActivityCache) -> None:
        cache.put(_make_entry(), b"x")
        entry = cache.get_entry("123", "garmin-main")

        assert entry is not None
        assert entry.external_id == "123"

    def test_returns_none_when_not_found(self, cache: ActivityCache) -> None:
        assert cache.get_entry("999", "garmin-main") is None

    def test_different_source_returns_none(self, cache: ActivityCache) -> None:
        cache.put(_make_entry(), b"x")

        assert cache.get_entry("123", "strava-main") is None


class TestAllEntries:
    def test_returns_empty_list_initially(self, cache: ActivityCache) -> None:
        assert cache.all_entries() == []

    def test_returns_all_stored_entries(self, cache: ActivityCache) -> None:
        cache.put(_make_entry("1"), b"x")
        cache.put(_make_entry("2"), b"x")

        assert len(cache.all_entries()) == 2

    def test_returns_copy_not_internal_list(self, cache: ActivityCache) -> None:
        cache.put(_make_entry(), b"x")
        result = cache.all_entries()
        result.clear()

        assert len(cache.all_entries()) == 1


class TestReadContent:
    def test_returns_stored_bytes(self, cache: ActivityCache) -> None:
        entry = cache.put(_make_entry(), b"fit-data")

        assert cache.read_content(entry) == b"fit-data"

    def test_raises_on_absolute_filename(self, cache: ActivityCache) -> None:
        entry = dataclasses.replace(_make_entry(), filename="/etc/hosts")
        with pytest.raises(ValueError, match="relative"):
            cache.read_content(entry)


class TestMarkRefresh:
    def test_sets_needs_refresh_for_source(self, cache: ActivityCache) -> None:
        cache.put(_make_entry("1", source_id="garmin-main"), b"x")
        cache.put(_make_entry("2", source_id="garmin-main"), b"x")
        cache.mark_refresh("garmin-main")

        assert all(e.needs_refresh for e in cache.all_entries())

    def test_does_not_affect_other_sources(self, cache: ActivityCache) -> None:
        cache.put(_make_entry("1", source_id="garmin-main"), b"x")
        cache.put(_make_entry("2", source_id="strava-main"), b"x")
        cache.mark_refresh("garmin-main")

        strava = cache.get_entry("2", "strava-main")
        assert strava is not None
        assert strava.needs_refresh is False

    def test_respects_start_date_bound(self, cache: ActivityCache) -> None:
        cache.put(_make_entry("1", start_time=_DT), b"x")  # 2026-01-01
        cache.put(_make_entry("2", start_time=_DT2), b"x")  # 2026-01-02
        cache.mark_refresh("garmin-main", start=date(2026, 1, 2))

        assert cache.get_entry("1", "garmin-main").needs_refresh is False  # type: ignore[union-attr]
        assert cache.get_entry("2", "garmin-main").needs_refresh is True  # type: ignore[union-attr]

    def test_respects_end_date_bound(self, cache: ActivityCache) -> None:
        cache.put(_make_entry("1", start_time=_DT), b"x")  # 2026-01-01
        cache.put(_make_entry("2", start_time=_DT2), b"x")  # 2026-01-02
        cache.mark_refresh("garmin-main", end=date(2026, 1, 1))

        assert cache.get_entry("1", "garmin-main").needs_refresh is True  # type: ignore[union-attr]
        assert cache.get_entry("2", "garmin-main").needs_refresh is False  # type: ignore[union-attr]

    def test_saves_index_after_marking(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        cache.put(_make_entry(), b"x")
        cache.mark_refresh("garmin-main")

        reloaded = ActivityCache(tmp_path)
        reloaded.load()
        assert reloaded.all_entries()[0].needs_refresh is True


class TestMarkUploaded:
    def test_adds_destination_to_uploaded_to(self, cache: ActivityCache) -> None:
        entry = cache.put(_make_entry(), b"x")
        updated = cache.mark_uploaded(entry, "strava-main")

        assert "strava-main" in updated.uploaded_to

    def test_is_idempotent(self, cache: ActivityCache) -> None:
        entry = cache.put(_make_entry(), b"x")
        entry = cache.mark_uploaded(entry, "strava-main")
        updated = cache.mark_uploaded(entry, "strava-main")

        assert updated.uploaded_to.count("strava-main") == 1

    def test_persists_across_reload(self, cache: ActivityCache, tmp_path: Path) -> None:
        entry = cache.put(_make_entry(), b"x")
        cache.mark_uploaded(entry, "strava-main")

        reloaded = ActivityCache(tmp_path)
        reloaded.load()
        assert "strava-main" in reloaded.all_entries()[0].uploaded_to

    def test_does_not_affect_other_entries(self, cache: ActivityCache) -> None:
        e1 = cache.put(_make_entry("1"), b"x")
        cache.put(_make_entry("2"), b"x")
        cache.mark_uploaded(e1, "strava-main")

        e2 = cache.get_entry("2", "garmin-main")
        assert e2 is not None
        assert e2.uploaded_to == ()

    def test_uploaded_to_remains_tuple_after_mark(self, cache: ActivityCache) -> None:
        entry = cache.put(_make_entry(), b"x")
        updated = cache.mark_uploaded(entry, "strava-main")

        assert isinstance(updated.uploaded_to, tuple)

    def test_returns_entry_unchanged_when_not_in_cache(
        self, cache: ActivityCache
    ) -> None:
        entry = _make_entry()  # not put into cache

        result = cache.mark_uploaded(entry, "strava-main")

        assert result is entry

    def test_reads_current_in_memory_state_not_stale_entry(
        self, cache: ActivityCache
    ) -> None:
        entry = cache.put(_make_entry(), b"x")
        cache.mark_refresh("garmin-main")  # sets needs_refresh=True in memory

        cache.mark_uploaded(entry, "strava-main")  # entry has stale needs_refresh=False

        current = cache.get_entry("123", "garmin-main")
        assert current is not None
        assert current.needs_refresh is True  # must not have been overwritten


# ---------------------------------------------------------------------------
# Helpers for FindOverlapping tests
# ---------------------------------------------------------------------------

_BASE = datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc)


def _meta(start_offset_s: int = 0, elapsed_s: int | None = 3600) -> ActivityMeta:
    return ActivityMeta(
        external_id="meta",
        name="Test",
        sport_type="running",
        start_time=_BASE + timedelta(seconds=start_offset_s),
        elapsed_s=elapsed_s,
    )


def _entry_at(
    offset_s: int,
    elapsed_s: int | None = 3600,
    eid: str = "e1",
) -> CacheEntry:
    return CacheEntry(
        external_id=eid,
        source_id="garmin-main",
        format="fit",
        start_time=_BASE + timedelta(seconds=offset_s),
        elapsed_s=elapsed_s,
    )


class TestFindOverlapping:
    # meta = [08:00, 09:00], 3600s

    def test_returns_empty_when_no_entries(self, cache: ActivityCache) -> None:
        assert cache.find_overlapping(_meta()) == []

    def test_returns_entry_that_fully_overlaps(self, cache: ActivityCache) -> None:
        cache.put(_entry_at(0), b"x")  # [08:00, 09:00]

        assert len(cache.find_overlapping(_meta())) == 1

    def test_returns_entry_that_partially_overlaps_from_before(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry_at(-1800), b"x")  # [07:30, 08:30] — overlap 1800s

        assert len(cache.find_overlapping(_meta())) == 1

    def test_returns_entry_that_partially_overlaps_from_after(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry_at(1800), b"x")  # [08:30, 09:30] — overlap 1800s

        assert len(cache.find_overlapping(_meta())) == 1

    def test_returns_empty_when_entry_ends_before_meta_starts(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry_at(-3600), b"x")  # [07:00, 08:00] — touching, not overlapping

        assert cache.find_overlapping(_meta()) == []

    def test_returns_empty_when_entry_starts_after_meta_ends(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry_at(3600), b"x")  # [09:00, 10:00] — touching, not overlapping

        assert cache.find_overlapping(_meta()) == []

    def test_returns_multiple_overlapping_entries(self, cache: ActivityCache) -> None:
        # two short Garmin entries covered by one long meta (Garmin/Strava split)
        cache.put(_entry_at(0, elapsed_s=1800, eid="g1"), b"x")  # [08:00, 08:30]
        cache.put(_entry_at(1800, elapsed_s=1800, eid="g2"), b"x")  # [08:30, 09:00]

        # meta covers [08:00, 09:00]
        result = cache.find_overlapping(_meta())

        assert len(result) == 2

    def test_min_overlap_s_filters_tiny_overlap(self, cache: ActivityCache) -> None:
        # entry starts at 08:59:30 → overlap with meta [08:00, 09:00] is 30s
        cache.put(_entry_at(3570), b"x")  # [08:59:30, 09:59:30]

        # default min_overlap_s=60 → excluded
        assert cache.find_overlapping(_meta()) == []

    def test_min_overlap_s_custom_allows_small_overlap(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry_at(3570), b"x")  # 30s overlap

        assert len(cache.find_overlapping(_meta(), min_overlap_s=30)) == 1

    def test_fallback_when_both_elapsed_none_within_tolerance(
        self, cache: ActivityCache
    ) -> None:
        # meta elapsed_s=None → treated as fallback (3600s)
        # entry starts 30 min after meta → overlap = 30 min ≥ 60s
        cache.put(_entry_at(1800, elapsed_s=None), b"x")

        assert len(cache.find_overlapping(_meta(elapsed_s=None))) == 1

    def test_fallback_when_both_elapsed_none_beyond_tolerance(
        self, cache: ActivityCache
    ) -> None:
        # entry starts 1h after meta → overlap = 0 (touches at boundary)
        cache.put(_entry_at(3600, elapsed_s=None), b"x")

        assert cache.find_overlapping(_meta(elapsed_s=None)) == []

    def test_meta_elapsed_none_entry_has_elapsed(self, cache: ActivityCache) -> None:
        # meta: [08:00, 08:00+fallback], entry: [08:15, 09:15] → clearly overlapping
        cache.put(_entry_at(900, elapsed_s=3600), b"x")  # [08:15, 09:15]

        assert len(cache.find_overlapping(_meta(elapsed_s=None))) == 1

    def test_includes_needs_refresh_entries(self, cache: ActivityCache) -> None:
        cache.put(_entry_at(0), b"x")
        cache.mark_refresh("garmin-main")

        result = cache.find_overlapping(_meta())
        assert len(result) == 1
        assert result[0].needs_refresh is True

    def test_excludes_entry_when_file_missing(
        self, cache: ActivityCache, tmp_path: Path
    ) -> None:
        entry = cache.put(_entry_at(0), b"x")
        (tmp_path / entry.filename).unlink()

        assert cache.find_overlapping(_meta()) == []

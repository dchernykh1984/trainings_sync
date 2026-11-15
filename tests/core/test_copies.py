from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.cache import CacheEntry
from app.core.copies import find_copies

_T0 = datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)


def _entry(
    external_id: str,
    *,
    minutes_later: int = 0,
    elapsed_s: int | None = 3600,
    fmt: str = "fit",
    source_id: str = "garmin",
) -> CacheEntry:
    return CacheEntry(
        external_id=external_id,
        source_id=source_id,
        format=fmt,
        start_time=_T0 + timedelta(minutes=minutes_later),
        elapsed_s=elapsed_s,
    )


def _dropped(*entries: CacheEntry) -> dict[str, str]:
    copies = find_copies(entries, min_overlap_s=60)
    return {drop_id: keep.external_id for (drop_id, _), keep in copies.items()}


def test_the_device_file_is_kept_over_a_conversion() -> None:
    assert _dropped(_entry("1", fmt="gpx"), _entry("2", fmt="tcx")) == {"1": "2"}
    assert _dropped(_entry("1", fmt="tcx"), _entry("2", fmt="fit")) == {"1": "2"}


def test_between_two_files_of_one_format_the_first_upload_is_kept() -> None:
    # 999 was issued before 1000, though it sorts after it as text.
    assert _dropped(_entry("1000"), _entry("999", minutes_later=5)) == {"1000": "999"}


def test_a_second_recording_that_overlaps_half_of_the_longer_is_a_copy() -> None:
    # 08:00-09:00 and 08:30-09:10: 30 minutes shared, half of the hour.
    late = _entry("2", minutes_later=30, elapsed_s=40 * 60)
    assert _dropped(_entry("1"), late) == {"2": "1"}


def test_a_run_inside_a_long_ride_is_not_a_copy() -> None:
    ride = _entry("1", elapsed_s=5 * 3600)
    run = _entry("2", minutes_later=120, elapsed_s=3600)
    assert _dropped(ride, run) == {}


def test_back_to_back_and_tiny_overlaps_are_not_copies() -> None:
    assert _dropped(_entry("1"), _entry("2", minutes_later=60)) == {}
    blip = _entry("2", elapsed_s=30)  # shares only 30 s with the hour
    assert _dropped(_entry("1", elapsed_s=30), blip) == {}


def test_entries_of_unknown_length_are_never_copies() -> None:
    assert _dropped(_entry("1", elapsed_s=None), _entry("2", elapsed_s=None)) == {}


def test_two_sources_never_hold_copies_of_each_other() -> None:
    assert _dropped(_entry("1"), _entry("2", source_id="strava")) == {}


def test_every_extra_copy_points_at_the_best_one() -> None:
    entries = (
        _entry("3", fmt="gpx"),
        _entry("2", fmt="tcx", minutes_later=1),
        _entry("1", fmt="fit", minutes_later=2),
    )
    assert _dropped(*entries) == {"3": "1", "2": "1"}


def test_an_entry_stays_when_its_only_copy_goes() -> None:
    # 1~2 and 2~3, but 1 and 3 share only 20 of 60 minutes: 2 goes for 1,
    # and 3 then has no kept copy left.
    entries = (
        _entry("1"),
        _entry("2", minutes_later=20),
        _entry("3", minutes_later=40),
    )
    assert _dropped(*entries) == {"2": "1"}

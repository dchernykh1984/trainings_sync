from __future__ import annotations

import dataclasses
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.connectors.base import Activity, ActivityMeta, ActivityUnavailableError
from app.connectors.strava import _build_gpx, _build_tcx
from app.core.cache import ActivityCache, CacheEntry
from app.core.copies import FoundCopy, find_copies, plan_removals, remove_copies

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


def test_of_two_recordings_of_one_format_the_longer_is_kept() -> None:
    # A second device started 6 minutes late and stopped an hour early.
    whole = _entry("2", elapsed_s=158 * 60)
    part = _entry("1", minutes_later=6, elapsed_s=97 * 60)
    assert _dropped(whole, part) == {"1": "2"}


def test_a_copy_well_longer_than_the_device_file_is_not_dropped() -> None:
    # The FIT missed the first 20 minutes the TCX holds: neither gives way.
    tcx = _entry("1", fmt="tcx", elapsed_s=134 * 60)
    fit = _entry("2", minutes_later=20, elapsed_s=94 * 60)
    assert _dropped(tcx, fit) == {}
    # A few seconds more is the same recording, and the FIT stays.
    assert _dropped(_entry("1", fmt="gpx", elapsed_s=3605), _entry("2")) == {"1": "2"}


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


# ---------------------------------------------------------------------------
# plan_removals / remove_copies
# ---------------------------------------------------------------------------


@pytest.fixture
def cache(tmp_path: Path) -> ActivityCache:
    c = ActivityCache(tmp_path)
    c.load()
    return c


def _cached(
    cache: ActivityCache,
    external_id: str,
    content: bytes,
    *,
    fmt: str = "fit",
    uploaded_to: tuple[str, ...] = ("icu",),
    source_id: str = "garmin",
) -> CacheEntry:
    entry = _entry(external_id, fmt=fmt, source_id=source_id)
    return cache.put(dataclasses.replace(entry, uploaded_to=uploaded_to), content)


def _service(files: dict[str, bytes], **starts: datetime) -> MagicMock:
    """A service holding one activity per id in `files`, at _T0 unless the id
    is given another start in `starts`."""

    def meta(activity_id: str) -> ActivityMeta:
        start = starts.get(activity_id, _T0)
        return ActivityMeta(
            external_id=activity_id,
            name="",
            sport_type="",
            start_time=start,
            elapsed_s=3600,
        )

    async def download(m: ActivityMeta) -> Activity:
        content = files[m.external_id]
        if content == b"unreadable":
            raise ActivityUnavailableError(m.external_id)
        return Activity(
            external_id=m.external_id,
            name="",
            sport_type="",
            start_time=m.start_time,
            content=content,
            format="fit",
        )

    service = MagicMock()
    service.list_activities = AsyncMock(return_value=[meta(i) for i in files])
    service.download_activity = AsyncMock(side_effect=download)
    service.delete_activity = AsyncMock()
    return service


async def test_the_activity_uploaded_from_the_copy_is_found_by_its_file(
    cache: ActivityCache,
) -> None:
    kept = _cached(cache, "10", b"fit")
    copy = _cached(cache, "20", b"gpx", fmt="gpx")
    service = _service({"i1": b"fit", "i2": b"gpx", "i3": b"another ride"})
    found = await plan_removals(cache, {"icu": service})
    assert found == [FoundCopy("icu", "i2", copy, kept, "i1")]


async def test_every_activity_made_from_the_copy_is_found(
    cache: ActivityCache,
) -> None:
    _cached(cache, "10", b"fit")
    _cached(cache, "20", b"gpx", fmt="gpx")
    service = _service({"i1": b"fit", "i2": b"gpx", "i3": b"gpx"})
    found = await plan_removals(cache, {"icu": service})
    assert [f.activity_id for f in found] == ["i2", "i3"]


async def test_an_activity_two_pairs_share_is_listed_once(
    cache: ActivityCache,
) -> None:
    # Garmin's GPX copy of a ride is Strava's copy byte for byte: each pair
    # finds the same intervals.icu activity.
    _cached(cache, "10", b"fit")
    _cached(cache, "20", b"gpx", fmt="gpx")
    _cached(cache, "s1", b"strava", source_id="strava")
    _cached(cache, "s2", b"gpx", fmt="gpx", source_id="strava")
    service = _service({"i1": b"fit", "i2": b"gpx", "i3": b"strava"})
    found = await plan_removals(cache, {"icu": service})
    assert [f.activity_id for f in found] == ["i2"]


@pytest.mark.parametrize(
    "files",
    [
        {"i2": b"gpx"},  # the kept copy is not there
        {"i1": b"unreadable", "i2": b"gpx"},  # nor can it be read
        {"i1": b"fit"},  # the copy is not there, or is gone already
    ],
)
async def test_a_workout_held_once_is_left_alone(
    cache: ActivityCache, files: dict[str, bytes]
) -> None:
    _cached(cache, "10", b"fit")
    _cached(cache, "20", b"gpx", fmt="gpx")
    assert await plan_removals(cache, {"icu": _service(files)}) == []


async def test_a_copy_whose_kept_one_was_never_uploaded_there_is_left_alone(
    cache: ActivityCache,
) -> None:
    _cached(cache, "10", b"fit", uploaded_to=())
    _cached(cache, "20", b"gpx", fmt="gpx")
    service = _service({"i2": b"gpx"})
    assert await plan_removals(cache, {"icu": service}) == []
    service.download_activity.assert_not_awaited()


async def test_two_copies_of_one_file_are_one_activity_that_stays(
    cache: ActivityCache,
) -> None:
    _cached(cache, "10", b"same")
    _cached(cache, "20", b"same", fmt="gpx")
    assert await plan_removals(cache, {"icu": _service({"i1": b"same"})}) == []


async def test_only_activities_at_the_copy_time_are_downloaded(
    cache: ActivityCache,
) -> None:
    _cached(cache, "10", b"fit")
    _cached(cache, "20", b"gpx", fmt="gpx")
    service = _service(
        {"i1": b"fit", "i2": b"gpx", "i9": b"next day"},
        i9=_T0 + timedelta(days=1),
    )
    await plan_removals(cache, {"icu": service})
    downloaded = [
        c.args[0].external_id for c in service.download_activity.await_args_list
    ]
    assert sorted(downloaded) == ["i1", "i2"]
    first, last = service.list_activities.await_args.args
    assert first < _T0.date() < last


async def test_a_service_no_copy_went_to_is_not_asked(cache: ActivityCache) -> None:
    _cached(cache, "10", b"fit")
    _cached(cache, "20", b"gpx", fmt="gpx")
    icu, other = _service({}), _service({})
    await plan_removals(cache, {"icu": icu, "elsewhere": other})
    icu.list_activities.assert_awaited_once()
    other.list_activities.assert_not_awaited()


async def test_removal_deletes_what_was_found_and_reports_failures(
    cache: ActivityCache,
) -> None:
    kept = _cached(cache, "10", b"fit")
    copy = _cached(cache, "20", b"gpx", fmt="gpx")
    service = _service({})
    service.delete_activity.side_effect = [None, RuntimeError("refused")]
    found = [
        FoundCopy("icu", "i2", copy, kept, "i1"),
        FoundCopy("icu", "i3", copy, kept, "i1"),
    ]
    failed = await remove_copies(found, {"icu": service})
    assert [c.args for c in service.delete_activity.await_args_list] == [
        ("i2",),
        ("i3",),
    ]
    assert [(item.activity_id, str(exc)) for item, exc in failed] == [("i3", "refused")]


# A source whose own activities are cached: Garmin, holding a ride twice
# because trainings-sync once uploaded Strava's copy of it there.


_OWN_GPX = b'<?xml version="1.0"?><gpx version="1.1" creator="trainings-sync"/>'


def _garmin_twins(
    cache: ActivityCache, copy_file: bytes = _OWN_GPX, strava_file: bytes = _OWN_GPX
) -> CacheEntry:
    """Garmin's TCX of a ride, its GPX copy, and the Strava activity that copy
    was uploaded from; the Strava one."""
    _cached(cache, "10", b"tcx", fmt="tcx", uploaded_to=())
    _cached(cache, "20", copy_file, fmt="gpx", uploaded_to=())
    return _cached(
        cache, "s1", strava_file, fmt="gpx", uploaded_to=("garmin",), source_id="strava"
    )


async def test_a_file_trainings_sync_uploaded_into_a_source_is_found(
    cache: ActivityCache,
) -> None:
    strava = _garmin_twins(cache)
    garmin = _service({"10": b"", "20": b""})
    found = await plan_removals(cache, {"garmin": garmin})
    kept = cache.get_entry("10", "garmin")
    assert kept is not None
    assert found == [FoundCopy("garmin", "20", strava, kept, "10")]
    garmin.download_activity.assert_not_awaited()


@pytest.mark.parametrize(("build", "fmt"), [(_build_gpx, "gpx"), (_build_tcx, "tcx")])
async def test_a_file_the_strava_connector_builds_is_known_as_its_own(
    cache: ActivityCache, build: Callable[[ActivityMeta, Any], bytes], fmt: str
) -> None:
    meta = ActivityMeta(external_id="s1", name="", sport_type="Ride", start_time=_T0)
    track = {
        "time": SimpleNamespace(data=[0, 60]),
        "latlng": SimpleNamespace(data=[[51.5, -0.1], [51.51, -0.11]]),
    }
    built = build(meta, track)
    _cached(cache, "10", b"fit", uploaded_to=())
    _cached(cache, "20", built, fmt=fmt, uploaded_to=())
    strava = _cached(
        cache, "s1", built, fmt=fmt, uploaded_to=("garmin",), source_id="strava"
    )
    found = await plan_removals(cache, {"garmin": _service({"10": b"", "20": b""})})
    kept = cache.get_entry("10", "garmin")
    assert kept is not None
    assert found == [FoundCopy("garmin", "20", strava, kept, "10")]


@pytest.mark.parametrize(
    ("copy_file", "strava_file"),
    [
        (_OWN_GPX, b"another file"),  # not what trainings-sync uploaded there
        # A device's own file, in both services byte for byte: Strava's copy
        # was only matched to Garmin's, never uploaded.
        (b'<gpx creator="Edge 530"/>', b'<gpx creator="Edge 530"/>'),
    ],
)
async def test_a_copy_trainings_sync_did_not_upload_is_left_alone(
    cache: ActivityCache, copy_file: bytes, strava_file: bytes
) -> None:
    _garmin_twins(cache, copy_file, strava_file)
    garmin = _service({"10": b"", "20": b""})
    assert await plan_removals(cache, {"garmin": garmin}) == []
    garmin.list_activities.assert_not_awaited()


@pytest.mark.parametrize("listed", [{"10": b""}, {"20": b""}])
async def test_nothing_is_deleted_from_a_source_that_lacks_either_copy(
    cache: ActivityCache, listed: dict[str, bytes]
) -> None:
    _garmin_twins(cache)
    assert await plan_removals(cache, {"garmin": _service(listed)}) == []

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta, ActivityUnavailableError
from app.core.cache import ActivityCache, CacheEntry
from app.core.planner import SourceSpec
from app.core.sync import SyncExecutor


def _make_tracker() -> MagicMock:
    tracker = MagicMock()
    tracker.add_task = AsyncMock()
    tracker.advance = AsyncMock()
    tracker.finish = AsyncMock()
    tracker.fail = AsyncMock()
    return tracker


_UTC = timezone.utc
_T0 = datetime(2026, 1, 1, 8, 0, tzinfo=_UTC)
_START = date(2026, 1, 1)
_END = date(2026, 1, 31)


def _meta(
    external_id: str = "act-1",
    start_time: datetime = _T0,
    elapsed_s: int | None = 3600,
) -> ActivityMeta:
    return ActivityMeta(
        external_id=external_id,
        name="Morning Run",
        sport_type="Run",
        start_time=start_time,
        elapsed_s=elapsed_s,
    )


def _activity(
    external_id: str = "act-1",
    start_time: datetime = _T0,
    elapsed_s: int | None = 3600,
    content: bytes = b"fit-content",
) -> Activity:
    return Activity(
        external_id=external_id,
        name="Morning Run",
        sport_type="Run",
        start_time=start_time,
        elapsed_s=elapsed_s,
        content=content,
        format="fit",
    )


def _gpx_activity(
    external_id: str = "strava-act-1",
    start_time: datetime = _T0,
    elapsed_s: int | None = 3600,
) -> Activity:
    return Activity(
        external_id=external_id,
        name="Morning Run",
        sport_type="Run",
        start_time=start_time,
        elapsed_s=elapsed_s,
        content=b"<gpx/>",
        format="gpx",
    )


def _entry(
    external_id: str = "act-1",
    source_id: str = "garmin",
    start_time: datetime = _T0,
    elapsed_s: int | None = 3600,
    needs_refresh: bool = False,
    uploaded_to: tuple[str, ...] = (),
) -> CacheEntry:
    return CacheEntry(
        external_id=external_id,
        source_id=source_id,
        format="fit",
        start_time=start_time,
        elapsed_s=elapsed_s,
        name="Morning Run",
        sport_type="Run",
        needs_refresh=needs_refresh,
        uploaded_to=uploaded_to,
    )


def _spec(source_id: str = "garmin", priority: int = 1) -> SourceSpec:
    return SourceSpec(source_id=source_id, priority=priority)


def _source_conn(
    metas: list[ActivityMeta] | None = None,
    activity: Activity | None = None,
) -> MagicMock:
    conn = MagicMock()
    conn.list_activities = AsyncMock(return_value=metas or [])
    conn.download_activity = AsyncMock(return_value=activity or _activity())
    return conn


def _dest_conn(existing: list | None = None) -> MagicMock:
    conn = MagicMock()
    conn.upload_activity = AsyncMock()
    conn.list_activities = AsyncMock(return_value=existing or [])
    return conn


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "cache"


@pytest.fixture
def cache(cache_dir: Path) -> ActivityCache:
    c = ActivityCache(cache_dir)
    c.load()
    return c


class TestSyncExecutorInit:
    def test_raises_on_duplicate_source_id(self, cache: ActivityCache) -> None:
        with pytest.raises(ValueError, match="source_id"):
            SyncExecutor(
                sources=[
                    (_spec("garmin"), _source_conn()),
                    (_spec("garmin"), _source_conn()),
                ],
                destinations=[],
                cache=cache,
            )

    def test_raises_on_duplicate_destination_id(self, cache: ActivityCache) -> None:
        dest = _dest_conn()
        with pytest.raises(ValueError, match="destination_id"):
            SyncExecutor(
                sources=[],
                destinations=[("strava", dest), ("strava", dest)],
                cache=cache,
            )

    def test_accepts_valid_config(self, cache: ActivityCache) -> None:
        SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
        )


class TestSyncExecutorDownload:
    async def test_lists_activities_from_all_sources(
        self, cache: ActivityCache
    ) -> None:
        garmin = _source_conn(metas=[])
        strava = _source_conn(metas=[])
        executor = SyncExecutor(
            sources=[(_spec("garmin"), garmin), (_spec("strava", 2), strava)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)
        garmin.list_activities.assert_called_once_with(_START, _END)
        strava.list_activities.assert_called_once_with(_START, _END)

    async def test_downloads_planned_activity_and_caches_it(
        self, cache: ActivityCache
    ) -> None:
        meta = _meta()
        act = _activity()
        conn = _source_conn(metas=[meta], activity=act)
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        conn.download_activity.assert_called_once_with(meta)
        assert cache.has("act-1", "garmin")
        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert cache.read_content(entry) == b"fit-content"

    async def test_cached_entry_stores_name_and_sport_type(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()], activity=_activity())
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert entry.name == "Morning Run"
        assert entry.sport_type == "Run"

    async def test_skips_cached_activity_without_force(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(), b"content")
        conn = _source_conn(metas=[_meta()])
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)
        conn.download_activity.assert_not_called()

    async def test_downloads_cached_activity_with_force(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(), b"content")
        conn = _source_conn(metas=[_meta()], activity=_activity())
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END, force=True)
        conn.download_activity.assert_called_once()

    async def test_redownloads_needs_refresh_entry_and_clears_flag(
        self, cache: ActivityCache
    ) -> None:
        # Persistent refresh: entry marked needs_refresh=True -> executor re-fetches
        # it without force=True; after put() the new entry has needs_refresh=False
        cache.put(_entry(needs_refresh=True), b"old-content")
        fresh = _activity(content=b"new-content")
        conn = _source_conn(metas=[_meta()], activity=fresh)
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        conn.download_activity.assert_called_once()
        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert not entry.needs_refresh
        assert cache.read_content(entry) == b"new-content"


class TestSyncExecutorUpload:
    async def test_uploads_cached_entry_to_destination(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"fit-content")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_called_once()

    async def test_uploaded_activity_has_correct_fields(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"fit-content")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.external_id == "act-1"
        assert uploaded.name == "Morning Run"
        assert uploaded.sport_type == "Run"
        assert uploaded.content == b"fit-content"
        assert uploaded.format == "fit"

    async def test_skips_already_uploaded_entry(self, cache: ActivityCache) -> None:
        cache.put(_entry(source_id="garmin", uploaded_to=("strava",)), b"content")
        dest = _dest_conn()
        dest.has_activity.return_value = True
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_not_called()

    async def test_reupload_when_destination_reports_activity_missing(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin", uploaded_to=("strava",)), b"content")
        dest = _dest_conn()
        dest.has_activity.return_value = False
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_called_once()

    async def test_skips_entry_whose_source_matches_destination(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="strava"), b"content")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("strava"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_not_called()

    async def test_skips_needs_refresh_entry(self, cache: ActivityCache) -> None:
        cache.put(_entry(source_id="garmin", needs_refresh=True), b"content")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_not_called()

    async def test_skips_entry_outside_date_range(self, cache: ActivityCache) -> None:
        outside = datetime(2025, 12, 31, 8, 0, tzinfo=_UTC)
        cache.put(_entry(external_id="old-act", start_time=outside), b"content")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_not_called()

    async def test_skips_entry_with_missing_file(
        self, cache: ActivityCache, cache_dir: Path
    ) -> None:
        stored = cache.put(_entry(source_id="garmin"), b"content")
        (cache_dir / stored.filename).unlink()
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_not_called()

    async def test_marks_entry_as_uploaded(self, cache: ActivityCache) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
        )
        await executor.run(_START, _END)
        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert "strava" in entry.uploaded_to

    async def test_uploads_to_multiple_destinations(self, cache: ActivityCache) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        dest_a = _dest_conn()
        dest_b = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest_a), ("polar", dest_b)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest_a.upload_activity.assert_called_once()
        dest_b.upload_activity.assert_called_once()

    async def test_skips_upload_when_destination_has_overlapping_activity(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        dest = _dest_conn(existing=[_meta()])  # same time window -> overlaps
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_not_called()

    async def test_overlap_skip_marks_entry_as_uploaded(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        dest = _dest_conn(existing=[_meta()])
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert "strava" in entry.uploaded_to

    async def test_dest_list_activities_not_called_when_no_candidates(
        self, cache: ActivityCache
    ) -> None:
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.list_activities.assert_not_called()


class TestSyncExecutorTracking:
    async def test_download_task_created_with_activity_count(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()], activity=_activity())
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        add_calls = {c.args[0]: c for c in tracker.add_task.call_args_list}
        assert "Download garmin activities" in add_calls
        assert add_calls["Download garmin activities"].kwargs["total"] == 1
        download_advances = [
            c
            for c in tracker.advance.call_args_list
            if c.args[0] == "Download garmin activities"
        ]
        assert len(download_advances) == 1
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Download garmin activities" in finish_names

    async def test_no_download_task_when_nothing_to_download(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[])
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        task_names = [c.args[0] for c in tracker.add_task.call_args_list]
        assert not any(n.startswith("Download") for n in task_names)

    async def test_advance_called_once_per_downloaded_activity(
        self, cache: ActivityCache
    ) -> None:
        metas = [_meta("a1"), _meta("a2")]
        activities = [_activity("a1"), _activity("a2")]
        conn = MagicMock()
        conn.list_activities = AsyncMock(return_value=metas)
        conn.download_activity = AsyncMock(side_effect=activities)
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        download_advances = [
            c
            for c in tracker.advance.call_args_list
            if c.args[0] == "Download garmin activities"
        ]
        assert len(download_advances) == 2

    async def test_separate_task_per_source(self, cache: ActivityCache) -> None:
        t_strava = datetime(
            2026, 1, 1, 10, 0, tzinfo=_UTC
        )  # 2h after garmin, no overlap
        garmin_conn = _source_conn(metas=[_meta("g1")], activity=_activity("g1"))
        strava_conn = _source_conn(
            metas=[_meta("s1", start_time=t_strava)],
            activity=_activity("s1", start_time=t_strava),
        )
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", 1), garmin_conn),
                (_spec("strava", 2), strava_conn),
            ],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        task_names = [call.args[0] for call in tracker.add_task.call_args_list]
        assert "Download garmin activities" in task_names
        assert "Download strava activities" in task_names

    async def test_download_task_failed_on_error(self, cache: ActivityCache) -> None:
        conn = MagicMock()
        conn.list_activities = AsyncMock(return_value=[_meta()])
        conn.download_activity = AsyncMock(side_effect=OSError("network error"))
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with pytest.raises(OSError):
            await executor.run(_START, _END)
        tracker.fail.assert_called_once_with(
            "Download garmin activities", error="network error"
        )
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Download garmin activities" not in finish_names

    async def test_unavailable_activity_is_skipped_silently(
        self, cache: ActivityCache
    ) -> None:
        metas = [_meta("a1"), _meta("a2")]
        conn = MagicMock()
        conn.list_activities = AsyncMock(return_value=metas)
        conn.download_activity = AsyncMock(
            side_effect=[ActivityUnavailableError("no streams"), _activity("a2")]
        )
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        assert cache.has("a2", "garmin")
        assert not cache.has("a1", "garmin")
        tracker.fail.assert_not_called()
        advances = [
            c
            for c in tracker.advance.call_args_list
            if c.args[0] == "Download garmin activities"
        ]
        assert len(advances) == 2

    async def test_upload_task_created_per_destination(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        task_names = [call.args[0] for call in tracker.add_task.call_args_list]
        assert "Upload to strava" in task_names

    async def test_upload_task_advance_called_per_activity(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(external_id="a1", source_id="garmin"), b"content-a1")
        cache.put(_entry(external_id="a2", source_id="garmin"), b"content-a2")
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        upload_advances = [
            c for c in tracker.advance.call_args_list if c.args[0] == "Upload to strava"
        ]
        assert len(upload_advances) == 2

    async def test_no_upload_task_when_nothing_to_upload(
        self, cache: ActivityCache
    ) -> None:
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        task_names = [call.args[0] for call in tracker.add_task.call_args_list]
        assert not any(n.startswith("Upload to") for n in task_names)

    async def test_upload_task_failed_on_error(self, cache: ActivityCache) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        dest = MagicMock()
        dest.upload_activity = AsyncMock(side_effect=OSError("upload failed"))
        dest.list_activities = AsyncMock(return_value=[])
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
            tracker=tracker,
        )
        with pytest.raises(OSError):
            await executor.run(_START, _END)
        tracker.fail.assert_called_once_with("Upload to strava", error="upload failed")

    async def test_plan_task_created_when_metas_present(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()], activity=_activity())
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        add_calls = {c.args[0]: c for c in tracker.add_task.call_args_list}
        assert "Sync: plan" in add_calls
        assert add_calls["Sync: plan"].kwargs["total"] == 1
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Sync: plan" in finish_names

    async def test_plan_task_not_created_when_no_metas(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[])
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        task_names = [c.args[0] for c in tracker.add_task.call_args_list]
        assert "Sync: plan" not in task_names

    async def test_collect_uploads_task_created_with_tracker(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        add_calls = {c.args[0]: c for c in tracker.add_task.call_args_list}
        assert "Sync: collect uploads" in add_calls
        assert add_calls["Sync: collect uploads"].kwargs["total"] == 1
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Sync: collect uploads" in finish_names

    async def test_collect_uploads_task_failed_on_error(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"content")
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
            tracker=tracker,
        )
        with (
            pytest.raises(RuntimeError, match="collect boom"),
            patch.object(
                executor, "_collect_uploads", side_effect=RuntimeError("collect boom")
            ),
        ):
            await executor.run(_START, _END)
        fail_calls = {c.args[0]: c for c in tracker.fail.call_args_list}
        assert "Sync: collect uploads" in fail_calls
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Sync: collect uploads" not in finish_names

    async def test_plan_task_failed_on_planner_error(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()])
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with (
            pytest.raises(RuntimeError, match="plan boom"),
            patch.object(
                executor._planner,
                "plan_items",
                side_effect=RuntimeError("plan boom"),
            ),
        ):
            await executor.run(_START, _END)
        fail_calls = {c.args[0]: c for c in tracker.fail.call_args_list}
        assert "Sync: plan" in fail_calls
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Sync: plan" not in finish_names


class TestSyncExecutorUploadPriority:
    async def test_higher_priority_source_wins_upload(
        self, cache: ActivityCache
    ) -> None:
        # hi (priority=1) and lo (priority=2): same interval -> only hi uploaded
        cache.put(_entry(external_id="hi-act", source_id="hi"), b"hi")
        cache.put(_entry(external_id="lo-act", source_id="lo"), b"lo")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("hi", priority=1), _source_conn()),
                (_spec("lo", priority=2), _source_conn()),
            ],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        assert dest.upload_activity.call_count == 1
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.external_id == "hi-act"

    async def test_both_uploaded_when_no_time_overlap(
        self, cache: ActivityCache
    ) -> None:
        # hi: 8:00-9:00, lo: 10:00-11:00 -- no overlap -> both uploaded
        t_hi = _T0
        t_lo = datetime(2026, 1, 1, 10, 0, tzinfo=_UTC)
        cache.put(_entry(external_id="hi-act", source_id="hi", start_time=t_hi), b"hi")
        cache.put(_entry(external_id="lo-act", source_id="lo", start_time=t_lo), b"lo")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("hi", priority=1), _source_conn()),
                (_spec("lo", priority=2), _source_conn()),
            ],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        assert dest.upload_activity.call_count == 2

    async def test_equal_priority_first_source_in_list_wins(
        self, cache: ActivityCache
    ) -> None:
        # source-a and source-b: equal priority, same interval -> source-a wins
        cache.put(_entry(external_id="a-act", source_id="source-a"), b"a")
        cache.put(_entry(external_id="b-act", source_id="source-b"), b"b")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("source-a", priority=1), _source_conn()),
                (_spec("source-b", priority=1), _source_conn()),
            ],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        assert dest.upload_activity.call_count == 1
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.external_id == "a-act"


class TestSyncExecutorStravaSource:
    async def test_strava_activity_downloaded_when_garmin_empty(
        self, cache: ActivityCache
    ) -> None:
        strava_act = _gpx_activity("strava-1")
        garmin_conn = _source_conn(metas=[])
        strava_conn = _source_conn(metas=[_meta("strava-1")], activity=strava_act)
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), garmin_conn),
                (_spec("strava", priority=2), strava_conn),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        strava_conn.download_activity.assert_called_once()
        garmin_conn.download_activity.assert_not_called()
        dest.upload_activity.assert_called_once()
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.format == "gpx"
        assert uploaded.external_id == "strava-1"

    async def test_strava_skipped_when_garmin_has_overlapping_activity(
        self, cache: ActivityCache
    ) -> None:
        # Garmin (priority=1) and Strava (priority=2) both have activity at 8:00-9:00.
        # Planner processes Garmin first; Strava overlaps -> Strava is skipped.
        garmin_conn = _source_conn(
            metas=[_meta("garmin-1")], activity=_activity("garmin-1")
        )
        strava_conn = _source_conn(metas=[_meta("strava-1")])
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), garmin_conn),
                (_spec("strava", priority=2), strava_conn),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        garmin_conn.download_activity.assert_called_once()
        strava_conn.download_activity.assert_not_called()
        assert dest.upload_activity.call_count == 1
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.external_id == "garmin-1"

    async def test_strava_downloaded_when_garmin_activity_nonoverlapping(
        self, cache: ActivityCache
    ) -> None:
        # Garmin at 8:00-9:00, Strava at 10:00-11:00 -> no overlap -> both downloaded.
        t_strava = datetime(2026, 1, 1, 10, 0, tzinfo=_UTC)
        garmin_conn = _source_conn(
            metas=[_meta("garmin-1")], activity=_activity("garmin-1")
        )
        strava_conn = _source_conn(
            metas=[_meta("strava-1", start_time=t_strava)],
            activity=_gpx_activity("strava-1", start_time=t_strava),
        )
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), garmin_conn),
                (_spec("strava", priority=2), strava_conn),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        garmin_conn.download_activity.assert_called_once()
        strava_conn.download_activity.assert_called_once()
        assert dest.upload_activity.call_count == 2

    async def test_strava_activity_cached_after_download(
        self, cache: ActivityCache
    ) -> None:
        strava_act = _gpx_activity("strava-1")
        strava_conn = _source_conn(metas=[_meta("strava-1")], activity=strava_act)
        executor = SyncExecutor(
            sources=[(_spec("strava", priority=1), strava_conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        assert cache.has("strava-1", "strava")
        entry = cache.get_entry("strava-1", "strava")
        assert entry is not None
        assert cache.read_content(entry) == b"<gpx/>"
        assert entry.format == "gpx"

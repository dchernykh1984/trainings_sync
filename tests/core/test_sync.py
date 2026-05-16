from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    MediaItem,
    TransientDownloadError,
)
from app.core.cache import ActivityCache, CacheEntry
from app.core.planner import SourceSpec
from app.core.sync import (
    _DOWNLOAD_ATTEMPTS,
    _RATE_LIMIT_PAUSE_S,
    SyncExecutor,
)


def _make_conn(user_label: str = "", max_concurrent: int = 1) -> MagicMock:
    conn = MagicMock()
    conn.user_label = user_label
    conn._max_concurrent = max_concurrent
    return conn


def _make_tracker() -> MagicMock:
    tracker = MagicMock()
    tracker.add_task = AsyncMock(side_effect=lambda name, **_: name)
    tracker.advance = AsyncMock()
    tracker.finish = AsyncMock()
    tracker.fail = AsyncMock()
    tracker.warn = AsyncMock()
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
    description: str | None = None,
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
        description=description,
    )


def _spec(source_id: str = "garmin", priority: int = 1) -> SourceSpec:
    return SourceSpec(source_id=source_id, priority=priority)


def _source_conn(
    metas: list[ActivityMeta] | None = None,
    activity: Activity | None = None,
) -> MagicMock:
    conn = _make_conn()
    conn.list_activities = AsyncMock(return_value=metas or [])
    conn.download_activity = AsyncMock(return_value=activity or _activity())
    return conn


def _dest_conn(existing: list | None = None) -> MagicMock:
    conn = _make_conn()
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

    async def test_cached_entry_stores_description(self, cache: ActivityCache) -> None:
        act = Activity(
            external_id="act-1",
            name="Morning Run",
            sport_type="Run",
            start_time=_T0,
            elapsed_s=3600,
            content=b"fit-content",
            format="fit",
            description="Felt great today",
        )
        conn = _source_conn(metas=[_meta()], activity=act)
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert entry.description == "Felt great today"

    async def test_media_stored_in_cache_after_download(
        self, cache: ActivityCache
    ) -> None:
        act = Activity(
            external_id="act-1",
            name="Morning Run",
            sport_type="Run",
            start_time=_T0,
            elapsed_s=3600,
            content=b"fit-content",
            format="fit",
            media=(
                MediaItem(content=b"photo-bytes", media_type="photo", caption="Summit"),
            ),
        )
        conn = _source_conn(metas=[_meta()], activity=act)
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        assert cache.has_media(entry)
        result = cache.read_media(entry)
        assert len(result) == 1
        assert result[0].content == b"photo-bytes"
        assert result[0].media_type == "photo"
        assert result[0].caption == "Summit"

    async def test_no_media_stored_when_activity_has_no_media(
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
        assert not cache.has_media(entry)

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

    async def test_download_log_includes_cache_filename(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()], activity=_activity())
        tracker = _make_tracker()
        tracker.sync_logger = MagicMock()
        tracker.sync_logger.info = MagicMock()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)

        entry = cache.get_entry("act-1", "garmin")
        assert entry is not None
        msgs = [c.args[0] for c in tracker.sync_logger.info.call_args_list]
        download_msgs = [m for m in msgs if "[download] garmin:" in m]
        assert download_msgs, "expected at least one download info log line"
        assert any(entry.filename in m for m in download_msgs)


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

    async def test_uploaded_activity_carries_description(
        self, cache: ActivityCache
    ) -> None:
        entry = CacheEntry(
            external_id="act-1",
            source_id="garmin",
            format="fit",
            start_time=_T0,
            elapsed_s=3600,
            name="Morning Run",
            sport_type="Run",
            description="Long climb today",
        )
        cache.put(entry, b"fit-content")
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.description == "Long climb today"

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

    async def test_cached_media_passed_to_uploaded_activity(
        self, cache: ActivityCache
    ) -> None:
        stored = cache.put(_entry(source_id="garmin"), b"fit-content")
        cache.put_media(
            stored,
            [MediaItem(content=b"photo-bytes", media_type="photo", caption="View")],
        )
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert len(uploaded.media) == 1
        assert uploaded.media[0].content == b"photo-bytes"
        assert uploaded.media[0].media_type == "photo"
        assert uploaded.media[0].caption == "View"

    async def test_uploaded_activity_has_empty_media_when_cache_has_none(
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
        assert uploaded.media == ()

    async def test_warns_via_tracker_when_dest_does_not_support_media(
        self, cache: ActivityCache
    ) -> None:
        stored = cache.put(_entry(source_id="garmin"), b"fit-content")
        cache.put_media(stored, [MediaItem(content=b"p", media_type="photo")])
        dest = _dest_conn()
        dest.supports_media_upload = False
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)

        tracker.warn.assert_called_once()
        warn_args = tracker.warn.call_args
        msg = warn_args.args[1]
        assert "act-1" in msg
        assert "not uploaded" in msg
        assert "strava" in msg

    async def test_no_warn_when_no_tracking_and_dest_does_not_support_media(
        self, cache: ActivityCache
    ) -> None:
        stored = cache.put(_entry(source_id="garmin"), b"fit-content")
        cache.put_media(stored, [MediaItem(content=b"p", media_type="photo")])
        dest = _dest_conn()
        dest.supports_media_upload = False
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
            # no tracker, so tracking=None
        )
        # Should complete without error, warning silently dropped
        await executor.run(_START, _END)
        dest.upload_activity.assert_called_once()

    async def test_upload_activity_receives_task_name_when_tracker_present(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry(source_id="garmin"), b"fit-content")
        dest = _dest_conn()
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)

        kwargs = dest.upload_activity.call_args[1]
        assert kwargs.get("task_name") == "Upload to strava"

    async def test_upload_activity_receives_none_task_name_when_no_tracker(
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

        kwargs = dest.upload_activity.call_args[1]
        assert kwargs.get("task_name") is None


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
        conn = _make_conn()
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

    async def test_download_error_warns_task_and_does_not_raise(
        self, cache: ActivityCache
    ) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta()])
        conn.download_activity = AsyncMock(
            side_effect=TransientDownloadError("network error")
        )
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        assert executor.download_failures == 1
        assert conn.download_activity.call_count == _DOWNLOAD_ATTEMPTS
        tracker.fail.assert_not_called()
        tracker.warn.assert_called()
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Download garmin activities" in finish_names

    async def test_unavailable_activity_is_skipped_silently(
        self, cache: ActivityCache
    ) -> None:
        metas = [_meta("a1"), _meta("a2")]
        conn = _make_conn()
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

    async def test_download_succeeds_on_retry(self, cache: ActivityCache) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[TransientDownloadError("transient"), _activity("a1")]
        )
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        assert cache.has("a1", "garmin")
        assert executor.download_failures == 0

    async def test_transient_download_error_is_non_fatal(
        self, cache: ActivityCache
    ) -> None:
        metas = [_meta("a1"), _meta("a2")]
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=metas)
        # a1 exhausts all attempts; a2 succeeds on first try
        conn.download_activity = AsyncMock(
            side_effect=[TransientDownloadError("timeout")] * _DOWNLOAD_ATTEMPTS
            + [_activity("a2")]
        )
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        assert cache.has("a2", "garmin")
        assert not cache.has("a1", "garmin")
        assert executor.download_failures == 1
        tracker.fail.assert_not_called()
        tracker.warn.assert_called()
        advances = [
            c
            for c in tracker.advance.call_args_list
            if c.args[0] == "Download garmin activities"
        ]
        assert len(advances) == 2

    async def test_download_failures_zero_on_clean_run(
        self, cache: ActivityCache
    ) -> None:
        metas = [_meta("a1")]
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=metas)
        conn.download_activity = AsyncMock(return_value=_activity("a1"))
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)
        assert executor.download_failures == 0

    async def test_non_transient_error_propagates_without_retry(
        self, cache: ActivityCache
    ) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(side_effect=ValueError("bad zip"))
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        with pytest.raises(ValueError, match="bad zip"):
            await executor.run(_START, _END)
        assert conn.download_activity.call_count == 1

    async def test_rate_limit_error_waits_fixed_pause_duration(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[RateLimitError("429", retry_after=300.0), _activity("a1")]
        )
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        sleep_calls: list[float] = []
        with patch(
            "asyncio.sleep", new=AsyncMock(side_effect=lambda s: sleep_calls.append(s))
        ):
            await executor.run(_START, _END)
        assert any(abs(s - _RATE_LIMIT_PAUSE_S) < 1.0 for s in sleep_calls)
        assert cache.has("a1", "garmin")

    async def test_rate_limit_error_respects_server_retry_after_when_longer(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        long_retry = _RATE_LIMIT_PAUSE_S + 300.0
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[RateLimitError("429", retry_after=long_retry), _activity("a1")]
        )
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        sleep_calls: list[float] = []
        with patch(
            "asyncio.sleep", new=AsyncMock(side_effect=lambda s: sleep_calls.append(s))
        ):
            await executor.run(_START, _END)
        assert any(abs(s - long_retry) < 1.0 for s in sleep_calls)
        assert cache.has("a1", "garmin")

    async def test_rate_limit_error_defers_progress_advance(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[RateLimitError("429", retry_after=1.0)] * _DOWNLOAD_ATTEMPTS
        )
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        advances = [
            c
            for c in tracker.advance.call_args_list
            if c.args[0] == "Download garmin activities"
        ]
        assert len(advances) == 1  # advance at end of all retries, not on each 429

    async def test_rate_limit_gate_blocks_concurrent_tasks(
        self, cache: ActivityCache
    ) -> None:
        """After 429, tasks wait outside the semaphore for the pause; no extra 429s."""
        from app.connectors.base import RateLimitError

        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1"), _meta("a2")])
        conn.download_activity = AsyncMock(
            side_effect=[
                RateLimitError("429", retry_after=300.0),
                _activity("a1"),
                _activity("a2"),
            ]
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            executor = SyncExecutor(
                sources=[(_spec("garmin"), conn)],
                destinations=[],
                cache=cache,
            )
            await executor.run(_START, _END)
        # Exactly 3 calls: 1 failed + 2 succeeded; the gate prevented extra 429s
        assert conn.download_activity.call_count == 3

    async def test_rate_limit_recheck_blocks_task_queued_at_semaphore(
        self, cache: ActivityCache
    ) -> None:
        """Task queued at sem finds version mismatch after 429 and re-waits."""
        from app.connectors.base import RateLimitError

        real_sleep = asyncio.sleep

        async def mock_sleep(t: float) -> None:
            await real_sleep(0)  # yield once; don't actually wait t seconds

        conn = _make_conn(max_concurrent=1)
        conn.list_activities = AsyncMock(return_value=[_meta("a1"), _meta("a2")])

        first_call = True

        async def yielding_download(meta: ActivityMeta) -> Activity:
            nonlocal first_call
            if first_call:
                first_call = False
                await asyncio.sleep(0)  # yield so a2 queues at semaphore
                raise RateLimitError("429", retry_after=300.0)
            return _activity(meta.external_id)

        conn.download_activity = yielding_download

        with patch("app.core.sync.asyncio.sleep", new=mock_sleep):
            executor = SyncExecutor(
                sources=[(_spec("garmin"), conn)],
                destinations=[],
                cache=cache,
            )
            await executor.run(_START, _END)

        assert cache.has("a1", "garmin")
        assert cache.has("a2", "garmin")

    async def test_rate_limit_error_advances_on_eventual_success(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[RateLimitError("429", retry_after=1.0), _activity("a1")]
        )
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        assert cache.has("a1", "garmin")
        assert executor.download_failures == 0
        advances = [
            c
            for c in tracker.advance.call_args_list
            if c.args[0] == "Download garmin activities"
        ]
        assert len(advances) == 1

    async def test_rate_limit_error_logged_at_warning(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[RateLimitError("429", retry_after=1.0), _activity("a1")]
        )
        log = MagicMock()
        tracker = _make_tracker()
        tracker.sync_logger = log
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        assert log.warning.called
        msg = log.warning.call_args.args[0]
        assert "rate limited" in msg
        assert "download failed" in msg

    async def test_rate_limit_pause_info_logged_once(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        conn = _make_conn(max_concurrent=2)
        conn.list_activities = AsyncMock(return_value=[_meta("a1"), _meta("a2")])
        conn.download_activity = AsyncMock(
            side_effect=[
                RateLimitError("429", retry_after=300.0),
                RateLimitError("429", retry_after=300.0),
                _activity("a1"),
                _activity("a2"),
            ]
        )
        log = MagicMock()
        tracker = _make_tracker()
        tracker.sync_logger = log
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        info_msgs = [call.args[0] for call in log.info.call_args_list]
        pause_msgs = [m for m in info_msgs if "pausing" in m]
        assert len(pause_msgs) == 1
        assert str(int(_RATE_LIMIT_PAUSE_S)) in pause_msgs[0]

    async def test_rate_limit_pause_log_shows_server_retry_after_when_longer(
        self, cache: ActivityCache
    ) -> None:
        from app.connectors.base import RateLimitError

        long_retry = _RATE_LIMIT_PAUSE_S + 300.0
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[RateLimitError("429", retry_after=long_retry), _activity("a1")]
        )
        log = MagicMock()
        tracker = _make_tracker()
        tracker.sync_logger = log
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        info_msgs = [call.args[0] for call in log.info.call_args_list]
        pause_msgs = [m for m in info_msgs if "pausing" in m]
        assert len(pause_msgs) == 1
        assert str(int(long_retry)) in pause_msgs[0]

    async def test_retry_attempt_start_logged_at_info(
        self, cache: ActivityCache
    ) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(
            side_effect=[TransientDownloadError("timeout"), _activity("a1")]
        )
        log = MagicMock()
        tracker = _make_tracker()
        tracker.sync_logger = log
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.run(_START, _END)
        info_msgs = [call.args[0] for call in log.info.call_args_list]
        assert any("attempt 2/3 starting" in m and "a1" in m for m in info_msgs)

    async def test_first_attempt_start_not_logged(self, cache: ActivityCache) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(return_value=_activity("a1"))
        log = MagicMock()
        tracker = _make_tracker()
        tracker.sync_logger = log
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        info_msgs = [call.args[0] for call in log.info.call_args_list]
        assert not any("starting" in m for m in info_msgs)

    async def test_non_transient_error_does_not_trigger_padding(
        self, cache: ActivityCache
    ) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta("a1")])
        conn.download_activity = AsyncMock(side_effect=ValueError("bad zip"))
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
        )
        sleep_calls: list = []
        with patch(
            "asyncio.sleep", new=AsyncMock(side_effect=lambda s: sleep_calls.append(s))
        ):
            with pytest.raises(ValueError):
                await executor.run(_START, _END)
        assert sleep_calls == []

    async def test_cache_write_error_fails_task_and_raises(
        self, cache: ActivityCache
    ) -> None:
        conn = _make_conn()
        conn.list_activities = AsyncMock(return_value=[_meta()])
        conn.download_activity = AsyncMock(return_value=_activity())
        tracker = _make_tracker()
        executor = SyncExecutor(
            sources=[(_spec("garmin"), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with patch.object(cache, "put", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                await executor.run(_START, _END)
        tracker.fail.assert_called_once_with(
            "Download garmin activities", error="disk full"
        )

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
        dest.user_label = ""
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
        assert "Sync: plan downloads garmin" in add_calls
        assert add_calls["Sync: plan downloads garmin"].kwargs["total"] == 1
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Sync: plan downloads garmin" in finish_names

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
        assert "Sync: plan downloads garmin" not in task_names

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
        assert "Sync: plan downloads garmin" in fail_calls
        finish_names = [c.args[0] for c in tracker.finish.call_args_list]
        assert "Sync: plan downloads garmin" not in finish_names


class TestSyncExecutorEntryOverlapsMeta:
    async def test_uploads_when_existing_dest_activity_does_not_overlap(
        self, cache: ActivityCache
    ) -> None:
        # Cache has an entry at 8:00-9:00; destination has existing meta at 11:00-12:00.
        # No overlap -> _entry_overlaps_meta returns False -> entry is uploaded.
        t_existing = datetime(2026, 1, 1, 11, 0, tzinfo=_UTC)
        cache.put(_entry(source_id="garmin"), b"fit-content")
        existing_meta = _meta(
            external_id="dest-1", start_time=t_existing, elapsed_s=3600
        )
        dest = _dest_conn(existing=[existing_meta])
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)
        dest.upload_activity.assert_called_once()


class TestSyncSourceUserLabelFallback:
    async def test_shadowed_entry_skipped_only_winner_uploaded(
        self, cache: ActivityCache
    ) -> None:
        # hi (priority=1) and lo (priority=2) overlap -> lo shadowed by hi.
        # Only hi is uploaded; the run must not raise.
        t0 = _T0
        cache.put(_entry(external_id="hi-act", source_id="hi", start_time=t0), b"hi")
        cache.put(_entry(external_id="lo-act", source_id="lo", start_time=t0), b"lo")
        dest = _dest_conn()
        tracker = _make_tracker()
        tracker.sync_logger = MagicMock()
        tracker.sync_logger.info = MagicMock()
        tracker.sync_logger.debug = MagicMock()
        executor = SyncExecutor(
            sources=[
                (_spec("hi", priority=1), _source_conn()),
                (_spec("lo", priority=2), _source_conn()),
            ],
            destinations=[("strava", dest)],
            cache=cache,
            tracker=tracker,
        )
        await executor.run(_START, _END)
        assert dest.upload_activity.call_count == 1

    async def test_source_user_label_returns_empty_when_not_found(
        self, cache: ActivityCache
    ) -> None:
        # _source_user_label should return "" for an unknown source_id not in
        # sources or destinations. Force this by putting a log and having a shadowed
        # entry whose source doesn't appear anywhere.
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
        )
        result = executor._source_user_label("unknown-source-id")
        assert result == ""

    async def test_source_user_label_found_in_destinations(
        self, cache: ActivityCache
    ) -> None:
        # source_id matches a destination connector id (not a source).
        dest_conn = _dest_conn()
        dest_conn.user_label = "strava-label"
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", dest_conn)],
            cache=cache,
        )
        result = executor._source_user_label("strava")
        assert result == "strava-label"

    async def test_dest_user_label_returns_empty_when_not_found(
        self, cache: ActivityCache
    ) -> None:
        # _dest_user_label returns "" when dest_id is absent from destinations.
        executor = SyncExecutor(
            sources=[(_spec("garmin"), _source_conn())],
            destinations=[("strava", _dest_conn())],
            cache=cache,
        )
        result = executor._dest_user_label("unknown-dest-id")
        assert result == ""


class TestLogUploadDecisionNoDest:
    async def test_log_upload_decision_with_dest_id_none_sets_where_to_empty(
        self, cache: ActivityCache
    ) -> None:
        # When dest_id is None (shadowed activity), _log_upload_decision uses where="".
        t0 = _T0
        cache.put(_entry(external_id="hi-act", source_id="hi", start_time=t0), b"hi")
        cache.put(_entry(external_id="lo-act", source_id="lo", start_time=t0), b"lo")
        dest = _dest_conn()
        tracker = _make_tracker()
        tracker.sync_logger = MagicMock()
        tracker.sync_logger.info = MagicMock()
        tracker.sync_logger.debug = MagicMock()
        executor = SyncExecutor(
            sources=[
                (_spec("hi", priority=1), _source_conn()),
                (_spec("lo", priority=2), _source_conn()),
            ],
            destinations=[("strava", dest)],
            cache=cache,
            tracker=tracker,
        )
        # lo is shadowed by hi, triggering _log_upload_decision with dest_id=None.
        await executor.run(_START, _END)
        msgs = [c.args[0] for c in tracker.sync_logger.debug.call_args_list]
        shadow_msgs = [m for m in msgs if "[upload-plan] lo:" in m]
        assert shadow_msgs, (
            "expected at least one upload-plan debug line for source 'lo'"
        )
        assert any("shadowed by hi" in m for m in shadow_msgs)
        assert not any(" -> " in m for m in shadow_msgs)


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


class TestBorrowedDescriptions:
    async def test_description_from_shadowed_entry_reaches_destination(
        self, cache: ActivityCache
    ) -> None:
        # Garmin has the activity (higher priority, no description).
        # Strava has the same activity with a description (shadowed by Garmin).
        # The description must be passed to the local-folder upload.
        garmin_entry = cache.put(
            _entry("g-1", "garmin", description=None), b"fit-content"
        )
        strava_entry = cache.put(
            _entry("s-1", "strava", description="A tough climb"), b"<gpx/>"
        )
        cache.mark_uploaded(garmin_entry, "local")
        cache.mark_uploaded(strava_entry, "local")

        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), _source_conn()),
                (_spec("strava", priority=2), _source_conn()),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        borrowed = executor._compute_borrowed_descriptions([garmin_entry, strava_entry])

        assert borrowed == {("g-1", "garmin"): "A tough climb"}

    async def test_no_borrow_when_winner_already_has_description(
        self, cache: ActivityCache
    ) -> None:
        garmin_entry = cache.put(
            _entry("g-1", "garmin", description="Already set"), b"fit-content"
        )
        strava_entry = cache.put(
            _entry("s-1", "strava", description="Strava note"), b"<gpx/>"
        )

        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), _source_conn()),
                (_spec("strava", priority=2), _source_conn()),
            ],
            destinations=[],
            cache=cache,
        )
        borrowed = executor._compute_borrowed_descriptions([garmin_entry, strava_entry])

        assert borrowed == {}

    async def test_description_propagated_end_to_end(
        self, cache: ActivityCache
    ) -> None:
        # Simulate a second sync run where both activities are already cached:
        # garmin (higher priority, no description) and strava (shadowed, has
        # description). The garmin entry should be uploaded to local with the
        # strava description.
        cache.put(_entry("g-1", "garmin", description=None), b"fit-content")
        strava_entry = cache.put(
            _entry("s-1", "strava", description="Great walk in the park"), b"<gpx/>"
        )
        cache.mark_uploaded(strava_entry, "garmin")

        garmin_conn = _source_conn(metas=[_meta("g-1")])
        strava_conn = _source_conn(metas=[_meta("s-1")])
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

        dest.upload_activity.assert_called_once()
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.description == "Great walk in the park"


_PHOTO = MediaItem(
    content=b"img", media_type="photo", url="https://cdn.example.com/p1.jpg"
)
_PHOTO2 = MediaItem(
    content=b"img2", media_type="photo", url="https://cdn.example.com/p2.jpg"
)
_PHOTO_NO_URL = MediaItem(content=b"img3", media_type="photo")


class TestComputeBorrowedMedia:
    def _executor(
        self,
        cache: ActivityCache,
        sources: list[tuple[str, int]] | None = None,
    ) -> SyncExecutor:
        if sources is None:
            sources = [("garmin", 1), ("strava", 2)]
        return SyncExecutor(
            sources=[(_spec(sid, priority=p), _source_conn()) for sid, p in sources],
            destinations=[],
            cache=cache,
        )

    def test_borrows_media_from_lower_priority_overlapping_source(
        self, cache: ActivityCache
    ) -> None:
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        strava = cache.put(_entry("s-1", "strava"), b"gpx")
        cache.put_media(strava, [_PHOTO])

        executor = self._executor(cache)
        result = executor._compute_borrowed_media([garmin, strava])

        assert result == {("g-1", "garmin"): [_PHOTO]}

    def test_no_borrow_when_winner_has_media(self, cache: ActivityCache) -> None:
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        strava = cache.put(_entry("s-1", "strava"), b"gpx")
        cache.put_media(garmin, [_PHOTO])
        cache.put_media(strava, [_PHOTO2])

        executor = self._executor(cache)
        result = executor._compute_borrowed_media([garmin, strava])

        assert result == {}

    def test_aggregates_media_from_multiple_overlapping_donors(
        self, cache: ActivityCache
    ) -> None:
        # G1 (priority 1, no media) overlaps both S1 and S2 (priority 2)
        # S1 and S2 are non-overlapping with each other but both overlap G1
        t_g1 = datetime(2026, 1, 1, 8, 0, tzinfo=_UTC)
        t_s1 = datetime(2026, 1, 1, 8, 0, tzinfo=_UTC)
        t_s2 = datetime(2026, 1, 1, 8, 30, tzinfo=_UTC)
        garmin = cache.put(
            _entry("g-1", "garmin", start_time=t_g1, elapsed_s=7200), b"fit"
        )
        s1 = cache.put(_entry("s-1", "strava", start_time=t_s1, elapsed_s=1800), b"gpx")
        s2 = cache.put(_entry("s-2", "strava", start_time=t_s2, elapsed_s=1800), b"gpx")
        cache.put_media(s1, [_PHOTO])
        cache.put_media(s2, [_PHOTO2])

        executor = self._executor(cache)
        result = executor._compute_borrowed_media([garmin, s1, s2])

        assert ("g-1", "garmin") in result
        assert _PHOTO in result[("g-1", "garmin")]
        assert _PHOTO2 in result[("g-1", "garmin")]

    def test_deduplicates_by_non_empty_url(self, cache: ActivityCache) -> None:
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        s1 = cache.put(_entry("s-1", "strava", elapsed_s=1800), b"gpx")
        t_s2 = datetime(2026, 1, 1, 8, 30, tzinfo=_UTC)
        s2 = cache.put(_entry("s-2", "strava", start_time=t_s2, elapsed_s=1800), b"gpx")
        cache.put_media(s1, [_PHOTO])
        cache.put_media(s2, [_PHOTO])  # same URL as s1

        executor = self._executor(cache)
        result = executor._compute_borrowed_media([garmin, s1, s2])

        borrowed = result.get(("g-1", "garmin"), [])
        assert borrowed.count(_PHOTO) == 1

    def test_items_without_url_are_not_deduplicated(self, cache: ActivityCache) -> None:
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        s1 = cache.put(_entry("s-1", "strava", elapsed_s=1800), b"gpx")
        t_s2 = datetime(2026, 1, 1, 8, 30, tzinfo=_UTC)
        s2 = cache.put(_entry("s-2", "strava", start_time=t_s2, elapsed_s=1800), b"gpx")
        cache.put_media(s1, [_PHOTO_NO_URL])
        cache.put_media(s2, [_PHOTO_NO_URL])

        executor = self._executor(cache)
        result = executor._compute_borrowed_media([garmin, s1, s2])

        borrowed = result.get(("g-1", "garmin"), [])
        assert len(borrowed) == 2  # both kept - no URL-based dedup

    def test_no_borrow_from_non_overlapping_source(self, cache: ActivityCache) -> None:
        t_distant = datetime(2026, 1, 1, 20, 0, tzinfo=_UTC)
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        strava = cache.put(
            _entry("s-1", "strava", start_time=t_distant, elapsed_s=3600), b"gpx"
        )
        cache.put_media(strava, [_PHOTO])

        executor = self._executor(cache)
        result = executor._compute_borrowed_media([garmin, strava])

        assert result == {}

    def test_no_borrow_from_higher_priority_source(self, cache: ActivityCache) -> None:
        # Strava priority=1 (higher), garmin priority=2 (lower).
        # Garmin has no media; strava has media but garmin must NOT borrow from
        # a higher-priority source.
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        strava = cache.put(_entry("s-1", "strava"), b"gpx")
        cache.put_media(strava, [_PHOTO])

        executor = SyncExecutor(
            sources=[
                (_spec("strava", priority=1), _source_conn()),
                (_spec("garmin", priority=2), _source_conn()),
            ],
            destinations=[],
            cache=cache,
        )
        result = executor._compute_borrowed_media([garmin, strava])

        assert result == {}

    def test_only_overlapping_donor_contributes(self, cache: ActivityCache) -> None:
        # winner A (garmin), unrelated B (strava, no overlap), overlapping C (local)
        t_distant = datetime(2026, 1, 1, 20, 0, tzinfo=_UTC)
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        b_entry = cache.put(
            _entry("s-1", "strava", start_time=t_distant, elapsed_s=3600), b"gpx"
        )
        c_entry = cache.put(_entry("l-1", "local"), b"fit")
        cache.put_media(b_entry, [_PHOTO])
        cache.put_media(c_entry, [_PHOTO2])

        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), _source_conn()),
                (_spec("strava", priority=2), _source_conn()),
                (_spec("local", priority=3), _source_conn()),
            ],
            destinations=[],
            cache=cache,
        )
        result = executor._compute_borrowed_media([garmin, b_entry, c_entry])

        assert result == {("g-1", "garmin"): [_PHOTO2]}

    async def test_media_propagated_end_to_end(self, cache: ActivityCache) -> None:
        # Garmin (priority 1, no media) wins; Strava (priority 2) has media.
        # Destination upload should carry Strava's photo.
        cache.put(_entry("g-1", "garmin"), b"fit-content")
        strava_entry = cache.put(_entry("s-1", "strava"), b"<gpx/>")
        cache.put_media(strava_entry, [_PHOTO])
        cache.mark_uploaded(strava_entry, "local")

        dest = _dest_conn()
        dest.supports_media_upload = True
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), _source_conn(metas=[_meta("g-1")])),
                (_spec("strava", priority=2), _source_conn(metas=[_meta("s-1")])),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        dest.upload_activity.assert_called_once()
        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.media == (_PHOTO,)

    async def test_winner_own_media_not_overridden_by_lower_priority(
        self, cache: ActivityCache
    ) -> None:
        garmin_entry = cache.put(_entry("g-1", "garmin"), b"fit-content")
        cache.put_media(garmin_entry, [_PHOTO])
        strava_entry = cache.put(_entry("s-1", "strava"), b"<gpx/>")
        cache.put_media(strava_entry, [_PHOTO2])
        cache.mark_uploaded(strava_entry, "local")

        dest = _dest_conn()
        dest.supports_media_upload = True
        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), _source_conn(metas=[_meta("g-1")])),
                (_spec("strava", priority=2), _source_conn(metas=[_meta("s-1")])),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        uploaded: Activity = dest.upload_activity.call_args[0][0]
        assert uploaded.media == (_PHOTO,)

    async def test_split_case_both_winners_get_lower_priority_media(
        self, cache: ActivityCache
    ) -> None:
        # Garmin G1 08:00-09:00 and G2 09:00-10:00 (priority 1, no media).
        # Strava S 08:00-10:00 (priority 2, has media).
        # Both G1 and G2 should be uploaded with Strava's media.
        t_g1 = datetime(2026, 1, 1, 8, 0, tzinfo=_UTC)
        t_g2 = datetime(2026, 1, 1, 9, 0, tzinfo=_UTC)
        t_s = datetime(2026, 1, 1, 8, 0, tzinfo=_UTC)

        cache.put(_entry("g-1", "garmin", start_time=t_g1, elapsed_s=3600), b"fit")
        cache.put(_entry("g-2", "garmin", start_time=t_g2, elapsed_s=3600), b"fit")
        strava = cache.put(
            _entry("s-1", "strava", start_time=t_s, elapsed_s=7200), b"gpx"
        )
        cache.put_media(strava, [_PHOTO])
        cache.mark_uploaded(strava, "local")

        dest = _dest_conn()
        dest.supports_media_upload = True
        executor = SyncExecutor(
            sources=[
                (
                    _spec("garmin", priority=1),
                    _source_conn(metas=[_meta("g-1", t_g1), _meta("g-2", t_g2)]),
                ),
                (
                    _spec("strava", priority=2),
                    _source_conn(metas=[_meta("s-1", t_s, elapsed_s=7200)]),
                ),
            ],
            destinations=[("local", dest)],
            cache=cache,
        )
        await executor.run(_START, _END)

        assert dest.upload_activity.call_count == 2
        for call in dest.upload_activity.call_args_list:
            uploaded: Activity = call[0][0]
            assert uploaded.media == (_PHOTO,), f"{uploaded.external_id} missing media"

    def test_debug_log_emitted_when_sync_logger_present(
        self, cache: ActivityCache
    ) -> None:
        garmin = cache.put(_entry("g-1", "garmin"), b"fit")
        strava = cache.put(_entry("s-1", "strava"), b"gpx")
        cache.put_media(strava, [_PHOTO])

        sync_logger = MagicMock()
        tracker = _make_tracker()
        tracker.sync_logger = sync_logger

        executor = SyncExecutor(
            sources=[
                (_spec("garmin", priority=1), _source_conn()),
                (_spec("strava", priority=2), _source_conn()),
            ],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        executor._compute_borrowed_media([garmin, strava])

        sync_logger.debug.assert_called_once()
        msg = sync_logger.debug.call_args[0][0]
        assert "g-1" in msg
        assert "s-1" in msg
        assert "borrows" in msg


# ---------------------------------------------------------------------------
# Cancellation cleanup
# ---------------------------------------------------------------------------


class TestCancellationCleanup:
    """Tracker tasks must be failed (not left dangling) on BaseException."""

    async def test_download_source_fails_tracker_on_cancelled_error(
        self, cache: ActivityCache
    ) -> None:
        tracker = _make_tracker()

        async def _raise_cancelled(meta: ActivityMeta) -> Activity:
            raise asyncio.CancelledError()

        conn = _source_conn(metas=[_meta()])
        conn.download_activity = _raise_cancelled

        executor = SyncExecutor(
            sources=[(_spec("a", priority=1), conn)],
            destinations=[],
            cache=cache,
            tracker=tracker,
        )
        with pytest.raises(asyncio.CancelledError):
            await executor.run(_START, _END)

        assert tracker.fail.called

    async def test_upload_to_dest_fails_tracker_on_cancelled_error(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry("act-1", "garmin"), b"content")
        tracker = _make_tracker()

        async def _raise_cancelled(
            activity: Activity, *, task_name: str | None = None
        ) -> None:
            raise asyncio.CancelledError()

        dest = _dest_conn()
        dest.upload_activity = _raise_cancelled

        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), _source_conn())],
            destinations=[("d1", dest)],
            cache=cache,
            tracker=tracker,
        )
        with pytest.raises(asyncio.CancelledError):
            await executor.run(_START, _END)

        assert tracker.fail.called


# ---------------------------------------------------------------------------
# Parallelism
# ---------------------------------------------------------------------------


class TestParallelism:
    async def test_sources_listed_in_parallel(self, cache: ActivityCache) -> None:
        call_log: list[str] = []

        async def _list_a(start: date, end: date) -> list[ActivityMeta]:
            call_log.append("a-start")
            await asyncio.sleep(0)
            call_log.append("a-done")
            return []

        async def _list_b(start: date, end: date) -> list[ActivityMeta]:
            call_log.append("b-start")
            await asyncio.sleep(0)
            call_log.append("b-done")
            return []

        conn_a, conn_b = _make_conn(), _make_conn()
        conn_a.list_activities = _list_a
        conn_b.list_activities = _list_b

        executor = SyncExecutor(
            sources=[
                (_spec("a", priority=1), conn_a),
                (_spec("b", priority=2), conn_b),
            ],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        assert call_log == ["a-start", "b-start", "a-done", "b-done"]

    async def test_source_downloads_run_in_parallel(self, cache: ActivityCache) -> None:
        call_log: list[str] = []

        async def _download_a(meta: ActivityMeta) -> Activity:
            call_log.append("a-start")
            await asyncio.sleep(0)
            call_log.append("a-done")
            return _activity(meta.external_id, meta.start_time, meta.elapsed_s)

        async def _download_b(meta: ActivityMeta) -> Activity:
            call_log.append("b-start")
            await asyncio.sleep(0)
            call_log.append("b-done")
            return _activity(meta.external_id, meta.start_time, meta.elapsed_s)

        t_a = _T0
        t_b = _T0 + timedelta(hours=2)
        conn_a = _source_conn(metas=[_meta("a-1", t_a)])
        conn_a.download_activity = _download_a
        conn_b = _source_conn(metas=[_meta("b-1", t_b)])
        conn_b.download_activity = _download_b

        executor = SyncExecutor(
            sources=[
                (_spec("a", priority=1), conn_a),
                (_spec("b", priority=2), conn_b),
            ],
            destinations=[],
            cache=cache,
        )
        await executor.run(_START, _END)

        assert call_log == ["a-start", "b-start", "a-done", "b-done"]

    async def test_destinations_listed_in_parallel(self, cache: ActivityCache) -> None:
        call_log: list[str] = []

        async def _list_d1(start: date, end: date) -> list[ActivityMeta]:
            call_log.append("d1-start")
            await asyncio.sleep(0)
            call_log.append("d1-done")
            return []

        async def _list_d2(start: date, end: date) -> list[ActivityMeta]:
            call_log.append("d2-start")
            await asyncio.sleep(0)
            call_log.append("d2-done")
            return []

        cache.put(_entry("act-1", "garmin"), b"content")

        d1, d2 = _dest_conn(), _dest_conn()
        d1.list_activities = _list_d1
        d2.list_activities = _list_d2

        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), _source_conn())],
            destinations=[("d1", d1), ("d2", d2)],
            cache=cache,
        )
        await executor.run(_START, _END)

        assert call_log == ["d1-start", "d2-start", "d1-done", "d2-done"]

    async def test_uploads_to_destinations_run_in_parallel(
        self, cache: ActivityCache
    ) -> None:
        call_log: list[str] = []

        async def _upload_d1(
            activity: Activity, *, task_name: str | None = None
        ) -> None:
            call_log.append("d1-start")
            await asyncio.sleep(0)
            call_log.append("d1-done")

        async def _upload_d2(
            activity: Activity, *, task_name: str | None = None
        ) -> None:
            call_log.append("d2-start")
            await asyncio.sleep(0)
            call_log.append("d2-done")

        cache.put(_entry("act-1", "garmin"), b"content")

        d1, d2 = _dest_conn(), _dest_conn()
        d1.upload_activity = _upload_d1
        d2.upload_activity = _upload_d2

        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), _source_conn())],
            destinations=[("d1", d1), ("d2", d2)],
            cache=cache,
        )
        await executor.run(_START, _END)

        assert call_log == ["d1-start", "d2-start", "d1-done", "d2-done"]


class TestLoginPipelining:
    async def test_source_list_waits_for_its_login_task(
        self, cache: ActivityCache
    ) -> None:
        call_log: list[str] = []

        async def _login_a() -> None:
            call_log.append("login-a")

        async def _list_a(start: date, end: date) -> list[ActivityMeta]:
            call_log.append("list-a")
            return []

        conn = _make_conn()
        conn.list_activities = _list_a
        login_task: asyncio.Task[None] = asyncio.create_task(_login_a())

        executor = SyncExecutor(
            sources=[(_spec("a", priority=1), conn)],
            destinations=[],
            cache=cache,
            login_tasks={"a": login_task},
        )
        await executor.run(_START, _END)

        assert call_log == ["login-a", "list-a"]

    async def test_dest_list_waits_for_its_login_task(
        self, cache: ActivityCache
    ) -> None:
        call_log: list[str] = []

        async def _login_d() -> None:
            call_log.append("login-d")

        async def _list_d(start: date, end: date) -> list[ActivityMeta]:
            call_log.append("list-d")
            return []

        cache.put(_entry("act-1", "garmin"), b"content")
        dest = _dest_conn()
        dest.list_activities = _list_d
        login_task: asyncio.Task[None] = asyncio.create_task(_login_d())

        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), _source_conn())],
            destinations=[("dest", dest)],
            cache=cache,
            login_tasks={"dest": login_task},
        )
        await executor.run(_START, _END)

        assert call_log == ["login-d", "list-d"]


# ---------------------------------------------------------------------------
# Phase API
# ---------------------------------------------------------------------------


class TestDownloadPhase:
    async def test_downloads_to_cache_without_uploading(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()])
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), conn)],
            destinations=[("local", dest)],
            cache=cache,
        )

        await executor.download_phase(_START, _END)

        assert cache.get_entry("act-1", "garmin") is not None
        dest.upload_activity.assert_not_awaited()

    async def test_download_phase_sets_failures_count(
        self, cache: ActivityCache
    ) -> None:
        conn = _source_conn(metas=[_meta()])
        conn.download_activity = AsyncMock(side_effect=TransientDownloadError("net"))
        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), conn)],
            destinations=[],
            cache=cache,
        )

        with patch("asyncio.sleep", new=AsyncMock()):
            await executor.download_phase(_START, _END)

        assert executor.download_failures == 1
        assert conn.download_activity.call_count == _DOWNLOAD_ATTEMPTS

    async def test_download_failures_zero_after_clean_phase(
        self, cache: ActivityCache
    ) -> None:
        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), _source_conn())],
            destinations=[],
            cache=cache,
        )
        await executor.download_phase(_START, _END)
        assert executor.download_failures == 0


class TestUploadPhase:
    async def test_uploads_from_cache_without_calling_source_connectors(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry("act-1", "garmin"), b"content")
        source_conn = _source_conn()
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), source_conn)],
            destinations=[("local", dest)],
            cache=cache,
        )

        await executor.upload_phase(_START, _END)

        dest.upload_activity.assert_awaited_once()
        source_conn.list_activities.assert_not_awaited()

    async def test_upload_phase_skips_already_uploaded(
        self, cache: ActivityCache
    ) -> None:
        cache.put(_entry("act-1", "garmin", uploaded_to=("local",)), b"content")
        dest = _dest_conn()
        dest.has_activity = MagicMock(return_value=True)
        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), _source_conn())],
            destinations=[("local", dest)],
            cache=cache,
        )

        await executor.upload_phase(_START, _END)

        dest.upload_activity.assert_not_awaited()

    async def test_phases_together_equal_run(self, cache: ActivityCache) -> None:
        """download_phase + upload_phase produces the same result as run()."""
        metas = [_meta("a-1", _T0), _meta("a-2", _T0 + timedelta(hours=2))]
        source_conn = _source_conn(metas=metas)
        source_conn.download_activity = AsyncMock(
            side_effect=lambda m: _activity(m.external_id, m.start_time, m.elapsed_s)
        )
        dest = _dest_conn()
        executor = SyncExecutor(
            sources=[(_spec("garmin", priority=1), source_conn)],
            destinations=[("local", dest)],
            cache=cache,
        )

        await executor.download_phase(_START, _END)
        await executor.upload_phase(_START, _END)

        assert dest.upload_activity.await_count == 2

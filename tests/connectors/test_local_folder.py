from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta, MediaItem
from app.connectors.local_folder import (
    LocalFolderConnector,
    _build_sidecar,
    _read_from_stem_dir,
    _read_sidecar,
    _write_sidecar,
    _write_stem_dir,
)
from app.core.cache import ActivityCache, CacheEntry
from app.parsers.base import ActivityData, ActivityParseError, ActivityParser
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker

_DT = datetime(2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc)
_FIXTURES = Path(__file__).parent.parent / "parsers" / "fixtures"


class _FakeRenderer(ProgressRenderer):
    def on_task_added(self, task: Task) -> None:
        pass

    def on_progress(self, task: Task) -> None:
        pass

    def on_task_done(self, task: Task) -> None:
        pass

    def on_task_failed(self, task: Task) -> None:
        pass

    def on_task_warning(self, task: Task, message: str) -> None:
        pass

    def on_total_updated(self, task: Task) -> None:
        pass


def _make_activity_data(
    start_time: datetime = _DT,
    name: str | None = None,
    sport_type: str | None = "cycling",
) -> ActivityData:
    return ActivityData(start_time=start_time, name=name, sport_type=sport_type)


def _make_parser(
    return_value: ActivityData | None = None,
    side_effect: list | None = None,
) -> ActivityParser:
    mock: ActivityParser = MagicMock(spec=ActivityParser)
    if side_effect is not None:
        mock.parse.side_effect = side_effect  # type: ignore[attr-defined]
    else:
        mock.parse.return_value = return_value or _make_activity_data()  # type: ignore[attr-defined]
    return mock


def _make_connector(
    folder: Path,
    tracker: TaskTracker,
    parsers: dict[str, ActivityParser] | None = None,
) -> LocalFolderConnector:
    return LocalFolderConnector(folder=folder, tracker=tracker, parsers=parsers)


def _make_meta(path: Path) -> ActivityMeta:
    return ActivityMeta(
        external_id=str(path),
        name="Ride",
        sport_type="cycling",
        start_time=_DT,
    )


def _make_activity(external_id: str = "12345") -> Activity:
    return Activity(
        external_id=external_id,
        name="Ride",
        sport_type="cycling",
        start_time=_DT,
        content=b"fit-bytes",
        format="fit",
    )


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def tracker_with_log() -> TaskTracker:
    sync_logger = MagicMock()
    sync_logger.info = MagicMock()
    return TaskTracker(_FakeRenderer(), sync_logger=sync_logger)


@pytest.fixture
def folder(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def connector(folder: Path, tracker: TaskTracker) -> LocalFolderConnector:
    return _make_connector(folder, tracker)


class TestLogin:
    async def test_task_done_on_success(
        self, connector: LocalFolderConnector, tracker: TaskTracker
    ) -> None:
        await connector.login()

        task = next(iter(tracker.tasks.values()))
        assert task.status == TaskStatus.DONE

    async def test_raises_when_folder_missing(
        self, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        connector = _make_connector(tmp_path / "nonexistent", tracker)

        with pytest.raises(FileNotFoundError, match="nonexistent"):
            await connector.login()

    async def test_task_failed_when_folder_missing(
        self, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        connector = _make_connector(tmp_path / "nonexistent", tracker)

        with pytest.raises(FileNotFoundError):
            await connector.login()

        task = next(iter(tracker.tasks.values()))
        assert task.status == TaskStatus.FAILED


class TestListActivities:
    async def test_returns_meta_for_file_in_range(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "ride.fit").write_bytes(b"fake")
        connector = _make_connector(folder, tracker, {".fit": _make_parser()})

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1
        assert result[0].sport_type == "cycling"
        assert result[0].start_time == _DT

    async def test_external_id_is_absolute_path(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        path = folder / "ride.fit"
        path.write_bytes(b"fake")
        connector = _make_connector(folder, tracker, {".fit": _make_parser()})

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result[0].external_id == str(path)

    async def test_name_empty_string_when_none(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "ride.fit").write_bytes(b"fake")
        connector = _make_connector(
            folder, tracker, {".fit": _make_parser(_make_activity_data(name=None))}
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result[0].name == ""

    async def test_sport_type_empty_string_when_none(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "ride.fit").write_bytes(b"fake")
        connector = _make_connector(
            folder,
            tracker,
            {".fit": _make_parser(_make_activity_data(sport_type=None))},
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result[0].sport_type == ""

    async def test_skips_unknown_extensions(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        (folder / "notes.txt").write_bytes(b"hello")
        (folder / "data.csv").write_bytes(b"a,b")

        result = await connector.list_activities(date(2026, 1, 1), date(2026, 12, 31))

        assert result == []

    async def test_skips_unparseable_file_but_returns_good(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "a_bad.fit").write_bytes(b"corrupt")
        (folder / "b_good.fit").write_bytes(b"fake")
        parser = _make_parser(
            side_effect=[ActivityParseError("bad header"), _make_activity_data()]
        )
        connector = _make_connector(folder, tracker, {".fit": parser})

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1

    async def test_warning_emitted_for_unparseable_file(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "bad.fit").write_bytes(b"corrupt")
        parser = _make_parser(side_effect=[ActivityParseError("corrupt header")])
        connector = _make_connector(folder, tracker, {".fit": parser})

        await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        task = next(t for t in tracker.tasks.values() if "scan" in t.name)
        assert len(task.warnings) == 1
        assert "bad.fit" in task.warnings[0]

    async def test_filters_by_date_range(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "a_may.fit").write_bytes(b"fake")
        (folder / "b_march.fit").write_bytes(b"fake")
        may_data = _make_activity_data(
            start_time=datetime(2026, 5, 1, 8, 0, tzinfo=timezone.utc)
        )
        march_data = _make_activity_data(
            start_time=datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)
        )
        connector = _make_connector(
            folder, tracker, {".fit": _make_parser(side_effect=[may_data, march_data])}
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 31))

        assert len(result) == 1
        assert result[0].start_time.date() == date(2026, 5, 1)

    async def test_scan_task_done_after_list(
        self, connector: LocalFolderConnector, tracker: TaskTracker
    ) -> None:
        await connector.list_activities(date(2026, 1, 1), date(2026, 12, 31))

        task = next(t for t in tracker.tasks.values() if "scan" in t.name)
        assert task.status == TaskStatus.DONE

    async def test_scan_task_failed_on_unexpected_error(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        (folder / "ride.fit").write_bytes(b"content")
        parser = _make_parser(side_effect=[RuntimeError("disk error")])
        connector = _make_connector(folder, tracker, {".fit": parser})

        with pytest.raises(RuntimeError, match="disk error"):
            await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        task = next(t for t in tracker.tasks.values() if "scan" in t.name)
        assert task.status == TaskStatus.FAILED

    async def test_returns_empty_for_empty_folder(
        self, connector: LocalFolderConnector
    ) -> None:
        result = await connector.list_activities(date(2026, 1, 1), date(2026, 12, 31))

        assert result == []


class TestDownloadActivity:
    async def test_returns_file_bytes(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        path = folder / "ride.fit"
        path.write_bytes(b"fit-content")

        result = await connector.download_activity(_make_meta(path))

        assert result.content == b"fit-content"

    async def test_format_derived_from_extension(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        for ext, expected in [(".fit", "fit"), (".gpx", "gpx"), (".tcx", "tcx")]:
            path = folder / f"ride{ext}"
            path.write_bytes(b"content")
            result = await connector.download_activity(_make_meta(path))
            assert result.format == expected

    async def test_preserves_meta_fields(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        path = folder / "ride.fit"
        path.write_bytes(b"content")
        meta = ActivityMeta(
            external_id=str(path),
            name="Morning Ride",
            sport_type="cycling",
            start_time=_DT,
        )

        result = await connector.download_activity(meta)

        assert result.name == "Morning Ride"
        assert result.sport_type == "cycling"
        assert result.start_time == _DT
        assert result.external_id == str(path)

    async def test_preserves_elapsed_s(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        path = folder / "ride.fit"
        path.write_bytes(b"content")
        meta = ActivityMeta(
            external_id=str(path),
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            elapsed_s=3600,
        )

        result = await connector.download_activity(meta)

        assert result.elapsed_s == 3600

    async def test_elapsed_s_none_when_meta_has_no_duration(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        path = folder / "ride.fit"
        path.write_bytes(b"content")

        result = await connector.download_activity(_make_meta(path))

        assert result.elapsed_s is None


class TestHasActivity:
    def test_returns_true_when_file_exists(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        (folder / "20260501T082855_12345.fit").write_bytes(b"content")
        assert connector.has_activity("12345", "garmin") is True

    def test_returns_false_when_file_missing(
        self, connector: LocalFolderConnector
    ) -> None:
        assert connector.has_activity("12345", "garmin") is False

    def test_matches_any_format_extension(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        (folder / "20260501T082855_12345.gpx").write_bytes(b"content")
        assert connector.has_activity("12345", "garmin") is True

    def test_does_not_match_partial_id(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        (folder / "20260501T082855_123456.fit").write_bytes(b"content")
        assert connector.has_activity("12345", "garmin") is False

    def test_returns_true_when_external_id_stem_contains_underscore(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        (folder / "20260501T082855_20260501T082855_12345.fit").write_bytes(b"content")
        assert (
            connector.has_activity("/some/folder/20260501T082855_12345.fit", "garmin")
            is True
        )

    def test_ignores_non_activity_extensions(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        (folder / "notes_12345.txt").write_bytes(b"content")
        assert connector.has_activity("12345", "garmin") is False


class TestUploadActivity:
    async def test_writes_bytes_to_folder(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        await connector.upload_activity(_make_activity())

        files = list(folder.iterdir())
        assert len(files) == 1
        assert files[0].read_bytes() == b"fit-bytes"

    async def test_filename_includes_timestamp_and_id(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        await connector.upload_activity(_make_activity(external_id="12345"))

        files = list(folder.iterdir())
        assert files[0].name == "20260501T082855_12345.fit"

    async def test_filename_uses_stem_when_id_is_path(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity = Activity(
            external_id="/some/folder/garmin_cycling.fit",
            name="",
            sport_type="cycling",
            start_time=_DT,
            content=b"bytes",
            format="fit",
        )

        await connector.upload_activity(activity)

        files = list(folder.iterdir())
        assert files[0].name == "20260501T082855_garmin_cycling.fit"

    async def test_correct_file_extension(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity = Activity(
            external_id="42",
            name="",
            sport_type="",
            start_time=_DT,
            content=b"bytes",
            format="gpx",
        )

        await connector.upload_activity(activity)

        files = list(folder.iterdir())
        assert files[0].suffix == ".gpx"


class TestLocalFolderConnectorIntegration:
    async def test_list_with_real_fit_file(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        src = _FIXTURES / "garmin_cycling.fit"
        (folder / "garmin_cycling.fit").write_bytes(src.read_bytes())

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1
        assert result[0].sport_type == "cycling"
        assert result[0].start_time == datetime(
            2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc
        )

    async def test_list_with_real_gpx_file(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        src = _FIXTURES / "garmin_cycling.gpx"
        (folder / "garmin_cycling.gpx").write_bytes(src.read_bytes())

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1
        assert result[0].start_time == datetime(
            2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc
        )

    async def test_upload_then_list(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        content = (_FIXTURES / "garmin_cycling.fit").read_bytes()
        activity = Activity(
            external_id="42",
            name="",
            sport_type="cycling",
            start_time=datetime(2026, 5, 1, 8, 28, 55, tzinfo=timezone.utc),
            content=content,
            format="fit",
        )

        await connector.upload_activity(activity)
        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1
        assert result[0].sport_type == "cycling"

    async def test_download_returns_original_bytes(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        content = (_FIXTURES / "garmin_cycling.fit").read_bytes()
        path = folder / "garmin_cycling.fit"
        path.write_bytes(content)
        meta = _make_meta(path)

        result = await connector.download_activity(meta)

        assert result.content == content
        assert result.format == "fit"

    async def test_download_reads_description_from_sidecar(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        path = folder / "activity.fit"
        path.write_bytes(b"fit-bytes")
        path.with_suffix(".json").write_text(
            json.dumps({"description": "Morning ride notes"}), encoding="utf-8"
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.description == "Morning ride notes"

    async def test_download_description_none_when_no_sidecar(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        path = folder / "activity.fit"
        path.write_bytes(b"fit-bytes")

        result = await connector.download_activity(_make_meta(path))

        assert result.description is None


def _make_cache_entry(
    cache: ActivityCache,
    *,
    dest_id: str,
    start_time: datetime = _DT,
    elapsed_s: int | None = 1800,
    external_id: str = "42",
    source_id: str = "garmin-main",
    name: str = "Morning Ride",
    sport_type: str = "cycling",
    local_path: str | None = None,
) -> CacheEntry:
    entry = CacheEntry(
        external_id=external_id,
        source_id=source_id,
        format="fit",
        start_time=start_time,
        elapsed_s=elapsed_s,
        name=name,
        sport_type=sport_type,
    )
    entry = cache.put(entry, b"fit-bytes")
    return cache.mark_uploaded(entry, dest_id, local_path=local_path)


class TestListActivitiesWithCache:
    async def test_returns_meta_from_cache_not_file_system(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(cache, dest_id="local-dest")
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1
        assert result[0].sport_type == "cycling"
        assert result[0].name == "Morning Ride"

    async def test_date_range_excludes_out_of_range(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(
            cache,
            dest_id="local-dest",
            start_time=datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc),
        )
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 31))

        assert result == []

    async def test_skips_entry_when_local_file_missing(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        folder = tmp_path / "output"
        folder.mkdir()
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(
            cache,
            dest_id="local-dest",
            local_path=str(folder / "nonexistent.fit"),
        )
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result == []

    async def test_includes_entry_without_local_path_backward_compat(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        # Entries uploaded before local_paths was introduced have no local_path.
        # list_activities must trust the uploaded_to flag in that case.
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(cache, dest_id="local-dest", local_path=None)
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1

    async def test_includes_elapsed_s_from_cache(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(cache, dest_id="local-dest", elapsed_s=3600)
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result[0].elapsed_s == 3600

    async def test_excludes_entry_uploaded_to_other_dest(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(cache, dest_id="other-dest")
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result == []

    async def test_does_not_parse_files_on_disk(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(cache, dest_id="local-dest")
        folder = tmp_path / "output"
        folder.mkdir()
        (folder / "extra.fit").write_bytes(b"content")
        mock_parser = _make_parser()
        connector = LocalFolderConnector(
            folder=folder,
            tracker=tracker,
            cache=cache,
            dest_id="local-dest",
            parsers={".fit": mock_parser},
        )

        await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        mock_parser.parse.assert_not_called()  # type: ignore[attr-defined]


class TestHasActivityWithCache:
    def test_returns_true_when_file_exists_at_stored_path(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        local_file = tmp_path / "output" / "ride.fit"
        local_file.parent.mkdir()
        local_file.write_bytes(b"content")
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        entry = cache.put(
            CacheEntry(
                external_id="42",
                source_id="garmin-main",
                format="fit",
                start_time=_DT,
                elapsed_s=None,
            ),
            b"fit-bytes",
        )
        cache.mark_uploaded(entry, "local-dest", local_path=str(local_file))
        connector = LocalFolderConnector(
            folder=tmp_path / "output",
            tracker=tracker,
            cache=cache,
            dest_id="local-dest",
        )

        assert connector.has_activity("42", "garmin-main") is True

    def test_returns_false_when_stored_file_missing(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        folder = tmp_path / "output"
        folder.mkdir()
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        entry = cache.put(
            CacheEntry(
                external_id="42",
                source_id="garmin-main",
                format="fit",
                start_time=_DT,
                elapsed_s=None,
            ),
            b"fit-bytes",
        )
        cache.mark_uploaded(entry, "local-dest", local_path=str(folder / "missing.fit"))
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        assert connector.has_activity("42", "garmin-main") is False

    def test_returns_false_when_no_cache_entry(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        folder = tmp_path / "output"
        folder.mkdir()
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        assert connector.has_activity("42", "garmin-main") is False


class TestUploadActivityReturnsPath:
    async def test_returns_absolute_path_string(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        result = await connector.upload_activity(_make_activity())

        assert isinstance(result, str)
        assert result == str(folder / "20260501T082855_12345.fit")
        assert Path(result).is_file()


class TestUploadActivitySidecar:
    async def test_meta_json_written_when_description_present(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        activity = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            description="Tough climb today",
        )
        await connector.upload_activity(activity)

        meta_json = folder / "20260501T082855_12345" / "meta.json"
        assert meta_json.is_file()
        assert json.loads(meta_json.read_text())["description"] == "Tough climb today"

    async def test_no_stem_dir_when_no_description_and_no_media(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        await connector.upload_activity(_make_activity())

        assert not (folder / "20260501T082855_12345").exists()

    async def test_stale_stem_dir_cleared_on_reupload_without_description_or_media(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity_with = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            description="Old note",
        )
        await connector.upload_activity(activity_with)
        stem_dir = folder / "20260501T082855_12345"
        assert stem_dir.exists()

        await connector.upload_activity(_make_activity())

        assert not stem_dir.exists()

    async def test_legacy_flat_sidecar_removed_on_reupload(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        dest = folder / "20260501T082855_12345.fit"
        dest.write_bytes(b"old-bytes")
        dest.with_suffix(".json").write_text(
            json.dumps({"description": "Legacy note"}), encoding="utf-8"
        )

        await connector.upload_activity(_make_activity())

        assert not dest.with_suffix(".json").exists()


class TestWriteSidecar:
    def test_cleans_up_tmp_and_reraises_on_failure(self, tmp_path: Path) -> None:
        sidecar = tmp_path / "activity.json"
        with (
            patch.object(Path, "replace", side_effect=OSError("disk full")),
            pytest.raises(OSError, match="disk full"),
        ):
            _write_sidecar(sidecar, '{"description": "x"}')

        assert not (tmp_path / "activity.tmp").exists()

    def test_writes_content_on_success(self, tmp_path: Path) -> None:
        import json

        sidecar = tmp_path / "activity.json"
        _write_sidecar(sidecar, '{"description": "hello"}')

        assert json.loads(sidecar.read_text())["description"] == "hello"
        assert not (tmp_path / "activity.tmp").exists()


class TestReadSidecar:
    def test_returns_empty_dict_when_no_sidecar(self, tmp_path: Path) -> None:
        path = tmp_path / "activity.fit"
        path.write_bytes(b"")
        assert _read_sidecar(path) == {}

    def test_returns_parsed_dict_when_sidecar_exists(self, tmp_path: Path) -> None:
        import json

        path = tmp_path / "activity.fit"
        path.write_bytes(b"")
        path.with_suffix(".json").write_text(
            json.dumps({"description": "Great ride"}), encoding="utf-8"
        )
        assert _read_sidecar(path) == {"description": "Great ride"}

    def test_reads_sidecar_with_same_stem(self, tmp_path: Path) -> None:
        path = tmp_path / "20260101T080000_act.gpx"
        path.write_bytes(b"")
        path.with_suffix(".json").write_text('{"description": "x"}', encoding="utf-8")
        result = _read_sidecar(path)
        assert result["description"] == "x"


class TestBuildSidecar:
    def test_returns_none_when_description_is_none(self) -> None:
        assert _build_sidecar(None) is None

    def test_returns_json_with_description_when_set(self) -> None:
        import json

        payload = _build_sidecar("Great climb")
        assert payload is not None
        assert json.loads(payload)["description"] == "Great climb"

    def test_preserves_unicode_characters(self) -> None:
        import json

        text = "caf\xe9 ride"
        payload = _build_sidecar(text)
        assert payload is not None
        assert json.loads(payload)["description"] == text


class TestReadFromStemDir:
    def test_returns_description_from_meta_json(self, tmp_path: Path) -> None:
        import json

        path = tmp_path / "activity.fit"
        path.write_bytes(b"")
        stem_dir = tmp_path / "activity"
        stem_dir.mkdir()
        (stem_dir / "meta.json").write_text(
            json.dumps({"description": "Great ride"}), encoding="utf-8"
        )

        description, media_refs = _read_from_stem_dir(path)

        assert description == "Great ride"
        assert media_refs == []

    def test_returns_media_refs_from_media_json(self, tmp_path: Path) -> None:
        import json

        path = tmp_path / "activity.fit"
        path.write_bytes(b"")
        stem_dir = tmp_path / "activity"
        stem_dir.mkdir()
        refs = [{"file": "photo_1.jpg", "type": "photo"}]
        (stem_dir / "media.json").write_text(json.dumps(refs), encoding="utf-8")

        _, media_refs = _read_from_stem_dir(path)

        assert media_refs == refs

    def test_returns_empty_when_no_stem_dir_and_no_legacy_sidecar(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "activity.fit"
        path.write_bytes(b"")

        description, media_refs = _read_from_stem_dir(path)

        assert description is None
        assert media_refs == []

    def test_legacy_sidecar_fallback_when_no_stem_dir(self, tmp_path: Path) -> None:
        import json

        path = tmp_path / "activity.fit"
        path.write_bytes(b"")
        path.with_suffix(".json").write_text(
            json.dumps({"description": "Legacy note"}), encoding="utf-8"
        )

        description, media_refs = _read_from_stem_dir(path)

        assert description == "Legacy note"
        assert media_refs == []

    def test_returns_none_and_empty_when_stem_dir_has_no_files(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "activity.fit"
        path.write_bytes(b"")
        (tmp_path / "activity").mkdir()

        description, media_refs = _read_from_stem_dir(path)

        assert description is None
        assert media_refs == []


class TestWriteStemDir:
    def test_creates_stem_dir_with_meta_json_when_description_set(
        self, tmp_path: Path
    ) -> None:
        import json

        stem_dir = tmp_path / "act"
        _write_stem_dir(stem_dir, "Great day", ())

        assert (stem_dir / "meta.json").exists()
        assert (
            json.loads((stem_dir / "meta.json").read_text())["description"]
            == "Great day"
        )

    def test_does_not_create_stem_dir_when_nothing_to_store(
        self, tmp_path: Path
    ) -> None:
        stem_dir = tmp_path / "act"
        _write_stem_dir(stem_dir, None, ())

        assert not stem_dir.exists()

    def test_creates_media_files_and_media_json(self, tmp_path: Path) -> None:
        import json

        stem_dir = tmp_path / "act"
        _write_stem_dir(
            stem_dir, None, (MediaItem(content=b"photo-data", media_type="photo"),)
        )

        assert (stem_dir / "photo_1.jpg").read_bytes() == b"photo-data"
        refs = json.loads((stem_dir / "media.json").read_text())
        assert refs == [{"file": "photo_1.jpg", "type": "photo"}]

    def test_caption_included_in_media_json(self, tmp_path: Path) -> None:
        import json

        stem_dir = tmp_path / "act"
        _write_stem_dir(
            stem_dir,
            None,
            (MediaItem(content=b"photo-data", media_type="photo", caption="Summit"),),
        )

        refs = json.loads((stem_dir / "media.json").read_text())
        assert refs[0]["caption"] == "Summit"

    def test_removes_stale_stem_dir_before_writing(self, tmp_path: Path) -> None:
        stem_dir = tmp_path / "act"
        stem_dir.mkdir()
        (stem_dir / "old_photo.jpg").write_bytes(b"stale")

        _write_stem_dir(stem_dir, "New description", ())

        assert not (stem_dir / "old_photo.jpg").exists()

    def test_no_media_json_when_no_media(self, tmp_path: Path) -> None:
        stem_dir = tmp_path / "act"
        _write_stem_dir(stem_dir, "Description only", ())

        assert not (stem_dir / "media.json").exists()


class TestDownloadActivityMedia:
    async def test_media_empty_when_no_media_in_stem_dir(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        path = folder / "ride.fit"
        path.write_bytes(b"content")

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()

    async def test_media_populated_from_stem_dir(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "photo_1.jpg").write_bytes(b"photo-data")
        (stem_dir / "media.json").write_text(
            json.dumps([{"file": "photo_1.jpg", "type": "photo"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert len(result.media) == 1
        assert result.media[0].content == b"photo-data"
        assert result.media[0].media_type == "photo"

    async def test_media_caption_restored(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "photo_1.jpg").write_bytes(b"photo-data")
        (stem_dir / "media.json").write_text(
            json.dumps(
                [{"file": "photo_1.jpg", "type": "photo", "caption": "Summit view"}]
            ),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media[0].caption == "Summit view"

    async def test_missing_media_file_is_skipped(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "media.json").write_text(
            json.dumps([{"file": "photo_1.jpg", "type": "photo"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()

    async def test_unknown_media_type_is_skipped(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "media.json").write_text(
            json.dumps([{"file": "audio_1.mp3", "type": "audio"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()

    async def test_malformed_ref_missing_file_key_is_skipped(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "media.json").write_text(
            json.dumps([{"type": "photo"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()

    async def test_path_traversal_with_dotdot_is_rejected(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        secret = folder / "secret.txt"
        secret.write_bytes(b"sensitive")
        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "media.json").write_text(
            json.dumps([{"file": "../secret.txt", "type": "photo"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()

    async def test_absolute_path_in_file_ref_is_rejected(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        secret = folder / "secret.txt"
        secret.write_bytes(b"sensitive")
        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "media.json").write_text(
            json.dumps([{"file": str(secret), "type": "photo"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()

    async def test_symlink_escaping_stem_dir_is_rejected(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        secret = folder / "secret.txt"
        secret.write_bytes(b"sensitive")
        path = folder / "ride.fit"
        path.write_bytes(b"content")
        stem_dir = folder / "ride"
        stem_dir.mkdir()
        (stem_dir / "evil.jpg").symlink_to(secret)
        (stem_dir / "media.json").write_text(
            json.dumps([{"file": "evil.jpg", "type": "photo"}]),
            encoding="utf-8",
        )

        result = await connector.download_activity(_make_meta(path))

        assert result.media == ()


class TestUploadActivityMedia:
    async def test_photo_file_written_in_stem_dir(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            media=(MediaItem(content=b"photo-data", media_type="photo"),),
        )

        await connector.upload_activity(activity)

        photo = folder / "20260501T082855_12345" / "photo_1.jpg"
        assert photo.is_file()
        assert photo.read_bytes() == b"photo-data"

    async def test_video_file_written_with_mp4_extension(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            media=(MediaItem(content=b"video-data", media_type="video"),),
        )

        await connector.upload_activity(activity)

        video = folder / "20260501T082855_12345" / "video_1.mp4"
        assert video.is_file()
        assert video.read_bytes() == b"video-data"

    async def test_media_json_includes_media_refs(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        activity = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            media=(MediaItem(content=b"photo-data", media_type="photo"),),
        )

        await connector.upload_activity(activity)

        media_json = folder / "20260501T082855_12345" / "media.json"
        assert json.loads(media_json.read_text()) == [
            {"file": "photo_1.jpg", "type": "photo"}
        ]

    async def test_media_json_includes_caption_when_present(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        import json

        activity = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            media=(
                MediaItem(
                    content=b"photo-data",
                    media_type="photo",
                    caption="Top of the climb",
                ),
            ),
        )

        await connector.upload_activity(activity)

        media_json = folder / "20260501T082855_12345" / "media.json"
        assert json.loads(media_json.read_text())[0]["caption"] == "Top of the climb"

    async def test_no_media_files_when_activity_has_no_media(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        await connector.upload_activity(_make_activity())

        files = {f.name for f in folder.iterdir()}
        assert not any("photo" in f or "video" in f for f in files)

    async def test_multiple_media_items_indexed_correctly(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            media=(
                MediaItem(content=b"photo1", media_type="photo"),
                MediaItem(content=b"photo2", media_type="photo"),
            ),
        )

        await connector.upload_activity(activity)

        stem_dir = folder / "20260501T082855_12345"
        assert (stem_dir / "photo_1.jpg").read_bytes() == b"photo1"
        assert (stem_dir / "photo_2.jpg").read_bytes() == b"photo2"

    async def test_stale_media_cleaned_up_on_reupload(
        self, connector: LocalFolderConnector, folder: Path
    ) -> None:
        activity_with_photo = Activity(
            external_id="12345",
            name="Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            media=(MediaItem(content=b"photo-data", media_type="photo"),),
        )
        await connector.upload_activity(activity_with_photo)
        stale_photo = folder / "20260501T082855_12345" / "photo_1.jpg"
        assert stale_photo.exists()

        await connector.upload_activity(_make_activity())

        assert not stale_photo.exists()


class TestLocalFolderMediaUploadSupport:
    def test_supports_media_upload_is_true(
        self, connector: LocalFolderConnector
    ) -> None:
        assert connector.supports_media_upload is True


class TestUserLabel:
    def test_user_label_is_folder_path(
        self, folder: Path, tracker: TaskTracker
    ) -> None:
        connector = _make_connector(folder, tracker)
        assert connector.user_label == str(folder)


class TestLoginWithLogging:
    async def test_login_logs_when_sync_logger_present(
        self, tracker_with_log: TaskTracker, tmp_path: Path
    ) -> None:
        connector = LocalFolderConnector(folder=tmp_path, tracker=tracker_with_log)
        await connector.login()
        log = tracker_with_log.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any("[local-folder]" in m and str(tmp_path) in m for m in msgs)


class TestListActivitiesWithLogging:
    async def test_disk_scan_logs_when_sync_logger_present(
        self, tracker_with_log: TaskTracker, tmp_path: Path
    ) -> None:
        connector = LocalFolderConnector(folder=tmp_path, tracker=tracker_with_log)
        await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))
        log = tracker_with_log.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any(
            "[local-folder]" in m and "disk scan" in m and str(tmp_path) in m
            for m in msgs
        )

    async def test_cache_backed_logs_when_sync_logger_present(
        self, tracker_with_log: TaskTracker, tmp_path: Path
    ) -> None:
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        _make_cache_entry(cache, dest_id="local-dest")
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder,
            tracker=tracker_with_log,
            cache=cache,
            dest_id="local-dest",
        )
        await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))
        log = tracker_with_log.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any("[local-folder]" in m and "cache-backed" in m for m in msgs)

    async def test_cache_backed_exception_fails_task_and_raises(
        self, tracker_with_log: TaskTracker, tmp_path: Path
    ) -> None:
        cache = MagicMock()
        cache.all_entries.side_effect = RuntimeError("cache boom")
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder,
            tracker=tracker_with_log,
            cache=cache,
            dest_id="local-dest",
        )
        with pytest.raises(RuntimeError, match="cache boom"):
            await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))
        task = next(iter(tracker_with_log.tasks.values()))
        assert task.status == TaskStatus.FAILED


class TestCacheBackedIntegration:
    async def test_upload_mark_uploaded_list_cycle(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        """upload returns path, mark_uploaded stores it, list_activities returns it."""
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        raw_entry = CacheEntry(
            external_id="42",
            source_id="garmin-main",
            format="fit",
            start_time=_DT,
            elapsed_s=3600,
            name="Morning Ride",
            sport_type="cycling",
        )
        cached_entry = cache.put(raw_entry, b"fit-bytes")

        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        activity = Activity(
            external_id="42",
            name="Morning Ride",
            sport_type="cycling",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
            elapsed_s=3600,
        )
        local_path = await connector.upload_activity(activity)
        cache.mark_uploaded(cached_entry, "local-dest", local_path=local_path)

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert len(result) == 1
        assert result[0].name == "Morning Ride"
        assert result[0].elapsed_s == 3600

    async def test_file_deleted_then_list_returns_empty(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        """If the uploaded file is later deleted, list_activities excludes the entry."""
        cache = ActivityCache(tmp_path / "cache")
        cache.load()
        cached_entry = cache.put(
            CacheEntry(
                external_id="42",
                source_id="garmin-main",
                format="fit",
                start_time=_DT,
                elapsed_s=None,
            ),
            b"fit-bytes",
        )
        folder = tmp_path / "output"
        folder.mkdir()
        connector = LocalFolderConnector(
            folder=folder, tracker=tracker, cache=cache, dest_id="local-dest"
        )

        activity = Activity(
            external_id="42",
            name="",
            sport_type="",
            start_time=_DT,
            content=b"fit-bytes",
            format="fit",
        )
        local_path = await connector.upload_activity(activity)
        assert local_path is not None
        cache.mark_uploaded(cached_entry, "local-dest", local_path=local_path)
        Path(local_path).unlink()  # simulate file being deleted

        result = await connector.list_activities(date(2026, 5, 1), date(2026, 5, 1))

        assert result == []

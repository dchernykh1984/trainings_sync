from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.connectors.base import Activity, ActivityMeta
from app.connectors.local_folder import LocalFolderConnector
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

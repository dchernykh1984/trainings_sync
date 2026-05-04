from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker

_START = date(2026, 1, 1)
_END = date(2026, 1, 31)
_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


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


def _make_meta(external_id: str = "1") -> ActivityMeta:
    return ActivityMeta(
        external_id=external_id,
        name="Morning Run",
        sport_type="running",
        start_time=_DT,
    )


def _make_activity(external_id: str = "1") -> Activity:
    return Activity(
        external_id=external_id,
        name="Morning Run",
        sport_type="running",
        start_time=_DT,
        content=b"fit-data",
        format="fit",
    )


class _FakeConnector(ServiceConnector):
    def __init__(self, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self.metas: list[ActivityMeta] = []
        self.uploaded: list[Activity] = []

    async def login(self) -> None:
        pass

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        return self.metas

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            content=b"fit-data",
            format="fit",
        )

    async def upload_activity(self, activity: Activity) -> None:
        self.uploaded.append(activity)


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def connector(tracker: TaskTracker) -> _FakeConnector:
    return _FakeConnector(tracker)


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


class TestActivityMeta:
    def test_accepts_utc_datetime(self) -> None:
        meta = _make_meta()

        assert meta.start_time.utcoffset().total_seconds() == 0  # type: ignore[union-attr]

    def test_raises_on_naive_datetime(self) -> None:
        with pytest.raises(ValueError, match="UTC"):
            ActivityMeta(
                external_id="1",
                name="",
                sport_type="",
                start_time=datetime(2026, 1, 1, 8, 0),
            )

    def test_raises_on_non_utc_offset(self) -> None:
        tz_plus3 = timezone(timedelta(hours=3))
        with pytest.raises(ValueError, match="UTC"):
            ActivityMeta(
                external_id="1",
                name="",
                sport_type="",
                start_time=datetime(2026, 1, 1, 8, 0, tzinfo=tz_plus3),
            )

    def test_raises_on_negative_elapsed(self) -> None:
        with pytest.raises(ValueError, match="elapsed_s"):
            ActivityMeta(
                external_id="1",
                name="",
                sport_type="",
                start_time=_DT,
                elapsed_s=-1,
            )

    def test_elapsed_s_defaults_to_none(self) -> None:
        assert _make_meta().elapsed_s is None

    def test_end_time_none_when_no_elapsed(self) -> None:
        assert _make_meta().end_time is None

    def test_end_time_computed_from_elapsed(self) -> None:
        meta = ActivityMeta(
            external_id="1",
            name="Run",
            sport_type="running",
            start_time=_DT,
            elapsed_s=3600,
        )
        assert meta.end_time == _DT + timedelta(seconds=3600)


class TestHasActivity:
    def test_default_returns_true(self, connector: _FakeConnector) -> None:
        assert connector.has_activity("12345", "garmin") is True


class TestDownloadAll:
    async def test_returns_empty_without_creating_task(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        result = await connector.download_all(_START, _END)

        assert result == []
        assert len(tracker.tasks) == 0

    async def test_returns_activities_in_order(self, connector: _FakeConnector) -> None:
        connector.metas = [_make_meta("1"), _make_meta("2"), _make_meta("3")]

        results = await connector.download_all(_START, _END)

        assert [a.external_id for a in results] == ["1", "2", "3"]

    async def test_creates_task_with_correct_total(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        connector.metas = [_make_meta("1"), _make_meta("2")]

        await connector.download_all(_START, _END)

        assert _first_task(tracker).total == 2

    async def test_task_done_after_all_downloads(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        connector.metas = [_make_meta("1"), _make_meta("2")]

        await connector.download_all(_START, _END)

        assert _first_task(tracker).status == TaskStatus.DONE

    async def test_task_fails_when_download_raises(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        connector.metas = [_make_meta("1")]

        with patch.object(
            connector,
            "download_activity",
            AsyncMock(side_effect=OSError("network error")),
        ):
            with pytest.raises(OSError):
                await connector.download_all(_START, _END)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_unique_task_name_on_repeated_calls(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        connector.metas = [_make_meta("1")]

        await connector.download_all(_START, _END)
        await connector.download_all(_START, _END)

        assert len(tracker.tasks) == 2

    async def test_respects_concurrency_limit(self, connector: _FakeConnector) -> None:
        connector._max_concurrent = 1
        connector.metas = [_make_meta("1"), _make_meta("2"), _make_meta("3")]
        order: list[str] = []

        async def _sequential_download(meta: ActivityMeta) -> Activity:
            order.append(meta.external_id)
            return Activity(
                external_id=meta.external_id,
                name=meta.name,
                sport_type=meta.sport_type,
                start_time=meta.start_time,
                content=b"fit-data",
                format="fit",
            )

        with patch.object(connector, "download_activity", _sequential_download):
            await connector.download_all(_START, _END)

        assert order == ["1", "2", "3"]


class TestUploadAll:
    async def test_returns_without_creating_task_for_empty_list(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        await connector.upload_all([])

        assert len(tracker.tasks) == 0

    async def test_uploads_all_activities(self, connector: _FakeConnector) -> None:
        activities = [_make_activity("1"), _make_activity("2")]

        await connector.upload_all(activities)

        assert len(connector.uploaded) == 2

    async def test_creates_task_with_correct_total(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        activities = [_make_activity("1"), _make_activity("2"), _make_activity("3")]

        await connector.upload_all(activities)

        assert _first_task(tracker).total == 3

    async def test_task_done_after_all_uploads(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        await connector.upload_all([_make_activity("1")])

        assert _first_task(tracker).status == TaskStatus.DONE

    async def test_task_fails_when_upload_raises(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        with patch.object(
            connector,
            "upload_activity",
            AsyncMock(side_effect=OSError("network error")),
        ):
            with pytest.raises(OSError):
                await connector.upload_all([_make_activity("1")])

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_unique_task_name_on_repeated_calls(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        await connector.upload_all([_make_activity("1")])
        await connector.upload_all([_make_activity("1")])

        assert len(tracker.tasks) == 2

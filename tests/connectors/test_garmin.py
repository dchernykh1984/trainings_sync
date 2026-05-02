from __future__ import annotations

import io
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta
from app.connectors.garmin import GarminConnector
from app.credentials.base import Credentials
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker

_START = date(2026, 1, 1)
_END = date(2026, 1, 31)
_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)

_CREDENTIALS = Credentials(login="user@example.com", password="secret")

_RAW_ACTIVITY = {
    "activityId": 12345,
    "activityName": "Morning Run",
    "startTimeGMT": "2026-01-01 08:00:00",
    "activityType": {"typeKey": "running"},
    "duration": 3600.0,
}


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


async def _call_sync(fn, *args, **kwargs):
    return fn(*args, **kwargs)


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def connector(tracker: TaskTracker) -> GarminConnector:
    return GarminConnector(credentials=_CREDENTIALS, tracker=tracker)


@pytest.fixture
def logged_in(connector: GarminConnector, mock_client: MagicMock) -> GarminConnector:
    connector._client = mock_client
    return connector


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


def _make_zip(fit_bytes: bytes = b"fit-content") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("activity_12345.fit", fit_bytes)
    return buf.getvalue()


def _make_meta(external_id: str = "12345") -> ActivityMeta:
    return ActivityMeta(
        external_id=external_id,
        name="Morning Run",
        sport_type="running",
        start_time=_DT,
    )


def _make_activity(external_id: str = "12345") -> Activity:
    return Activity(
        external_id=external_id,
        name="Morning Run",
        sport_type="running",
        start_time=_DT,
        content=b"fit-content",
        format="fit",
    )


class TestLogin:
    async def test_task_done_on_success(
        self, connector: GarminConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.garmin.Garmin"),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.DONE

    async def test_sets_client_on_success(self, connector: GarminConnector) -> None:
        with (
            patch("app.connectors.garmin.Garmin"),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await connector.login()

        assert connector._client is not None

    async def test_task_fails_on_error(
        self, connector: GarminConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.garmin.Garmin"),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=OSError("network error"),
            ),
            pytest.raises(OSError),
        ):
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.FAILED


class TestListActivities:
    async def test_returns_activity_metas(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities_by_date.return_value = [_RAW_ACTIVITY]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.list_activities(_START, _END)

        assert len(result) == 1
        assert result[0].external_id == "12345"
        assert result[0].name == "Morning Run"
        assert result[0].sport_type == "running"
        assert result[0].start_time == _DT
        assert result[0].elapsed_s == 3600

    async def test_passes_date_range_to_client(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities_by_date.return_value = []

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        mock_client.get_activities_by_date.assert_called_once_with(
            "2026-01-01", "2026-01-31"
        )

    async def test_elapsed_s_none_when_duration_missing(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        raw = {k: v for k, v in _RAW_ACTIVITY.items() if k != "duration"}
        mock_client.get_activities_by_date.return_value = [raw]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.list_activities(_START, _END)

        assert result[0].elapsed_s is None

    async def test_raises_when_not_logged_in(self, connector: GarminConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
            await connector.list_activities(_START, _END)


class TestDownloadActivity:
    async def test_returns_activity_with_content(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip(b"fit-content")

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.content == b"fit-content"
        assert result.format == "fit"
        assert result.external_id == "12345"

    async def test_extracts_fit_from_zip(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        fit_bytes = b"real-fit-bytes"
        mock_client.download_activity.return_value = _make_zip(fit_bytes)

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.content == fit_bytes

    async def test_raises_when_not_logged_in(self, connector: GarminConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
            await connector.download_activity(_make_meta())


class TestUploadActivity:
    async def test_calls_client_upload(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(_make_activity())

        mock_client.upload_activity.assert_called_once()
        path_arg = mock_client.upload_activity.call_args[0][0]
        assert path_arg.endswith(f".{_make_activity().format}")

    async def test_temp_file_cleaned_up_after_upload(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        captured: list[str] = []
        mock_client.upload_activity.side_effect = lambda p: captured.append(p)

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(_make_activity())

        assert not Path(captured[0]).exists()

    async def test_raises_when_not_logged_in(self, connector: GarminConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
            await connector.upload_activity(_make_activity())

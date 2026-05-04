from __future__ import annotations

import io
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta
from app.connectors.strava import StravaConnector
from app.credentials.base import StravaCredentials
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker

_START = date(2026, 1, 1)
_END = date(2026, 1, 31)
_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)

_CREDENTIALS = StravaCredentials(
    client_id=12345,
    client_secret="secret",
    refresh_token="refresh_token_value",
)


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


async def _call_sync(fn, *args, **kwargs):
    return fn(*args, **kwargs)


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def connector(tracker: TaskTracker) -> StravaConnector:
    return StravaConnector(credentials=_CREDENTIALS, tracker=tracker)


@pytest.fixture
def logged_in(connector: StravaConnector, mock_client: MagicMock) -> StravaConnector:
    connector._client = mock_client
    return connector


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


def _make_meta(external_id: str = "99999") -> ActivityMeta:
    return ActivityMeta(
        external_id=external_id,
        name="Morning Run",
        sport_type="Run",
        start_time=_DT,
    )


def _make_activity(external_id: str = "99999") -> Activity:
    return Activity(
        external_id=external_id,
        name="Morning Run",
        sport_type="Run",
        start_time=_DT,
        content=b"fit-content",
        format="fit",
    )


def _make_strava_activity(
    activity_id: int = 99999,
    name: str = "Morning Run",
    sport_type_root: str = "Run",
    start_date: datetime = _DT,
    elapsed_time: int | None = 3600,
) -> MagicMock:
    a = MagicMock()
    a.id = activity_id
    a.name = name
    a.sport_type.root = sport_type_root
    a.start_date = start_date
    a.elapsed_time = elapsed_time
    return a


class TestLogin:
    async def test_task_done_on_success(
        self, connector: StravaConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            mock_client_class.return_value.refresh_access_token.return_value = {
                "access_token": "test_token",
                "refresh_token": "new_refresh",
                "expires_at": 9999999999,
            }
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.DONE

    async def test_sets_client_on_success(self, connector: StravaConnector) -> None:
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            mock_client_class.return_value.refresh_access_token.return_value = {
                "access_token": "test_token",
                "refresh_token": "new_refresh",
                "expires_at": 9999999999,
            }
            await connector.login()

        assert connector._client is not None

    async def test_updates_credentials_with_new_refresh_token(
        self, connector: StravaConnector
    ) -> None:
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            mock_client_class.return_value.refresh_access_token.return_value = {
                "access_token": "test_token",
                "refresh_token": "rotated_refresh",
                "expires_at": 9999999999,
            }
            await connector.login()

        assert connector._credentials.refresh_token == "rotated_refresh"

    async def test_calls_on_token_refresh_callback(self, tracker: TaskTracker) -> None:
        received: list[StravaCredentials] = []
        connector = StravaConnector(
            credentials=_CREDENTIALS,
            tracker=tracker,
            on_token_refresh=received.append,
        )
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            mock_client_class.return_value.refresh_access_token.return_value = {
                "access_token": "test_token",
                "refresh_token": "rotated_refresh",
                "expires_at": 9999999999,
            }
            await connector.login()

        assert len(received) == 1
        assert received[0].refresh_token == "rotated_refresh"
        assert received[0].client_id == _CREDENTIALS.client_id

    async def test_task_fails_on_error(
        self, connector: StravaConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.strava.Client"),
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=OSError("network error"),
            ),
            pytest.raises(OSError),
        ):
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_task_fails_when_callback_raises(self, tracker: TaskTracker) -> None:
        def _failing_callback(creds: StravaCredentials) -> None:
            raise OSError("disk full")

        connector = StravaConnector(
            credentials=_CREDENTIALS,
            tracker=tracker,
            on_token_refresh=_failing_callback,
        )
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(OSError),
        ):
            mock_client_class.return_value.refresh_access_token.return_value = {
                "access_token": "test_token",
                "refresh_token": "rotated_refresh",
                "expires_at": 9999999999,
            }
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.FAILED


class TestListActivities:
    async def test_returns_activity_metas(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities.return_value = [_make_strava_activity()]

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.list_activities(_START, _END)

        assert len(result) == 1
        assert result[0].external_id == "99999"
        assert result[0].name == "Morning Run"
        assert result[0].sport_type == "Run"
        assert result[0].start_time == _DT
        assert result[0].elapsed_s == 3600

    async def test_passes_date_range_to_client(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities.return_value = []

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        mock_client.get_activities.assert_called_once_with(
            after=datetime(2026, 1, 1),
            before=datetime(2026, 2, 1),
        )

    async def test_elapsed_s_none_when_elapsed_time_missing(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities.return_value = [
            _make_strava_activity(elapsed_time=None)
        ]

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.list_activities(_START, _END)

        assert result[0].elapsed_s is None

    async def test_raises_when_not_logged_in(self, connector: StravaConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
            await connector.list_activities(_START, _END)


class TestDownloadActivity:
    async def test_raises_not_implemented(self, connector: StravaConnector) -> None:
        with pytest.raises(NotImplementedError):
            await connector.download_activity(_make_meta())


class TestUploadActivity:
    async def test_calls_client_upload_with_correct_args(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_client.upload_activity.return_value = mock_uploader

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(_make_activity())

        call_kwargs = mock_client.upload_activity.call_args.kwargs
        assert call_kwargs["data_type"] == "fit"
        assert call_kwargs["name"] == "Morning Run"
        assert isinstance(call_kwargs["activity_file"], io.BytesIO)
        assert call_kwargs["activity_file"].read() == b"fit-content"

    async def test_waits_for_upload_completion(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_client.upload_activity.return_value = mock_uploader

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(_make_activity())

        mock_uploader.wait.assert_called_once()

    async def test_raises_when_not_logged_in(self, connector: StravaConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
            await connector.upload_activity(_make_activity())

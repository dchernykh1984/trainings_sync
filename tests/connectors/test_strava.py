from __future__ import annotations

import asyncio
import io
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests
from stravalib.exc import ObjectNotFound

from app.connectors.base import (
    Activity,
    ActivityMeta,
    RateLimitError,
    TransientDownloadError,
)
from app.connectors.strava import (
    _RATE_LIMIT_MARGIN,
    _RESET_PADDING_S,
    StravaConnector,
    _make_strava_session,
    _StravaRateLimiter,
    _TimeoutHTTPAdapter,
)
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


def _setup_login_mock(
    mock_client_class: MagicMock,
    new_token: str = "new_refresh",  # noqa: S107
) -> None:
    mock_client_class.return_value.refresh_access_token.return_value = {
        "access_token": "test_token",
        "refresh_token": new_token,
        "expires_at": 9999999999,
    }
    mock_client_class.return_value.get_athlete.return_value.id = 1613761
    mock_client_class.return_value.get_athlete.return_value.firstname = "John"
    mock_client_class.return_value.get_athlete.return_value.lastname = "Doe"


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def tracker_with_log() -> TaskTracker:
    sync_logger = MagicMock()
    sync_logger.info = MagicMock()
    sync_logger.debug = MagicMock()
    sync_logger.warning = MagicMock()
    return TaskTracker(_FakeRenderer(), sync_logger=sync_logger)


@pytest.fixture
def mock_client() -> MagicMock:
    m = MagicMock()
    m.get_activity.return_value.description = None
    m.get_activity.return_value.total_photo_count = 0
    m.get_activity_photos.return_value = []
    return m


@pytest.fixture
def connector(tracker: TaskTracker) -> StravaConnector:
    return StravaConnector(credentials=_CREDENTIALS, tracker=tracker)


@pytest.fixture
def connector_with_log(tracker_with_log: TaskTracker) -> StravaConnector:
    return StravaConnector(credentials=_CREDENTIALS, tracker=tracker_with_log)


@pytest.fixture
def logged_in(connector: StravaConnector, mock_client: MagicMock) -> StravaConnector:
    connector._client = mock_client
    return connector


@pytest.fixture
def logged_in_with_log(
    connector_with_log: StravaConnector, mock_client: MagicMock
) -> StravaConnector:
    connector_with_log._client = mock_client
    return connector_with_log


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


def _make_meta(external_id: str = "99999") -> ActivityMeta:
    return ActivityMeta(
        external_id=external_id,
        name="Morning Run",
        sport_type="Run",
        start_time=_DT,
    )


def _make_stream(data: list) -> MagicMock:
    m = MagicMock()
    m.data = data
    return m


def _make_gps_streams() -> dict:
    return {
        "time": _make_stream([0, 60]),
        "latlng": _make_stream([[51.5, -0.1], [51.51, -0.11]]),
        "altitude": _make_stream([100.0, 101.0]),
        "heartrate": _make_stream([150, 155]),
        "cadence": _make_stream([80, 82]),
    }


def _make_indoor_streams() -> dict:
    return {
        "time": _make_stream([0, 60]),
        "heartrate": _make_stream([150, 155]),
        "cadence": _make_stream([80, 82]),
        "watts": _make_stream([200, 210]),
    }


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
            _setup_login_mock(mock_client_class)
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
            _setup_login_mock(mock_client_class)
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
            _setup_login_mock(mock_client_class, new_token="rotated_refresh")
            await connector.login()

        assert connector._credentials.refresh_token == "rotated_refresh"

    async def test_calls_on_token_refresh_callback(self, tracker: TaskTracker) -> None:
        received: list[tuple[StravaCredentials, str]] = []
        connector = StravaConnector(
            credentials=_CREDENTIALS,
            tracker=tracker,
            on_token_refresh=lambda creds, label: received.append((creds, label)),
        )
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            _setup_login_mock(mock_client_class, new_token="rotated_refresh")
            await connector.login()

        assert len(received) == 1
        creds, label = received[0]
        assert creds.refresh_token == "rotated_refresh"
        assert creds.client_id == _CREDENTIALS.client_id
        assert label == "John Doe"

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

    async def test_task_fails_on_cancelled_error(
        self, connector: StravaConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.strava.Client"),
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_task_fails_when_callback_raises(self, tracker: TaskTracker) -> None:
        def _failing_callback(creds: StravaCredentials, label: str) -> None:
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


class TestListActivitiesError:
    async def test_exception_in_get_activities_fails_task_and_raises(
        self, logged_in: StravaConnector, mock_client: MagicMock, tracker: TaskTracker
    ) -> None:
        mock_client.get_activities.side_effect = OSError("network boom")

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(OSError, match="network boom"),
        ):
            await logged_in.list_activities(_START, _END)

        task = next(iter(tracker.tasks.values()))
        assert task.status == TaskStatus.FAILED


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
    async def test_returns_gpx_for_outdoor_activity(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.format == "gpx"
        assert result.external_id == "99999"
        assert result.name == "Morning Run"

    async def test_gpx_contains_correct_trackpoints(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        root = ET.fromstring(result.content)  # noqa: S314
        ns = {"g": "http://www.topografix.com/GPX/1/1"}
        trk = root.find("g:trk", ns)
        assert trk is not None
        type_el = trk.find("g:type", ns)
        assert type_el is not None
        assert type_el.text == "Run"
        trkpts = root.findall(".//g:trkpt", ns)
        assert len(trkpts) == 2
        assert trkpts[0].attrib["lat"] == "51.5"
        assert trkpts[0].attrib["lon"] == "-0.1"
        time_el = trkpts[0].find("g:time", ns)
        assert time_el is not None
        assert time_el.text == "2026-01-01T08:00:00Z"
        # second point: start_time + 60s
        time_el2 = trkpts[1].find("g:time", ns)
        assert time_el2 is not None
        assert time_el2.text == "2026-01-01T08:01:00Z"

    async def test_gpx_includes_heartrate_and_cadence_extensions(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        root = ET.fromstring(result.content)  # noqa: S314
        ns = {
            "g": "http://www.topografix.com/GPX/1/1",
            "tpx": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1",
        }
        hr_els = root.findall(".//tpx:hr", ns)
        assert len(hr_els) == 2
        assert hr_els[0].text == "150"
        cad_els = root.findall(".//tpx:cad", ns)
        assert len(cad_els) == 2
        assert cad_els[0].text == "80"

    async def test_gpx_includes_elevation(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        root = ET.fromstring(result.content)  # noqa: S314
        ns = {"g": "http://www.topografix.com/GPX/1/1"}
        ele_els = root.findall(".//g:ele", ns)
        assert len(ele_els) == 2
        assert ele_els[0].text == "100.0"

    async def test_returns_tcx_for_indoor_activity(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_indoor_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.format == "tcx"
        assert result.external_id == "99999"

    async def test_tcx_contains_trackpoints_with_hr_cadence_and_watts(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_indoor_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        root = ET.fromstring(result.content)  # noqa: S314
        ns = {
            "tcd": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2",
            "ae": "http://www.garmin.com/xmlschemas/ActivityExtension/v2",
        }
        trackpoints = root.findall(".//tcd:Trackpoint", ns)
        assert len(trackpoints) == 2
        hr_els = root.findall(".//tcd:HeartRateBpm/tcd:Value", ns)
        assert hr_els[0].text == "150"
        watts_els = root.findall(".//ae:Watts", ns)
        assert len(watts_els) == 2
        assert watts_els[0].text == "200"

    async def test_streams_api_called_with_int_activity_id(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.download_activity(_make_meta(external_id="99999"))

        mock_client.get_activity_streams.assert_called_once_with(
            99999, types=["time", "latlng", "altitude", "heartrate", "cadence", "watts"]
        )

    async def test_get_activity_failure_raises_and_cleans_up(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity.side_effect = RuntimeError("network error")
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(RuntimeError, match="network error"):
                await logged_in.download_activity(_make_meta())

    async def test_get_activity_request_error_raises_transient_download_error(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:

        mock_client.get_activity.side_effect = requests.ConnectionError("timeout")
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(TransientDownloadError, match="timeout"):
                await logged_in.download_activity(_make_meta())

    async def test_get_activity_streams_request_error_raises_transient_download_error(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:

        mock_client.get_activity_streams.side_effect = requests.ConnectionError(
            "connection reset"
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(TransientDownloadError, match="connection reset"):
                await logged_in.download_activity(_make_meta())

    async def test_get_activity_429_raises_rate_limit_error(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        from app.connectors.base import RateLimitError

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "300"}
        mock_client.get_activity.side_effect = requests.HTTPError(
            "429 Client Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(RateLimitError) as exc_info:
                await logged_in.download_activity(_make_meta())
        assert exc_info.value.retry_after == 300.0

    async def test_get_activity_streams_429_raises_rate_limit_error(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        from app.connectors.base import RateLimitError

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {}
        mock_client.get_activity_streams.side_effect = requests.HTTPError(
            "429 Client Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(RateLimitError) as exc_info:
                await logged_in.download_activity(_make_meta())
        assert exc_info.value.retry_after == 900.0  # default when no Retry-After header

    async def test_get_activity_non_429_http_error_raises_transient(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.headers = {}
        mock_client.get_activity.side_effect = requests.HTTPError(
            "500 Server Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(TransientDownloadError):
                await logged_in.download_activity(_make_meta())

    async def test_get_activity_429_non_numeric_retry_after_uses_default(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        from app.connectors.base import RateLimitError

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}
        mock_client.get_activity.side_effect = requests.HTTPError(
            "429 Client Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(RateLimitError) as exc_info:
                await logged_in.download_activity(_make_meta())
        assert exc_info.value.retry_after == 900.0

    async def test_returns_minimal_tcx_when_time_stream_absent(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = {}
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.format == "tcx"
        ns = {"tcd": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
        root = ET.fromstring(result.content)  # noqa: S314
        assert not root.findall(".//tcd:Trackpoint", ns)

    async def test_returns_minimal_tcx_when_time_stream_empty(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = {"time": _make_stream([])}
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.format == "tcx"
        ns = {"tcd": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
        root = ET.fromstring(result.content)  # noqa: S314
        assert not root.findall(".//tcd:Trackpoint", ns)

    async def test_description_populated_from_get_activity(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.description = "Great ride today!"
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.description == "Great ride today!"

    async def test_description_none_when_get_activity_has_no_description(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.description = None
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.description is None

    async def test_gpx_tolerates_shorter_optional_streams(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        streams = {
            "time": _make_stream([0, 60]),
            "latlng": _make_stream([[51.5, -0.1], [51.51, -0.11]]),
            "heartrate": _make_stream([150]),  # only 1 sample instead of 2
        }
        mock_client.get_activity_streams.return_value = streams
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        root = ET.fromstring(result.content)  # noqa: S314
        ns = {
            "g": "http://www.topografix.com/GPX/1/1",
            "tpx": "http://www.garmin.com/xmlschemas/TrackPointExtension/v1",
        }
        trkpts = root.findall(".//g:trkpt", ns)
        assert len(trkpts) == 2
        hr_els = root.findall(".//tpx:hr", ns)
        assert len(hr_els) == 1  # second trackpoint has no HR

    async def test_tcx_tolerates_shorter_optional_streams(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        streams = {
            "time": _make_stream([0, 60]),
            "watts": _make_stream([200]),  # only 1 sample instead of 2
        }
        mock_client.get_activity_streams.return_value = streams
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        root = ET.fromstring(result.content)  # noqa: S314
        ns = {"ae": "http://www.garmin.com/xmlschemas/ActivityExtension/v2"}
        watts_els = root.findall(".//ae:Watts", ns)
        assert len(watts_els) == 1  # second trackpoint has no watts

    async def test_401_on_get_activity_refreshes_token_and_raises_transient(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        from stravalib.exc import AccessUnauthorized

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.headers = {}
        mock_client.get_activity.side_effect = AccessUnauthorized(
            "Unauthorized: Authorization Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with patch.object(logged_in, "login", new=AsyncMock()) as mock_login:
                with pytest.raises(TransientDownloadError):
                    await logged_in.download_activity(_make_meta())
        mock_login.assert_called_once()

    async def test_401_on_get_activity_streams_refreshes_token_and_raises_transient(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        from stravalib.exc import AccessUnauthorized

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.headers = {}
        mock_client.get_activity_streams.side_effect = AccessUnauthorized(
            "Unauthorized: Authorization Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with patch.object(logged_in, "login", new=AsyncMock()) as mock_login:
                with pytest.raises(TransientDownloadError):
                    await logged_in.download_activity(_make_meta())
        mock_login.assert_called_once()

    async def test_concurrent_401s_refresh_token_once(
        self, logged_in: StravaConnector
    ) -> None:
        login_calls = 0

        async def fake_login() -> None:
            nonlocal login_calls
            login_calls += 1
            logged_in._token_generation += 1
            await asyncio.sleep(0)  # yield so the second task tries the lock while held

        gen = logged_in._token_generation
        with patch.object(logged_in, "login", new=fake_login):
            await asyncio.gather(
                logged_in._refresh_token_if_needed(gen),
                logged_in._refresh_token_if_needed(gen),
            )
        assert login_calls == 1

    async def test_stale_gen_skips_token_refresh(
        self, logged_in: StravaConnector
    ) -> None:
        from stravalib.exc import AccessUnauthorized

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.headers = {}
        exc = AccessUnauthorized("Unauthorized", response=mock_response)

        stale_gen = logged_in._token_generation
        logged_in._token_generation += 1  # another task already refreshed

        login_calls = 0

        async def fake_login() -> None:
            nonlocal login_calls
            login_calls += 1

        with patch.object(logged_in, "login", new=fake_login):
            await logged_in._raise_for_http_error(exc, stale_gen)

        assert login_calls == 0

    async def test_raises_when_not_logged_in(self, connector: StravaConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
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

    async def test_polls_until_activity_id_is_set(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_uploader.activity_id = None
        mock_client.upload_activity.return_value = mock_uploader

        def do_poll():
            mock_uploader.activity_id = 42

        mock_uploader.poll.side_effect = do_poll

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()),
        ):
            await logged_in.upload_activity(_make_activity())

        mock_uploader.poll.assert_called_once()

    async def test_raises_when_not_logged_in(self, connector: StravaConnector) -> None:
        with pytest.raises(RuntimeError, match="login"):
            await connector.upload_activity(_make_activity())

    async def test_sets_description_when_result_has_id(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_uploader.activity_id = 42
        mock_client.upload_activity.return_value = mock_uploader
        activity = Activity(
            external_id="99999",
            name="Morning Run",
            sport_type="Run",
            start_time=_DT,
            content=b"content",
            format="tcx",
            description="Felt great",
        )

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(activity)

        mock_client.update_activity.assert_called_once_with(
            42, description="Felt great"
        )

    async def test_skips_description_update_when_result_has_no_id(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_uploader.activity_id = 0  # falsy not None -- exits loop, skips update
        mock_client.upload_activity.return_value = mock_uploader
        activity = Activity(
            external_id="99999",
            name="Morning Run",
            sport_type="Run",
            start_time=_DT,
            content=b"content",
            format="tcx",
            description="Felt great",
        )

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(activity)

        mock_client.update_activity.assert_not_called()

    async def test_skips_description_update_when_no_description(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_uploader.activity_id = 42
        mock_client.upload_activity.return_value = mock_uploader

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(_make_activity())

        mock_client.update_activity.assert_not_called()

    async def test_logs_warning_when_result_has_no_id(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_uploader.activity_id = 0  # falsy: triggers the "no ID" warning
        mock_client.upload_activity.return_value = mock_uploader
        activity = Activity(
            external_id="99999",
            name="Morning Run",
            sport_type="Run",
            start_time=_DT,
            content=b"content",
            format="tcx",
            description="Felt great",
        )

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in_with_log.upload_activity(activity)

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("[strava]" in m and "description not set" in m for m in msgs)
        assert any(logged_in_with_log.user_label in m for m in msgs)


class TestLoginWithLogging:
    async def test_login_logs_when_sync_logger_present(
        self, connector_with_log: StravaConnector, tracker_with_log: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            _setup_login_mock(mock_client_class)
            await connector_with_log.login()

        log = tracker_with_log.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any("athlete_id=1613761" in m and "John Doe" in m for m in msgs)

    async def test_login_success_log_without_athlete_name(
        self, tracker_with_log: TaskTracker
    ) -> None:
        connector = StravaConnector(credentials=_CREDENTIALS, tracker=tracker_with_log)
        with (
            patch("app.connectors.strava.Client") as mock_client_class,
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            # athlete with no name parts -> _athlete_name stays ""
            mock_client_class.return_value.refresh_access_token.return_value = {
                "access_token": "tok",
                "refresh_token": "rt",
                "expires_at": 9999999999,
            }
            mock_client_class.return_value.get_athlete.return_value.id = 1234
            mock_client_class.return_value.get_athlete.return_value.firstname = ""
            mock_client_class.return_value.get_athlete.return_value.lastname = ""
            await connector.login()

        log = tracker_with_log.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any("athlete_id=1234" in m for m in msgs)
        assert not any("John Doe" in m for m in msgs)


class TestListActivitiesWithLogging:
    async def test_list_activities_logs_pages_when_sync_logger_present(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities.return_value = [_make_strava_activity()]

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in_with_log.list_activities(_START, _END)

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msg = log.debug.call_args.args[0]  # type: ignore[attr-defined]
        assert "[strava]" in msg
        assert "page" in msg


class TestDownloadActivityObjectNotFound:
    async def test_returns_minimal_tcx_when_streams_not_found(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.side_effect = ObjectNotFound("not found")
        mock_client.get_activity.return_value.description = "Manual workout"

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.format == "tcx"
        assert result.description == "Manual workout"
        ns = {"tcd": "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"}
        root = ET.fromstring(result.content)  # noqa: S314
        assert not root.findall(".//tcd:Trackpoint", ns)

    async def test_fallback_logs_when_sync_logger_present(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.side_effect = ObjectNotFound("not found")
        mock_client.get_activity.return_value.description = None

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in_with_log.download_activity(_make_meta())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any("[strava]" in m and "minimal TCX fallback" in m for m in msgs)


def _make_photo(
    url: str = "https://photos.strava.com/photo.jpg", caption: str | None = None
) -> MagicMock:
    photo = MagicMock()
    photo.urls = {"2048": url}
    photo.caption = caption
    return photo


class TestDownloadActivityPhotos:
    async def test_photos_returned_as_media(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 2
        mock_client.get_activity_photos.return_value = [
            _make_photo("https://example.com/p1.jpg", caption="Summit"),
            _make_photo("https://example.com/p2.jpg", caption=None),
        ]

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch(
                "app.connectors.strava._fetch_url_bytes",
                side_effect=[b"bytes-1", b"bytes-2"],
            ),
        ):
            result = await logged_in.download_activity(_make_meta())

        assert len(result.media) == 2
        assert result.media[0].content == b"bytes-1"
        assert result.media[0].media_type == "photo"
        assert result.media[0].caption == "Summit"
        assert result.media[0].url == "https://example.com/p1.jpg"
        assert result.media[1].content == b"bytes-2"
        assert result.media[1].caption is None

    async def test_media_empty_when_no_photos(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity_photos.return_value = []

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.media == ()

    async def test_get_activity_photos_429_raises_rate_limit_error(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        from app.connectors.base import RateLimitError

        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"Retry-After": "120"}
        mock_client.get_activity_photos.side_effect = requests.HTTPError(
            "429 Client Error", response=mock_response
        )
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(RateLimitError) as exc_info:
                await logged_in.download_activity(_make_meta())
        assert exc_info.value.retry_after == 120.0

    async def test_401_on_get_activity_photos_raises_transient_and_refreshes_token(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:

        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.headers = {}
        mock_client.get_activity_photos.side_effect = requests.HTTPError(
            "401 Unauthorized", response=mock_response
        )

        login_calls = 0

        async def fake_login() -> None:
            nonlocal login_calls
            login_calls += 1
            logged_in._token_generation += 1

        with (
            patch.object(logged_in, "login", new=fake_login),
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(TransientDownloadError),
        ):
            await logged_in.download_activity(_make_meta())

        assert login_calls == 1

    async def test_photo_fetch_http_error_non_429_raises_transient_and_logs_warning(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.headers = {}
        mock_client.get_activity_photos.side_effect = requests.HTTPError(
            "403 Forbidden", response=mock_response
        )
        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(TransientDownloadError),
        ):
            await logged_in_with_log.download_activity(_make_meta())
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to fetch photo list" in m for m in msgs)

    async def test_photo_fetch_exception_raises_transient_and_logs_warning(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_client.get_activity_photos.side_effect = OSError("API error")

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(TransientDownloadError),
        ):
            await logged_in_with_log.download_activity(_make_meta())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to fetch photo list" in m for m in msgs)
        assert any("API error" in m for m in msgs)

    async def test_photo_url_download_exception_raises_transient_and_logs_warning(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_client.get_activity_photos.return_value = [
            _make_photo("https://example.com/p1.jpg"),
        ]

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch(
                "app.connectors.strava._fetch_url_bytes",
                side_effect=OSError("download failed"),
            ),
            pytest.raises(TransientDownloadError),
        ):
            await logged_in_with_log.download_activity(_make_meta())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to download photo #1" in m for m in msgs)
        assert any("download failed" in m for m in msgs)
        assert not any("example.com" in m for m in msgs)

    async def test_photo_without_url_skipped(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        photo = MagicMock()
        photo.urls = {}
        photo.caption = None
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_client.get_activity_photos.return_value = [photo]

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.media == ()

    async def test_photos_fetched_for_minimal_tcx_fallback(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = {}
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_client.get_activity_photos.return_value = [
            _make_photo("https://example.com/p.jpg")
        ]

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch(
                "app.connectors.strava._fetch_url_bytes",
                return_value=b"photo-bytes",
            ),
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.format == "tcx"
        assert len(result.media) == 1
        assert result.media[0].content == b"photo-bytes"

    async def test_get_activity_photos_called_with_activity_id(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.download_activity(_make_meta(external_id="99999"))

        mock_client.get_activity_photos.assert_called_once_with(99999, size=2048)

    async def test_photo_fetch_not_called_when_total_photo_count_is_zero(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 0

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.media == ()
        mock_client.get_activity_photos.assert_not_called()


class TestUploadActivityMediaSupport:
    def test_supports_media_upload_is_false(self, connector: StravaConnector) -> None:
        assert connector.supports_media_upload is False


class TestTimeoutHTTPAdapter:
    def test_injects_default_timeout(self) -> None:
        from app.connectors.strava import _REQUEST_TIMEOUT_S

        adapter = _TimeoutHTTPAdapter()
        with patch.object(
            adapter.__class__.__bases__[0], "send", return_value=MagicMock()
        ) as mock_send:
            adapter.send(MagicMock())
        _, kwargs = mock_send.call_args
        assert kwargs.get("timeout") == _REQUEST_TIMEOUT_S

    def test_does_not_override_explicit_timeout(self) -> None:
        adapter = _TimeoutHTTPAdapter()
        with patch.object(
            adapter.__class__.__bases__[0], "send", return_value=MagicMock()
        ) as mock_send:
            adapter.send(MagicMock(), timeout=5)
        _, kwargs = mock_send.call_args
        assert kwargs.get("timeout") == 5


class TestMakeStravaSession:
    def test_returns_requests_session(self) -> None:
        assert isinstance(_make_strava_session(), requests.Session)

    def test_https_adapter_is_timeout_adapter(self) -> None:
        session = _make_strava_session()
        assert isinstance(
            session.get_adapter("https://example.com"), _TimeoutHTTPAdapter
        )

    def test_http_adapter_is_timeout_adapter(self) -> None:
        session = _make_strava_session()
        assert isinstance(
            session.get_adapter("http://example.com"), _TimeoutHTTPAdapter
        )

    def test_rate_limiter_hook_raises_rate_limit_error_on_429(self) -> None:
        from app.connectors.strava import _attach_strava_rate_limiter

        limiter = _StravaRateLimiter()
        fake_resp = MagicMock()
        fake_resp.status_code = 429
        fake_resp.headers = {}
        s = requests.Session()
        with patch.object(s, "send", return_value=fake_resp):
            _attach_strava_rate_limiter(s, limiter)
            with pytest.raises(RateLimitError):
                s.send(MagicMock())

    def test_rate_limiter_hook_passes_through_non_429(self) -> None:
        from app.connectors.strava import _attach_strava_rate_limiter

        limiter = _StravaRateLimiter()
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "5,50",
        }
        log_calls: list[str] = []
        s = requests.Session()
        with patch.object(s, "send", return_value=fake_resp):
            _attach_strava_rate_limiter(s, limiter, log_fn=log_calls.append)
            result = s.send(MagicMock())
        assert result is fake_resp
        assert len(log_calls) == 1
        assert "15min=5/100" in log_calls[0]
        assert "daily=50/1000" in log_calls[0]

    def test_rate_limiter_hook_warns_when_api_response_has_no_rate_limit_headers(
        self,
    ) -> None:
        from app.connectors.strava import _attach_strava_rate_limiter

        limiter = _StravaRateLimiter()
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.headers = {}
        fake_resp.url = (
            "https://www.strava.com/api/v3/athlete/activities"
            "?access_token=SECRET&page=1"
        )
        warn_calls: list[str] = []
        log_calls: list[str] = []
        s = requests.Session()
        with patch.object(s, "send", return_value=fake_resp):
            _attach_strava_rate_limiter(
                s, limiter, log_fn=log_calls.append, warn_fn=warn_calls.append
            )
            s.send(MagicMock())
        assert len(warn_calls) == 1
        assert "/api/v3/" in warn_calls[0]
        assert "SECRET" not in warn_calls[0]
        assert "access_token=REDACTED" in warn_calls[0]
        assert log_calls == []  # no usage parsed, no debug line

    def test_rate_limiter_hook_no_warn_when_warn_fn_is_none(self) -> None:
        from app.connectors.strava import _attach_strava_rate_limiter

        limiter = _StravaRateLimiter()
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.headers = {}
        fake_resp.url = "https://www.strava.com/api/v3/athlete/activities"
        s = requests.Session()
        with patch.object(s, "send", return_value=fake_resp):
            _attach_strava_rate_limiter(s, limiter)  # warn_fn=None by default
            s.send(MagicMock())  # must not raise

    def test_rate_limiter_hook_no_warn_for_oauth_response_without_headers(
        self,
    ) -> None:
        from app.connectors.strava import _attach_strava_rate_limiter

        limiter = _StravaRateLimiter()
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.headers = {}
        fake_resp.url = "https://www.strava.com/oauth/token"
        warn_calls: list[str] = []
        s = requests.Session()
        with patch.object(s, "send", return_value=fake_resp):
            _attach_strava_rate_limiter(s, limiter, warn_fn=warn_calls.append)
            s.send(MagicMock())
        assert warn_calls == []


class TestStravaRateLimiter:
    """Unit tests for _StravaRateLimiter.

    Clock and sleep are injected so no real waits occur.
    """

    # ------------------------------------------------------------------
    # Header parsing
    # ------------------------------------------------------------------

    def test_buckets_start_as_none(self) -> None:
        limiter = _StravaRateLimiter()
        assert limiter._limit_15min is None
        assert limiter._usage_15min is None
        assert limiter._limit_daily is None
        assert limiter._usage_daily is None
        assert limiter._read_limit_15min is None
        assert limiter._read_usage_15min is None
        assert limiter._read_limit_daily is None
        assert limiter._read_usage_daily is None

    def test_parse_headers_valid(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "5,50",
            "X-ReadRateLimit-Limit": "80,800",
            "X-ReadRateLimit-Usage": "3,30",
        }
        limiter.update_from_headers(resp)
        assert limiter._limit_15min == 100
        assert limiter._limit_daily == 1000
        assert limiter._usage_15min == 5
        assert limiter._usage_daily == 50
        assert limiter._read_limit_15min == 80
        assert limiter._read_limit_daily == 800
        assert limiter._read_usage_15min == 3
        assert limiter._read_usage_daily == 30

    def test_parse_headers_missing_keeps_previous(self) -> None:
        limiter = _StravaRateLimiter()
        resp1 = MagicMock()
        resp1.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "5,50"}
        limiter.update_from_headers(resp1)
        resp2 = MagicMock()
        resp2.headers = {}  # missing all headers
        limiter.update_from_headers(resp2)
        assert limiter._limit_15min == 100
        assert limiter._usage_15min == 5

    def test_parse_headers_malformed_keeps_previous(self) -> None:
        limiter = _StravaRateLimiter()
        resp1 = MagicMock()
        resp1.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "5,50"}
        limiter.update_from_headers(resp1)
        resp2 = MagicMock()
        resp2.headers = {"X-RateLimit-Limit": "bad", "X-RateLimit-Usage": "also-bad"}
        limiter.update_from_headers(resp2)
        assert limiter._limit_15min == 100
        assert limiter._usage_15min == 5

    def test_parse_retry_after_optional_malformed_returns_none(self) -> None:
        from app.connectors.strava import _parse_retry_after_optional

        headers = MagicMock()
        headers.get.return_value = "not-a-number"
        assert _parse_retry_after_optional(headers) is None

    # ------------------------------------------------------------------
    # Quarter-hour and midnight helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # retry_after_for_429
    # ------------------------------------------------------------------

    def test_retry_after_for_429_daily_near_limit(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "5,999"}
        limiter.update_from_headers(resp)
        # daily usage (999) >= daily limit (1000) - _RATE_LIMIT_MARGIN -> daily pause
        pause = limiter.retry_after_for_429()
        assert pause > 0
        expected = limiter._reset_time_daily + _RESET_PADDING_S - time.time()
        assert abs(pause - expected) < 2

    def test_retry_after_for_429_15min_near_limit(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,5"}
        limiter.update_from_headers(resp)
        # 15min usage (99) >= limit - _RATE_LIMIT_MARGIN; daily fine -> 15min pause
        pause = limiter.retry_after_for_429()
        assert pause > 0
        expected = limiter._reset_time_15min + _RESET_PADDING_S - time.time()
        assert abs(pause - expected) < 2

    def test_retry_after_for_429_unknown_buckets_fallback(self) -> None:
        limiter = _StravaRateLimiter()
        # No headers parsed -- all buckets are None; falls back to 15min reset target.
        pause = limiter.retry_after_for_429()
        expected = limiter._reset_time_15min + _RESET_PADDING_S - time.time()
        assert abs(pause - expected) < 2

    def test_retry_after_for_429_respects_retry_after_header(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "1,1"}
        limiter.update_from_headers(resp)
        # Computed pause from clock will be < 9000s; header sets a higher lower bound.
        pause = limiter.retry_after_for_429(retry_after_header=9000.0)
        assert pause == 9000.0

    # ------------------------------------------------------------------
    # wait_if_needed
    # ------------------------------------------------------------------

    async def test_wait_if_needed_no_sleep_when_unknown(self) -> None:
        limiter = _StravaRateLimiter()
        sleep_calls: list[float] = []
        with patch(
            "app.connectors.strava.asyncio.sleep",
            new=AsyncMock(side_effect=lambda s: sleep_calls.append(s)),
        ):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        assert sleep_calls == []

    async def test_wait_if_needed_no_sleep_below_margin(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "50,500"}
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []
        with patch(
            "app.connectors.strava.asyncio.sleep",
            new=AsyncMock(side_effect=lambda s: sleep_calls.append(s)),
        ):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        assert sleep_calls == []

    async def test_wait_if_needed_15min_pause(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,500"}
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            limiter._usage_15min = 0  # simulate window reset so next iteration exits

        with patch("app.connectors.strava.asyncio.sleep", new=fake_sleep):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] > 0

    async def test_wait_if_needed_daily_pause(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "1,999"}
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            limiter._usage_daily = 0  # simulate midnight reset

        with patch("app.connectors.strava.asyncio.sleep", new=fake_sleep):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] > 0

    async def test_wait_if_needed_daily_wins_over_15min(self) -> None:
        # Both daily and 15min near limit -- daily (longer) should fire first.
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,999"}
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            limiter._usage_daily = 0  # clear daily so next check exits
            limiter._usage_15min = 0

        with patch("app.connectors.strava.asyncio.sleep", new=fake_sleep):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        # Daily check fires first in _compute_needed_pause (longer wait wins).
        assert len(sleep_calls) == 1

    async def test_wait_if_needed_read_bucket_pauses_non_upload(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "1,1",
            "X-ReadRateLimit-Limit": "80,800",
            "X-ReadRateLimit-Usage": "79,5",
        }
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            limiter._read_usage_15min = 0  # simulate window reset

        with patch("app.connectors.strava.asyncio.sleep", new=fake_sleep):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        assert len(sleep_calls) == 1

    async def test_wait_if_needed_read_bucket_does_not_pause_upload(self) -> None:
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "1,1",
            "X-ReadRateLimit-Limit": "80,800",
            "X-ReadRateLimit-Usage": "79,5",
        }
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []
        with patch(
            "app.connectors.strava.asyncio.sleep",
            new=AsyncMock(side_effect=lambda s: sleep_calls.append(s)),
        ):
            await limiter.wait_if_needed(is_non_upload=False, log_fn=None)
        assert sleep_calls == []

    async def test_wait_if_needed_read_daily_pauses_non_upload(self) -> None:
        """Read daily bucket near limit triggers midnight pause for non-upload calls."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "1,1",  # overall daily fine
            "X-ReadRateLimit-Limit": "80,800",
            "X-ReadRateLimit-Usage": "1,799",  # read daily near limit
        }
        limiter.update_from_headers(resp)
        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            limiter._read_usage_daily = 0

        with patch("app.connectors.strava.asyncio.sleep", new=fake_sleep):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        assert len(sleep_calls) == 1

    async def test_wait_if_needed_logs_pause_message(self) -> None:
        """wait_if_needed calls log_fn with pause duration and resume time."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,5"}
        limiter.update_from_headers(resp)
        log_calls: list[str] = []

        async def fake_sleep(s: float) -> None:
            limiter._usage_15min = 0

        with patch("app.connectors.strava.asyncio.sleep", new=fake_sleep):
            await limiter.wait_if_needed(
                is_non_upload=True,
                log_fn=lambda msg: log_calls.append(msg),
            )
        assert len(log_calls) == 1
        assert "pausing" in log_calls[0]
        assert "until" in log_calls[0]

    async def test_wait_if_needed_resets_stale_usage_after_quarter(self) -> None:
        """Usage near limit; after reset window + padding passes, no sleep occurs."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,5"}
        limiter.update_from_headers(resp)
        # Advance clock past the reset time including _RESET_PADDING_S.
        future_ts = limiter._reset_time_15min + _RESET_PADDING_S + 1.0
        with (
            patch("app.connectors.strava.time.time", return_value=future_ts),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        mock_sleep.assert_not_called()

    async def test_wait_if_needed_resets_stale_daily_usage_after_midnight(self) -> None:
        """Daily usage near limit; after midnight + padding passes, no sleep occurs."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "1,999"}
        limiter.update_from_headers(resp)
        future_ts = limiter._reset_time_daily + _RESET_PADDING_S + 1.0
        with (
            patch("app.connectors.strava.time.time", return_value=future_ts),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        mock_sleep.assert_not_called()

    # ------------------------------------------------------------------
    # _call_api retry and hook
    # ------------------------------------------------------------------

    async def test_hook_logs_429_before_rate_limit_error(self) -> None:
        """Session hook fires: update_from_headers then raises RateLimitError on 429."""
        from app.connectors.strava import _attach_strava_rate_limiter

        limiter = _StravaRateLimiter()
        fake_resp = MagicMock()
        fake_resp.status_code = 429
        fake_resp.headers = {
            "Retry-After": "120",
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "100,500",
        }
        s = requests.Session()
        with patch.object(s, "send", return_value=fake_resp):
            _attach_strava_rate_limiter(s, limiter)
            with pytest.raises(RateLimitError) as exc_info:
                s.send(MagicMock())

        assert exc_info.value.retry_after == 120.0
        # Limiter state was updated from headers before the error was raised.
        assert limiter._usage_15min == 100

    async def test_wait_if_needed_no_double_sleep_after_reset(self) -> None:
        """After a stale reset zeroes usage, next wait_if_needed does not sleep."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,5"}
        limiter.update_from_headers(resp)
        # Advance clock past reset + padding; first call zeroes usage, no sleep.
        future_ts = limiter._reset_time_15min + _RESET_PADDING_S + 1.0
        with (
            patch("app.connectors.strava.time.time", return_value=future_ts),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
            # Second call: usage already 0 after stale reset -- no sleep.
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        mock_sleep.assert_not_called()

    async def test_wait_if_needed_does_not_reset_within_padding(self) -> None:
        """Usage is NOT zeroed when clock is past boundary but still within padding."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,5"}
        limiter.update_from_headers(resp)
        # Clock is just past the boundary but before padding expires -- no reset.
        within_padding_ts = limiter._reset_time_15min + _RESET_PADDING_S - 1.0
        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            limiter._usage_15min = 0  # manually clear so loop exits

        with (
            patch("app.connectors.strava.time.time", return_value=within_padding_ts),
            patch("app.connectors.strava.asyncio.sleep", new=fake_sleep),
        ):
            await limiter.wait_if_needed(is_non_upload=True, log_fn=None)
        # Sleep must have fired: usage was not cleared by the stale-check.
        assert len(sleep_calls) == 1

    def test_compute_needed_pause_uses_stored_reset_target(self) -> None:
        """Within padding window: pause must be remaining padding, not next boundary."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "90,5"}
        limiter.update_from_headers(resp)

        # 1 second before the padding deadline -- usage is still stale.
        target_ts = limiter._reset_time_15min + _RESET_PADDING_S
        now_ts = target_ts - 1.0

        with patch("app.connectors.strava.time.time", return_value=now_ts):
            pause, _ = limiter._compute_needed_pause(
                is_non_upload=True, margin=_RATE_LIMIT_MARGIN
            )

        # Must be ~1s (time left until padding expires), not ~900s (next boundary).
        assert 0.9 <= pause <= 2.0, f"expected ~1s pause, got {pause:.1f}s"

    def test_compute_needed_pause_daily_uses_stored_reset_target(self) -> None:
        """Daily padding window: pause must be remaining padding, not next midnight."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "200,2000", "X-RateLimit-Usage": "190,5"}
        limiter.update_from_headers(resp)

        target_ts = limiter._reset_time_daily + _RESET_PADDING_S
        now_ts = target_ts - 1.0

        with patch("app.connectors.strava.time.time", return_value=now_ts):
            pause, _ = limiter._compute_needed_pause(
                is_non_upload=True, margin=_RATE_LIMIT_MARGIN
            )

        # Must be ~1s (remaining padding), not ~86400s (next midnight UTC).
        assert 0.9 <= pause <= 2.0, f"expected ~1s pause, got {pause:.1f}s"

    async def test_call_api_retries_on_429(self, logged_in: StravaConnector) -> None:
        """_call_api catches RateLimitError from session hook and retries the call."""
        call_count = [0]

        def flaky():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RateLimitError("strava 429", retry_after=0.0)
            return "ok"

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()),
        ):
            result = await logged_in._call_api(flaky)

        assert result == "ok"
        assert call_count[0] == 2

    async def test_call_api_logs_warning_on_429(
        self, logged_in_with_log: StravaConnector
    ) -> None:
        """_call_api logs a warning at WARNING level when 429 is received."""
        call_count = [0]

        def flaky():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RateLimitError("strava 429", retry_after=0.0)
            return "ok"

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()),
        ):
            await logged_in_with_log._call_api(flaky)

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("[strava] 429 received" in m for m in msgs)
        assert any("pausing" in m for m in msgs)

    async def test_call_api_raises_on_last_attempt_without_sleeping(
        self, logged_in: StravaConnector
    ) -> None:
        """On the last attempt _call_api raises immediately instead of sleeping."""
        from app.connectors.strava import _CALL_API_MAX_ATTEMPTS

        def always_429():
            raise RateLimitError("strava 429", retry_after=0.0)

        sleep_calls: list[float] = []

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch(
                "app.connectors.strava.asyncio.sleep",
                new=AsyncMock(side_effect=lambda s: sleep_calls.append(s)),
            ),
        ):
            with pytest.raises(TransientDownloadError):
                await logged_in._call_api(always_429)

        # Slept on every attempt EXCEPT the last one.
        assert len(sleep_calls) == _CALL_API_MAX_ATTEMPTS - 1

    async def test_retry_after_for_429_upload_ignores_read_bucket(self) -> None:
        """retry_after_for_429(is_non_upload=False) ignores read daily bucket."""
        limiter = _StravaRateLimiter()
        resp = MagicMock()
        resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "1,1",
            "X-ReadRateLimit-Limit": "80,800",
            "X-ReadRateLimit-Usage": "1,799",  # read daily near limit
        }
        limiter.update_from_headers(resp)
        # With is_non_upload=True the read-daily bucket triggers a midnight pause.
        pause_non_upload = limiter.retry_after_for_429(is_non_upload=True)
        # With is_non_upload=False the read bucket is ignored; use 15-min pause.
        pause_upload = limiter.retry_after_for_429(is_non_upload=False)
        # Both should be positive, but upload pause must not exceed non-upload pause.
        assert pause_upload > 0
        assert pause_upload <= pause_non_upload

    async def test_update_from_headers_refreshes_reset_timestamps(self) -> None:
        """Parsing valid usage headers refreshes reset timestamps to next boundary."""
        limiter = _StravaRateLimiter()
        old_reset_15 = limiter._reset_time_15min
        old_reset_daily = limiter._reset_time_daily

        resp = MagicMock()
        resp.headers = {
            "X-RateLimit-Limit": "100,1000",
            "X-RateLimit-Usage": "5,50",
        }
        limiter.update_from_headers(resp)

        # Timestamps updated to a future boundary (fp rounding tolerance).
        assert limiter._reset_time_15min >= old_reset_15 - 1e-6
        assert limiter._reset_time_daily >= old_reset_daily - 1e-6

    async def test_upload_recreates_bytesio_on_retry(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        """Each upload attempt receives a fresh BytesIO so retries do not read EOF."""
        streams_seen: list[bytes] = []
        call_count = [0]

        def capture_upload(**kwargs):
            call_count[0] += 1
            streams_seen.append(kwargs["activity_file"].read())
            if call_count[0] == 1:
                raise RateLimitError("strava 429", retry_after=0.0)
            result = MagicMock()
            result.activity_id = 42
            return result

        mock_client.upload_activity.side_effect = capture_upload

        activity = Activity(
            external_id="99999",
            name="Morning Run",
            sport_type="Run",
            start_time=_DT,
            content=b"fit-data",
            format="fit",
        )
        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch("app.connectors.strava.asyncio.sleep", new=AsyncMock()),
        ):
            await logged_in.upload_activity(activity)

        assert call_count[0] == 2
        # Both attempts must have received the full content, not EOF.
        assert all(data == b"fit-data" for data in streams_seen)

    async def test_call_api_preserves_run_with_timeout(
        self, logged_in: StravaConnector
    ) -> None:
        """_call_api wraps asyncio.to_thread with _run_with_timeout."""
        from app.connectors.base import _run_with_timeout

        called_with_coroutine = [False]
        original = _run_with_timeout

        async def spy(coro):
            called_with_coroutine[0] = True
            return await original(coro)

        with (
            patch("app.connectors.strava._run_with_timeout", side_effect=spy),
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in._call_api(lambda: "result")

        assert called_with_coroutine[0]

    async def test_concurrent_downloads_respect_small_limit(
        self, logged_in: StravaConnector
    ) -> None:
        """Two concurrent _call_api calls both pause when near the 15-min limit."""
        resp = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "100,1000", "X-RateLimit-Usage": "99,5"}
        logged_in._rate_limiter.update_from_headers(resp)

        sleep_calls: list[float] = []

        async def fake_sleep(s: float) -> None:
            sleep_calls.append(s)
            logged_in._rate_limiter._usage_15min = 0  # reset after sleep

        with (
            patch("app.connectors.strava.asyncio.sleep", new=fake_sleep),
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await asyncio.gather(
                logged_in._call_api(lambda: "a"),
                logged_in._call_api(lambda: "b"),
            )

        # At least one task must have slept (saw usage at limit).
        assert len(sleep_calls) >= 1

    async def test_strava_client_created_without_default_limiter(
        self, connector: StravaConnector
    ) -> None:
        """Both Client instances must be created with rate_limit_requests=False."""
        created_kwargs: list[dict] = []

        def capture_client(**kwargs):
            created_kwargs.append(kwargs)
            return MagicMock()

        import contextlib

        with patch("app.connectors.strava.Client", side_effect=capture_client):
            with contextlib.suppress(Exception):
                await connector.login()
        for kwargs in created_kwargs:
            assert kwargs.get("rate_limit_requests") is False

    def test_connectors_with_same_client_id_share_rate_limiter(
        self, tracker: TaskTracker
    ) -> None:
        """Two connectors with the same client_id must use one _StravaRateLimiter."""
        creds = StravaCredentials(
            client_id=_CREDENTIALS.client_id,
            client_secret="s",
            refresh_token="r",
        )
        # Ensure registry is clean for this client_id before the test.
        StravaConnector._limiter_registry.pop(creds.client_id, None)
        c1 = StravaConnector(creds, tracker)
        c2 = StravaConnector(creds, tracker)
        assert c1._rate_limiter is c2._rate_limiter

    def test_connectors_with_different_client_ids_use_separate_limiters(
        self, tracker: TaskTracker
    ) -> None:
        """Two connectors with different client_ids must have independent limiters."""
        creds_a = StravaCredentials(
            client_id=11111, client_secret="s", refresh_token="r"
        )
        creds_b = StravaCredentials(
            client_id=22222, client_secret="s", refresh_token="r"
        )
        StravaConnector._limiter_registry.pop(11111, None)
        StravaConnector._limiter_registry.pop(22222, None)
        c1 = StravaConnector(creds_a, tracker)
        c2 = StravaConnector(creds_b, tracker)
        assert c1._rate_limiter is not c2._rate_limiter

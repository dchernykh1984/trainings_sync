from __future__ import annotations

import asyncio
import io
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests
from stravalib.exc import ObjectNotFound

from app.connectors.base import Activity, ActivityMeta
from app.connectors.strava import (
    StravaConnector,
    _make_strava_session,
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
        from app.connectors.base import TransientDownloadError

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
        from app.connectors.base import TransientDownloadError

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
        from app.connectors.base import TransientDownloadError

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

    async def test_sets_description_when_result_has_id(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_uploader = MagicMock()
        mock_uploader.wait.return_value.id = 42
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
        mock_uploader.wait.return_value = None
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
        mock_uploader.wait.return_value.id = 42
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
        mock_uploader.wait.return_value = None
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


class TestDownloadBytes:
    def test_reads_response_body_with_timeout(self) -> None:
        from app.connectors.strava import _PHOTO_DOWNLOAD_TIMEOUT_S, _download_bytes

        mock_resp = MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = b"image-data"

        with patch(
            "app.connectors.strava.urllib.request.urlopen", return_value=mock_resp
        ) as mock_urlopen:
            result = _download_bytes("https://example.com/photo.jpg")

        assert result == b"image-data"
        mock_urlopen.assert_called_once_with(
            "https://example.com/photo.jpg", timeout=_PHOTO_DOWNLOAD_TIMEOUT_S
        )


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
                "app.connectors.strava._download_bytes",
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

    async def test_photo_fetch_http_error_non_429_returns_empty_and_logs_warning(
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
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in_with_log.download_activity(_make_meta())
        assert result.media == ()
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to fetch photo list" in m for m in msgs)

    async def test_photo_fetch_exception_returns_empty_and_logs_warning(
        self, logged_in_with_log: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = _make_gps_streams()
        mock_client.get_activity.return_value.total_photo_count = 1
        mock_client.get_activity_photos.side_effect = OSError("API error")

        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in_with_log.download_activity(_make_meta())

        assert result.media == ()
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to fetch photo list" in m for m in msgs)
        assert any("API error" in m for m in msgs)

    async def test_photo_url_download_exception_skips_photo_and_logs_warning(
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
                "app.connectors.strava._download_bytes",
                side_effect=OSError("download failed"),
            ),
        ):
            result = await logged_in_with_log.download_activity(_make_meta())

        assert result.media == ()
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
                "app.connectors.strava._download_bytes",
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

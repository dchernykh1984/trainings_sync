from __future__ import annotations

import io
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from stravalib.exc import ObjectNotFound

from app.connectors.base import Activity, ActivityMeta, ActivityUnavailableError
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
    return TaskTracker(_FakeRenderer(), sync_logger=sync_logger)


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


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

    async def test_raises_when_time_stream_absent(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = {}
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(
                ActivityUnavailableError, match="time stream is absent or empty"
            ):
                await logged_in.download_activity(_make_meta())

    async def test_raises_when_time_stream_empty(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.return_value = {"time": _make_stream([])}
        with patch(
            "app.connectors.strava.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(
                ActivityUnavailableError, match="time stream is absent or empty"
            ):
                await logged_in.download_activity(_make_meta())

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
    async def test_object_not_found_raises_activity_unavailable_error(
        self, logged_in: StravaConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activity_streams.side_effect = ObjectNotFound("not found")

        with (
            patch(
                "app.connectors.strava.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(ActivityUnavailableError, match="streams not found"),
        ):
            await logged_in.download_activity(_make_meta())

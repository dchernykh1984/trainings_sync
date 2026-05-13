from __future__ import annotations

import asyncio
import io
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta, MediaItem
from app.connectors.garmin import (
    _PAGE_SIZE,
    GarminConnector,
    _download_garmin_photo,
    _list_activity_photos,
    _set_activity_description,
    _upload_photo_to_activity,
)
from app.credentials.base import Credentials
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker

try:
    from garminconnect.exceptions import (
        GarminConnectConnectionError,  # type: ignore[import-untyped]
    )
except ImportError:
    GarminConnectConnectionError = OSError  # type: ignore[assignment,misc]

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

    def on_total_updated(self, task: Task) -> None:
        pass


async def _call_sync(fn, *args, **kwargs):
    return fn(*args, **kwargs)


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
    m.get_activity_details.return_value = {"description": None}
    return m


@pytest.fixture
def connector(tracker: TaskTracker) -> GarminConnector:
    return GarminConnector(credentials=_CREDENTIALS, tracker=tracker)


@pytest.fixture
def connector_with_log(tracker_with_log: TaskTracker) -> GarminConnector:
    return GarminConnector(credentials=_CREDENTIALS, tracker=tracker_with_log)


@pytest.fixture
def logged_in(connector: GarminConnector, mock_client: MagicMock) -> GarminConnector:
    connector._client = mock_client
    return connector


@pytest.fixture
def logged_in_with_log(
    connector_with_log: GarminConnector, mock_client: MagicMock
) -> GarminConnector:
    connector_with_log._client = mock_client
    return connector_with_log


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


def _make_zip(
    content: bytes = b"fit-content", *, filename: str = "activity_12345.fit"
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
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

    async def test_task_fails_on_cancelled_error(
        self, connector: GarminConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.garmin.Garmin"),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=asyncio.CancelledError(),
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await connector.login()

        assert _first_task(tracker).status == TaskStatus.FAILED


def _make_full_page() -> list[dict]:
    return [_RAW_ACTIVITY] * 20


class TestListActivities:
    async def test_returns_activity_metas(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.connectapi.return_value = [_RAW_ACTIVITY]

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
        mock_client.connectapi.return_value = []

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        params = mock_client.connectapi.call_args[1]["params"]
        assert params["startDate"] == "2026-01-01"
        assert params["endDate"] == "2026-01-31"
        assert params["start"] == "0"
        assert params["limit"] == "20"

    async def test_elapsed_s_none_when_duration_missing(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        raw = {k: v for k, v in _RAW_ACTIVITY.items() if k != "duration"}
        mock_client.connectapi.return_value = [raw]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.list_activities(_START, _END)

        assert result[0].elapsed_s is None

    async def test_fetches_multiple_pages(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.connectapi.side_effect = [_make_full_page(), [_RAW_ACTIVITY] * 3]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.list_activities(_START, _END)

        assert len(result) == 23
        assert mock_client.connectapi.call_count == 2

    async def test_second_page_uses_correct_start_offset(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.connectapi.side_effect = [_make_full_page(), [_RAW_ACTIVITY]]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        second_call_params = mock_client.connectapi.call_args_list[1][1]["params"]
        assert second_call_params["start"] == "20"

    async def test_tracker_task_done_after_listing(
        self, logged_in: GarminConnector, mock_client: MagicMock, tracker: TaskTracker
    ) -> None:
        mock_client.connectapi.return_value = [_RAW_ACTIVITY]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        tasks = list(tracker.tasks.values())
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.DONE

    async def test_tracker_advances_by_activity_count(
        self, logged_in: GarminConnector, mock_client: MagicMock, tracker: TaskTracker
    ) -> None:
        mock_client.connectapi.side_effect = [_make_full_page(), [_RAW_ACTIVITY] * 5]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        task = next(t for t in tracker.tasks.values() if "fetch" in t.name)
        assert task.progress == _PAGE_SIZE + 5
        assert task.status == TaskStatus.DONE

    async def test_tracker_task_has_indeterminate_total(
        self, logged_in: GarminConnector, mock_client: MagicMock, tracker: TaskTracker
    ) -> None:
        mock_client.connectapi.side_effect = [_make_full_page(), [_RAW_ACTIVITY] * 3]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.list_activities(_START, _END)

        task = next(t for t in tracker.tasks.values() if "fetch" in t.name)
        assert task.total is None

    async def test_tracker_task_fails_on_api_error(
        self, logged_in: GarminConnector, tracker: TaskTracker
    ) -> None:
        with (
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=OSError("api error"),
            ),
            pytest.raises(OSError),
        ):
            await logged_in.list_activities(_START, _END)

        tasks = list(tracker.tasks.values())
        assert tasks[0].status == TaskStatus.FAILED

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

    async def test_falls_back_to_gpx_when_no_fit(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip(
            b"gpx-content", filename="activity_12345.gpx"
        )

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.content == b"gpx-content"
        assert result.format == "gpx"

    async def test_falls_back_to_tcx_when_no_fit_or_gpx(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip(
            b"tcx-content", filename="activity_12345.tcx"
        )

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.content == b"tcx-content"
        assert result.format == "tcx"

    async def test_uppercase_extension_is_accepted(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip(
            b"fit-content", filename="ACTIVITY_12345.FIT"
        )

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.content == b"fit-content"
        assert result.format == "fit"

    async def test_raises_value_error_when_no_supported_file_in_zip(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip(
            b"csv-content", filename="activity_12345.csv"
        )

        with (
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(ValueError, match="no supported file"),
        ):
            await logged_in.download_activity(_make_meta())

    async def test_description_populated_from_description_key(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        mock_client.get_activity_details.return_value = {"description": "Great climb!"}

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.description == "Great climb!"

    async def test_description_populated_from_activity_description_key(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        mock_client.get_activity_details.return_value = {
            "activityDescription": "Great climb!"
        }

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.description == "Great climb!"

    async def test_description_none_when_not_in_details(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        mock_client.get_activity_details.return_value = {}

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.description is None

    async def test_description_none_when_details_api_fails(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        mock_client.get_activity_details.side_effect = RuntimeError("timeout")

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())

        assert result.description is None
        assert result.content == b"fit-content"

    async def test_details_failure_logs_warning_when_sync_logger_present(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        mock_client.get_activity_details.side_effect = RuntimeError("timeout")

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in_with_log.download_activity(_make_meta())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any(
            "[garmin]" in m
            and _CREDENTIALS.login in m
            and "description unavailable" in m
            for m in msgs
        )

    async def test_zip_failure_raises_and_cancels_detail_task(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.side_effect = RuntimeError("network error")

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(RuntimeError, match="network error"):
                await logged_in.download_activity(_make_meta())

    async def test_network_error_on_zip_raises_transient_download_error(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        from app.connectors.base import TransientDownloadError

        mock_client.download_activity.side_effect = OSError("connection reset")

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            with pytest.raises(TransientDownloadError, match="connection reset"):
                await logged_in.download_activity(_make_meta())

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

    async def test_ignores_duplicate_activity_error(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.upload_activity.side_effect = GarminConnectConnectionError(
            "Duplicate Activity"
        )
        mock_client.get_activities_by_date.return_value = []

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in.upload_activity(_make_activity())

    async def test_reraises_non_duplicate_connection_error(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.upload_activity.side_effect = GarminConnectConnectionError(
            "Some other error"
        )
        mock_client.get_activities_by_date.return_value = []

        with (
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(GarminConnectConnectionError),
        ):
            await logged_in.upload_activity(_make_activity())

    async def test_set_activity_name_called_after_upload(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(_make_activity())

        mock_client.set_activity_name.assert_called_once_with("99999", "Morning Run")

    async def test_set_activity_type_called_for_known_strava_sport(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="Run",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
        )
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)

        mock_client.set_activity_type.assert_called_once_with("99999", 0, "running", 0)

    async def test_set_activity_type_not_called_for_unknown_sport(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        activity = Activity(
            external_id="12345",
            name="Activity",
            sport_type="UnknownSport",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
        )
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)

        mock_client.set_activity_type.assert_not_called()

    async def test_metadata_not_set_when_activity_id_not_found(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities_by_date.return_value = []

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(_make_activity())

        mock_client.set_activity_name.assert_not_called()
        mock_client.set_activity_type.assert_not_called()

    async def test_pre_existing_activity_not_rematched_as_uploaded(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        existing = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[existing], [existing]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(_make_activity())

        mock_client.set_activity_name.assert_not_called()

    async def test_set_activity_description_called_when_description_present(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            description="Hard effort on the climb",
        )
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._set_activity_description") as mock_set_desc,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)

        mock_set_desc.assert_called_once_with(
            mock_client, 99999, "Hard effort on the climb"
        )

    async def test_set_activity_description_not_called_when_no_description(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._set_activity_description") as mock_set_desc,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(_make_activity())

        mock_set_desc.assert_not_called()


class TestSetActivityDescription:
    def test_calls_client_put_with_correct_payload(self) -> None:
        client = MagicMock()
        client.garmin_connect_activity = (
            "https://connect.garmin.com/activity-service/activity"
        )
        _set_activity_description(client, 42, "A tough ride")
        client.client.put.assert_called_once_with(
            "connectapi",
            "https://connect.garmin.com/activity-service/activity/42",
            json={"activityId": 42, "description": "A tough ride"},
            api=True,
        )


class TestUserLabel:
    def test_user_label_returns_login(self, connector: GarminConnector) -> None:
        assert connector.user_label == _CREDENTIALS.login


class TestFindUploadedIdException:
    async def test_activity_without_start_time_gmt_is_skipped(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        """Activity missing 'startTimeGMT' triggers KeyError -> continue."""
        # First call (pre-existing): empty; second call (post-upload): entry missing key
        mock_client.get_activities_by_date.side_effect = [
            [],
            [{"activityId": 99999}],  # missing "startTimeGMT"
        ]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in.upload_activity(_make_activity())

        # The entry was skipped (no match found), so None is returned
        assert result is None
        mock_client.set_activity_name.assert_not_called()


class TestLoginWithLogging:
    async def test_login_logs_success_when_sync_logger_present(
        self, connector_with_log: GarminConnector, tracker_with_log: TaskTracker
    ) -> None:
        with (
            patch("app.connectors.garmin.Garmin"),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await connector_with_log.login()

        log = tracker_with_log.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.info.call_args_list]  # type: ignore[attr-defined]
        assert any("user@example.com" in m and "Login" in m for m in msgs)
        assert any("user@example.com" in m and "success" in m for m in msgs)


class TestListActivitiesWithLogging:
    async def test_list_activities_logs_pages_when_sync_logger_present(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.connectapi.return_value = [_RAW_ACTIVITY]

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in_with_log.list_activities(_START, _END)

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msg = log.debug.call_args.args[0]  # type: ignore[attr-defined]
        assert "[garmin]" in msg
        assert "user@example.com" in msg
        assert "page" in msg


class TestUploadActivityWithLogging:
    async def test_upload_logs_success_when_activity_id_found(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in_with_log.upload_activity(_make_activity())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msg = log.info.call_args.args[0]  # type: ignore[attr-defined]
        assert "[garmin]" in msg
        assert "user@example.com" in msg
        assert "success" in msg

    async def test_upload_logs_warning_when_activity_id_not_found(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities_by_date.return_value = []

        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in_with_log.upload_activity(_make_activity())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msg = log.warning.call_args.args[0]  # type: ignore[attr-defined]
        assert "[garmin]" in msg
        assert "user@example.com" in msg
        assert "not found" in msg

    async def test_upload_logs_duplicate_skip(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.upload_activity.side_effect = GarminConnectConnectionError(
            "Duplicate Activity"
        )
        mock_client.get_activities_by_date.return_value = []

        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            await logged_in_with_log.upload_activity(_make_activity())

        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msg = log.info.call_args.args[0]  # type: ignore[attr-defined]
        assert "[garmin]" in msg
        assert "user@example.com" in msg
        assert "duplicate" in msg


def _make_photo_dict(
    url: str = "https://example.com/p.jpg", caption: str | None = None
) -> dict:
    d: dict = {"url": url}
    if caption is not None:
        d["caption"] = caption
    return d


class TestListActivityPhotos:
    def test_returns_list_from_api(self) -> None:
        client = MagicMock()
        client.garmin_connect_activity = "/activity-service/activity"
        photos = [{"url": "https://example.com/p.jpg"}]
        client.connectapi.return_value = photos
        assert _list_activity_photos(client, 12345) == photos

    def test_returns_empty_when_api_returns_none(self) -> None:
        client = MagicMock()
        client.garmin_connect_activity = "/activity-service/activity"
        client.connectapi.return_value = None
        assert _list_activity_photos(client, 12345) == []

    def test_returns_empty_when_api_returns_non_list(self) -> None:
        client = MagicMock()
        client.garmin_connect_activity = "/activity-service/activity"
        client.connectapi.return_value = {"photos": []}
        assert _list_activity_photos(client, 12345) == []

    def test_calls_correct_endpoint(self) -> None:
        client = MagicMock()
        client.garmin_connect_activity = "/activity-service/activity"
        client.connectapi.return_value = []
        _list_activity_photos(client, 12345)
        client.connectapi.assert_called_once_with(
            "/activity-service/activity/12345/image"
        )


class TestDownloadGarminPhoto:
    def test_returns_response_content(self) -> None:
        with patch("app.connectors.garmin._requests.get") as mock_get:
            mock_get.return_value.content = b"photo-bytes"
            assert _download_garmin_photo("https://example.com/p.jpg") == b"photo-bytes"
        mock_get.assert_called_once_with("https://example.com/p.jpg", timeout=30)

    def test_raises_on_http_error(self) -> None:
        with patch("app.connectors.garmin._requests.get") as mock_get:
            mock_get.return_value.raise_for_status.side_effect = OSError("HTTP 403")
            with pytest.raises(OSError):
                _download_garmin_photo("https://example.com/p.jpg")


class TestUploadPhotoToActivity:
    def test_calls_client_post_with_photo_file(self) -> None:
        client = MagicMock()
        client.garmin_connect_activity = "/activity-service/activity"
        _upload_photo_to_activity(client, 99999, b"photo-data", 1)
        client.client.post.assert_called_once()
        args, kwargs = client.client.post.call_args
        assert args[0] == "connectapi"
        assert args[1] == "/activity-service/activity/99999/image"
        assert kwargs["api"] is True
        name, file_obj, content_type = kwargs["files"]["file"]
        assert name == "photo_1.jpg"
        assert file_obj.read() == b"photo-data"
        assert content_type == "image/jpeg"


class TestDownloadActivityPhotos:
    async def test_media_empty_when_no_photos(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with patch(
            "app.connectors.garmin.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=_call_sync,
        ):
            result = await logged_in.download_activity(_make_meta())
        assert result.media == ()

    async def test_media_populated_from_photos(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                return_value=[_make_photo_dict()],
            ),
            patch(
                "app.connectors.garmin._download_garmin_photo",
                return_value=b"photo-bytes",
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in.download_activity(_make_meta())
        assert len(result.media) == 1
        assert result.media[0].content == b"photo-bytes"
        assert result.media[0].media_type == "photo"
        assert result.media[0].url == "https://example.com/p.jpg"

    async def test_photo_caption_stored(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                return_value=[_make_photo_dict(caption="Nice view")],
            ),
            patch(
                "app.connectors.garmin._download_garmin_photo",
                return_value=b"photo-bytes",
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in.download_activity(_make_meta())
        assert result.media[0].caption == "Nice view"

    async def test_photo_fetch_exception_returns_empty_and_logs_warning(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                side_effect=RuntimeError("api error"),
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in_with_log.download_activity(_make_meta())
        assert result.media == ()
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to fetch photo list" in m for m in msgs)
        assert any("api error" in m for m in msgs)

    async def test_photo_fetch_404_is_silent_debug_not_warning(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                side_effect=RuntimeError("HTTP Error 404 Not Found"),
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in_with_log.download_activity(_make_meta())
        assert result.media == ()
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        assert log.warning.call_count == 0  # type: ignore[attr-defined]
        debug_msgs = [c.args[0] for c in log.debug.call_args_list]  # type: ignore[attr-defined]
        assert any("no photos" in m for m in debug_msgs)

    async def test_photo_download_exception_skips_photo_and_logs_warning(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        photo_url = "https://cdn.garmin.com/photo_abc123.jpg"
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                return_value=[_make_photo_dict(url=photo_url)],
            ),
            patch(
                "app.connectors.garmin._download_garmin_photo",
                side_effect=OSError("download failed"),
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in_with_log.download_activity(_make_meta())
        assert result.media == ()
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("failed to download photo #1" in m for m in msgs)
        assert any("download failed" in m for m in msgs)
        assert not any(photo_url in m for m in msgs)

    async def test_photo_without_url_is_skipped(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                return_value=[{"caption": "no url here"}],
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in.download_activity(_make_meta())
        assert result.media == ()

    async def test_fallback_url_keys_used(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.return_value = _make_zip()
        with (
            patch(
                "app.connectors.garmin._list_activity_photos",
                return_value=[{"imageUrl": "https://example.com/image.jpg"}],
            ),
            patch(
                "app.connectors.garmin._download_garmin_photo",
                return_value=b"photo-bytes",
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            result = await logged_in.download_activity(_make_meta())
        assert len(result.media) == 1
        assert result.media[0].url == "https://example.com/image.jpg"

    async def test_zip_failure_cancels_photos_task(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.download_activity.side_effect = RuntimeError("network error")
        with (
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            pytest.raises(RuntimeError, match="network error"),
        ):
            await logged_in.download_activity(_make_meta())


class TestUploadActivityPhotos:
    async def test_photos_uploaded_after_activity(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(MediaItem(content=b"photo1", media_type="photo"),),
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity"
            ) as mock_upload_photo,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)
        mock_upload_photo.assert_called_once_with(mock_client, 99999, b"photo1", 1)

    async def test_upload_photo_failure_logs_warning_and_continues(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(
                MediaItem(content=b"photo1", media_type="photo"),
                MediaItem(content=b"photo2", media_type="photo"),
            ),
        )
        tracker = logged_in_with_log._tracker
        task_name = await tracker.add_task("Upload to garmin", total=2)
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity",
                side_effect=[OSError("upload failed"), None],
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in_with_log.upload_activity(activity, task_name=task_name)
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("photo #1 not uploaded" in m for m in msgs)
        assert not any("photo #2 not uploaded" in m for m in msgs)

    async def test_no_photo_upload_when_no_media(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity"
            ) as mock_upload_photo,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(_make_activity())
        mock_upload_photo.assert_not_called()

    async def test_no_photo_upload_when_activity_id_not_found(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        mock_client.get_activities_by_date.return_value = []
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(MediaItem(content=b"photo1", media_type="photo"),),
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity"
            ) as mock_upload_photo,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)
        mock_upload_photo.assert_not_called()

    async def test_video_media_skipped_logs_warning_and_shows_tracker_warning(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(MediaItem(content=b"vid", media_type="video"),),
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity"
            ) as mock_upload_photo,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch.object(
                logged_in_with_log._tracker, "warn", new_callable=AsyncMock
            ) as mock_warn,
        ):
            await logged_in_with_log.upload_activity(activity, task_name="upload #1")
        mock_upload_photo.assert_not_called()
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("skipped media #1" in m for m in msgs)
        assert any("video" in m for m in msgs)
        mock_warn.assert_awaited_once()
        warn_msg = mock_warn.call_args[0][1]
        assert "media #1" in warn_msg
        assert "video" in warn_msg

    async def test_failed_photo_upload_shows_tracker_warning(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(MediaItem(content=b"photo1", media_type="photo"),),
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity",
                side_effect=OSError("endpoint unavailable"),
            ),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch.object(
                logged_in_with_log._tracker, "warn", new_callable=AsyncMock
            ) as mock_warn,
        ):
            await logged_in_with_log.upload_activity(activity, task_name="upload #1")
        mock_warn.assert_awaited_once()
        warn_msg = mock_warn.call_args[0][1]
        assert "photo #1" in warn_msg
        assert "endpoint unavailable" in warn_msg

    async def test_photo_endpoint_unavailable_bails_on_first_404(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(
                MediaItem(content=b"photo1", media_type="photo"),
                MediaItem(content=b"photo2", media_type="photo"),
            ),
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity",
                side_effect=OSError("API Error 404 - Not Found"),
            ) as mock_upload,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)
        assert mock_upload.call_count == 1

    async def test_photo_endpoint_unavailable_warns_and_skips_remaining(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=(
                MediaItem(content=b"photo1", media_type="photo"),
                MediaItem(content=b"photo2", media_type="photo"),
            ),
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch(
                "app.connectors.garmin._upload_photo_to_activity",
                side_effect=OSError("API Error 404 - Not Found"),
            ) as mock_upload,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch.object(
                logged_in_with_log._tracker, "warn", new_callable=AsyncMock
            ) as mock_warn,
        ):
            await logged_in_with_log.upload_activity(activity, task_name="upload #1")
        assert mock_upload.call_count == 1
        mock_warn.assert_awaited_once()
        warn_msg = mock_warn.call_args.args[1]
        assert "endpoint unavailable" in warn_msg

    async def test_excess_media_truncated_to_max(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        media = tuple(
            MediaItem(content=f"p{i}".encode(), media_type="photo") for i in range(5)
        )
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=media,
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch("app.connectors.garmin._GARMIN_MAX_MEDIA", 3),
            patch(
                "app.connectors.garmin._upload_photo_to_activity"
            ) as mock_upload_photo,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)
        assert mock_upload_photo.call_count == 3

    async def test_excess_media_warn_via_log_and_tracker(
        self, logged_in_with_log: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        media = tuple(
            MediaItem(content=f"p{i}".encode(), media_type="photo") for i in range(5)
        )
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=media,
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch("app.connectors.garmin._GARMIN_MAX_MEDIA", 3),
            patch("app.connectors.garmin._upload_photo_to_activity"),
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
            patch.object(
                logged_in_with_log._tracker, "warn", new_callable=AsyncMock
            ) as mock_warn,
        ):
            await logged_in_with_log.upload_activity(activity, task_name="upload #1")
        log = logged_in_with_log._tracker.sync_logger
        assert log is not None
        msgs = [c.args[0] for c in log.warning.call_args_list]  # type: ignore[attr-defined]
        assert any("5 media files" in m for m in msgs)
        assert any("3" in m for m in msgs)
        mock_warn.assert_awaited_once()
        warn_msg = mock_warn.call_args.args[1]
        assert "5" in warn_msg
        assert "3" in warn_msg

    async def test_mixed_media_limit_applies_to_total_count(
        self, logged_in: GarminConnector, mock_client: MagicMock
    ) -> None:
        post = {"activityId": 99999, "startTimeGMT": "2026-01-01 08:00:00"}
        mock_client.get_activities_by_date.side_effect = [[], [post]]
        # 2 photos + 2 videos = 4 total; limit is 3 -> only first 3 passed to loop
        mixed = (
            MediaItem(content=b"p1", media_type="photo"),
            MediaItem(content=b"v1", media_type="video"),
            MediaItem(content=b"p2", media_type="photo"),
            MediaItem(content=b"v2", media_type="video"),
        )
        activity = Activity(
            external_id="12345",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-content",
            format="fit",
            media=mixed,
        )
        with (
            patch("app.connectors.garmin._UPLOAD_SETTLE_S", 0),
            patch("app.connectors.garmin._PHOTO_SETTLE_S", 0),
            patch("app.connectors.garmin._GARMIN_MAX_MEDIA", 3),
            patch(
                "app.connectors.garmin._upload_photo_to_activity"
            ) as mock_upload_photo,
            patch(
                "app.connectors.garmin.asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=_call_sync,
            ),
        ):
            await logged_in.upload_activity(activity)
        # Only photos from the first 3 items are uploaded (v1 is skipped as video)
        assert mock_upload_photo.call_count == 2


class TestGarminMediaUploadSupport:
    def test_supports_media_upload_is_true(self, connector: GarminConnector) -> None:
        assert connector.supports_media_upload is True

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    MediaItem,
    ServiceConnector,
    TransientDownloadError,
    _fetch_url_bytes,
    _redact_url,
    _run_with_timeout,
    attach_debug_logging,
)
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

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> None:
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


class TestUserLabel:
    def test_default_user_label_is_empty_string(
        self, connector: _FakeConnector
    ) -> None:
        assert connector.user_label == ""


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

    async def test_warns_via_tracker_when_media_upload_not_supported(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        connector.supports_media_upload = False
        activity = Activity(
            external_id="1",
            name="Morning Run",
            sport_type="running",
            start_time=_DT,
            content=b"fit-data",
            format="fit",
            media=(MediaItem(content=b"photo", media_type="photo"),),
        )

        await connector.upload_all([activity])

        task = _first_task(tracker)
        assert len(task.warnings) == 1
        assert "'1'" in task.warnings[0]
        assert "not uploaded" in task.warnings[0]

    async def test_no_warning_when_activity_has_no_media(
        self, connector: _FakeConnector, tracker: TaskTracker
    ) -> None:
        connector.supports_media_upload = False

        await connector.upload_all([_make_activity("1")])

        task = _first_task(tracker)
        assert task.warnings == []


class TestMediaItem:
    def test_valid_photo(self) -> None:
        item = MediaItem(content=b"img", media_type="photo")
        assert item.media_type == "photo"

    def test_valid_video(self) -> None:
        item = MediaItem(content=b"vid", media_type="video")
        assert item.media_type == "video"

    def test_raises_on_invalid_media_type(self) -> None:
        with pytest.raises(ValueError, match="media_type"):
            MediaItem(content=b"x", media_type="music")  # type: ignore[arg-type]

    def test_caption_defaults_to_none(self) -> None:
        item = MediaItem(content=b"x", media_type="photo")
        assert item.caption is None

    def test_url_defaults_to_empty_string(self) -> None:
        item = MediaItem(content=b"x", media_type="photo")
        assert item.url == ""

    def test_caption_and_url_stored(self) -> None:
        item = MediaItem(
            content=b"x",
            media_type="photo",
            caption="Great view",
            url="https://example.com/photo.jpg",
        )
        assert item.caption == "Great view"
        assert item.url == "https://example.com/photo.jpg"


class TestRedactUrl:
    def test_url_without_query_returned_unchanged(self) -> None:
        url = "https://example.com/api/v3/activities/123"
        assert _redact_url(url) == url

    def test_safe_params_left_unchanged(self) -> None:
        url = "https://example.com/api?after=1704067200&before=1706745600&per_page=200"
        assert _redact_url(url) == url

    def test_aws_signature_params_redacted(self) -> None:
        url = (
            "https://s3.example.com/file"
            "?X-Amz-Signature=abc123&X-Amz-Credential=KEY&X-Amz-Algorithm=AWS4"
        )
        result = _redact_url(url)
        assert "abc123" not in result
        assert "KEY" not in result
        assert result.count("REDACTED") == 3

    def test_cloudfront_params_redacted(self) -> None:
        url = "https://cdn.example.com/photo?Policy=eyJ&Signature=abc&Key-Pair-Id=APKA"
        result = _redact_url(url)
        assert "eyJ" not in result
        assert "abc" not in result
        assert "REDACTED" in result

    def test_only_sensitive_params_redacted(self) -> None:
        url = "https://example.com/api?page=1&X-Amz-Signature=secret"
        result = _redact_url(url)
        assert "page=1" in result
        assert "secret" not in result
        assert "REDACTED" in result

    def test_strava_oauth_token_params_redacted(self) -> None:
        url = (
            "https://www.strava.com/oauth/token"
            "?client_id=12345&client_secret=ACTUAL_SECRET"
            "&refresh_token=ACTUAL_REFRESH_TOKEN&grant_type=refresh_token"
        )
        result = _redact_url(url)
        assert "ACTUAL_SECRET" not in result
        assert "ACTUAL_REFRESH_TOKEN" not in result
        assert "client_id=12345" in result
        assert "grant_type=refresh_token" in result
        assert result.count("REDACTED") == 2


def _make_fake_send(status: int = 200, side_effect: Exception | None = None):
    """Return a mock suitable for use as session.send before attach_debug_logging."""
    fake_resp = MagicMock()
    fake_resp.status_code = status
    mock = MagicMock(return_value=fake_resp, side_effect=side_effect)
    return mock, fake_resp


class TestAttachDebugLogging:
    def test_logs_method_url_status_on_success(self) -> None:
        calls: list[str] = []
        session = requests.Session()
        # Replace send BEFORE attach so our mock becomes original_send in the closure.
        fake_send, _ = _make_fake_send(200)
        session.send = fake_send  # type: ignore[method-assign]
        attach_debug_logging(session, calls.append)

        prep = session.prepare_request(
            requests.Request("GET", "https://example.com/path")
        )
        session.send(prep)

        assert len(calls) == 1
        assert "GET" in calls[0]
        assert "example.com/path" in calls[0]
        assert "200" in calls[0]

    def test_logs_exception_type_on_network_failure(self) -> None:
        calls: list[str] = []
        session = requests.Session()
        fake_send, _ = _make_fake_send(side_effect=requests.ConnectionError("down"))
        session.send = fake_send  # type: ignore[method-assign]
        attach_debug_logging(session, calls.append)

        prep = session.prepare_request(
            requests.Request("GET", "https://example.com/path")
        )
        with pytest.raises(requests.ConnectionError):
            session.send(prep)

        assert len(calls) == 1
        assert "ConnectionError" in calls[0]
        assert "GET" in calls[0]

    def test_redacts_sensitive_query_params(self) -> None:
        calls: list[str] = []
        session = requests.Session()
        fake_send, _ = _make_fake_send(200)
        session.send = fake_send  # type: ignore[method-assign]
        attach_debug_logging(session, calls.append)

        prep = session.prepare_request(
            requests.Request(
                "GET", "https://cdn.example.com/photo?X-Amz-Signature=topsecret"
            )
        )
        session.send(prep)

        assert "topsecret" not in calls[0]
        assert "REDACTED" in calls[0]


class TestFetchUrlBytes:
    def test_returns_content_on_success(self) -> None:
        with patch("app.connectors.base.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.content = b"photo-data"
            mock_get.return_value.raise_for_status = MagicMock()

            result = _fetch_url_bytes("https://cdn.example.com/photo.jpg", 30)

        assert result == b"photo-data"

    def test_logs_status_on_success(self) -> None:
        calls: list[str] = []
        with patch("app.connectors.base.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.content = b"data"
            mock_get.return_value.raise_for_status = MagicMock()

            _fetch_url_bytes("https://example.com/file", 30, calls.append)

        assert len(calls) == 1
        assert "200" in calls[0]
        assert "GET" in calls[0]

    def test_logs_exception_type_on_failure(self) -> None:
        calls: list[str] = []
        with patch("app.connectors.base.requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("err")

            with pytest.raises(requests.ConnectionError):
                _fetch_url_bytes("https://example.com/file", 30, calls.append)

        assert len(calls) == 1
        assert "ConnectionError" in calls[0]

    def test_no_log_when_log_fn_is_none(self) -> None:
        with patch("app.connectors.base.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.content = b"data"
            mock_get.return_value.raise_for_status = MagicMock()

            result = _fetch_url_bytes("https://example.com/file", 30, None)

        assert result == b"data"

    def test_redacts_sensitive_params_in_log(self) -> None:
        calls: list[str] = []
        with patch("app.connectors.base.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            mock_get.return_value.content = b"data"
            mock_get.return_value.raise_for_status = MagicMock()

            _fetch_url_bytes(
                "https://cdn.example.com/photo?Signature=secret123",
                30,
                calls.append,
            )

        assert "secret123" not in calls[0]
        assert "REDACTED" in calls[0]


class TestRunWithTimeout:
    async def test_passes_result_through(self) -> None:
        async def immediate() -> int:
            return 42

        assert await _run_with_timeout(immediate()) == 42

    async def test_timeout_raises_transient_download_error(self) -> None:
        fut: asyncio.Future[None] = asyncio.Future()
        with patch(
            "app.connectors.base.asyncio.wait_for", side_effect=asyncio.TimeoutError
        ):
            with pytest.raises(TransientDownloadError, match="timed out after 30s"):
                await _run_with_timeout(fut)

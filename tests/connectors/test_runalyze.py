from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    UploadRefusedError,
)
from app.connectors.runalyze import _BASE_URL, RunalyzeConnector
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

from .api_fakes import FIT, FakeRenderer, calls, logged, resp

_TOKEN = "secret-personal-token"
_CREDENTIALS = Credentials(login="", password=_TOKEN)
_DT = datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def renderer() -> FakeRenderer:
    return FakeRenderer()


@pytest.fixture
def log() -> MagicMock:
    return MagicMock()


@pytest.fixture
def http() -> MagicMock:
    """Stands in for Session.request: every API call the connector makes."""
    return MagicMock()


@pytest.fixture
def connector(
    renderer: FakeRenderer, log: MagicMock, http: MagicMock
) -> RunalyzeConnector:
    c = RunalyzeConnector(_CREDENTIALS, TaskTracker(renderer, sync_logger=log))
    c._session = MagicMock(request=http)
    return c


def _activity() -> Activity:
    return Activity(
        external_id="g1",
        name="Morning Run",
        sport_type="running",
        start_time=_DT,
        elapsed_s=60,
        content=FIT,
        format="fit",
    )


def test_every_call_carries_the_token() -> None:
    c = RunalyzeConnector(_CREDENTIALS, TaskTracker(FakeRenderer()))
    assert c._session.headers["token"] == _TOKEN


def test_the_token_never_follows_a_redirect_elsewhere() -> None:
    session = RunalyzeConnector(_CREDENTIALS, TaskTracker(FakeRenderer()))._session
    first = session.prepare_request(requests.Request("POST", f"{_BASE_URL}/ping"))
    redirect = resp(302)
    redirect.request = first
    elsewhere = first.copy()
    elsewhere.prepare_url("https://example.com/ping", None)
    session.rebuild_auth(elsewhere, redirect)
    assert "token" not in elsewhere.headers


def test_debug_log_never_carries_the_token(log: MagicMock) -> None:
    c = RunalyzeConnector(_CREDENTIALS, TaskTracker(FakeRenderer(), log))
    with patch.object(requests.adapters.HTTPAdapter, "send") as send:
        send.return_value = resp(content=b"pong")
        c._session.get(f"{_BASE_URL}/ping")
    assert "[http] GET" in logged(log, "debug")
    assert _TOKEN not in logged(log, "debug")


async def test_login_checks_the_token(
    connector: RunalyzeConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(content=b"pong")
    await connector.login()
    assert calls(http) == [("GET", f"{_BASE_URL}/ping")]
    assert "Login: token accepted" in logged(log, "info")


@pytest.mark.parametrize(
    ("answer", "raised"),
    [
        (resp(content=b"No valid token"), PermissionError),
        (resp(401), requests.HTTPError),
    ],
)
async def test_a_refused_token_fails_the_login(
    connector: RunalyzeConnector,
    renderer: FakeRenderer,
    http: MagicMock,
    answer: requests.Response,
    raised: type[Exception],
) -> None:
    http.return_value = answer
    with pytest.raises(raised):
        await connector.login()
    assert len(renderer.failed) == 1


async def test_nothing_is_listed(connector: RunalyzeConnector, http: MagicMock) -> None:
    # Reading needs a paid plan; Runalyze skips a file it already has instead.
    assert await connector.list_activities(date(2026, 1, 1), date(2026, 12, 31)) == []
    http.assert_not_called()


async def test_nothing_can_be_downloaded(connector: RunalyzeConnector) -> None:
    meta = ActivityMeta(external_id="1", name="", sport_type="", start_time=_DT)
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(meta)


async def test_upload_sends_the_file(
    connector: RunalyzeConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(content=b"ok")
    assert await connector.upload_activity(_activity()) is None
    assert calls(http) == [("POST", f"{_BASE_URL}/activities/uploads")]
    assert http.call_args.kwargs["files"] == {"file": ("activity.fit", FIT)}
    assert "'g1' 2026-07-01 - queued" in logged(log, "info")


async def test_an_upload_with_a_refused_token_raises(
    connector: RunalyzeConnector, http: MagicMock
) -> None:
    http.return_value = resp(content=b'{"error": "No valid token"}')
    with pytest.raises(PermissionError):
        await connector.upload_activity(_activity())


@pytest.mark.parametrize("status", [413, 422])
async def test_a_file_runalyze_refuses_is_refused(
    connector: RunalyzeConnector, http: MagicMock, status: int
) -> None:
    # Sending it again gets the same answer; the engine passes over it.
    http.return_value = resp(status, content=b"File too large")
    with pytest.raises(UploadRefusedError, match=r"^refused \(File too large\)$"):
        await connector.upload_activity(_activity())


async def test_an_upload_the_server_cannot_take_now_raises(
    connector: RunalyzeConnector, http: MagicMock
) -> None:
    http.return_value = resp(500)
    with pytest.raises(requests.HTTPError):
        await connector.upload_activity(_activity())


async def test_a_429_without_retry_after_waits_a_minute(
    connector: RunalyzeConnector, http: MagicMock
) -> None:
    http.side_effect = [resp(429), resp(content=b"ok")]
    with patch("app.connectors.http_api.asyncio.sleep", new=AsyncMock()) as sleep:
        await connector.upload_activity(_activity())
    sleep.assert_awaited_once_with(60.0)


def test_upload_does_not_report_media() -> None:
    assert RunalyzeConnector.supports_media_upload is False

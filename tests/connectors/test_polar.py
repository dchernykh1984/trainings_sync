from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    TransientDownloadError,
)
from app.connectors.polar import _BASE_URL, PolarConnector, _seconds, _start
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

from .api_fakes import FIT, FakeRenderer, calls, logged, resp

_TOKEN = "secret-access-token"
_CREDENTIALS = Credentials(login="", password=_TOKEN)
_DT = datetime(2026, 7, 1, 7, 40, 2, tzinfo=timezone.utc)
_META = ActivityMeta(
    external_id="2AC312F", name="Running", sport_type="RUNNING", start_time=_DT
)


def _exercise(exercise_id: str, start: str | None, **fields: object) -> dict:
    return {
        "id": exercise_id,
        "start_time": start,
        "start_time_utc_offset": 180,
        "duration": "PT1H",
        "sport": "RUNNING",
    } | fields


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
) -> PolarConnector:
    c = PolarConnector(_CREDENTIALS, TaskTracker(renderer, sync_logger=log))
    c._session = MagicMock(request=http)
    return c


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("PT2H44M45S", 9885),
        ("PT59.5S", 59),
        ("P1DT1H", 90000),
        ("PT", None),
        ("PT0S", None),
        ("PT0.4S", None),
        ("", None),
        ("1 hour", None),
        (3600, None),
        (None, None),
    ],
)
def test_seconds_reads_an_iso_8601_duration(
    value: object, expected: int | None
) -> None:
    assert _seconds(value) == expected


@pytest.mark.parametrize(
    ("exercise", "expected"),
    [
        # Local time and its offset in minutes, in either spelling.
        ({"start_time": "2026-07-01T10:40:02", "start_time_utc_offset": 180}, _DT),
        ({"start-time": "2026-07-01T10:40:02", "start-time-utc-offset": 180}, _DT),
        ({"start_time": "2026-07-01T10:40:02+03:00", "start_time_utc_offset": 0}, _DT),
        ({"start_time": "2026-07-01T10:40:02"}, None),
        ({"start_time": "2026-07-01T10:40:02", "start_time_utc_offset": True}, None),
        ({"start_time": "yesterday", "start_time_utc_offset": 180}, None),
        ({"start_time": "2026-07-01T10:40:02", "start_time_utc_offset": 1440}, None),
        ({"start_time_utc_offset": 180}, None),
    ],
)
def test_start_is_the_local_time_moved_to_utc(
    exercise: dict, expected: datetime | None
) -> None:
    assert _start(exercise) == expected


# ---------------------------------------------------------------------------
# Auth and login
# ---------------------------------------------------------------------------


def test_every_call_carries_the_token_and_asks_for_json() -> None:
    c = PolarConnector(_CREDENTIALS, TaskTracker(FakeRenderer()))
    assert c._session.headers["Authorization"] == f"Bearer {_TOKEN}"
    assert c._session.headers["Accept"] == "application/json"


def test_debug_log_never_carries_the_token(log: MagicMock) -> None:
    c = PolarConnector(_CREDENTIALS, TaskTracker(FakeRenderer(), log))
    with patch.object(requests.adapters.HTTPAdapter, "send") as send:
        send.return_value = resp(body=[])
        c._session.get(f"{_BASE_URL}/exercises")
    assert "[http] GET" in logged(log, "debug")
    assert _TOKEN not in logged(log, "debug")


async def test_login_registers_the_user_under_the_default_member_id(
    connector: PolarConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(body={"polar-user-id": 1})
    await connector.login()
    assert calls(http) == [("POST", f"{_BASE_URL}/users")]
    assert http.call_args.kwargs["json"] == {"member-id": "trainings-sync"}
    assert connector.user_label == "trainings-sync"
    assert "Login: success (member id trainings-sync)" in logged(log, "info")


async def test_the_credential_login_is_the_member_id(http: MagicMock) -> None:
    c = PolarConnector(
        Credentials(login=" me ", password=_TOKEN), TaskTracker(FakeRenderer())
    )
    c._session = MagicMock(request=http)
    http.return_value = resp(body={})
    await c.login()
    assert http.call_args.kwargs["json"] == {"member-id": "me"}


async def test_a_user_registered_on_an_earlier_run_logs_in(
    connector: PolarConnector, log: MagicMock, http: MagicMock
) -> None:
    # A 409 also means the member id is another account's on this client.
    http.return_value = resp(409)
    await connector.login()
    assert "member id trainings-sync is already registered" in logged(log, "info")
    assert "give this one its own login" in logged(log, "info")


async def test_a_user_without_the_consents_gets_a_clear_error(
    connector: PolarConnector, renderer: FakeRenderer, http: MagicMock
) -> None:
    http.return_value = resp(403)
    with pytest.raises(PermissionError, match="mandatory consents"):
        await connector.login()
    assert len(renderer.failed) == 1


async def test_a_refused_token_fails_the_login(
    connector: PolarConnector, http: MagicMock
) -> None:
    http.return_value = resp(401)
    with pytest.raises(requests.HTTPError):
        await connector.login()


# ---------------------------------------------------------------------------
# list_activities
# ---------------------------------------------------------------------------


async def test_list_keeps_the_date_range_and_skips_sessions_without_a_start(
    connector: PolarConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(
        body=[
            _exercise("a", "2026-07-01T10:40:02", detailed_sport_info="ROAD_RUNNING"),
            _exercise("b", "2026-06-29T10:00:00"),  # before the range
            _exercise("c", None),
        ]
    )
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert calls(http) == [("GET", f"{_BASE_URL}/exercises")]
    assert metas == [
        ActivityMeta(
            external_id="a",
            name="Road running",
            sport_type="ROAD_RUNNING",
            start_time=_DT,
            elapsed_s=3600,
        )
    ]
    assert "1 sessions skipped without a start time" in logged(log, "info")


@pytest.mark.parametrize(
    ("local", "offset"),
    [
        ("2026-07-01T00:30:00", 180),  # still June 30 in UTC
        ("2026-07-01T23:30:00", -300),  # already July 2 in UTC
    ],
)
async def test_the_range_counts_the_day_where_the_session_took_place(
    connector: PolarConnector, http: MagicMock, local: str, offset: int
) -> None:
    http.return_value = resp(body=[_exercise("a", local, start_time_utc_offset=offset)])
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert [m.external_id for m in metas] == ["a"]


async def test_an_empty_answer_lists_nothing(
    connector: PolarConnector, http: MagicMock
) -> None:
    http.return_value = resp(204)
    assert await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1)) == []


async def test_list_failure_fails_the_task(
    connector: PolarConnector, renderer: FakeRenderer, http: MagicMock
) -> None:
    http.return_value = resp(500)
    with pytest.raises(requests.HTTPError):
        await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert len(renderer.failed) == 1


# ---------------------------------------------------------------------------
# download_activity / upload_activity
# ---------------------------------------------------------------------------


async def test_download_fetches_the_fit_file(
    connector: PolarConnector, http: MagicMock
) -> None:
    http.return_value = resp(content=FIT)
    activity = await connector.download_activity(_META)
    assert calls(http) == [("GET", f"{_BASE_URL}/exercises/2AC312F/fit")]
    assert http.call_args.kwargs["headers"] == {"Accept": "*/*"}
    assert (activity.format, activity.content, activity.name) == ("fit", FIT, "Running")


@pytest.mark.parametrize("status", [401, 403])
async def test_download_with_a_refused_token_fails_loudly(
    connector: PolarConnector, http: MagicMock, status: int
) -> None:
    http.return_value = resp(status)
    with pytest.raises(requests.HTTPError):
        await connector.download_activity(_META)


async def test_a_session_that_left_the_window_is_unavailable(
    connector: PolarConnector, http: MagicMock
) -> None:
    http.return_value = resp(404)
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(_META)


async def test_a_server_error_on_download_is_transient(
    connector: PolarConnector, http: MagicMock
) -> None:
    http.return_value = resp(502)
    with pytest.raises(TransientDownloadError):
        await connector.download_activity(_META)


async def test_a_session_without_a_fit_file_is_unavailable(
    connector: PolarConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(204)
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(_META)
    assert "no FIT file for this session, skipped" in logged(log, "warning")


def _limits(usage: str, limit: str, reset: str) -> dict[str, str]:
    return {
        "RateLimit-Usage": usage,
        "RateLimit-Limit": limit,
        "RateLimit-Reset": reset,
    }


@pytest.mark.parametrize(
    ("headers", "pause"),
    [
        # The 15-minute window is used up: wait for it to start over.
        (_limits("520, 1000", "520, 5100", "300, 40000"), 300.0),
        (_limits("600,1000", "520,5100", "12,40000"), 12.0),
        # It starts over right now: still a second, not three tries at once.
        (_limits("520, 1000", "520, 5100", "0, 40000"), 1.0),
        # The daily one: wait one short window at a time, not hours at once.
        (_limits("100, 5100", "520, 5100", "300, 40000"), 900.0),
        # Nothing says which window: a minute.
        ({}, 60.0),
        (_limits("100, 1000", "520, 5100", "300, 40000"), 60.0),
        (_limits("lots", "520, 5100", "300, 40000"), 60.0),
        ({"Retry-After": "5"}, 60.0),
    ],
)
async def test_a_429_waits_for_the_used_up_window(
    connector: PolarConnector, http: MagicMock, headers: dict, pause: float
) -> None:
    http.side_effect = [resp(429, headers=headers), resp(content=FIT)]
    with patch("app.connectors.http_api.asyncio.sleep", new=AsyncMock()) as sleep:
        await connector.download_activity(_META)
    sleep.assert_awaited_once_with(pause)


async def test_nothing_can_be_uploaded(connector: PolarConnector) -> None:
    activity = Activity(
        external_id="g1",
        name="Ride",
        sport_type="cycling",
        start_time=_DT,
        content=FIT,
        format="fit",
    )
    with pytest.raises(NotImplementedError):
        await connector.upload_activity(activity)

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
    UploadDeferredError,
    UploadRefusedError,
)
from app.connectors.ride_with_gps import (
    _BASE_URL,
    _PAGE_SIZE,
    _TASK_POLL_S,
    _TASK_POLLS,
    RideWithGpsConnector,
    _departed,
)
from app.credentials.base import Credentials
from app.tracking.tracker import TaskStatus, TaskTracker

from .api_fakes import FIT, FakeRenderer, calls, logged, resp

_KEY = "api-key"
_TOKEN = "secret-auth-token"
_CREDENTIALS = Credentials(login=f" {_KEY} ", password=_TOKEN)
_DT = datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc)
_META = ActivityMeta(
    external_id="77", name="Ride", sport_type="cycling:road", start_time=_DT
)
_TRIPS = f"{_BASE_URL}/trips.json"


def _trip(trip_id: int, departed_at: str | None, **fields: object) -> dict:
    return {
        "id": trip_id,
        "name": f"Trip {trip_id}",
        "departed_at": departed_at,
    } | fields


def _page(trips: list[dict], *, last: bool) -> requests.Response:
    next_url = None if last else f"{_TRIPS}?page=2"
    return resp(
        body={"trips": trips, "meta": {"pagination": {"next_page_url": next_url}}}
    )


def _task(status: str, **fields: object) -> requests.Response:
    task = {"id": 5, "status": status, "items": [], "errors": []} | fields
    return resp(202 if status == "pending" else 200, body={"task": task})


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
) -> RideWithGpsConnector:
    c = RideWithGpsConnector(_CREDENTIALS, TaskTracker(renderer, sync_logger=log))
    c._session = MagicMock(request=http)
    return c


@pytest.fixture
def no_sleep():
    with patch("app.connectors.ride_with_gps.asyncio.sleep", new=AsyncMock()) as sleep:
        yield sleep


def _activity(**overrides: object) -> Activity:
    fields: dict = {
        "external_id": "g1",
        "name": "Morning Ride",
        "sport_type": "cycling",
        "start_time": _DT,
        "elapsed_s": 60,
        "content": FIT,
        "format": "fit",
    }
    fields.update(overrides)
    return Activity(**fields)


# ---------------------------------------------------------------------------
# Auth and login
# ---------------------------------------------------------------------------


def test_every_call_carries_the_key_and_the_token() -> None:
    c = RideWithGpsConnector(_CREDENTIALS, TaskTracker(FakeRenderer()))
    assert c._session.headers["x-rwgps-api-key"] == _KEY
    assert c._session.headers["x-rwgps-auth-token"] == _TOKEN


def _redirected(session: requests.Session, url: str) -> requests.PreparedRequest:
    """The request a redirect from a trip download to `url` would send."""
    first = session.prepare_request(requests.Request("GET", f"{_BASE_URL}/trips/1.fit"))
    redirect = resp(302)
    redirect.request = first
    follow = first.copy()
    follow.prepare_url(url, None)
    session.rebuild_auth(follow, redirect)
    return follow


def test_the_key_and_the_token_never_follow_a_redirect_elsewhere() -> None:
    session = RideWithGpsConnector(_CREDENTIALS, TaskTracker(FakeRenderer()))._session
    elsewhere = _redirected(session, "https://files.example.com/1.fit")
    assert "x-rwgps-api-key" not in elsewhere.headers
    assert "x-rwgps-auth-token" not in elsewhere.headers
    home = _redirected(session, f"{_BASE_URL}/trips/2.fit")
    assert home.headers["x-rwgps-auth-token"] == _TOKEN


def test_debug_log_never_carries_the_token(log: MagicMock) -> None:
    c = RideWithGpsConnector(_CREDENTIALS, TaskTracker(FakeRenderer(), log))
    with patch.object(requests.adapters.HTTPAdapter, "send") as send:
        send.return_value = resp(body={"user": {"id": 1}})
        c._session.get(f"{_BASE_URL}/users/current.json")
    assert "[http] GET" in logged(log, "debug")
    assert _TOKEN not in logged(log, "debug")


async def test_login_names_the_account(
    connector: RideWithGpsConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(body={"user": {"id": 7, "name": "Alex Rider"}})
    await connector.login()
    assert calls(http) == [("GET", f"{_BASE_URL}/users/current.json")]
    assert connector.user_label == "Alex Rider"
    assert "Login: success (Alex Rider)" in logged(log, "info")


async def test_login_without_a_name_uses_the_user_id(
    connector: RideWithGpsConnector, http: MagicMock
) -> None:
    http.return_value = resp(body={"user": {"id": 7}})
    await connector.login()
    assert connector.user_label == "user 7"


async def test_a_refused_login_fails_the_task(
    connector: RideWithGpsConnector, renderer: FakeRenderer, http: MagicMock
) -> None:
    http.return_value = resp(401, body={"errors": ["Authentication failed"]})
    with pytest.raises(requests.HTTPError):
        await connector.login()
    assert [t.status for t in renderer.failed] == [TaskStatus.FAILED]


# ---------------------------------------------------------------------------
# list_activities
# ---------------------------------------------------------------------------


async def test_list_reads_every_page_and_keeps_the_date_range(
    connector: RideWithGpsConnector, log: MagicMock, http: MagicMock
) -> None:
    # The list has no date filter and no documented order.
    http.side_effect = [
        _page(
            [
                _trip(1, "2026-07-01T08:00:00-07:00"),
                _trip(2, "2026-06-30T23:00:00+00:00"),  # before the range
                _trip(3, None),  # no start time
            ],
            last=False,
        ),
        _page([_trip(4, "2026-07-02T06:00:00Z")], last=True),
    ]
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 2))
    assert [m.external_id for m in metas] == ["1", "4"]
    pages = [call.kwargs["params"] for call in http.call_args_list]
    assert pages == [
        {"page": 1, "page_size": _PAGE_SIZE},
        {"page": 2, "page_size": _PAGE_SIZE},
    ]
    assert "1 trips skipped without a start time" in logged(log, "info")


async def test_list_maps_a_trip_to_utc(
    connector: RideWithGpsConnector, http: MagicMock
) -> None:
    trip = _trip(
        1,
        "2026-07-01T08:00:00-07:00",
        activity_type="cycling:gravel",
        duration=7856.4,
        description="hot day",
    )
    old = _trip(2, "2026-06-01T08:00:00Z", description="long ago")
    http.return_value = _page([trip, old], last=True)
    (meta,) = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert meta == ActivityMeta(
        external_id="1",
        name="Trip 1",
        sport_type="cycling:gravel",
        start_time=datetime(2026, 7, 1, 15, 0, tzinfo=timezone.utc),
        elapsed_s=7856,
    )
    # Only a trip in the range keeps its description for the download.
    assert connector._descriptions == {"1": "hot day"}


@pytest.mark.parametrize(
    "departed_at",
    [
        "2026-07-01T00:30:00+03:00",  # still June 30 in UTC
        "2026-07-01T23:30:00-05:00",  # already July 2 in UTC
    ],
)
async def test_the_range_counts_the_day_where_the_trip_took_place(
    connector: RideWithGpsConnector, http: MagicMock, departed_at: str
) -> None:
    http.return_value = _page([_trip(1, departed_at)], last=True)
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert [m.external_id for m in metas] == ["1"]


@pytest.mark.parametrize(
    ("duration", "elapsed_s"),
    [
        ("3600.5", 3600),
        (3600, 3600),
        ("soon", None),
        ("nan", None),
        ("1e999", None),
        ("-3", None),
        (0, None),
        (None, None),
    ],
)
async def test_a_duration_that_does_not_read_leaves_the_trip_without_one(
    connector: RideWithGpsConnector,
    http: MagicMock,
    duration: object,
    elapsed_s: int | None,
) -> None:
    http.return_value = _page(
        [_trip(1, "2026-07-01T08:00:00Z", duration=duration)], last=True
    )
    (meta,) = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert meta.elapsed_s == elapsed_s


@pytest.mark.parametrize(
    "second_page",
    [
        [],  # empty, though the first page said there was more
        [_trip(1, "2026-07-01T08:00:00Z")],  # the first page again
    ],
)
async def test_paging_ends_at_a_page_with_nothing_new(
    connector: RideWithGpsConnector, http: MagicMock, second_page: list[dict]
) -> None:
    # A next_page_url that never runs out must not hang the run.
    http.side_effect = [
        _page([_trip(1, "2026-07-01T08:00:00Z")], last=False),
        _page(second_page, last=False),
        _page([_trip(3, "2026-07-01T09:00:00Z")], last=True),
    ]
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert [m.external_id for m in metas] == ["1"]
    assert http.call_count == 2


@pytest.mark.parametrize("meta", [None, {}, {"pagination": None}])
async def test_a_page_without_pagination_is_the_last(
    connector: RideWithGpsConnector, http: MagicMock, meta: object
) -> None:
    trips = [_trip(1, "2026-07-01T08:00:00Z")]
    http.return_value = resp(body={"trips": trips, "meta": meta})
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert [m.external_id for m in metas] == ["1"]
    assert http.call_count == 1


@pytest.mark.parametrize(
    ("departed_at", "expected"),
    [
        # Without an offset the time is taken as UTC.
        ("2026-07-01T08:00:00", datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc)),
        ("not a date", None),
        (12345, None),
        ("", None),
    ],
)
def test_departed_reads_only_a_usable_start(
    departed_at: object, expected: datetime | None
) -> None:
    assert _departed({"departed_at": departed_at}) == expected


async def test_list_failure_fails_the_task(
    connector: RideWithGpsConnector, renderer: FakeRenderer, http: MagicMock
) -> None:
    http.return_value = resp(500)
    with pytest.raises(requests.HTTPError):
        await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert len(renderer.failed) == 1


# ---------------------------------------------------------------------------
# download_activity
# ---------------------------------------------------------------------------


async def test_download_fetches_the_trip_as_fit(
    connector: RideWithGpsConnector, http: MagicMock
) -> None:
    connector._descriptions["77"] = "hot day"
    http.return_value = resp(content=FIT)
    activity = await connector.download_activity(_META)
    assert calls(http) == [("GET", f"{_BASE_URL}/trips/77.fit")]
    assert (activity.format, activity.content) == ("fit", FIT)
    assert activity.description == "hot day"


@pytest.mark.parametrize("status", [401, 403])
async def test_download_refused_to_the_account_fails_loudly(
    connector: RideWithGpsConnector, http: MagicMock, status: int
) -> None:
    # The list holds only the account's own trips: a 403 there is the account's.
    http.return_value = resp(status)
    with pytest.raises(requests.HTTPError):
        await connector.download_activity(_META)


async def test_a_trip_that_is_gone_is_unavailable(
    connector: RideWithGpsConnector, http: MagicMock
) -> None:
    http.return_value = resp(404)
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(_META)


@pytest.mark.parametrize("status", [408, 500, 503])
async def test_a_server_error_on_download_is_transient(
    connector: RideWithGpsConnector, http: MagicMock, status: int
) -> None:
    http.return_value = resp(status)
    with pytest.raises(TransientDownloadError):
        await connector.download_activity(_META)


async def test_an_answer_that_is_not_fit_is_unavailable(
    connector: RideWithGpsConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = resp(content=b"<html>maintenance</html>")
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(_META)
    assert "not a FIT file, skipped" in logged(log, "warning")


# ---------------------------------------------------------------------------
# upload_activity
# ---------------------------------------------------------------------------


async def test_upload_sends_the_file_with_its_name_and_description(
    connector: RideWithGpsConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
) -> None:
    created = [{"item_type": "trip", "item_id": 9, "item_url": "x"}]
    http.side_effect = [_task("pending"), _task("completed", items=created)]
    assert await connector.upload_activity(_activity(description="hot day")) is None
    post, poll = http.call_args_list
    assert (post.args[0], post.args[1]) == ("POST", _TRIPS)
    assert post.kwargs["data"] == {"name": "Morning Ride", "description": "hot day"}
    assert post.kwargs["files"] == {"file": ("activity.fit", FIT)}
    assert (poll.args[0], poll.args[1]) == ("GET", f"{_BASE_URL}/tasks/5.json")
    no_sleep.assert_awaited_once_with(_TASK_POLL_S)
    assert "success (id=9)" in logged(log, "info")


async def test_upload_without_a_name_or_description_sends_neither(
    connector: RideWithGpsConnector, no_sleep: AsyncMock, http: MagicMock
) -> None:
    http.return_value = _task("completed", items=[{"item_id": 9}])
    await connector.upload_activity(_activity(name=""))
    assert http.call_args.kwargs["data"] == {}


async def test_a_duplicate_upload_changes_nothing(
    connector: RideWithGpsConnector, log: MagicMock, http: MagicMock
) -> None:
    duplicate = {"code": "duplicate", "message": "This trip already exists"}
    http.return_value = _task("completed", errors=[duplicate])
    assert await connector.upload_activity(_activity()) is None
    assert "duplicate, skipped" in logged(log, "info")


@pytest.mark.parametrize(
    ("errors", "why"),
    [
        # The documented codes for a file it cannot make a trip of.
        *[
            ([{"code": code, "message": "no"}], "no trip made (no)")
            for code in ("time_data_missing", "no_tracks", "empty_file", "parse_failed")
        ],
        ([{"code": "parse_failed"}], "no trip made (parse_failed)"),
    ],
)
async def test_a_file_ride_with_gps_makes_no_trip_of_is_refused(
    connector: RideWithGpsConnector, http: MagicMock, errors: list[dict], why: str
) -> None:
    # Sending it again gets the same answer; the engine passes over it.
    http.return_value = _task("completed", errors=errors)
    with pytest.raises(UploadRefusedError) as refused:
        await connector.upload_activity(_activity())
    assert str(refused.value) == why


@pytest.mark.parametrize(
    "answers",
    [
        # The look at the result fails after the file was taken.
        [_task("pending"), resp(502)],
        [_task("pending"), requests.ConnectionError("reset")],
        [_task("pending"), resp(body={"unexpected": True})],
        # The upload is taken but the answer names no task.
        [resp(202, body={"accepted": True})],
        [resp(202, content=b"<html>queued</html>")],
        [resp(202, body=["queued"])],
        # The task is done, in a shape it was not expected in.
        [_task("completed", items=[123])],
        [_task("completed", items=5)],
        [resp(body={"task": "done"})],
    ],
)
async def test_a_result_that_cannot_be_read_counts_as_taken(
    connector: RideWithGpsConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
    answers: list,
) -> None:
    # Raising would stop every upload queued behind it for this run.
    http.side_effect = answers
    assert await connector.upload_activity(_activity()) is None
    assert "taken, its result could not be read" in logged(log, "warning")


@pytest.mark.parametrize(
    ("errors", "why"),
    [
        # The other documented codes, one it may add later, and plain text.
        *[
            ([{"code": code, "message": "no"}], "no trip made (no)")
            for code in (
                "failed_to_process",
                "weigh_in_failed",
                "build_failed",
                "error",
                "something_new",
            )
        ],
        (["Internal error"], "no trip made (Internal error)"),
    ],
)
async def test_a_failure_of_its_own_processing_waits_for_the_next_run(
    connector: RideWithGpsConnector, http: MagicMock, errors: list, why: str
) -> None:
    # It may not last, so the file is not given up on; nor does it stop the run.
    http.return_value = _task("completed", errors=errors)
    with pytest.raises(UploadDeferredError) as deferred:
        await connector.upload_activity(_activity())
    assert str(deferred.value) == why


@pytest.mark.parametrize("items", [[], [{"item_type": "trip", "item_id": None}]])
async def test_a_task_done_without_a_trip_or_a_reason_counts_as_taken(
    connector: RideWithGpsConnector, log: MagicMock, http: MagicMock, items: list
) -> None:
    # Nothing says the file was refused: the trip may not be linked yet.
    http.return_value = _task("completed", items=items)
    assert await connector.upload_activity(_activity()) is None
    assert "finished without naming a trip, counted as taken" in logged(log, "warning")


async def test_a_task_that_stays_pending_counts_as_taken(
    connector: RideWithGpsConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
) -> None:
    http.return_value = _task("pending")
    assert await connector.upload_activity(_activity()) is None
    assert http.call_count == 1 + _TASK_POLLS
    assert "taken, still being processed" in logged(log, "info")


@pytest.mark.parametrize("status", [413, 422])
async def test_a_file_the_server_refuses_is_refused(
    connector: RideWithGpsConnector, http: MagicMock, status: int
) -> None:
    http.return_value = resp(status, body={"errors": ["Empty file"]})
    with pytest.raises(UploadRefusedError) as refused:
        await connector.upload_activity(_activity())
    assert str(refused.value) == 'refused ({"errors": ["Empty file"]})'
    assert http.call_count == 1


async def test_a_request_the_server_refuses_raises(
    connector: RideWithGpsConnector, http: MagicMock
) -> None:
    # A 400 says the request itself is wrong, which would hold for every file.
    http.return_value = resp(400, body={"errors": ["No file provided"]})
    with pytest.raises(requests.HTTPError):
        await connector.upload_activity(_activity())


async def test_a_429_names_ride_with_gps_while_waiting(
    connector: RideWithGpsConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
) -> None:
    http.side_effect = [resp(429, headers={"Retry-After": "30"}), resp(content=FIT)]
    await connector.download_activity(_META)
    assert "[Ride with GPS] 429 received, pausing 30s" in logged(log, "warning")


async def test_upload_does_not_report_media(connector: RideWithGpsConnector) -> None:
    assert connector.supports_media_upload is False

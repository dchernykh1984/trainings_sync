from __future__ import annotations

import asyncio
import gzip
import ssl
from collections.abc import Coroutine
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests
import urllib3

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    CallTimeoutError,
    RateLimitError,
    TransientDownloadError,
    UploadRefusedError,
    _run_with_timeout,
)
from app.connectors.intervals_icu import (
    _BASE_URL,
    _CALL_TIMEOUT_S,
    _DEFAULT_RETRY_AFTER_S,
    _MAX_ATTEMPTS,
    _REQUEST_TIMEOUT_S,
    _STUB_MATCH_S,
    _UPLOAD_ATTEMPTS,
    _UPLOAD_CALL_TIMEOUT_S,
    _UPLOAD_RESPONSE_TIMEOUT_S,
    _UPLOAD_RETRY_PAUSE_S,
    IntervalsIcuConnector,
    _decode_file,
)
from app.core.cache import ActivityCache, CacheEntry
from app.core.planner import SourceSpec
from app.core.sync import SyncExecutor
from app.credentials.base import Credentials
from app.tracking.tracker import TaskStatus, TaskTracker

from .api_fakes import FIT as _FIT
from .api_fakes import FakeRenderer as _FakeRenderer
from .api_fakes import calls as _calls
from .api_fakes import logged as _logged
from .api_fakes import resp as _resp

_KEY = "secret-api-key"
_CREDENTIALS = Credentials(login="", password=_KEY)
_DT = datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)
_META = ActivityMeta(
    external_id="i101", name="Ride", sport_type="Ride", start_time=_DT, elapsed_s=60
)
_GPX = b'<?xml version="1.0"?><gpx version="1.1"><trk/></gpx>'
_TCX = b'<?xml version="1.0"?><TrainingCenterDatabase xmlns="x"/>'


@pytest.fixture
def renderer() -> _FakeRenderer:
    return _FakeRenderer()


@pytest.fixture
def log() -> MagicMock:
    return MagicMock()


@pytest.fixture
def http() -> MagicMock:
    """Stands in for Session.request: every API call the connector makes."""
    return MagicMock()


@pytest.fixture
def connector(
    renderer: _FakeRenderer, log: MagicMock, http: MagicMock
) -> IntervalsIcuConnector:
    c = IntervalsIcuConnector(_CREDENTIALS, TaskTracker(renderer, sync_logger=log))
    c._session = MagicMock(request=http)
    return c


@pytest.fixture
def no_sleep():
    with patch("app.connectors.intervals_icu.asyncio.sleep", new=AsyncMock()) as sleep:
        yield sleep


# ---------------------------------------------------------------------------
# Auth and login
# ---------------------------------------------------------------------------


def test_session_authenticates_with_the_api_key() -> None:
    c = IntervalsIcuConnector(_CREDENTIALS, TaskTracker(_FakeRenderer()))
    assert c._session.auth == ("API_KEY", _KEY)


def test_debug_log_never_carries_the_api_key(log: MagicMock) -> None:
    c = IntervalsIcuConnector(_CREDENTIALS, TaskTracker(_FakeRenderer(), log))
    with patch.object(requests.adapters.HTTPAdapter, "send") as send:
        send.return_value = _resp(200, body={"id": "i1"})
        c._session.get(f"{_BASE_URL}/athlete/0")
    assert "[http] GET" in _logged(log, "debug")
    assert _KEY not in _logged(log, "debug")


async def test_login_reads_the_key_owner_by_default(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = _resp(body={"id": "i42", "name": "Denis"})
    await connector.login()
    assert _calls(http) == [("GET", f"{_BASE_URL}/athlete/0")]
    assert connector.user_label == "Denis"
    assert "success (athlete_id=i42, Denis)" in _logged(log, "info")


async def test_the_credential_login_names_the_athlete(http: MagicMock) -> None:
    c = IntervalsIcuConnector(
        Credentials(login=" i42 ", password=_KEY), TaskTracker(_FakeRenderer())
    )
    http = MagicMock(return_value=_resp(body={"id": "i42"}))
    c._session = MagicMock(request=http)
    await c.login()
    assert _calls(http) == [("GET", f"{_BASE_URL}/athlete/i42")]
    assert c.user_label == "i42"  # no name: fall back to the id


async def test_login_labels_the_key_owner_by_the_id_it_returns(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    assert connector.user_label == "0"
    http.return_value = _resp(body={"id": "i42", "name": ""})
    await connector.login()
    assert connector.user_label == "i42"


async def test_login_rejected_key_fails_the_task(
    connector: IntervalsIcuConnector, renderer: _FakeRenderer, http: MagicMock
) -> None:
    http.return_value = _resp(401)
    with pytest.raises(requests.HTTPError):
        await connector.login()
    assert renderer.failed[0].status == TaskStatus.FAILED
    assert "Login failed" in (renderer.failed[0].error or "")


# ---------------------------------------------------------------------------
# list_activities
# ---------------------------------------------------------------------------


async def test_list_asks_for_the_whole_date_range(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(body=[])
    await connector.list_activities(date(2026, 1, 1), date(2026, 1, 31))
    call = http.call_args
    assert call.args[1] == f"{_BASE_URL}/athlete/0/activities"
    params = call.kwargs["params"]
    assert params["oldest"] == "2026-01-01"
    assert params["newest"] == "2026-01-31T23:59:59"  # the end day in full
    assert "start_date" in params["fields"].split(",")


async def test_list_maps_activities_to_utc_metas(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(
        body=[
            {
                "id": "i101",
                "name": "Morning Ride",
                "type": "Ride",
                "start_date": "2026-01-01T08:00:00Z",
                "elapsed_time": 3600,
            },
            {"id": 7, "start_date": "2026-01-02T10:00:00+02:00", "name": None},
            {"id": "i103", "start_date": "2026-01-03T06:30:00"},
        ]
    )
    metas = await connector.list_activities(date(2026, 1, 1), date(2026, 1, 31))
    assert metas[0] == ActivityMeta(
        external_id="i101",
        name="Morning Ride",
        sport_type="Ride",
        start_time=_DT,
        elapsed_s=3600,
    )
    assert metas[1].external_id == "7"
    assert metas[1].name == ""
    assert metas[1].start_time == datetime(2026, 1, 2, 8, 0, tzinfo=timezone.utc)
    assert metas[1].elapsed_s is None
    assert metas[2].start_time == datetime(2026, 1, 3, 6, 30, tzinfo=timezone.utc)


_STUB = {"id": "1234", "source": "STRAVA", "start_date_local": "2026-07-01T10:00:00"}


async def _login_in(
    connector: IntervalsIcuConnector, http: MagicMock, tz: object
) -> None:
    http.return_value = _resp(body={"id": "i42", "timezone": tz})
    await connector.login()


async def test_a_strava_stub_is_listed_as_present_but_never_downloaded(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    # The API cannot serve a Strava import, but a destination still has to see
    # it, or the same ride is uploaded a second time.
    await _login_in(connector, http, "Europe/Berlin")
    http.return_value = _resp(body=[_STUB])
    [stub] = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert stub.downloadable is False
    # 10:00 in Berlin is 08:00 UTC in summer.
    assert stub.start_time == datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc)
    assert stub.elapsed_s == _STUB_MATCH_S
    assert "1 Strava imports listed as present only" in _logged(log, "info")


async def test_a_strava_stub_keeps_the_same_ride_from_being_uploaded_again(
    connector: IntervalsIcuConnector, http: MagicMock, tmp_path: Path
) -> None:
    await _login_in(connector, http, "Europe/Berlin")
    cache = ActivityCache(tmp_path / "cache")
    cache.load()
    rides = {
        "g1": datetime(2026, 7, 1, 8, 0, 20, tzinfo=timezone.utc),  # the stub's ride
        "g2": datetime(2026, 7, 1, 9, 30, tzinfo=timezone.utc),  # not on Strava
    }
    for external_id, start in rides.items():
        entry = CacheEntry(
            external_id=external_id,
            source_id="garmin",
            format="fit",
            start_time=start,
            elapsed_s=3600,
            name="Ride",
            sport_type="cycling",
        )
        cache.put(entry, external_id.encode())
    garmin = MagicMock(user_label="", _max_concurrent=1)
    garmin.list_activities = AsyncMock(return_value=[])
    http.side_effect = [
        _resp(body=[_STUB]),
        _resp(201, body={"activities": [{"id": "i9"}]}),
    ]
    executor = SyncExecutor(
        sources=[(SourceSpec(source_id="garmin", priority=1), garmin)],
        destinations=[("intervals", connector)],
        cache=cache,
    )
    await executor.run(date(2026, 7, 1), date(2026, 7, 1))
    [post] = [c for c in http.call_args_list if c.args[0] == "POST"]
    assert post.kwargs["files"]["file"][1] == b"g2"
    marked = cache.get_entry("g1", "garmin")
    assert marked is not None
    assert "intervals" in marked.uploaded_to


@pytest.mark.parametrize("tz", [None, "", "Not/AZone", 5])
async def test_a_strava_stub_without_a_known_time_zone_is_dropped(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock, tz: object
) -> None:
    await _login_in(connector, http, tz)
    http.return_value = _resp(body=[_STUB])
    assert await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1)) == []
    # Duplicates may follow, so the cause has to be named.
    assert "no usable time zone, so 1 Strava imports" in _logged(log, "warning")


async def test_activities_without_a_start_time_are_skipped(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    await _login_in(connector, http, "UTC")
    http.return_value = _resp(
        body=[
            {"id": "1235", "source": "STRAVA"},
            {"id": "i102", "source": "UPLOAD", "start_date_local": "2026-07-01T09:00"},
            {"id": "i101", "source": "UPLOAD", "start_date": "2026-07-01T08:00:00Z"},
        ]
    )
    metas = await connector.list_activities(date(2026, 7, 1), date(2026, 7, 1))
    assert [m.external_id for m in metas] == ["i101"]
    assert metas[0].downloadable is True
    assert "2 skipped without a usable start time" in _logged(log, "info")
    assert _logged(log, "warning") == ""  # the time zone is known


async def test_list_failure_fails_the_task(
    connector: IntervalsIcuConnector, renderer: _FakeRenderer, http: MagicMock
) -> None:
    http.return_value = _resp(500)
    with pytest.raises(requests.HTTPError):
        await connector.list_activities(date(2026, 1, 1), date(2026, 1, 31))
    assert renderer.failed


# ---------------------------------------------------------------------------
# download_activity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "fmt"),
    [(_FIT, "fit"), (_GPX, "gpx"), (_TCX, "tcx"), (gzip.compress(_GPX), "gpx")],
)
def test_decode_file_detects_the_format(raw: bytes, fmt: str) -> None:
    decoded = _decode_file(raw)
    assert decoded is not None
    assert decoded[1] == fmt
    assert decoded[0] in (_FIT, _GPX, _TCX)


@pytest.mark.parametrize(
    "raw", [b"", b"PK\x03\x04zip", b"\x1f\x8bnot really gzip", b"<html></html>"]
)
def test_decode_file_rejects_what_is_not_an_activity(raw: bytes) -> None:
    assert _decode_file(raw) is None


async def test_download_fetches_the_original_file(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(content=gzip.compress(_FIT))
    activity = await connector.download_activity(_META)
    assert _calls(http) == [("GET", f"{_BASE_URL}/activity/i101/file")]
    assert activity == Activity(
        external_id="i101",
        name="Ride",
        sport_type="Ride",
        start_time=_DT,
        elapsed_s=60,
        content=_FIT,
        format="fit",
    )


async def test_download_reuses_the_description_from_the_listing(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.side_effect = [
        _resp(
            body=[
                {
                    "id": "i101",
                    "start_date": "2026-01-01T08:00:00Z",
                    "description": "Easy spin",
                }
            ]
        ),
        _resp(content=_GPX),
    ]
    [meta] = await connector.list_activities(date(2026, 1, 1), date(2026, 1, 31))
    activity = await connector.download_activity(meta)
    assert activity.description == "Easy spin"
    assert activity.format == "gpx"
    assert http.call_count == 2


@pytest.mark.parametrize("status", [401, 403])
async def test_download_with_a_refused_key_fails_loudly(
    connector: IntervalsIcuConnector, http: MagicMock, status: int
) -> None:
    # Skipping here would report a clean run that downloaded nothing.
    http.return_value = _resp(status)
    with pytest.raises(requests.HTTPError):
        await connector.download_activity(_META)


@pytest.mark.parametrize("status", [400, 404, 422])
async def test_download_without_a_file_is_unavailable(
    connector: IntervalsIcuConnector, status: int, http: MagicMock
) -> None:
    http.return_value = _resp(status)
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(_META)


async def test_download_of_an_unknown_format_is_unavailable(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = _resp(content=b"PK\x03\x04zip")
    with pytest.raises(ActivityUnavailableError):
        await connector.download_activity(_META)
    assert "not FIT/GPX/TCX" in _logged(log, "warning")


@pytest.mark.parametrize("status", [408, 500, 502])
async def test_download_server_error_is_transient(
    connector: IntervalsIcuConnector, http: MagicMock, status: int
) -> None:
    http.return_value = _resp(status)
    with pytest.raises(TransientDownloadError):
        await connector.download_activity(_META)


async def test_network_error_is_transient(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.side_effect = requests.ConnectionError("reset")
    with pytest.raises(TransientDownloadError, match="reset"):
        await connector.download_activity(_META)


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


async def test_429_waits_retry_after_then_succeeds(
    connector: IntervalsIcuConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
) -> None:
    http.side_effect = [
        _resp(429, headers={"Retry-After": "370"}),
        _resp(content=_FIT),
    ]
    activity = await connector.download_activity(_META)
    assert activity.format == "fit"
    no_sleep.assert_awaited_once_with(370.0)
    assert "429 received, pausing 370s" in _logged(log, "warning")


async def test_429_without_retry_after_uses_the_default_pause(
    connector: IntervalsIcuConnector, no_sleep: AsyncMock, http: MagicMock
) -> None:
    http.side_effect = [_resp(429), _resp(body=[])]
    await connector.list_activities(date(2026, 1, 1), date(2026, 1, 31))
    no_sleep.assert_awaited_once_with(_DEFAULT_RETRY_AFTER_S)


async def test_429_on_every_attempt_hands_the_pause_to_the_engine(
    connector: IntervalsIcuConnector, no_sleep: AsyncMock, http: MagicMock
) -> None:
    # A RateLimitError makes the engine pause every download of this source.
    http.return_value = _resp(429, headers={"Retry-After": "900"})
    with pytest.raises(RateLimitError, match="429") as exc:
        await connector.download_activity(_META)
    assert exc.value.retry_after == 900.0
    assert http.call_count == _MAX_ATTEMPTS
    assert no_sleep.await_count == _MAX_ATTEMPTS - 1  # no pause before giving up


# ---------------------------------------------------------------------------
# upload_activity
# ---------------------------------------------------------------------------


_POST = ("POST", f"{_BASE_URL}/athlete/0/activities")


def _wrapped(error_type: type[Exception], reason: Exception) -> Exception:
    """What requests raises for a failure urllib3 reports with `reason`."""
    return error_type(urllib3.exceptions.MaxRetryError(MagicMock(), "/api", reason))


# A refused connection or a name that did not resolve.
_REFUSED = _wrapped(
    requests.ConnectionError, urllib3.exceptions.NewConnectionError(MagicMock(), "x")
)
# A TLS connection dropped while the file was going out.
_TLS_DROPPED = _wrapped(
    requests.exceptions.SSLError,
    urllib3.exceptions.SSLError(ssl.SSLEOFError(8, "EOF in violation of protocol")),
)


def _activity(**overrides: object) -> Activity:
    fields: dict = {
        "external_id": "g1",
        "name": "Morning Run",
        "sport_type": "running",
        "start_time": _DT,
        "elapsed_s": 60,
        "content": _FIT,
        "format": "fit",
    }
    fields.update(overrides)
    return Activity(**fields)


async def test_upload_posts_the_file_with_its_name(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = _resp(201, body={"id": "i9", "activities": [{"id": "i9"}]})
    assert await connector.upload_activity(_activity()) is None
    call = http.call_args
    assert (call.args[0], call.args[1]) == (
        "POST",
        f"{_BASE_URL}/athlete/0/activities",
    )
    assert call.kwargs["params"] == {"name": "Morning Run"}
    assert call.kwargs["files"] == {"file": ("activity.fit", _FIT)}
    assert "success (id=i9)" in _logged(log, "info")


async def test_upload_without_a_name_sends_no_name(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(201, body={"activities": []})
    await connector.upload_activity(_activity(name=""))
    assert http.call_args.kwargs["params"] is None


async def test_upload_sets_the_description_in_a_separate_update(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.side_effect = [
        _resp(201, body={"activities": [{"id": "i9"}, {"id": "i10"}]}),
        _resp(body={"id": "i9"}),
    ]
    await connector.upload_activity(_activity(description="x" * 5000))
    put = http.call_args
    assert (put.args[0], put.args[1]) == ("PUT", f"{_BASE_URL}/activity/i9")
    assert put.kwargs["json"] == {"description": "x" * 5000}
    assert "description" not in (http.call_args_list[0].kwargs["params"] or {})


async def test_upload_of_a_duplicate_changes_nothing(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = _resp(200, content=b"")  # the body is not even read
    await connector.upload_activity(_activity(description="kept as is"))
    assert http.call_count == 1  # no description update
    assert "duplicate, skipped" in _logged(log, "info")


async def test_failed_description_update_keeps_the_upload(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.side_effect = [
        _resp(201, body={"activities": [{"id": "i9"}]}),
        _resp(500),
    ]
    assert await connector.upload_activity(_activity(description="d")) is None
    assert "description not set (500" in _logged(log, "warning")


async def test_description_without_an_uploaded_id_is_reported(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = _resp(201, body={"activities": []})
    await connector.upload_activity(_activity(description="d"))
    assert http.call_count == 1
    assert "activity ID unavailable" in _logged(log, "warning")


async def test_upload_rejected_by_the_server_raises(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(400)
    with pytest.raises(requests.HTTPError):
        await connector.upload_activity(_activity())
    assert http.call_count == 1  # a 4xx does not change on a second try


@pytest.mark.parametrize("status", [413, 422])
async def test_a_file_intervals_icu_refuses_is_refused_with_the_reason(
    connector: IntervalsIcuConnector, http: MagicMock, status: int
) -> None:
    # A file with no data points gets a 422 for good; the engine passes over it.
    http.return_value = _resp(status, content=b'{"error":\n  "No data in file"}')
    with pytest.raises(UploadRefusedError) as refused:
        await connector.upload_activity(_activity())
    assert str(refused.value) == 'refused ({"error": "No data in file"})'
    assert http.call_count == 1  # sending it again gets the same answer


async def test_a_refusal_without_a_body_gives_the_status_line(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(422)
    with pytest.raises(UploadRefusedError, match=r"^refused \(422 Client Error"):
        await connector.upload_activity(_activity())


async def test_upload_waits_longer_for_its_answer_than_other_calls(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(201, body={"activities": [{"id": "i9"}]})
    await connector.upload_activity(_activity())
    assert http.call_args.kwargs["timeout"] == (
        _REQUEST_TIMEOUT_S,
        _UPLOAD_RESPONSE_TIMEOUT_S,
    )


@pytest.mark.parametrize(
    "failure",
    [
        # No answer, or one cut off.
        requests.ReadTimeout("read timed out"),
        requests.ConnectionError("reset"),
        requests.exceptions.ChunkedEncodingError("cut off"),
        requests.exceptions.ContentDecodingError("truncated gzip"),
        _TLS_DROPPED,
        # A server error, from intervals.icu or a gateway in front of it.
        _resp(504),
        _resp(524),
        _resp(502),
        _resp(408),
        # Nothing reached the server.
        requests.ConnectTimeout("connect timed out"),
        _REFUSED,
    ],
)
async def test_upload_is_sent_again_after_no_answer_or_a_server_error(
    connector: IntervalsIcuConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
    failure: object,
) -> None:
    # One pause for every failure: a server that may still be at work on the
    # file gets time to finish it before the next send.
    http.side_effect = [failure, _resp(201, body={"activities": [{"id": "i9"}]})]
    assert await connector.upload_activity(_activity()) is None
    assert _calls(http) == [_POST, _POST]
    no_sleep.assert_awaited_once_with(_UPLOAD_RETRY_PAUSE_S)
    assert f"attempt 1/{_UPLOAD_ATTEMPTS} failed" in _logged(log, "warning")
    assert "success (id=i9)" in _logged(log, "info")


@pytest.mark.parametrize(
    "failure",
    [
        requests.ReadTimeout("read timed out"),
        _resp(504),
        _TLS_DROPPED,
        _REFUSED,
        _resp(502),
    ],
)
async def test_a_200_after_a_failed_try_leaves_the_activity_as_it_is(
    connector: IntervalsIcuConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
    failure: object,
) -> None:
    # The 200 cannot tell this upload's earlier try from an activity that was
    # there before and may have been edited, so nothing is written over it.
    http.side_effect = [failure, _resp(200, body={"activities": [{"id": "i7"}]})]
    assert await connector.upload_activity(_activity(description="d")) is None
    assert _calls(http) == [_POST, _POST]  # no description update
    assert "duplicate, skipped" in _logged(log, "info")
    assert "left as it is: description not set" in _logged(log, "warning")


async def test_a_failed_try_shows_on_the_upload_task(
    connector: IntervalsIcuConnector,
    renderer: _FakeRenderer,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
) -> None:
    # A wait of minutes must not look like a hung upload.
    task = await connector._tracker.add_task("Upload to intervals.icu", total=1)
    http.side_effect = [
        requests.ReadTimeout("read timed out"),
        _resp(201, body={"activities": [{"id": "i9"}]}),
    ]
    await connector.upload_activity(_activity(), task_name=task)
    attempt = f"attempt 1/{_UPLOAD_ATTEMPTS} failed"
    assert any(attempt in w for w in renderer.warnings)
    assert _logged(log, "warning").count(attempt) == 1  # by the tracker alone


@pytest.mark.parametrize("body", [b"", b"[]", b'{"activities": []}'])
async def test_an_answer_without_a_usable_id_still_counts_as_uploaded(
    connector: IntervalsIcuConnector,
    log: MagicMock,
    no_sleep: AsyncMock,
    http: MagicMock,
    body: bytes,
) -> None:
    # Reading the id must not turn an upload that went through into a failure.
    http.return_value = _resp(201, content=body)
    assert await connector.upload_activity(_activity(description="d")) is None
    assert "activity ID unavailable" in _logged(log, "warning")
    assert "success (id=None)" in _logged(log, "info")


async def test_only_an_upload_gets_the_longer_call_bound(
    connector: IntervalsIcuConnector,
    monkeypatch: pytest.MonkeyPatch,
    http: MagicMock,
) -> None:
    bounds: list[float] = []

    async def spy(coro: Coroutine[Any, Any, Any], timeout_s: float) -> Any:
        bounds.append(timeout_s)
        return await _run_with_timeout(coro, timeout_s)

    monkeypatch.setattr("app.connectors.http_api._run_with_timeout", spy)
    http.side_effect = [_resp(body=[]), _resp(201, body={"activities": [{"id": "i9"}]})]
    await connector.list_activities(date(2026, 1, 1), date(2026, 1, 1))
    await connector.upload_activity(_activity())
    assert bounds == [_CALL_TIMEOUT_S, _UPLOAD_CALL_TIMEOUT_S]


async def test_an_upload_cut_off_by_the_call_bound_is_not_sent_again(
    connector: IntervalsIcuConnector,
    monkeypatch: pytest.MonkeyPatch,
    http: MagicMock,
) -> None:
    # Its thread may still be sending, and a second send could slip past the
    # server's duplicate check.
    async def call_bound_passes(coro: Coroutine[Any, Any, Any], timeout: float) -> Any:
        coro.close()  # never started, so no thread is left behind
        raise asyncio.TimeoutError

    monkeypatch.setattr("app.connectors.base.asyncio.wait_for", call_bound_passes)
    with pytest.raises(CallTimeoutError):
        await connector.upload_activity(_activity())
    http.assert_not_called()


@pytest.mark.parametrize(
    ("failure", "raised"),
    [
        (requests.ReadTimeout("read timed out"), TransientDownloadError),
        (_resp(503), requests.HTTPError),
    ],
)
async def test_upload_gives_up_after_the_last_attempt(
    connector: IntervalsIcuConnector,
    no_sleep: AsyncMock,
    http: MagicMock,
    failure: object,
    raised: type[Exception],
) -> None:
    http.side_effect = [failure] * _UPLOAD_ATTEMPTS
    with pytest.raises(raised):
        await connector.upload_activity(_activity())
    assert _calls(http) == [_POST] * _UPLOAD_ATTEMPTS
    assert [c.args[0] for c in no_sleep.await_args_list] == [_UPLOAD_RETRY_PAUSE_S] * (
        _UPLOAD_ATTEMPTS - 1
    )


async def test_upload_hit_by_the_rate_limit_is_not_sent_again(
    connector: IntervalsIcuConnector, no_sleep: AsyncMock, http: MagicMock
) -> None:
    http.return_value = _resp(429, headers={"Retry-After": "60"})
    with pytest.raises(RateLimitError):
        await connector.upload_activity(_activity())
    assert http.call_count == _MAX_ATTEMPTS  # only the 429 waits, no second round


async def test_upload_does_not_report_media(connector: IntervalsIcuConnector) -> None:
    assert connector.supports_media_upload is False


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def test_delete_removes_one_activity(
    connector: IntervalsIcuConnector, log: MagicMock, http: MagicMock
) -> None:
    http.return_value = _resp(body={"id": "i101"})
    await connector.delete_activity("i101")
    assert _calls(http) == [("DELETE", f"{_BASE_URL}/activity/i101")]
    assert "Delete" in _logged(log, "info")
    assert "'i101'" in _logged(log, "info")


async def test_a_refused_delete_raises(
    connector: IntervalsIcuConnector, http: MagicMock
) -> None:
    http.return_value = _resp(404)
    with pytest.raises(requests.HTTPError):
        await connector.delete_activity("i101")

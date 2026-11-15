"""intervals.icu connector: activities in and out through its public REST API.

Auth is HTTP Basic with the literal user name "API_KEY" and the athlete's
personal API key (intervals.icu Settings -> Developer Settings). The key never
rotates, so unlike Strava it can live in KeePass.
"""

from __future__ import annotations

import asyncio
import gzip
import zlib
from datetime import date, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    CallTimeoutError,
    RateLimitError,
    TransientDownloadError,
)
from app.connectors.http_api import (
    HttpApiConnector,
    _is_fit,
    _parse_start,
    _transient_status,
)
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

_BASE_URL = "https://intervals.icu/api/v1"
# Socket timeout: catches a connection that stalls.
_REQUEST_TIMEOUT_S: float = 30.0
# Whole-call bound: a big file on a slow link can take minutes and still move.
_CALL_TIMEOUT_S: float = 300.0
# intervals.icu analyses an uploaded file before it answers, so a slow answer
# to an upload is not a dead connection: wait longer than the socket timeout.
_UPLOAD_RESPONSE_TIMEOUT_S: float = 120.0
# An upload's whole-call bound also covers sending the file and that wait.
_UPLOAD_CALL_TIMEOUT_S: float = 600.0
_UPLOAD_ATTEMPTS: int = 3
# Pause before sending a failed upload again: a server too slow to answer may
# still be analysing the file, so it gets as long again to finish first.
_UPLOAD_RETRY_PAUSE_S: float = _UPLOAD_RESPONSE_TIMEOUT_S
_MAX_ATTEMPTS: int = 3
# A 429 from the per-IP limit (10 calls/s) carries no Retry-After header.
_DEFAULT_RETRY_AFTER_S: float = 5.0
# Without a field list every activity in the listing carries its full analysis.
_LIST_FIELDS = (
    "id,name,type,start_date,start_date_local,elapsed_time,description,source"
)
# Activities intervals.icu imported from Strava are API stubs: an id, a local
# start time and no file.
_STRAVA_SOURCE = "STRAVA"
# A stub has no duration. A Strava import starts when its original recording
# does, so for overlap matching a stub stands for its first two minutes only.
_STUB_MATCH_S: int = 120


def _zone(name: object) -> tzinfo | None:
    """The athlete's time zone, or None when it is missing or unknown here."""
    if not isinstance(name, str) or not name:
        return None
    try:
        return ZoneInfo(name)
    except (KeyError, ValueError):  # ZoneInfoNotFoundError is a KeyError
        return None


def _worth_resending(exc: Exception) -> bool:
    """Whether sending a failed upload again may help: not after a 4xx, not
    after a 429 that _request has already waited out, and not past the call
    bound, where the send may still be running in its thread and a second one
    alongside it could slip past the server's duplicate check."""
    if isinstance(exc, (RateLimitError, CallTimeoutError)):
        return False
    if isinstance(exc, requests.HTTPError):
        return _transient_status(exc)
    return True  # no answer, or one cut off


def _first_id(resp: requests.Response) -> str | None:
    """The first activity id an upload answer names, if it names one."""
    try:
        return str(resp.json()["activities"][0]["id"])
    except (ValueError, LookupError, TypeError):  # not JSON, or not that shape
        return None


def _decode_file(content: bytes) -> tuple[bytes, str] | None:
    """Return (file bytes, fit/gpx/tcx) for an original file, None if unusable."""
    if content[:2] == b"\x1f\x8b":  # intervals.icu may serve the file gzipped
        try:
            content = gzip.decompress(content)
        except (OSError, EOFError, zlib.error):
            return None
    if _is_fit(content):
        return content, "fit"
    head = content[:2048].lower()
    if b"<gpx" in head:
        return content, "gpx"
    if b"trainingcenterdatabase" in head:
        return content, "tcx"
    return None


class IntervalsIcuConnector(HttpApiConnector):
    _service = "intervals.icu"
    _base_url = _BASE_URL
    # intervals.icu's own limits, equal to the shared defaults today: set here
    # so that a change of those defaults does not reach this service unnoticed.
    _request_timeout_s = _REQUEST_TIMEOUT_S
    _call_timeout_s = _CALL_TIMEOUT_S
    _max_attempts = _MAX_ATTEMPTS
    _default_retry_after_s = _DEFAULT_RETRY_AFTER_S
    # Few calls in flight: the API also caps every IP at 10 calls per second.
    _max_concurrent = 2

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        # The credential login names the athlete; "0" means the key's owner.
        self._athlete_id = credentials.login.strip() or "0"
        self._label = ""  # set by login: the athlete's name, else their id
        # Set by login; places the local start time of a Strava stub in UTC.
        self._zone: tzinfo | None = None
        self._session.auth = ("API_KEY", credentials.password)
        # The listing already carries descriptions, so a download does not
        # spend one more request per activity fetching them again.
        self._descriptions: dict[str, str] = {}

    @property
    def user_label(self) -> str:
        return self._label or self._athlete_id

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"intervals.icu ({self.user_label}): login", total=1
        )
        log = self._tracker.sync_logger
        if log:
            log.info(f"[intervals.icu] Login: athlete_id={self._athlete_id}")
        try:
            resp = await self._request("GET", f"/athlete/{self._athlete_id}")
            athlete = resp.json()
            athlete_id = str(athlete.get("id") or self._athlete_id)
            name = athlete.get("name") or ""
            self._zone = _zone(athlete.get("timezone"))
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        self._label = name or athlete_id
        if log:
            name_part = f", {name}" if name else ""
            log.info(
                f"[intervals.icu] Login: success (athlete_id={athlete_id}{name_part})"
            )
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    def _to_meta(self, a: dict[str, Any]) -> ActivityMeta | None:
        """An ActivityMeta for one listed activity, None if it has no start time."""
        external_id = str(a["id"])
        if a.get("source") == _STRAVA_SOURCE:
            # Listed only so that a destination sees it as already present.
            local = a.get("start_date_local")
            if not local or self._zone is None:
                return None
            return ActivityMeta(
                external_id=external_id,
                name="",
                sport_type=a.get("type") or "",
                start_time=_parse_start(local, self._zone),
                elapsed_s=_STUB_MATCH_S,
                downloadable=False,
            )
        if not a.get("start_date"):
            return None
        if a.get("description"):
            self._descriptions[external_id] = a["description"]
        elapsed = a.get("elapsed_time")
        return ActivityMeta(
            external_id=external_id,
            name=a.get("name") or "",
            sport_type=a.get("type") or "",
            start_time=_parse_start(a["start_date"]),
            elapsed_s=int(elapsed) if elapsed is not None else None,
        )

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        task_name = await self._tracker.add_task(
            f"intervals.icu ({self.user_label}): fetch activity list", total=None
        )
        log = self._tracker.sync_logger
        metas: list[ActivityMeta] = []
        skipped = 0
        unplaced_stubs = 0
        try:
            resp = await self._request(
                "GET",
                f"/athlete/{self._athlete_id}/activities",
                params={
                    "oldest": start.isoformat(),
                    # newest takes a date-time, so the end day counts in full.
                    "newest": f"{end.isoformat()}T23:59:59",
                    "fields": _LIST_FIELDS,
                },
            )
            for a in resp.json():
                meta = self._to_meta(a)
                if meta is not None:
                    metas.append(meta)
                elif a.get("source") == _STRAVA_SOURCE and self._zone is None:
                    unplaced_stubs += 1
                else:
                    skipped += 1
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        if metas:
            await self._tracker.advance(task_name, amount=len(metas))
        await self._tracker.finish(task_name)
        stubs = sum(not m.downloadable for m in metas)
        if log and (stubs or skipped):
            log.info(
                f"[intervals.icu] List ({self.user_label}): {stubs} Strava"
                " imports listed as present only (the API serves no file),"
                f" {skipped} skipped without a usable start time"
            )
        if log and unplaced_stubs:
            log.warning(
                f"[intervals.icu] List ({self.user_label}): the athlete profile"
                f" has no usable time zone, so {unplaced_stubs} Strava imports"
                " cannot be matched by time and may be uploaded again"
            )
        return metas

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        log = self._tracker.sync_logger
        resp = await self._fetch_file(f"/activity/{meta.external_id}/file")
        decoded = _decode_file(resp.content)
        if decoded is None:
            if log:
                log.warning(
                    f"[intervals.icu] Download ({self.user_label}):"
                    f" {meta.external_id!r} - original file is not FIT/GPX/TCX,"
                    " skipped"
                )
            raise ActivityUnavailableError(
                f"activity {meta.external_id}: unsupported file format"
            )
        content, fmt = decoded
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            elapsed_s=meta.elapsed_s,
            content=content,
            format=fmt,
            description=self._descriptions.get(meta.external_id),
        )

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        log = self._tracker.sync_logger
        account = self.user_label
        # A file with no data points in it, say, raises UploadRefusedError.
        created, uploaded_id = await self._post_file(activity, task_name)
        if not created:
            if log:
                log.info(
                    f"[intervals.icu] Upload ({account}): {activity.external_id!r}"
                    f" {activity.start_time.date()} - duplicate, skipped"
                )
            return None
        if activity.description:
            await self._set_description(activity, uploaded_id)
        if log:
            log.info(
                f"[intervals.icu] Upload ({account}): {activity.external_id!r}"
                f" {activity.start_time.date()} - success (id={uploaded_id})"
            )
        return None

    async def _post_file(
        self, activity: Activity, task_name: str | None
    ) -> tuple[bool, str | None]:
        """Upload the file: (False, None) if intervals.icu already had it,
        else (True, the new activity's id when known).

        A single slow answer must not stop the run, since the engine gives up
        the whole destination on the first failed upload. So after no answer,
        a lost connection or a 5xx the file is sent again. That is safe once
        the first send is stored: intervals.icu recognises a file it already
        has and answers 200 instead of storing it twice. A server still
        analysing the first send could take the second for a new file, so
        every resend first waits as long again as the answer wait: one pause
        for every failure, rather than a guess from the kind of failure
        whether the server is still at work.

        A 200 leaves the activity as it is. It cannot tell this upload's
        earlier try from an activity that was there before and may have been
        edited, so nothing is written over it, and a lost description of this
        upload's own is only reported.
        """
        log = self._tracker.sync_logger
        attempt = 1
        while True:
            try:
                resp = await self._send_file(
                    "POST",
                    f"/athlete/{self._athlete_id}/activities",
                    timeout=(_REQUEST_TIMEOUT_S, _UPLOAD_RESPONSE_TIMEOUT_S),
                    call_timeout=_UPLOAD_CALL_TIMEOUT_S,
                    params={"name": activity.name} if activity.name else None,
                    files={"file": (f"activity.{activity.format}", activity.content)},
                )
            except (requests.HTTPError, TransientDownloadError) as exc:
                if not _worth_resending(exc) or attempt == _UPLOAD_ATTEMPTS:
                    raise
                message = (
                    f"{activity.external_id!r} - {exc}; attempt"
                    f" {attempt}/{_UPLOAD_ATTEMPTS} failed, sending again in"
                    f" {_UPLOAD_RETRY_PAUSE_S:.0f}s"
                )
                if task_name is not None:
                    await self._tracker.warn(task_name, message)  # logs it too
                elif log:
                    log.warning(
                        f"[intervals.icu] Upload ({self.user_label}): {message}"
                    )
            else:
                if resp.status_code != 200:  # 201: created
                    return True, _first_id(resp)
                if attempt > 1 and activity.description and log:
                    log.warning(
                        f"[intervals.icu] Upload ({self.user_label}):"
                        f" {activity.external_id!r} - already on intervals.icu"
                        " after a failed try, left as it is: description not set"
                    )
                return False, None
            await asyncio.sleep(_UPLOAD_RETRY_PAUSE_S)
            attempt += 1

    async def _set_description(
        self, activity: Activity, uploaded_id: str | None
    ) -> None:
        # A separate update, not an upload query parameter: a long description
        # would push the upload URL past the server's request-line limit.
        log = self._tracker.sync_logger
        problem = "activity ID unavailable after upload"
        if uploaded_id is not None:
            try:
                await self._request(
                    "PUT",
                    f"/activity/{uploaded_id}",
                    json={"description": activity.description},
                )
                return
            except (requests.RequestException, TransientDownloadError) as exc:
                problem = str(exc)
        # The activity itself is in; a missing description must not fail it.
        if log:
            log.warning(
                f"[intervals.icu] Upload ({self.user_label}):"
                f" {activity.external_id!r} - description not set ({problem})"
            )

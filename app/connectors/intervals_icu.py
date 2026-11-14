"""intervals.icu connector: activities in and out through its public REST API.

Auth is HTTP Basic with the literal user name "API_KEY" and the athlete's
personal API key (intervals.icu Settings -> Developer Settings). The key never
rotates, so unlike Strava it can live in KeePass.
"""

from __future__ import annotations

import asyncio
import gzip
import zlib
from datetime import date, datetime, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    RateLimitError,
    ServiceConnector,
    TransientDownloadError,
    _parse_retry_after,
    _run_with_timeout,
    attach_debug_logging,
)
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

_BASE_URL = "https://intervals.icu/api/v1"
# Socket timeout: catches a connection that stalls.
_REQUEST_TIMEOUT_S: float = 30.0
# Whole-call bound: a big file on a slow link can take minutes and still move.
_CALL_TIMEOUT_S: float = 300.0
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


def _parse_start(value: str, zone: tzinfo = timezone.utc) -> datetime:
    """A start time in UTC; a value without an offset is read in `zone`."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone)
    return dt.astimezone(timezone.utc)


def _zone(name: object) -> tzinfo | None:
    """The athlete's time zone, or None when it is missing or unknown here."""
    if not isinstance(name, str) or not name:
        return None
    try:
        return ZoneInfo(name)
    except (KeyError, ValueError):  # ZoneInfoNotFoundError is a KeyError
        return None


def _decode_file(content: bytes) -> tuple[bytes, str] | None:
    """Return (file bytes, fit/gpx/tcx) for an original file, None if unusable."""
    if content[:2] == b"\x1f\x8b":  # intervals.icu may serve the file gzipped
        try:
            content = gzip.decompress(content)
        except (OSError, EOFError, zlib.error):
            return None
    if content[8:12] == b".FIT":
        return content, "fit"
    head = content[:2048].lower()
    if b"<gpx" in head:
        return content, "gpx"
    if b"trainingcenterdatabase" in head:
        return content, "tcx"
    return None


class IntervalsIcuConnector(ServiceConnector):
    # Few calls in flight: the API also caps every IP at 10 calls per second.
    _max_concurrent = 2

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        # The credential login names the athlete; "0" means the key's owner.
        self._athlete_id = credentials.login.strip() or "0"
        self._label = ""  # set by login: the athlete's name, else their id
        # Set by login; places the local start time of a Strava stub in UTC.
        self._zone: tzinfo | None = None
        self._session = requests.Session()
        self._session.auth = ("API_KEY", credentials.password)
        log = tracker.sync_logger
        if log:
            attach_debug_logging(self._session, log.debug)
        # The listing already carries descriptions, so a download does not
        # spend one more request per activity fetching them again.
        self._descriptions: dict[str, str] = {}

    @property
    def user_label(self) -> str:
        return self._label or self._athlete_id

    async def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> requests.Response:
        """Send one API call, waiting out 429s.

        An error status raises HTTPError, a failed connection or call
        TransientDownloadError, and a 429 on the last attempt RateLimitError.
        """
        log = self._tracker.sync_logger
        attempt = 1
        while True:
            try:
                resp: requests.Response = await _run_with_timeout(
                    asyncio.to_thread(
                        self._session.request,
                        method,
                        _BASE_URL + path,
                        timeout=_REQUEST_TIMEOUT_S,
                        **kwargs,
                    ),
                    _CALL_TIMEOUT_S,
                )
            except requests.RequestException as exc:
                raise TransientDownloadError(str(exc)) from exc
            if resp.status_code != 429:
                resp.raise_for_status()
                return resp
            pause = _parse_retry_after(resp.headers, _DEFAULT_RETRY_AFTER_S)
            if attempt == _MAX_ATTEMPTS:
                # A download hands this to the engine, which then pauses every
                # download from this source together.
                raise RateLimitError(
                    f"intervals.icu 429 after {_MAX_ATTEMPTS} attempts",
                    retry_after=pause,
                )
            if log:
                log.warning(
                    f"[intervals.icu] 429 received, pausing {pause:.0f}s"
                    f" (attempt {attempt}/{_MAX_ATTEMPTS})"
                )
            await asyncio.sleep(pause)
            attempt += 1

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
        try:
            resp = await self._request("GET", f"/activity/{meta.external_id}/file")
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in (401, 403):
                raise  # the key itself is refused: fail the run, do not skip
            if status >= 500 or status == 408:
                raise TransientDownloadError(str(exc)) from exc
            # Any other 4xx will not change on retry (a manual entry has no
            # file), while the engine retries a transient error every minute.
            raise ActivityUnavailableError(str(exc)) from exc
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
        resp = await self._request(
            "POST",
            f"/athlete/{self._athlete_id}/activities",
            params={"name": activity.name} if activity.name else None,
            files={"file": (f"activity.{activity.format}", activity.content)},
        )
        if resp.status_code == 200:  # 201 means created; 200 means all duplicates
            if log:
                log.info(
                    f"[intervals.icu] Upload ({account}): {activity.external_id!r}"
                    f" {activity.start_time.date()} - duplicate, skipped"
                )
            return None
        created = [str(a["id"]) for a in resp.json().get("activities") or []]
        uploaded_id = created[0] if created else None
        if activity.description:
            await self._set_description(activity, uploaded_id)
        if log:
            log.info(
                f"[intervals.icu] Upload ({account}): {activity.external_id!r}"
                f" {activity.start_time.date()} - success (id={uploaded_id})"
            )
        return None

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

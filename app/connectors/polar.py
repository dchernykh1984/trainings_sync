"""Polar Flow connector: training sessions read through Polar AccessLink.

Any Polar Flow user can create an API client (https://admin.polaraccesslink.com)
and authorize it once in a browser; the access token that gives lasts about
a year (the expires_in of Polar's answer). The credential password is that
token, and the login is the member id the user is registered under with the
client (any name, one per account on a client; empty means "trainings-sync").

AccessLink is read-only, so this connector is only a source. It lists only the
sessions uploaded to Flow in the last 30 days, and only those uploaded after
the user was registered: it keeps up with new sessions, not the history.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
)
from app.connectors.http_api import (
    HttpApiConnector,
    _is_fit,
    _parse_start,
    _status,
)
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

_BASE_URL = "https://www.polaraccesslink.com/v3"
_DEFAULT_MEMBER_ID = "trainings-sync"
# The longest pause after a 429: one short-term window. A used-up daily window
# then shows as pauses one after another, not as one silent wait of hours.
_MAX_PAUSE_S = 900.0
_RATE_HEADERS = ("RateLimit-Usage", "RateLimit-Limit", "RateLimit-Reset")
# "P1D", "PT2H44M45S", "PT59.5S": the duration form AccessLink uses.
_DURATION = re.compile(
    r"P(?:(\d+(?:\.\d+)?)D)?"
    r"(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?"
)


def _seconds(value: object) -> int | None:
    """Whole seconds in an ISO 8601 duration, None if it is not one."""
    if not isinstance(value, str):
        return None
    match = _DURATION.fullmatch(value)
    if match is None or not any(match.groups()):
        return None
    days, hours, minutes, seconds = (float(g) if g else 0.0 for g in match.groups())
    total = int(days * 86400 + hours * 3600 + minutes * 60 + seconds)
    return total or None  # no time at all says nothing about the session


def _field(exercise: dict[str, Any], name: str) -> Any:
    """A field in either spelling AccessLink uses: start_time or start-time."""
    value = exercise.get(name)
    return value if value is not None else exercise.get(name.replace("_", "-"))


def _local_day(exercise: dict[str, Any]) -> date:
    """The day a session started on where it took place; it has a start."""
    return datetime.fromisoformat(_field(exercise, "start_time")).date()


def _start(exercise: dict[str, Any]) -> datetime | None:
    """A session's start in UTC, from its local time and its offset in minutes."""
    local = _field(exercise, "start_time")
    offset = _field(exercise, "start_time_utc_offset")
    if not isinstance(local, str) or type(offset) is not int:
        return None
    try:  # an offset of a day or more is no time zone either
        return _parse_start(local, timezone(timedelta(minutes=offset)))
    except ValueError:
        return None


class PolarConnector(HttpApiConnector):
    _service = "Polar Flow"
    _base_url = _BASE_URL
    _max_concurrent = 2
    # Pause after a 429 that does not say which window is used up.
    _default_retry_after_s = 60.0

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self._session.headers["Authorization"] = f"Bearer {credentials.password}"
        # AccessLink can also answer in XML; the FIT download asks for */*.
        self._session.headers["Accept"] = "application/json"
        self._member_id = credentials.login.strip() or _DEFAULT_MEMBER_ID

    @property
    def user_label(self) -> str:
        return self._member_id

    def _pause_after_429(self, resp: requests.Response) -> float:
        # No Retry-After: each of _RATE_HEADERS holds two values, for the
        # 15-minute window and the 24-hour one, and RateLimit-Reset is the
        # seconds until that window starts over.
        try:
            usage, limit, reset = (
                [float(v) for v in resp.headers[name].split(",")]
                for name in _RATE_HEADERS
            )
        except (KeyError, ValueError):
            return self._default_retry_after_s
        windows = zip(usage, limit, reset, strict=False)
        used_up = [r for u, cap, r in windows if u >= cap]
        pause = max(used_up, default=self._default_retry_after_s)
        # A window that starts over right now still gets a second, not a burst.
        return min(max(pause, 1.0), _MAX_PAUSE_S)

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"Polar Flow ({self._member_id}): login", total=1
        )
        log = self._tracker.sync_logger
        try:
            await self._register()
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        if log:
            log.info(f"[Polar Flow] Login: success (member id {self._member_id})")
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def _register(self) -> None:
        """Register the user with the client: nothing is listed before that."""
        try:
            await self._request("POST", "/users", json={"member-id": self._member_id})
        except requests.HTTPError as exc:
            status = _status(exc)
            if status == 409:
                # Registered on an earlier run - or, in the same answer, the
                # member id is another account's on this client.
                if log := self._tracker.sync_logger:
                    log.info(
                        f"[Polar Flow] Login: member id {self._member_id} is"
                        " already registered with the client (by this account"
                        " on an earlier run, or by another one: then give this"
                        " one its own login)"
                    )
                return
            if status == 403:
                raise PermissionError(
                    "Polar Flow refuses access until all its mandatory consents"
                    " are accepted in Polar Flow"
                ) from exc
            raise

    def _to_meta(self, exercise: dict[str, Any]) -> ActivityMeta | None:
        """An ActivityMeta for one listed session, None without an id or a start."""
        start = _start(exercise)
        if start is None or not exercise.get("id"):
            return None
        sport = str(
            _field(exercise, "detailed_sport_info") or exercise.get("sport") or ""
        )
        return ActivityMeta(
            external_id=str(exercise["id"]),
            # A session has no title of its own; its sport stands in for one.
            name=sport.replace("_", " ").capitalize(),
            sport_type=sport,
            start_time=start,
            elapsed_s=_seconds(exercise.get("duration")),
        )

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        task_name = await self._tracker.add_task(
            f"Polar Flow ({self._member_id}): fetch activity list", total=None
        )
        log = self._tracker.sync_logger
        metas: list[ActivityMeta] = []
        skipped = 0
        try:
            resp = await self._request("GET", "/exercises")
            for exercise in resp.json() if resp.content else []:
                meta = self._to_meta(exercise)
                if meta is None:
                    skipped += 1
                # The local day, as Garmin and intervals.icu count it: an
                # evening west of UTC is already the next day there.
                elif start <= _local_day(exercise) <= end:
                    metas.append(meta)
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        if metas:
            await self._tracker.advance(task_name, amount=len(metas))
        await self._tracker.finish(task_name)
        if log and skipped:
            log.info(
                f"[Polar Flow] List ({self._member_id}): {skipped} sessions skipped"
                " without a start time"
            )
        return metas

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        log = self._tracker.sync_logger
        # A 404: the session has left the 30-day window, or was deleted.
        resp = await self._fetch_file(
            f"/exercises/{meta.external_id}/fit", headers={"Accept": "*/*"}
        )
        if not _is_fit(resp.content):
            if log:
                log.warning(
                    f"[Polar Flow] Download ({self._member_id}): {meta.external_id!r}"
                    " - no FIT file for this session, skipped"
                )
            raise ActivityUnavailableError(f"session {meta.external_id}: no FIT file")
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            elapsed_s=meta.elapsed_s,
            content=resp.content,
            format="fit",
        )

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        # The config never makes Polar Flow a destination.
        raise NotImplementedError("Polar Flow takes no uploads through AccessLink")

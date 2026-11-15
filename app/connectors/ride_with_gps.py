"""Ride with GPS connector: trips in and out through its v1 API.

Auth is an API key plus an auth token, both made on the account's API clients
page (https://ridewithgps.com/api/api_clients). The credential login is the
key and the password the token. Neither rotates, so both can live in KeePass.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Any

import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    TransientDownloadError,
    UploadDeferredError,
    UploadRefusedError,
)
from app.connectors.http_api import (
    HttpApiConnector,
    _is_fit,
    _parse_start,
)
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

_BASE_URL = "https://ridewithgps.com/api/v1"
# The largest page the trips list serves.
_PAGE_SIZE = 200
# An upload is processed in the background; its task is polled this often...
_TASK_POLL_S = 2.0
# ...this many times, about a minute, before the upload just counts as taken.
_TASK_POLLS = 30
# Task error codes for a file Ride with GPS cannot make a trip of: sending it
# again gets the same answer. Any other code may be its processing failing.
_UNUSABLE_FILE = frozenset(
    {"time_data_missing", "no_tracks", "empty_file", "parse_failed"}
)


def _departed(trip: dict[str, Any]) -> datetime | None:
    """When a trip started, in UTC; None if the trip does not say."""
    value = trip.get("departed_at")
    if not isinstance(value, str) or not value:
        return None
    try:
        return _parse_start(value)
    except ValueError:
        return None


def _local_day(trip: dict[str, Any]) -> date:
    """The day a trip started on where it took place; it has a start."""
    return datetime.fromisoformat(trip["departed_at"]).date()


def _whole_seconds(value: Any) -> int | None:
    """A trip's duration in whole seconds, None if it gives none that reads."""
    try:
        seconds = int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
    return seconds if seconds > 0 else None  # an activity cannot run backwards


class RideWithGpsConnector(HttpApiConnector):
    _service = "Ride with GPS"
    _base_url = _BASE_URL
    # No published rate limit; a couple of calls in flight is polite enough.
    _max_concurrent = 2
    _secret_headers = ("x-rwgps-api-key", "x-rwgps-auth-token")

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self._session.headers.update(
            {
                "x-rwgps-api-key": credentials.login.strip(),
                "x-rwgps-auth-token": credentials.password,
            }
        )
        self._label = ""  # set by login: the account's name
        # The trips list already carries descriptions, so a download does not
        # spend one more request per trip fetching them again.
        self._descriptions: dict[str, str] = {}

    @property
    def user_label(self) -> str:
        return self._label

    def _named(self, step: str) -> str:
        account = f" ({self._label})" if self._label else ""
        return f"Ride with GPS{account}: {step}"

    async def login(self) -> None:
        task_name = await self._tracker.add_task(self._named("login"), total=1)
        log = self._tracker.sync_logger
        try:
            resp = await self._request("GET", "/users/current.json")
            user = resp.json()["user"]
            name = user.get("name") or f"user {user.get('id')}"
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        self._label = name
        if log:
            log.info(f"[Ride with GPS] Login: success ({name})")
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    def _to_meta(self, trip: dict[str, Any]) -> ActivityMeta | None:
        """An ActivityMeta for one listed trip, None without an id or a start."""
        start = _departed(trip)
        if start is None or trip.get("id") is None:
            return None
        return ActivityMeta(
            external_id=str(trip["id"]),
            name=trip.get("name") or "",
            sport_type=trip.get("activity_type") or "",
            start_time=start,
            elapsed_s=_whole_seconds(trip.get("duration")),
        )

    async def _trips(self) -> list[dict[str, Any]]:
        """Every trip on the account, page by page.

        The list has no date filter and no documented order, so all of it is
        read. It ends at the last page, or at a page with no trip that an
        earlier one did not already bring, so that paging that loops cannot
        hang the run.
        """
        trips: list[dict[str, Any]] = []
        seen: set[Any] = set()
        page = 1
        while True:
            resp = await self._request(
                "GET", "/trips.json", params={"page": page, "page_size": _PAGE_SIZE}
            )
            body = resp.json()
            fresh = [t for t in body.get("trips") or [] if t.get("id") not in seen]
            if not fresh:
                return trips
            seen.update(t.get("id") for t in fresh)
            trips.extend(fresh)
            pagination = (body.get("meta") or {}).get("pagination") or {}
            if not pagination.get("next_page_url"):
                return trips
            page += 1

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        task_name = await self._tracker.add_task(
            self._named("fetch activity list"), total=None
        )
        log = self._tracker.sync_logger
        metas: list[ActivityMeta] = []
        skipped = 0
        try:
            for trip in await self._trips():
                meta = self._to_meta(trip)
                if meta is None:
                    skipped += 1
                # The local day, as Garmin and intervals.icu count it: an
                # evening west of UTC is already the next day there.
                elif start <= _local_day(trip) <= end:
                    metas.append(meta)
                    # Kept for the download: the list already carries it.
                    if trip.get("description"):
                        self._descriptions[meta.external_id] = trip["description"]
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        if metas:
            await self._tracker.advance(task_name, amount=len(metas))
        await self._tracker.finish(task_name)
        if log and skipped:
            log.info(
                f"[Ride with GPS] List ({self._label}): {skipped} trips skipped"
                " without a start time"
            )
        return metas

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        log = self._tracker.sync_logger
        # The list holds only the account's own trips, so a 403 here is about
        # the account (a client that may not export, say), and fails the run.
        resp = await self._fetch_file(f"/trips/{meta.external_id}.fit")
        if not _is_fit(resp.content):
            if log:
                log.warning(
                    f"[Ride with GPS] Download ({self._label}): {meta.external_id!r}"
                    " - the answer is not a FIT file, skipped"
                )
            raise ActivityUnavailableError(f"trip {meta.external_id}: not a FIT file")
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            elapsed_s=meta.elapsed_s,
            content=resp.content,
            format="fit",
            description=self._descriptions.get(meta.external_id),
        )

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        log = self._tracker.sync_logger
        fields = {"name": activity.name, "description": activity.description or ""}
        resp = await self._send_file(
            "POST",
            "/trips.json",
            data={k: v for k, v in fields.items() if v},
            files={"file": (f"activity.{activity.format}", activity.content)},
        )
        where = (
            f"[Ride with GPS] Upload ({self._label}): {activity.external_id!r}"
            f" {activity.start_time.date()}"
        )
        try:
            task = await self._finished(resp.json()["task"])
            if task is not None:
                self._report(task, where)  # may refuse the file: not caught here
        except (
            TransientDownloadError,
            requests.HTTPError,
            ValueError,
            LookupError,
            TypeError,
            AttributeError,
        ) as exc:
            # The file is taken: a failed look at the result, or one in a shape
            # it was not expected in, must not stop the uploads behind it. Sent
            # again, it would come back as a duplicate.
            if log:
                log.warning(f"{where} - taken, its result could not be read ({exc})")
            return None
        if task is None and log:
            log.info(f"{where} - taken, still being processed")
        return None

    def _report(self, task: dict[str, Any], where: str) -> None:
        """Log what a finished upload task made of the file, or refuse it."""
        log = self._tracker.sync_logger
        errors = task.get("errors") or []
        items = task.get("items") or []
        trips = [str(i["item_id"]) for i in items if i.get("item_id") is not None]
        codes = {e.get("code") for e in errors if isinstance(e, dict)}
        problem = "; ".join(
            str(e.get("message") or e.get("code")) if isinstance(e, dict) else str(e)
            for e in errors
        )
        if trips:
            if log:
                log.info(f"{where} - success (id={', '.join(trips)})")
        elif "duplicate" in codes:
            if log:
                log.info(f"{where} - duplicate, skipped")
        elif codes & _UNUSABLE_FILE:
            # A route without timestamps, say: sent again, the same answer.
            raise UploadRefusedError(f"no trip made ({problem})")
        elif errors:
            # Its own processing failing, say: the next run tries again.
            raise UploadDeferredError(f"no trip made ({problem})")
        elif log:
            # Done, naming neither a trip nor a reason: the trip may not be
            # linked yet. Taken, like any other answer it does not expect.
            log.warning(f"{where} - finished without naming a trip, counted as taken")

    async def _finished(self, task: dict[str, Any]) -> dict[str, Any] | None:
        """The upload task once it has run, None if it is still pending."""
        for _ in range(_TASK_POLLS):
            if task.get("status") == "completed":
                return task
            await asyncio.sleep(_TASK_POLL_S)
            resp = await self._request("GET", f"/tasks/{task['id']}.json")
            task = resp.json()["task"]
        return task if task.get("status") == "completed" else None

"""Runalyze connector: activities uploaded through its Personal API.

Auth is a personal API token (Runalyze -> Settings -> Personal API), sent in a
"token" header; the credential password is the token. Every account may
upload through that API, but reading activities back needs a paid plan and is
not documented, so this connector is only ever a destination.
"""

from __future__ import annotations

from datetime import date

import requests

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
)
from app.connectors.http_api import HttpApiConnector
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

_BASE_URL = "https://runalyze.com/api/v1"
# What Runalyze answers, with a 200, to a token it does not accept.
_REFUSED = "No valid token"


def _refuse_bad_token(resp: requests.Response) -> None:
    if _REFUSED in resp.text:
        raise PermissionError("Runalyze refused the API token")


class RunalyzeConnector(HttpApiConnector):
    _service = "Runalyze"
    _base_url = _BASE_URL
    _secret_headers = ("token",)
    # A 429 that does not say how long to wait: the limits are hourly.
    _default_retry_after_s = 60.0

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self._session.headers["token"] = credentials.password

    async def login(self) -> None:
        task_name = await self._tracker.add_task("Runalyze: login", total=1)
        log = self._tracker.sync_logger
        try:
            _refuse_bad_token(await self._request("GET", "/ping"))
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        if log:
            log.info("[Runalyze] Login: token accepted")
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        # Nothing to list: Runalyze skips a file it already has, which is what
        # keeps an upload from making a second copy.
        return []

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        raise ActivityUnavailableError("Runalyze serves no activities to download")

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        log = self._tracker.sync_logger
        resp = await self._send_file(
            "POST",
            "/activities/uploads",
            files={"file": (f"activity.{activity.format}", activity.content)},
        )
        _refuse_bad_token(resp)
        if log:
            # Runalyze imports the file from a queue, so it shows up later.
            log.info(
                f"[Runalyze] Upload: {activity.external_id!r}"
                f" {activity.start_time.date()} - queued"
            )
        return None

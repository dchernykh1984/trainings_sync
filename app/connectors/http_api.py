"""Shared plumbing for connectors that talk to one REST API through requests."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, tzinfo
from typing import Any

import requests

from app.connectors.base import (
    ActivityUnavailableError,
    RateLimitError,
    ServiceConnector,
    TransientDownloadError,
    UploadRefusedError,
    _parse_retry_after,
    _run_with_timeout,
    attach_debug_logging,
)
from app.tracking.tracker import TaskTracker


def _status(exc: requests.HTTPError) -> int:
    return exc.response.status_code if exc.response is not None else 0


def _transient_status(exc: requests.HTTPError) -> bool:
    """A 5xx or a 408: the same request may succeed later."""
    status = _status(exc)
    return status >= 500 or status == 408


def _refused_file(exc: requests.HTTPError) -> bool:
    """A 413 or a 422: the service refuses this very file (too big, or nothing
    in it that it can use), and sending it again gets the same answer."""
    return _status(exc) in (413, 422)


def _parse_start(value: str, zone: tzinfo = timezone.utc) -> datetime:
    """A start time in UTC; a value without an offset is read in `zone`."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=zone)
    return dt.astimezone(timezone.utc)


def _is_fit(content: bytes) -> bool:
    """Whether bytes are a FIT file: its header holds ".FIT" at offset 8."""
    return content[8:12] == b".FIT"


def _reason(exc: requests.HTTPError) -> str:
    """The service's own words for an error, on one line and kept short."""
    text = exc.response.text if exc.response is not None else ""
    return " ".join(text.split())[:200] or str(exc)


class _Session(requests.Session):
    """A session that keeps a service's secret headers to the service's host.

    On a redirect to another host requests drops Authorization, but not a
    header of another name, such as Ride with GPS's key and token.
    """

    def __init__(self, secret_headers: tuple[str, ...]) -> None:
        super().__init__()
        self._secret_headers = secret_headers

    def rebuild_auth(
        self, prepared_request: requests.PreparedRequest, response: requests.Response
    ) -> None:
        super().rebuild_auth(prepared_request, response)
        old_url = response.request.url or ""
        if self.should_strip_auth(old_url, prepared_request.url or ""):
            for name in self._secret_headers:
                prepared_request.headers.pop(name, None)


class HttpApiConnector(ServiceConnector):
    """A connector behind one HTTP API: a requests session and its call loop.

    A subclass names the service for log lines, gives the base URL, and puts
    its auth on `self._session`; auth in a header other than Authorization is
    named in `_secret_headers`, so that no redirect carries it to another host.
    """

    _service: str = ""
    _base_url: str = ""
    # Socket timeout: catches a connection that stalls.
    _request_timeout_s: float = 30.0
    # Whole-call bound: a big file on a slow link can take minutes and still move.
    _call_timeout_s: float = 300.0
    _max_attempts: int = 3
    # Pause after a 429 that does not say how long to wait.
    _default_retry_after_s: float = 5.0
    _secret_headers: tuple[str, ...] = ()

    def __init__(self, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self._session: requests.Session = _Session(self._secret_headers)
        log = tracker.sync_logger
        if log:
            attach_debug_logging(self._session, log.debug)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        timeout: float | tuple[float, float] | None = None,
        call_timeout: float | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send one API call to `path` under the base URL, waiting out 429s.

        An error status raises HTTPError, a failed connection or call
        TransientDownloadError, and a 429 on the last attempt RateLimitError.
        `timeout` is the socket timeout, in the form requests takes, and
        `call_timeout` the bound on the whole call; both default to the
        class settings.
        """
        log = self._tracker.sync_logger
        socket_timeout = self._request_timeout_s if timeout is None else timeout
        bound = self._call_timeout_s if call_timeout is None else call_timeout
        attempt = 1
        while True:
            try:
                resp: requests.Response = await _run_with_timeout(
                    asyncio.to_thread(
                        self._session.request,
                        method,
                        self._base_url + path,
                        timeout=socket_timeout,
                        **kwargs,
                    ),
                    bound,
                )
            except requests.RequestException as exc:
                raise TransientDownloadError(str(exc)) from exc
            if resp.status_code != 429:
                resp.raise_for_status()
                return resp
            pause = _parse_retry_after(resp.headers, self._default_retry_after_s)
            if attempt == self._max_attempts:
                # A download hands this to the engine, which then pauses every
                # download from this source together.
                raise RateLimitError(
                    f"{self._service} 429 after {self._max_attempts} attempts",
                    retry_after=pause,
                )
            if log:
                log.warning(
                    f"[{self._service}] 429 received, pausing {pause:.0f}s"
                    f" (attempt {attempt}/{self._max_attempts})"
                )
            await asyncio.sleep(pause)
            attempt += 1

    async def _fetch_file(self, path: str, **kwargs: Any) -> requests.Response:
        """_request for a download, its failures sorted for the engine.

        A 401 or a 403 is raised as it is: the account is refused, so the run
        fails rather than skip every file quietly. A 5xx or a 408 becomes
        TransientDownloadError, which the engine tries again; any other error
        status (a file that is gone, or a manual entry with none) becomes
        ActivityUnavailableError, which a retry would not change.
        """
        try:
            return await self._request("GET", path, **kwargs)
        except requests.HTTPError as exc:
            if _status(exc) in (401, 403):
                raise
            if _transient_status(exc):
                raise TransientDownloadError(str(exc)) from exc
            raise ActivityUnavailableError(str(exc)) from exc

    async def _send_file(
        self, method: str, path: str, **kwargs: Any
    ) -> requests.Response:
        """_request for an upload: a file the service refuses for good (see
        _refused_file) raises UploadRefusedError, which the engine passes over
        instead of stopping every upload queued behind it."""
        try:
            return await self._request(method, path, **kwargs)
        except requests.HTTPError as exc:
            if not _refused_file(exc):
                raise
            raise UploadRefusedError(f"refused ({_reason(exc)})") from exc

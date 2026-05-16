from __future__ import annotations

import asyncio
import itertools
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Literal, TypeVar
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

from app.tracking.tracker import TaskTracker

_SENSITIVE_PARAMS = frozenset(
    {
        "access_token",
        "client_secret",
        "refresh_token",
        "code",
        "token",
        "key",
        "secret",
        "X-Amz-Algorithm",
        "X-Amz-Credential",
        "X-Amz-Date",
        "X-Amz-Expires",
        "X-Amz-Signature",
        "X-Amz-SignedHeaders",
        "X-Amz-Security-Token",
        "AWSAccessKeyId",
        "Signature",
        "Key-Pair-Id",
        "Policy",
    }
)


def _redact_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query, keep_blank_values=True)
    redacted = {
        k: ["REDACTED"] if k in _SENSITIVE_PARAMS else v for k, v in params.items()
    }
    return urlunparse(parsed._replace(query=urlencode(redacted, doseq=True)))


def attach_debug_logging(session: Any, log_fn: Any) -> None:
    """Patch session.send so every request is logged: success and network failures."""
    original_send = session.send

    def _send(request: Any, **kwargs: Any) -> Any:
        t0 = time.monotonic()
        try:
            resp = original_send(request, **kwargs)
            log_fn(
                f"[http] {request.method} {_redact_url(request.url)}"
                f" -> {resp.status_code} ({time.monotonic() - t0:.2f}s)"
            )
            return resp
        except Exception as exc:
            log_fn(
                f"[http] {request.method} {_redact_url(request.url)}"
                f" -> {type(exc).__name__} ({time.monotonic() - t0:.2f}s)"
            )
            raise

    session.send = _send  # type: ignore[method-assign]


def _fetch_url_bytes(url: str, timeout: float, log_fn: Any = None) -> bytes:
    """GET url and return content bytes, logging success and failure to log_fn."""
    t0 = time.monotonic()
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        if log_fn is not None:
            log_fn(
                f"[http] GET {_redact_url(url)}"
                f" -> {resp.status_code} ({time.monotonic() - t0:.2f}s)"
            )
        return bytes(resp.content)
    except Exception as exc:
        if log_fn is not None:
            log_fn(
                f"[http] GET {_redact_url(url)}"
                f" -> {type(exc).__name__} ({time.monotonic() - t0:.2f}s)"
            )
        raise


class ActivityUnavailableError(Exception):
    """Raised when an activity exists in the list but cannot be downloaded.

    Connectors raise this for activities that are permanently unavailable
    (e.g. manual Strava entries without sensor streams). The sync engine
    skips them silently rather than failing the entire run.
    """


class TransientDownloadError(Exception):
    """Raised by connectors for transient network errors that are worth retrying."""


class RateLimitError(TransientDownloadError):
    """Raised when the service returns HTTP 429; carries the suggested retry delay."""

    def __init__(self, message: str, retry_after: float = 900.0) -> None:
        super().__init__(message)
        self.retry_after = retry_after


_T = TypeVar("_T")
_API_TIMEOUT_S: float = 30.0


async def _run_with_timeout(
    coro: Awaitable[_T], timeout_s: float = _API_TIMEOUT_S
) -> _T:
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except asyncio.TimeoutError:
        raise TransientDownloadError(
            f"operation timed out after {timeout_s:.0f}s"
        ) from None


@dataclass(frozen=True)
class ActivityMeta:
    external_id: str
    name: str
    sport_type: str
    start_time: datetime  # must be UTC (utcoffset == 0)
    elapsed_s: int | None = field(default=None, kw_only=True)  # wall-clock duration

    def __post_init__(self) -> None:
        offset = self.start_time.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError(
                f"ActivityMeta.start_time must be UTC, got: {self.start_time}"
            )
        if self.elapsed_s is not None and self.elapsed_s < 0:
            raise ValueError(f"elapsed_s must be >= 0, got: {self.elapsed_s}")

    @property
    def end_time(self) -> datetime | None:
        if self.elapsed_s is None:
            return None
        return self.start_time + timedelta(seconds=self.elapsed_s)


@dataclass(frozen=True)
class MediaItem:
    content: bytes
    media_type: Literal["photo", "video"]
    caption: str | None = field(default=None, kw_only=True)
    url: str = field(default="", kw_only=True)

    def __post_init__(self) -> None:
        if self.media_type not in ("photo", "video"):
            raise ValueError(
                f"media_type must be 'photo' or 'video', got: {self.media_type!r}"
            )


@dataclass(frozen=True)
class Activity(ActivityMeta):
    content: bytes
    format: str
    description: str | None = field(default=None, kw_only=True)
    media: tuple[MediaItem, ...] = field(default_factory=tuple, kw_only=True)


class ServiceConnector(ABC):
    _max_concurrent: int = 5
    supports_media_upload: bool = False

    def __init__(self, tracker: TaskTracker) -> None:
        self._tracker = tracker
        self._counter = itertools.count(1)

    @property
    def user_label(self) -> str:
        return ""

    def _task_name(self, label: str) -> str:
        return f"{label} #{next(self._counter)}"

    @abstractmethod
    async def login(self) -> None: ...

    @abstractmethod
    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]: ...

    @abstractmethod
    async def download_activity(self, meta: ActivityMeta) -> Activity: ...

    @abstractmethod
    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None: ...

    def has_activity(self, external_id: str, source_id: str) -> bool:
        """Return True if the activity is known to exist at this destination.

        The default conservatively returns True (trust the uploaded_to cache).
        Override in connectors that can cheaply verify local state, e.g. by
        checking whether the file is present on disk.
        """
        return True

    async def download_all(self, start: date, end: date) -> list[Activity]:
        metas = await self.list_activities(start, end)
        if not metas:
            return []

        task_name = await self._tracker.add_task(
            self._task_name("Download activities"), total=len(metas)
        )
        sem = asyncio.Semaphore(self._max_concurrent)

        async def _download(meta: ActivityMeta) -> Activity:
            async with sem:
                activity = await self.download_activity(meta)
            await self._tracker.advance(task_name)
            return activity

        try:
            results = await asyncio.gather(*(_download(m) for m in metas))
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        await self._tracker.finish(task_name)
        return list(results)

    async def upload_all(self, activities: Sequence[Activity]) -> None:
        if not activities:
            return

        task_name = await self._tracker.add_task(
            self._task_name("Upload activities"), total=len(activities)
        )
        sem = asyncio.Semaphore(self._max_concurrent)

        async def _upload(activity: Activity) -> None:
            async with sem:
                await self.upload_activity(activity, task_name=task_name)
            if activity.media and not self.supports_media_upload:
                n = len(activity.media)
                await self._tracker.warn(
                    task_name,
                    f"{activity.external_id!r}: {n} media item(s) not uploaded"
                    " (not supported)",
                )
            await self._tracker.advance(task_name)

        try:
            await asyncio.gather(*(_upload(a) for a in activities))
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        await self._tracker.finish(task_name)

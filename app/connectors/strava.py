from __future__ import annotations

import asyncio
import io
import itertools
import logging
import os
import threading
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any, ClassVar

import requests
from requests.adapters import HTTPAdapter
from stravalib import Client
from stravalib.exc import ObjectNotFound

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    MediaItem,
    RateLimitError,
    ServiceConnector,
    TransientDownloadError,
    _fetch_url_bytes,
    _redact_url,
    _run_with_timeout,
    attach_debug_logging,
)
from app.credentials.base import StravaCredentials
from app.tracking.tracker import TaskTracker

os.environ.setdefault("SILENCE_TOKEN_WARNINGS", "true")
logging.getLogger("stravalib").setLevel(logging.ERROR)

_log = logging.getLogger(__name__)

_STREAM_TYPES = ["time", "latlng", "altitude", "heartrate", "cadence", "watts"]
_GPX_NS = "http://www.topografix.com/GPX/1/1"
_TPX_NS = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"
_TCD_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
_AE_NS = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"

_PHOTO_DOWNLOAD_TIMEOUT_S: int = 30
_REQUEST_TIMEOUT_S: float = 30.0
_CALL_API_MAX_ATTEMPTS: int = 3
_UPLOAD_POLL_INTERVAL_S: float = 1.0
# Workers per connector instance (caps asyncio.to_thread concurrency).
_MAX_CONCURRENT: int = 2
# Stop proactive waits this many requests before the hard limit.
# Up to 5 sync processes x _MAX_CONCURRENT in-flight requests each can all
# read the same stale usage value and proceed before any response arrives.
_RATE_LIMIT_MARGIN: int = _MAX_CONCURRENT * 5
# Seconds of padding after a window boundary before zeroing stale usage.
# Guards against local-clock drift vs Strava's server clock.
_RESET_PADDING_S: float = 10.0


def _parse_retry_after(headers: Any, default: float = 900.0) -> float:
    try:
        return float(headers.get("Retry-After", default))
    except (ValueError, TypeError):
        return default


def _parse_retry_after_optional(headers: Any) -> float | None:
    try:
        value = headers.get("Retry-After")
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _parse_rate_limit_pair(value: str | None) -> tuple[int, int] | None:
    if value is None:
        return None
    try:
        parts = [int(x.strip()) for x in value.split(",")]
        if len(parts) >= 2:
            return parts[0], parts[1]
    except (ValueError, TypeError):
        pass
    return None


def _format_utc_resume_time(pause_s: float) -> str:
    resume_dt = datetime.fromtimestamp(time.time() + pause_s, tz=timezone.utc)
    return resume_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _fmt_bucket(usage: int | None, limit: int | None) -> str:
    return f"{usage}/{limit}" if usage is not None and limit is not None else "-"


class _StravaRateLimiter:
    """Tracks Strava API rate-limit state from response headers and throttles calls."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # All usage/limit buckets start as None; wait_if_needed ignores unknown buckets.
        # Missing/malformed headers keep the previous valid value; bad data never zeroes
        # a bucket.
        self._limit_15min: int | None = None
        self._usage_15min: int | None = None
        self._limit_daily: int | None = None
        self._usage_daily: int | None = None
        self._read_limit_15min: int | None = None
        self._read_usage_15min: int | None = None
        self._read_limit_daily: int | None = None
        self._read_usage_daily: int | None = None
        # Reset timestamps: after these pass, stale usage is zeroed under lock.
        self._reset_time_15min: float = self._next_quarter_hour_ts()
        self._reset_time_daily: float = self._next_midnight_utc_ts()

    @staticmethod
    def _next_quarter_hour_ts() -> float:
        now = time.time()
        dt = datetime.fromtimestamp(now, tz=timezone.utc)
        seconds_into_slot = (
            (dt.minute % 15) * 60 + dt.second + dt.microsecond / 1_000_000
        )
        return now + (15 * 60 - seconds_into_slot)

    @staticmethod
    def _next_midnight_utc_ts() -> float:
        now = time.time()
        dt = datetime.fromtimestamp(now, tz=timezone.utc)
        tomorrow = (dt + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return tomorrow.timestamp()

    @staticmethod
    def _seconds_until(target: float) -> float:
        return max(1.0, target - time.time())

    def update_from_headers(
        self, response: Any
    ) -> tuple[bool, tuple[int | None, ...] | None]:
        """Called from session.send hook (in thread). Updates rate-limit state.

        Parsing and state updates happen under lock; logging happens after lock
        release to avoid holding the lock during I/O.

        Returns (usage_parsed, snapshot) where snapshot holds the eight bucket
        values captured under lock, or None when no usage headers were present.
        """
        headers = getattr(response, "headers", {})
        warnings: list[str] = []
        with self._lock:
            any_usage_parsed = False
            for limit_hdr, usage_hdr, is_read in [
                ("X-RateLimit-Limit", "X-RateLimit-Usage", False),
                ("X-ReadRateLimit-Limit", "X-ReadRateLimit-Usage", True),
            ]:
                raw_limit = headers.get(limit_hdr)
                limit_pair = _parse_rate_limit_pair(raw_limit)
                if raw_limit is not None and limit_pair is None:
                    warnings.append(f"malformed {limit_hdr}: {raw_limit!r}")
                elif limit_pair is not None:
                    if is_read:
                        self._read_limit_15min, self._read_limit_daily = limit_pair
                    else:
                        self._limit_15min, self._limit_daily = limit_pair

                raw_usage = headers.get(usage_hdr)
                usage_pair = _parse_rate_limit_pair(raw_usage)
                if raw_usage is not None and usage_pair is None:
                    warnings.append(f"malformed {usage_hdr}: {raw_usage!r}")
                elif usage_pair is not None:
                    any_usage_parsed = True
                    if is_read:
                        self._read_usage_15min, self._read_usage_daily = usage_pair
                    else:
                        self._usage_15min, self._usage_daily = usage_pair

            # Refresh reset timestamps whenever we receive fresh usage data.
            # This prevents the stale check in wait_if_needed from immediately
            # zeroing out usage that was just parsed from the current window.
            if any_usage_parsed:
                self._reset_time_15min = self._next_quarter_hour_ts()
                self._reset_time_daily = self._next_midnight_utc_ts()
                snapshot = (
                    self._usage_15min,
                    self._limit_15min,
                    self._usage_daily,
                    self._limit_daily,
                    self._read_usage_15min,
                    self._read_limit_15min,
                    self._read_usage_daily,
                    self._read_limit_daily,
                )
            else:
                snapshot = None

        for w in warnings:
            _log.debug("[strava] rate limiter: %s", w)
        return any_usage_parsed, snapshot

    def retry_after_for_429(
        self,
        retry_after_header: float | None = None,
        is_non_upload: bool = True,
    ) -> float:
        """Compute pause duration when a real 429 is received.

        Checks daily buckets first (longer wait wins). Falls back to the stored
        15-min reset target + padding even when all buckets are unknown. Uses
        Retry-After header as an additional lower bound.

        is_non_upload mirrors the same flag in wait_if_needed: upload requests
        (is_non_upload=False) are not subject to X-ReadRateLimit-* limits.
        """
        with self._lock:
            daily_near = (
                self._usage_daily is not None
                and self._limit_daily is not None
                and self._usage_daily >= self._limit_daily - _RATE_LIMIT_MARGIN
            ) or (
                is_non_upload
                and (
                    self._read_usage_daily is not None
                    and self._read_limit_daily is not None
                    and self._read_usage_daily
                    >= self._read_limit_daily - _RATE_LIMIT_MARGIN
                )
            )
            if daily_near:
                pause = self._seconds_until(self._reset_time_daily + _RESET_PADDING_S)
            else:
                pause = self._seconds_until(self._reset_time_15min + _RESET_PADDING_S)
        if retry_after_header is not None:
            pause = max(pause, retry_after_header)
        return pause

    async def wait_if_needed(
        self,
        is_non_upload: bool,
        log_fn: Any,
    ) -> None:
        """Proactively sleep before an API call if we are near the rate limit.

        is_non_upload=True  -- checks both overall and X-ReadRateLimit buckets.
        is_non_upload=False -- checks only overall buckets (POST /uploads etc.).

        Stale usage is zeroed only after _RESET_PADDING_S past the boundary to
        guard against local-clock drift relative to Strava's server clock.
        """
        while True:
            with self._lock:
                now = time.time()
                # Stale-check: zero expired window usage (mutates state under lock).
                # _RESET_PADDING_S delay prevents racing the server-side reset.
                if now >= self._reset_time_15min + _RESET_PADDING_S:
                    self._usage_15min = 0
                    self._read_usage_15min = 0
                    self._reset_time_15min = self._next_quarter_hour_ts()
                if now >= self._reset_time_daily + _RESET_PADDING_S:
                    self._usage_daily = 0
                    self._read_usage_daily = 0
                    self._reset_time_daily = self._next_midnight_utc_ts()

                pause, reason = self._compute_needed_pause(
                    is_non_upload, _RATE_LIMIT_MARGIN
                )

            if pause <= 0:
                return

            if log_fn:
                log_fn(
                    f"[strava] {reason}, pausing {pause:.0f}s"
                    f" until {_format_utc_resume_time(pause)}"
                )
            await asyncio.sleep(pause)
            # Re-check after waking: another task may have updated state.

    def _compute_needed_pause(
        self, is_non_upload: bool, margin: int
    ) -> tuple[float, str]:
        """Return (pause_s, reason). Must be called under self._lock."""
        # Daily buckets first (longer wait wins).
        if (
            self._usage_daily is not None
            and self._limit_daily is not None
            and self._usage_daily >= self._limit_daily - margin
        ):
            return (
                self._seconds_until(self._reset_time_daily + _RESET_PADDING_S),
                f"daily limit reached ({self._usage_daily}/{self._limit_daily})",
            )
        if is_non_upload and (
            self._read_usage_daily is not None
            and self._read_limit_daily is not None
            and self._read_usage_daily >= self._read_limit_daily - margin
        ):
            return (
                self._seconds_until(self._reset_time_daily + _RESET_PADDING_S),
                f"daily non-upload limit reached"
                f" ({self._read_usage_daily}/{self._read_limit_daily})",
            )
        # 15-minute buckets.
        if (
            self._usage_15min is not None
            and self._limit_15min is not None
            and self._usage_15min >= self._limit_15min - margin
        ):
            return (
                self._seconds_until(self._reset_time_15min + _RESET_PADDING_S),
                f"15-min limit reached ({self._usage_15min}/{self._limit_15min})",
            )
        if is_non_upload and (
            self._read_usage_15min is not None
            and self._read_limit_15min is not None
            and self._read_usage_15min >= self._read_limit_15min - margin
        ):
            return (
                self._seconds_until(self._reset_time_15min + _RESET_PADDING_S),
                f"15-min non-upload limit reached"
                f" ({self._read_usage_15min}/{self._read_limit_15min})",
            )
        return 0.0, ""


class _TimeoutHTTPAdapter(HTTPAdapter):
    """requests HTTPAdapter that injects a default timeout on every request."""

    def send(self, request, **kwargs):  # type: ignore[override]
        # requests passes timeout=None explicitly, so setdefault won't fire;
        # replace None with our default.
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = _REQUEST_TIMEOUT_S
        return super().send(request, **kwargs)


def _attach_strava_rate_limiter(
    session: requests.Session,
    rate_limiter: _StravaRateLimiter,
    log_fn: Any = None,
    warn_fn: Any = None,
) -> None:
    """Wrap session.send to update rate-limit state and raise RateLimitError on 429.

    This is the outer hook; attach_debug_logging must be called first so that
    the debug log fires before RateLimitError is raised.

    The hook does NOT sleep -- all waits happen in the async event loop via _call_api.
    Transport exceptions (ConnectionError etc.) still propagate after debug logging.

    log_fn / warn_fn route through SyncLogger so messages reach sync.log.
    """
    original_send = session.send

    def _send(request: Any, **kwargs: Any) -> Any:
        resp = original_send(request, **kwargs)
        usage_parsed, snapshot = rate_limiter.update_from_headers(resp)
        if log_fn is not None and snapshot is not None:
            u15, l15, ud, ld, ru15, rl15, rud, rld = snapshot
            log_fn(
                f"[strava] rate limits:"
                f" 15min={_fmt_bucket(u15, l15)}, daily={_fmt_bucket(ud, ld)},"
                f" read_15min={_fmt_bucket(ru15, rl15)},"
                f" read_daily={_fmt_bucket(rud, rld)}"
            )
        if resp.status_code == 429:
            retry_after = _parse_retry_after_optional(resp.headers) or 0.0
            raise RateLimitError("strava 429", retry_after=retry_after)
        url = getattr(resp, "url", "")
        if not usage_parsed and isinstance(url, str) and "/api/v3/" in url:
            if warn_fn is not None:
                warn_fn(
                    "[strava] rate limit headers absent on API response:"
                    f" {_redact_url(url)}"
                )
        return resp

    session.send = _send  # type: ignore[method-assign]


def _make_strava_session(
    log_fn: Any = None,
    warn_fn: Any = None,
    rate_limiter: _StravaRateLimiter | None = None,
) -> requests.Session:
    session = requests.Session()
    adapter = _TimeoutHTTPAdapter()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if log_fn is not None:
        attach_debug_logging(session, log_fn)  # inner hook: debug log
    if rate_limiter is not None:
        _attach_strava_rate_limiter(  # outer hook: rate limiter + usage log
            session, rate_limiter, log_fn=log_fn, warn_fn=warn_fn
        )
    return session


def _stream_data(streams: Any, key: str) -> list | None:
    s = streams.get(key) if streams is not None else None
    return s.data if s is not None else None


def _fmt_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_gpx(meta: ActivityMeta, streams: Any) -> bytes:
    ET.register_namespace("", _GPX_NS)
    ET.register_namespace("gpxtpx", _TPX_NS)
    g = _GPX_NS
    t = _TPX_NS

    gpx = ET.Element(f"{{{g}}}gpx", {"version": "1.1", "creator": "trainings-sync"})

    meta_el = ET.SubElement(gpx, f"{{{g}}}metadata")
    ET.SubElement(meta_el, f"{{{g}}}name").text = meta.name
    ET.SubElement(meta_el, f"{{{g}}}time").text = _fmt_time(meta.start_time)

    trk = ET.SubElement(gpx, f"{{{g}}}trk")
    ET.SubElement(trk, f"{{{g}}}name").text = meta.name
    if meta.sport_type:
        ET.SubElement(trk, f"{{{g}}}type").text = meta.sport_type
    trkseg = ET.SubElement(trk, f"{{{g}}}trkseg")

    time_data = _stream_data(streams, "time") or []
    latlng_data = _stream_data(streams, "latlng") or []
    alt_data = _stream_data(streams, "altitude")
    hr_data = _stream_data(streams, "heartrate")
    cad_data = _stream_data(streams, "cadence")

    for i, (t_s, ll) in enumerate(zip(time_data, latlng_data, strict=False)):
        trkpt = ET.SubElement(
            trkseg, f"{{{g}}}trkpt", {"lat": str(ll[0]), "lon": str(ll[1])}
        )
        if alt_data is not None and i < len(alt_data):
            ET.SubElement(trkpt, f"{{{g}}}ele").text = str(alt_data[i])
        ET.SubElement(trkpt, f"{{{g}}}time").text = _fmt_time(
            meta.start_time + timedelta(seconds=t_s)
        )
        has_hr = hr_data is not None and i < len(hr_data)
        has_cad = cad_data is not None and i < len(cad_data)
        if has_hr or has_cad:
            ext = ET.SubElement(trkpt, f"{{{g}}}extensions")
            tpe = ET.SubElement(ext, f"{{{t}}}TrackPointExtension")
            if has_hr:
                ET.SubElement(tpe, f"{{{t}}}hr").text = str(hr_data[i])  # type: ignore[index]
            if has_cad:
                ET.SubElement(tpe, f"{{{t}}}cad").text = str(cad_data[i])  # type: ignore[index]

    return ET.tostring(gpx, encoding="utf-8", xml_declaration=True)


def _build_tcx(meta: ActivityMeta, streams: Any) -> bytes:
    ET.register_namespace("", _TCD_NS)
    ET.register_namespace("ae", _AE_NS)
    c = _TCD_NS
    a = _AE_NS

    tcd = ET.Element(f"{{{c}}}TrainingCenterDatabase")
    acts = ET.SubElement(tcd, f"{{{c}}}Activities")
    act = ET.SubElement(acts, f"{{{c}}}Activity", {"Sport": meta.sport_type})
    ET.SubElement(act, f"{{{c}}}Id").text = _fmt_time(meta.start_time)

    start_str = _fmt_time(meta.start_time)
    lap = ET.SubElement(act, f"{{{c}}}Lap", {"StartTime": start_str})
    ET.SubElement(lap, f"{{{c}}}TotalTimeSeconds").text = str(meta.elapsed_s or 0)
    ET.SubElement(lap, f"{{{c}}}Intensity").text = "Active"
    ET.SubElement(lap, f"{{{c}}}TriggerMethod").text = "Manual"

    time_data = _stream_data(streams, "time") or []
    hr_data = _stream_data(streams, "heartrate")
    cad_data = _stream_data(streams, "cadence")
    watts_data = _stream_data(streams, "watts")

    if time_data:
        track = ET.SubElement(lap, f"{{{c}}}Track")
        for i, t_s in enumerate(time_data):
            tp = ET.SubElement(track, f"{{{c}}}Trackpoint")
            ET.SubElement(tp, f"{{{c}}}Time").text = _fmt_time(
                meta.start_time + timedelta(seconds=t_s)
            )
            if hr_data is not None and i < len(hr_data):
                hr_el = ET.SubElement(tp, f"{{{c}}}HeartRateBpm")
                ET.SubElement(hr_el, f"{{{c}}}Value").text = str(hr_data[i])
            if cad_data is not None and i < len(cad_data):
                ET.SubElement(tp, f"{{{c}}}Cadence").text = str(cad_data[i])
            if watts_data is not None and i < len(watts_data):
                ext = ET.SubElement(tp, f"{{{c}}}Extensions")
                tpx = ET.SubElement(ext, f"{{{a}}}TPX")
                ET.SubElement(tpx, f"{{{a}}}Watts").text = str(watts_data[i])

    return ET.tostring(tcd, encoding="utf-8", xml_declaration=True)


class StravaConnector(ServiceConnector):
    _max_concurrent = _MAX_CONCURRENT
    # Shared per client_id so all connectors under the same Strava app see the
    # same rate-limit state.  Protected by _limiter_registry_lock for safe
    # concurrent construction.
    _limiter_registry: ClassVar[dict[int, _StravaRateLimiter]] = {}
    _limiter_registry_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        credentials: StravaCredentials,
        tracker: TaskTracker,
        on_token_refresh: Callable[[StravaCredentials, str], None] | None = None,
    ) -> None:
        super().__init__(tracker)
        self._credentials = credentials
        self._on_token_refresh = on_token_refresh
        self._client: Client | None = None
        self._athlete_id: str = ""
        self._athlete_name: str = ""
        self._token_generation: int = 0
        self._token_refresh_lock: asyncio.Lock = asyncio.Lock()
        # Shared limiter: all connectors with the same client_id use one instance
        # so they see each other's usage and don't collectively exceed the quota.
        with StravaConnector._limiter_registry_lock:
            if credentials.client_id not in StravaConnector._limiter_registry:
                StravaConnector._limiter_registry[credentials.client_id] = (
                    _StravaRateLimiter()
                )
            self._rate_limiter = StravaConnector._limiter_registry[
                credentials.client_id
            ]

    @property
    def user_label(self) -> str:
        return (
            self._athlete_name or self._athlete_id or str(self._credentials.client_id)
        )

    def _require_client(self) -> Client:
        if self._client is None:
            raise RuntimeError("Not logged in - call login() first")
        return self._client

    async def _call_api(
        self,
        fn: Any,
        *args: Any,
        is_non_upload: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Rate-limit-aware wrapper around every Strava API call.

        Waits proactively before each attempt, retries on 429 (RateLimitError from
        the session hook), re-raises HTTPError for 401 and other codes so call sites
        can handle token refresh as before.
        """
        log = self._tracker.sync_logger
        log_fn = log.info if log else None
        for attempt in range(_CALL_API_MAX_ATTEMPTS):
            await self._rate_limiter.wait_if_needed(
                is_non_upload=is_non_upload,
                log_fn=log_fn,
            )
            try:
                return await _run_with_timeout(asyncio.to_thread(fn, *args, **kwargs))
            except RateLimitError as exc:
                # Raised by the session.send hook; limiter state already updated.
                if attempt == _CALL_API_MAX_ATTEMPTS - 1:
                    # Last attempt: raise immediately rather than sleeping for up to
                    # 24 h and then failing anyway.
                    raise TransientDownloadError(
                        f"strava 429 after {_CALL_API_MAX_ATTEMPTS} retries"
                    ) from exc
                retry_after_header = exc.retry_after if exc.retry_after > 0 else None
                pause = self._rate_limiter.retry_after_for_429(
                    retry_after_header, is_non_upload=is_non_upload
                )
                if log:
                    log.warning(
                        f"[strava] 429 received, pausing {pause:.0f}s"
                        f" until {_format_utc_resume_time(pause)}"
                    )
                await asyncio.sleep(pause)
            except requests.HTTPError:
                # 401 and other HTTP errors propagate to call-site error handling
                # (_raise_for_http_error handles 401 token refresh).
                raise

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"Strava ({self._credentials.client_id}): login", total=1
        )
        log = self._tracker.sync_logger
        log_fn = log.debug if log else None
        warn_fn = log.warning if log else None
        if log:
            log.info(f"[strava] Login: client_id={self._credentials.client_id}")
        try:
            # Inject session via constructor; stravalib uses client.protocol.rsession.
            tmp_client = Client(
                rate_limit_requests=False,
                requests_session=_make_strava_session(
                    log_fn, warn_fn=warn_fn, rate_limiter=self._rate_limiter
                ),
            )
            token_info = await self._call_api(
                tmp_client.refresh_access_token,
                is_non_upload=False,
                client_id=self._credentials.client_id,
                client_secret=self._credentials.client_secret,
                refresh_token=self._credentials.refresh_token,
            )
            new_credentials = StravaCredentials(
                client_id=self._credentials.client_id,
                client_secret=self._credentials.client_secret,
                refresh_token=token_info["refresh_token"],
            )
            self._credentials = new_credentials
            self._client = Client(
                access_token=token_info["access_token"],
                rate_limit_requests=False,
                requests_session=_make_strava_session(
                    log_fn, warn_fn=warn_fn, rate_limiter=self._rate_limiter
                ),
            )
            try:
                athlete = await self._call_api(
                    self._client.get_athlete, is_non_upload=True
                )
                self._athlete_id = str(athlete.id) if athlete.id is not None else ""
                parts = [athlete.firstname or "", athlete.lastname or ""]
                self._athlete_name = " ".join(p for p in parts if p)
            finally:
                if self._on_token_refresh is not None:
                    self._on_token_refresh(new_credentials, self.user_label)
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        if log:
            name_part = f", {self._athlete_name}" if self._athlete_name else ""
            log.info(
                f"[strava] Login: success"
                f" (athlete_id={self._athlete_id or 'unknown'}{name_part})"
            )
        self._token_generation += 1
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def _refresh_token_if_needed(self, gen: int) -> None:
        """Refresh token if it hasn't been refreshed since gen was captured."""
        async with self._token_refresh_lock:
            if self._token_generation == gen:
                await self.login()

    async def _raise_for_http_error(self, exc: requests.HTTPError, gen: int) -> None:
        """Raise RateLimitError on 429, refresh token on 401. Returns on other codes."""
        if exc.response is None:  # pragma: no cover
            return
        status = exc.response.status_code
        if status == 429:
            raise RateLimitError(
                str(exc), retry_after=_parse_retry_after(exc.response.headers)
            ) from exc
        if status == 401:
            await self._refresh_token_if_needed(gen)

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        client = self._require_client()
        after = datetime(start.year, start.month, start.day)
        before = datetime(end.year, end.month, end.day) + timedelta(days=1)

        task_name = await self._tracker.add_task(
            f"Strava ({self.user_label}): fetch activity list", total=None
        )
        log = self._tracker.sync_logger
        raw: list = []
        seen_ids: set[int] = set()
        _page_size = 200
        page_num = 0
        try:
            it = iter(client.get_activities(after=after, before=before))
            while True:
                batch: list = await self._call_api(
                    lambda: list(itertools.islice(it, _page_size)),
                    is_non_upload=True,
                )
                if not batch or batch[0].id in seen_ids:
                    break
                page_num += 1
                seen_ids.update(a.id for a in batch)
                raw.extend(batch)
                await self._tracker.advance(task_name, amount=len(batch))
                if log:
                    log.debug(
                        f"[strava] List ({self.user_label}):"
                        f" page {page_num} -> {len(batch)} activities"
                    )
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        await self._tracker.finish(task_name)
        return [
            ActivityMeta(
                external_id=str(a.id),
                name=a.name or "",
                sport_type=a.sport_type.root if a.sport_type else "",
                start_time=a.start_date or datetime.min.replace(tzinfo=timezone.utc),
                elapsed_s=int(a.elapsed_time) if a.elapsed_time is not None else None,
            )
            for a in raw
        ]

    async def _fetch_activity_detail(
        self, client: Client, activity_id: int, gen: int
    ) -> Any:
        try:
            return await self._call_api(
                client.get_activity, activity_id, is_non_upload=True
            )
        except requests.HTTPError as exc:
            await self._raise_for_http_error(exc, gen)
            status = exc.response.status_code if exc.response is not None else 0
            if status in (403, 404):
                raise ActivityUnavailableError(str(exc)) from exc
            raise TransientDownloadError(str(exc)) from exc
        except requests.RequestException as exc:
            raise TransientDownloadError(str(exc)) from exc

    async def _fetch_activity_streams(
        self, client: Client, activity_id: int, gen: int
    ) -> tuple[Any, bool]:
        """Returns (streams, no_streams). no_streams=True means minimal TCX fallback."""
        try:
            streams = await self._call_api(
                client.get_activity_streams,
                activity_id,
                is_non_upload=True,
                types=_STREAM_TYPES,
            )
            return streams, False
        except ObjectNotFound:
            return None, True
        except requests.HTTPError as exc:
            await self._raise_for_http_error(exc, gen)
            status = exc.response.status_code if exc.response is not None else 0
            if status in (403, 404):
                return None, True
            raise TransientDownloadError(str(exc)) from exc
        except requests.RequestException as exc:
            raise TransientDownloadError(str(exc)) from exc

    async def _fetch_photo_list(
        self, client: Client, activity_id: int, gen: int
    ) -> list:
        """Fetch raw photo list. Returns [] on 403/404 (best-effort media)."""
        log = self._tracker.sync_logger
        account = f" ({self.user_label})" if self.user_label else ""
        try:
            return await self._call_api(
                lambda: list(client.get_activity_photos(activity_id, size=2048)),
                is_non_upload=True,
            )
        except requests.HTTPError as exc:
            if exc.response is not None:
                if exc.response.status_code == 429:
                    raise RateLimitError(
                        str(exc), retry_after=_parse_retry_after(exc.response.headers)
                    ) from exc
                if exc.response.status_code == 401:
                    await self._refresh_token_if_needed(gen)
                if exc.response.status_code in (403, 404):
                    if log:
                        log.warning(
                            f"[strava] Download{account}: {activity_id!r}"
                            f" - photo list unavailable ({exc.response.status_code}),"
                            f" skipping media"
                        )
                    return []
            if log:
                log.warning(
                    f"[strava] Download{account}: {activity_id!r}"
                    f" - failed to fetch photo list: {exc}"
                )
            raise TransientDownloadError(str(exc)) from exc
        except Exception as exc:
            if log:
                log.warning(
                    f"[strava] Download{account}: {activity_id!r}"
                    f" - failed to fetch photo list: {exc}"
                )
            raise TransientDownloadError(str(exc)) from exc

    async def _fetch_photos(
        self, client: Client, activity_id: int, gen: int
    ) -> list[MediaItem]:
        log = self._tracker.sync_logger
        account = f" ({self.user_label})" if self.user_label else ""
        photos = await self._fetch_photo_list(client, activity_id, gen)
        items: list[MediaItem] = []
        for i, photo in enumerate(photos, 1):
            url = (photo.urls or {}).get("2048") or (photo.urls or {}).get("100")
            if not url:
                continue
            try:
                content = await _run_with_timeout(
                    asyncio.to_thread(
                        _fetch_url_bytes,
                        url,
                        _PHOTO_DOWNLOAD_TIMEOUT_S,
                        log.debug if log else None,
                    )
                )
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status in (403, 404):
                    if log:
                        log.warning(
                            f"[strava] Download{account}: {activity_id!r}"
                            f" - photo #{i} unavailable ({status}), skipping"
                        )
                    continue
                if log:
                    log.warning(
                        f"[strava] Download{account}: {activity_id!r}"
                        f" - failed to download photo #{i}: {exc}"
                    )
                raise TransientDownloadError(str(exc)) from exc
            except Exception as exc:
                if log:
                    log.warning(
                        f"[strava] Download{account}: {activity_id!r}"
                        f" - failed to download photo #{i}: {exc}"
                    )
                raise TransientDownloadError(str(exc)) from exc
            items.append(
                MediaItem(
                    content=content,
                    media_type="photo",
                    caption=getattr(photo, "caption", None) or None,
                    url=url,
                )
            )
        return items

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        client = self._require_client()
        log = self._tracker.sync_logger
        activity_id = int(meta.external_id)
        gen = self._token_generation  # capture before any API call

        raw = await self._fetch_activity_detail(client, activity_id, gen)
        description: str | None = getattr(raw, "description", None) or None
        streams, no_streams = await self._fetch_activity_streams(
            client, activity_id, gen
        )

        has_photos = (getattr(raw, "total_photo_count", None) or 0) > 0
        media = await self._fetch_photos(client, activity_id, gen) if has_photos else []

        if no_streams or not _stream_data(streams, "time"):
            if log:
                log.info(
                    f"[strava] Download ({self.user_label}): {meta.external_id!r}"
                    f" {meta.name!r} - no sensor data, minimal TCX fallback"
                )
            return Activity(
                external_id=meta.external_id,
                name=meta.name,
                sport_type=meta.sport_type,
                start_time=meta.start_time,
                elapsed_s=meta.elapsed_s,
                content=_build_tcx(meta, None),
                format="tcx",
                description=description,
                media=tuple(media),
            )
        if bool(_stream_data(streams, "latlng")):
            content = _build_gpx(meta, streams)
            fmt = "gpx"
        else:
            content = _build_tcx(meta, streams)
            fmt = "tcx"
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            elapsed_s=meta.elapsed_s,
            content=content,
            format=fmt,
            description=description,
            media=tuple(media),
        )

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        client = self._require_client()
        log = self._tracker.sync_logger
        # Lambda recreates BytesIO on each attempt: requests consumes file-like
        # objects when preparing multipart uploads, so a retry must start fresh.
        uploader = await self._call_api(
            lambda: client.upload_activity(
                activity_file=io.BytesIO(activity.content),
                data_type=activity.format,  # type: ignore[arg-type]
                name=activity.name,
            ),
            is_non_upload=False,
        )
        # Replace blocking uploader.wait() (uses time.sleep) with async polling
        # so each poll goes through the rate limiter and the event loop stays live.
        while uploader.activity_id is None:
            await self._call_api(uploader.poll, is_non_upload=True)
            await asyncio.sleep(_UPLOAD_POLL_INTERVAL_S)
        uploaded_id = uploader.activity_id
        if activity.description:
            if uploaded_id:
                await self._call_api(
                    client.update_activity,
                    uploaded_id,
                    is_non_upload=True,
                    description=activity.description,
                )
            elif log:
                log.warning(
                    f"[strava] Upload ({self.user_label}): {activity.external_id!r}"
                    " - description not set (activity ID unavailable after upload)"
                )
        return None

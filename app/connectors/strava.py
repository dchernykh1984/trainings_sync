from __future__ import annotations

import asyncio
import io
import itertools
import logging
import os
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from stravalib import Client
from stravalib.exc import ObjectNotFound

from app.connectors.base import (
    Activity,
    ActivityMeta,
    MediaItem,
    RateLimitError,
    ServiceConnector,
    TransientDownloadError,
)
from app.credentials.base import StravaCredentials
from app.tracking.tracker import TaskTracker

os.environ.setdefault("SILENCE_TOKEN_WARNINGS", "true")
logging.getLogger("stravalib").setLevel(logging.ERROR)

_STREAM_TYPES = ["time", "latlng", "altitude", "heartrate", "cadence", "watts"]
_GPX_NS = "http://www.topografix.com/GPX/1/1"
_TPX_NS = "http://www.garmin.com/xmlschemas/TrackPointExtension/v1"
_TCD_NS = "http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2"
_AE_NS = "http://www.garmin.com/xmlschemas/ActivityExtension/v2"


_PHOTO_DOWNLOAD_TIMEOUT_S: int = 30
_REQUEST_TIMEOUT_S: float = 30.0


def _parse_retry_after(headers: Any, default: float = 900.0) -> float:
    try:
        return float(headers.get("Retry-After", default))
    except (ValueError, TypeError):
        return default


class _TimeoutHTTPAdapter(HTTPAdapter):
    """requests HTTPAdapter that injects a default timeout on every request."""

    def send(self, request, **kwargs):  # type: ignore[override]
        # requests passes timeout=None explicitly, so setdefault won't fire;
        # replace None with our default.
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = _REQUEST_TIMEOUT_S
        return super().send(request, **kwargs)


def _make_strava_session() -> requests.Session:
    session = requests.Session()
    adapter = _TimeoutHTTPAdapter()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _download_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=_PHOTO_DOWNLOAD_TIMEOUT_S) as resp:  # noqa: S310
        return resp.read()


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
    _max_concurrent = 2

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

    @property
    def user_label(self) -> str:
        return (
            self._athlete_name or self._athlete_id or str(self._credentials.client_id)
        )

    def _require_client(self) -> Client:
        if self._client is None:
            raise RuntimeError("Not logged in - call login() first")
        return self._client

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"Strava ({self._credentials.client_id}): login", total=1
        )
        log = self._tracker.sync_logger
        if log:
            log.info(f"[strava] Login: client_id={self._credentials.client_id}")
        try:
            token_info = await asyncio.to_thread(
                Client(requests_session=_make_strava_session()).refresh_access_token,
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
                requests_session=_make_strava_session(),
            )
            try:
                athlete = await asyncio.to_thread(self._client.get_athlete)
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
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

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
                batch: list = await asyncio.to_thread(
                    lambda: list(itertools.islice(it, _page_size))
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

    async def _fetch_photos(self, client: Client, activity_id: int) -> list[MediaItem]:
        log = self._tracker.sync_logger
        account = f" ({self.user_label})" if self.user_label else ""
        try:
            photos: list = await asyncio.to_thread(
                lambda: list(client.get_activity_photos(activity_id, size=2048))
            )
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                raise RateLimitError(
                    str(exc), retry_after=_parse_retry_after(exc.response.headers)
                ) from exc
            if log:
                log.warning(
                    f"[strava] Download{account}: {activity_id!r}"
                    f" - failed to fetch photo list: {exc}"
                )
            return []
        except Exception as exc:
            if log:
                log.warning(
                    f"[strava] Download{account}: {activity_id!r}"
                    f" - failed to fetch photo list: {exc}"
                )
            return []
        items: list[MediaItem] = []
        for i, photo in enumerate(photos or [], 1):
            url = (photo.urls or {}).get("2048") or (photo.urls or {}).get("100")
            if not url:
                continue
            try:
                content = await asyncio.to_thread(_download_bytes, url)
            except Exception as exc:
                if log:
                    log.warning(
                        f"[strava] Download{account}: {activity_id!r}"
                        f" - failed to download photo #{i}: {exc}"
                    )
                continue
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

        try:
            raw = await asyncio.to_thread(client.get_activity, activity_id)
        except requests.RequestException as exc:
            if (
                isinstance(exc, requests.HTTPError)
                and exc.response is not None
                and exc.response.status_code == 429
            ):
                retry_after = _parse_retry_after(exc.response.headers)
                raise RateLimitError(str(exc), retry_after=retry_after) from exc
            raise TransientDownloadError(str(exc)) from exc
        description: str | None = getattr(raw, "description", None) or None

        no_streams = False
        try:
            streams = await asyncio.to_thread(
                client.get_activity_streams,
                activity_id,
                types=_STREAM_TYPES,
            )
        except ObjectNotFound:
            no_streams = True
            streams = None
        except requests.RequestException as exc:
            if (
                isinstance(exc, requests.HTTPError)
                and exc.response is not None
                and exc.response.status_code == 429
            ):
                retry_after = _parse_retry_after(exc.response.headers)
                raise RateLimitError(str(exc), retry_after=retry_after) from exc
            raise TransientDownloadError(str(exc)) from exc

        has_photos = (getattr(raw, "total_photo_count", None) or 0) > 0
        media = await self._fetch_photos(client, activity_id) if has_photos else []

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
        uploader = await asyncio.to_thread(
            client.upload_activity,
            activity_file=io.BytesIO(activity.content),
            data_type=activity.format,  # type: ignore[arg-type]
            name=activity.name,
        )
        result = await asyncio.to_thread(uploader.wait)
        if activity.description:
            uploaded_id = getattr(result, "id", None) if result is not None else None
            if uploaded_id:
                await asyncio.to_thread(
                    client.update_activity,
                    uploaded_id,
                    description=activity.description,
                )
            elif log:
                log.warning(
                    f"[strava] Upload ({self.user_label}): {activity.external_id!r}"
                    " - description not set (activity ID unavailable after upload)"
                )
        return None

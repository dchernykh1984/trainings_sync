from __future__ import annotations

import asyncio
import io
import logging
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests as _requests
from garminconnect import Garmin  # type: ignore[import-untyped]
from garminconnect.exceptions import (
    GarminConnectConnectionError,  # type: ignore[import-untyped]
)

from app.connectors.base import (
    Activity,
    ActivityMeta,
    MediaItem,
    ServiceConnector,
    TransientDownloadError,
)
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

logging.getLogger("garminconnect").setLevel(logging.ERROR)

_PREFERRED_EXTENSIONS = (".fit", ".gpx", ".tcx")
_PAGE_SIZE = 20
# Garmin processes uploads asynchronously; wait before querying for the new activity ID.
_UPLOAD_SETTLE_S: float = 2.0
_PHOTO_SETTLE_S: float = 0.0
_GARMIN_MAX_MEDIA: int = 10

# Maps Strava sport_type strings to Garmin Connect typeKey values.
_STRAVA_TO_GARMIN_TYPE: dict[str, str] = {
    "AlpineSki": "resort_skiing_snowboarding_ws",
    "BackcountrySki": "backcountry_skiing_snowboarding_ws",
    "Hike": "hiking",
    "Ride": "cycling",
    "Run": "running",
    "Swim": "open_water_swimming",
    "TrailRun": "trail_running",
    "VirtualRide": "indoor_cycling",
    "Walk": "walking",
    "WeightTraining": "strength_training",
    "Workout": "fitness_equipment",
    "Yoga": "yoga",
    "Rowing": "rowing",
    "StandUpPaddling": "stand_up_paddleboarding",
}


def _list_activity_photos(client: Garmin, activity_id: int) -> list[dict]:
    url = f"{client.garmin_connect_activity}/{activity_id}/image"
    result = client.connectapi(url)
    return result if isinstance(result, list) else []


def _download_garmin_photo(url: str) -> bytes:
    resp = _requests.get(url, timeout=30)
    resp.raise_for_status()
    return bytes(resp.content)


def _upload_photo_to_activity(
    client: Garmin, activity_id: int, content: bytes, index: int
) -> None:
    url = f"{client.garmin_connect_activity}/{activity_id}/image"
    client.client.post(
        "connectapi",
        url,
        files={"file": (f"photo_{index}.jpg", io.BytesIO(content), "image/jpeg")},
        api=True,
    )


def _set_activity_description(
    client: Garmin, activity_id: int, description: str
) -> None:
    url = f"{client.garmin_connect_activity}/{activity_id}"
    client.client.put(
        "connectapi",
        url,
        json={"activityId": activity_id, "description": description},
        api=True,
    )


async def _ids_on_date(client: Garmin, date_str: str) -> set[int]:
    raw: list[dict] = await asyncio.to_thread(
        client.get_activities_by_date, date_str, date_str
    )
    return {int(a["activityId"]) for a in raw if "activityId" in a}


async def _find_uploaded_id(
    client: Garmin,
    activity: Activity,
    pre_existing_ids: set[int],
) -> int | None:
    """Return Garmin activity ID for a just-uploaded activity.

    Garmin processes uploads asynchronously, so we wait briefly then search by
    start_time among activities on the same date, excluding pre-existing ones.
    """
    await asyncio.sleep(_UPLOAD_SETTLE_S)
    date_str = activity.start_time.strftime("%Y-%m-%d")
    raw: list[dict] = await asyncio.to_thread(
        client.get_activities_by_date, date_str, date_str
    )
    for a in raw:
        if int(a.get("activityId", 0)) in pre_existing_ids:
            continue
        try:
            a_start = datetime.fromisoformat(
                a["startTimeGMT"].replace(" ", "T")
            ).replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        if abs((a_start - activity.start_time).total_seconds()) < 60:
            return int(a["activityId"])
    return None


class GarminConnector(ServiceConnector):
    _max_concurrent = 3
    supports_media_upload = True

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self._credentials = credentials
        self._client: Garmin | None = None

    @property
    def user_label(self) -> str:
        return self._credentials.login

    def _require_client(self) -> Garmin:
        if self._client is None:
            raise RuntimeError("Not logged in - call login() first")
        return self._client

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"Garmin ({self._credentials.login}): login", total=1
        )
        log = self._tracker.sync_logger
        if log:
            log.info(f"[garmin] Login: account={self._credentials.login!r}")
        client = Garmin(
            email=self._credentials.login,
            password=self._credentials.password,
        )
        try:
            await asyncio.to_thread(client.login)
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        if log:
            log.info(f"[garmin] Login: success ({self._credentials.login})")
        self._client = client
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        client = self._require_client()
        task_name = await self._tracker.add_task(
            f"Garmin ({self._credentials.login}): fetch activity list", total=None
        )
        log = self._tracker.sync_logger
        raw: list[dict] = []
        page_start = 0
        url = client.garmin_connect_activities
        try:
            while True:
                params = {
                    "startDate": start.isoformat(),
                    "endDate": end.isoformat(),
                    "start": str(page_start),
                    "limit": str(_PAGE_SIZE),
                }
                page: list[dict] = (
                    await asyncio.to_thread(client.connectapi, url, params=params) or []
                )
                if not page:
                    break
                raw.extend(page)
                page_start += _PAGE_SIZE
                await self._tracker.advance(task_name, amount=len(page))
                if log:
                    log.debug(
                        f"[garmin] List ({self._credentials.login}):"
                        f" page {page_start // _PAGE_SIZE} -> {len(page)} activities"
                    )
                if len(page) < _PAGE_SIZE:
                    break
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        await self._tracker.finish(task_name)
        return [
            ActivityMeta(
                external_id=str(a["activityId"]),
                name=a.get("activityName", ""),
                sport_type=(a.get("activityType") or {}).get("typeKey", ""),
                start_time=datetime.fromisoformat(
                    a["startTimeGMT"].replace(" ", "T")
                ).replace(tzinfo=timezone.utc),
                elapsed_s=int(a["duration"]) if a.get("duration") is not None else None,
            )
            for a in raw
        ]

    async def _fetch_photos(self, client: Garmin, activity_id: int) -> list[MediaItem]:
        log = self._tracker.sync_logger
        account = self._credentials.login
        try:
            photos = await asyncio.to_thread(_list_activity_photos, client, activity_id)
        except Exception as exc:
            if log:
                if "404" in str(exc):
                    log.debug(
                        f"[garmin] Download ({account}): {activity_id!r} - no photos"
                    )
                else:
                    log.warning(
                        f"[garmin] Download ({account}): {activity_id!r}"
                        f" - failed to fetch photo list: {exc}"
                    )
            return []
        items: list[MediaItem] = []
        for i, photo in enumerate(photos, 1):
            url = photo.get("url") or photo.get("imageUrl") or photo.get("originalUrl")
            if not url:
                continue
            try:
                content = await asyncio.to_thread(_download_garmin_photo, url)
            except Exception as exc:
                if log:
                    log.warning(
                        f"[garmin] Download ({account}): {activity_id!r}"
                        f" - failed to download photo #{i}: {exc}"
                    )
                continue
            items.append(
                MediaItem(
                    content=content,
                    media_type="photo",
                    caption=photo.get("caption") or None,
                    url=url,
                )
            )
        return items

    async def _upload_single_photo(
        self,
        client: Garmin,
        activity_id: int,
        activity_external_id: str,
        photo: MediaItem,
        index: int,
        task_name: str | None,
    ) -> None:
        try:
            await asyncio.to_thread(
                _upload_photo_to_activity, client, activity_id, photo.content, index
            )
        except Exception as exc:
            if "404" in str(exc):
                raise  # caller bails on all remaining photos
            if task_name:
                await self._tracker.warn(
                    task_name,
                    f"{activity_external_id!r}: photo #{index} not uploaded ({exc})",
                )

    async def _cap_to_garmin_limit(
        self,
        media: tuple[MediaItem, ...],
        activity_external_id: str,
        task_name: str | None,
    ) -> tuple[MediaItem, ...]:
        if len(media) <= _GARMIN_MAX_MEDIA:
            return media
        n_dropped = len(media) - _GARMIN_MAX_MEDIA
        log = self._tracker.sync_logger
        account = self._credentials.login
        if log:
            log.warning(
                f"[garmin] Upload ({account}): {activity_external_id!r}"
                f" - {len(media)} media files exceed Garmin's {_GARMIN_MAX_MEDIA}-file"
                f" limit; uploading first {_GARMIN_MAX_MEDIA}, {n_dropped} skipped"
            )
        if task_name:
            await self._tracker.warn(
                task_name,
                f"{activity_external_id!r}: {len(media)} media files exceed Garmin's"
                f" {_GARMIN_MAX_MEDIA}-file limit;"
                f" uploading first {_GARMIN_MAX_MEDIA}, {n_dropped} skipped",
            )
        return media[:_GARMIN_MAX_MEDIA]

    async def _upload_photos(
        self,
        client: Garmin,
        activity_id: int,
        activity_external_id: str,
        media: tuple[MediaItem, ...],
        task_name: str | None = None,
    ) -> None:
        if not media:
            return
        await asyncio.sleep(_PHOTO_SETTLE_S)
        media = await self._cap_to_garmin_limit(media, activity_external_id, task_name)
        log = self._tracker.sync_logger
        account = self._credentials.login
        for i, photo in enumerate(media, 1):
            if photo.media_type != "photo":
                if log:
                    log.warning(
                        f"[garmin] Upload ({account}): {activity_external_id!r}"
                        f" - skipped media #{i}"
                        f" (unsupported type: {photo.media_type!r})"
                    )
                if task_name:
                    await self._tracker.warn(
                        task_name,
                        f"{activity_external_id!r}: media #{i} skipped"
                        f" (type {photo.media_type!r} not supported by Garmin)",
                    )
                continue
            try:
                await self._upload_single_photo(
                    client, activity_id, activity_external_id, photo, i, task_name
                )
            except Exception:
                if log:
                    log.warning(
                        f"[garmin] Upload ({account}): {activity_external_id!r}"
                        f" - photo endpoint unavailable (404), skipping all photos"
                    )
                if task_name:
                    await self._tracker.warn(
                        task_name,
                        f"{activity_external_id!r}: photo endpoint unavailable,"
                        f" skipping all photos",
                    )
                return

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        client = self._require_client()
        log = self._tracker.sync_logger
        account = self._credentials.login
        activity_id = int(meta.external_id)
        zip_task: asyncio.Task[bytes] = asyncio.create_task(
            asyncio.to_thread(
                client.download_activity,
                activity_id,
                dl_fmt=Garmin.ActivityDownloadFormat.ORIGINAL,
            )
        )
        detail_task: asyncio.Task[Any] = asyncio.create_task(
            asyncio.to_thread(client.get_activity_details, activity_id)
        )
        photos_task: asyncio.Task[list[MediaItem]] = asyncio.create_task(
            self._fetch_photos(client, activity_id)
        )
        try:
            zip_bytes = await zip_task
        except (
            OSError,
            GarminConnectConnectionError,
            _requests.RequestException,
        ) as exc:
            detail_task.cancel()
            photos_task.cancel()
            await asyncio.gather(detail_task, photos_task, return_exceptions=True)
            raise TransientDownloadError(str(exc)) from exc
        except Exception:
            detail_task.cancel()
            photos_task.cancel()
            await asyncio.gather(detail_task, photos_task, return_exceptions=True)
            raise
        description: str | None = None
        try:
            details = await detail_task
            if isinstance(details, dict):
                description = (
                    details.get("description")
                    or details.get("activityDescription")
                    or None
                )
        except Exception as exc:
            if log:
                log.warning(
                    f"[garmin] Download ({account}): {meta.external_id!r}"
                    f" - description unavailable: {exc}"
                )
        media = await photos_task
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            entry = next(
                (
                    n
                    for ext in _PREFERRED_EXTENSIONS
                    for n in names
                    if n.lower().endswith(ext)
                ),
                None,
            )
            if entry is None:
                raise ValueError(
                    f"activity {meta.external_id}: no supported file in archive"
                    f" (found: {names})"
                )
            fmt = Path(entry).suffix.lstrip(".").lower()
            content = zf.read(entry)
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            content=content,
            format=fmt,
            elapsed_s=meta.elapsed_s,
            description=description,
            media=tuple(media),
        )

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        log = self._tracker.sync_logger
        client = self._require_client()
        account = self._credentials.login
        date_str = activity.start_time.strftime("%Y-%m-%d")
        pre_existing_ids = await _ids_on_date(client, date_str)

        with tempfile.NamedTemporaryFile(
            suffix=f".{activity.format}", delete=False
        ) as f:
            f.write(activity.content)
            tmp_path = f.name
        try:
            await asyncio.to_thread(client.upload_activity, tmp_path)
        except GarminConnectConnectionError as e:
            if "Duplicate Activity" not in str(e):
                raise
            if log:
                log.info(
                    f"[garmin] Upload ({account}): {activity.external_id!r}"
                    f" {activity.start_time.date()} - duplicate, skipped"
                )
            return None
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        activity_id = await _find_uploaded_id(client, activity, pre_existing_ids)
        if activity_id is None:
            if log:
                log.warning(
                    f"[garmin] Upload ({account}): {activity.external_id!r}"
                    f" - uploaded but activity ID not found"
                )
            return None
        if activity.name:
            await asyncio.to_thread(
                client.set_activity_name, str(activity_id), activity.name
            )
        garmin_type = _STRAVA_TO_GARMIN_TYPE.get(activity.sport_type)
        if garmin_type:
            await asyncio.to_thread(
                client.set_activity_type, str(activity_id), 0, garmin_type, 0
            )
        if activity.description:
            await asyncio.to_thread(
                _set_activity_description, client, activity_id, activity.description
            )
        await self._upload_photos(
            client, activity_id, activity.external_id, activity.media, task_name
        )
        if log:
            log.info(
                f"[garmin] Upload ({account}): {activity.external_id!r}"
                f" {activity.start_time.date()} - success (id={activity_id})"
            )
        return None

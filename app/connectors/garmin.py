from __future__ import annotations

import asyncio
import io
import logging
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from garminconnect import Garmin  # type: ignore[import-untyped]
from garminconnect.exceptions import (
    GarminConnectConnectionError,  # type: ignore[import-untyped]
)

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

logging.getLogger("garminconnect").setLevel(logging.ERROR)

_PREFERRED_EXTENSIONS = (".fit", ".gpx", ".tcx")
_PAGE_SIZE = 20
# Garmin processes uploads asynchronously; wait before querying for the new activity ID.
_UPLOAD_SETTLE_S: float = 2.0

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

    def __init__(self, credentials: Credentials, tracker: TaskTracker) -> None:
        super().__init__(tracker)
        self._credentials = credentials
        self._client: Garmin | None = None

    def _require_client(self) -> Garmin:
        if self._client is None:
            raise RuntimeError("Not logged in — call login() first")
        return self._client

    async def login(self) -> None:
        task_name = self._task_name("Garmin: login")
        await self._tracker.add_task(task_name, total=1)
        client = Garmin(
            email=self._credentials.login,
            password=self._credentials.password,
        )
        try:
            await asyncio.to_thread(client.login)
        except Exception as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        self._client = client
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        client = self._require_client()
        task_name = self._task_name("Garmin: fetch activity list")
        await self._tracker.add_task(task_name, total=None)
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
                await self._tracker.advance(task_name)
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

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        client = self._require_client()
        zip_bytes: bytes = await asyncio.to_thread(
            client.download_activity,
            int(meta.external_id),
            dl_fmt=Garmin.ActivityDownloadFormat.ORIGINAL,
        )
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
        )

    async def upload_activity(self, activity: Activity) -> None:
        client = self._require_client()
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
            return
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        activity_id = await _find_uploaded_id(client, activity, pre_existing_ids)
        if activity_id is None:
            return
        if activity.name:
            await asyncio.to_thread(
                client.set_activity_name, str(activity_id), activity.name
            )
        garmin_type = _STRAVA_TO_GARMIN_TYPE.get(activity.sport_type)
        if garmin_type:
            await asyncio.to_thread(
                client.set_activity_type, str(activity_id), 0, garmin_type, 0
            )

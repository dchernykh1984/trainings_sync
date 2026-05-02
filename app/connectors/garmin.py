from __future__ import annotations

import asyncio
import io
import tempfile
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from garminconnect import Garmin  # type: ignore[import-untyped]

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker


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
        raw: list[dict] = await asyncio.to_thread(
            client.get_activities_by_date,
            start.isoformat(),
            end.isoformat(),
        )
        return [
            ActivityMeta(
                external_id=str(a["activityId"]),
                name=a.get("activityName", ""),
                sport_type=(a.get("activityType") or {}).get("typeKey", ""),
                start_time=datetime.fromisoformat(
                    a["startTimeGMT"].replace(" ", "T")
                ).replace(tzinfo=timezone.utc),
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
            fit_name = next(n for n in zf.namelist() if n.endswith(".fit"))
            fit_bytes = zf.read(fit_name)
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            content=fit_bytes,
            format="fit",
        )

    async def upload_activity(self, activity: Activity) -> None:
        client = self._require_client()
        with tempfile.NamedTemporaryFile(
            suffix=f".{activity.format}", delete=False
        ) as f:
            f.write(activity.content)
            tmp_path = f.name
        try:
            await asyncio.to_thread(client.upload_activity, tmp_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

from __future__ import annotations

import asyncio
import io
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

from stravalib import Client

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
from app.credentials.base import StravaCredentials
from app.tracking.tracker import TaskTracker


class StravaConnector(ServiceConnector):
    _max_concurrent = 2

    def __init__(
        self,
        credentials: StravaCredentials,
        tracker: TaskTracker,
        on_token_refresh: Callable[[StravaCredentials], None] | None = None,
    ) -> None:
        super().__init__(tracker)
        self._credentials = credentials
        self._on_token_refresh = on_token_refresh
        self._client: Client | None = None

    def _require_client(self) -> Client:
        if self._client is None:
            raise RuntimeError("Not logged in — call login() first")
        return self._client

    async def login(self) -> None:
        task_name = self._task_name("Strava: login")
        await self._tracker.add_task(task_name, total=1)
        try:
            token_info = await asyncio.to_thread(
                Client().refresh_access_token,
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
            if self._on_token_refresh is not None:
                self._on_token_refresh(new_credentials)
            self._client = Client(access_token=token_info["access_token"])
        except Exception as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        client = self._require_client()
        after = datetime(start.year, start.month, start.day)
        before = datetime(end.year, end.month, end.day) + timedelta(days=1)
        raw: list = await asyncio.to_thread(
            lambda: list(client.get_activities(after=after, before=before))
        )
        return [
            ActivityMeta(
                external_id=str(a.id),
                name=a.name or "",
                sport_type=a.sport_type.root if a.sport_type else "",
                start_time=a.start_date or datetime.min.replace(tzinfo=timezone.utc),
            )
            for a in raw
        ]

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        raise NotImplementedError(
            "Strava API does not expose raw activity file downloads"
        )

    async def upload_activity(self, activity: Activity) -> None:
        client = self._require_client()
        uploader = await asyncio.to_thread(
            client.upload_activity,
            activity_file=io.BytesIO(activity.content),
            data_type=activity.format,  # type: ignore[arg-type]
            name=activity.name,
        )
        await asyncio.to_thread(uploader.wait)

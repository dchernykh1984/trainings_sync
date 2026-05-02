from __future__ import annotations

import asyncio
import itertools
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime

from app.tracking.tracker import TaskTracker


@dataclass(frozen=True)
class ActivityMeta:
    external_id: str
    name: str
    sport_type: str
    start_time: datetime  # must be UTC (utcoffset == 0)

    def __post_init__(self) -> None:
        offset = self.start_time.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError(
                f"ActivityMeta.start_time must be UTC, got: {self.start_time}"
            )


@dataclass(frozen=True)
class Activity(ActivityMeta):
    content: bytes
    format: str


class ServiceConnector(ABC):
    _max_concurrent: int = 5

    def __init__(self, tracker: TaskTracker) -> None:
        self._tracker = tracker
        self._counter = itertools.count(1)

    def _task_name(self, label: str) -> str:
        return f"{label} #{next(self._counter)}"

    @abstractmethod
    async def login(self) -> None: ...

    @abstractmethod
    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]: ...

    @abstractmethod
    async def download_activity(self, meta: ActivityMeta) -> Activity: ...

    @abstractmethod
    async def upload_activity(self, activity: Activity) -> None: ...

    async def download_all(self, start: date, end: date) -> list[Activity]:
        metas = await self.list_activities(start, end)
        if not metas:
            return []

        task_name = self._task_name("Download activities")
        await self._tracker.add_task(task_name, total=len(metas))
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

        task_name = self._task_name("Upload activities")
        await self._tracker.add_task(task_name, total=len(activities))
        sem = asyncio.Semaphore(self._max_concurrent)

        async def _upload(activity: Activity) -> None:
            async with sem:
                await self.upload_activity(activity)
            await self._tracker.advance(task_name)

        try:
            await asyncio.gather(*(_upload(a) for a in activities))
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        await self._tracker.finish(task_name)

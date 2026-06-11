from __future__ import annotations

import asyncio
from datetime import date, timedelta

from app.connectors.local_folder_wellness import LocalFolderWellnessConnector
from app.connectors.wellness_base import (
    TimeModel,
    WellnessConnector,
    WellnessDataType,
)
from app.core.wellness_cache import WellnessCache
from app.tracking.tracker import TaskTracker

_MAX_CONCURRENT = 5


class WellnessOrchestrator:
    def __init__(
        self,
        connectors: dict[str, WellnessConnector],
        cache: WellnessCache,
        tracker: TaskTracker,
    ) -> None:
        self._connectors = connectors
        self._cache = cache
        self._tracker = tracker

    async def run(self, start: date, end: date, *, force: bool = False) -> None:
        sources = {
            cid: c
            for cid, c in self._connectors.items()
            if not isinstance(c, LocalFolderWellnessConnector)
        }
        destinations = {
            cid: c
            for cid, c in self._connectors.items()
            if isinstance(c, LocalFolderWellnessConnector)
        }

        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

        for src_id, source in sources.items():
            if force:
                self._cache.invalidate(src_id)
            await self._download_from_source(source, src_id, dates, start, end)

        for dest in destinations.values():
            await self._upload_to_destination(dest, sources, dates, start, end)

    async def _download_from_source(
        self,
        source: WellnessConnector,
        connector_id: str,
        dates: list[date],
        start: date,
        end: date,
    ) -> None:
        supported = source.supported_types()
        if not supported:
            return

        daily_types = [
            dt for dt, s in supported.items() if s.time_model == TimeModel.DAILY
        ]
        range_types = [
            dt for dt, s in supported.items() if s.time_model == TimeModel.RANGE
        ]
        snapshot_types = [
            dt for dt, s in supported.items() if s.time_model == TimeModel.SNAPSHOT
        ]

        total = len(daily_types) * len(dates) + len(range_types) + len(snapshot_types)
        if total == 0:
            return

        task_name = await self._tracker.add_task(
            f"Wellness download ({connector_id})", total=total
        )
        sem = asyncio.Semaphore(_MAX_CONCURRENT)

        daily_coros = [
            self._fetch_daily(source, connector_id, dt, d, task_name, sem)
            for dt in daily_types
            for d in dates
        ]
        range_coros = [
            self._fetch_range(source, connector_id, dt, start, end, task_name, sem)
            for dt in range_types
        ]
        snapshot_coros = [
            self._fetch_snapshot(source, connector_id, dt, task_name, sem)
            for dt in snapshot_types
        ]

        await asyncio.gather(*daily_coros, *range_coros, *snapshot_coros)
        await self._tracker.finish(task_name)

    async def _fetch_daily(
        self,
        source: WellnessConnector,
        connector_id: str,
        data_type: WellnessDataType,
        d: date,
        task_name: str,
        sem: asyncio.Semaphore,
    ) -> None:
        key = WellnessCache.daily_key(d)
        if self._cache.has(connector_id, data_type, key):
            await self._tracker.advance(task_name)
            return
        async with sem:
            result = await source.fetch_daily(data_type, d)
        if result is not None:
            self._cache.write(connector_id, data_type, key, result)
        await self._tracker.advance(task_name)

    async def _fetch_range(
        self,
        source: WellnessConnector,
        connector_id: str,
        data_type: WellnessDataType,
        start: date,
        end: date,
        task_name: str,
        sem: asyncio.Semaphore,
    ) -> None:
        key = WellnessCache.range_key(start, end)
        if self._cache.has(connector_id, data_type, key):
            await self._tracker.advance(task_name)
            return
        async with sem:
            result = await source.fetch_range(data_type, start, end)
        if result is not None:
            self._cache.write(connector_id, data_type, key, result)
        await self._tracker.advance(task_name)

    async def _fetch_snapshot(
        self,
        source: WellnessConnector,
        connector_id: str,
        data_type: WellnessDataType,
        task_name: str,
        sem: asyncio.Semaphore,
    ) -> None:
        if self._cache.has(connector_id, data_type, WellnessCache.SNAPSHOT_KEY):
            await self._tracker.advance(task_name)
            return
        async with sem:
            result = await source.fetch_snapshot(data_type)
        if result is not None:
            self._cache.write(
                connector_id, data_type, WellnessCache.SNAPSHOT_KEY, result
            )
        await self._tracker.advance(task_name)

    async def _upload_to_destination(
        self,
        dest: WellnessConnector,
        sources: dict[str, WellnessConnector],
        dates: list[date],
        start: date,
        end: date,
    ) -> None:
        push_items = self._collect_push_items(dest, sources, dates, start, end)
        if not push_items:
            return

        task_name = await self._tracker.add_task(
            f"Wellness upload ({dest.connector_id})", total=len(push_items)
        )
        for push_dt, push_d, push_key, push_source_id in push_items:
            data = self._cache.read(push_source_id, push_dt, push_key)
            if data is not None:
                await dest.push_record(push_dt, push_d, data)
            await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    def _collect_push_items(
        self,
        dest: WellnessConnector,
        sources: dict[str, WellnessConnector],
        dates: list[date],
        start: date,
        end: date,
    ) -> list[tuple[WellnessDataType, date | None, str, str]]:
        dest_supported = dest.supported_types()
        if not dest_supported:
            return []

        items: list[tuple[WellnessDataType, date | None, str, str]] = []
        for source_id, source in sources.items():
            source_supported = source.supported_types()
            for data_type in dest_supported:
                if data_type not in source_supported:
                    continue
                items.extend(
                    self._items_for_type(
                        source_id,
                        data_type,
                        source_supported[data_type].time_model,
                        dates,
                        start,
                        end,
                    )
                )
        return items

    def _items_for_type(
        self,
        source_id: str,
        data_type: WellnessDataType,
        time_model: TimeModel,
        dates: list[date],
        start: date,
        end: date,
    ) -> list[tuple[WellnessDataType, date | None, str, str]]:
        items: list[tuple[WellnessDataType, date | None, str, str]] = []
        if time_model == TimeModel.DAILY:
            for d in dates:
                key = WellnessCache.daily_key(d)
                if self._cache.has(source_id, data_type, key):
                    items.append((data_type, d, key, source_id))
        elif time_model == TimeModel.RANGE:
            key = WellnessCache.range_key(start, end)
            if self._cache.has(source_id, data_type, key):
                items.append((data_type, None, key, source_id))
        elif time_model == TimeModel.SNAPSHOT:
            key = WellnessCache.SNAPSHOT_KEY
            if self._cache.has(source_id, data_type, key):
                items.append((data_type, None, key, source_id))
        return items

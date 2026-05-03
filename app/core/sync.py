from __future__ import annotations

import sys
from datetime import date, timedelta

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
from app.core.cache import ActivityCache, CacheEntry
from app.core.planner import DownloadItem, SourceSpec, SyncPlanner
from app.tracking.tracker import TaskTracker

_UNKNOWN_PRIORITY: int = sys.maxsize
_UNKNOWN_ORDER: int = sys.maxsize


def _entries_overlap(
    a: CacheEntry,
    b: CacheEntry,
    min_overlap_s: int,
    fallback_s: int,
) -> bool:
    a_end = a.start_time + timedelta(
        seconds=a.elapsed_s if a.elapsed_s is not None else fallback_s
    )
    b_end = b.start_time + timedelta(
        seconds=b.elapsed_s if b.elapsed_s is not None else fallback_s
    )
    overlap_start = max(a.start_time, b.start_time)
    overlap_end = min(a_end, b_end)
    if overlap_end <= overlap_start:
        return False
    return (overlap_end - overlap_start).total_seconds() >= min_overlap_s


def _shadowed_by_higher_priority(
    entry: CacheEntry,
    candidates: list[CacheEntry],
    source_priority: dict[str, int],
    source_order: dict[str, int],
    min_overlap_s: int,
    fallback_s: int,
) -> bool:
    entry_key = (
        source_priority.get(entry.source_id, _UNKNOWN_PRIORITY),
        source_order.get(entry.source_id, _UNKNOWN_ORDER),
    )
    for other in candidates:
        if other.source_id == entry.source_id:
            continue
        other_key = (
            source_priority.get(other.source_id, _UNKNOWN_PRIORITY),
            source_order.get(other.source_id, _UNKNOWN_ORDER),
        )
        if other_key < entry_key and _entries_overlap(
            entry, other, min_overlap_s, fallback_s
        ):
            return True
    return False


class SyncExecutor:
    def __init__(
        self,
        sources: list[tuple[SourceSpec, ServiceConnector]],
        destinations: list[tuple[str, ServiceConnector]],
        cache: ActivityCache,
        planner: SyncPlanner | None = None,
        tracker: TaskTracker | None = None,
    ) -> None:
        source_ids = [spec.source_id for spec, _ in sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("duplicate source_id in sources")
        dest_ids = [dest_id for dest_id, _ in destinations]
        if len(dest_ids) != len(set(dest_ids)):
            raise ValueError("duplicate destination_id in destinations")

        self._sources = sources
        self._destinations = destinations
        self._cache = cache
        self._planner = planner if planner is not None else SyncPlanner()
        self._tracker = tracker

    async def run(
        self,
        start: date,
        end: date,
        *,
        force: bool = False,
    ) -> None:
        await self._download(start, end, force=force)
        await self._upload(start, end)

    async def _download_source(
        self,
        source_id: str,
        items: list[DownloadItem],
        connector: ServiceConnector,
        tracking: tuple[TaskTracker, str] | None,
    ) -> None:
        try:
            for item in items:
                activity = await connector.download_activity(item.meta)
                entry = CacheEntry(
                    external_id=activity.external_id,
                    source_id=source_id,
                    format=activity.format,
                    start_time=activity.start_time,
                    elapsed_s=activity.elapsed_s,
                    name=activity.name,
                    sport_type=activity.sport_type,
                )
                self._cache.put(entry, activity.content)
                if tracking is not None:
                    await tracking[0].advance(tracking[1])
        except Exception as exc:
            if tracking is not None:
                await tracking[0].fail(tracking[1], error=str(exc))
            raise
        if tracking is not None:
            await tracking[0].finish(tracking[1])

    async def _download(self, start: date, end: date, *, force: bool) -> None:
        source_metas: list[tuple[SourceSpec, list[ActivityMeta]]] = []
        for spec, connector in self._sources:
            metas = await connector.list_activities(start, end)
            source_metas.append((spec, metas))

        plan = self._planner.plan(source_metas, self._cache, force=force)

        by_source: dict[str, list[DownloadItem]] = {}
        for item in plan.to_download:
            by_source.setdefault(item.source_id, []).append(item)

        tracker = self._tracker
        if tracker is not None:
            for source_id, items in by_source.items():
                await tracker.add_task(
                    f"Download {source_id} activities", total=len(items)
                )

        source_map = {spec.source_id: conn for spec, conn in self._sources}
        for source_id, items in by_source.items():
            task_name = f"Download {source_id} activities"
            tracking = (tracker, task_name) if tracker is not None else None
            await self._download_source(
                source_id, items, source_map[source_id], tracking
            )

    def _collect_uploads(self, start: date, end: date) -> dict[str, list[CacheEntry]]:
        source_priority = {spec.source_id: spec.priority for spec, _ in self._sources}
        source_order = {spec.source_id: i for i, (spec, _) in enumerate(self._sources)}
        min_overlap_s = self._planner.min_overlap_s
        fallback_s = self._planner.fallback_s

        candidates = [
            e
            for e in self._cache.all_entries()
            if not e.needs_refresh
            and start <= e.start_time.date() <= end
            and self._cache.has(e.external_id, e.source_id)
        ]

        by_dest: dict[str, list[CacheEntry]] = {}
        for entry in candidates:
            if _shadowed_by_higher_priority(
                entry,
                candidates,
                source_priority,
                source_order,
                min_overlap_s,
                fallback_s,
            ):
                continue
            for dest_id, _ in self._destinations:
                if dest_id == entry.source_id:
                    continue
                if dest_id in entry.uploaded_to:
                    continue
                by_dest.setdefault(dest_id, []).append(entry)
        return by_dest

    async def _upload_to_dest(
        self,
        dest_id: str,
        entries: list[CacheEntry],
        connector: ServiceConnector,
        tracking: tuple[TaskTracker, str] | None,
    ) -> None:
        try:
            for entry in entries:
                content = self._cache.read_content(entry)
                activity = Activity(
                    external_id=entry.external_id,
                    name=entry.name,
                    sport_type=entry.sport_type,
                    start_time=entry.start_time,
                    elapsed_s=entry.elapsed_s,
                    content=content,
                    format=entry.format,
                )
                await connector.upload_activity(activity)
                self._cache.mark_uploaded(entry, dest_id)
                if tracking is not None:
                    await tracking[0].advance(tracking[1])
        except Exception as exc:
            if tracking is not None:
                await tracking[0].fail(tracking[1], error=str(exc))
            raise
        if tracking is not None:
            await tracking[0].finish(tracking[1])

    async def _upload(self, start: date, end: date) -> None:
        by_dest = self._collect_uploads(start, end)
        if not by_dest:
            return

        tracker = self._tracker
        if tracker is not None:
            for dest_id, entries in by_dest.items():
                await tracker.add_task(f"Upload to {dest_id}", total=len(entries))

        dest_map = {dest_id: conn for dest_id, conn in self._destinations}
        for dest_id, entries in by_dest.items():
            task_name = f"Upload to {dest_id}"
            tracking = (tracker, task_name) if tracker is not None else None
            await self._upload_to_dest(dest_id, entries, dest_map[dest_id], tracking)

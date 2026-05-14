from __future__ import annotations

import asyncio
from datetime import date

from app.connectors.base import ServiceConnector
from app.core.cache import ActivityCache
from app.core.config import SyncGroupConfig
from app.core.connector_factory import resolve_group_destinations, resolve_group_sources
from app.core.sync import SyncExecutor
from app.tracking.tracker import TaskTracker


def _group_connector_ids(group: SyncGroupConfig) -> set[str]:
    return {src.id for src in group.sources} | set(group.destinations)


class SyncOrchestrator:
    def __init__(
        self,
        groups: tuple[SyncGroupConfig, ...],
        connectors: dict[str, ServiceConnector],
        cache: ActivityCache,
        tracker: TaskTracker | None = None,
        login_tasks: dict[str, asyncio.Task[None]] | None = None,
    ) -> None:
        self._groups = groups
        self._connectors = connectors
        self._cache = cache
        self._tracker = tracker
        self._login_tasks = login_tasks

    async def run(self, start: date, end: date, *, force: bool = False) -> int:
        # One lock per connector; groups acquire in sorted order to prevent deadlocks.
        # Groups with disjoint connector sets run in parallel; groups sharing any
        # connector are serialized on that connector's lock.
        locks: dict[str, asyncio.Lock] = {
            conn_id: asyncio.Lock() for conn_id in self._connectors
        }
        results = await asyncio.gather(
            *(
                self._run_group_locked(group, locks, start, end, force=force)
                for group in self._groups
            )
        )
        return sum(results)

    async def _run_group_locked(
        self,
        group: SyncGroupConfig,
        locks: dict[str, asyncio.Lock],
        start: date,
        end: date,
        *,
        force: bool,
    ) -> int:
        conn_ids = sorted(_group_connector_ids(group))
        acquired: list[asyncio.Lock] = []
        try:
            for conn_id in conn_ids:
                await locks[conn_id].acquire()
                acquired.append(locks[conn_id])
            return await self._run_group(group, start, end, force=force)
        finally:
            for lock in acquired:
                lock.release()

    async def _run_group(
        self,
        group: SyncGroupConfig,
        start: date,
        end: date,
        *,
        force: bool,
    ) -> int:
        log = self._tracker.sync_logger if self._tracker is not None else None
        if log is not None:
            log.info(f"[group] {group.id} - started")
        sources = resolve_group_sources(group, self._connectors)
        destinations = resolve_group_destinations(group, self._connectors, self._cache)
        executor = SyncExecutor(
            sources=sources,
            destinations=destinations,
            cache=self._cache,
            tracker=self._tracker,
            task_prefix=f"{group.id}: ",
            login_tasks=self._login_tasks,
        )
        try:
            await executor.run(start, end, force=force)
        except BaseException:
            if log is not None:
                log.error(f"[group] {group.id} - failed")
            raise
        if log is not None:
            log.info(f"[group] {group.id} - done")
        return executor.download_failures

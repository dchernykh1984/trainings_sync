from __future__ import annotations

import asyncio
from datetime import date

from app.connectors.base import ActivityMeta, ServiceConnector
from app.core.cache import ActivityCache
from app.core.config import SyncGroupConfig
from app.core.connector_factory import resolve_group_destinations, resolve_group_sources
from app.core.planner import SourceSpec
from app.core.sync import SyncExecutor
from app.tracking.tracker import TaskTracker


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
        list_cache: dict[tuple[str, date, date], list[ActivityMeta]] = {}

        # Collect unique source IDs in declaration order across all groups.
        unique_source_ids: list[str] = list(
            dict.fromkeys(src.id for group in self._groups for src in group.sources)
        )
        source_executors = [
            self._build_source_executor(src_id, list_cache)
            for src_id in unique_source_ids
        ]

        # Phase 1: download each source exactly once, in parallel.
        await asyncio.gather(
            *(
                self._download_source_phase(src_id, ex, start, end, force=force)
                for src_id, ex in zip(unique_source_ids, source_executors, strict=True)
            )
        )

        # Phase 2: upload phase per group, built after source downloads complete.
        executors = [(g, self._build_executor(g, list_cache)) for g in self._groups]
        conn_locks: dict[str, asyncio.Lock] = {
            cid: asyncio.Lock() for cid in self._connectors
        }
        await asyncio.gather(
            *(
                self._upload_group_locked(g, ex, conn_locks, start, end)
                for g, ex in executors
            )
        )

        return sum(ex.download_failures for ex in source_executors)

    def _build_source_executor(
        self,
        source_id: str,
        list_cache: dict[tuple[str, date, date], list[ActivityMeta]],
    ) -> SyncExecutor:
        connector = self._connectors[source_id]
        return SyncExecutor(
            sources=[(SourceSpec(source_id=source_id, priority=1), connector)],
            destinations=[],
            cache=self._cache,
            tracker=self._tracker,
            task_prefix="",
            login_tasks=self._login_tasks,
            list_cache=list_cache,
        )

    def _build_executor(
        self,
        group: SyncGroupConfig,
        list_cache: dict[tuple[str, date, date], list[ActivityMeta]],
    ) -> SyncExecutor:
        return SyncExecutor(
            sources=resolve_group_sources(group, self._connectors),
            destinations=resolve_group_destinations(
                group, self._connectors, self._cache
            ),
            cache=self._cache,
            tracker=self._tracker,
            task_prefix=f"{group.id}: ",
            login_tasks=self._login_tasks,
            list_cache=list_cache,
        )

    async def _download_source_phase(
        self,
        source_id: str,
        executor: SyncExecutor,
        start: date,
        end: date,
        *,
        force: bool,
    ) -> None:
        log = self._tracker.sync_logger if self._tracker is not None else None
        if log is not None:
            log.info(f"[source] {source_id} - download started")
        try:
            await executor.download_phase(start, end, force=force)
        except BaseException:
            if log is not None:
                log.error(f"[source] {source_id} - download failed")
            raise
        if log is not None:
            log.info(f"[source] {source_id} - download done")

    async def _upload_group_locked(
        self,
        group: SyncGroupConfig,
        executor: SyncExecutor,
        conn_locks: dict[str, asyncio.Lock],
        start: date,
        end: date,
    ) -> None:
        log = self._tracker.sync_logger if self._tracker is not None else None
        conn_ids = sorted({src.id for src in group.sources} | set(group.destinations))
        acquired: list[asyncio.Lock] = []
        try:
            for conn_id in conn_ids:
                await conn_locks[conn_id].acquire()
                acquired.append(conn_locks[conn_id])
            if log is not None:
                log.info(f"[group] {group.id} - upload started")
            await executor.upload_phase(start, end)
        except BaseException:
            if log is not None:
                log.error(f"[group] {group.id} - upload failed")
            raise
        finally:
            for lock in acquired:
                lock.release()
        if log is not None:
            log.info(f"[group] {group.id} - upload done")

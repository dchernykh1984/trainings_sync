from __future__ import annotations

from datetime import date

from app.connectors.base import ServiceConnector
from app.core.cache import ActivityCache
from app.core.config import SyncGroupConfig
from app.core.connector_factory import resolve_group_destinations, resolve_group_sources
from app.core.sync import SyncExecutor
from app.tracking.tracker import TaskTracker


class SyncOrchestrator:
    def __init__(
        self,
        groups: tuple[SyncGroupConfig, ...],
        connectors: dict[str, ServiceConnector],
        cache: ActivityCache,
        tracker: TaskTracker | None = None,
    ) -> None:
        self._groups = groups
        self._connectors = connectors
        self._cache = cache
        self._tracker = tracker

    async def run(self, start: date, end: date, *, force: bool = False) -> int:
        log = self._tracker.sync_logger if self._tracker is not None else None
        total_failures = 0
        for group in self._groups:
            if log is not None:
                log.info(f"[group] {group.id} - started")
            sources = resolve_group_sources(group, self._connectors)
            destinations = resolve_group_destinations(
                group, self._connectors, self._cache
            )
            executor = SyncExecutor(
                sources=sources,
                destinations=destinations,
                cache=self._cache,
                tracker=self._tracker,
                task_prefix=f"{group.id}: ",
            )
            try:
                await executor.run(start, end, force=force)
            except Exception:
                if log is not None:
                    log.error(f"[group] {group.id} - failed")
                raise
            total_failures += executor.download_failures
            if log is not None:
                log.info(f"[group] {group.id} - done")
        return total_failures

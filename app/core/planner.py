from __future__ import annotations

import bisect
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.connectors.base import ActivityMeta
from app.core.cache import ActivityCache, CacheEntry


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    priority: int  # lower = higher priority


@dataclass(frozen=True)
class DownloadItem:
    source_id: str
    meta: ActivityMeta


@dataclass(frozen=True)
class SyncPlan:
    to_download: tuple[DownloadItem, ...]


_MIN_OVERLAP_S: int = 60
_FALLBACK_S: int = 3600


def _metas_overlap(
    a: ActivityMeta,
    b: ActivityMeta,
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


def _meta_entry_overlap(
    meta: ActivityMeta,
    entry: CacheEntry,
    min_overlap_s: int,
    fallback_s: int,
) -> bool:
    meta_end = meta.start_time + timedelta(
        seconds=meta.elapsed_s if meta.elapsed_s is not None else fallback_s
    )
    entry_end = entry.start_time + timedelta(
        seconds=entry.elapsed_s if entry.elapsed_s is not None else fallback_s
    )
    overlap_start = max(meta.start_time, entry.start_time)
    overlap_end = min(meta_end, entry_end)
    if overlap_end <= overlap_start:
        return False
    return (overlap_end - overlap_start).total_seconds() >= min_overlap_s


class SyncPlanner:
    def __init__(
        self,
        min_overlap_s: int = _MIN_OVERLAP_S,
        fallback_s: int = _FALLBACK_S,
    ) -> None:
        if min_overlap_s < 0:
            raise ValueError("min_overlap_s must be >= 0")
        if fallback_s < 0:
            raise ValueError("fallback_s must be >= 0")
        self._min_overlap_s = min_overlap_s
        self._fallback_s = fallback_s

    @property
    def min_overlap_s(self) -> int:
        return self._min_overlap_s

    @property
    def fallback_s(self) -> int:
        return self._fallback_s

    def plan_items(
        self,
        sources: list[tuple[SourceSpec, list[ActivityMeta]]],
        cache: ActivityCache,
        *,
        force: bool = False,
    ) -> Iterator[DownloadItem | None]:
        sorted_sources = sorted(sources, key=lambda x: x[0].priority)
        source_priority = {spec.source_id: spec.priority for spec, _ in sorted_sources}

        healthy = cache.healthy_entries()
        healthy_ids = frozenset((e.external_id, e.source_id) for e in healthy)
        refresh_ids = frozenset(
            (e.external_id, e.source_id) for e in cache.all_entries() if e.needs_refresh
        )

        healthy_sorted = sorted(healthy, key=lambda e: e.start_time)
        healthy_starts: list[datetime] = [e.start_time for e in healthy_sorted]
        # Window covers the longest possible activity so bisect narrows the range right
        window_s = max(
            (e.elapsed_s for e in healthy_sorted if e.elapsed_s is not None),
            default=self._fallback_s,
        )
        window_s = max(window_s, self._fallback_s)

        already_planned: list[DownloadItem] = []
        for spec, metas in sorted_sources:
            for meta in metas:
                if self._should_download(
                    meta,
                    spec,
                    force,
                    already_planned,
                    source_priority,
                    healthy_sorted,
                    healthy_starts,
                    window_s,
                    healthy_ids,
                    refresh_ids,
                ):
                    item = DownloadItem(source_id=spec.source_id, meta=meta)
                    already_planned.append(item)
                    yield item
                else:
                    yield None

    def plan(
        self,
        sources: list[tuple[SourceSpec, list[ActivityMeta]]],
        cache: ActivityCache,
        *,
        force: bool = False,
    ) -> SyncPlan:
        return SyncPlan(
            to_download=tuple(
                item
                for item in self.plan_items(sources, cache, force=force)
                if item is not None
            )
        )

    def _should_download(
        self,
        meta: ActivityMeta,
        spec: SourceSpec,
        force: bool,
        already_planned: list[DownloadItem],
        source_priority: dict[str, int],
        healthy_sorted: list[CacheEntry],
        healthy_starts: list[datetime],
        window_s: int,
        healthy_ids: frozenset[tuple[str, str]],
        refresh_ids: frozenset[tuple[str, str]],
    ) -> bool:
        if not force and (meta.external_id, spec.source_id) in healthy_ids:
            return False

        if not force:
            exact_needs_refresh = (meta.external_id, spec.source_id) in refresh_ids
            if not exact_needs_refresh:
                meta_end = meta.start_time + timedelta(
                    seconds=meta.elapsed_s
                    if meta.elapsed_s is not None
                    else self._fallback_s
                )
                lo = bisect.bisect_left(
                    healthy_starts, meta.start_time - timedelta(seconds=window_s)
                )
                hi = bisect.bisect_left(healthy_starts, meta_end)
                for e in healthy_sorted[lo:hi]:
                    if (
                        e.source_id != spec.source_id
                        and source_priority.get(e.source_id, spec.priority + 1)
                        <= spec.priority
                        and _meta_entry_overlap(
                            meta, e, self._min_overlap_s, self._fallback_s
                        )
                    ):
                        return False

        for planned in already_planned:
            if planned.source_id != spec.source_id and _metas_overlap(
                meta,
                planned.meta,
                self._min_overlap_s,
                self._fallback_s,
            ):
                return False

        return True

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta

from app.connectors.base import ActivityMeta
from app.core.cache import ActivityCache


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
        already_planned: list[DownloadItem] = []
        for spec, metas in sorted_sources:
            for meta in metas:
                if self._should_download(
                    meta, spec, cache, force, already_planned, source_priority
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
        cache: ActivityCache,
        force: bool,
        already_planned: list[DownloadItem],
        source_priority: dict[str, int],
    ) -> bool:
        if not force and cache.has(meta.external_id, spec.source_id):
            return False

        if not force:
            exact = cache.get_entry(meta.external_id, spec.source_id)
            exact_needs_refresh = exact is not None and exact.needs_refresh
            if not exact_needs_refresh:
                overlapping = cache.find_overlapping(
                    meta,
                    min_overlap_s=self._min_overlap_s,
                    fallback_tolerance_s=self._fallback_s,
                )
                healthy_overlapping = [
                    e
                    for e in overlapping
                    if not e.needs_refresh
                    and e.source_id != spec.source_id
                    and source_priority.get(e.source_id, spec.priority + 1)
                    <= spec.priority
                ]
                if healthy_overlapping:
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

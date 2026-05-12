from __future__ import annotations

import sys
from datetime import date, timedelta

from app.connectors.base import (
    Activity,
    ActivityMeta,
    ActivityUnavailableError,
    ServiceConnector,
)
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


def _entry_overlaps_meta(
    entry: CacheEntry,
    meta: ActivityMeta,
    min_overlap_s: int,
    fallback_s: int,
) -> bool:
    entry_end = entry.start_time + timedelta(
        seconds=entry.elapsed_s if entry.elapsed_s is not None else fallback_s
    )
    meta_end = meta.start_time + timedelta(
        seconds=meta.elapsed_s if meta.elapsed_s is not None else fallback_s
    )
    overlap_start = max(entry.start_time, meta.start_time)
    overlap_end = min(entry_end, meta_end)
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
) -> str | None:
    """Return the shadowing source_id, or None if the entry is not shadowed."""
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
            return other.source_id
    return None


class SyncExecutor:
    def __init__(
        self,
        sources: list[tuple[SourceSpec, ServiceConnector]],
        destinations: list[tuple[str, ServiceConnector]],
        cache: ActivityCache,
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
        self._planner = SyncPlanner()
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

    def _cache_activity(self, source_id: str, activity: Activity) -> CacheEntry:
        entry = CacheEntry(
            external_id=activity.external_id,
            source_id=source_id,
            format=activity.format,
            start_time=activity.start_time,
            elapsed_s=activity.elapsed_s,
            name=activity.name,
            sport_type=activity.sport_type,
            description=activity.description,
        )
        stored = self._cache.put(entry, activity.content)
        if activity.media:
            self._cache.put_media(stored, list(activity.media))
        return stored

    async def _download_source(
        self,
        source_id: str,
        items: list[DownloadItem],
        connector: ServiceConnector,
        tracking: tuple[TaskTracker, str] | None,
    ) -> None:
        log = self._tracker.sync_logger if self._tracker is not None else None
        label = connector.user_label
        account = f" ({label})" if label else ""
        try:
            for item in items:
                try:
                    activity = await connector.download_activity(item.meta)
                except ActivityUnavailableError:
                    if log:
                        log.info(
                            f"[download] {source_id}{account}:"
                            f" {item.meta.external_id!r} - unavailable, skipped"
                        )
                    if tracking is not None:
                        await tracking[0].advance(tracking[1])
                    continue
                stored = self._cache_activity(source_id, activity)
                if log:
                    log.info(
                        f"[download] {source_id}{account}: {activity.external_id!r}"
                        f" {activity.start_time.date()}"
                        f' "{activity.name}" -> {stored.filename}'
                    )
                if tracking is not None:
                    await tracking[0].advance(tracking[1])
        except Exception as exc:
            if tracking is not None:
                await tracking[0].fail(tracking[1], error=str(exc))
            raise
        if tracking is not None:
            await tracking[0].finish(tracking[1])

    async def _plan(
        self,
        source_metas: list[tuple[SourceSpec, list[ActivityMeta]]],
        *,
        force: bool,
    ) -> list[DownloadItem]:
        tracker = self._tracker
        total_metas = sum(len(metas) for _, metas in source_metas)
        plan_task: str | None = None
        if tracker is not None and total_metas > 0:
            plan_task = await tracker.add_task("Sync: plan", total=total_metas)
        to_download: list[DownloadItem] = []
        try:
            for maybe_item in self._planner.plan_items(
                source_metas, self._cache, force=force
            ):
                if maybe_item is not None:
                    to_download.append(maybe_item)
                if plan_task is not None and tracker is not None:
                    await tracker.advance(plan_task)
        except Exception as exc:
            if plan_task is not None and tracker is not None:
                await tracker.fail(plan_task, error=str(exc))
            raise
        if plan_task is not None and tracker is not None:
            await tracker.finish(plan_task)
        self._log_plan_summary(to_download, source_metas)
        return to_download

    def _log_plan_summary(
        self,
        to_download: list[DownloadItem],
        source_metas: list[tuple[SourceSpec, list[ActivityMeta]]],
    ) -> None:
        log = self._tracker.sync_logger if self._tracker is not None else None
        if log is None:
            return
        by_src: dict[str, int] = {}
        for item in to_download:
            by_src[item.source_id] = by_src.get(item.source_id, 0) + 1
        for spec, metas in source_metas:
            to_dl = by_src.get(spec.source_id, 0)
            label = self._source_user_label(spec.source_id)
            account = f" ({label})" if label else ""
            log.info(
                f"[plan] {spec.source_id}{account}:"
                f" {to_dl} to download, {len(metas) - to_dl} skipped"
            )

    async def _download(self, start: date, end: date, *, force: bool) -> None:
        source_metas: list[tuple[SourceSpec, list[ActivityMeta]]] = []
        for spec, connector in self._sources:
            metas = await connector.list_activities(start, end)
            source_metas.append((spec, metas))

        to_download = await self._plan(source_metas, force=force)

        by_source: dict[str, list[DownloadItem]] = {}
        for item in to_download:
            by_source.setdefault(item.source_id, []).append(item)

        tracker = self._tracker
        source_map = {spec.source_id: conn for spec, conn in self._sources}
        source_task_names: dict[str, str] = {}
        if tracker is not None:
            for source_id, items in by_source.items():
                label = source_map[source_id].user_label
                suffix = f" ({label})" if label else ""
                source_task_names[source_id] = await tracker.add_task(
                    f"Download {source_id}{suffix} activities", total=len(items)
                )

        for source_id, items in by_source.items():
            task_name = source_task_names.get(source_id)
            tracking = (
                (tracker, task_name) if tracker is not None and task_name else None
            )
            await self._download_source(
                source_id, items, source_map[source_id], tracking
            )

    def _get_candidates(self, start: date, end: date) -> list[CacheEntry]:
        return [
            e
            for e in self._cache.all_entries()
            if not e.needs_refresh
            and start <= e.start_time.date() <= end
            and self._cache.has(e.external_id, e.source_id)
        ]

    def _source_user_label(self, source_id: str) -> str:
        for spec, connector in self._sources:
            if spec.source_id == source_id:
                return connector.user_label
        for did, connector in self._destinations:
            if did == source_id:
                return connector.user_label
        return ""

    def _dest_user_label(self, dest_id: str) -> str:
        for did, connector in self._destinations:
            if did == dest_id:
                return connector.user_label
        return ""

    def _log_upload_decision(
        self, entry: CacheEntry, dest_id: str | None, reason: str
    ) -> None:
        log = self._tracker.sync_logger if self._tracker is not None else None
        if log is None:
            return
        src_label = self._source_user_label(entry.source_id)
        src_suffix = f" ({src_label})" if src_label else ""
        if dest_id is not None:
            dest_label = self._dest_user_label(dest_id)
            dest_suffix = f" ({dest_label})" if dest_label else ""
            where = f" -> {dest_id}{dest_suffix}"
        else:
            where = ""
        log.debug(
            f"[upload-plan] {entry.source_id}{src_suffix}: {entry.external_id!r}"
            f" {entry.start_time.date()}{where}: {reason}"
        )

    async def _collect_uploads(
        self,
        candidates: list[CacheEntry],
        start: date,
        end: date,
        tracking: tuple[TaskTracker, str] | None = None,
    ) -> dict[str, list[CacheEntry]]:
        if not candidates:
            return {}
        source_priority = {spec.source_id: spec.priority for spec, _ in self._sources}
        source_order = {spec.source_id: i for i, (spec, _) in enumerate(self._sources)}
        min_overlap_s = self._planner.min_overlap_s
        fallback_s = self._planner.fallback_s

        dest_existing: dict[str, list[ActivityMeta]] = {}
        for dest_id, connector in self._destinations:
            dest_existing[dest_id] = await connector.list_activities(start, end)

        by_dest: dict[str, list[CacheEntry]] = {}
        for entry in candidates:
            shadower = _shadowed_by_higher_priority(
                entry,
                candidates,
                source_priority,
                source_order,
                min_overlap_s,
                fallback_s,
            )
            if shadower is not None:
                shadower_label = self._source_user_label(shadower)
                shadower_suffix = f" ({shadower_label})" if shadower_label else ""
                self._log_upload_decision(
                    entry, None, f"shadowed by {shadower}{shadower_suffix}"
                )
            else:
                for dest_id, connector in self._destinations:
                    if dest_id == entry.source_id:
                        continue
                    if dest_id in entry.uploaded_to and connector.has_activity(
                        entry.external_id, entry.source_id
                    ):
                        self._log_upload_decision(entry, dest_id, "already uploaded")
                        continue
                    existing = dest_existing.get(dest_id, [])
                    if any(
                        _entry_overlaps_meta(entry, m, min_overlap_s, fallback_s)
                        for m in existing
                    ):
                        self._cache.mark_uploaded(entry, dest_id)
                        self._log_upload_decision(
                            entry, dest_id, "overlaps existing - marked uploaded"
                        )
                        continue
                    by_dest.setdefault(dest_id, []).append(entry)
                    self._log_upload_decision(entry, dest_id, "queued for upload")
            if tracking is not None:
                await tracking[0].advance(tracking[1])
        return by_dest

    def _compute_borrowed_descriptions(
        self, candidates: list[CacheEntry]
    ) -> dict[tuple[str, str], str]:
        source_priority = {spec.source_id: spec.priority for spec, _ in self._sources}
        source_order = {spec.source_id: i for i, (spec, _) in enumerate(self._sources)}
        min_overlap_s = self._planner.min_overlap_s
        fallback_s = self._planner.fallback_s

        result: dict[tuple[str, str], str] = {}
        for winner in candidates:
            if winner.description is not None:
                continue
            winner_key = (
                source_priority.get(winner.source_id, _UNKNOWN_PRIORITY),
                source_order.get(winner.source_id, _UNKNOWN_ORDER),
            )
            for other in candidates:
                if other.source_id == winner.source_id:
                    continue
                if other.description is None:
                    continue
                other_key = (
                    source_priority.get(other.source_id, _UNKNOWN_PRIORITY),
                    source_order.get(other.source_id, _UNKNOWN_ORDER),
                )
                if winner_key < other_key and _entries_overlap(
                    winner, other, min_overlap_s, fallback_s
                ):
                    result[(winner.external_id, winner.source_id)] = other.description
                    break
        return result

    async def _upload_to_dest(
        self,
        dest_id: str,
        entries: list[CacheEntry],
        connector: ServiceConnector,
        tracking: tuple[TaskTracker, str] | None,
        borrowed_descriptions: dict[tuple[str, str], str] | None = None,
    ) -> None:
        log = self._tracker.sync_logger if self._tracker is not None else None
        dest_label = connector.user_label
        dest_suffix = f" ({dest_label})" if dest_label else ""
        try:
            for entry in entries:
                content = self._cache.read_content(entry)
                description = entry.description
                if description is None and borrowed_descriptions is not None:
                    description = borrowed_descriptions.get(
                        (entry.external_id, entry.source_id)
                    )
                media = self._cache.read_media(entry)
                activity = Activity(
                    external_id=entry.external_id,
                    name=entry.name,
                    sport_type=entry.sport_type,
                    start_time=entry.start_time,
                    elapsed_s=entry.elapsed_s,
                    content=content,
                    format=entry.format,
                    description=description,
                    media=tuple(media),
                )
                local_path = await connector.upload_activity(activity)
                self._cache.mark_uploaded(entry, dest_id, local_path=local_path)
                if log:
                    result = local_path if local_path is not None else "ok"
                    src_label = self._source_user_label(entry.source_id)
                    src_suffix = f" ({src_label})" if src_label else ""
                    log.info(
                        f"[upload] {entry.external_id!r}"
                        f" ({entry.source_id}{src_suffix})"
                        f" -> {dest_id}{dest_suffix}: {result}"
                    )
                if tracking is not None:
                    await tracking[0].advance(tracking[1])
        except Exception as exc:
            if tracking is not None:
                await tracking[0].fail(tracking[1], error=str(exc))
            raise
        if tracking is not None:
            await tracking[0].finish(tracking[1])

    async def _upload(self, start: date, end: date) -> None:
        tracker = self._tracker
        candidates = self._get_candidates(start, end)
        collect_task: str | None = None
        collect_tracking: tuple[TaskTracker, str] | None = None
        if tracker is not None and candidates:
            collect_task = await tracker.add_task(
                "Sync: collect uploads", total=len(candidates)
            )
            collect_tracking = (tracker, collect_task)
        try:
            by_dest = await self._collect_uploads(
                candidates, start, end, tracking=collect_tracking
            )
        except Exception as exc:
            if collect_task is not None and tracker is not None:
                await tracker.fail(collect_task, error=str(exc))
            raise
        if collect_task is not None and tracker is not None:
            await tracker.finish(collect_task)
        if not by_dest:
            return

        borrowed_descriptions = self._compute_borrowed_descriptions(candidates)

        tracker = self._tracker
        dest_map = {dest_id: conn for dest_id, conn in self._destinations}
        dest_task_names: dict[str, str] = {}
        if tracker is not None:
            for dest_id, entries in by_dest.items():
                label = dest_map[dest_id].user_label
                suffix = f" ({label})" if label else ""
                dest_task_names[dest_id] = await tracker.add_task(
                    f"Upload to {dest_id}{suffix}", total=len(entries)
                )

        for dest_id, entries in by_dest.items():
            task_name = dest_task_names.get(dest_id)
            tracking = (
                (tracker, task_name) if tracker is not None and task_name else None
            )
            await self._upload_to_dest(
                dest_id, entries, dest_map[dest_id], tracking, borrowed_descriptions
            )

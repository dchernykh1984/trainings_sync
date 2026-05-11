from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
from app.parsers.base import ActivityParseError, ActivityParser
from app.parsers.fit import FitParser
from app.parsers.gpx import GpxParser
from app.parsers.tcx import TcxParser
from app.tracking.tracker import TaskTracker

_DEFAULT_PARSERS: dict[str, ActivityParser] = {
    ".fit": FitParser(),
    ".gpx": GpxParser(),
    ".tcx": TcxParser(),
}


class LocalFolderConnector(ServiceConnector):
    def __init__(
        self,
        folder: Path,
        tracker: TaskTracker,
        parsers: dict[str, ActivityParser] | None = None,
        cache: object | None = None,
        dest_id: str = "",
    ) -> None:
        super().__init__(tracker)
        self._folder = folder
        self._parsers = parsers if parsers is not None else _DEFAULT_PARSERS
        self._cache = cache
        self._dest_id = dest_id

    async def login(self) -> None:
        task_name = self._task_name("Local folder: connect")
        await self._tracker.add_task(task_name, total=1)
        if not self._folder.is_dir():
            error = f"Folder not found: {self._folder}"
            await self._tracker.fail(task_name, error=error)
            raise FileNotFoundError(error)
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    def _list_from_cache(self, start: date, end: date) -> list[ActivityMeta]:
        from app.core.cache import ActivityCache

        cache: ActivityCache = self._cache  # type: ignore[assignment]
        metas: list[ActivityMeta] = []
        for e in cache.all_entries():
            if self._dest_id not in e.uploaded_to:
                continue
            if not (start <= e.start_time.date() <= end):
                continue
            local_path = dict(e.local_paths).get(self._dest_id)
            if local_path is not None and not Path(local_path).exists():
                continue
            metas.append(
                ActivityMeta(
                    external_id=local_path or "",
                    name=e.name,
                    sport_type=e.sport_type,
                    start_time=e.start_time,
                    elapsed_s=e.elapsed_s,
                )
            )
        return metas

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        if self._cache is not None and self._dest_id:
            task_name = self._task_name("Local folder: scan")
            await self._tracker.add_task(task_name, total=1)
            try:
                metas = await asyncio.to_thread(self._list_from_cache, start, end)
            except Exception as exc:
                await self._tracker.fail(task_name, error=str(exc))
                raise
            await self._tracker.advance(task_name)
            await self._tracker.finish(task_name)
            return metas

        def _scan() -> tuple[list[ActivityMeta], list[str]]:
            result: list[ActivityMeta] = []
            warnings: list[str] = []
            for path in sorted(self._folder.iterdir()):
                parser = self._parsers.get(path.suffix.lower())
                if parser is None:
                    continue
                try:
                    data = parser.parse(path.read_bytes())
                except ActivityParseError as exc:
                    warnings.append(f"Skipped {path.name}: {exc}")
                    continue
                if not (start <= data.start_time.date() <= end):
                    continue
                result.append(
                    ActivityMeta(
                        external_id=str(path),
                        name=data.name or "",
                        sport_type=data.sport_type or "",
                        start_time=data.start_time,
                    )
                )
            return result, warnings

        task_name = self._task_name("Local folder: scan")
        await self._tracker.add_task(task_name, total=1)
        try:
            metas, warnings = await asyncio.to_thread(_scan)
        except Exception as exc:
            await self._tracker.fail(task_name, error=str(exc))
            raise
        for warning in warnings:
            await self._tracker.warn(task_name, warning)
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)
        return metas

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        path = Path(meta.external_id)
        content = await asyncio.to_thread(path.read_bytes)
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            content=content,
            format=path.suffix.lstrip(".").lower(),
        )

    def has_activity(self, external_id: str, source_id: str) -> bool:
        if self._cache is not None and self._dest_id:
            from app.core.cache import ActivityCache

            cache: ActivityCache = self._cache  # type: ignore[assignment]
            entry = cache.get_entry(external_id, source_id)
            if entry is not None:
                local_path = dict(entry.local_paths).get(self._dest_id)
                if local_path is not None:
                    return Path(local_path).is_file()
        activity_stem = Path(external_id).stem
        return any(
            f.stem.endswith(f"_{activity_stem}")
            for f in self._folder.iterdir()
            if f.is_file() and f.suffix.lower() in self._parsers
        )

    async def upload_activity(self, activity: Activity) -> str | None:
        stem = Path(activity.external_id).stem
        filename = (
            f"{activity.start_time.strftime('%Y%m%dT%H%M%S')}_{stem}.{activity.format}"
        )
        dest = self._folder / filename
        await asyncio.to_thread(dest.write_bytes, activity.content)
        return str(dest)

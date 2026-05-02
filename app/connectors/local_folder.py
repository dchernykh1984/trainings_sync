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
    ) -> None:
        super().__init__(tracker)
        self._folder = folder
        self._parsers = parsers if parsers is not None else _DEFAULT_PARSERS

    async def login(self) -> None:
        task_name = self._task_name("Local folder: connect")
        await self._tracker.add_task(task_name, total=1)
        if not self._folder.is_dir():
            error = f"Folder not found: {self._folder}"
            await self._tracker.fail(task_name, error=error)
            raise FileNotFoundError(error)
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        def _scan() -> tuple[list[ActivityMeta], list[str]]:
            metas: list[ActivityMeta] = []
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
                metas.append(
                    ActivityMeta(
                        external_id=str(path),
                        name=data.name or "",
                        sport_type=data.sport_type or "",
                        start_time=data.start_time,
                    )
                )
            return metas, warnings

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

    async def upload_activity(self, activity: Activity) -> None:
        stem = Path(activity.external_id).stem
        filename = (
            f"{activity.start_time.strftime('%Y%m%dT%H%M%S')}_{stem}.{activity.format}"
        )
        dest = self._folder / filename
        await asyncio.to_thread(dest.write_bytes, activity.content)

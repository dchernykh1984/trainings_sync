from __future__ import annotations

import asyncio
import json
import shutil
from datetime import date
from pathlib import Path

from app.connectors.base import Activity, ActivityMeta, MediaItem, ServiceConnector
from app.parsers.base import ActivityParseError, ActivityParser
from app.parsers.fit import FitParser
from app.parsers.gpx import GpxParser
from app.parsers.tcx import TcxParser
from app.tracking.tracker import TaskTracker


def _write_sidecar(path: Path, payload: str) -> None:
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _read_sidecar(path: Path) -> dict:
    """Return parsed sidecar JSON for *path*, or {} when no sidecar exists."""
    sidecar = path.with_suffix(".json")
    if not sidecar.exists():
        return {}
    return json.loads(sidecar.read_text(encoding="utf-8"))


_MEDIA_EXTENSION: dict[str, str] = {"photo": ".jpg", "video": ".mp4"}


def _build_sidecar(description: str | None) -> str | None:
    """Return JSON payload to persist, or None when description is absent."""
    if description is None:
        return None
    return json.dumps({"description": description}, ensure_ascii=False)


def _read_from_stem_dir(path: Path) -> tuple[str | None, list[dict]]:
    """Read description and media refs from same-stem directory.

    Falls back to legacy flat .json sidecar when no stem directory exists.
    """
    stem_dir = path.parent / path.stem
    if not stem_dir.is_dir():
        legacy = _read_sidecar(path)
        return legacy.get("description") or None, []
    description: str | None = None
    meta_path = stem_dir / "meta.json"
    if meta_path.exists():
        description = (
            json.loads(meta_path.read_text(encoding="utf-8")).get("description") or None
        )
    media_refs: list[dict] = []
    media_path = stem_dir / "media.json"
    if media_path.exists():
        raw = json.loads(media_path.read_text(encoding="utf-8"))
        media_refs = raw if isinstance(raw, list) else []
    return description, media_refs


def _write_stem_dir(
    stem_dir: Path,
    description: str | None,
    media: tuple[MediaItem, ...],
) -> None:
    """Write description and media into a same-stem directory, replacing prior state."""
    if stem_dir.exists():
        shutil.rmtree(stem_dir)
    payload = _build_sidecar(description)
    if payload is None and not media:
        return
    stem_dir.mkdir()
    if payload is not None:
        _write_sidecar(stem_dir / "meta.json", payload)
    media_refs: list[dict] = []
    for i, item in enumerate(media, 1):
        ext = _MEDIA_EXTENSION.get(item.media_type, ".bin")
        media_filename = f"{item.media_type}_{i}{ext}"
        (stem_dir / media_filename).write_bytes(item.content)
        ref: dict = {"file": media_filename, "type": item.media_type}
        if item.caption is not None:
            ref["caption"] = item.caption
        media_refs.append(ref)
    if media_refs:
        _write_sidecar(
            stem_dir / "media.json", json.dumps(media_refs, ensure_ascii=False)
        )


_DEFAULT_PARSERS: dict[str, ActivityParser] = {
    ".fit": FitParser(),
    ".gpx": GpxParser(),
    ".tcx": TcxParser(),
}


class LocalFolderConnector(ServiceConnector):
    supports_media_upload = True

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

    @property
    def user_label(self) -> str:
        return str(self._folder)

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"Local folder ({self._folder}): connect", total=1
        )
        log = self._tracker.sync_logger
        if log:
            log.info(f"[local-folder] Connect: folder={self._folder}")
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

    def _scan_disk(
        self, start: date, end: date
    ) -> tuple[list[ActivityMeta], list[str]]:
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

    async def list_activities(self, start: date, end: date) -> list[ActivityMeta]:
        log = self._tracker.sync_logger
        task_name = await self._tracker.add_task(
            f"Local folder ({self._folder}): scan", total=1
        )
        if self._cache is not None and self._dest_id:
            try:
                metas = await asyncio.to_thread(self._list_from_cache, start, end)
            except Exception as exc:
                await self._tracker.fail(task_name, error=str(exc))
                raise
            if log:
                log.info(
                    f"[local-folder] List (cache-backed): {len(metas)} activities"
                    f" in {self._folder}"
                )
        else:
            try:
                metas, warnings = await asyncio.to_thread(self._scan_disk, start, end)
            except Exception as exc:
                await self._tracker.fail(task_name, error=str(exc))
                raise
            for warning in warnings:
                await self._tracker.warn(task_name, warning)
            if log:
                log.info(
                    f"[local-folder] List (disk scan): {len(metas)} activities"
                    f" in {self._folder}"
                )
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)
        return metas

    async def download_activity(self, meta: ActivityMeta) -> Activity:
        path = Path(meta.external_id)
        content = await asyncio.to_thread(path.read_bytes)
        description, media_refs = await asyncio.to_thread(_read_from_stem_dir, path)
        stem_dir = path.parent / path.stem
        media: list[MediaItem] = []
        for ref in media_refs:
            media_type = ref.get("type", "")
            if media_type not in ("photo", "video"):
                continue
            media_filename = ref.get("file", "")
            if not media_filename or Path(media_filename).name != media_filename:
                continue
            media_path = stem_dir / media_filename
            if not media_path.resolve().is_relative_to(stem_dir.resolve()):
                continue
            try:
                media_content = await asyncio.to_thread(media_path.read_bytes)
            except OSError:
                continue
            media.append(
                MediaItem(
                    content=media_content,
                    media_type=media_type,  # type: ignore[arg-type]
                    caption=ref.get("caption") or None,
                )
            )
        return Activity(
            external_id=meta.external_id,
            name=meta.name,
            sport_type=meta.sport_type,
            start_time=meta.start_time,
            elapsed_s=meta.elapsed_s,
            content=content,
            format=path.suffix.lstrip(".").lower(),
            description=description,
            media=tuple(media),
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

    def as_destination(self, cache: object, dest_id: str) -> LocalFolderConnector:
        return LocalFolderConnector(
            folder=self._folder,
            tracker=self._tracker,
            parsers=self._parsers,
            cache=cache,
            dest_id=dest_id,
        )

    async def upload_activity(
        self, activity: Activity, *, task_name: str | None = None
    ) -> str | None:
        stem = Path(activity.external_id).stem
        filename = (
            f"{activity.start_time.strftime('%Y%m%dT%H%M%S')}_{stem}.{activity.format}"
        )
        dest = self._folder / filename
        await asyncio.to_thread(dest.write_bytes, activity.content)
        await asyncio.to_thread(dest.with_suffix(".json").unlink, missing_ok=True)
        await asyncio.to_thread(
            _write_stem_dir,
            self._folder / dest.stem,
            activity.description,
            activity.media,
        )
        return str(dest)

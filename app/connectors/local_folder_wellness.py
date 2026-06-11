from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from app.connectors.wellness_base import (
    DataTypeSpec,
    WellnessConnector,
    WellnessDataType,
)
from app.connectors.wellness_capabilities import LOCAL_FOLDER_CAPABILITIES


class LocalFolderWellnessConnector(WellnessConnector):
    def __init__(
        self,
        connector_id: str,
        folder: Path,
        tracker: object,
    ) -> None:

        super().__init__(tracker)  # type: ignore[arg-type]
        self._connector_id = connector_id
        self._folder = folder

    @property
    def connector_id(self) -> str:
        return self._connector_id

    async def login(self) -> None:
        from app.tracking.tracker import TaskTracker

        tracker: TaskTracker = self._tracker  # type: ignore[assignment]
        task_name = await tracker.add_task(
            f"Local folder wellness ({self._folder}): connect", total=1
        )
        try:
            if not self._folder.is_dir():
                raise FileNotFoundError(f"Folder not found: {self._folder}")
            await tracker.advance(task_name)
            await tracker.finish(task_name)
        except BaseException as exc:
            await tracker.fail(task_name, error=str(exc))
            raise

    def supported_types(self) -> dict[WellnessDataType, DataTypeSpec]:
        return LOCAL_FOLDER_CAPABILITIES

    async def fetch_daily(self, data_type: WellnessDataType, d: date) -> dict | None:
        p = self._data_path(data_type, d.isoformat())
        if not p.is_file():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    async def fetch_range(
        self, data_type: WellnessDataType, start: date, end: date
    ) -> dict | None:
        key = f"{start}_{end}"
        p = self._data_path(data_type, key)
        if not p.is_file():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    async def fetch_snapshot(self, data_type: WellnessDataType) -> dict | None:
        p = self._data_path(data_type, "snapshot")
        if not p.is_file():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    async def push_record(
        self, data_type: WellnessDataType, d: date | None, data: dict
    ) -> None:
        date_key = d.isoformat() if d is not None else "snapshot"
        p = self._data_path(data_type, date_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)

    def _data_path(self, data_type: WellnessDataType, date_key: str) -> Path:
        return self._folder / "wellness" / data_type.value / f"{date_key}.json"

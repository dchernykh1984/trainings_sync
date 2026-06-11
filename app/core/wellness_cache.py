from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path
from typing import ClassVar

from app.connectors.wellness_base import WellnessDataType


class WellnessCache:
    SNAPSHOT_KEY: ClassVar[str] = "snapshot"

    def __init__(self, cache_dir: Path) -> None:
        self._dir = cache_dir / "wellness"

    def has(
        self, connector_id: str, data_type: WellnessDataType, date_key: str
    ) -> bool:
        return self._path(connector_id, data_type, date_key).is_file()

    def read(
        self, connector_id: str, data_type: WellnessDataType, date_key: str
    ) -> dict | None:
        p = self._path(connector_id, data_type, date_key)
        if not p.is_file():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def write(
        self,
        connector_id: str,
        data_type: WellnessDataType,
        date_key: str,
        data: dict,
    ) -> None:
        p = self._path(connector_id, data_type, date_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)

    def invalidate(self, connector_id: str) -> None:
        d = self._dir / connector_id
        if d.is_dir():
            shutil.rmtree(d)

    def _path(
        self, connector_id: str, data_type: WellnessDataType, date_key: str
    ) -> Path:
        return self._dir / connector_id / data_type.value / f"{date_key}.json"

    @staticmethod
    def range_key(start: date, end: date) -> str:
        return f"{start}_{end}"

    @staticmethod
    def daily_key(d: date) -> str:
        return d.isoformat()

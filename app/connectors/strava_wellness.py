from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.connectors.strava import StravaConnector

from app.connectors.wellness_base import (
    DataTypeSpec,
    WellnessConnector,
    WellnessDataType,
)
from app.connectors.wellness_capabilities import STRAVA_CAPABILITIES
from app.tracking.tracker import TaskTracker


class StravaWellnessConnector(WellnessConnector):
    def __init__(
        self,
        connector_id: str,
        strava_connector: StravaConnector,
        tracker: TaskTracker,
    ) -> None:
        super().__init__(tracker)
        self._connector_id = connector_id
        self._strava_connector = strava_connector

    @property
    def _client(self):  # type: ignore[override]
        return self._strava_connector._client

    @property
    def connector_id(self) -> str:
        return self._connector_id

    async def login(self) -> None:
        task_name = await self._tracker.add_task(
            f"Strava wellness ({self._connector_id}): connect", total=1
        )
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    def supported_types(self) -> dict[WellnessDataType, DataTypeSpec]:
        return STRAVA_CAPABILITIES

    async def fetch_snapshot(self, data_type: WellnessDataType) -> dict | None:
        if data_type == WellnessDataType.ATHLETE_STATS:
            return await self._fetch_athlete_stats()
        if data_type == WellnessDataType.ATHLETE_ZONES:
            return await self._fetch_athlete_zones()
        self._log_unsupported("fetch_snapshot", data_type)
        return None

    async def _fetch_athlete_stats(self) -> dict | None:
        log = self._tracker.sync_logger
        try:
            athlete = await asyncio.to_thread(self._client.get_athlete)
            athlete_id = athlete.id
            result = await asyncio.to_thread(self._client.get_athlete_stats, athlete_id)
            return json.loads(result.model_dump_json())
        except Exception as exc:
            if log:
                log.debug(
                    f"[strava-wellness] {self._connector_id}:"
                    f" fetch_snapshot(athlete_stats) failed: {exc}"
                )
            return None

    async def _fetch_athlete_zones(self) -> dict | None:
        log = self._tracker.sync_logger
        try:
            result = await asyncio.to_thread(self._client.get_athlete_zones)
            return json.loads(result.model_dump_json())
        except Exception as exc:
            if log:
                log.debug(
                    f"[strava-wellness] {self._connector_id}:"
                    f" fetch_snapshot(athlete_zones) failed: {exc}"
                )
            return None

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import date
from typing import TYPE_CHECKING, Any

from garminconnect import Garmin  # type: ignore[import-untyped]

from app.connectors.base import _run_with_timeout
from app.connectors.wellness_base import (
    AccessLevel,
    DataTypeSpec,
    WellnessConnector,
    WellnessDataType,
)
from app.connectors.wellness_capabilities import GARMIN_CAPABILITIES
from app.credentials.base import Credentials
from app.tracking.tracker import TaskTracker

if TYPE_CHECKING:
    from app.connectors.garmin import GarminConnector

_UNHANDLED = object()

_DAILY_FETCH: dict[WellnessDataType, Callable[[Garmin, str], Any]] = {
    WellnessDataType.SLEEP: lambda c, d: c.get_sleep_data(d),
    WellnessDataType.HRV: lambda c, d: c.get_hrv_data(d),
    WellnessDataType.HEART_RATES: lambda c, d: c.get_heart_rates(d),
    WellnessDataType.RESTING_HR: lambda c, d: c.get_rhr_day(d),
    WellnessDataType.BODY_BATTERY_EVENTS: lambda c, d: c.get_body_battery_events(d),
    WellnessDataType.STRESS_DAILY: lambda c, d: c.get_stress_data(d),
    WellnessDataType.ALL_DAY_STRESS: lambda c, d: c.get_all_day_stress(d),
    WellnessDataType.SPO2: lambda c, d: c.get_spo2_data(d),
    WellnessDataType.RESPIRATION: lambda c, d: c.get_respiration_data(d),
    WellnessDataType.STEPS_DAILY: lambda c, d: c.get_steps_data(d),
    WellnessDataType.FLOORS: lambda c, d: c.get_floors(d),
    WellnessDataType.INTENSITY_MINUTES: lambda c, d: c.get_intensity_minutes_data(d),
    WellnessDataType.VO2MAX: lambda c, d: c.get_max_metrics(d),
    WellnessDataType.TRAINING_READINESS: lambda c, d: c.get_training_readiness(d),
    WellnessDataType.MORNING_TRAINING_READINESS: lambda c, d: (
        c.get_morning_training_readiness(d)
    ),
    WellnessDataType.TRAINING_STATUS: lambda c, d: c.get_training_status(d),
    WellnessDataType.FITNESS_AGE: lambda c, d: c.get_fitnessage_data(d),
    WellnessDataType.DAILY_WEIGH_INS: lambda c, d: c.get_daily_weigh_ins(d),
    WellnessDataType.HYDRATION: lambda c, d: c.get_hydration_data(d),
    WellnessDataType.USER_SUMMARY: lambda c, d: c.get_user_summary(d),
    WellnessDataType.STATS: lambda c, d: c.get_stats(d),
    WellnessDataType.LIFESTYLE_LOGGING: lambda c, d: c.get_lifestyle_logging_data(d),
}


def _normalize_result(result: Any) -> dict | None:
    if result is None:
        return None
    if isinstance(result, dict):
        return result
    if isinstance(result, list):
        return {"items": result}
    return {"value": result}


class GarminWellnessConnector(WellnessConnector):
    def __init__(
        self,
        connector_id: str,
        credentials: Credentials,
        tracker: TaskTracker,
        client: Garmin | None = None,
    ) -> None:
        super().__init__(tracker)
        self._connector_id = connector_id
        self._credentials = credentials
        self._client: Garmin | None = client

    @classmethod
    def from_garmin_connector(
        cls,
        connector_id: str,
        garmin_connector: GarminConnector,
        tracker: TaskTracker,
    ) -> GarminWellnessConnector:
        instance = cls(
            connector_id=connector_id,
            credentials=garmin_connector._credentials,
            tracker=tracker,
            client=garmin_connector._client,
        )
        return instance

    @property
    def connector_id(self) -> str:
        return self._connector_id

    def _require_client(self) -> Garmin:
        if self._client is None:
            raise RuntimeError("Not logged in - call login() first")
        return self._client

    async def login(self) -> None:
        if self._client is not None:
            return
        task_name = await self._tracker.add_task(
            f"Garmin wellness ({self._credentials.login}): login", total=1
        )
        log = self._tracker.sync_logger
        client = Garmin(
            email=self._credentials.login,
            password=self._credentials.password,
        )
        try:
            await _run_with_timeout(asyncio.to_thread(client.login))
        except BaseException as exc:
            await self._tracker.fail(task_name, error=f"Login failed: {exc}")
            raise
        if log:
            log.info(f"[garmin-wellness] Login: success ({self._credentials.login})")
        self._client = client
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)

    def supported_types(self) -> dict[WellnessDataType, DataTypeSpec]:
        return GARMIN_CAPABILITIES

    async def fetch_daily(self, data_type: WellnessDataType, d: date) -> dict | None:
        fetch_fn = _DAILY_FETCH.get(data_type)
        if fetch_fn is None:
            self._log_unsupported("fetch_daily", data_type)
            return None
        client = self._require_client()
        log = self._tracker.sync_logger
        date_str = d.isoformat()
        try:
            result = await _run_with_timeout(
                asyncio.to_thread(fetch_fn, client, date_str)
            )
            return _normalize_result(result)
        except Exception as exc:
            if log:
                log.debug(
                    f"[garmin-wellness] {self._connector_id}:"
                    f" fetch_daily({data_type.value}, {date_str}) failed: {exc}"
                )
            return None

    async def fetch_range(
        self, data_type: WellnessDataType, start: date, end: date
    ) -> dict | None:
        client = self._require_client()
        log = self._tracker.sync_logger
        start_str = start.isoformat()
        end_str = end.isoformat()
        weeks = max(1, (end - start).days // 7 + 1)
        try:
            result = await self._fetch_range_for_type(
                client, data_type, start_str, end_str, weeks
            )
            return _normalize_result(result)
        except Exception as exc:
            if log:
                log.debug(
                    f"[garmin-wellness] {self._connector_id}:"
                    f" fetch_range({data_type.value},"
                    f" {start_str}, {end_str}) failed: {exc}"
                )
            return None

    async def _fetch_range_for_type(
        self,
        client: Garmin,
        data_type: WellnessDataType,
        start_str: str,
        end_str: str,
        weeks: int,
    ) -> Any:
        result = await self._fetch_range_body_metrics(
            client, data_type, start_str, end_str
        )
        if result is not _UNHANDLED:
            return result
        result = await self._fetch_range_activity_metrics(
            client, data_type, start_str, end_str, weeks
        )
        if result is not _UNHANDLED:
            return result
        self._log_unsupported("fetch_range", data_type)
        return None

    async def _fetch_range_body_metrics(
        self,
        client: Garmin,
        data_type: WellnessDataType,
        start_str: str,
        end_str: str,
    ) -> Any:
        if data_type == WellnessDataType.BODY_BATTERY:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_body_battery, start_str, end_str)
            )
        if data_type == WellnessDataType.BODY_COMPOSITION:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_body_composition, start_str, end_str)
            )
        if data_type == WellnessDataType.WEIGH_INS:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_weigh_ins, start_str, end_str)
            )
        if data_type == WellnessDataType.BLOOD_PRESSURE:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_blood_pressure, start_str, end_str)
            )
        if data_type == WellnessDataType.DAILY_STEPS_RANGE:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_daily_steps, start_str, end_str)
            )
        if data_type == WellnessDataType.LACTATE_THRESHOLD:
            return await _run_with_timeout(
                asyncio.to_thread(
                    client.get_lactate_threshold,
                    start_date=start_str,
                    end_date=end_str,
                )
            )
        return _UNHANDLED

    async def _fetch_range_activity_metrics(
        self,
        client: Garmin,
        data_type: WellnessDataType,
        start_str: str,
        end_str: str,
        weeks: int,
    ) -> Any:
        if data_type == WellnessDataType.WEEKLY_STEPS:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_weekly_steps, end_str, weeks)
            )
        if data_type == WellnessDataType.WEEKLY_STRESS:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_weekly_stress, end_str, weeks)
            )
        if data_type == WellnessDataType.WEEKLY_INTENSITY_MINUTES:
            return await _run_with_timeout(
                asyncio.to_thread(
                    client.get_weekly_intensity_minutes, start_str, end_str
                )
            )
        if data_type == WellnessDataType.ENDURANCE_SCORE:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_endurance_score, start_str, end_str)
            )
        if data_type == WellnessDataType.RUNNING_TOLERANCE:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_running_tolerance, start_str, end_str)
            )
        if data_type == WellnessDataType.RACE_PREDICTIONS:
            return await _run_with_timeout(
                asyncio.to_thread(
                    client.get_race_predictions,
                    startdate=start_str,
                    enddate=end_str,
                )
            )
        if data_type == WellnessDataType.HILL_SCORE:
            return await _run_with_timeout(
                asyncio.to_thread(client.get_hill_score, start_str, end_str)
            )
        return _UNHANDLED

    async def fetch_snapshot(self, data_type: WellnessDataType) -> dict | None:
        client = self._require_client()
        log = self._tracker.sync_logger
        if data_type != WellnessDataType.PERSONAL_RECORDS:
            self._log_unsupported("fetch_snapshot", data_type)
            return None
        try:
            result = await _run_with_timeout(
                asyncio.to_thread(client.get_personal_record)
            )
            return _normalize_result(result)
        except Exception as exc:
            if log:
                log.debug(
                    f"[garmin-wellness] {self._connector_id}:"
                    f" fetch_snapshot({data_type.value}) failed: {exc}"
                )
            return None

    async def push_record(
        self, data_type: WellnessDataType, d: date | None, data: dict
    ) -> None:
        spec = GARMIN_CAPABILITIES.get(data_type)
        if spec is None or spec.access != AccessLevel.READ_WRITE:
            self._log_unsupported("push_record", data_type)
            return
        client = self._require_client()
        log = self._tracker.sync_logger
        try:
            await self._push_for_type(client, data_type, d, data)
        except Exception as exc:
            if log:
                log.warning(
                    f"[garmin-wellness] {self._connector_id}:"
                    f" push_record({data_type.value}) failed: {exc}"
                )

    async def _push_for_type(
        self,
        client: Garmin,
        data_type: WellnessDataType,
        d: date | None,
        data: dict,
    ) -> None:
        if data_type == WellnessDataType.DAILY_WEIGH_INS:
            weight = data.get("weight") or data.get("value")
            if weight is not None:
                await _run_with_timeout(
                    asyncio.to_thread(client.add_weigh_in, float(weight))
                )
            return
        if data_type == WellnessDataType.HYDRATION:
            value_ml = data.get("value_in_ml") or data.get("value")
            cdate = d.isoformat() if d is not None else None
            if value_ml is not None:
                await _run_with_timeout(
                    asyncio.to_thread(
                        client.add_hydration_data,
                        float(value_ml),
                        cdate=cdate,
                    )
                )
            return
        if data_type in (WellnessDataType.WEIGH_INS, WellnessDataType.BODY_COMPOSITION):
            weight = data.get("weight") or data.get("value")
            timestamp = data.get("timestamp")
            if weight is not None:
                await _run_with_timeout(
                    asyncio.to_thread(
                        client.add_body_composition,
                        timestamp,
                        float(weight),
                    )
                )
            return
        if data_type == WellnessDataType.BLOOD_PRESSURE:
            systolic = data.get("systolic")
            diastolic = data.get("diastolic")
            pulse = data.get("pulse", 0)
            if systolic is not None and diastolic is not None:
                await _run_with_timeout(
                    asyncio.to_thread(
                        client.set_blood_pressure,
                        int(systolic),
                        int(diastolic),
                        int(pulse),
                    )
                )
            return
        self._log_unsupported("push_record", data_type)

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date
from enum import Enum

from app.tracking.tracker import TaskTracker


class WellnessDataType(str, Enum):
    SLEEP = "sleep"
    HRV = "hrv"
    HEART_RATES = "heart_rates"
    RESTING_HR = "resting_hr"
    BODY_BATTERY_EVENTS = "body_battery_events"
    STRESS_DAILY = "stress_daily"
    ALL_DAY_STRESS = "all_day_stress"
    SPO2 = "spo2"
    RESPIRATION = "respiration"
    STEPS_DAILY = "steps_daily"
    FLOORS = "floors"
    INTENSITY_MINUTES = "intensity_minutes"
    VO2MAX = "vo2max"
    TRAINING_READINESS = "training_readiness"
    MORNING_TRAINING_READINESS = "morning_training_readiness"
    TRAINING_STATUS = "training_status"
    FITNESS_AGE = "fitness_age"
    DAILY_WEIGH_INS = "daily_weigh_ins"
    HYDRATION = "hydration"
    USER_SUMMARY = "user_summary"
    STATS = "stats"
    LIFESTYLE_LOGGING = "lifestyle_logging"
    BODY_BATTERY = "body_battery"
    BODY_COMPOSITION = "body_composition"
    WEIGH_INS = "weigh_ins"
    BLOOD_PRESSURE = "blood_pressure"
    DAILY_STEPS_RANGE = "daily_steps_range"
    WEEKLY_STEPS = "weekly_steps"
    WEEKLY_STRESS = "weekly_stress"
    WEEKLY_INTENSITY_MINUTES = "weekly_intensity_minutes"
    LACTATE_THRESHOLD = "lactate_threshold"
    ENDURANCE_SCORE = "endurance_score"
    RUNNING_TOLERANCE = "running_tolerance"
    RACE_PREDICTIONS = "race_predictions"
    HILL_SCORE = "hill_score"
    PERSONAL_RECORDS = "personal_records"
    ATHLETE_STATS = "athlete_stats"
    ATHLETE_ZONES = "athlete_zones"


class TimeModel(str, Enum):
    DAILY = "daily"
    RANGE = "range"
    SNAPSHOT = "snapshot"


class AccessLevel(str, Enum):
    READ = "read"
    READ_WRITE = "read_write"


@dataclass(frozen=True)
class DataTypeSpec:
    time_model: TimeModel
    access: AccessLevel


class WellnessConnector(ABC):
    def __init__(self, tracker: TaskTracker) -> None:
        self._tracker = tracker

    @property
    @abstractmethod
    def connector_id(self) -> str: ...

    @abstractmethod
    async def login(self) -> None: ...

    def supported_types(self) -> dict[WellnessDataType, DataTypeSpec]:
        return {}

    async def fetch_daily(self, data_type: WellnessDataType, d: date) -> dict | None:
        self._log_unsupported("fetch_daily", data_type)
        return None

    async def fetch_range(
        self, data_type: WellnessDataType, start: date, end: date
    ) -> dict | None:
        self._log_unsupported("fetch_range", data_type)
        return None

    async def fetch_snapshot(self, data_type: WellnessDataType) -> dict | None:
        self._log_unsupported("fetch_snapshot", data_type)
        return None

    async def push_record(
        self, data_type: WellnessDataType, d: date | None, data: dict
    ) -> None:
        self._log_unsupported("push_record", data_type)

    def _log_unsupported(self, method: str, data_type: WellnessDataType) -> None:
        log = self._tracker.sync_logger
        if log:
            log.debug(
                f"[wellness] {self.connector_id}: {method}({data_type.value})"
                " not supported - skipped"
            )

    async def _find_earliest_supported_date(
        self,
        probe_fn: Callable[[date, date], Awaitable[dict | None]],
        lo: date,
        hi: date,
    ) -> date | None:
        """Binary search for the earliest date for which probe_fn(d, hi) succeeds.

        Returns None if even hi itself fails.
        """
        try:
            result = await probe_fn(hi, hi)
            if result is None:
                return None
        except Exception:
            return None

        best = hi
        while lo < hi:
            mid_ord = (lo.toordinal() + hi.toordinal()) // 2
            mid = date.fromordinal(mid_ord)
            try:
                result = await probe_fn(mid, hi)
                success = result is not None
            except Exception:
                success = False
            if success:
                best = mid
                hi = mid
            else:
                from datetime import timedelta

                lo = mid + timedelta(days=1)
        return best

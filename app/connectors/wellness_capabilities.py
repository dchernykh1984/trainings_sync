from __future__ import annotations

from app.connectors.wellness_base import (
    AccessLevel,
    DataTypeSpec,
    TimeModel,
    WellnessDataType,
)

GARMIN_CAPABILITIES: dict[WellnessDataType, DataTypeSpec] = {
    WellnessDataType.SLEEP: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.HRV: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.HEART_RATES: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.RESTING_HR: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.BODY_BATTERY_EVENTS: DataTypeSpec(
        TimeModel.DAILY, AccessLevel.READ
    ),
    WellnessDataType.STRESS_DAILY: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.ALL_DAY_STRESS: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.SPO2: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.RESPIRATION: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.STEPS_DAILY: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.FLOORS: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.INTENSITY_MINUTES: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.VO2MAX: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.TRAINING_READINESS: DataTypeSpec(
        TimeModel.DAILY, AccessLevel.READ
    ),
    WellnessDataType.MORNING_TRAINING_READINESS: DataTypeSpec(
        TimeModel.DAILY, AccessLevel.READ
    ),
    WellnessDataType.TRAINING_STATUS: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.FITNESS_AGE: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.USER_SUMMARY: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.STATS: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.LIFESTYLE_LOGGING: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ),
    WellnessDataType.DAILY_WEIGH_INS: DataTypeSpec(
        TimeModel.DAILY, AccessLevel.READ_WRITE
    ),
    WellnessDataType.HYDRATION: DataTypeSpec(TimeModel.DAILY, AccessLevel.READ_WRITE),
    WellnessDataType.BODY_BATTERY: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.DAILY_STEPS_RANGE: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.WEEKLY_STEPS: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.WEEKLY_STRESS: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.WEEKLY_INTENSITY_MINUTES: DataTypeSpec(
        TimeModel.RANGE, AccessLevel.READ
    ),
    WellnessDataType.LACTATE_THRESHOLD: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.ENDURANCE_SCORE: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.RUNNING_TOLERANCE: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.RACE_PREDICTIONS: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.HILL_SCORE: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ),
    WellnessDataType.BODY_COMPOSITION: DataTypeSpec(
        TimeModel.RANGE, AccessLevel.READ_WRITE
    ),
    WellnessDataType.WEIGH_INS: DataTypeSpec(TimeModel.RANGE, AccessLevel.READ_WRITE),
    WellnessDataType.BLOOD_PRESSURE: DataTypeSpec(
        TimeModel.RANGE, AccessLevel.READ_WRITE
    ),
    WellnessDataType.PERSONAL_RECORDS: DataTypeSpec(
        TimeModel.SNAPSHOT, AccessLevel.READ
    ),
}

STRAVA_CAPABILITIES: dict[WellnessDataType, DataTypeSpec] = {
    WellnessDataType.ATHLETE_STATS: DataTypeSpec(TimeModel.SNAPSHOT, AccessLevel.READ),
    WellnessDataType.ATHLETE_ZONES: DataTypeSpec(TimeModel.SNAPSHOT, AccessLevel.READ),
}

LOCAL_FOLDER_CAPABILITIES: dict[WellnessDataType, DataTypeSpec] = {
    dt: DataTypeSpec(spec.time_model, AccessLevel.READ_WRITE)
    for dt, spec in {**GARMIN_CAPABILITIES, **STRAVA_CAPABILITIES}.items()
}

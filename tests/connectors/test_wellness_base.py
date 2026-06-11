from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from app.connectors.wellness_base import (
    AccessLevel,
    DataTypeSpec,
    TimeModel,
    WellnessConnector,
    WellnessDataType,
)
from app.tracking.tracker import ProgressRenderer, Task, TaskTracker


class _FakeRenderer(ProgressRenderer):
    def on_task_added(self, task: Task) -> None:
        pass

    def on_progress(self, task: Task) -> None:
        pass

    def on_task_done(self, task: Task) -> None:
        pass

    def on_task_failed(self, task: Task) -> None:
        pass

    def on_task_warning(self, task: Task, message: str) -> None:
        pass

    def on_total_updated(self, task: Task) -> None:
        pass


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def tracker_with_log() -> TaskTracker:
    log = MagicMock()
    log.debug = MagicMock()
    return TaskTracker(_FakeRenderer(), sync_logger=log)


class _ConcreteConnector(WellnessConnector):
    @property
    def connector_id(self) -> str:
        return "test-connector"

    async def login(self) -> None:
        pass


class TestWellnessConnectorDefaults:
    async def test_fetch_daily_returns_none(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        result = await c.fetch_daily(WellnessDataType.SLEEP, date(2026, 1, 1))
        assert result is None

    async def test_fetch_range_returns_none(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        result = await c.fetch_range(
            WellnessDataType.BODY_BATTERY, date(2026, 1, 1), date(2026, 1, 31)
        )
        assert result is None

    async def test_fetch_snapshot_returns_none(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        result = await c.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result is None

    async def test_push_record_does_nothing(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        await c.push_record(WellnessDataType.SLEEP, date(2026, 1, 1), {})

    def test_supported_types_empty(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        assert c.supported_types() == {}

    async def test_log_unsupported_calls_debug(
        self, tracker_with_log: TaskTracker
    ) -> None:
        c = _ConcreteConnector(tracker_with_log)
        await c.fetch_daily(WellnessDataType.SLEEP, date(2026, 1, 1))
        assert tracker_with_log.sync_logger is not None
        tracker_with_log.sync_logger.debug.assert_called_once()  # type: ignore[attr-defined]
        call_arg = tracker_with_log.sync_logger.debug.call_args[0][0]  # type: ignore[attr-defined]
        assert "fetch_daily" in call_arg
        assert "sleep" in call_arg

    async def test_log_unsupported_no_log(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        # Should not raise even without a logger
        await c.fetch_daily(WellnessDataType.SLEEP, date(2026, 1, 1))


class TestFindEarliestSupportedDate:
    async def test_returns_none_when_hi_fails(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)

        async def probe(d: date, hi: date) -> dict | None:
            return None

        result = await c._find_earliest_supported_date(
            probe, date(2020, 1, 1), date(2026, 1, 1)
        )
        assert result is None

    async def test_returns_none_when_hi_raises(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)

        async def probe(d: date, hi: date) -> dict | None:
            raise RuntimeError("network error")

        result = await c._find_earliest_supported_date(
            probe, date(2020, 1, 1), date(2026, 1, 1)
        )
        assert result is None

    async def test_returns_hi_when_all_succeed(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)

        async def probe(d: date, hi: date) -> dict | None:
            return {"ok": True}

        result = await c._find_earliest_supported_date(
            probe, date(2026, 1, 1), date(2026, 1, 31)
        )
        assert result is not None
        assert result <= date(2026, 1, 1)

    async def test_bisects_to_earliest_success(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)
        cutoff = date(2026, 1, 15)

        async def probe(d: date, hi: date) -> dict | None:
            return {"ok": True} if d >= cutoff else None

        result = await c._find_earliest_supported_date(
            probe, date(2026, 1, 1), date(2026, 1, 31)
        )
        assert result is not None
        assert result <= cutoff

    async def test_returns_hi_when_lo_equals_hi(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)

        async def probe(d: date, hi: date) -> dict | None:
            return {}

        d = date(2026, 1, 15)
        result = await c._find_earliest_supported_date(probe, d, d)
        assert result == d

    async def test_empty_dict_counts_as_success(self, tracker: TaskTracker) -> None:
        c = _ConcreteConnector(tracker)

        async def probe(d: date, hi: date) -> dict | None:
            return {}

        result = await c._find_earliest_supported_date(
            probe, date(2026, 1, 1), date(2026, 1, 31)
        )
        assert result is not None

    async def test_probe_exception_treated_as_failure(
        self, tracker: TaskTracker
    ) -> None:
        c = _ConcreteConnector(tracker)
        cutoff = date(2026, 1, 20)

        async def probe(d: date, hi: date) -> dict | None:
            if d < cutoff:
                raise ValueError("too early")
            return {"ok": True}

        result = await c._find_earliest_supported_date(
            probe, date(2026, 1, 1), date(2026, 1, 31)
        )
        assert result is not None
        assert result <= cutoff


class TestDataTypeSpec:
    def test_frozen(self) -> None:
        import dataclasses

        spec = DataTypeSpec(TimeModel.DAILY, AccessLevel.READ)
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.time_model = TimeModel.RANGE  # type: ignore[misc]

    def test_equality(self) -> None:
        a = DataTypeSpec(TimeModel.DAILY, AccessLevel.READ)
        b = DataTypeSpec(TimeModel.DAILY, AccessLevel.READ)
        assert a == b

    def test_inequality(self) -> None:
        a = DataTypeSpec(TimeModel.DAILY, AccessLevel.READ)
        b = DataTypeSpec(TimeModel.RANGE, AccessLevel.READ)
        assert a != b


class TestEnums:
    def test_wellness_data_type_values(self) -> None:
        assert WellnessDataType.SLEEP.value == "sleep"
        assert WellnessDataType.ATHLETE_STATS.value == "athlete_stats"

    def test_time_model_values(self) -> None:
        assert TimeModel.DAILY.value == "daily"
        assert TimeModel.RANGE.value == "range"
        assert TimeModel.SNAPSHOT.value == "snapshot"

    def test_access_level_values(self) -> None:
        assert AccessLevel.READ.value == "read"
        assert AccessLevel.READ_WRITE.value == "read_write"

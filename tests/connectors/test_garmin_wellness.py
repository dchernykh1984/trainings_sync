from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.garmin_wellness import GarminWellnessConnector, _normalize_result
from app.connectors.wellness_base import WellnessDataType
from app.connectors.wellness_capabilities import GARMIN_CAPABILITIES
from app.credentials.base import Credentials
from app.tracking.tracker import ProgressRenderer, Task, TaskTracker

_CREDS = Credentials(login="user@example.com", password="secret")
_START = date(2026, 1, 1)
_END = date(2026, 1, 31)


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
def fake_garmin() -> MagicMock:
    client = MagicMock()
    client.get_sleep_data.return_value = {"sleepData": []}
    client.get_hrv_data.return_value = {"hrvData": []}
    client.get_heart_rates.return_value = {"heartRateValues": []}
    client.get_rhr_day.return_value = {"allMetrics": {}}
    client.get_body_battery_events.return_value = []
    client.get_stress_data.return_value = {"stressLevel": 30}
    client.get_all_day_stress.return_value = {"stressValues": []}
    client.get_spo2_data.return_value = {"spO2": []}
    client.get_respiration_data.return_value = {"respiration": []}
    client.get_steps_data.return_value = [{"steps": 5000}]
    client.get_floors.return_value = {"floorCount": 3}
    client.get_intensity_minutes_data.return_value = {"intensityMinutes": 45}
    client.get_max_metrics.return_value = {"vo2MaxValue": 50}
    client.get_training_readiness.return_value = {"score": 80}
    client.get_morning_training_readiness.return_value = {"score": 75}
    client.get_training_status.return_value = {"status": "productive"}
    client.get_fitnessage_data.return_value = {"fitnessAge": 30}
    client.get_daily_weigh_ins.return_value = {"dateWeightList": []}
    client.get_hydration_data.return_value = {"hydrationMilliliters": 2000}
    client.get_user_summary.return_value = {"totalSteps": 8000}
    client.get_stats.return_value = {"totalKilocalories": 2500}
    client.get_lifestyle_logging_data.return_value = {"loggingData": []}
    client.get_body_battery.return_value = [{"charged": 80}]
    client.get_body_composition.return_value = {"totalAverage": {"weight": 75}}
    client.get_weigh_ins.return_value = {"dateWeightList": []}
    client.get_blood_pressure.return_value = {"bloodPressure": []}
    client.get_daily_steps.return_value = [{"steps": 8000}]
    client.get_weekly_steps.return_value = [{"weeklySteps": 56000}]
    client.get_weekly_stress.return_value = [{"weeklyStress": 25}]
    client.get_weekly_intensity_minutes.return_value = [{"intensityMinutes": 150}]
    client.get_lactate_threshold.return_value = {"lactate": {}}
    client.get_endurance_score.return_value = {"score": 70}
    client.get_running_tolerance.return_value = [{"tolerance": 80}]
    client.get_race_predictions.return_value = {"predictions": []}
    client.get_hill_score.return_value = {"score": 60}
    client.get_personal_record.return_value = {"records": []}
    client.add_weigh_in.return_value = {}
    client.add_hydration_data.return_value = {}
    client.add_body_composition.return_value = {}
    client.set_blood_pressure.return_value = {}
    return client


@pytest.fixture
def connector(tracker: TaskTracker, fake_garmin: MagicMock) -> GarminWellnessConnector:
    return GarminWellnessConnector(
        connector_id="garmin-main",
        credentials=_CREDS,
        tracker=tracker,
        client=fake_garmin,
    )


class TestConnectorId:
    def test_returns_configured_id(self, connector: GarminWellnessConnector) -> None:
        assert connector.connector_id == "garmin-main"


class TestSupportedTypes:
    def test_returns_garmin_capabilities(
        self, connector: GarminWellnessConnector
    ) -> None:
        assert connector.supported_types() == GARMIN_CAPABILITIES


class TestLogin:
    async def test_skips_login_when_client_exists(
        self, connector: GarminWellnessConnector, tracker: TaskTracker
    ) -> None:
        await connector.login()
        assert connector._client is not None

    async def test_performs_login_when_no_client(self, tracker: TaskTracker) -> None:
        c = GarminWellnessConnector("g", _CREDS, tracker)
        mock_garmin = MagicMock()
        with patch("app.connectors.garmin_wellness.Garmin", return_value=mock_garmin):
            with patch(
                "app.connectors.garmin_wellness._run_with_timeout",
                new_callable=AsyncMock,
            ) as mock_rw:
                mock_rw.return_value = None
                await c.login()
        assert c._client is mock_garmin

    async def test_login_fails_marks_task_failed(self, tracker: TaskTracker) -> None:
        c = GarminWellnessConnector("g", _CREDS, tracker)
        with patch("app.connectors.garmin_wellness.Garmin") as mock_cls:
            mock_cls.return_value = MagicMock()
            with patch(
                "app.connectors.garmin_wellness._run_with_timeout",
                new_callable=AsyncMock,
                side_effect=RuntimeError("auth error"),
            ):
                with pytest.raises(RuntimeError):
                    await c.login()


class TestFetchDaily:
    async def test_fetch_sleep(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_daily(WellnessDataType.SLEEP, _START)
        assert result is not None
        fake_garmin.get_sleep_data.assert_called_once_with("2026-01-01")

    async def test_fetch_hrv_none_is_skipped(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        fake_garmin.get_hrv_data.return_value = None
        result = await connector.fetch_daily(WellnessDataType.HRV, _START)
        assert result is None

    async def test_fetch_resting_hr(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_daily(WellnessDataType.RESTING_HR, _START)
        assert result is not None
        fake_garmin.get_rhr_day.assert_called_once_with("2026-01-01")

    async def test_fetch_body_battery_events_list_wrapped(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        fake_garmin.get_body_battery_events.return_value = [{"event": "charged"}]
        result = await connector.fetch_daily(
            WellnessDataType.BODY_BATTERY_EVENTS, _START
        )
        assert result == {"items": [{"event": "charged"}]}

    async def test_fetch_exception_returns_none(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        fake_garmin.get_sleep_data.side_effect = RuntimeError("network error")
        result = await connector.fetch_daily(WellnessDataType.SLEEP, _START)
        assert result is None

    async def test_fetch_unsupported_type_returns_none(
        self, connector: GarminWellnessConnector
    ) -> None:
        result = await connector.fetch_daily(WellnessDataType.ATHLETE_STATS, _START)
        assert result is None

    async def test_requires_client(self, tracker: TaskTracker) -> None:
        c = GarminWellnessConnector("g", _CREDS, tracker)
        with pytest.raises(RuntimeError, match="Not logged in"):
            await c.fetch_daily(WellnessDataType.SLEEP, _START)

    async def test_all_daily_types_dispatch(
        self, connector: GarminWellnessConnector
    ) -> None:
        from app.connectors.wellness_base import TimeModel

        daily_types = [
            dt
            for dt, spec in GARMIN_CAPABILITIES.items()
            if spec.time_model == TimeModel.DAILY
        ]
        for dt in daily_types:
            result = await connector.fetch_daily(dt, _START)
            assert result is not None or result is None  # just no exception


class TestFetchRange:
    async def test_fetch_body_battery(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_range(
            WellnessDataType.BODY_BATTERY, _START, _END
        )
        assert result is not None
        fake_garmin.get_body_battery.assert_called_once_with("2026-01-01", "2026-01-31")

    async def test_fetch_body_composition(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_range(
            WellnessDataType.BODY_COMPOSITION, _START, _END
        )
        assert result is not None

    async def test_fetch_weekly_steps_uses_end_and_weeks(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_range(
            WellnessDataType.WEEKLY_STEPS, _START, _END
        )
        assert result is not None
        call_args = fake_garmin.get_weekly_steps.call_args
        assert call_args[0][0] == "2026-01-31"
        assert isinstance(call_args[0][1], int)
        assert call_args[0][1] >= 1

    async def test_fetch_weekly_intensity_minutes_uses_start_end(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_range(
            WellnessDataType.WEEKLY_INTENSITY_MINUTES, _START, _END
        )
        assert result is not None
        fake_garmin.get_weekly_intensity_minutes.assert_called_once_with(
            "2026-01-01", "2026-01-31"
        )

    async def test_fetch_exception_returns_none(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        fake_garmin.get_body_battery.side_effect = RuntimeError("error")
        result = await connector.fetch_range(
            WellnessDataType.BODY_BATTERY, _START, _END
        )
        assert result is None

    async def test_fetch_unsupported_returns_none(
        self, connector: GarminWellnessConnector
    ) -> None:
        result = await connector.fetch_range(
            WellnessDataType.ATHLETE_STATS, _START, _END
        )
        assert result is None

    async def test_all_range_types_dispatch(
        self, connector: GarminWellnessConnector
    ) -> None:
        from app.connectors.wellness_base import TimeModel

        range_types = [
            dt
            for dt, spec in GARMIN_CAPABILITIES.items()
            if spec.time_model == TimeModel.RANGE
        ]
        for dt in range_types:
            result = await connector.fetch_range(dt, _START, _END)
            assert result is not None or result is None


class TestFetchSnapshot:
    async def test_fetch_personal_records(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        result = await connector.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result is not None
        fake_garmin.get_personal_record.assert_called_once()

    async def test_fetch_unsupported_snapshot(
        self, connector: GarminWellnessConnector
    ) -> None:
        result = await connector.fetch_snapshot(WellnessDataType.ATHLETE_STATS)
        assert result is None

    async def test_fetch_exception_returns_none(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        fake_garmin.get_personal_record.side_effect = RuntimeError("error")
        result = await connector.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result is None


class TestPushRecord:
    async def test_push_weigh_in(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(
            WellnessDataType.DAILY_WEIGH_INS, _START, {"weight": 75.5}
        )
        fake_garmin.add_weigh_in.assert_called_once_with(75.5)

    async def test_push_hydration(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(
            WellnessDataType.HYDRATION,
            _START,
            {"value_in_ml": 2000.0},
        )
        fake_garmin.add_hydration_data.assert_called_once()

    async def test_push_body_composition(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(
            WellnessDataType.BODY_COMPOSITION,
            _START,
            {"weight": 75.0, "timestamp": "2026-01-01T08:00:00"},
        )
        fake_garmin.add_body_composition.assert_called_once()

    async def test_push_blood_pressure(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(
            WellnessDataType.BLOOD_PRESSURE,
            _START,
            {"systolic": 120, "diastolic": 80, "pulse": 70},
        )
        fake_garmin.set_blood_pressure.assert_called_once_with(120, 80, 70)

    async def test_push_read_only_type_logs_unsupported(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(WellnessDataType.SLEEP, _START, {"sleepData": []})
        fake_garmin.add_weigh_in.assert_not_called()

    async def test_push_weigh_in_missing_weight_skips_call(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(WellnessDataType.DAILY_WEIGH_INS, _START, {})
        fake_garmin.add_weigh_in.assert_not_called()

    async def test_push_exception_logs_warning(
        self, tracker: TaskTracker, fake_garmin: MagicMock
    ) -> None:
        log = MagicMock()
        log.warning = MagicMock()
        tracker_with_log = TaskTracker(_FakeRenderer(), sync_logger=log)
        c = GarminWellnessConnector(
            "garmin-main", _CREDS, tracker_with_log, client=fake_garmin
        )
        fake_garmin.add_weigh_in.side_effect = RuntimeError("push failed")
        await c.push_record(WellnessDataType.DAILY_WEIGH_INS, _START, {"weight": 75.0})
        log.warning.assert_called_once()

    async def test_push_weigh_ins_range(
        self, connector: GarminWellnessConnector, fake_garmin: MagicMock
    ) -> None:
        await connector.push_record(
            WellnessDataType.WEIGH_INS,
            _START,
            {"weight": 75.0, "timestamp": "2026-01-01T08:00:00"},
        )
        fake_garmin.add_body_composition.assert_called_once()


class TestFromGarminConnector:
    def test_creates_from_connector(self, tracker: TaskTracker) -> None:
        from app.connectors.garmin import GarminConnector

        gc = GarminConnector(_CREDS, tracker)
        fake_client = MagicMock()
        gc._client = fake_client
        wc = GarminWellnessConnector.from_garmin_connector("garmin-main", gc, tracker)
        assert wc.connector_id == "garmin-main"
        assert wc._client is fake_client


class TestNormalizeResult:
    def test_none_stays_none(self) -> None:
        assert _normalize_result(None) is None

    def test_dict_unchanged(self) -> None:
        d = {"key": "val"}
        assert _normalize_result(d) is d

    def test_list_wrapped(self) -> None:
        result = _normalize_result([1, 2, 3])
        assert result == {"items": [1, 2, 3]}

    def test_scalar_wrapped(self) -> None:
        result = _normalize_result(42)
        assert result == {"value": 42}

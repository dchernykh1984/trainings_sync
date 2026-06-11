from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.connectors.strava_wellness import StravaWellnessConnector
from app.connectors.wellness_base import WellnessDataType
from app.connectors.wellness_capabilities import STRAVA_CAPABILITIES
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
def fake_strava_client() -> MagicMock:
    client = MagicMock()
    athlete = MagicMock()
    athlete.id = 12345

    stats = MagicMock()
    stats.model_dump_json.return_value = '{"all_run_totals": {"count": 10}}'

    zones = MagicMock()
    zones.model_dump_json.return_value = '{"heart_rate": {"custom_zones": false}}'

    client.get_athlete.return_value = athlete
    client.get_athlete_stats.return_value = stats
    client.get_athlete_zones.return_value = zones
    return client


@pytest.fixture
def connector(
    tracker: TaskTracker, fake_strava_client: MagicMock
) -> StravaWellnessConnector:
    return StravaWellnessConnector("strava-main", fake_strava_client, tracker)


class TestConnectorId:
    def test_returns_configured_id(self, connector: StravaWellnessConnector) -> None:
        assert connector.connector_id == "strava-main"


class TestSupportedTypes:
    def test_returns_strava_capabilities(
        self, connector: StravaWellnessConnector
    ) -> None:
        assert connector.supported_types() == STRAVA_CAPABILITIES


class TestLogin:
    async def test_login_creates_and_finishes_task(
        self, connector: StravaWellnessConnector, tracker: TaskTracker
    ) -> None:
        await connector.login()
        tasks = tracker.tasks
        assert any("Strava wellness" in name for name in tasks)
        task = next(t for n, t in tasks.items() if "Strava wellness" in n)
        from app.tracking.tracker import TaskStatus

        assert task.status == TaskStatus.DONE


class TestFetchSnapshot:
    async def test_fetch_athlete_stats(
        self, connector: StravaWellnessConnector, fake_strava_client: MagicMock
    ) -> None:
        result = await connector.fetch_snapshot(WellnessDataType.ATHLETE_STATS)
        assert result is not None
        assert "all_run_totals" in result

    async def test_fetch_athlete_zones(
        self, connector: StravaWellnessConnector, fake_strava_client: MagicMock
    ) -> None:
        result = await connector.fetch_snapshot(WellnessDataType.ATHLETE_ZONES)
        assert result is not None
        assert "heart_rate" in result

    async def test_fetch_unsupported_returns_none(
        self, connector: StravaWellnessConnector
    ) -> None:
        result = await connector.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result is None

    async def test_fetch_stats_exception_returns_none(
        self, connector: StravaWellnessConnector, fake_strava_client: MagicMock
    ) -> None:
        fake_strava_client.get_athlete.side_effect = RuntimeError("network error")
        result = await connector.fetch_snapshot(WellnessDataType.ATHLETE_STATS)
        assert result is None

    async def test_fetch_zones_exception_returns_none(
        self, connector: StravaWellnessConnector, fake_strava_client: MagicMock
    ) -> None:
        fake_strava_client.get_athlete_zones.side_effect = RuntimeError("network error")
        result = await connector.fetch_snapshot(WellnessDataType.ATHLETE_ZONES)
        assert result is None

    async def test_fetch_stats_logs_on_error(self, tracker: TaskTracker) -> None:
        log = MagicMock()
        log.debug = MagicMock()
        tracker_with_log = TaskTracker(_FakeRenderer(), sync_logger=log)
        client = MagicMock()
        client.get_athlete.side_effect = RuntimeError("fail")
        c = StravaWellnessConnector("strava-main", client, tracker_with_log)
        result = await c.fetch_snapshot(WellnessDataType.ATHLETE_STATS)
        assert result is None
        log.debug.assert_called_once()

    async def test_athlete_id_used_for_stats(
        self, connector: StravaWellnessConnector, fake_strava_client: MagicMock
    ) -> None:
        await connector.fetch_snapshot(WellnessDataType.ATHLETE_STATS)
        fake_strava_client.get_athlete_stats.assert_called_once_with(12345)

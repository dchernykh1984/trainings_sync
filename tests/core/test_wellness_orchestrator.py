from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.connectors.local_folder_wellness import LocalFolderWellnessConnector
from app.connectors.wellness_base import (
    AccessLevel,
    DataTypeSpec,
    TimeModel,
    WellnessConnector,
    WellnessDataType,
)
from app.core.wellness_cache import WellnessCache
from app.core.wellness_orchestrator import WellnessOrchestrator
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker

_START = date(2026, 1, 1)
_END = date(2026, 1, 3)


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
def cache(tmp_path: Path) -> WellnessCache:
    return WellnessCache(tmp_path / "cache")


def _make_source_connector(
    connector_id: str,
    tracker: TaskTracker,
    *,
    daily_types: list[WellnessDataType] | None = None,
    range_types: list[WellnessDataType] | None = None,
    snapshot_types: list[WellnessDataType] | None = None,
    daily_data: dict | None = None,
    range_data: dict | None = None,
    snapshot_data: dict | None = None,
) -> WellnessConnector:
    supported: dict[WellnessDataType, DataTypeSpec] = {}
    for dt in daily_types or []:
        supported[dt] = DataTypeSpec(TimeModel.DAILY, AccessLevel.READ)
    for dt in range_types or []:
        supported[dt] = DataTypeSpec(TimeModel.RANGE, AccessLevel.READ)
    for dt in snapshot_types or []:
        supported[dt] = DataTypeSpec(TimeModel.SNAPSHOT, AccessLevel.READ)

    conn = MagicMock(spec=WellnessConnector)
    conn.connector_id = connector_id
    conn.supported_types.return_value = supported
    conn.fetch_daily = AsyncMock(return_value=daily_data or {"data": "daily"})
    conn.fetch_range = AsyncMock(return_value=range_data or {"data": "range"})
    conn.fetch_snapshot = AsyncMock(return_value=snapshot_data or {"data": "snapshot"})
    conn.push_record = AsyncMock()
    return conn


def _make_local_folder_connector(
    connector_id: str,
    folder: Path,
    tracker: TaskTracker,
) -> LocalFolderWellnessConnector:
    c = LocalFolderWellnessConnector(connector_id, folder, tracker)
    return c


class TestRunDownload:
    async def test_fetches_daily_for_each_date(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        assert source.fetch_daily.call_count == 3  # type: ignore[union-attr, attr-defined]

    async def test_fetches_range_once(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, range_types=[WellnessDataType.BODY_BATTERY]
        )
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        source.fetch_range.assert_called_once()  # type: ignore[union-attr, attr-defined]

    async def test_fetches_snapshot_once(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, snapshot_types=[WellnessDataType.PERSONAL_RECORDS]
        )
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        source.fetch_snapshot.assert_called_once()  # type: ignore[union-attr, attr-defined]

    async def test_caches_fetched_daily_data(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        key = WellnessCache.daily_key(_START)
        assert cache.has("garmin", WellnessDataType.SLEEP, key)
        assert cache.read("garmin", WellnessDataType.SLEEP, key) == {"data": "daily"}

    async def test_caches_fetched_range_data(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, range_types=[WellnessDataType.BODY_BATTERY]
        )
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        key = WellnessCache.range_key(_START, _END)
        assert cache.has("garmin", WellnessDataType.BODY_BATTERY, key)

    async def test_skips_cached_daily_data(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        key = WellnessCache.daily_key(_START)
        cache.write("garmin", WellnessDataType.SLEEP, key, {"cached": True})

        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        assert source.fetch_daily.call_count == 2  # type: ignore[union-attr, attr-defined]

    async def test_skips_cached_range_data(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, range_types=[WellnessDataType.BODY_BATTERY]
        )
        key = WellnessCache.range_key(_START, _END)
        cache.write("garmin", WellnessDataType.BODY_BATTERY, key, {"cached": True})

        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        source.fetch_range.assert_not_called()  # type: ignore[union-attr, attr-defined]

    async def test_skips_cached_snapshot_data(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, snapshot_types=[WellnessDataType.PERSONAL_RECORDS]
        )
        cache.write(
            "garmin",
            WellnessDataType.PERSONAL_RECORDS,
            WellnessCache.SNAPSHOT_KEY,
            {"cached": True},
        )

        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        source.fetch_snapshot.assert_not_called()  # type: ignore[union-attr, attr-defined]

    async def test_does_not_cache_none_result(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin",
            tracker,
            daily_types=[WellnessDataType.SLEEP],
            daily_data=None,
        )
        source.fetch_daily = AsyncMock(return_value=None)  # type: ignore[union-attr, method-assign]
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        key = WellnessCache.daily_key(_START)
        assert not cache.has("garmin", WellnessDataType.SLEEP, key)


class TestRunForce:
    async def test_force_invalidates_cache(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        key = WellnessCache.daily_key(_START)
        cache.write("garmin", WellnessDataType.SLEEP, key, {"old": True})

        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END, force=True)

        assert source.fetch_daily.call_count == 3  # type: ignore[union-attr, attr-defined]
        result = cache.read("garmin", WellnessDataType.SLEEP, key)
        assert result == {"data": "daily"}

    async def test_no_force_uses_cache(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        key = WellnessCache.daily_key(_START)
        cache.write("garmin", WellnessDataType.SLEEP, key, {"old": True})

        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END, force=False)

        assert source.fetch_daily.call_count == 2  # type: ignore[union-attr, attr-defined]


class TestRunUpload:
    async def test_pushes_cached_data_to_local_folder(
        self, cache: WellnessCache, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        dest_folder = tmp_path / "dest"
        dest_folder.mkdir()
        dest = _make_local_folder_connector("local", dest_folder, tracker)

        orch = WellnessOrchestrator({"garmin": source, "local": dest}, cache, tracker)
        await orch.run(_START, _END)

        for d_offset in range(3):
            from datetime import timedelta

            d = _START + timedelta(days=d_offset)
            result = await dest.fetch_daily(WellnessDataType.SLEEP, d)
            assert result == {"data": "daily"}

    async def test_no_upload_task_when_nothing_to_push(
        self, cache: WellnessCache, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        source = _make_source_connector(
            "garmin",
            tracker,
            daily_types=[WellnessDataType.SLEEP],
            daily_data=None,
        )
        source.fetch_daily = AsyncMock(return_value=None)  # type: ignore[union-attr, method-assign]
        dest_folder = tmp_path / "dest"
        dest_folder.mkdir()
        dest = _make_local_folder_connector("local", dest_folder, tracker)

        orch = WellnessOrchestrator({"garmin": source, "local": dest}, cache, tracker)
        await orch.run(_START, _END)

        tasks = tracker.tasks
        upload_tasks = [n for n in tasks if "Wellness upload" in n]
        assert len(upload_tasks) == 0

    async def test_pushes_snapshot_to_local_folder(
        self, cache: WellnessCache, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        source = _make_source_connector(
            "garmin",
            tracker,
            snapshot_types=[WellnessDataType.PERSONAL_RECORDS],
            snapshot_data={"records": [{"type": "run"}]},
        )
        dest_folder = tmp_path / "dest"
        dest_folder.mkdir()
        dest = _make_local_folder_connector("local", dest_folder, tracker)

        orch = WellnessOrchestrator({"garmin": source, "local": dest}, cache, tracker)
        await orch.run(_START, _END)

        result = await dest.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result == {"records": [{"type": "run"}]}


class TestProgressTracking:
    async def test_download_task_created_and_finished(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        tasks = tracker.tasks
        download_tasks = [
            t for n, t in tasks.items() if "Wellness download (garmin)" in n
        ]
        assert len(download_tasks) == 1
        assert download_tasks[0].status == TaskStatus.DONE

    async def test_upload_task_created_and_finished(
        self, cache: WellnessCache, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        source = _make_source_connector(
            "garmin", tracker, daily_types=[WellnessDataType.SLEEP]
        )
        dest_folder = tmp_path / "dest"
        dest_folder.mkdir()
        dest = _make_local_folder_connector("local", dest_folder, tracker)

        orch = WellnessOrchestrator({"garmin": source, "local": dest}, cache, tracker)
        await orch.run(_START, _END)

        tasks = tracker.tasks
        upload_tasks = [t for n, t in tasks.items() if "Wellness upload (local)" in n]
        assert len(upload_tasks) == 1
        assert upload_tasks[0].status == TaskStatus.DONE

    async def test_no_download_task_when_no_supported_types(
        self, cache: WellnessCache, tracker: TaskTracker
    ) -> None:
        source = _make_source_connector("garmin", tracker)
        orch = WellnessOrchestrator({"garmin": source}, cache, tracker)
        await orch.run(_START, _END)

        tasks = tracker.tasks
        download_tasks = [n for n in tasks if "Wellness download" in n]
        assert len(download_tasks) == 0


class TestLocalFolderNotTreatedAsSource:
    async def test_local_folder_not_fetched_from(
        self, cache: WellnessCache, tracker: TaskTracker, tmp_path: Path
    ) -> None:
        folder = tmp_path / "local"
        folder.mkdir()
        dest = _make_local_folder_connector("local", folder, tracker)

        orch = WellnessOrchestrator({"local": dest}, cache, tracker)
        await orch.run(_START, _END)

        tasks = tracker.tasks
        download_tasks = [n for n in tasks if "Wellness download" in n]
        assert len(download_tasks) == 0

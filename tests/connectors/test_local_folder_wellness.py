from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.connectors.local_folder_wellness import LocalFolderWellnessConnector
from app.connectors.wellness_base import WellnessDataType
from app.connectors.wellness_capabilities import LOCAL_FOLDER_CAPABILITIES
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker


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
def folder(tmp_path: Path) -> Path:
    p = tmp_path / "local"
    p.mkdir()
    return p


@pytest.fixture
def connector(folder: Path, tracker: TaskTracker) -> LocalFolderWellnessConnector:
    return LocalFolderWellnessConnector("local-main", folder, tracker)


class TestConnectorId:
    def test_returns_configured_id(
        self, connector: LocalFolderWellnessConnector
    ) -> None:
        assert connector.connector_id == "local-main"


class TestSupportedTypes:
    def test_returns_local_folder_capabilities(
        self, connector: LocalFolderWellnessConnector
    ) -> None:
        assert connector.supported_types() == LOCAL_FOLDER_CAPABILITIES


class TestLogin:
    async def test_login_succeeds_for_existing_folder(
        self, connector: LocalFolderWellnessConnector, tracker: TaskTracker
    ) -> None:
        await connector.login()
        tasks = tracker.tasks
        task = next(t for n, t in tasks.items() if "Local folder wellness" in n)
        assert task.status == TaskStatus.DONE

    async def test_login_fails_for_missing_folder(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        c = LocalFolderWellnessConnector(
            "local-main", tmp_path / "nonexistent", tracker
        )
        with pytest.raises(FileNotFoundError):
            await c.login()

    async def test_login_marks_task_failed_on_error(
        self, tmp_path: Path, tracker: TaskTracker
    ) -> None:
        c = LocalFolderWellnessConnector(
            "local-main", tmp_path / "nonexistent", tracker
        )
        with pytest.raises(FileNotFoundError):
            await c.login()
        tasks = tracker.tasks
        task = next(t for n, t in tasks.items() if "Local folder wellness" in n)
        assert task.status == TaskStatus.FAILED


class TestPushRecord:
    async def test_push_daily_creates_file(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        d = date(2026, 1, 15)
        data = {"steps": 8000}
        await connector.push_record(WellnessDataType.STEPS_DAILY, d, data)
        p = folder / "wellness" / "steps_daily" / "2026-01-15.json"
        assert p.is_file()
        assert json.loads(p.read_text()) == data

    async def test_push_snapshot_creates_timestamped_file(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        data: dict[str, list[object]] = {"records": []}
        await connector.push_record(WellnessDataType.PERSONAL_RECORDS, None, data)
        type_dir = folder / "wellness" / "personal_records"
        files = list(type_dir.glob("*.json"))
        assert len(files) == 1
        assert json.loads(files[0].read_text()) == data

    async def test_push_snapshot_preserves_history(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        await connector.push_record(WellnessDataType.PERSONAL_RECORDS, None, {"v": 1})
        await connector.push_record(WellnessDataType.PERSONAL_RECORDS, None, {"v": 2})
        type_dir = folder / "wellness" / "personal_records"
        files = list(type_dir.glob("*.json"))
        assert len(files) == 2

    async def test_push_overwrites_existing(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        d = date(2026, 1, 1)
        await connector.push_record(WellnessDataType.SLEEP, d, {"v": 1})
        await connector.push_record(WellnessDataType.SLEEP, d, {"v": 2})
        p = folder / "wellness" / "sleep" / "2026-01-01.json"
        assert json.loads(p.read_text())["v"] == 2

    async def test_push_creates_parent_dirs(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        await connector.push_record(WellnessDataType.HRV, date(2026, 1, 1), {"hrv": 50})
        assert (folder / "wellness" / "hrv").is_dir()

    async def test_push_atomic_write(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        d = date(2026, 1, 1)
        await connector.push_record(WellnessDataType.SLEEP, d, {"data": "test"})
        p = folder / "wellness" / "sleep" / "2026-01-01.json"
        tmp = p.with_suffix(".tmp")
        assert not tmp.exists()
        assert p.is_file()


class TestFetchDaily:
    async def test_returns_none_when_file_missing(
        self, connector: LocalFolderWellnessConnector
    ) -> None:
        result = await connector.fetch_daily(WellnessDataType.SLEEP, date(2026, 1, 1))
        assert result is None

    async def test_returns_data_when_file_exists(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        d = date(2026, 1, 15)
        data: dict[str, list[object]] = {"sleepData": []}
        await connector.push_record(WellnessDataType.SLEEP, d, data)
        result = await connector.fetch_daily(WellnessDataType.SLEEP, d)
        assert result == data


class TestFetchRange:
    async def test_returns_none_when_file_missing(
        self, connector: LocalFolderWellnessConnector
    ) -> None:
        result = await connector.fetch_range(
            WellnessDataType.BODY_BATTERY, date(2026, 1, 1), date(2026, 1, 31)
        )
        assert result is None

    async def test_returns_data_when_file_exists(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        start = date(2026, 1, 1)
        end = date(2026, 1, 31)
        data: dict[str, list[object]] = {"battery": []}
        key = f"{start}_{end}"
        p = folder / "wellness" / "body_battery" / f"{key}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data), encoding="utf-8")
        result = await connector.fetch_range(WellnessDataType.BODY_BATTERY, start, end)
        assert result == data


class TestFetchSnapshot:
    async def test_returns_none_when_file_missing(
        self, connector: LocalFolderWellnessConnector
    ) -> None:
        result = await connector.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result is None

    async def test_returns_data_when_file_exists(
        self, connector: LocalFolderWellnessConnector, folder: Path
    ) -> None:
        data = {"records": [{"type": "run"}]}
        await connector.push_record(WellnessDataType.PERSONAL_RECORDS, None, data)
        result = await connector.fetch_snapshot(WellnessDataType.PERSONAL_RECORDS)
        assert result == data

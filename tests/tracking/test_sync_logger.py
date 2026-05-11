from __future__ import annotations

from collections.abc import Generator
from datetime import date
from pathlib import Path

import pytest

from app.tracking.sync_logger import SyncLogger
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
def log_path(tmp_path: Path) -> Path:
    return tmp_path / "sync.log"


@pytest.fixture
def logger(log_path: Path) -> Generator[SyncLogger, None, None]:
    sl = SyncLogger(log_path)
    yield sl
    sl.close()


class TestSyncLoggerInit:
    def test_creates_log_file_on_first_write(
        self, logger: SyncLogger, log_path: Path
    ) -> None:
        logger.info("hello")
        assert log_path.exists()

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "a" / "b" / "sync.log"
        sl = SyncLogger(path)
        sl.info("x")
        sl.close()
        assert path.exists()

    def test_path_property(self, logger: SyncLogger, log_path: Path) -> None:
        assert logger.path == log_path


class TestSyncLoggerWrite:
    def test_info_written_to_file(self, logger: SyncLogger, log_path: Path) -> None:
        logger.info("test info message")
        content = log_path.read_text(encoding="utf-8")
        assert "INFO" in content
        assert "test info message" in content

    def test_debug_written_to_file(self, logger: SyncLogger, log_path: Path) -> None:
        logger.debug("debug detail")
        content = log_path.read_text(encoding="utf-8")
        assert "DEBUG" in content
        assert "debug detail" in content

    def test_warning_written_to_file(self, logger: SyncLogger, log_path: Path) -> None:
        logger.warning("watch out")
        content = log_path.read_text(encoding="utf-8")
        assert "WARNING" in content
        assert "watch out" in content

    def test_error_written_to_file(self, logger: SyncLogger, log_path: Path) -> None:
        logger.error("something broke")
        content = log_path.read_text(encoding="utf-8")
        assert "ERROR" in content
        assert "something broke" in content

    def test_error_with_exc_info_writes_traceback(
        self, logger: SyncLogger, log_path: Path
    ) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            logger.error("caught", exc_info=True)
        content = log_path.read_text(encoding="utf-8")
        assert "Traceback" in content
        assert "ValueError: boom" in content

    def test_appends_across_multiple_calls(
        self, logger: SyncLogger, log_path: Path
    ) -> None:
        logger.info("first")
        logger.info("second")
        content = log_path.read_text(encoding="utf-8")
        assert "first" in content
        assert "second" in content

    def test_appends_across_instances(self, tmp_path: Path) -> None:
        path = tmp_path / "sync.log"
        sl1 = SyncLogger(path)
        sl1.info("from run 1")
        sl1.close()
        sl2 = SyncLogger(path)
        sl2.info("from run 2")
        sl2.close()
        content = path.read_text(encoding="utf-8")
        assert "from run 1" in content
        assert "from run 2" in content


class TestRunStartEnd:
    def test_run_start_writes_separator_and_header(
        self, logger: SyncLogger, log_path: Path
    ) -> None:
        logger.run_start(date(2024, 1, 1), date(2024, 12, 31), force=False)
        content = log_path.read_text(encoding="utf-8")
        assert "===" in content
        assert "2024-01-01" in content
        assert "2024-12-31" in content
        assert "force=False" in content

    def test_run_end_writes_finished_line(
        self, logger: SyncLogger, log_path: Path
    ) -> None:
        logger.run_start(date(2024, 1, 1), date(2024, 12, 31), force=False)
        logger.run_end()
        content = log_path.read_text(encoding="utf-8")
        assert "finished" in content


class TestTaskTrackerIntegration:
    @pytest.fixture
    def tracker(self, logger: SyncLogger) -> TaskTracker:
        return TaskTracker(_FakeRenderer(), sync_logger=logger)

    async def test_task_start_logged(
        self, tracker: TaskTracker, log_path: Path
    ) -> None:
        await tracker.add_task("My task", total=10)
        content = log_path.read_text(encoding="utf-8")
        assert "My task" in content
        assert "started" in content

    async def test_task_done_logged(self, tracker: TaskTracker, log_path: Path) -> None:
        await tracker.add_task("My task", total=1)
        await tracker.advance("My task")
        await tracker.finish("My task")
        content = log_path.read_text(encoding="utf-8")
        assert "My task" in content
        assert "done" in content

    async def test_task_fail_logged_as_error(
        self, tracker: TaskTracker, log_path: Path
    ) -> None:
        await tracker.add_task("Bad task", total=1)
        await tracker.fail("Bad task", error="network error")
        content = log_path.read_text(encoding="utf-8")
        assert "ERROR" in content
        assert "Bad task" in content
        assert "network error" in content

    async def test_task_warning_logged(
        self, tracker: TaskTracker, log_path: Path
    ) -> None:
        await tracker.add_task("My task", total=1)
        await tracker.warn("My task", "skipped bad.fit")
        content = log_path.read_text(encoding="utf-8")
        assert "WARNING" in content
        assert "skipped bad.fit" in content

    async def test_no_log_when_sync_logger_is_none(self, tmp_path: Path) -> None:
        tracker = TaskTracker(_FakeRenderer(), sync_logger=None)
        await tracker.add_task("task", total=1)
        await tracker.finish("task")
        assert not (tmp_path / "sync.log").exists()

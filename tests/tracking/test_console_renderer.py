from unittest.mock import MagicMock, patch

import pytest

from app.tracking.console_renderer import ConsoleRenderer
from app.tracking.tracker import Task, TaskStatus


@pytest.fixture
def mock_progress() -> MagicMock:
    return MagicMock()


@pytest.fixture
def renderer(mock_progress: MagicMock) -> ConsoleRenderer:
    with patch("app.tracking.console_renderer.Progress", return_value=mock_progress):
        return ConsoleRenderer()


class TestConsoleRendererContextManager:
    def test_stop_delegates_to_progress(
        self, renderer: ConsoleRenderer, mock_progress: MagicMock
    ) -> None:
        renderer.stop()
        mock_progress.stop.assert_called_once()

    def test_exit_calls_stop(self, renderer: ConsoleRenderer) -> None:
        with patch.object(renderer, "stop") as mock_stop:
            with renderer:
                pass
        mock_stop.assert_called_once()


class TestOnTaskAdded:
    def test_registers_task_with_progress(
        self, renderer: ConsoleRenderer, mock_progress: MagicMock
    ) -> None:
        task = Task(name="sync", total=10)
        renderer.on_task_added(task)

        mock_progress.add_task.assert_called_once_with("sync", total=10)

    def test_stores_task_id(
        self, renderer: ConsoleRenderer, mock_progress: MagicMock
    ) -> None:
        mock_progress.add_task.return_value = 42
        task = Task(name="sync", total=10)
        renderer.on_task_added(task)

        assert renderer._task_ids["sync"] == 42


class TestOnProgress:
    def test_updates_completed(
        self, renderer: ConsoleRenderer, mock_progress: MagicMock
    ) -> None:
        mock_progress.add_task.return_value = 7
        renderer.on_task_added(Task(name="sync", total=10))

        task = Task(name="sync", total=10, status=TaskStatus.RUNNING, progress=3)
        renderer.on_progress(task)

        mock_progress.update.assert_called_with(7, completed=3)


class TestOnTaskDone:
    def test_updates_to_total_and_stops(
        self, renderer: ConsoleRenderer, mock_progress: MagicMock
    ) -> None:
        mock_progress.add_task.return_value = 5
        renderer.on_task_added(Task(name="sync", total=10))

        task = Task(name="sync", total=10, status=TaskStatus.DONE, progress=10)
        renderer.on_task_done(task)

        mock_progress.update.assert_called_with(5, completed=10)
        mock_progress.stop_task.assert_called_once_with(5)


class TestOnTaskFailed:
    def test_updates_description_and_stops(
        self, renderer: ConsoleRenderer, mock_progress: MagicMock
    ) -> None:
        mock_progress.add_task.return_value = 3
        renderer.on_task_added(Task(name="sync", total=10))

        task = Task(name="sync", total=10, status=TaskStatus.FAILED, error="timeout")
        renderer.on_task_failed(task)

        mock_progress.update.assert_called_with(
            3, description="[bold red]sync FAILED: timeout"
        )
        mock_progress.stop_task.assert_called_once_with(3)

from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from app.tracking.tracker import ProgressRenderer, Task


class _RendererSignals(QObject):
    """Qt signal carrier - kept separate to avoid QObject/ABCMeta metaclass conflict."""

    task_added = Signal(str, object)  # name, total (int | None)
    progress_updated = Signal(str, int)  # name, progress
    task_done = Signal(str, list)  # name, warnings
    task_failed = Signal(str, str)  # name, error
    total_updated = Signal(str, int)  # name, new_total


class GuiRenderer(ProgressRenderer):
    """Bridges TaskTracker events to Qt signals for the GUI sync tab."""

    def __init__(self) -> None:
        self._signals = _RendererSignals()

    @property
    def signals(self) -> _RendererSignals:
        return self._signals

    def on_task_added(self, task: Task) -> None:
        self._signals.task_added.emit(task.name, task.total)

    def on_progress(self, task: Task) -> None:
        self._signals.progress_updated.emit(task.name, task.progress)

    def on_task_done(self, task: Task) -> None:
        self._signals.task_done.emit(task.name, list(task.warnings))

    def on_task_failed(self, task: Task) -> None:
        self._signals.task_failed.emit(task.name, task.error or "")

    def on_task_warning(self, task: Task, message: str) -> None:
        pass  # warnings are accumulated in task.warnings and shown at on_task_done

    def on_total_updated(self, task: Task) -> None:
        self._signals.total_updated.emit(task.name, task.total or 0)

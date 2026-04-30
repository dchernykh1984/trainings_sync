from app.tracking.tracker import ProgressRenderer, Task


class GuiRenderer(ProgressRenderer):
    """Stub — wired to PySide6 widgets in a future commit."""

    def on_task_added(self, task: Task) -> None:
        pass

    def on_progress(self, task: Task) -> None:
        pass

    def on_task_done(self, task: Task) -> None:
        pass

    def on_task_failed(self, task: Task) -> None:
        pass

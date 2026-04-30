from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)

from app.tracking.tracker import ProgressRenderer, Task


class ConsoleRenderer(ProgressRenderer):
    def __init__(self) -> None:
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
        )
        self._task_ids: dict[str, TaskID] = {}
        self._progress.start()

    def on_task_added(self, task: Task) -> None:
        task_id = self._progress.add_task(task.name, total=task.total)
        self._task_ids[task.name] = task_id

    def on_progress(self, task: Task) -> None:
        task_id = self._task_ids[task.name]
        self._progress.update(task_id, completed=task.progress)

    def on_task_done(self, task: Task) -> None:
        task_id = self._task_ids[task.name]
        self._progress.update(task_id, completed=task.total)
        self._progress.stop_task(task_id)

    def on_task_failed(self, task: Task) -> None:
        task_id = self._task_ids[task.name]
        self._progress.update(
            task_id,
            description=f"[bold red]{task.name} FAILED: {task.error}",
        )
        self._progress.stop_task(task_id)

    def stop(self) -> None:
        self._progress.stop()

    def __enter__(self) -> "ConsoleRenderer":
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()

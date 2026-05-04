from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from enum import Enum, auto


class TaskStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    DONE = auto()
    FAILED = auto()


@dataclass
class Task:
    name: str
    total: int | None
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


class ProgressRenderer(ABC):
    @abstractmethod
    def on_task_added(self, task: Task) -> None: ...

    @abstractmethod
    def on_progress(self, task: Task) -> None: ...

    @abstractmethod
    def on_task_done(self, task: Task) -> None: ...

    @abstractmethod
    def on_task_failed(self, task: Task) -> None: ...

    @abstractmethod
    def on_task_warning(self, task: Task, message: str) -> None: ...

    @abstractmethod
    def on_total_updated(self, task: Task) -> None: ...


class TaskTracker:
    def __init__(self, renderer: ProgressRenderer) -> None:
        self._renderer = renderer
        self._tasks: dict[str, Task] = {}
        self._lock = asyncio.Lock()

    async def add_task(self, name: str, total: int | None) -> None:
        if total is not None and total <= 0:
            raise ValueError(f"total must be positive, got {total}")
        async with self._lock:
            if name in self._tasks:
                raise ValueError(f"Task {name!r} already exists")
            task = Task(name=name, total=total)
            self._tasks[name] = task
        self._renderer.on_task_added(task)

    async def advance(self, name: str, amount: int = 1) -> None:
        if amount <= 0:
            raise ValueError(f"amount must be positive, got {amount}")
        async with self._lock:
            task = self._tasks[name]
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            if task.total is None:
                task.progress += amount
            else:
                task.progress = min(task.progress + amount, task.total)
            if task.status == TaskStatus.PENDING:
                task.status = TaskStatus.RUNNING
        self._renderer.on_progress(task)

    async def finish(self, name: str) -> None:
        async with self._lock:
            task = self._tasks[name]
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            task.status = TaskStatus.DONE
            if task.total is not None:
                task.progress = task.total
        self._renderer.on_task_done(task)

    async def fail(self, name: str, error: str) -> None:
        async with self._lock:
            task = self._tasks[name]
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            task.status = TaskStatus.FAILED
            task.error = error
        self._renderer.on_task_failed(task)

    async def update_total(self, name: str, total: int) -> None:
        if total <= 0:
            raise ValueError(f"total must be positive, got {total}")
        async with self._lock:
            task = self._tasks[name]
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            if task.total is not None and total <= task.total:
                return
            task.total = total
            if task.progress > total:
                task.progress = total
        self._renderer.on_total_updated(task)

    async def warn(self, name: str, message: str) -> None:
        async with self._lock:
            task = self._tasks[name]
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
            task.warnings.append(message)
        self._renderer.on_task_warning(task, message)

    @property
    def tasks(self) -> dict[str, Task]:
        return {
            name: replace(task, warnings=list(task.warnings))
            for name, task in self._tasks.items()
        }

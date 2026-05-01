from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto


class TaskStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    DONE = auto()
    FAILED = auto()


@dataclass
class Task:
    name: str
    total: int
    status: TaskStatus = TaskStatus.PENDING
    progress: int = 0
    error: str | None = None


class ProgressRenderer(ABC):
    @abstractmethod
    def on_task_added(self, task: Task) -> None: ...

    @abstractmethod
    def on_progress(self, task: Task) -> None: ...

    @abstractmethod
    def on_task_done(self, task: Task) -> None: ...

    @abstractmethod
    def on_task_failed(self, task: Task) -> None: ...


class TaskTracker:
    def __init__(self, renderer: ProgressRenderer) -> None:
        self._renderer = renderer
        self._tasks: dict[str, Task] = {}
        self._lock = asyncio.Lock()

    async def add_task(self, name: str, total: int) -> None:
        async with self._lock:
            task = Task(name=name, total=total)
            self._tasks[name] = task
        self._renderer.on_task_added(task)

    async def advance(self, name: str, amount: int = 1) -> None:
        async with self._lock:
            task = self._tasks[name]
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
                return
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

    @property
    def tasks(self) -> dict[str, Task]:
        return dict(self._tasks)

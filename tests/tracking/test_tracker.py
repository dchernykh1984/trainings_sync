import pytest

from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker


class _FakeRenderer(ProgressRenderer):
    def __init__(self) -> None:
        self.added: list[Task] = []
        self.progressed: list[Task] = []
        self.done: list[Task] = []
        self.failed: list[Task] = []

    def on_task_added(self, task: Task) -> None:
        self.added.append(task)

    def on_progress(self, task: Task) -> None:
        self.progressed.append(task)

    def on_task_done(self, task: Task) -> None:
        self.done.append(task)

    def on_task_failed(self, task: Task) -> None:
        self.failed.append(task)


@pytest.fixture
def renderer() -> _FakeRenderer:
    return _FakeRenderer()


@pytest.fixture
def tracker(renderer: _FakeRenderer) -> TaskTracker:
    return TaskTracker(renderer)


class TestAddTask:
    async def test_creates_pending_task(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)

        assert "sync" in tracker.tasks
        task = tracker.tasks["sync"]
        assert task.status == TaskStatus.PENDING
        assert task.progress == 0
        assert task.total == 10

    async def test_notifies_renderer(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=5)

        assert len(renderer.added) == 1
        assert renderer.added[0].name == "sync"

    async def test_raises_on_duplicate_name(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)

        with pytest.raises(ValueError, match="sync"):
            await tracker.add_task("sync", total=5)

    async def test_raises_on_non_positive_total(self, tracker: TaskTracker) -> None:
        with pytest.raises(ValueError, match="total"):
            await tracker.add_task("sync", total=0)

        with pytest.raises(ValueError, match="total"):
            await tracker.add_task("sync", total=-1)


class TestAdvance:
    async def test_transitions_to_running(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.advance("sync")

        assert tracker.tasks["sync"].status == TaskStatus.RUNNING

    async def test_increments_progress(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.advance("sync", amount=3)

        assert tracker.tasks["sync"].progress == 3

    async def test_clamps_at_total(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=5)
        await tracker.advance("sync", amount=100)

        assert tracker.tasks["sync"].progress == 5

    async def test_notifies_renderer(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.advance("sync", amount=2)

        assert len(renderer.progressed) == 1
        assert renderer.progressed[0].progress == 2

    async def test_raises_on_non_positive_amount(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)

        with pytest.raises(ValueError, match="amount"):
            await tracker.advance("sync", amount=0)

        with pytest.raises(ValueError, match="amount"):
            await tracker.advance("sync", amount=-1)

    async def test_accumulates_across_calls(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.advance("sync", amount=3)
        await tracker.advance("sync", amount=4)

        assert tracker.tasks["sync"].progress == 7

    async def test_ignored_after_finish(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.finish("sync")
        await tracker.advance("sync", amount=5)

        assert tracker.tasks["sync"].status == TaskStatus.DONE
        assert tracker.tasks["sync"].progress == 10
        assert len(renderer.progressed) == 0


class TestFinish:
    async def test_sets_done_status(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.finish("sync")

        task = tracker.tasks["sync"]
        assert task.status == TaskStatus.DONE
        assert task.progress == task.total

    async def test_notifies_renderer(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.finish("sync")

        assert len(renderer.done) == 1

    async def test_ignored_after_fail(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.fail("sync", error="timeout")
        await tracker.finish("sync")

        assert tracker.tasks["sync"].status == TaskStatus.FAILED
        assert len(renderer.done) == 0


class TestFail:
    async def test_sets_failed_status(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.fail("sync", error="timeout")

        task = tracker.tasks["sync"]
        assert task.status == TaskStatus.FAILED
        assert task.error == "timeout"

    async def test_notifies_renderer(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.fail("sync", error="timeout")

        assert len(renderer.failed) == 1

    async def test_ignored_after_finish(
        self, tracker: TaskTracker, renderer: _FakeRenderer
    ) -> None:
        await tracker.add_task("sync", total=10)
        await tracker.finish("sync")
        await tracker.fail("sync", error="timeout")

        assert tracker.tasks["sync"].status == TaskStatus.DONE
        assert len(renderer.failed) == 0


class TestTasksProperty:
    async def test_returns_copy(self, tracker: TaskTracker) -> None:
        await tracker.add_task("sync", total=10)
        snapshot = tracker.tasks
        snapshot.clear()

        assert "sync" in tracker.tasks

from __future__ import annotations

import asyncio
from datetime import date, timezone
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from app.connectors.base import ServiceConnector
from app.core.cache import ActivityCache
from app.core.config import GroupSourceConfig, SyncGroupConfig
from app.core.orchestrator import SyncOrchestrator

_UTC = timezone.utc
_START = date(2026, 1, 1)
_END = date(2026, 1, 31)


def _group(
    group_id: str, sources: list[str], destinations: list[str]
) -> SyncGroupConfig:
    return SyncGroupConfig(
        id=group_id,
        sources=tuple(
            GroupSourceConfig(id=s, priority=i + 1) for i, s in enumerate(sources)
        ),
        destinations=tuple(destinations),
    )


def _mock_connector() -> ServiceConnector:
    conn = MagicMock()
    conn.user_label = ""
    conn._max_concurrent = 1
    return cast(ServiceConnector, conn)


@pytest.fixture
def cache(tmp_path: Path) -> ActivityCache:
    c = ActivityCache(tmp_path / "cache")
    c.load()
    return c


# ---------------------------------------------------------------------------
# Sequential execution
# ---------------------------------------------------------------------------


async def test_runs_all_groups(cache: ActivityCache) -> None:
    g1 = _group("g1", ["strava"], ["garmin"])
    g2 = _group("g2", ["garmin"], ["local"])
    connectors = {
        "strava": _mock_connector(),
        "garmin": _mock_connector(),
        "local": _mock_connector(),
    }
    ran: set[str] = set()

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        ran.add(executor_self._task_prefix)

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)

    assert ran == {"g1: ", "g2: "}


async def test_groups_run_in_parallel(cache: ActivityCache) -> None:
    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["garmin"], [])
    connectors = {"strava": _mock_connector(), "garmin": _mock_connector()}
    run_log: list[str] = []

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        run_log.append(f"{executor_self._task_prefix}start")
        await asyncio.sleep(0)
        run_log.append(f"{executor_self._task_prefix}done")

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)

    assert run_log == ["g1: start", "g2: start", "g1: done", "g2: done"]


async def test_returns_total_download_failures_across_groups(
    cache: ActivityCache,
) -> None:
    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["garmin"], [])
    connectors = {"strava": _mock_connector(), "garmin": _mock_connector()}

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        executor_self._download_failures = 2

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        total = await orchestrator.run(_START, _END)

    assert total == 4  # 2 per group x 2 groups


async def test_task_prefix_set_to_group_id(cache: ActivityCache) -> None:
    g = _group("my-group", ["src"], [])
    connectors = {"src": _mock_connector()}
    captured_prefixes: list[str] = []

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        captured_prefixes.append(executor_self._task_prefix)

    orchestrator = SyncOrchestrator(groups=(g,), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)

    assert captured_prefixes == ["my-group: "]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


async def test_logs_group_start_and_end_when_sync_logger_present(
    cache: ActivityCache,
) -> None:
    g = _group("test-group", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        pass

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)

    calls = [c[0][0] for c in sync_logger.info.call_args_list]
    assert any("test-group" in m and "started" in m for m in calls)
    assert any("test-group" in m and "done" in m for m in calls)


async def test_logs_group_failed_when_executor_raises(cache: ActivityCache) -> None:
    g = _group("failing-group", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    async def _raising_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with patch("app.core.orchestrator.SyncExecutor.run", new=_raising_run):
        with pytest.raises(RuntimeError, match="boom"):
            await orchestrator.run(_START, _END)

    info_calls = [c[0][0] for c in sync_logger.info.call_args_list]
    error_calls = [c[0][0] for c in sync_logger.error.call_args_list]
    assert any("failing-group" in m and "started" in m for m in info_calls)
    assert any("failing-group" in m and "failed" in m for m in error_calls)
    assert not any("done" in m for m in info_calls)


async def test_no_log_when_no_tracker(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        pass

    orchestrator = SyncOrchestrator(groups=(g,), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)  # must not raise


async def test_no_log_when_tracker_has_no_sync_logger(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}

    tracker = MagicMock()
    tracker.sync_logger = None

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        pass

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)  # must not raise


# ---------------------------------------------------------------------------
# force flag forwarded
# ---------------------------------------------------------------------------


async def test_force_forwarded_to_executor(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}
    received_force: list[bool] = []

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        received_force.append(force)

    orchestrator = SyncOrchestrator(groups=(g,), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END, force=True)

    assert received_force == [True]


# ---------------------------------------------------------------------------
# Conflict-aware scheduling
# ---------------------------------------------------------------------------


async def test_groups_sharing_connector_run_sequentially(cache: ActivityCache) -> None:
    # g1 and g2 both use "strava"; they must be serialized via the strava lock.
    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["strava"], [])
    connectors = {"strava": _mock_connector()}
    run_log: list[str] = []

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        run_log.append(f"{executor_self._task_prefix}start")
        await asyncio.sleep(0)  # yield; g2 must not interleave because lock is held
        run_log.append(f"{executor_self._task_prefix}done")

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)

    assert run_log == ["g1: start", "g1: done", "g2: start", "g2: done"]


# ---------------------------------------------------------------------------
# Cancellation cleanup
# ---------------------------------------------------------------------------


async def test_externally_cancelled_run_logs_group_as_failed(
    cache: ActivityCache,
) -> None:
    # When orchestrator.run() is externally cancelled, asyncio.gather propagates
    # CancelledError to all running groups. The except BaseException in _run_group
    # must catch it and log "failed" (not leave the group logged only as "started").
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    async def _hanging_run(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        await asyncio.sleep(100)

    with patch("app.core.orchestrator.SyncExecutor.run", new=_hanging_run):
        orchestrator = SyncOrchestrator(
            groups=(g,), connectors=connectors, cache=cache, tracker=tracker
        )
        task = asyncio.create_task(orchestrator.run(_START, _END))
        await asyncio.sleep(0)  # let outer task create child tasks via asyncio.gather
        await asyncio.sleep(0)  # let child task run until it awaits _hanging_run
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    error_calls = [c[0][0] for c in sync_logger.error.call_args_list]
    assert any("g" in m and "failed" in m for m in error_calls)


async def test_login_tasks_passed_to_executor(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}
    captured: list = []

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        captured.append(executor_self._login_tasks)

    login_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(0))
    login_tasks = {"src": login_task}
    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, login_tasks=login_tasks
    )
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)
    await login_task

    assert captured == [login_tasks]

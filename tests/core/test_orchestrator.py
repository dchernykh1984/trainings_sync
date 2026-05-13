from __future__ import annotations

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


async def test_runs_all_groups_sequentially(cache: ActivityCache) -> None:
    g1 = _group("g1", ["strava"], ["garmin"])
    g2 = _group("g2", ["garmin"], ["local"])
    connectors = {
        "strava": _mock_connector(),
        "garmin": _mock_connector(),
        "local": _mock_connector(),
    }
    run_order: list[str] = []

    async def _fake_run(executor_self, start, end, *, force=False):  # type: ignore[no-untyped-def]
        # identify which group by task_prefix
        run_order.append(executor_self._task_prefix)

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.run", new=_fake_run):
        await orchestrator.run(_START, _END)

    assert run_order == ["g1: ", "g2: "]


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

from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import Activity, ActivityMeta, ServiceConnector
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
    conn.list_activities = AsyncMock(return_value=[])
    return cast(ServiceConnector, conn)


@pytest.fixture
def cache(tmp_path: Path) -> ActivityCache:
    c = ActivityCache(tmp_path / "cache")
    c.load()
    return c


# ---------------------------------------------------------------------------
# Source-level download orchestration
# ---------------------------------------------------------------------------


async def test_downloads_all_unique_sources(cache: ActivityCache) -> None:
    g1 = _group("g1", ["strava"], ["garmin"])
    g2 = _group("g2", ["garmin"], ["local"])
    connectors = {
        "strava": _mock_connector(),
        "garmin": _mock_connector(),
        "local": _mock_connector(),
    }
    downloaded: set[str] = set()

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        for spec, _ in executor_self._sources:
            downloaded.add(spec.source_id)

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)

    assert downloaded == {"strava", "garmin"}


async def test_source_downloads_run_in_parallel(cache: ActivityCache) -> None:
    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["garmin"], [])
    connectors = {"strava": _mock_connector(), "garmin": _mock_connector()}
    download_log: list[str] = []

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        src_id = executor_self._sources[0][0].source_id
        download_log.append(f"{src_id}-start")
        await asyncio.sleep(0)
        download_log.append(f"{src_id}-done")

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)

    assert download_log == [
        "strava-start",
        "garmin-start",
        "strava-done",
        "garmin-done",
    ]


async def test_returns_total_download_failures_across_sources(
    cache: ActivityCache,
) -> None:
    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["garmin"], [])
    connectors = {"strava": _mock_connector(), "garmin": _mock_connector()}

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        executor_self._download_failures = 2

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        total = await orchestrator.run(_START, _END)

    assert total == 4  # 2 per source x 2 sources


async def test_source_executor_has_empty_task_prefix(cache: ActivityCache) -> None:
    g = _group("my-group", ["src"], [])
    connectors = {"src": _mock_connector()}
    captured_prefixes: list[str] = []

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        captured_prefixes.append(executor_self._task_prefix)

    orchestrator = SyncOrchestrator(groups=(g,), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)

    assert captured_prefixes == [""]


async def test_same_source_downloaded_once_across_groups(cache: ActivityCache) -> None:
    # strava appears in both groups; orchestrator must download it exactly once.
    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["strava"], [])
    connectors = {"strava": _mock_connector()}

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    await orchestrator.run(_START, _END)

    # list_activities is called inside the single source executor's download_phase.
    cast(MagicMock, connectors["strava"]).list_activities.assert_called_once()


async def test_same_source_has_single_download_task(cache: ActivityCache) -> None:
    # When strava is in two groups, download_activity must be called once and
    # exactly one "Download strava" progress task must appear - not one per group.
    t0 = datetime(2026, 1, 15, 8, 0, tzinfo=timezone.utc)
    meta = ActivityMeta(
        external_id="act-1",
        name="Morning Run",
        sport_type="Run",
        start_time=t0,
        elapsed_s=3600,
    )
    activity = Activity(
        external_id="act-1",
        name="Morning Run",
        sport_type="Run",
        start_time=t0,
        elapsed_s=3600,
        content=b"fit-data",
        format="fit",
    )
    strava_mock = MagicMock()
    strava_mock.user_label = ""
    strava_mock._max_concurrent = 1
    strava_mock.list_activities = AsyncMock(return_value=[meta])
    strava_mock.download_activity = AsyncMock(return_value=activity)

    g1 = _group("g1", ["strava"], [])
    g2 = _group("g2", ["strava"], [])
    connectors: dict[str, ServiceConnector] = {
        "strava": cast(ServiceConnector, strava_mock)
    }

    tracker = MagicMock()
    tracker.sync_logger = None
    tracker.add_task = AsyncMock(side_effect=lambda name, **_: name)
    tracker.advance = AsyncMock()
    tracker.finish = AsyncMock()
    tracker.fail = AsyncMock()
    tracker.warn = AsyncMock()

    orchestrator = SyncOrchestrator(
        groups=(g1, g2), connectors=connectors, cache=cache, tracker=tracker
    )
    await orchestrator.run(_START, _END)

    assert strava_mock.download_activity.call_count == 1
    all_task_names = [c.args[0] for c in tracker.add_task.call_args_list]
    assert len([n for n in all_task_names if "Download strava" in n]) == 1


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


async def test_logs_source_start_and_end_when_sync_logger_present(
    cache: ActivityCache,
) -> None:
    g = _group("test-group", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        pass

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)

    calls = [c[0][0] for c in sync_logger.info.call_args_list]
    assert any("src" in m and "download started" in m for m in calls)
    assert any("src" in m and "download done" in m for m in calls)
    assert any("test-group" in m and "upload started" in m for m in calls)
    assert any("test-group" in m and "upload done" in m for m in calls)


async def test_logs_source_failed_when_download_raises(cache: ActivityCache) -> None:
    g = _group("failing-group", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    async def _raising_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with patch(
        "app.core.orchestrator.SyncExecutor.download_phase", new=_raising_download
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await orchestrator.run(_START, _END)

    info_calls = [c[0][0] for c in sync_logger.info.call_args_list]
    error_calls = [c[0][0] for c in sync_logger.error.call_args_list]
    assert any("src" in m and "download started" in m for m in info_calls)
    assert any("src" in m and "download failed" in m for m in error_calls)
    assert not any("download done" in m for m in info_calls)


async def test_logs_group_failed_when_upload_phase_raises(cache: ActivityCache) -> None:
    g = _group("upload-fail-group", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        pass

    async def _raising_upload(executor_self, start, end):  # type: ignore[no-untyped-def]
        raise RuntimeError("upload boom")

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with (
        patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download),
        patch("app.core.orchestrator.SyncExecutor.upload_phase", new=_raising_upload),
    ):
        with pytest.raises(RuntimeError, match="upload boom"):
            await orchestrator.run(_START, _END)

    error_calls = [c[0][0] for c in sync_logger.error.call_args_list]
    assert any("upload-fail-group" in m and "upload failed" in m for m in error_calls)


async def test_no_log_when_no_tracker(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        pass

    orchestrator = SyncOrchestrator(groups=(g,), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)  # must not raise


async def test_no_log_when_tracker_has_no_sync_logger(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}

    tracker = MagicMock()
    tracker.sync_logger = None

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        pass

    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, tracker=tracker
    )
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)  # must not raise


# ---------------------------------------------------------------------------
# force flag forwarded
# ---------------------------------------------------------------------------


async def test_force_forwarded_to_executor(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}
    received_force: list[bool] = []

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        received_force.append(force)

    orchestrator = SyncOrchestrator(groups=(g,), connectors=connectors, cache=cache)
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END, force=True)

    assert received_force == [True]


# ---------------------------------------------------------------------------
# Phase-aware scheduling
# ---------------------------------------------------------------------------


async def test_uploads_serialized_when_source_is_other_groups_destination(
    cache: ActivityCache,
) -> None:
    # strava-to-garmin must finish uploading before garmin-to-local reads garmin.
    # They share "garmin" in their connector sets (dest vs source), so upload
    # phases must be serialized even though their destinations differ.
    g1 = _group("g1", ["strava"], ["garmin"])
    g2 = _group("g2", ["garmin"], ["local"])
    connectors = {
        "strava": _mock_connector(),
        "garmin": _mock_connector(),
        "local": _mock_connector(),
    }
    upload_log: list[str] = []

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        pass

    async def _fake_upload(executor_self, start, end):  # type: ignore[no-untyped-def]
        upload_log.append(f"{executor_self._task_prefix}start")
        await asyncio.sleep(
            0
        )  # yield; sibling must not interleave because lock is held
        upload_log.append(f"{executor_self._task_prefix}done")

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with (
        patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download),
        patch("app.core.orchestrator.SyncExecutor.upload_phase", new=_fake_upload),
    ):
        await orchestrator.run(_START, _END)

    assert upload_log in [
        ["g1: start", "g1: done", "g2: start", "g2: done"],
        ["g2: start", "g2: done", "g1: start", "g1: done"],
    ]


async def test_uploads_with_shared_destination_run_sequentially(
    cache: ActivityCache,
) -> None:
    # g1 and g2 both upload to "garmin"; they must be serialized via the
    # garmin destination lock to prevent duplicate uploads.
    g1 = _group("g1", ["strava"], ["garmin"])
    g2 = _group("g2", ["local"], ["garmin"])
    connectors = {
        "strava": _mock_connector(),
        "local": _mock_connector(),
        "garmin": _mock_connector(),
    }
    upload_log: list[str] = []

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        pass

    async def _fake_upload(executor_self, start, end):  # type: ignore[no-untyped-def]
        upload_log.append(f"{executor_self._task_prefix}start")
        await asyncio.sleep(
            0
        )  # yield; sibling must not interleave because lock is held
        upload_log.append(f"{executor_self._task_prefix}done")

    orchestrator = SyncOrchestrator(groups=(g1, g2), connectors=connectors, cache=cache)
    with (
        patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download),
        patch("app.core.orchestrator.SyncExecutor.upload_phase", new=_fake_upload),
    ):
        await orchestrator.run(_START, _END)

    assert upload_log in [
        ["g1: start", "g1: done", "g2: start", "g2: done"],
        ["g2: start", "g2: done", "g1: start", "g1: done"],
    ]


# ---------------------------------------------------------------------------
# Cancellation cleanup
# ---------------------------------------------------------------------------


async def test_externally_cancelled_run_logs_source_as_failed(
    cache: ActivityCache,
) -> None:
    # When orchestrator.run() is externally cancelled, asyncio.gather propagates
    # CancelledError to all running source download phases. The except BaseException
    # in _download_source_phase must catch it and log "failed".
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}

    sync_logger = MagicMock()
    tracker = MagicMock()
    tracker.sync_logger = sync_logger

    reached = asyncio.Event()

    async def _hanging_download(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        reached.set()
        await asyncio.sleep(100)

    with patch(
        "app.core.orchestrator.SyncExecutor.download_phase", new=_hanging_download
    ):
        orchestrator = SyncOrchestrator(
            groups=(g,), connectors=connectors, cache=cache, tracker=tracker
        )
        task = asyncio.create_task(orchestrator.run(_START, _END))
        await reached.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    error_calls = [c[0][0] for c in sync_logger.error.call_args_list]
    assert any("src" in m and "download failed" in m for m in error_calls)


async def test_login_tasks_passed_to_source_executor(cache: ActivityCache) -> None:
    g = _group("g", ["src"], [])
    connectors = {"src": _mock_connector()}
    captured: list = []

    async def _fake_download(executor_self, start, end, *, force=False, **_kw):  # type: ignore[no-untyped-def]
        captured.append(executor_self._login_tasks)

    login_task: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(0))
    login_tasks = {"src": login_task}
    orchestrator = SyncOrchestrator(
        groups=(g,), connectors=connectors, cache=cache, login_tasks=login_tasks
    )
    with patch("app.core.orchestrator.SyncExecutor.download_phase", new=_fake_download):
        await orchestrator.run(_START, _END)
    await login_task

    assert captured == [login_tasks]

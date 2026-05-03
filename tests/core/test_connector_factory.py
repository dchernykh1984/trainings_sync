from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pytest

from app.connectors.garmin import GarminConnector
from app.connectors.local_folder import LocalFolderConnector
from app.connectors.strava import StravaConnector
from app.core.config import (
    AppConfig,
    GarminDestinationConfig,
    GarminSourceConfig,
    LocalFolderDestinationConfig,
    LocalFolderSourceConfig,
    StravaDestinationConfig,
    StravaSourceConfig,
)
from app.core.connector_factory import build_destinations, build_sources
from app.credentials.base import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
    StravaCredentials,
)
from app.tracking.tracker import ProgressRenderer, Task, TaskTracker

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeRenderer(ProgressRenderer):
    def on_task_added(self, task: Task) -> None:
        pass

    def on_progress(self, task: Task) -> None:
        pass

    def on_task_done(self, task: Task) -> None:
        pass

    def on_task_failed(self, task: Task) -> None:
        pass

    def on_task_warning(self, task: Task, message: str) -> None:
        pass


class _FakeProvider(CredentialProvider):
    def __init__(self, creds: list[Credentials]) -> None:
        self._queue = list(creds)
        self.get_credentials_calls = 0
        self.get_many_calls = 0

    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        self.get_credentials_calls += 1
        return self._queue.pop(0)

    async def get_many(
        self, requests: Sequence[CredentialRequest]
    ) -> list[Credentials]:
        self.get_many_calls += 1
        return [self._queue.pop(0) for _ in requests]


# ---------------------------------------------------------------------------
# Constants and helpers
# ---------------------------------------------------------------------------

_GARMIN_CRED = CredentialRequest(
    service="Garmin Connect",
    url="https://connect.garmin.com",
    login="user@example.com",
)
_STRAVA_CRED = CredentialRequest(service="Strava", url="https://www.strava.com")

_GARMIN_SRC = GarminSourceConfig(id="garmin-main", priority=1, credential=_GARMIN_CRED)
_STRAVA_SRC = StravaSourceConfig(
    id="strava-main", priority=2, client_id=99999, credential=_STRAVA_CRED
)

_GARMIN_DEST = GarminDestinationConfig(id="garmin-upload", credential=_GARMIN_CRED)
_STRAVA_DEST = StravaDestinationConfig(
    id="strava-upload", client_id=99999, credential=_STRAVA_CRED
)

_GARMIN_CREDS = Credentials(login="user@example.com", password="garmin-pass")
_STRAVA_CREDS = Credentials(login="client-secret-val", password="refresh-token-val")


def _cfg(
    sources: tuple = (_GARMIN_SRC,),
    destinations: tuple = (),
) -> AppConfig:
    return AppConfig(
        cache_dir=Path("/nonexistent/cache"),
        sources=sources,
        destinations=destinations,
    )


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


# ---------------------------------------------------------------------------
# build_sources — connector types
# ---------------------------------------------------------------------------


async def test_garmin_source_builds_garmin_connector(tracker: TaskTracker) -> None:
    provider = _FakeProvider([_GARMIN_CREDS])
    sources = await build_sources(_cfg(), provider, tracker)
    assert len(sources) == 1
    spec, connector = sources[0]
    assert isinstance(connector, GarminConnector)
    assert spec.source_id == "garmin-main"
    assert spec.priority == 1


async def test_local_source_builds_local_connector(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderSourceConfig(id="local", priority=2, folder=tmp_path)
    sources = await build_sources(_cfg(sources=(local,)), _FakeProvider([]), tracker)
    _, connector = sources[0]
    assert isinstance(connector, LocalFolderConnector)


async def test_mixed_sources_built_in_order(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderSourceConfig(id="local", priority=2, folder=tmp_path)
    provider = _FakeProvider([_GARMIN_CREDS])
    sources = await build_sources(_cfg(sources=(_GARMIN_SRC, local)), provider, tracker)
    assert len(sources) == 2
    assert isinstance(sources[0][1], GarminConnector)
    assert isinstance(sources[1][1], LocalFolderConnector)


async def test_strava_source_raises(tracker: TaskTracker) -> None:
    with pytest.raises(ValueError, match="Strava cannot be used as a download source"):
        await build_sources(_cfg(sources=(_STRAVA_SRC,)), _FakeProvider([]), tracker)


# ---------------------------------------------------------------------------
# build_sources — credential batching
# ---------------------------------------------------------------------------


async def test_sources_credentials_fetched_in_one_batch(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    garmin2 = GarminSourceConfig(
        id="garmin-2",
        priority=2,
        credential=CredentialRequest("G2", "https://g2.com"),
    )
    local = LocalFolderSourceConfig(id="local", priority=3, folder=tmp_path)
    provider = _FakeProvider([_GARMIN_CREDS, Credentials("u2", "p2")])
    await build_sources(_cfg(sources=(_GARMIN_SRC, garmin2, local)), provider, tracker)
    assert provider.get_many_calls == 1
    assert provider.get_credentials_calls == 0


async def test_all_local_sources_batch_is_empty(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderSourceConfig(id="local", priority=1, folder=tmp_path)
    provider = _FakeProvider([])
    await build_sources(_cfg(sources=(local,)), provider, tracker)
    assert provider.get_many_calls == 1
    assert provider.get_credentials_calls == 0


# ---------------------------------------------------------------------------
# build_destinations — connector types
# ---------------------------------------------------------------------------


async def test_garmin_destination_builds_garmin_connector(
    tracker: TaskTracker,
) -> None:
    provider = _FakeProvider([_GARMIN_CREDS])
    dests = await build_destinations(
        _cfg(destinations=(_GARMIN_DEST,)), provider, tracker
    )
    assert len(dests) == 1
    dest_id, connector = dests[0]
    assert dest_id == "garmin-upload"
    assert isinstance(connector, GarminConnector)


async def test_strava_destination_builds_strava_connector(
    tracker: TaskTracker,
) -> None:
    provider = _FakeProvider([_STRAVA_CREDS])
    dests = await build_destinations(
        _cfg(destinations=(_STRAVA_DEST,)), provider, tracker
    )
    _, connector = dests[0]
    assert isinstance(connector, StravaConnector)
    assert connector._credentials.client_id == 99999
    assert connector._credentials.client_secret == "client-secret-val"
    assert connector._credentials.refresh_token == "refresh-token-val"


async def test_local_destination_builds_local_connector(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderDestinationConfig(id="local-dest", folder=tmp_path)
    dests = await build_destinations(
        _cfg(destinations=(local,)), _FakeProvider([]), tracker
    )
    _, connector = dests[0]
    assert isinstance(connector, LocalFolderConnector)


async def test_empty_destinations_returns_empty_list(tracker: TaskTracker) -> None:
    dests = await build_destinations(_cfg(destinations=()), _FakeProvider([]), tracker)
    assert dests == []


async def test_all_destination_types_built_in_order(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderDestinationConfig(id="local-dest", folder=tmp_path)
    provider = _FakeProvider([_GARMIN_CREDS, _STRAVA_CREDS])
    dests = await build_destinations(
        _cfg(destinations=(_GARMIN_DEST, _STRAVA_DEST, local)),
        provider,
        tracker,
    )
    assert len(dests) == 3
    assert isinstance(dests[0][1], GarminConnector)
    assert isinstance(dests[1][1], StravaConnector)
    assert isinstance(dests[2][1], LocalFolderConnector)


# ---------------------------------------------------------------------------
# build_destinations — duplicate Strava credential check
# ---------------------------------------------------------------------------


async def test_duplicate_strava_cred_raises(tracker: TaskTracker) -> None:
    strava2 = StravaDestinationConfig(
        id="strava-2", client_id=99999, credential=_STRAVA_CRED
    )
    with pytest.raises(ValueError, match="duplicate Strava credential ref"):
        await build_destinations(
            _cfg(destinations=(_STRAVA_DEST, strava2)), _FakeProvider([]), tracker
        )


async def test_different_strava_cred_refs_allowed(tracker: TaskTracker) -> None:
    other_cred = CredentialRequest("Strava", "https://strava.com", login="other")
    strava2 = StravaDestinationConfig(
        id="strava-2", client_id=11111, credential=other_cred
    )
    provider = _FakeProvider([_STRAVA_CREDS, Credentials("s2", "r2")])
    dests = await build_destinations(
        _cfg(destinations=(_STRAVA_DEST, strava2)), provider, tracker
    )
    assert len(dests) == 2


# ---------------------------------------------------------------------------
# build_destinations — on_strava_token_refresh callback
# ---------------------------------------------------------------------------


async def test_strava_token_refresh_callback_called_with_connector_id(
    tracker: TaskTracker,
) -> None:
    refreshed: list[tuple[str, StravaCredentials]] = []

    def on_refresh(dest_id: str, new_creds: StravaCredentials) -> None:
        refreshed.append((dest_id, new_creds))

    provider = _FakeProvider([_STRAVA_CREDS])
    dests = await build_destinations(
        _cfg(destinations=(_STRAVA_DEST,)),
        provider,
        tracker,
        on_strava_token_refresh=on_refresh,
    )
    _, connector = dests[0]
    assert isinstance(connector, StravaConnector)

    new_creds = StravaCredentials(
        client_id=99999, client_secret="s", refresh_token="new-rt"
    )
    assert connector._on_token_refresh is not None
    connector._on_token_refresh(new_creds)

    assert refreshed == [("strava-upload", new_creds)]


async def test_no_callback_sets_strava_callback_to_none(tracker: TaskTracker) -> None:
    provider = _FakeProvider([_STRAVA_CREDS])
    dests = await build_destinations(
        _cfg(destinations=(_STRAVA_DEST,)), provider, tracker
    )
    _, connector = dests[0]
    assert isinstance(connector, StravaConnector)
    assert connector._on_token_refresh is None


# ---------------------------------------------------------------------------
# build_destinations — credential batching
# ---------------------------------------------------------------------------


async def test_destinations_credentials_fetched_in_one_batch(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderDestinationConfig(id="local-dest", folder=tmp_path)
    provider = _FakeProvider([_GARMIN_CREDS, _STRAVA_CREDS])
    await build_destinations(
        _cfg(destinations=(_GARMIN_DEST, _STRAVA_DEST, local)),
        provider,
        tracker,
    )
    assert provider.get_many_calls == 1
    assert provider.get_credentials_calls == 0

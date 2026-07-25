from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pytest

from app.connectors.base import ServiceConnector
from app.connectors.garmin import GarminConnector
from app.connectors.local_folder import LocalFolderConnector
from app.connectors.strava import StravaConnector
from app.core.cache import ActivityCache, CacheEntry
from app.core.config import (
    AppConfig,
    GarminConnectorConfig,
    GroupSourceConfig,
    LocalFolderConnectorConfig,
    StravaConnectorConfig,
    SyncGroupConfig,
)
from app.core.connector_factory import (
    build_connectors,
    resolve_group_destinations,
    resolve_group_sources,
)
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

    def on_total_updated(self, task: Task) -> None:
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
        self, requests: Sequence[CredentialRequest], context: str = ""
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

_GARMIN_CFG = GarminConnectorConfig(id="garmin", credential=_GARMIN_CRED)
_STRAVA_CFG = StravaConnectorConfig(
    id="strava", client_id=99999, credential=_STRAVA_CRED
)

_GARMIN_CREDS = Credentials(login="user@example.com", password="garmin-pass")
_STRAVA_CREDS = Credentials(login="client-secret-val", password="refresh-token-val")

_DEFAULT_GROUP = SyncGroupConfig(
    id="default",
    sources=(GroupSourceConfig(id="garmin", priority=1),),
    destinations=(),
)


def _cfg(
    connectors: tuple = (_GARMIN_CFG,),
    sync_groups: tuple = (_DEFAULT_GROUP,),
) -> AppConfig:
    return AppConfig(
        cache_dir=Path("/nonexistent/cache"),
        connectors=connectors,
        sync_groups=sync_groups,
    )


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def cache(tmp_path: Path) -> ActivityCache:
    c = ActivityCache(tmp_path / "cache")
    c.load()
    return c


# ---------------------------------------------------------------------------
# build_connectors - connector types
# ---------------------------------------------------------------------------


async def test_garmin_connector_builds_garmin_connector(
    tracker: TaskTracker, cache: ActivityCache
) -> None:
    provider = _FakeProvider([_GARMIN_CREDS])
    result = await build_connectors(_cfg(), provider, tracker)
    assert len(result) == 1
    assert "garmin" in result
    assert isinstance(result["garmin"], GarminConnector)


async def test_strava_connector_builds_strava_connector(
    tracker: TaskTracker, cache: ActivityCache
) -> None:
    group = SyncGroupConfig(
        id="g", sources=(GroupSourceConfig(id="strava", priority=1),), destinations=()
    )
    provider = _FakeProvider([_STRAVA_CREDS])
    result = await build_connectors(
        _cfg(connectors=(_STRAVA_CFG,), sync_groups=(group,)), provider, tracker
    )
    assert isinstance(result["strava"], StravaConnector)
    c = result["strava"]
    assert isinstance(c, StravaConnector)
    assert c._credentials.client_id == 99999
    assert c._credentials.client_secret == "client-secret-val"
    assert c._credentials.refresh_token == "refresh-token-val"


async def test_local_connector_builds_local_connector(
    tracker: TaskTracker, cache: ActivityCache, tmp_path: Path
) -> None:
    local = LocalFolderConnectorConfig(id="local", folder=tmp_path)
    group = SyncGroupConfig(
        id="g", sources=(GroupSourceConfig(id="local", priority=1),), destinations=()
    )
    provider = _FakeProvider([])
    result = await build_connectors(
        _cfg(connectors=(local,), sync_groups=(group,)), provider, tracker
    )
    assert isinstance(result["local"], LocalFolderConnector)


async def test_local_connector_built_in_source_mode(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    local = LocalFolderConnectorConfig(id="my-local", folder=tmp_path)
    group = SyncGroupConfig(
        id="g", sources=(GroupSourceConfig(id="my-local", priority=1),), destinations=()
    )
    provider = _FakeProvider([])
    result = await build_connectors(
        _cfg(connectors=(local,), sync_groups=(group,)), provider, tracker
    )
    connector = result["my-local"]
    assert isinstance(connector, LocalFolderConnector)
    assert connector._cache is None
    assert connector._dest_id == ""


async def test_all_connector_types_built(
    tracker: TaskTracker, cache: ActivityCache, tmp_path: Path
) -> None:
    local = LocalFolderConnectorConfig(id="local", folder=tmp_path)
    group = SyncGroupConfig(
        id="g",
        sources=(
            GroupSourceConfig(id="garmin", priority=1),
            GroupSourceConfig(id="strava", priority=2),
        ),
        destinations=("local",),
    )
    provider = _FakeProvider([_GARMIN_CREDS, _STRAVA_CREDS])
    result = await build_connectors(
        _cfg(connectors=(_GARMIN_CFG, _STRAVA_CFG, local), sync_groups=(group,)),
        provider,
        tracker,
    )
    assert len(result) == 3
    assert isinstance(result["garmin"], GarminConnector)
    assert isinstance(result["strava"], StravaConnector)
    assert isinstance(result["local"], LocalFolderConnector)


async def test_result_is_dict_keyed_by_connector_id(
    tracker: TaskTracker, cache: ActivityCache, tmp_path: Path
) -> None:
    local = LocalFolderConnectorConfig(id="local", folder=tmp_path)
    group = SyncGroupConfig(
        id="g",
        sources=(GroupSourceConfig(id="garmin", priority=1),),
        destinations=("local",),
    )
    provider = _FakeProvider([_GARMIN_CREDS])
    result = await build_connectors(
        _cfg(connectors=(_GARMIN_CFG, local), sync_groups=(group,)),
        provider,
        tracker,
    )
    assert set(result.keys()) == {"garmin", "local"}


# ---------------------------------------------------------------------------
# build_connectors - credential batching
# ---------------------------------------------------------------------------


async def test_credentials_fetched_in_one_batch(
    tracker: TaskTracker, cache: ActivityCache, tmp_path: Path
) -> None:
    garmin2 = GarminConnectorConfig(
        id="garmin2", credential=CredentialRequest("G2", "https://g2.com")
    )
    local = LocalFolderConnectorConfig(id="local", folder=tmp_path)
    group = SyncGroupConfig(
        id="g",
        sources=(
            GroupSourceConfig(id="garmin", priority=1),
            GroupSourceConfig(id="garmin2", priority=2),
        ),
        destinations=("local",),
    )
    provider = _FakeProvider([_GARMIN_CREDS, Credentials("u2", "p2")])
    await build_connectors(
        _cfg(connectors=(_GARMIN_CFG, garmin2, local), sync_groups=(group,)),
        provider,
        tracker,
    )
    assert provider.get_many_calls == 1
    assert provider.get_credentials_calls == 0


async def test_all_local_connectors_batch_is_empty(
    tracker: TaskTracker, cache: ActivityCache, tmp_path: Path
) -> None:
    local = LocalFolderConnectorConfig(id="local", folder=tmp_path)
    group = SyncGroupConfig(
        id="g", sources=(GroupSourceConfig(id="local", priority=1),), destinations=()
    )
    provider = _FakeProvider([])
    await build_connectors(
        _cfg(connectors=(local,), sync_groups=(group,)), provider, tracker
    )
    assert provider.get_many_calls == 1
    assert provider.get_credentials_calls == 0


# ---------------------------------------------------------------------------
# build_connectors - Strava token refresh callback
# ---------------------------------------------------------------------------


async def test_strava_callback_wired(
    tracker: TaskTracker, cache: ActivityCache
) -> None:
    refreshed: list[tuple[str, StravaCredentials, str]] = []

    def on_refresh(
        connector_id: str, new_creds: StravaCredentials, user_label: str
    ) -> None:
        refreshed.append((connector_id, new_creds, user_label))

    group = SyncGroupConfig(
        id="g", sources=(GroupSourceConfig(id="strava", priority=1),), destinations=()
    )
    provider = _FakeProvider([_STRAVA_CREDS])
    result = await build_connectors(
        _cfg(connectors=(_STRAVA_CFG,), sync_groups=(group,)),
        provider,
        tracker,
        on_strava_token_refresh=on_refresh,
    )
    connector = result["strava"]
    assert isinstance(connector, StravaConnector)

    new_creds = StravaCredentials(
        client_id=99999, client_secret="s", refresh_token="new-rt"
    )
    assert connector._on_token_refresh is not None
    connector._on_token_refresh(new_creds, "John Doe")

    assert refreshed == [("strava", new_creds, "John Doe")]


async def test_strava_no_callback_sets_to_none(
    tracker: TaskTracker, cache: ActivityCache
) -> None:
    group = SyncGroupConfig(
        id="g", sources=(GroupSourceConfig(id="strava", priority=1),), destinations=()
    )
    provider = _FakeProvider([_STRAVA_CREDS])
    result = await build_connectors(
        _cfg(connectors=(_STRAVA_CFG,), sync_groups=(group,)), provider, tracker
    )
    connector = result["strava"]
    assert isinstance(connector, StravaConnector)
    assert connector._on_token_refresh is None


# ---------------------------------------------------------------------------
# resolve_group_sources
# ---------------------------------------------------------------------------


def test_resolve_group_sources_returns_specs_and_connectors(tmp_path: Path) -> None:
    from unittest.mock import MagicMock

    garmin_conn = MagicMock()
    strava_conn = MagicMock()
    connectors: dict[str, Any] = {
        "garmin": garmin_conn,
        "strava": strava_conn,
        "local": MagicMock(),
    }
    group = SyncGroupConfig(
        id="g",
        sources=(
            GroupSourceConfig(id="garmin", priority=1),
            GroupSourceConfig(id="strava", priority=2),
        ),
        destinations=("local",),
    )
    result = resolve_group_sources(group, connectors)
    assert len(result) == 2
    spec0, conn0 = result[0]
    assert spec0.source_id == "garmin"
    assert spec0.priority == 1
    assert conn0 is garmin_conn
    spec1, conn1 = result[1]
    assert spec1.source_id == "strava"
    assert spec1.priority == 2
    assert conn1 is strava_conn


def test_resolve_group_sources_preserves_order(tmp_path: Path) -> None:
    from unittest.mock import MagicMock

    c1, c2, c3 = MagicMock(), MagicMock(), MagicMock()
    connectors: dict[str, Any] = {"a": c1, "b": c2, "c": c3}
    group = SyncGroupConfig(
        id="g",
        sources=(
            GroupSourceConfig(id="c", priority=3),
            GroupSourceConfig(id="a", priority=1),
            GroupSourceConfig(id="b", priority=2),
        ),
        destinations=(),
    )
    result = resolve_group_sources(group, connectors)
    assert [r[1] for r in result] == [c3, c1, c2]


# ---------------------------------------------------------------------------
# resolve_group_destinations
# ---------------------------------------------------------------------------


def test_resolve_group_destinations_returns_id_connector_pairs() -> None:
    from unittest.mock import MagicMock

    local_conn = MagicMock()
    garmin_conn = MagicMock()
    connectors: dict[str, Any] = {"garmin": garmin_conn, "local": local_conn}
    group = SyncGroupConfig(
        id="g",
        sources=(GroupSourceConfig(id="garmin", priority=1),),
        destinations=("local",),
    )
    result = resolve_group_destinations(group, connectors, cache=object())
    assert result == [("local", local_conn)]


def test_resolve_group_destinations_multiple() -> None:
    from unittest.mock import MagicMock

    c1, c2 = MagicMock(), MagicMock()
    connectors: dict[str, Any] = {"d1": c1, "d2": c2, "src": MagicMock()}
    group = SyncGroupConfig(
        id="g",
        sources=(GroupSourceConfig(id="src", priority=1),),
        destinations=("d1", "d2"),
    )
    result = resolve_group_destinations(group, connectors, cache=object())
    assert len(result) == 2
    assert result[0] == ("d1", c1)
    assert result[1] == ("d2", c2)


def test_resolve_group_destinations_wraps_local_as_destination(
    cache: ActivityCache, tmp_path: Path
) -> None:
    from app.tracking.tracker import TaskTracker

    local_source = LocalFolderConnector(
        folder=tmp_path, tracker=TaskTracker(_FakeRenderer())
    )
    connectors: dict[str, Any] = {"local": local_source}
    group = SyncGroupConfig(
        id="g",
        sources=(GroupSourceConfig(id="local", priority=1),),
        destinations=("local",),
    )
    result = resolve_group_destinations(group, connectors, cache=cache)
    assert len(result) == 1
    dest_id, dest_conn = result[0]
    assert dest_id == "local"
    assert isinstance(dest_conn, LocalFolderConnector)
    assert dest_conn is not local_source
    assert dest_conn._cache is cache
    assert dest_conn._dest_id == "local"


# ---------------------------------------------------------------------------
# Cache keys follow the uid, labels follow the name
# ---------------------------------------------------------------------------


async def test_connectors_are_keyed_by_uid_not_by_name(
    tracker: TaskTracker, tmp_path: Path
) -> None:
    # The key is what the cache is filed under, so it must be the thing that
    # does not move when the user renames a connector.
    local = LocalFolderConnectorConfig(id="Garmin Denis", uid="3f9a1c", folder=tmp_path)
    result = await build_connectors(
        _cfg(connectors=(local,), sync_groups=()), _FakeProvider([]), tracker
    )
    assert list(result) == ["3f9a1c"]


def test_a_group_resolves_names_to_uids() -> None:
    group = SyncGroupConfig(
        id="g",
        sources=(GroupSourceConfig(id="Garmin Denis", priority=1),),
        destinations=("Garmin Denis",),
    )
    connectors: dict[str, ServiceConnector] = {
        "3f9a1c": cast(ServiceConnector, object())
    }
    uid_by_id = {"Garmin Denis": "3f9a1c"}

    ((spec, _),) = resolve_group_sources(group, connectors, uid_by_id)
    ((dest_id, _),) = resolve_group_destinations(group, connectors, object(), uid_by_id)

    assert spec.source_id == "3f9a1c"
    assert dest_id == "3f9a1c"


def test_renaming_a_connector_keeps_its_cached_activities(tmp_path: Path) -> None:
    # The whole point of keying by uid: a rename is a label change, so nothing
    # in the cache has to move and nothing is re-downloaded.
    cache = ActivityCache(tmp_path)
    entry = CacheEntry(
        external_id="1",
        source_id="3f9a1c",  # the uid, not the name
        format="gpx",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        elapsed_s=60,
    )
    cache.put(entry, b"track")

    before = LocalFolderConnectorConfig(
        id="Garmin Denis", uid="3f9a1c", folder=tmp_path
    )
    after = LocalFolderConnectorConfig(id="Garmin Home", uid="3f9a1c", folder=tmp_path)

    assert cache.has("1", before.uid)
    assert cache.has("1", after.uid)  # renamed, and still found


def test_two_connectors_swapping_names_keep_their_own_activities(
    tmp_path: Path,
) -> None:
    # The case that made a name-keyed cache so hard to migrate: with uids it
    # is not a case at all, because neither key moves.
    cache = ActivityCache(tmp_path)
    for uid, payload in (("aaa", b"alpha"), ("bbb", b"bravo")):
        cache.put(
            CacheEntry(
                external_id=uid,
                source_id=uid,
                format="gpx",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                elapsed_s=60,
            ),
            payload,
        )

    # Alpha and Bravo swap display names; the uids are untouched.
    a = LocalFolderConnectorConfig(id="Bravo", uid="aaa", folder=tmp_path)
    b = LocalFolderConnectorConfig(id="Alpha", uid="bbb", folder=tmp_path)

    alpha = cache.get_entry("aaa", a.uid)
    bravo = cache.get_entry("bbb", b.uid)
    assert alpha is not None and bravo is not None
    assert cache.read_content(alpha) == b"alpha"
    assert cache.read_content(bravo) == b"bravo"


def test_a_config_built_in_code_still_gets_an_identity() -> None:
    # An empty uid would collapse every such connector onto one cache key.
    assert LocalFolderConnectorConfig(id="local", folder=Path("/tmp")).uid == "local"

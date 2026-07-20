"""Tests for app.gui.config_store."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.core.config import (
    GarminConnectorConfig,
    LocalFolderConnectorConfig,
    StravaConnectorConfig,
)
from app.gui.config_store import (
    ConfigStore,
    ConnectorEntry,
    CredentialEntry,
    GroupSourceEntry,
    GuiConfig,
    SyncGroupEntry,
    _atomic_write,
    _connector_to_app,
    _group_to_app,
    _parse_connector_entry,
    _parse_group_entry,
    _serialize_connector,
    _serialize_group,
)


@pytest.fixture()
def store(tmp_path: Path) -> ConfigStore:
    return ConfigStore(config_dir=tmp_path / "cfg")


# ---------------------------------------------------------------------------
# ConfigStore - directory creation
# ---------------------------------------------------------------------------


def test_store_creates_directory(tmp_path: Path) -> None:
    cfg_dir = tmp_path / "a" / "b" / "c"
    assert not cfg_dir.exists()
    store = ConfigStore(config_dir=cfg_dir)
    assert store.config_dir.exists()


def test_cache_dir_property(store: ConfigStore) -> None:
    assert store.cache_dir == store.config_dir / "cache"


def test_credentials_path_property(store: ConfigStore) -> None:
    assert store.credentials_path == store.config_dir / "credentials.json"


# ---------------------------------------------------------------------------
# Credentials - load / save
# ---------------------------------------------------------------------------


def test_load_credentials_empty_when_file_absent(store: ConfigStore) -> None:
    assert store.load_credentials() == []


def test_save_and_load_credentials(store: ConfigStore) -> None:
    entries = [
        CredentialEntry(
            "Garmin Connect", "https://connect.garmin.com", "me@example.com", "s3cr3t"
        ),
        CredentialEntry(
            "Strava", "https://www.strava.com/api/v3", "clientsecret", "refreshtoken"
        ),
    ]
    store.save_credentials(entries)
    loaded = store.load_credentials()
    assert loaded == entries


def test_save_credentials_uses_atomic_write(store: ConfigStore) -> None:
    # After saving, no .tmp file should remain.
    store.save_credentials([CredentialEntry("S", "U", "L", "P")])
    tmp = store.credentials_path.with_suffix(".tmp")
    assert not tmp.exists()
    assert store.credentials_path.exists()


def test_save_credentials_overwrites(store: ConfigStore) -> None:
    store.save_credentials([CredentialEntry("A", "U1", "L1", "P1")])
    store.save_credentials([CredentialEntry("B", "U2", "L2", "P2")])
    loaded = store.load_credentials()
    assert len(loaded) == 1
    assert loaded[0].service == "B"


# ---------------------------------------------------------------------------
# GuiConfig - load / save
# ---------------------------------------------------------------------------


def test_load_gui_config_defaults_when_absent(store: ConfigStore) -> None:
    cfg = store.load_gui_config()
    assert cfg.connectors == []
    assert cfg.sync_groups == []
    assert cfg.start == ""
    assert cfg.end == ""
    assert cfg.force is False
    assert cfg.skip_wellness is False


def test_save_and_load_gui_config_round_trips(store: ConfigStore) -> None:
    cfg = GuiConfig(
        connectors=[
            ConnectorEntry(
                id="garmin",
                type="garmin",
                credential_service="Garmin Connect",
                credential_url="https://connect.garmin.com",
                credential_login="me@example.com",
            ),
            ConnectorEntry(
                id="strava",
                type="strava",
                credential_service="Strava",
                credential_url="https://www.strava.com/api/v3",
                client_id=12345,
            ),
            ConnectorEntry(id="local", type="local_folder", folder="/tmp/activities"),
        ],
        sync_groups=[
            SyncGroupEntry(
                id="s-to-g",
                sources=[GroupSourceEntry(id="strava", priority=1)],
                destinations=["garmin"],
            ),
        ],
        start="2025-01-01",
        end="2025-12-31",
        force=True,
        skip_wellness=True,
    )
    store.save_gui_config(cfg)
    loaded = store.load_gui_config()

    assert loaded.start == "2025-01-01"
    assert loaded.end == "2025-12-31"
    assert loaded.force is True
    assert loaded.skip_wellness is True
    assert len(loaded.connectors) == 3
    assert loaded.connectors[0].id == "garmin"
    assert loaded.connectors[1].client_id == 12345
    assert loaded.connectors[2].folder == "/tmp/activities"
    assert len(loaded.sync_groups) == 1
    sg = loaded.sync_groups[0]
    assert sg.id == "s-to-g"
    assert sg.sources[0].id == "strava"
    assert sg.sources[0].priority == 1
    assert sg.destinations == ["garmin"]


def test_save_gui_config_omits_empty_dates(store: ConfigStore) -> None:
    store.save_gui_config(GuiConfig())
    raw = json.loads((store.config_dir / "config.json").read_text())
    assert "start" not in raw
    assert "end" not in raw


def test_save_gui_config_includes_nonempty_dates(store: ConfigStore) -> None:
    store.save_gui_config(GuiConfig(start="2026-01-01", end="2026-06-30"))
    raw = json.loads((store.config_dir / "config.json").read_text())
    assert raw["start"] == "2026-01-01"
    assert raw["end"] == "2026-06-30"


# ---------------------------------------------------------------------------
# to_app_config conversion
# ---------------------------------------------------------------------------


def test_to_app_config_garmin_connector(store: ConfigStore) -> None:
    cfg = GuiConfig(
        connectors=[
            ConnectorEntry(
                id="g",
                type="garmin",
                credential_service="Garmin Connect",
                credential_url="https://connect.garmin.com",
                credential_login="user@example.com",
            )
        ],
        sync_groups=[
            SyncGroupEntry(
                id="grp",
                sources=[GroupSourceEntry(id="g", priority=1)],
                destinations=[],
            )
        ],
    )
    app_cfg = store.to_app_config(cfg)
    assert len(app_cfg.connectors) == 1
    garmin = app_cfg.connectors[0]
    assert isinstance(garmin, GarminConnectorConfig)
    assert garmin.id == "g"
    assert garmin.credential.login == "user@example.com"


def test_to_app_config_strava_connector(store: ConfigStore) -> None:
    cfg = GuiConfig(
        connectors=[
            ConnectorEntry(
                id="s",
                type="strava",
                credential_service="Strava",
                credential_url="https://www.strava.com/api/v3",
                client_id=99,
            )
        ],
        sync_groups=[
            SyncGroupEntry(
                id="grp",
                sources=[GroupSourceEntry(id="s", priority=1)],
                destinations=[],
            )
        ],
    )
    app_cfg = store.to_app_config(cfg)
    strava = app_cfg.connectors[0]
    assert isinstance(strava, StravaConnectorConfig)
    assert strava.client_id == 99


def test_to_app_config_local_connector(store: ConfigStore, tmp_path: Path) -> None:
    folder = str(tmp_path)
    cfg = GuiConfig(
        connectors=[ConnectorEntry(id="l", type="local_folder", folder=folder)],
        sync_groups=[
            SyncGroupEntry(
                id="grp",
                sources=[GroupSourceEntry(id="l", priority=1)],
                destinations=[],
            )
        ],
    )
    app_cfg = store.to_app_config(cfg)
    local = app_cfg.connectors[0]
    assert isinstance(local, LocalFolderConnectorConfig)
    assert local.folder == Path(folder).resolve()


def test_to_app_config_date_parsing(store: ConfigStore) -> None:
    cfg = GuiConfig(
        connectors=[ConnectorEntry(id="l", type="local_folder", folder="/tmp")],
        sync_groups=[
            SyncGroupEntry(
                id="grp",
                sources=[GroupSourceEntry(id="l", priority=1)],
                destinations=[],
            )
        ],
        start="2025-03-01",
        end="2025-09-30",
    )
    app_cfg = store.to_app_config(cfg)
    assert app_cfg.start == date(2025, 3, 1)
    assert app_cfg.end == date(2025, 9, 30)


def test_to_app_config_empty_dates_are_none(store: ConfigStore) -> None:
    cfg = GuiConfig(
        connectors=[ConnectorEntry(id="l", type="local_folder", folder="/tmp")],
        sync_groups=[
            SyncGroupEntry(
                id="grp",
                sources=[GroupSourceEntry(id="l", priority=1)],
                destinations=[],
            )
        ],
    )
    app_cfg = store.to_app_config(cfg)
    assert app_cfg.start is None
    assert app_cfg.end is None


def test_to_app_config_cache_dir_is_inside_config_dir(store: ConfigStore) -> None:
    cfg = GuiConfig()
    app_cfg = store.to_app_config(cfg)
    assert app_cfg.cache_dir == store.cache_dir


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def test_atomic_write(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    _atomic_write(path, {"key": "value"})
    assert path.exists()
    assert not path.with_suffix(".tmp").exists()
    assert json.loads(path.read_text())["key"] == "value"


def test_parse_connector_entry_garmin() -> None:
    raw = {
        "id": "g",
        "type": "garmin",
        "credential_service": "Garmin Connect",
        "credential_url": "https://connect.garmin.com",
        "credential_login": "me@example.com",
    }
    e = _parse_connector_entry(raw)
    assert e.id == "g"
    assert e.type == "garmin"
    assert e.credential_login == "me@example.com"


def test_parse_connector_entry_defaults_missing_keys() -> None:
    e = _parse_connector_entry({"id": "x", "type": "garmin"})
    assert e.credential_service == ""
    assert e.credential_url == ""
    assert e.credential_login == ""
    assert e.client_id == 0
    assert e.folder == ""


def test_parse_group_entry() -> None:
    raw = {
        "id": "g1",
        "sources": [{"id": "s", "priority": 2}],
        "destinations": ["d1", "d2"],
    }
    e = _parse_group_entry(raw)
    assert e.id == "g1"
    assert e.sources[0].priority == 2
    assert e.destinations == ["d1", "d2"]


def test_serialize_connector_garmin() -> None:
    c = ConnectorEntry(
        id="g",
        type="garmin",
        credential_service="Garmin Connect",
        credential_url="https://connect.garmin.com",
        credential_login="me@example.com",
    )
    d = _serialize_connector(c)
    assert d == {
        "id": "g",
        "type": "garmin",
        "credential_service": "Garmin Connect",
        "credential_url": "https://connect.garmin.com",
        "credential_login": "me@example.com",
    }


def test_serialize_connector_garmin_omits_empty_login() -> None:
    c = ConnectorEntry(
        id="g",
        type="garmin",
        credential_service="Garmin Connect",
        credential_url="https://connect.garmin.com",
    )
    d = _serialize_connector(c)
    assert "credential_login" not in d


def test_serialize_connector_strava() -> None:
    c = ConnectorEntry(
        id="s",
        type="strava",
        credential_service="Strava",
        credential_url="https://www.strava.com/api/v3",
        client_id=12345,
    )
    d = _serialize_connector(c)
    assert d["client_id"] == 12345
    assert "folder" not in d


def test_serialize_connector_local_folder() -> None:
    c = ConnectorEntry(id="l", type="local_folder", folder="/data")
    d = _serialize_connector(c)
    assert d["folder"] == "/data"
    assert "credential_service" not in d


def test_serialize_group() -> None:
    g = SyncGroupEntry(
        id="grp",
        sources=[GroupSourceEntry(id="s", priority=3)],
        destinations=["d"],
    )
    d = _serialize_group(g)
    assert d == {
        "id": "grp",
        "sources": [{"id": "s", "priority": 3}],
        "destinations": ["d"],
    }


def test_connector_to_app_unknown_type_raises() -> None:
    c = ConnectorEntry(id="x", type="unknown")
    with pytest.raises(ValueError, match="unknown connector type"):
        _connector_to_app(c)


def test_group_to_app() -> None:
    g = SyncGroupEntry(
        id="grp",
        sources=[GroupSourceEntry(id="s", priority=1)],
        destinations=["d"],
    )
    sg = _group_to_app(g)
    assert sg.id == "grp"
    assert sg.sources[0].id == "s"
    assert sg.destinations == ("d",)

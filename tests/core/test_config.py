from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.core.config import (
    ConfigError,
    GarminConnectorConfig,
    GroupSourceConfig,
    LocalFolderConnectorConfig,
    StravaConnectorConfig,
    SyncGroupConfig,
    load_config,
)
from app.credentials.base import CredentialRequest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GARMIN_CONNECTOR: dict = {
    "id": "garmin",
    "type": "garmin",
    "credential_service": "Garmin Connect",
    "credential_url": "https://connect.garmin.com",
    "credential_login": "user@example.com",
}

_STRAVA_CONNECTOR: dict = {
    "id": "strava",
    "type": "strava",
    "client_id": 99999,
    "credential_service": "Strava",
    "credential_url": "https://www.strava.com",
}

_LOCAL_CONNECTOR: dict = {
    "id": "local",
    "type": "local_folder",
    "folder": "./activities",
}

_SYNC_GROUP: dict = {
    "id": "garmin-to-local",
    "sources": [{"id": "garmin", "priority": 1}],
    "destinations": ["local"],
}


def _write(tmp_path: Path, data: object, name: str = "sync.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _minimal(extra: dict | None = None) -> dict:
    base: dict = {
        "cache_dir": "./cache",
        "connectors": [_GARMIN_CONNECTOR, _LOCAL_CONNECTOR],
        "sync_groups": [_SYNC_GROUP],
    }
    if extra:
        base.update(extra)
    return base


# ---------------------------------------------------------------------------
# Happy path - connector types
# ---------------------------------------------------------------------------


def test_garmin_connector(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, _minimal()))
    assert len(cfg.connectors) == 2
    c = cfg.connectors[0]
    assert isinstance(c, GarminConnectorConfig)
    assert c.id == "garmin"
    assert c.credential == CredentialRequest(
        service="Garmin Connect",
        url="https://connect.garmin.com",
        login="user@example.com",
    )


def test_strava_connector(tmp_path: Path) -> None:
    data = {**_minimal(), "connectors": [_STRAVA_CONNECTOR, _LOCAL_CONNECTOR]}
    data["sync_groups"] = [
        {
            "id": "g",
            "sources": [{"id": "strava", "priority": 1}],
            "destinations": ["local"],
        }
    ]
    cfg = load_config(_write(tmp_path, data))
    c = cfg.connectors[0]
    assert isinstance(c, StravaConnectorConfig)
    assert c.client_id == 99999
    assert c.credential == CredentialRequest(
        service="Strava", url="https://www.strava.com", login=None
    )


def test_local_folder_connector(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, _minimal()))
    c = cfg.connectors[1]
    assert isinstance(c, LocalFolderConnectorConfig)
    assert c.folder == tmp_path / "activities"


def test_all_connector_types_together(tmp_path: Path) -> None:
    data = {
        "cache_dir": "./cache",
        "connectors": [_GARMIN_CONNECTOR, _STRAVA_CONNECTOR, _LOCAL_CONNECTOR],
        "sync_groups": [
            {
                "id": "g1",
                "sources": [
                    {"id": "garmin", "priority": 1},
                    {"id": "strava", "priority": 2},
                ],
                "destinations": ["local"],
            }
        ],
    }
    cfg = load_config(_write(tmp_path, data))
    assert len(cfg.connectors) == 3


# ---------------------------------------------------------------------------
# Happy path - sync group parsing
# ---------------------------------------------------------------------------


def test_sync_group_parsed(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, _minimal()))
    assert len(cfg.sync_groups) == 1
    g = cfg.sync_groups[0]
    assert isinstance(g, SyncGroupConfig)
    assert g.id == "garmin-to-local"
    assert g.sources == (GroupSourceConfig(id="garmin", priority=1),)
    assert g.destinations == ("local",)


def test_multiple_groups_parsed(tmp_path: Path) -> None:
    data = {
        "cache_dir": "./cache",
        "connectors": [_GARMIN_CONNECTOR, _STRAVA_CONNECTOR, _LOCAL_CONNECTOR],
        "sync_groups": [
            {
                "id": "strava-to-garmin",
                "sources": [{"id": "strava", "priority": 1}],
                "destinations": ["garmin"],
            },
            {
                "id": "services-to-local",
                "sources": [
                    {"id": "garmin", "priority": 1},
                    {"id": "strava", "priority": 2},
                ],
                "destinations": ["local"],
            },
        ],
    }
    cfg = load_config(_write(tmp_path, data))
    assert len(cfg.sync_groups) == 2
    assert cfg.sync_groups[0].id == "strava-to-garmin"
    assert cfg.sync_groups[1].id == "services-to-local"


def test_connector_reuse_across_groups_allowed(tmp_path: Path) -> None:
    data = {
        "cache_dir": "./cache",
        "connectors": [_GARMIN_CONNECTOR, _STRAVA_CONNECTOR, _LOCAL_CONNECTOR],
        "sync_groups": [
            {
                "id": "g1",
                "sources": [{"id": "strava", "priority": 1}],
                "destinations": ["garmin"],
            },
            {
                "id": "g2",
                "sources": [{"id": "garmin", "priority": 1}],
                "destinations": ["local"],
            },
        ],
    }
    cfg = load_config(_write(tmp_path, data))
    assert len(cfg.sync_groups) == 2


def test_empty_destinations_in_group_allowed(tmp_path: Path) -> None:
    data = {**_minimal()}
    data["sync_groups"] = [
        {"id": "g", "sources": [{"id": "garmin", "priority": 1}], "destinations": []}
    ]
    cfg = load_config(_write(tmp_path, data))
    assert cfg.sync_groups[0].destinations == ()


# ---------------------------------------------------------------------------
# Happy path - AppConfig fields
# ---------------------------------------------------------------------------


def test_cache_dir_resolved(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, _minimal()))
    assert cfg.cache_dir == tmp_path / "cache"
    assert cfg.cache_dir.is_absolute()


def test_absolute_cache_dir_kept(tmp_path: Path) -> None:
    abs_cache = str(tmp_path / "abs_cache")
    cfg = load_config(_write(tmp_path, {**_minimal(), "cache_dir": abs_cache}))
    assert cfg.cache_dir == Path(abs_cache)


def test_start_end_parsed(tmp_path: Path) -> None:
    cfg = load_config(
        _write(tmp_path, {**_minimal(), "start": "2025-01-01", "end": "2025-12-31"})
    )
    assert cfg.start == date(2025, 1, 1)
    assert cfg.end == date(2025, 12, 31)


def test_start_equals_end_allowed(tmp_path: Path) -> None:
    cfg = load_config(
        _write(tmp_path, {**_minimal(), "start": "2025-06-01", "end": "2025-06-01"})
    )
    assert cfg.start == cfg.end


def test_start_only(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "start": "2025-01-01"}))
    assert cfg.start == date(2025, 1, 1)
    assert cfg.end is None


def test_end_only(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "end": "2025-12-31"}))
    assert cfg.start is None
    assert cfg.end == date(2025, 12, 31)


def test_no_dates(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, _minimal()))
    assert cfg.start is None
    assert cfg.end is None


def test_credential_login_optional(tmp_path: Path) -> None:
    connector = {k: v for k, v in _GARMIN_CONNECTOR.items() if k != "credential_login"}
    data = {**_minimal(), "connectors": [connector, _LOCAL_CONNECTOR]}
    cfg = load_config(_write(tmp_path, data))
    assert cfg.connectors[0].credential.login is None  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def test_relative_folder_resolved_against_config_dir(tmp_path: Path) -> None:
    subdir = tmp_path / "configs"
    subdir.mkdir()
    p = subdir / "sync.json"
    local = {**_LOCAL_CONNECTOR, "folder": "../acts"}
    p.write_text(
        json.dumps(
            {
                "cache_dir": "../cache",
                "connectors": [_GARMIN_CONNECTOR, local],
                "sync_groups": [_SYNC_GROUP],
            }
        ),
        encoding="utf-8",
    )
    cfg = load_config(p)
    assert cfg.cache_dir == tmp_path / "cache"
    assert cfg.connectors[1].folder == tmp_path / "acts"  # type: ignore[union-attr]


def test_tilde_expansion_in_folder(tmp_path: Path) -> None:
    local = {**_LOCAL_CONNECTOR, "folder": "~/my_activities"}
    data = {**_minimal(), "connectors": [_GARMIN_CONNECTOR, local]}
    cfg = load_config(_write(tmp_path, data))
    folder = cfg.connectors[1].folder  # type: ignore[union-attr]
    assert folder.is_absolute()
    assert not str(folder).startswith("~")


def test_load_config_accepts_relative_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "sync.json").write_text(json.dumps(_minimal()), encoding="utf-8")
    cfg = load_config(Path("sync.json"))
    assert cfg.cache_dir.is_absolute()


# ---------------------------------------------------------------------------
# ConfigError - file / JSON errors
# ---------------------------------------------------------------------------


def test_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="config file not found"):
        load_config(tmp_path / "missing.json")


def test_invalid_json(tmp_path: Path) -> None:
    p = tmp_path / "sync.json"
    p.write_text("{bad json", encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid JSON"):
        load_config(p)


def test_root_not_object(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="must be a JSON object"):
        load_config(_write(tmp_path, [1, 2, 3]))


# ---------------------------------------------------------------------------
# ConfigError - root-level field validation
# ---------------------------------------------------------------------------


def test_missing_cache_dir(tmp_path: Path) -> None:
    data = {
        "connectors": [_GARMIN_CONNECTOR, _LOCAL_CONNECTOR],
        "sync_groups": [_SYNC_GROUP],
    }
    with pytest.raises(ConfigError, match="'cache_dir'"):
        load_config(_write(tmp_path, data))


def test_connectors_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'connectors' must not be empty"):
        load_config(
            _write(tmp_path, {"cache_dir": "./cache", "sync_groups": [_SYNC_GROUP]})
        )


def test_connectors_not_a_list(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'connectors' must be a list"):
        load_config(_write(tmp_path, {"cache_dir": "./cache", "connectors": {}}))


def test_connectors_empty_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'connectors' must not be empty"):
        load_config(
            _write(
                tmp_path, {"cache_dir": "./cache", "connectors": [], "sync_groups": []}
            )
        )


def test_sync_groups_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'sync_groups' must not be empty"):
        load_config(
            _write(
                tmp_path,
                {
                    "cache_dir": "./cache",
                    "connectors": [_GARMIN_CONNECTOR, _LOCAL_CONNECTOR],
                },
            )
        )


def test_sync_groups_not_a_list(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'sync_groups' must be a list"):
        load_config(
            _write(
                tmp_path,
                {
                    "cache_dir": "./cache",
                    "connectors": [_GARMIN_CONNECTOR, _LOCAL_CONNECTOR],
                    "sync_groups": "bad",
                },
            )
        )


def test_sync_groups_empty_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'sync_groups' must not be empty"):
        load_config(
            _write(
                tmp_path,
                {
                    "cache_dir": "./cache",
                    "connectors": [_GARMIN_CONNECTOR, _LOCAL_CONNECTOR],
                    "sync_groups": [],
                },
            )
        )


def test_start_after_end(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"'start'.*must not be after.*'end'"):
        load_config(
            _write(tmp_path, {**_minimal(), "start": "2025-06-01", "end": "2025-01-01"})
        )


def test_invalid_date_format(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not a valid date"):
        load_config(_write(tmp_path, {**_minimal(), "start": "01/01/2025"}))


def test_date_not_a_string(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="must be a date string"):
        load_config(_write(tmp_path, {**_minimal(), "end": 20250101}))


# ---------------------------------------------------------------------------
# ConfigError - connector validation
# ---------------------------------------------------------------------------


def test_connector_not_a_dict(tmp_path: Path) -> None:
    data = {**_minimal(), "connectors": ["not-a-dict", _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match=r"connectors\[0\].*must be an object"):
        load_config(_write(tmp_path, data))


def test_connector_missing_id(tmp_path: Path) -> None:
    c = {k: v for k, v in _GARMIN_CONNECTOR.items() if k != "id"}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="'id'"):
        load_config(_write(tmp_path, data))


def test_connector_empty_id(tmp_path: Path) -> None:
    c = {**_GARMIN_CONNECTOR, "id": ""}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="'id' must not be empty"):
        load_config(_write(tmp_path, data))


def test_connector_missing_type(tmp_path: Path) -> None:
    c = {k: v for k, v in _GARMIN_CONNECTOR.items() if k != "type"}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="'type'"):
        load_config(_write(tmp_path, data))


def test_connector_unknown_type(tmp_path: Path) -> None:
    c = {**_GARMIN_CONNECTOR, "type": "polar"}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="unknown connector type"):
        load_config(_write(tmp_path, data))


def test_connector_missing_credential_service(tmp_path: Path) -> None:
    c = {k: v for k, v in _GARMIN_CONNECTOR.items() if k != "credential_service"}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="'credential_service'"):
        load_config(_write(tmp_path, data))


def test_connector_missing_credential_url(tmp_path: Path) -> None:
    c = {k: v for k, v in _GARMIN_CONNECTOR.items() if k != "credential_url"}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="'credential_url'"):
        load_config(_write(tmp_path, data))


def test_strava_connector_missing_client_id(tmp_path: Path) -> None:
    c = {k: v for k, v in _STRAVA_CONNECTOR.items() if k != "client_id"}
    data = {
        "cache_dir": "./cache",
        "connectors": [c, _LOCAL_CONNECTOR],
        "sync_groups": [
            {
                "id": "g",
                "sources": [{"id": "strava", "priority": 1}],
                "destinations": ["local"],
            }
        ],
    }
    with pytest.raises(ConfigError, match="'client_id'"):
        load_config(_write(tmp_path, data))


def test_connector_non_string_field(tmp_path: Path) -> None:
    c = {**_GARMIN_CONNECTOR, "credential_service": 42}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="must be a string"):
        load_config(_write(tmp_path, data))


def test_connector_non_string_credential_login(tmp_path: Path) -> None:
    c = {**_GARMIN_CONNECTOR, "credential_login": 123}
    data = {**_minimal(), "connectors": [c, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="'credential_login' must be a string"):
        load_config(_write(tmp_path, data))


def test_local_connector_missing_folder(tmp_path: Path) -> None:
    c = {k: v for k, v in _LOCAL_CONNECTOR.items() if k != "folder"}
    data = {**_minimal(), "connectors": [_GARMIN_CONNECTOR, c]}
    with pytest.raises(ConfigError, match="'folder'"):
        load_config(_write(tmp_path, data))


def test_duplicate_connector_ids(tmp_path: Path) -> None:
    c2 = {**_GARMIN_CONNECTOR, "id": "garmin"}
    data = {**_minimal(), "connectors": [_GARMIN_CONNECTOR, c2, _LOCAL_CONNECTOR]}
    with pytest.raises(ConfigError, match="duplicate connector id"):
        load_config(_write(tmp_path, data))


def test_strava_duplicate_credential_ref_rejected(tmp_path: Path) -> None:
    strava2 = {**_STRAVA_CONNECTOR, "id": "strava2"}
    data = {
        "cache_dir": "./cache",
        "connectors": [_STRAVA_CONNECTOR, strava2, _LOCAL_CONNECTOR],
        "sync_groups": [
            {
                "id": "g",
                "sources": [{"id": "strava", "priority": 1}],
                "destinations": ["local"],
            }
        ],
    }
    with pytest.raises(ConfigError, match="duplicate Strava credential ref"):
        load_config(_write(tmp_path, data))


def test_strava_different_credential_refs_allowed(tmp_path: Path) -> None:
    strava2 = {
        **_STRAVA_CONNECTOR,
        "id": "strava2",
        "credential_url": "https://other.strava.com",
    }
    data = {
        "cache_dir": "./cache",
        "connectors": [_STRAVA_CONNECTOR, strava2, _LOCAL_CONNECTOR],
        "sync_groups": [
            {
                "id": "g",
                "sources": [
                    {"id": "strava", "priority": 1},
                    {"id": "strava2", "priority": 2},
                ],
                "destinations": ["local"],
            }
        ],
    }
    cfg = load_config(_write(tmp_path, data))
    assert len(cfg.connectors) == 3


# ---------------------------------------------------------------------------
# ConfigError - sync group validation
# ---------------------------------------------------------------------------


def test_group_not_a_dict(tmp_path: Path) -> None:
    data = {**_minimal(), "sync_groups": ["not-a-dict"]}
    with pytest.raises(ConfigError, match=r"sync_groups\[0\].*must be an object"):
        load_config(_write(tmp_path, data))


def test_group_missing_id(tmp_path: Path) -> None:
    g = {k: v for k, v in _SYNC_GROUP.items() if k != "id"}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'id'"):
        load_config(_write(tmp_path, data))


def test_group_empty_id(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "id": ""}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'id' must not be empty"):
        load_config(_write(tmp_path, data))


def test_group_sources_not_a_list(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": "bad"}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'sources' must be a list"):
        load_config(_write(tmp_path, data))


def test_group_sources_empty_raises(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": []}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'sources' must not be empty"):
        load_config(_write(tmp_path, data))


def test_group_source_not_a_dict(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": ["not-a-dict"]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="must be an object"):
        load_config(_write(tmp_path, data))


def test_group_source_unknown_connector_id(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": [{"id": "unknown", "priority": 1}]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="unknown connector id"):
        load_config(_write(tmp_path, data))


def test_group_source_empty_id(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": [{"id": "", "priority": 1}]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'id' must not be empty"):
        load_config(_write(tmp_path, data))


def test_group_source_missing_priority(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": [{"id": "garmin"}]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'priority'"):
        load_config(_write(tmp_path, data))


def test_group_source_bool_priority_rejected(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "sources": [{"id": "garmin", "priority": True}]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="must be an integer"):
        load_config(_write(tmp_path, data))


def test_duplicate_source_in_group(tmp_path: Path) -> None:
    g = {
        **_SYNC_GROUP,
        "sources": [{"id": "garmin", "priority": 1}, {"id": "garmin", "priority": 2}],
    }
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="duplicate source id"):
        load_config(_write(tmp_path, data))


def test_group_destinations_not_a_list(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "destinations": "bad"}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="'destinations' must be a list"):
        load_config(_write(tmp_path, data))


def test_group_destination_not_a_string(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "destinations": [42]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="must be a string"):
        load_config(_write(tmp_path, data))


def test_group_destination_empty_string_rejected(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "destinations": [""]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="must not be empty"):
        load_config(_write(tmp_path, data))


def test_group_destination_unknown_connector_id(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "destinations": ["unknown"]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="unknown connector id"):
        load_config(_write(tmp_path, data))


def test_duplicate_destination_in_group(tmp_path: Path) -> None:
    g = {**_SYNC_GROUP, "destinations": ["local", "local"]}
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="duplicate destination id"):
        load_config(_write(tmp_path, data))


def test_connector_both_source_and_destination_in_group_rejected(
    tmp_path: Path,
) -> None:
    g = {
        "id": "g",
        "sources": [{"id": "garmin", "priority": 1}],
        "destinations": ["garmin"],
    }
    data = {**_minimal(), "sync_groups": [g]}
    with pytest.raises(ConfigError, match="both source and destination"):
        load_config(_write(tmp_path, data))


def test_duplicate_sync_group_ids(tmp_path: Path) -> None:
    data = {**_minimal(), "sync_groups": [_SYNC_GROUP, _SYNC_GROUP]}
    with pytest.raises(ConfigError, match="duplicate sync group id"):
        load_config(_write(tmp_path, data))

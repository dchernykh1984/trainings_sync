from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.core.config import (
    ConfigError,
    GarminDestinationConfig,
    GarminSourceConfig,
    LocalFolderDestinationConfig,
    LocalFolderSourceConfig,
    StravaDestinationConfig,
    StravaSourceConfig,
    load_config,
)
from app.credentials.base import CredentialRequest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GARMIN_SOURCE: dict = {
    "id": "garmin-main",
    "type": "garmin",
    "priority": 1,
    "credential_service": "Garmin Connect",
    "credential_url": "https://connect.garmin.com",
    "credential_login": "user@example.com",
}

_STRAVA_SOURCE: dict = {
    "id": "strava-main",
    "type": "strava",
    "priority": 2,
    "client_id": 99999,
    "credential_service": "Strava",
    "credential_url": "https://www.strava.com",
}

_LOCAL_SOURCE: dict = {
    "id": "local-main",
    "type": "local_folder",
    "priority": 3,
    "folder": "./activities",
}

_GARMIN_DEST: dict = {
    "id": "garmin-upload",
    "type": "garmin",
    "credential_service": "Garmin Connect",
    "credential_url": "https://connect.garmin.com",
}

_STRAVA_DEST: dict = {
    "id": "strava-upload",
    "type": "strava",
    "client_id": 99999,
    "credential_service": "Strava",
    "credential_url": "https://www.strava.com",
}

_LOCAL_DEST: dict = {
    "id": "local-backup",
    "type": "local_folder",
    "folder": "./backup",
}


def _write(tmp_path: Path, data: object, name: str = "sync.json") -> Path:
    p = tmp_path / name
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _minimal(extra: dict | None = None) -> dict:
    base: dict = {"cache_dir": "./cache", "sources": [_GARMIN_SOURCE]}
    if extra:
        base.update(extra)
    return base


# ---------------------------------------------------------------------------
# Happy path — source types
# ---------------------------------------------------------------------------


def test_garmin_source(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, _minimal()))
    assert len(cfg.sources) == 1
    src = cfg.sources[0]
    assert isinstance(src, GarminSourceConfig)
    assert src.id == "garmin-main"
    assert src.priority == 1
    assert src.credential == CredentialRequest(
        service="Garmin Connect",
        url="https://connect.garmin.com",
        login="user@example.com",
    )


def test_strava_source(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "sources": [_STRAVA_SOURCE]}))
    src = cfg.sources[0]
    assert isinstance(src, StravaSourceConfig)
    assert src.client_id == 99999
    assert src.credential == CredentialRequest(
        service="Strava", url="https://www.strava.com", login=None
    )


def test_local_folder_source(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "sources": [_LOCAL_SOURCE]}))
    src = cfg.sources[0]
    assert isinstance(src, LocalFolderSourceConfig)
    assert src.folder == tmp_path / "activities"


# ---------------------------------------------------------------------------
# Happy path — destination types
# ---------------------------------------------------------------------------


def test_garmin_destination(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "destinations": [_GARMIN_DEST]}))
    dest = cfg.destinations[0]
    assert isinstance(dest, GarminDestinationConfig)
    assert dest.id == "garmin-upload"
    assert dest.credential.service == "Garmin Connect"


def test_strava_destination(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "destinations": [_STRAVA_DEST]}))
    dest = cfg.destinations[0]
    assert isinstance(dest, StravaDestinationConfig)
    assert dest.client_id == 99999


def test_local_folder_destination(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "destinations": [_LOCAL_DEST]}))
    dest = cfg.destinations[0]
    assert isinstance(dest, LocalFolderDestinationConfig)
    assert dest.folder == tmp_path / "backup"


def test_all_types_together(tmp_path: Path) -> None:
    data = {
        "cache_dir": "./cache",
        "sources": [_GARMIN_SOURCE, _STRAVA_SOURCE, _LOCAL_SOURCE],
        "destinations": [_GARMIN_DEST, _STRAVA_DEST, _LOCAL_DEST],
    }
    cfg = load_config(_write(tmp_path, data))
    assert len(cfg.sources) == 3
    assert len(cfg.destinations) == 3


# ---------------------------------------------------------------------------
# Happy path — AppConfig fields
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


def test_empty_destinations_allowed(tmp_path: Path) -> None:
    cfg = load_config(_write(tmp_path, {**_minimal(), "destinations": []}))
    assert cfg.destinations == ()


def test_credential_login_optional(tmp_path: Path) -> None:
    src = {**_GARMIN_SOURCE}
    del src["credential_login"]
    cfg = load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))
    assert cfg.sources[0].credential.login is None  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def test_relative_folder_resolved_against_config_dir(tmp_path: Path) -> None:
    subdir = tmp_path / "configs"
    subdir.mkdir()
    p = subdir / "sync.json"
    p.write_text(
        json.dumps(
            {
                "cache_dir": "../cache",
                "sources": [{**_LOCAL_SOURCE, "folder": "../acts"}],
            }
        ),
        encoding="utf-8",
    )
    cfg = load_config(p)
    assert cfg.cache_dir == tmp_path / "cache"
    assert cfg.sources[0].folder == tmp_path / "acts"  # type: ignore[union-attr]


def test_tilde_expansion_in_folder(tmp_path: Path) -> None:
    src = {**_LOCAL_SOURCE, "folder": "~/my_activities"}
    cfg = load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))
    folder = cfg.sources[0].folder  # type: ignore[union-attr]
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
# ConfigError — file / JSON errors
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
# ConfigError — root-level field validation
# ---------------------------------------------------------------------------


def test_missing_cache_dir(tmp_path: Path) -> None:
    data = {"sources": [_GARMIN_SOURCE]}
    with pytest.raises(ConfigError, match="'cache_dir'"):
        load_config(_write(tmp_path, data))


def test_sources_missing_defaults_to_empty_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'sources' must not be empty"):
        load_config(_write(tmp_path, {"cache_dir": "./cache"}))


def test_sources_not_a_list(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'sources' must be a list"):
        load_config(_write(tmp_path, {"cache_dir": "./cache", "sources": {}}))


def test_empty_sources(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="must not be empty"):
        load_config(_write(tmp_path, {"cache_dir": "./cache", "sources": []}))


def test_destinations_not_a_list(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="'destinations' must be a list"):
        load_config(_write(tmp_path, {**_minimal(), "destinations": "bad"}))


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
# ConfigError — source validation
# ---------------------------------------------------------------------------


def test_source_not_a_dict(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"sources\[0\].*must be an object"):
        load_config(_write(tmp_path, {**_minimal(), "sources": ["not-a-dict"]}))


def test_source_missing_id(tmp_path: Path) -> None:
    src = {k: v for k, v in _GARMIN_SOURCE.items() if k != "id"}
    with pytest.raises(ConfigError, match="'id'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_empty_id(tmp_path: Path) -> None:
    src = {**_GARMIN_SOURCE, "id": ""}
    with pytest.raises(ConfigError, match="'id' must not be empty"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_missing_type(tmp_path: Path) -> None:
    src = {k: v for k, v in _GARMIN_SOURCE.items() if k != "type"}
    with pytest.raises(ConfigError, match="'type'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_unknown_type(tmp_path: Path) -> None:
    src = {**_GARMIN_SOURCE, "type": "polar"}
    with pytest.raises(ConfigError, match="unknown source type"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_missing_priority(tmp_path: Path) -> None:
    src = {k: v for k, v in _GARMIN_SOURCE.items() if k != "priority"}
    with pytest.raises(ConfigError, match="'priority'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_bool_priority_rejected(tmp_path: Path) -> None:
    src = {**_GARMIN_SOURCE, "priority": True}
    with pytest.raises(ConfigError, match="must be an integer"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_missing_credential_service(tmp_path: Path) -> None:
    src = {k: v for k, v in _GARMIN_SOURCE.items() if k != "credential_service"}
    with pytest.raises(ConfigError, match="'credential_service'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_missing_credential_url(tmp_path: Path) -> None:
    src = {k: v for k, v in _GARMIN_SOURCE.items() if k != "credential_url"}
    with pytest.raises(ConfigError, match="'credential_url'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_strava_source_missing_client_id(tmp_path: Path) -> None:
    src = {k: v for k, v in _STRAVA_SOURCE.items() if k != "client_id"}
    with pytest.raises(ConfigError, match="'client_id'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_non_string_field(tmp_path: Path) -> None:
    src = {**_GARMIN_SOURCE, "credential_service": 42}
    with pytest.raises(ConfigError, match="must be a string"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_source_non_string_credential_login(tmp_path: Path) -> None:
    src = {**_GARMIN_SOURCE, "credential_login": 123}
    with pytest.raises(ConfigError, match="'credential_login' must be a string"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_local_source_missing_folder(tmp_path: Path) -> None:
    src = {k: v for k, v in _LOCAL_SOURCE.items() if k != "folder"}
    with pytest.raises(ConfigError, match="'folder'"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [src]}))


def test_duplicate_source_ids(tmp_path: Path) -> None:
    src2 = {**_GARMIN_SOURCE, "id": "garmin-main", "priority": 5}
    with pytest.raises(ConfigError, match="duplicate source id"):
        load_config(_write(tmp_path, {**_minimal(), "sources": [_GARMIN_SOURCE, src2]}))


# ---------------------------------------------------------------------------
# ConfigError — destination validation
# ---------------------------------------------------------------------------


def test_destination_not_a_dict(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"destinations\[0\].*must be an object"):
        load_config(_write(tmp_path, {**_minimal(), "destinations": [42]}))


def test_destination_missing_id(tmp_path: Path) -> None:
    dest = {k: v for k, v in _GARMIN_DEST.items() if k != "id"}
    with pytest.raises(ConfigError, match="'id'"):
        load_config(_write(tmp_path, {**_minimal(), "destinations": [dest]}))


def test_destination_empty_id(tmp_path: Path) -> None:
    dest = {**_GARMIN_DEST, "id": ""}
    with pytest.raises(ConfigError, match="'id' must not be empty"):
        load_config(_write(tmp_path, {**_minimal(), "destinations": [dest]}))


def test_destination_unknown_type(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown destination type"):
        load_config(
            _write(
                tmp_path,
                {**_minimal(), "destinations": [{**_GARMIN_DEST, "type": "polar"}]},
            )
        )


def test_strava_destination_missing_client_id(tmp_path: Path) -> None:
    dest = {k: v for k, v in _STRAVA_DEST.items() if k != "client_id"}
    with pytest.raises(ConfigError, match="'client_id'"):
        load_config(_write(tmp_path, {**_minimal(), "destinations": [dest]}))


def test_local_destination_missing_folder(tmp_path: Path) -> None:
    dest = {k: v for k, v in _LOCAL_DEST.items() if k != "folder"}
    with pytest.raises(ConfigError, match="'folder'"):
        load_config(_write(tmp_path, {**_minimal(), "destinations": [dest]}))


def test_duplicate_destination_ids(tmp_path: Path) -> None:
    dest2 = {**_LOCAL_DEST, "id": "local-backup"}
    with pytest.raises(ConfigError, match="duplicate destination id"):
        load_config(
            _write(tmp_path, {**_minimal(), "destinations": [_LOCAL_DEST, dest2]})
        )

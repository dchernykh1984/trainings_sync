from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.credentials.base import CredentialRequest


@dataclass(frozen=True)
class GarminSourceConfig:
    id: str
    priority: int
    credential: CredentialRequest


@dataclass(frozen=True)
class StravaSourceConfig:
    id: str
    priority: int
    client_id: int
    # get_credentials -> Credentials(login=client_secret, password=refresh_token)
    credential: CredentialRequest


@dataclass(frozen=True)
class LocalFolderSourceConfig:
    id: str
    priority: int
    folder: Path


SourceConfig = GarminSourceConfig | StravaSourceConfig | LocalFolderSourceConfig


@dataclass(frozen=True)
class GarminDestinationConfig:
    id: str
    credential: CredentialRequest


@dataclass(frozen=True)
class StravaDestinationConfig:
    id: str
    client_id: int
    # get_credentials -> Credentials(login=client_secret, password=refresh_token)
    credential: CredentialRequest


@dataclass(frozen=True)
class LocalFolderDestinationConfig:
    id: str
    folder: Path


DestinationConfig = (
    GarminDestinationConfig | StravaDestinationConfig | LocalFolderDestinationConfig
)


@dataclass(frozen=True)
class AppConfig:
    cache_dir: Path
    sources: tuple[SourceConfig, ...]
    destinations: tuple[DestinationConfig, ...]
    start: date | None = None
    end: date | None = None


class ConfigError(ValueError):
    pass


def _require_str(d: dict, key: str, where: str) -> str:
    if key not in d:
        raise ConfigError(f"{where}: missing required field {key!r}")
    value = d[key]
    if not isinstance(value, str):
        raise ConfigError(f"{where}: field {key!r} must be a string")
    return value


def _require_int(d: dict, key: str, where: str) -> int:
    if key not in d:
        raise ConfigError(f"{where}: missing required field {key!r}")
    value = d[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(f"{where}: field {key!r} must be an integer")
    return value


def _parse_credential(d: dict, where: str) -> CredentialRequest:
    service = _require_str(d, "credential_service", where)
    url = _require_str(d, "credential_url", where)
    login = d.get("credential_login")
    if login is not None and not isinstance(login, str):
        raise ConfigError(f"{where}: field 'credential_login' must be a string")
    return CredentialRequest(service=service, url=url, login=login)


def _resolve_path(value: str, base_dir: Path) -> Path:
    p = Path(value).expanduser()
    result = p if p.is_absolute() else base_dir / p
    return result.resolve()


def _parse_date_optional(d: dict, key: str, where: str) -> date | None:
    value = d.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{where}: field {key!r} must be a date string (YYYY-MM-DD)")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ConfigError(f"{where}: field {key!r} is not a valid date: {exc}") from exc


def _parse_source(raw: object, index: int, base_dir: Path) -> SourceConfig:
    where = f"sources[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: must be an object")

    source_id = _require_str(raw, "id", where)
    if not source_id:
        raise ConfigError(f"{where}: 'id' must not be empty")

    source_type = _require_str(raw, "type", where)
    priority = _require_int(raw, "priority", where)

    if source_type == "garmin":
        return GarminSourceConfig(
            id=source_id,
            priority=priority,
            credential=_parse_credential(raw, where),
        )
    if source_type == "strava":
        return StravaSourceConfig(
            id=source_id,
            priority=priority,
            client_id=_require_int(raw, "client_id", where),
            credential=_parse_credential(raw, where),
        )
    if source_type == "local_folder":
        folder_str = _require_str(raw, "folder", where)
        return LocalFolderSourceConfig(
            id=source_id,
            priority=priority,
            folder=_resolve_path(folder_str, base_dir),
        )
    raise ConfigError(f"{where}: unknown source type {source_type!r}")


def _parse_destination(raw: object, index: int, base_dir: Path) -> DestinationConfig:
    where = f"destinations[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: must be an object")

    dest_id = _require_str(raw, "id", where)
    if not dest_id:
        raise ConfigError(f"{where}: 'id' must not be empty")

    dest_type = _require_str(raw, "type", where)

    if dest_type == "garmin":
        return GarminDestinationConfig(
            id=dest_id,
            credential=_parse_credential(raw, where),
        )
    if dest_type == "strava":
        return StravaDestinationConfig(
            id=dest_id,
            client_id=_require_int(raw, "client_id", where),
            credential=_parse_credential(raw, where),
        )
    if dest_type == "local_folder":
        folder_str = _require_str(raw, "folder", where)
        return LocalFolderDestinationConfig(
            id=dest_id,
            folder=_resolve_path(folder_str, base_dir),
        )
    raise ConfigError(f"{where}: unknown destination type {dest_type!r}")


def _check_unique_ids(ids: list[str], label: str) -> None:
    seen: set[str] = set()
    for sid in ids:
        if sid in seen:
            raise ConfigError(f"duplicate {label} id: {sid!r}")
        seen.add(sid)


def load_config(path: Path) -> AppConfig:
    path = path.expanduser().resolve()

    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc

    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError("config root must be a JSON object")

    base_dir = path.parent
    cache_dir = _resolve_path(_require_str(raw, "cache_dir", "root"), base_dir)

    raw_sources = raw.get("sources", [])
    if not isinstance(raw_sources, list):
        raise ConfigError("root: 'sources' must be a list")
    if not raw_sources:
        raise ConfigError("root: 'sources' must not be empty")

    sources = tuple(_parse_source(s, i, base_dir) for i, s in enumerate(raw_sources))
    _check_unique_ids([s.id for s in sources], "source")

    raw_dests = raw.get("destinations", [])
    if not isinstance(raw_dests, list):
        raise ConfigError("root: 'destinations' must be a list")

    destinations = tuple(
        _parse_destination(d, i, base_dir) for i, d in enumerate(raw_dests)
    )
    _check_unique_ids([d.id for d in destinations], "destination")

    start = _parse_date_optional(raw, "start", "root")
    end = _parse_date_optional(raw, "end", "root")

    if start is not None and end is not None and start > end:
        raise ConfigError(f"root: 'start' ({start}) must not be after 'end' ({end})")

    return AppConfig(
        cache_dir=cache_dir,
        sources=sources,
        destinations=destinations,
        start=start,
        end=end,
    )

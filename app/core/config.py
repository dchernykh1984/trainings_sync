from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.credentials.base import CredentialRequest


@dataclass(frozen=True)
class GarminConnectorConfig:
    id: str
    credential: CredentialRequest


@dataclass(frozen=True)
class StravaConnectorConfig:
    id: str
    client_id: int
    credential: CredentialRequest


@dataclass(frozen=True)
class LocalFolderConnectorConfig:
    id: str
    folder: Path


ConnectorConfig = (
    GarminConnectorConfig | StravaConnectorConfig | LocalFolderConnectorConfig
)


@dataclass(frozen=True)
class GroupSourceConfig:
    id: str
    priority: int


@dataclass(frozen=True)
class SyncGroupConfig:
    id: str
    sources: tuple[GroupSourceConfig, ...]
    destinations: tuple[str, ...]


@dataclass(frozen=True)
class AppConfig:
    cache_dir: Path
    connectors: tuple[ConnectorConfig, ...]
    sync_groups: tuple[SyncGroupConfig, ...]
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


def _parse_connector(raw: object, index: int, base_dir: Path) -> ConnectorConfig:
    where = f"connectors[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: must be an object")

    connector_id = _require_str(raw, "id", where)
    if not connector_id:
        raise ConfigError(f"{where}: 'id' must not be empty")

    connector_type = _require_str(raw, "type", where)

    if connector_type == "garmin":
        return GarminConnectorConfig(
            id=connector_id,
            credential=_parse_credential(raw, where),
        )
    if connector_type == "strava":
        return StravaConnectorConfig(
            id=connector_id,
            client_id=_require_int(raw, "client_id", where),
            credential=_parse_credential(raw, where),
        )
    if connector_type == "local_folder":
        folder_str = _require_str(raw, "folder", where)
        return LocalFolderConnectorConfig(
            id=connector_id,
            folder=_resolve_path(folder_str, base_dir),
        )
    raise ConfigError(f"{where}: unknown connector type {connector_type!r}")


def _parse_group_sources(
    raw_sources: object, where: str, connector_ids: set[str]
) -> list[GroupSourceConfig]:
    if not isinstance(raw_sources, list):
        raise ConfigError(f"{where}: 'sources' must be a list")
    if not raw_sources:
        raise ConfigError(f"{where}: 'sources' must not be empty")
    sources: list[GroupSourceConfig] = []
    seen: set[str] = set()
    for i, raw_src in enumerate(raw_sources):
        src_where = f"{where}.sources[{i}]"
        if not isinstance(raw_src, dict):
            raise ConfigError(f"{src_where}: must be an object")
        src_id = _require_str(raw_src, "id", src_where)
        if not src_id:
            raise ConfigError(f"{src_where}: 'id' must not be empty")
        if src_id not in connector_ids:
            raise ConfigError(f"{src_where}: unknown connector id {src_id!r}")
        if src_id in seen:
            raise ConfigError(f"{where}: duplicate source id {src_id!r}")
        seen.add(src_id)
        sources.append(
            GroupSourceConfig(
                id=src_id, priority=_require_int(raw_src, "priority", src_where)
            )
        )
    return sources


def _parse_group_destinations(
    raw_destinations: object, where: str, connector_ids: set[str]
) -> list[str]:
    if not isinstance(raw_destinations, list):
        raise ConfigError(f"{where}: 'destinations' must be a list")
    destinations: list[str] = []
    seen: set[str] = set()
    for i, raw_dest in enumerate(raw_destinations):
        dest_where = f"{where}.destinations[{i}]"
        if not isinstance(raw_dest, str):
            raise ConfigError(f"{dest_where}: must be a string")
        if not raw_dest:
            raise ConfigError(f"{dest_where}: must not be empty")
        if raw_dest not in connector_ids:
            raise ConfigError(f"{dest_where}: unknown connector id {raw_dest!r}")
        if raw_dest in seen:
            raise ConfigError(f"{where}: duplicate destination id {raw_dest!r}")
        seen.add(raw_dest)
        destinations.append(raw_dest)
    return destinations


def _parse_group(raw: object, index: int, connector_ids: set[str]) -> SyncGroupConfig:
    where = f"sync_groups[{index}]"
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: must be an object")

    group_id = _require_str(raw, "id", where)
    if not group_id:
        raise ConfigError(f"{where}: 'id' must not be empty")

    sources = _parse_group_sources(raw.get("sources", []), where, connector_ids)
    source_ids = {s.id for s in sources}
    destinations = _parse_group_destinations(
        raw.get("destinations", []), where, connector_ids
    )
    dest_ids = set(destinations)

    conflicts = source_ids & dest_ids
    if conflicts:
        names = ", ".join(sorted(conflicts))
        raise ConfigError(
            f"{where}: connector(s) appear as both source and destination: {names}"
        )

    return SyncGroupConfig(
        id=group_id,
        sources=tuple(sources),
        destinations=tuple(destinations),
    )


def _check_unique_ids(ids: list[str], label: str) -> None:
    seen: set[str] = set()
    for sid in ids:
        if sid in seen:
            raise ConfigError(f"duplicate {label} id: {sid!r}")
        seen.add(sid)


def _check_strava_credential_uniqueness(
    connectors: tuple[ConnectorConfig, ...],
) -> None:
    seen: set[CredentialRequest] = set()
    for c in connectors:
        if isinstance(c, StravaConnectorConfig):
            if c.credential in seen:
                raise ConfigError(
                    f"connector {c.id!r}: duplicate Strava credential ref -"
                    " each Strava account must appear at most once"
                    " (sharing a refresh token causes rotation races)"
                )
            seen.add(c.credential)


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

    raw_connectors = raw.get("connectors", [])
    if not isinstance(raw_connectors, list):
        raise ConfigError("root: 'connectors' must be a list")
    if not raw_connectors:
        raise ConfigError("root: 'connectors' must not be empty")

    connectors = tuple(
        _parse_connector(c, i, base_dir) for i, c in enumerate(raw_connectors)
    )
    _check_unique_ids([c.id for c in connectors], "connector")
    _check_strava_credential_uniqueness(connectors)

    raw_groups = raw.get("sync_groups", [])
    if not isinstance(raw_groups, list):
        raise ConfigError("root: 'sync_groups' must be a list")
    if not raw_groups:
        raise ConfigError("root: 'sync_groups' must not be empty")

    connector_ids = {c.id for c in connectors}
    sync_groups = tuple(
        _parse_group(g, i, connector_ids) for i, g in enumerate(raw_groups)
    )
    _check_unique_ids([g.id for g in sync_groups], "sync group")

    start = _parse_date_optional(raw, "start", "root")
    end = _parse_date_optional(raw, "end", "root")

    if start is not None and end is not None and start > end:
        raise ConfigError(f"root: 'start' ({start}) must not be after 'end' ({end})")

    return AppConfig(
        cache_dir=cache_dir,
        connectors=connectors,
        sync_groups=sync_groups,
        start=start,
        end=end,
    )

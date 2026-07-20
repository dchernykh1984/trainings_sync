from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from app.core.config import (
    AppConfig,
    GarminConnectorConfig,
    GroupSourceConfig,
    LocalFolderConnectorConfig,
    StravaConnectorConfig,
    SyncGroupConfig,
)
from app.credentials.base import CredentialRequest

_DEFAULT_CONFIG_DIR = Path.home() / ".config" / "trainings-sync"


# ---------------------------------------------------------------------------
# GUI data models
# ---------------------------------------------------------------------------


@dataclass
class CredentialEntry:
    service: str
    url: str
    login: str
    password: str


@dataclass
class CredentialSource:
    """Which backend the sync reads credentials from.

    ``source`` is "json" (the built-in credentials.json managed in the GUI) or
    "keepass" (an external .kdbx read via its path). The KeePass master password
    is never stored here - it is prompted at sync time.
    """

    source: str = "json"  # "json" | "keepass"
    keepass_path: str = ""


@dataclass
class ConnectorEntry:
    id: str
    type: str  # "garmin" | "strava" | "local_folder"
    credential_service: str = ""
    credential_url: str = ""
    credential_login: str = ""
    client_id: int = 0
    folder: str = ""


@dataclass
class GroupSourceEntry:
    id: str
    priority: int


@dataclass
class SyncGroupEntry:
    id: str
    sources: list[GroupSourceEntry] = field(default_factory=list)
    destinations: list[str] = field(default_factory=list)


@dataclass
class GuiConfig:
    connectors: list[ConnectorEntry] = field(default_factory=list)
    sync_groups: list[SyncGroupEntry] = field(default_factory=list)
    start: str = ""  # YYYY-MM-DD or empty -> default 2000-01-01 used at sync time
    end: str = ""  # YYYY-MM-DD or empty -> default today used at sync time
    force: bool = False
    skip_wellness: bool = False


# ---------------------------------------------------------------------------
# ConfigStore
# ---------------------------------------------------------------------------


class ConfigStore:
    """Reads and writes GUI config + credentials to a fixed directory."""

    def __init__(self, config_dir: Path = _DEFAULT_CONFIG_DIR) -> None:
        self._dir = Path(config_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    @property
    def config_dir(self) -> Path:
        return self._dir

    @property
    def cache_dir(self) -> Path:
        return self._dir / "cache"

    @property
    def credentials_path(self) -> Path:
        return self._dir / "credentials.json"

    @property
    def _config_path(self) -> Path:
        return self._dir / "config.json"

    @property
    def _credential_source_path(self) -> Path:
        return self._dir / "credential_source.json"

    # ------------------------------------------------------------------
    # Credential source (JSON store vs KeePass)
    # ------------------------------------------------------------------

    def load_credential_source(self) -> CredentialSource:
        if not self._credential_source_path.exists():
            return CredentialSource()
        raw = json.loads(self._credential_source_path.read_text(encoding="utf-8"))
        return CredentialSource(
            source=raw.get("source", "json"),
            keepass_path=raw.get("keepass_path", ""),
        )

    def save_credential_source(self, source: CredentialSource) -> None:
        _atomic_write(
            self._credential_source_path,
            {"source": source.source, "keepass_path": source.keepass_path},
        )

    # ------------------------------------------------------------------
    # Credentials
    # ------------------------------------------------------------------

    def load_credentials(self) -> list[CredentialEntry]:
        if not self.credentials_path.exists():
            return []
        return self.load_credentials_from(self.credentials_path)

    def load_credentials_from(self, path: Path) -> list[CredentialEntry]:
        """Parse a credentials JSON file (CLI/GUI format) at an arbitrary path."""
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("credentials file must contain a JSON array")
        return [_parse_credential_entry(e) for e in raw]

    def save_credentials(self, entries: list[CredentialEntry]) -> None:
        data = [
            {
                "service": e.service,
                "url": e.url,
                "login": e.login,
                "password": e.password,
            }
            for e in entries
        ]
        _atomic_write(self.credentials_path, data)

    # ------------------------------------------------------------------
    # GUI config
    # ------------------------------------------------------------------

    def load_gui_config(self) -> GuiConfig:
        if not self._config_path.exists():
            return GuiConfig()
        raw: dict = json.loads(self._config_path.read_text(encoding="utf-8"))
        return _parse_gui_config(raw)

    def load_gui_config_from(self, path: Path) -> GuiConfig:
        """Parse a config JSON file at an arbitrary path.

        Accepts both the GUI's own ``config.json`` and CLI config files; the
        CLI-only ``cache_dir`` field is ignored (the GUI uses a fixed cache).
        """
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("config file must contain a JSON object")
        return _parse_gui_config(raw)

    def save_gui_config(self, config: GuiConfig) -> None:
        data: dict = {
            "connectors": [_serialize_connector(c) for c in config.connectors],
            "sync_groups": [_serialize_group(g) for g in config.sync_groups],
            "force": config.force,
            "skip_wellness": config.skip_wellness,
        }
        if config.start:
            data["start"] = config.start
        if config.end:
            data["end"] = config.end
        _atomic_write(self._config_path, data)

    # ------------------------------------------------------------------
    # Conversion to AppConfig (used by the sync engine)
    # ------------------------------------------------------------------

    def to_app_config(self, config: GuiConfig) -> AppConfig:
        connectors = tuple(_connector_to_app(c) for c in config.connectors)
        sync_groups = tuple(_group_to_app(g) for g in config.sync_groups)
        start = date.fromisoformat(config.start) if config.start else None
        end = date.fromisoformat(config.end) if config.end else None
        return AppConfig(
            cache_dir=self.cache_dir,
            connectors=connectors,
            sync_groups=sync_groups,
            start=start,
            end=end,
        )


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _atomic_write(path: Path, data: object) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _parse_credential_entry(raw: dict) -> CredentialEntry:
    return CredentialEntry(
        service=raw["service"],
        url=raw["url"],
        login=raw["login"],
        password=raw["password"],
    )


def _parse_gui_config(raw: dict) -> GuiConfig:
    return GuiConfig(
        connectors=[_parse_connector_entry(c) for c in raw.get("connectors", [])],
        sync_groups=[_parse_group_entry(g) for g in raw.get("sync_groups", [])],
        start=raw.get("start", ""),
        end=raw.get("end", ""),
        force=raw.get("force", False),
        skip_wellness=raw.get("skip_wellness", False),
    )


def _parse_connector_entry(raw: dict) -> ConnectorEntry:
    return ConnectorEntry(
        id=raw.get("id", ""),
        type=raw.get("type", ""),
        credential_service=raw.get("credential_service", ""),
        credential_url=raw.get("credential_url", ""),
        credential_login=raw.get("credential_login", ""),
        client_id=raw.get("client_id", 0),
        folder=raw.get("folder", ""),
    )


def _parse_group_entry(raw: dict) -> SyncGroupEntry:
    return SyncGroupEntry(
        id=raw.get("id", ""),
        sources=[
            GroupSourceEntry(id=s["id"], priority=s["priority"])
            for s in raw.get("sources", [])
        ],
        destinations=list(raw.get("destinations", [])),
    )


def _serialize_connector(c: ConnectorEntry) -> dict:
    d: dict = {"id": c.id, "type": c.type}
    if c.type == "garmin":
        d["credential_service"] = c.credential_service
        d["credential_url"] = c.credential_url
        if c.credential_login:
            d["credential_login"] = c.credential_login
    elif c.type == "strava":
        d["client_id"] = c.client_id
        d["credential_service"] = c.credential_service
        d["credential_url"] = c.credential_url
        if c.credential_login:
            d["credential_login"] = c.credential_login
    elif c.type == "local_folder":
        d["folder"] = c.folder
    return d


def _serialize_group(g: SyncGroupEntry) -> dict:
    return {
        "id": g.id,
        "sources": [{"id": s.id, "priority": s.priority} for s in g.sources],
        "destinations": list(g.destinations),
    }


def _connector_to_app(
    c: ConnectorEntry,
) -> GarminConnectorConfig | StravaConnectorConfig | LocalFolderConnectorConfig:
    if c.type == "garmin":
        return GarminConnectorConfig(
            id=c.id,
            credential=CredentialRequest(
                service=c.credential_service,
                url=c.credential_url,
                login=c.credential_login or None,
            ),
        )
    if c.type == "strava":
        return StravaConnectorConfig(
            id=c.id,
            client_id=c.client_id,
            credential=CredentialRequest(
                service=c.credential_service,
                url=c.credential_url,
                login=c.credential_login or None,
            ),
        )
    if c.type == "local_folder":
        return LocalFolderConnectorConfig(
            id=c.id,
            folder=Path(c.folder).expanduser().resolve(),
        )
    raise ValueError(f"unknown connector type: {c.type!r}")


def _group_to_app(g: SyncGroupEntry) -> SyncGroupConfig:
    return SyncGroupConfig(
        id=g.id,
        sources=tuple(
            GroupSourceConfig(id=s.id, priority=s.priority) for s in g.sources
        ),
        destinations=tuple(g.destinations),
    )

from __future__ import annotations

from collections.abc import Callable

from app.connectors.base import ServiceConnector
from app.connectors.garmin import GarminConnector
from app.connectors.local_folder import LocalFolderConnector
from app.connectors.strava import StravaConnector
from app.core.config import (
    AppConfig,
    GarminConnectorConfig,
    StravaConnectorConfig,
    SyncGroupConfig,
)
from app.core.planner import SourceSpec
from app.credentials.base import CredentialProvider, StravaCredentials
from app.tracking.tracker import TaskTracker

OnStravaTokenRefresh = Callable[[str, StravaCredentials, str], None]


def _strava_callback(
    connector_id: str,
    on_refresh: OnStravaTokenRefresh | None,
) -> Callable[[StravaCredentials, str], None] | None:
    if on_refresh is None:
        return None

    def _cb(new_creds: StravaCredentials, user_label: str) -> None:
        on_refresh(connector_id, new_creds, user_label)

    return _cb


async def build_connectors(
    config: AppConfig,
    provider: CredentialProvider,
    tracker: TaskTracker,
    on_strava_token_refresh: OnStravaTokenRefresh | None = None,
) -> dict[str, ServiceConnector]:
    cred_requests = [
        cfg.credential
        for cfg in config.connectors
        if isinstance(cfg, (GarminConnectorConfig, StravaConnectorConfig))
    ]
    creds = iter(await provider.get_many(cred_requests, context="connectors"))

    result: dict[str, ServiceConnector] = {}
    for cfg in config.connectors:
        connector: ServiceConnector
        if isinstance(cfg, GarminConnectorConfig):
            connector = GarminConnector(credentials=next(creds), tracker=tracker)
        elif isinstance(cfg, StravaConnectorConfig):
            raw = next(creds)
            strava_creds = StravaCredentials(
                client_id=cfg.client_id,
                client_secret=raw.login,
                refresh_token=raw.password,
            )
            connector = StravaConnector(
                credentials=strava_creds,
                tracker=tracker,
                on_token_refresh=_strava_callback(cfg.id, on_strava_token_refresh),
            )
        else:  # LocalFolderConnectorConfig - source mode (no cache/dest_id)
            connector = LocalFolderConnector(folder=cfg.folder, tracker=tracker)
        result[cfg.id] = connector

    return result


def resolve_group_sources(
    group: SyncGroupConfig, connectors: dict[str, ServiceConnector]
) -> list[tuple[SourceSpec, ServiceConnector]]:
    return [
        (SourceSpec(source_id=src.id, priority=src.priority), connectors[src.id])
        for src in group.sources
    ]


def resolve_group_destinations(
    group: SyncGroupConfig,
    connectors: dict[str, ServiceConnector],
    cache: object,
) -> list[tuple[str, ServiceConnector]]:
    result: list[tuple[str, ServiceConnector]] = []
    for dest_id in group.destinations:
        connector = connectors[dest_id]
        if isinstance(connector, LocalFolderConnector):
            connector = connector.as_destination(cache, dest_id)
        result.append((dest_id, connector))
    return result

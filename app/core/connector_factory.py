from __future__ import annotations

from collections.abc import Callable, Iterable

from app.connectors.base import ServiceConnector
from app.connectors.garmin import GarminConnector
from app.connectors.intervals_icu import IntervalsIcuConnector
from app.connectors.local_folder import LocalFolderConnector
from app.connectors.polar import PolarConnector
from app.connectors.ride_with_gps import RideWithGpsConnector
from app.connectors.runalyze import RunalyzeConnector
from app.connectors.strava import StravaConnector
from app.connectors.wellness_base import WellnessConnector
from app.core.config import (
    CREDENTIAL_CONNECTOR_CONFIGS,
    AppConfig,
    ConfigError,
    CredentialOnlyConfig,
    GarminConnectorConfig,
    IntervalsIcuConnectorConfig,
    LocalFolderConnectorConfig,
    PolarConnectorConfig,
    RideWithGpsConnectorConfig,
    RunalyzeConnectorConfig,
    StravaConnectorConfig,
    SyncGroupConfig,
)
from app.core.planner import SourceSpec
from app.credentials.base import CredentialProvider, Credentials, StravaCredentials
from app.tracking.tracker import TaskTracker

OnStravaTokenRefresh = Callable[[str, StravaCredentials, str], None]

# The connector each credential-only config builds, from that credential and
# the tracker alone.
_CREDENTIAL_ONLY_CONNECTORS: dict[
    type[CredentialOnlyConfig], Callable[[Credentials, TaskTracker], ServiceConnector]
] = {
    GarminConnectorConfig: GarminConnector,
    IntervalsIcuConnectorConfig: IntervalsIcuConnector,
    RideWithGpsConnectorConfig: RideWithGpsConnector,
    RunalyzeConnectorConfig: RunalyzeConnector,
    PolarConnectorConfig: PolarConnector,
}


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
        if isinstance(cfg, CREDENTIAL_CONNECTOR_CONFIGS)
    ]
    creds = iter(await provider.get_many(cred_requests, context="connectors"))

    result: dict[str, ServiceConnector] = {}
    for cfg in config.connectors:
        connector: ServiceConnector
        if isinstance(cfg, StravaConnectorConfig):
            raw = next(creds)
            strava_creds = StravaCredentials(
                client_id=cfg.client_id,
                client_secret=raw.login,
                refresh_token=raw.password,
            )
            connector = StravaConnector(
                credentials=strava_creds,
                tracker=tracker,
                on_token_refresh=_strava_callback(cfg.uid, on_strava_token_refresh),
            )
        elif isinstance(cfg, CredentialOnlyConfig):
            make = _CREDENTIAL_ONLY_CONNECTORS[type(cfg)]
            connector = make(next(creds), tracker)
        else:  # LocalFolderConnectorConfig - source mode (no cache/dest_id)
            connector = LocalFolderConnector(folder=cfg.folder, tracker=tracker)
        result[cfg.uid] = connector

    _check_polar_members(result.values())
    return result


def _check_polar_members(connectors: Iterable[ServiceConnector]) -> None:
    """Refuse two Polar Flow connectors on one member id.

    AccessLink answers the second registration as if it were done, so that
    account would list nothing, run after run, with no error to say why.
    """
    members = [c.user_label for c in connectors if isinstance(c, PolarConnector)]
    taken = next((m for m in members if members.count(m) > 1), None)
    if taken is not None:
        raise ConfigError(
            f"two Polar Flow connectors use the member id {taken!r}:"
            " give each account its own login"
        )


async def build_wellness_connectors(
    config: AppConfig,
    provider: CredentialProvider,
    tracker: TaskTracker,
    activity_connectors: dict[str, ServiceConnector],
) -> dict[str, WellnessConnector]:
    from app.connectors.garmin_wellness import GarminWellnessConnector
    from app.connectors.local_folder_wellness import LocalFolderWellnessConnector
    from app.connectors.strava_wellness import StravaWellnessConnector

    result: dict[str, WellnessConnector] = {}
    for cfg in config.connectors:
        if isinstance(cfg, GarminConnectorConfig):
            activity_conn = activity_connectors.get(cfg.uid)
            if isinstance(activity_conn, GarminConnector):
                connector: WellnessConnector = (
                    GarminWellnessConnector.from_garmin_connector(
                        cfg.uid, activity_conn, tracker, display_name=cfg.id
                    )
                )
            else:
                continue
        elif isinstance(cfg, StravaConnectorConfig):
            activity_conn = activity_connectors.get(cfg.uid)
            if isinstance(activity_conn, StravaConnector):
                connector = StravaWellnessConnector(
                    connector_id=cfg.uid,
                    strava_connector=activity_conn,
                    tracker=tracker,
                    display_name=cfg.id,
                )
            else:
                continue
        elif isinstance(cfg, LocalFolderConnectorConfig):
            connector = LocalFolderWellnessConnector(
                connector_id=cfg.uid,
                folder=cfg.folder,
                tracker=tracker,
                display_name=cfg.id,
            )
        else:
            continue
        result[cfg.uid] = connector
    return result


def resolve_group_sources(
    group: SyncGroupConfig,
    connectors: dict[str, ServiceConnector],
    uid_by_id: dict[str, str],
) -> list[tuple[SourceSpec, ServiceConnector]]:
    """Resolve a group's source names to the uids everything else works by."""
    return [
        (
            SourceSpec(source_id=uid_by_id[src.id], priority=src.priority),
            connectors[uid_by_id[src.id]],
        )
        for src in group.sources
    ]


def resolve_group_destinations(
    group: SyncGroupConfig,
    connectors: dict[str, ServiceConnector],
    cache: object,
    uid_by_id: dict[str, str],
) -> list[tuple[str, ServiceConnector]]:
    result: list[tuple[str, ServiceConnector]] = []
    for name in group.destinations:
        uid = uid_by_id[name]
        connector = connectors[uid]
        if isinstance(connector, LocalFolderConnector):
            connector = connector.as_destination(cache, uid)
        result.append((uid, connector))
    return result

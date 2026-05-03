from __future__ import annotations

from collections.abc import Callable

from app.connectors.base import ServiceConnector
from app.connectors.garmin import GarminConnector
from app.connectors.local_folder import LocalFolderConnector
from app.connectors.strava import StravaConnector
from app.core.config import (
    AppConfig,
    GarminDestinationConfig,
    GarminSourceConfig,
    LocalFolderSourceConfig,
    StravaDestinationConfig,
    StravaSourceConfig,
)
from app.core.planner import SourceSpec
from app.credentials.base import CredentialProvider, StravaCredentials
from app.tracking.tracker import TaskTracker

OnStravaTokenRefresh = Callable[[str, StravaCredentials], None]


def _strava_callback(
    connector_id: str,
    on_refresh: OnStravaTokenRefresh | None,
) -> Callable[[StravaCredentials], None] | None:
    if on_refresh is None:
        return None

    def _cb(new_creds: StravaCredentials) -> None:
        on_refresh(connector_id, new_creds)

    return _cb


async def build_sources(
    config: AppConfig,
    provider: CredentialProvider,
    tracker: TaskTracker,
) -> list[tuple[SourceSpec, ServiceConnector]]:
    for src in config.sources:
        if isinstance(src, StravaSourceConfig):
            raise ValueError(
                f"source {src.id!r}: Strava cannot be used as a download source — "
                "the Strava API does not support activity file downloads"
            )

    cred_requests = [
        src.credential for src in config.sources if isinstance(src, GarminSourceConfig)
    ]
    creds = iter(await provider.get_many(cred_requests))

    result: list[tuple[SourceSpec, ServiceConnector]] = []
    for src_cfg in config.sources:
        connector: ServiceConnector
        if isinstance(src_cfg, GarminSourceConfig):
            connector = GarminConnector(credentials=next(creds), tracker=tracker)
        elif isinstance(src_cfg, LocalFolderSourceConfig):
            connector = LocalFolderConnector(folder=src_cfg.folder, tracker=tracker)
        else:
            raise AssertionError(  # pragma: no cover
                f"unexpected source type: {type(src_cfg).__name__}"
            )

        spec = SourceSpec(source_id=src_cfg.id, priority=src_cfg.priority)
        result.append((spec, connector))
    return result


async def build_destinations(
    config: AppConfig,
    provider: CredentialProvider,
    tracker: TaskTracker,
    on_strava_token_refresh: OnStravaTokenRefresh | None = None,
) -> list[tuple[str, ServiceConnector]]:
    seen_strava_creds: set = set()
    for dest in config.destinations:
        if isinstance(dest, StravaDestinationConfig):
            if dest.credential in seen_strava_creds:
                raise ValueError(
                    f"destination {dest.id!r}: duplicate Strava credential ref — "
                    "each Strava account must appear at most once "
                    "(sharing a refresh token causes rotation races)"
                )
            seen_strava_creds.add(dest.credential)

    cred_requests = [
        dest.credential
        for dest in config.destinations
        if isinstance(dest, (GarminDestinationConfig, StravaDestinationConfig))
    ]
    creds = iter(await provider.get_many(cred_requests))

    result: list[tuple[str, ServiceConnector]] = []
    for dest_cfg in config.destinations:
        connector: ServiceConnector
        if isinstance(dest_cfg, GarminDestinationConfig):
            connector = GarminConnector(credentials=next(creds), tracker=tracker)
        elif isinstance(dest_cfg, StravaDestinationConfig):
            raw = next(creds)
            strava_creds = StravaCredentials(
                client_id=dest_cfg.client_id,
                client_secret=raw.login,
                refresh_token=raw.password,
            )
            connector = StravaConnector(
                credentials=strava_creds,
                tracker=tracker,
                on_token_refresh=_strava_callback(dest_cfg.id, on_strava_token_refresh),
            )
        else:  # LocalFolderDestinationConfig
            connector = LocalFolderConnector(folder=dest_cfg.folder, tracker=tracker)

        result.append((dest_cfg.id, connector))
    return result

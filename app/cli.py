from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys
from datetime import date
from pathlib import Path

from app.core.cache import ActivityCache
from app.core.config import (
    AppConfig,
    ConfigError,
    GarminConnectorConfig,
    StravaConnectorConfig,
    load_config,
)
from app.core.connector_factory import build_connectors
from app.core.orchestrator import SyncOrchestrator
from app.credentials.base import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
    StravaCredentials,
)
from app.credentials.json_file import JsonFileProvider
from app.credentials.keepass import KeePassProvider
from app.tracking.console_renderer import ConsoleRenderer
from app.tracking.sync_logger import SyncLogger
from app.tracking.tracker import TaskTracker


class _NullProvider(CredentialProvider):
    """Placeholder for local-only configs that require no credentials."""

    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        raise AssertionError(  # pragma: no cover
            f"unexpected credential request for {request.service!r}"
        )


def _credentials_needed(config: AppConfig) -> bool:
    return any(
        isinstance(c, (GarminConnectorConfig, StravaConnectorConfig))
        for c in config.connectors
    )


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}: expected YYYY-MM-DD"
        ) from exc


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trainings-sync",
        description=(
            "Sync training activities between Garmin, Strava, and local folders."
        ),
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        metavar="PATH",
        help="path to the JSON config file",
    )
    parser.add_argument(
        "--start",
        type=_parse_date,
        metavar="DATE",
        help="start date (YYYY-MM-DD); overrides config value; defaults to 2000-01-01",
    )
    parser.add_argument(
        "--end",
        type=_parse_date,
        metavar="DATE",
        help="end date (YYYY-MM-DD); overrides config value, defaults to today",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download activities even if already cached",
    )
    creds_group = parser.add_mutually_exclusive_group(required=False)
    creds_group.add_argument(
        "--creds-json",
        type=Path,
        metavar="PATH",
        help="JSON credentials file (required for Garmin/Strava connectors)",
    )
    creds_group.add_argument(
        "--creds-keepass",
        type=Path,
        metavar="PATH",
        help="KeePass database (.kdbx); password via KEEPASS_PASSWORD env var or stdin",
    )
    return parser


def _make_provider(
    args: argparse.Namespace,
    tracker: TaskTracker,
    *,
    keepass_password: str | None = None,
) -> CredentialProvider:
    if args.creds_json:
        return JsonFileProvider(
            path=args.creds_json.expanduser().resolve(), tracker=tracker
        )

    password = (
        keepass_password
        or os.environ.get("KEEPASS_PASSWORD")
        or getpass.getpass("KeePass master password: ")
    )
    return KeePassProvider(
        path=args.creds_keepass.expanduser().resolve(),
        password=password,
        tracker=tracker,
    )


def _validate(
    args: argparse.Namespace, config: AppConfig, start: date, end: date
) -> None:
    if start > end:
        print(f"error: --start ({start}) is after --end ({end})", file=sys.stderr)
        sys.exit(1)
    if _credentials_needed(config) and not (args.creds_json or args.creds_keepass):
        print(
            "error: this config requires credentials -"
            " provide --creds-json or --creds-keepass",
            file=sys.stderr,
        )
        sys.exit(1)
    has_strava = any(isinstance(c, StravaConnectorConfig) for c in config.connectors)
    if args.creds_keepass and has_strava:
        print(
            "error: --creds-keepass does not support Strava -"
            " KeePass refresh token persistence is not implemented; use --creds-json",
            file=sys.stderr,
        )
        sys.exit(1)


async def _run_sync(
    args: argparse.Namespace,
    config: AppConfig,
    sync_logger: SyncLogger,
    start: date,
    end: date,
) -> None:
    # Prompt before ConsoleRenderer starts - getpass conflicts with Rich.
    keepass_password: str | None = None
    if _credentials_needed(config) and args.creds_keepass:
        keepass_password = os.environ.get("KEEPASS_PASSWORD") or getpass.getpass(
            "KeePass master password: "
        )

    strava_cred_map = {
        c.id: c.credential
        for c in config.connectors
        if isinstance(c, StravaConnectorConfig)
    }

    print(
        f"Sync log: {sync_logger.path}\n"
        "If progress appears frozen, check the log file above for details."
    )
    download_failures = 0
    with ConsoleRenderer() as renderer:
        tracker = TaskTracker(renderer, sync_logger=sync_logger)
        provider = (
            _make_provider(args, tracker, keepass_password=keepass_password)
            if _credentials_needed(config)
            else _NullProvider()
        )

        def _on_token_refresh(
            connector_id: str, new_creds: StravaCredentials, user_label: str
        ) -> None:
            if isinstance(provider, JsonFileProvider):
                provider.update_refresh_token(
                    strava_cred_map[connector_id], new_creds.refresh_token
                )
                sync_logger.info(
                    f"[strava] Token refresh ({user_label}): saved to {args.creds_json}"
                )

        cache = ActivityCache(config.cache_dir)
        cache.load()

        try:
            connectors = await build_connectors(
                config,
                provider,
                tracker,
                on_strava_token_refresh=_on_token_refresh,
            )
        except CredentialsNotFoundError:
            sys.exit(1)  # already reported via tracker
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            sys.exit(1)

        login_tasks = {
            cid: asyncio.create_task(c.login()) for cid, c in connectors.items()
        }
        orchestrator = SyncOrchestrator(
            groups=config.sync_groups,
            connectors=connectors,
            cache=cache,
            tracker=tracker,
            login_tasks=login_tasks,
        )
        try:
            download_failures = await orchestrator.run(start, end, force=args.force)
        finally:
            for t in login_tasks.values():
                if not t.done():
                    t.cancel()
            await asyncio.gather(*login_tasks.values(), return_exceptions=True)

    if download_failures:
        n = download_failures
        noun = "activity" if n == 1 else "activities"
        print(
            f"warning: {n} {noun} failed to download"
            f" (see {sync_logger.path} for details)",
            file=sys.stderr,
        )


async def _run(args: argparse.Namespace) -> None:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    sync_logger: SyncLogger | None = None
    try:
        sync_logger = SyncLogger(config.cache_dir / "sync.log")

        start: date = (
            args.start if args.start is not None else (config.start or date(2000, 1, 1))
        )
        end: date = args.end if args.end is not None else (config.end or date.today())

        _validate(args, config, start, end)

        sync_logger.run_start(start=start, end=end, force=args.force)
        if _credentials_needed(config):
            if args.creds_json:
                sync_logger.info(f"[credentials] Source: JSON file {args.creds_json}")
            elif args.creds_keepass:
                sync_logger.info(f"[credentials] Source: KeePass {args.creds_keepass}")

        await _run_sync(args, config, sync_logger, start, end)

        sync_logger.run_end()
    except Exception as exc:
        if sync_logger is not None:
            sync_logger.error(f"Unexpected error: {exc}", exc_info=True)
            print(f"error: {exc}", file=sys.stderr)
            print(
                f"(see {sync_logger.path} for the full traceback)",
                file=sys.stderr,
            )
        else:
            print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        if sync_logger is not None:
            sync_logger.close()


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    try:
        asyncio.run(_run(args))
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

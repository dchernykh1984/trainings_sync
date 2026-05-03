from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys
import traceback
from datetime import date
from pathlib import Path

from app.core.cache import ActivityCache
from app.core.config import (
    AppConfig,
    ConfigError,
    GarminDestinationConfig,
    GarminSourceConfig,
    StravaDestinationConfig,
    load_config,
)
from app.core.connector_factory import build_destinations, build_sources
from app.core.sync import SyncExecutor
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
from app.tracking.tracker import TaskTracker


class _NullProvider(CredentialProvider):
    """Placeholder for local-only configs that require no credentials."""

    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        raise AssertionError(  # pragma: no cover
            f"unexpected credential request for {request.service!r}"
        )


def _credentials_needed(config: AppConfig) -> bool:
    return any(isinstance(s, GarminSourceConfig) for s in config.sources) or any(
        isinstance(d, (GarminDestinationConfig, StravaDestinationConfig))
        for d in config.destinations
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
            "error: this config requires credentials — "
            "provide --creds-json or --creds-keepass",
            file=sys.stderr,
        )
        sys.exit(1)
    if args.creds_keepass and any(
        isinstance(dest, StravaDestinationConfig) for dest in config.destinations
    ):
        print(
            "error: --creds-keepass does not support Strava destinations — "
            "KeePass refresh token persistence is not implemented; use --creds-json",
            file=sys.stderr,
        )
        sys.exit(1)


async def _run(args: argparse.Namespace) -> None:
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)

    start: date = (
        args.start if args.start is not None else (config.start or date(2000, 1, 1))
    )
    end: date = args.end if args.end is not None else (config.end or date.today())

    _validate(args, config, start, end)

    # Prompt before ConsoleRenderer starts — getpass conflicts with Rich live rendering.
    keepass_password: str | None = None
    if _credentials_needed(config) and args.creds_keepass:
        keepass_password = os.environ.get("KEEPASS_PASSWORD") or getpass.getpass(
            "KeePass master password: "
        )

    strava_cred_map = {
        dest.id: dest.credential
        for dest in config.destinations
        if isinstance(dest, StravaDestinationConfig)
    }

    with ConsoleRenderer() as renderer:
        tracker = TaskTracker(renderer)
        provider = (
            _make_provider(args, tracker, keepass_password=keepass_password)
            if _credentials_needed(config)
            else _NullProvider()
        )

        def _on_token_refresh(dest_id: str, new_creds: StravaCredentials) -> None:
            if isinstance(provider, JsonFileProvider):
                provider.update_refresh_token(
                    strava_cred_map[dest_id], new_creds.refresh_token
                )
                print(
                    f"info: Strava refresh token for {dest_id!r} auto-updated"
                    f" in {args.creds_json}.",
                    file=sys.stderr,
                )

        try:
            sources = await build_sources(config, provider, tracker)
            destinations = await build_destinations(
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

        for _, connector in sources:
            await connector.login()
        for _, connector in destinations:
            await connector.login()

        cache = ActivityCache(config.cache_dir)
        cache.load()

        executor = SyncExecutor(sources=sources, destinations=destinations, cache=cache)
        await executor.run(start, end, force=args.force)


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    try:
        asyncio.run(_run(args))
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

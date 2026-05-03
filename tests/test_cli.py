from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.cli import _run, _validate
from app.core.config import AppConfig, StravaDestinationConfig
from app.credentials.base import CredentialRequest, StravaCredentials

_STRAVA_CRED = CredentialRequest(service="Strava", url="https://www.strava.com")
_STRAVA_DEST = StravaDestinationConfig(
    id="strava-upload", client_id=99999, credential=_STRAVA_CRED
)
_START = date(2024, 1, 1)
_END = date(2024, 12, 31)


def _args(
    *, creds_json: Path | None = None, creds_keepass: Path | None = None
) -> argparse.Namespace:
    return argparse.Namespace(creds_json=creds_json, creds_keepass=creds_keepass)


def _cfg(destinations: tuple = ()) -> AppConfig:
    return AppConfig(
        cache_dir=Path("/nonexistent/cache"), sources=(), destinations=destinations
    )


class TestValidate:
    def test_start_after_end_is_rejected(self, capsys: pytest.CaptureFixture) -> None:
        with pytest.raises(SystemExit) as exc:
            _validate(_args(), _cfg(), date(2024, 12, 31), date(2024, 1, 1))
        assert exc.value.code == 1
        assert "--start" in capsys.readouterr().err

    def test_missing_provider_with_credentials_needed_is_rejected(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            _validate(_args(), _cfg(destinations=(_STRAVA_DEST,)), _START, _END)
        assert exc.value.code == 1
        assert "credentials" in capsys.readouterr().err

    def test_keepass_with_strava_destination_is_rejected(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        args = _args(creds_keepass=Path("vault.kdbx"))
        with pytest.raises(SystemExit) as exc:
            _validate(args, _cfg(destinations=(_STRAVA_DEST,)), _START, _END)
        assert exc.value.code == 1
        assert "--creds-keepass" in capsys.readouterr().err

    def test_valid_args_pass_without_exception(self) -> None:
        args = _args(creds_json=Path("creds.json"))
        _validate(args, _cfg(destinations=(_STRAVA_DEST,)), _START, _END)


class TestTokenRefreshCallback:
    async def test_json_file_updated_on_strava_token_refresh(
        self, tmp_path: Path
    ) -> None:
        creds_file = tmp_path / "creds.json"
        creds_file.write_text(
            json.dumps(
                [
                    {
                        "service": "Strava",
                        "url": "https://www.strava.com",
                        "login": "client-secret",
                        "password": "old-rt",
                    }
                ]
            ),
            encoding="utf-8",
        )

        strava_cred = CredentialRequest("Strava", "https://www.strava.com")
        strava_dest = StravaDestinationConfig(
            id="strava-dest", client_id=99, credential=strava_cred
        )
        config = AppConfig(cache_dir=tmp_path, sources=(), destinations=(strava_dest,))
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=creds_file,
            creds_keepass=None,
        )
        new_rt = "new-refresh-token"

        async def _fake_build_destinations(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            class _FakeConnector:
                async def login(self):
                    if on_strava_token_refresh:
                        on_strava_token_refresh(
                            "strava-dest",
                            StravaCredentials(
                                client_id=99,
                                client_secret="client-secret",
                                refresh_token=new_rt,
                            ),
                        )

            return [("strava-dest", _FakeConnector())]

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_sources", new=AsyncMock(return_value=[])),
            patch("app.cli.build_destinations", new=_fake_build_destinations),
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.SyncExecutor") as mock_executor,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()
            mock_executor.return_value.run = AsyncMock()
            await _run(args)

        data = json.loads(creds_file.read_text(encoding="utf-8"))
        assert data[0]["password"] == new_rt

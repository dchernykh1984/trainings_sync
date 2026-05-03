from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pytest

from app.cli import _validate
from app.core.config import AppConfig, StravaDestinationConfig
from app.credentials.base import CredentialRequest

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

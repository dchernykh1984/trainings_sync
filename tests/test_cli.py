from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.cli import (
    _build_arg_parser,
    _make_provider,
    _parse_date,
    _run,
    _validate,
    main,
)
from app.core.config import (
    AppConfig,
    GarminConnectorConfig,
    GroupSourceConfig,
    StravaConnectorConfig,
    SyncGroupConfig,
)
from app.credentials.base import (
    CredentialRequest,
    CredentialsNotFoundError,
    StravaCredentials,
)

_GARMIN_CRED = CredentialRequest(
    service="Garmin Connect",
    url="https://connect.garmin.com",
    login="user@example.com",
)
_STRAVA_CRED = CredentialRequest(service="Strava", url="https://www.strava.com")
_STRAVA_CONNECTOR = StravaConnectorConfig(
    id="strava", client_id=99999, credential=_STRAVA_CRED
)
_DEFAULT_GROUP = SyncGroupConfig(
    id="default",
    sources=(GroupSourceConfig(id="strava", priority=1),),
    destinations=(),
)
_START = date(2024, 1, 1)
_END = date(2024, 12, 31)


def _args(
    *, creds_json: Path | None = None, creds_keepass: Path | None = None
) -> argparse.Namespace:
    return argparse.Namespace(creds_json=creds_json, creds_keepass=creds_keepass)


def _cfg(connectors: tuple = (), sync_groups: tuple = ()) -> AppConfig:
    return AppConfig(
        cache_dir=Path("/nonexistent/cache"),
        connectors=connectors,
        sync_groups=sync_groups,
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
            _validate(
                _args(),
                _cfg(connectors=(_STRAVA_CONNECTOR,), sync_groups=(_DEFAULT_GROUP,)),
                _START,
                _END,
            )
        assert exc.value.code == 1
        assert "credentials" in capsys.readouterr().err

    def test_keepass_with_strava_connector_is_rejected(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        args = _args(creds_keepass=Path("vault.kdbx"))
        with pytest.raises(SystemExit) as exc:
            _validate(
                args,
                _cfg(connectors=(_STRAVA_CONNECTOR,), sync_groups=(_DEFAULT_GROUP,)),
                _START,
                _END,
            )
        assert exc.value.code == 1
        assert "--creds-keepass" in capsys.readouterr().err

    def test_strava_connector_requires_credentials(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        with pytest.raises(SystemExit) as exc:
            _validate(
                _args(),
                _cfg(connectors=(_STRAVA_CONNECTOR,), sync_groups=(_DEFAULT_GROUP,)),
                _START,
                _END,
            )
        assert exc.value.code == 1
        assert "credentials" in capsys.readouterr().err

    def test_valid_args_pass_without_exception(self) -> None:
        args = _args(creds_json=Path("creds.json"))
        _validate(
            args,
            _cfg(connectors=(_STRAVA_CONNECTOR,), sync_groups=(_DEFAULT_GROUP,)),
            _START,
            _END,
        )


class TestBuildConnectorsCallback:
    async def test_on_strava_token_refresh_passed_to_build_connectors(
        self, tmp_path: Path
    ) -> None:
        creds_file = tmp_path / "creds.json"
        creds_file.write_text(
            json.dumps(
                [
                    {
                        "service": "Strava",
                        "url": "https://www.strava.com",
                        "login": "secret",
                        "password": "old-rt",
                    }
                ]
            ),
            encoding="utf-8",
        )
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(_STRAVA_CONNECTOR,),
            sync_groups=(_DEFAULT_GROUP,),
        )
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=creds_file,
            creds_keepass=None,
        )
        received_callback: list = []

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            received_callback.append(on_strava_token_refresh)
            return {}

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()
            mock_orch.return_value.run = AsyncMock(return_value=0)
            await _run(args)

        assert len(received_callback) == 1
        assert callable(received_callback[0])

    async def test_strava_connector_token_refresh_updates_json_file(
        self, tmp_path: Path
    ) -> None:
        creds_file = tmp_path / "creds.json"
        creds_file.write_text(
            json.dumps(
                [
                    {
                        "service": "Strava",
                        "url": "https://www.strava.com",
                        "login": "secret",
                        "password": "old-rt",
                    }
                ]
            ),
            encoding="utf-8",
        )
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(_STRAVA_CONNECTOR,),
            sync_groups=(_DEFAULT_GROUP,),
        )
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=creds_file,
            creds_keepass=None,
        )
        new_rt = "new-refresh-token"

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            class _FakeConnector:
                async def login(self) -> None:
                    if on_strava_token_refresh:
                        on_strava_token_refresh(
                            "strava",
                            StravaCredentials(
                                client_id=99999,
                                client_secret="secret",
                                refresh_token=new_rt,
                            ),
                            "John Doe",
                        )

            return {"strava": _FakeConnector()}

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()

            async def _mock_run_refresh_1(*args, **kwargs):
                lt = mock_orch.call_args.kwargs.get("login_tasks", {})
                await asyncio.gather(*lt.values())
                return 0

            mock_orch.return_value.run = _mock_run_refresh_1
            await _run(args)

        data = json.loads(creds_file.read_text(encoding="utf-8"))
        assert data[0]["password"] == new_rt


class TestTokenRefreshCallback:
    async def test_json_file_updated_on_strava_connector_token_refresh(
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
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(_STRAVA_CONNECTOR,),
            sync_groups=(_DEFAULT_GROUP,),
        )
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=creds_file,
            creds_keepass=None,
        )
        new_rt = "new-refresh-token"

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            class _FakeConnector:
                async def login(self) -> None:
                    if on_strava_token_refresh:
                        on_strava_token_refresh(
                            "strava",
                            StravaCredentials(
                                client_id=99999,
                                client_secret="client-secret",
                                refresh_token=new_rt,
                            ),
                            "John Doe",
                        )

            return {"strava": _FakeConnector()}

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()

            async def _mock_run_refresh_2(*args, **kwargs):
                lt = mock_orch.call_args.kwargs.get("login_tasks", {})
                await asyncio.gather(*lt.values())
                return 0

            mock_orch.return_value.run = _mock_run_refresh_2
            await _run(args)

        data = json.loads(creds_file.read_text(encoding="utf-8"))
        assert data[0]["password"] == new_rt


class TestMain:
    def test_unexpected_exception_prints_error_and_exits_1(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        with (
            patch("app.cli._build_arg_parser") as mock_parser,
            patch("app.cli._run", new=AsyncMock(side_effect=RuntimeError("boom"))),
            pytest.raises(SystemExit) as exc,
        ):
            mock_parser.return_value.parse_args.return_value = argparse.Namespace()
            main()

        assert exc.value.code == 1
        err = capsys.readouterr().err
        assert "boom" in err
        assert "Traceback" not in err


class TestParseDate:
    def test_valid_date_is_parsed(self) -> None:
        result = _parse_date("2024-03-15")
        assert result == date(2024, 3, 15)

    def test_invalid_date_raises_argument_type_error(self) -> None:
        with pytest.raises(argparse.ArgumentTypeError, match="invalid date"):
            _parse_date("not-a-date")


class TestBuildArgParser:
    def test_returns_argument_parser(self) -> None:
        parser = _build_arg_parser()
        assert parser is not None

    def test_config_is_required(self) -> None:
        parser = _build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_config_is_parsed_as_path(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.json"
        parser = _build_arg_parser()
        args = parser.parse_args(["--config", str(config_file)])
        assert args.config == config_file

    def test_force_flag_defaults_to_false(self, tmp_path: Path) -> None:
        parser = _build_arg_parser()
        args = parser.parse_args(["--config", str(tmp_path / "c.json")])
        assert args.force is False

    def test_force_flag_set_when_provided(self, tmp_path: Path) -> None:
        parser = _build_arg_parser()
        args = parser.parse_args(["--config", str(tmp_path / "c.json"), "--force"])
        assert args.force is True

    def test_creds_json_and_creds_keepass_are_mutually_exclusive(
        self, tmp_path: Path
    ) -> None:
        parser = _build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "--config",
                    str(tmp_path / "c.json"),
                    "--creds-json",
                    str(tmp_path / "creds.json"),
                    "--creds-keepass",
                    str(tmp_path / "vault.kdbx"),
                ]
            )

    def test_start_date_parsed_correctly(self, tmp_path: Path) -> None:
        parser = _build_arg_parser()
        args = parser.parse_args(
            ["--config", str(tmp_path / "c.json"), "--start", "2024-01-15"]
        )
        assert args.start == date(2024, 1, 15)

    def test_invalid_start_date_exits(self, tmp_path: Path) -> None:
        parser = _build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["--config", str(tmp_path / "c.json"), "--start", "bad-date"]
            )


class TestMakeProvider:
    def test_returns_json_provider_when_creds_json_set(
        self, tmp_path: Path, tracker: MagicMock
    ) -> None:
        from app.credentials.json_file import JsonFileProvider

        creds_file = tmp_path / "creds.json"
        creds_file.write_text("[]", encoding="utf-8")
        args = argparse.Namespace(creds_json=creds_file, creds_keepass=None)
        provider = _make_provider(args, tracker)
        assert isinstance(provider, JsonFileProvider)

    def test_returns_keepass_provider_when_creds_keepass_set(
        self, tmp_path: Path, tracker: MagicMock
    ) -> None:
        from app.credentials.keepass import KeePassProvider

        kdbx = tmp_path / "vault.kdbx"
        kdbx.write_bytes(b"fake")
        args = argparse.Namespace(creds_json=None, creds_keepass=kdbx)
        provider = _make_provider(args, tracker, keepass_password="secret")
        assert isinstance(provider, KeePassProvider)

    def test_keepass_reads_env_var_when_no_explicit_password(
        self, tmp_path: Path, tracker: MagicMock
    ) -> None:
        from app.credentials.keepass import KeePassProvider

        kdbx = tmp_path / "vault.kdbx"
        kdbx.write_bytes(b"fake")
        args = argparse.Namespace(creds_json=None, creds_keepass=kdbx)
        with patch.dict("os.environ", {"KEEPASS_PASSWORD": "env-secret"}):
            provider = _make_provider(args, tracker)
        assert isinstance(provider, KeePassProvider)


@pytest.fixture
def tracker() -> MagicMock:
    return MagicMock()


class TestRunConfigError:
    async def test_config_error_exits_with_message(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        from app.core.config import ConfigError

        args = argparse.Namespace(
            config=Path("/nonexistent/config.json"),
            start=None,
            end=None,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )
        with (
            patch("app.cli.load_config", side_effect=ConfigError("bad config")),
            pytest.raises(SystemExit) as exc,
        ):
            await _run(args)

        assert exc.value.code == 1
        assert "bad config" in capsys.readouterr().err

    async def test_unexpected_exception_logs_and_exits(
        self, capsys: pytest.CaptureFixture, tmp_path: Path
    ) -> None:
        config = AppConfig(cache_dir=tmp_path, connectors=(), sync_groups=())
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )
        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli._validate"),
            patch("app.cli._run_sync", side_effect=RuntimeError("unexpected boom")),
            pytest.raises(SystemExit) as exc,
        ):
            await _run(args)

        assert exc.value.code == 1
        err = capsys.readouterr().err
        assert "unexpected boom" in err

    async def test_unexpected_exception_with_no_sync_logger_prints_error(
        self, capsys: pytest.CaptureFixture, tmp_path: Path
    ) -> None:
        config = AppConfig(cache_dir=tmp_path, connectors=(), sync_groups=())
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )
        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.SyncLogger", side_effect=RuntimeError("logger init fail")),
            pytest.raises(SystemExit) as exc,
        ):
            await _run(args)

        assert exc.value.code == 1
        assert "logger init fail" in capsys.readouterr().err


class TestRunKeepassLogging:
    async def test_keepass_credential_logged_when_used(self, tmp_path: Path) -> None:
        garmin_connector = GarminConnectorConfig(id="garmin", credential=_GARMIN_CRED)
        group = SyncGroupConfig(
            id="g",
            sources=(GroupSourceConfig(id="garmin", priority=1),),
            destinations=(),
        )
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(garmin_connector,),
            sync_groups=(group,),
        )
        kdbx = tmp_path / "vault.kdbx"
        kdbx.write_bytes(b"fake")
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=kdbx,
        )
        logged_messages: list[str] = []

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli._run_sync", new=AsyncMock()),
            patch(
                "app.cli.SyncLogger",
                return_value=MagicMock(
                    **{
                        "run_start": MagicMock(),
                        "run_end": MagicMock(),
                        "close": MagicMock(),
                        "info": lambda msg: logged_messages.append(msg),
                        "path": tmp_path / "sync.log",
                    }
                ),
            ),
        ):
            await _run(args)

        assert any("KeePass" in m for m in logged_messages)


class TestRunSyncErrors:
    async def test_credentials_not_found_exits_1(self, tmp_path: Path) -> None:
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(_STRAVA_CONNECTOR,),
            sync_groups=(_DEFAULT_GROUP,),
        )
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("[]", encoding="utf-8")
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=creds_file,
            creds_keepass=None,
        )

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            raise CredentialsNotFoundError(_STRAVA_CRED)

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
            pytest.raises(SystemExit) as exc,
        ):
            mock_cache.return_value.load = MagicMock()
            await _run(args)

        assert exc.value.code == 1

    async def test_download_failures_warning_printed(
        self, capsys: pytest.CaptureFixture, tmp_path: Path
    ) -> None:
        config = AppConfig(cache_dir=tmp_path, connectors=(), sync_groups=())
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("[]", encoding="utf-8")
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=AsyncMock(return_value={})),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()
            mock_orch.return_value.run = AsyncMock(return_value=3)
            await _run(args)

        err = capsys.readouterr().err
        assert "3 activities failed to download" in err

    async def test_value_error_in_build_exits_1(
        self, capsys: pytest.CaptureFixture, tmp_path: Path
    ) -> None:
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(_STRAVA_CONNECTOR,),
            sync_groups=(_DEFAULT_GROUP,),
        )
        creds_file = tmp_path / "creds.json"
        creds_file.write_text("[]", encoding="utf-8")
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=creds_file,
            creds_keepass=None,
        )

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            raise ValueError("bad config value")

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
            pytest.raises(SystemExit) as exc,
        ):
            mock_cache.return_value.load = MagicMock()
            await _run(args)

        assert exc.value.code == 1
        assert "bad config value" in capsys.readouterr().err


class TestRunSyncKeepassPassword:
    async def test_keepass_password_prompted_before_renderer(
        self, tmp_path: Path
    ) -> None:
        """KeePass password must be prompted before ConsoleRenderer starts."""
        strava_connector = StravaConnectorConfig(
            id="strava", client_id=99, credential=_STRAVA_CRED
        )
        group = SyncGroupConfig(
            id="g",
            sources=(GroupSourceConfig(id="strava", priority=1),),
            destinations=(),
        )
        config = AppConfig(
            cache_dir=tmp_path,
            connectors=(strava_connector,),
            sync_groups=(group,),
        )
        kdbx = tmp_path / "vault.kdbx"
        kdbx.write_bytes(b"fake")
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=kdbx,
        )
        sync_logger = MagicMock()
        sync_logger.info = MagicMock()
        call_order: list[str] = []

        def _env_get(key: str, default: str | None = None) -> str | None:
            if key == "KEEPASS_PASSWORD":
                call_order.append("env_get")
            return "env-password" if key == "KEEPASS_PASSWORD" else default

        def _renderer_enter(_: object) -> MagicMock:
            call_order.append("renderer_enter")
            return MagicMock()

        renderer_mock = MagicMock()
        renderer_mock.__enter__ = _renderer_enter
        renderer_mock.__exit__ = MagicMock(return_value=False)

        with (
            patch("app.cli.os.environ.get", side_effect=_env_get),
            patch("app.cli.build_connectors", new=AsyncMock(return_value={})),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer", return_value=renderer_mock),
            patch("app.cli._make_provider") as mock_make_provider,
        ):
            mock_cache.return_value.load = MagicMock()
            mock_orch.return_value.run = AsyncMock(return_value=0)
            mock_make_provider.return_value = MagicMock()
            from app.cli import _run_sync

            await _run_sync(args, config, sync_logger, _START, _END)

        assert "env_get" in call_order
        assert "renderer_enter" in call_order
        assert call_order.index("env_get") < call_order.index("renderer_enter")
        kw = mock_make_provider.call_args.kwargs
        assert kw.get("keepass_password") == "env-password"


class TestParallelLogin:
    async def test_connectors_are_logged_in_parallel(self, tmp_path: Path) -> None:
        config = AppConfig(cache_dir=tmp_path, connectors=(), sync_groups=())
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )
        call_log: list[str] = []

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            class _ConnA:
                async def login(self) -> None:
                    call_log.append("a-start")
                    await asyncio.sleep(0)
                    call_log.append("a-done")

            class _ConnB:
                async def login(self) -> None:
                    call_log.append("b-start")
                    await asyncio.sleep(0)
                    call_log.append("b-done")

            return {"a": _ConnA(), "b": _ConnB()}

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()

            async def _mock_run(*args, **kwargs):
                lt = mock_orch.call_args.kwargs.get("login_tasks", {})
                await asyncio.gather(*lt.values())
                return 0

            mock_orch.return_value.run = _mock_run
            await _run(args)

        assert call_log == ["a-start", "b-start", "a-done", "b-done"]

    async def test_login_failure_cancels_sibling_login(self, tmp_path: Path) -> None:
        config = AppConfig(cache_dir=tmp_path, connectors=(), sync_groups=())
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )
        call_log: list[str] = []

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            class _ConnFails:
                async def login(self) -> None:
                    call_log.append("fail-start")
                    await asyncio.sleep(0)  # let sibling reach its await point
                    raise RuntimeError("boom")

            class _ConnSlow:
                async def login(self) -> None:
                    call_log.append("slow-start")
                    try:
                        await asyncio.sleep(100)
                        call_log.append("slow-done")
                    except asyncio.CancelledError:
                        call_log.append("slow-cancelled")
                        raise

            return {"fail": _ConnFails(), "slow": _ConnSlow()}

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()

            async def _mock_run_failing(*args, **kwargs):
                lt = mock_orch.call_args.kwargs.get("login_tasks", {})
                await lt["fail"]  # propagates RuntimeError to trigger finally cleanup
                return 0

            mock_orch.return_value.run = _mock_run_failing
            with pytest.raises(SystemExit):
                await _run(args)

        assert "slow-cancelled" in call_log

    async def test_fast_source_listed_before_slow_connector_login_finishes(
        self, tmp_path: Path
    ) -> None:
        """Pipeline property: a connector's list starts as soon as its own login
        completes, without waiting for other connectors' logins."""
        config = AppConfig(cache_dir=tmp_path, connectors=(), sync_groups=())
        args = argparse.Namespace(
            config=tmp_path / "unused.json",
            start=_START,
            end=_END,
            force=False,
            creds_json=None,
            creds_keepass=None,
        )
        slow_done_when_fast_listed: list[bool] = []

        async def _fake_build_connectors(
            _cfg, _provider, _tracker, on_strava_token_refresh=None
        ):
            class _ConnFast:
                async def login(self) -> None:
                    pass  # completes immediately

            class _ConnSlow:
                async def login(self) -> None:
                    await asyncio.sleep(100)  # still running when fast lists

            return {"fast": _ConnFast(), "slow": _ConnSlow()}

        with (
            patch("app.cli.load_config", return_value=config),
            patch("app.cli.build_connectors", new=_fake_build_connectors),
            patch("app.cli.SyncOrchestrator") as mock_orch,
            patch("app.cli.ActivityCache") as mock_cache,
            patch("app.cli.ConsoleRenderer"),
        ):
            mock_cache.return_value.load = MagicMock()

            async def _mock_run_pipeline(*args, **kwargs):
                lt = mock_orch.call_args.kwargs.get("login_tasks", {})
                await lt["fast"]  # simulate fast source starting list_activities
                slow_done_when_fast_listed.append(lt["slow"].done())
                lt["slow"].cancel()
                await asyncio.gather(lt["slow"], return_exceptions=True)
                return 0

            mock_orch.return_value.run = _mock_run_pipeline
            await _run(args)

        assert slow_done_when_fast_listed == [False]


class TestMainIfNameMain:
    def test_if_name_main_block_is_covered(self) -> None:
        """Cover the if __name__ == '__main__' guard in app/cli.py."""
        import runpy

        with (
            patch("app.cli._build_arg_parser") as mock_parser,
            patch("app.cli._run", new=AsyncMock(side_effect=SystemExit(0))),
            pytest.raises(SystemExit),
        ):
            mock_parser.return_value.parse_args.return_value = argparse.Namespace()
            import app.cli as _cli_mod

            runpy.run_path(
                str(Path(_cli_mod.__file__)),  # type: ignore[arg-type]
                run_name="__main__",
            )

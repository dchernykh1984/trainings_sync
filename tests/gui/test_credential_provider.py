"""Tests for app.gui.credential_provider.GuiCredentialProvider."""

from __future__ import annotations

import asyncio

import pytest

from app.credentials.base import (
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
)
from app.gui.config_store import CredentialEntry
from app.gui.credential_provider import GuiCredentialProvider
from app.tracking.tracker import ProgressRenderer, Task, TaskTracker


class _NullRenderer(ProgressRenderer):
    def on_task_added(self, task: Task) -> None: ...
    def on_progress(self, task: Task) -> None: ...
    def on_task_done(self, task: Task) -> None: ...
    def on_task_failed(self, task: Task) -> None: ...
    def on_task_warning(self, task: Task, message: str) -> None: ...
    def on_total_updated(self, task: Task) -> None: ...


def _tracker() -> TaskTracker:
    return TaskTracker(_NullRenderer())


def test_manual_credential_returns_inline_password() -> None:
    entries = [
        CredentialEntry("Garmin Connect", "https://connect.garmin.com", "me@x", "pw")
    ]
    provider = GuiCredentialProvider(entries, {}, _tracker())
    creds = asyncio.run(
        provider.get_credentials(
            CredentialRequest("Garmin Connect", "https://connect.garmin.com", "me@x")
        )
    )
    assert creds == Credentials(login="me@x", password="pw")


def test_no_matching_entry_raises() -> None:
    provider = GuiCredentialProvider([], {}, _tracker())
    with pytest.raises(CredentialsNotFoundError):
        asyncio.run(
            provider.get_credentials(CredentialRequest("Nope", "http://x", None))
        )


def test_keepass_credential_delegates_to_keepass_provider(monkeypatch) -> None:
    constructed: dict[str, object] = {}

    class _FakeKeePass:
        def __init__(self, path, password, tracker) -> None:
            constructed["path"] = str(path)
            constructed["password"] = password

        async def get_credentials(self, request: CredentialRequest) -> Credentials:
            return Credentials(login="kp-login", password="kp-secret")

    import app.credentials.keepass as keepass_mod

    monkeypatch.setattr(keepass_mod, "KeePassProvider", _FakeKeePass)

    entries = [
        CredentialEntry(
            "Garmin Connect",
            "https://connect.garmin.com",
            "me@x",
            source="keepass",
            keepass_path="/home/me/db.kdbx",
        )
    ]
    provider = GuiCredentialProvider(
        entries, {"/home/me/db.kdbx": "master-pw"}, _tracker()
    )
    creds = asyncio.run(
        provider.get_credentials(
            CredentialRequest("Garmin Connect", "https://connect.garmin.com", "me@x")
        )
    )
    assert creds == Credentials(login="kp-login", password="kp-secret")
    assert constructed["password"] == "master-pw"
    assert constructed["path"] == "/home/me/db.kdbx"


def test_keepass_provider_is_cached_per_path(monkeypatch) -> None:
    calls: list[str] = []

    class _FakeKeePass:
        def __init__(self, path, password, tracker) -> None:
            calls.append(str(path))

        async def get_credentials(self, request: CredentialRequest) -> Credentials:
            return Credentials(login="l", password="p")

    import app.credentials.keepass as keepass_mod

    monkeypatch.setattr(keepass_mod, "KeePassProvider", _FakeKeePass)

    entries = [
        CredentialEntry(
            "A", "http://a", "la", source="keepass", keepass_path="/db.kdbx"
        ),
        CredentialEntry(
            "B", "http://b", "lb", source="keepass", keepass_path="/db.kdbx"
        ),
    ]
    provider = GuiCredentialProvider(entries, {"/db.kdbx": "pw"}, _tracker())
    asyncio.run(provider.get_credentials(CredentialRequest("A", "http://a", "la")))
    asyncio.run(provider.get_credentials(CredentialRequest("B", "http://b", "lb")))
    # Same .kdbx -> the provider (and its opened database) is built only once.
    assert calls == ["/db.kdbx"]


def test_update_refresh_token_updates_manual_and_notifies() -> None:
    saved: list[list[CredentialEntry]] = []
    entries = [
        CredentialEntry("Strava", "https://www.strava.com/api/v3", "cs", "old-token")
    ]
    provider = GuiCredentialProvider(
        entries, {}, _tracker(), on_manual_update=saved.append
    )
    provider.update_refresh_token(
        CredentialRequest("Strava", "https://www.strava.com/api/v3", "cs"),
        "new-token",
    )
    assert entries[0].password == "new-token"
    assert saved and saved[0][0].password == "new-token"


def test_update_refresh_token_unknown_raises() -> None:
    provider = GuiCredentialProvider([], {}, _tracker())
    with pytest.raises(CredentialsNotFoundError):
        provider.update_refresh_token(
            CredentialRequest("Strava", "http://x", "cs"), "tok"
        )

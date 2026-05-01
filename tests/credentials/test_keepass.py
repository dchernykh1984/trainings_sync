from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pykeepass.exceptions import CredentialsError  # type: ignore[import-untyped]

from app.credentials.base import (
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
    InvalidMasterPasswordError,
)
from app.credentials.keepass import KeePassProvider
from app.tracking.tracker import ProgressRenderer, Task, TaskStatus, TaskTracker


class _FakeRenderer(ProgressRenderer):
    def on_task_added(self, task: Task) -> None:
        pass

    def on_progress(self, task: Task) -> None:
        pass

    def on_task_done(self, task: Task) -> None:
        pass

    def on_task_failed(self, task: Task) -> None:
        pass

    def on_task_warning(self, task: Task, message: str) -> None:
        pass


def _make_entry(
    username: str, password: str, url: str = "https://garmin.com"
) -> MagicMock:
    entry = MagicMock()
    entry.username = username
    entry.password = password
    entry.url = url
    return entry


@pytest.fixture
def tracker() -> TaskTracker:
    return TaskTracker(_FakeRenderer())


@pytest.fixture
def provider(tracker: TaskTracker) -> KeePassProvider:
    return KeePassProvider(path=Path("dummy.kdbx"), password="master", tracker=tracker)


@pytest.fixture
def mock_db() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_open_db(mock_db: MagicMock):
    with patch(
        "app.credentials.keepass.asyncio.to_thread",
        new_callable=AsyncMock,
        return_value=mock_db,
    ):
        yield


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


class TestGetCredentials:
    async def test_returns_credentials(
        self, provider: KeePassProvider, mock_db: MagicMock, mock_open_db: None
    ) -> None:
        mock_db.entries = [_make_entry("alice", "s3cr3t")]
        request = CredentialRequest(
            service="garmin", url="https://garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result == Credentials(login="alice", password="s3cr3t")

    async def test_raises_when_not_found(
        self, provider: KeePassProvider, mock_db: MagicMock, mock_open_db: None
    ) -> None:
        mock_db.entries = []
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_credentials(request)

    async def test_task_fails_when_not_found(
        self,
        provider: KeePassProvider,
        tracker: TaskTracker,
        mock_db: MagicMock,
        mock_open_db: None,
    ) -> None:
        mock_db.entries = []
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_warns_on_duplicate_entries(
        self,
        provider: KeePassProvider,
        tracker: TaskTracker,
        mock_db: MagicMock,
        mock_open_db: None,
    ) -> None:
        mock_db.entries = [
            _make_entry("alice", "s3cr3t"),
            _make_entry("alice", "other"),
        ]
        request = CredentialRequest(
            service="garmin", url="https://garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result.login == "alice"
        assert _first_task(tracker).warnings != []

    async def test_matches_by_partial_url(
        self, provider: KeePassProvider, mock_db: MagicMock, mock_open_db: None
    ) -> None:
        mock_db.entries = [
            _make_entry("alice", "s3cr3t", url="https://connect.garmin.com/app/")
        ]
        request = CredentialRequest(
            service="garmin", url="connect.garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result == Credentials(login="alice", password="s3cr3t")

    async def test_service_field_is_ignored(
        self, provider: KeePassProvider, mock_db: MagicMock, mock_open_db: None
    ) -> None:
        mock_db.entries = [_make_entry("alice", "s3cr3t")]
        request = CredentialRequest(
            service="strava", url="https://garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result == Credentials(login="alice", password="s3cr3t")

    async def test_fails_on_invalid_master_password(
        self, provider: KeePassProvider, tracker: TaskTracker
    ) -> None:
        with patch(
            "app.credentials.keepass.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=CredentialsError("Invalid credentials"),
        ):
            request = CredentialRequest(service="garmin", url="https://garmin.com")
            with pytest.raises(InvalidMasterPasswordError):
                await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_fails_when_db_open_raises(
        self, provider: KeePassProvider, tracker: TaskTracker
    ) -> None:
        with patch(
            "app.credentials.keepass.asyncio.to_thread",
            new_callable=AsyncMock,
            side_effect=OSError("file not found"),
        ):
            request = CredentialRequest(service="garmin", url="https://garmin.com")
            with pytest.raises(OSError):
                await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_unique_task_name_on_repeated_calls(
        self, provider: KeePassProvider, mock_db: MagicMock, mock_open_db: None
    ) -> None:
        mock_db.entries = [_make_entry("alice", "pass")]
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        await provider.get_credentials(request)
        await provider.get_credentials(request)

        assert len(provider._tracker.tasks) == 2


class TestGetMany:
    async def test_returns_empty_list_without_opening_db(
        self, provider: KeePassProvider
    ) -> None:
        with patch(
            "app.credentials.keepass.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_to_thread:
            result = await provider.get_many([])
            mock_to_thread.assert_not_called()

        assert result == []

    async def test_opens_db_once_for_multiple_requests(
        self, provider: KeePassProvider, mock_db: MagicMock
    ) -> None:
        mock_db.entries = [
            _make_entry("alice", "pass1", url="https://garmin.com"),
            _make_entry("bob", "pass2", url="https://strava.com"),
        ]
        requests = [
            CredentialRequest(
                service="garmin", url="https://garmin.com", login="alice"
            ),
            CredentialRequest(service="strava", url="https://strava.com", login="bob"),
        ]
        with patch(
            "app.credentials.keepass.asyncio.to_thread",
            new_callable=AsyncMock,
            return_value=mock_db,
        ) as mock_to_thread:
            results = await provider.get_many(requests)
            mock_to_thread.assert_called_once()

        assert results == [
            Credentials(login="alice", password="pass1"),
            Credentials(login="bob", password="pass2"),
        ]

    async def test_raises_when_credential_not_found(
        self, provider: KeePassProvider, mock_db: MagicMock, mock_open_db: None
    ) -> None:
        mock_db.entries = []
        requests = [CredentialRequest(service="garmin", url="https://garmin.com")]

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_many(requests)

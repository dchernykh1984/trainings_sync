from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from app.credentials.base import (
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
)
from app.credentials.json_file import JsonFileProvider
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

    def on_total_updated(self, task: Task) -> None:
        pass


_ENTRIES = [
    {
        "service": "garmin",
        "url": "https://garmin.com",
        "login": "alice",
        "password": "pass1",
    },
    {
        "service": "strava",
        "url": "https://strava.com",
        "login": "bob",
        "password": "pass2",
    },
]


def _make_provider(
    tmp_path: Path, data: object = _ENTRIES
) -> tuple[JsonFileProvider, TaskTracker]:
    creds_file = tmp_path / "creds.json"
    creds_file.write_text(json.dumps(data), encoding="utf-8")
    tracker = TaskTracker(_FakeRenderer())
    return JsonFileProvider(path=creds_file, tracker=tracker), tracker


def _first_task(tracker: TaskTracker) -> Task:
    return next(iter(tracker.tasks.values()))


class TestGetCredentials:
    async def test_returns_credentials(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest(
            service="garmin", url="https://garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result == Credentials(login="alice", password="pass1")

    async def test_raises_when_not_found(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest(service="garmin", url="https://unknown.com")

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_credentials(request)

    async def test_different_service_does_not_match(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest(
            service="other", url="https://garmin.com", login="alice"
        )

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_credentials(request)

    async def test_task_fails_when_not_found(self, tmp_path: Path) -> None:
        provider, tracker = _make_provider(tmp_path)
        request = CredentialRequest(service="garmin", url="https://unknown.com")

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_warns_on_duplicate_entries(self, tmp_path: Path) -> None:
        data = [
            {
                "service": "garmin",
                "url": "https://garmin.com",
                "login": "alice",
                "password": "pass1",
            },
            {
                "service": "garmin",
                "url": "https://garmin.com",
                "login": "alice",
                "password": "pass2",
            },
        ]
        provider, tracker = _make_provider(tmp_path, data)
        request = CredentialRequest(
            service="garmin", url="https://garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result == Credentials(login="alice", password="pass1")
        assert _first_task(tracker).warnings != []

    async def test_fails_when_file_is_missing(self, tmp_path: Path) -> None:
        tracker = TaskTracker(_FakeRenderer())
        provider = JsonFileProvider(path=tmp_path / "missing.json", tracker=tracker)
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(FileNotFoundError):
            await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_fails_when_top_level_is_not_a_list(self, tmp_path: Path) -> None:
        provider, tracker = _make_provider(tmp_path, data={"key": "value"})
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(ValueError, match="Expected a JSON array"):
            await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_fails_when_item_is_not_a_dict(self, tmp_path: Path) -> None:
        provider, tracker = _make_provider(tmp_path, data=["bad-entry"])
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(ValueError, match="not an object"):
            await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_fails_when_required_field_is_missing(self, tmp_path: Path) -> None:
        data = [{"service": "garmin", "url": "https://garmin.com", "login": "alice"}]
        provider, tracker = _make_provider(tmp_path, data)
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(ValueError, match="password"):
            await provider.get_credentials(request)

        assert _first_task(tracker).status == TaskStatus.FAILED

    async def test_fails_when_field_is_not_a_string(self, tmp_path: Path) -> None:
        data = [
            {
                "service": "garmin",
                "url": "https://garmin.com",
                "login": "alice",
                "password": 12345,
            }
        ]
        provider, _ = _make_provider(tmp_path, data)
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        with pytest.raises(ValueError, match="password"):
            await provider.get_credentials(request)

    async def test_unique_task_name_on_repeated_calls(self, tmp_path: Path) -> None:
        provider, tracker = _make_provider(tmp_path)
        request = CredentialRequest(
            service="garmin", url="https://garmin.com", login="alice"
        )

        await provider.get_credentials(request)
        await provider.get_credentials(request)

        assert len(tracker.tasks) == 2

    async def test_matches_by_url_only_when_login_is_none(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest(service="garmin", url="https://garmin.com")

        result = await provider.get_credentials(request)

        assert result.login == "alice"

    async def test_matches_by_partial_url(self, tmp_path: Path) -> None:
        data = [
            {
                "service": "garmin",
                "url": "https://connect.garmin.com/app/",
                "login": "alice",
                "password": "pass1",
            }
        ]
        provider, _ = _make_provider(tmp_path, data)
        request = CredentialRequest(
            service="garmin", url="connect.garmin.com", login="alice"
        )

        result = await provider.get_credentials(request)

        assert result == Credentials(login="alice", password="pass1")


class TestGetMany:
    async def test_returns_empty_list_without_reading_file(
        self, tmp_path: Path
    ) -> None:
        provider, _ = _make_provider(tmp_path)
        with patch(
            "app.credentials.json_file.asyncio.to_thread",
            new_callable=AsyncMock,
        ) as mock_to_thread:
            result = await provider.get_many([])
            mock_to_thread.assert_not_called()

        assert result == []

    async def test_reads_file_once_for_multiple_requests(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        requests = [
            CredentialRequest(
                service="garmin", url="https://garmin.com", login="alice"
            ),
            CredentialRequest(service="strava", url="https://strava.com", login="bob"),
        ]

        results = await provider.get_many(requests)

        assert results == [
            Credentials(login="alice", password="pass1"),
            Credentials(login="bob", password="pass2"),
        ]

    async def test_raises_when_credential_not_found(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        requests = [CredentialRequest(service="x", url="https://unknown.com")]

        with pytest.raises(CredentialsNotFoundError):
            await provider.get_many(requests)


class TestUpdateRefreshToken:
    def test_updates_password_in_matching_entry(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest("garmin", "https://garmin.com", login="alice")

        provider.update_refresh_token(request, "new-token")

        data = json.loads((tmp_path / "creds.json").read_text(encoding="utf-8"))
        alice = next(e for e in data if e["login"] == "alice")
        assert alice["password"] == "new-token"

    def test_other_entries_are_unchanged(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest("garmin", "https://garmin.com", login="alice")

        provider.update_refresh_token(request, "new-token")

        data = json.loads((tmp_path / "creds.json").read_text(encoding="utf-8"))
        bob = next(e for e in data if e["login"] == "bob")
        assert bob["password"] == "pass2"

    def test_raises_when_entry_not_found(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest("garmin", "https://unknown.com")

        with pytest.raises(CredentialsNotFoundError):
            provider.update_refresh_token(request, "new-token")

    def test_file_not_written_when_entry_not_found(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest("garmin", "https://unknown.com")
        original = (tmp_path / "creds.json").read_bytes()

        with pytest.raises(CredentialsNotFoundError):
            provider.update_refresh_token(request, "new-token")

        assert (tmp_path / "creds.json").read_bytes() == original

    def test_no_tmp_file_remains_after_successful_update(self, tmp_path: Path) -> None:
        provider, _ = _make_provider(tmp_path)
        request = CredentialRequest("garmin", "https://garmin.com", login="alice")

        provider.update_refresh_token(request, "new-token")

        assert not (tmp_path / "creds.tmp").exists()

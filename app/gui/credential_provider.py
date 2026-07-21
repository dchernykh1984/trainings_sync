"""Credential provider that resolves the GUI's per-account credential entries.

Manual accounts return their inline password; KeePass accounts delegate to a
KeePassProvider for their .kdbx file (opened with a master password supplied
per file). Strava token refreshes are written back to manual accounts only.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from app.credentials.base import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
)
from app.gui.config_store import CredentialEntry
from app.tracking.tracker import TaskTracker


def find_credential(
    entries: list[CredentialEntry], service: str, url: str, login: str | None
) -> CredentialEntry | None:
    """Find the credential a connector references (service + url + login)."""
    matches = [
        e
        for e in entries
        if e.service == service and url in e.url and (not login or e.login == login)
    ]
    return matches[0] if matches else None


class GuiCredentialProvider(CredentialProvider):
    def __init__(
        self,
        entries: list[CredentialEntry],
        keepass_passwords: dict[str, str],
        tracker: TaskTracker,
        on_manual_update: Callable[[list[CredentialEntry]], None] | None = None,
    ) -> None:
        self._entries = entries
        self._keepass_passwords = keepass_passwords
        self._tracker = tracker
        self._on_manual_update = on_manual_update
        self._keepass_cache: dict[str, CredentialProvider] = {}

    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        entry = self._match(request)
        if entry is None:
            raise CredentialsNotFoundError(request)
        if entry.source == "keepass":
            provider = self._keepass_provider(entry.keepass_path)
            return await provider.get_credentials(request)
        return await self._get_manual(request, entry)

    def _match(self, request: CredentialRequest) -> CredentialEntry | None:
        return find_credential(
            self._entries, request.service, request.url, request.login
        )

    def _keepass_provider(self, path: str) -> CredentialProvider:
        if path not in self._keepass_cache:
            from app.credentials.keepass import KeePassProvider

            self._keepass_cache[path] = KeePassProvider(
                path=Path(path).expanduser(),
                password=self._keepass_passwords.get(path, ""),
                tracker=self._tracker,
            )
        return self._keepass_cache[path]

    async def _get_manual(
        self, request: CredentialRequest, entry: CredentialEntry
    ) -> Credentials:
        task = await self._tracker.add_task(f"Credentials ({request.service})", total=1)
        await self._tracker.advance(task)
        await self._tracker.finish(task)
        return Credentials(login=entry.login, password=entry.password)

    def update_refresh_token(self, request: CredentialRequest, new_token: str) -> None:
        for entry in self._entries:
            if (
                entry.source == "manual"
                and entry.service == request.service
                and request.url in entry.url
                and (request.login is None or entry.login == request.login)
            ):
                entry.password = new_token
                break
        else:
            raise CredentialsNotFoundError(request)
        if self._on_manual_update is not None:
            self._on_manual_update(self._entries)

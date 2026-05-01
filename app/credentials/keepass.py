from __future__ import annotations

import asyncio
import itertools
from collections.abc import Sequence
from pathlib import Path

from pykeepass import PyKeePass  # type: ignore[import-untyped]
from pykeepass.exceptions import CredentialsError  # type: ignore[import-untyped]

from app.credentials.base import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
    InvalidMasterPasswordError,
)
from app.tracking.tracker import TaskTracker


class KeePassProvider(CredentialProvider):
    def __init__(self, path: Path, password: str, tracker: TaskTracker) -> None:
        self._path = path
        self._password = password
        self._tracker = tracker
        self._counter = itertools.count(1)

    def _task_name(self, label: str) -> str:
        return f"{label} #{next(self._counter)}"

    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        task_name = self._task_name(f"KeePass: {request.service}")
        await self._tracker.add_task(task_name, total=2)

        db = await self._open_db(task_name)
        await self._tracker.advance(task_name)

        credentials = await self._find(db, request, task_name)
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)
        return credentials

    async def get_many(
        self, requests: Sequence[CredentialRequest]
    ) -> list[Credentials]:
        if not requests:
            return []

        task_name = self._task_name("KeePass credentials")
        await self._tracker.add_task(task_name, total=1 + len(requests))

        db = await self._open_db(task_name)
        await self._tracker.advance(task_name)

        results: list[Credentials] = []
        for request in requests:
            credentials = await self._find(db, request, task_name)
            results.append(credentials)
            await self._tracker.advance(task_name)

        await self._tracker.finish(task_name)
        return results

    async def _open_db(self, task_name: str) -> PyKeePass:
        try:
            return await asyncio.to_thread(
                PyKeePass, str(self._path), password=self._password
            )
        except CredentialsError as exc:
            await self._tracker.fail(task_name, error=f"Failed to open database: {exc}")
            raise InvalidMasterPasswordError(str(self._path)) from exc
        except Exception as exc:
            await self._tracker.fail(task_name, error=f"Failed to open database: {exc}")
            raise

    async def _find(
        self, db: PyKeePass, request: CredentialRequest, task_name: str
    ) -> Credentials:
        entries = [
            e
            for e in db.entries
            if request.url in (e.url or "")
            and (request.login is None or e.username == request.login)
        ]

        if not entries:
            error = f"No credentials found for {request.service!r}"
            await self._tracker.fail(task_name, error=error)
            raise CredentialsNotFoundError(request)

        if len(entries) > 1:
            await self._tracker.warn(
                task_name,
                f"Multiple entries for {request.service!r}, using first",
            )

        entry = entries[0]
        return Credentials(
            login=entry.username or "",
            password=entry.password or "",
        )

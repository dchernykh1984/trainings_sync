from __future__ import annotations

import asyncio
import itertools
import json
from collections.abc import Sequence
from pathlib import Path

from app.credentials.base import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
    CredentialsNotFoundError,
)
from app.tracking.tracker import TaskTracker

_REQUIRED_FIELDS = ("service", "url", "login", "password")


class JsonFileProvider(CredentialProvider):
    def __init__(self, path: Path, tracker: TaskTracker) -> None:
        self._path = path
        self._tracker = tracker
        self._counter = itertools.count(1)

    def _task_name(self, label: str) -> str:
        return f"{label} #{next(self._counter)}"

    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        task_name = self._task_name(f"JSON credentials: {request.service}")
        await self._tracker.add_task(task_name, total=2)

        entries = await self._load(task_name)
        await self._tracker.advance(task_name)

        credentials = await self._find(entries, request, task_name)
        await self._tracker.advance(task_name)
        await self._tracker.finish(task_name)
        return credentials

    async def get_many(
        self, requests: Sequence[CredentialRequest]
    ) -> list[Credentials]:
        if not requests:
            return []

        task_name = self._task_name("JSON credentials")
        await self._tracker.add_task(task_name, total=1 + len(requests))

        entries = await self._load(task_name)
        await self._tracker.advance(task_name)

        results: list[Credentials] = []
        for request in requests:
            credentials = await self._find(entries, request, task_name)
            results.append(credentials)
            await self._tracker.advance(task_name)

        await self._tracker.finish(task_name)
        return results

    async def _load(self, task_name: str) -> list[dict[str, str]]:
        try:
            data = await asyncio.to_thread(
                lambda: json.loads(self._path.read_text(encoding="utf-8"))
            )
        except Exception as exc:
            await self._tracker.fail(task_name, error=f"Failed to read file: {exc}")
            raise

        if not isinstance(data, list):
            error = f"Expected a JSON array, got {type(data).__name__}"
            await self._tracker.fail(task_name, error=error)
            raise ValueError(error)

        for i, item in enumerate(data):
            if not isinstance(item, dict):
                error = f"Entry {i} is not an object"
                await self._tracker.fail(task_name, error=error)
                raise ValueError(error)
            for field in _REQUIRED_FIELDS:
                if not isinstance(item.get(field), str):
                    error = f"Entry {i}: field {field!r} is missing or not a string"
                    await self._tracker.fail(task_name, error=error)
                    raise ValueError(error)

        return data

    async def _find(
        self,
        entries: list[dict[str, str]],
        request: CredentialRequest,
        task_name: str,
    ) -> Credentials:
        matches = [
            e
            for e in entries
            if e["service"] == request.service
            and request.url in e["url"]
            and (request.login is None or e["login"] == request.login)
        ]

        if not matches:
            error = f"No credentials found for {request.service!r}"
            await self._tracker.fail(task_name, error=error)
            raise CredentialsNotFoundError(request)

        if len(matches) > 1:
            await self._tracker.warn(
                task_name,
                f"Multiple entries for {request.service!r}, using first",
            )

        entry = matches[0]
        return Credentials(login=entry["login"], password=entry["password"])

    def update_refresh_token(self, request: CredentialRequest, new_token: str) -> None:
        data = json.loads(self._path.read_text(encoding="utf-8"))
        for entry in data:
            if (
                entry["service"] == request.service
                and request.url in entry["url"]
                and (request.login is None or entry["login"] == request.login)
            ):
                entry["password"] = new_token
                break
        self._path.write_text(json.dumps(data, indent=2), encoding="utf-8")

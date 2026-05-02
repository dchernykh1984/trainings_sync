from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class CredentialRequest:
    service: str
    url: str
    login: str | None = None


@dataclass(frozen=True)
class Credentials:
    login: str
    password: str


@dataclass(frozen=True)
class StravaCredentials:
    client_id: int
    client_secret: str
    refresh_token: str


class CredentialsNotFoundError(Exception):
    def __init__(self, request: CredentialRequest) -> None:
        super().__init__(
            f"No credentials found for {request.service!r} at {request.url!r}"
        )
        self.request = request


class InvalidMasterPasswordError(Exception):
    def __init__(self, path: str) -> None:
        super().__init__(f"Invalid master password for database {path!r}")
        self.path = path


class CredentialProvider(ABC):
    @abstractmethod
    async def get_credentials(self, request: CredentialRequest) -> Credentials: ...

    async def get_many(
        self, requests: Sequence[CredentialRequest]
    ) -> list[Credentials]:
        return [await self.get_credentials(request) for request in requests]

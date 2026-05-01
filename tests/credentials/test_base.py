from app.credentials.base import (
    CredentialProvider,
    CredentialRequest,
    Credentials,
)


class _FakeProvider(CredentialProvider):
    async def get_credentials(self, request: CredentialRequest) -> Credentials:
        return Credentials(login=request.login or "default", password="secret")


class TestGetMany:
    async def test_returns_credentials_in_order(self) -> None:
        provider = _FakeProvider()
        requests = [
            CredentialRequest(
                service="garmin", url="https://garmin.com", login="alice"
            ),
            CredentialRequest(service="strava", url="https://strava.com", login="bob"),
        ]
        result = await provider.get_many(requests)

        assert len(result) == 2
        assert result[0].login == "alice"
        assert result[1].login == "bob"

    async def test_returns_empty_list_for_no_requests(self) -> None:
        provider = _FakeProvider()
        result = await provider.get_many([])

        assert result == []

    async def test_handles_duplicate_services(self) -> None:
        provider = _FakeProvider()
        requests = [
            CredentialRequest(
                service="strava", url="https://strava.com", login="alice"
            ),
            CredentialRequest(service="strava", url="https://strava.com", login="bob"),
        ]
        result = await provider.get_many(requests)

        assert len(result) == 2
        assert result[0].login == "alice"
        assert result[1].login == "bob"

import asyncio
from types import TracebackType
from typing import Any, Callable, Coroutine, Generator, Type
import aiohttp
import jwt
from datetime import datetime, timedelta


_LAG_MULTIPLIER = 0.9


# Needed because `asyncio` has its own Awaitable type we can't reuse.
class _AuthResponseContextManager(Coroutine[Any, Any, aiohttp.ClientResponse]):
    _coro: Coroutine[Any, Any, aiohttp.ClientResponse]
    _response: aiohttp.ClientResponse | None
    __slots__ = ("_coro", "_response")

    def __init__(self, coro: Coroutine[Any, Any, aiohttp.ClientResponse]):
        self._coro = coro

    def send(self, arg: None) -> asyncio.Future[Any]:
        return self._coro.send(arg)

    def throw(self, *args: Any, **kwargs: Any) -> asyncio.Future[Any]:
        return self._coro.throw(*args, **kwargs)

    def close(self) -> None:
        return self._coro.close()

    def __await__(self) -> Generator[Any, None, aiohttp.ClientResponse]:
        ret = self._coro.__await__()
        return ret

    async def __aenter__(self):
        self._response = await self._coro
        return self._response

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ):
        assert self._response is not None
        self._response.release()
        await self._response.wait_for_close()


class OzmaAuth:
    _api_url = "https://account.ozma.io/auth/realms/default/.well-known/openid-configuration"
    _client_id: str
    _client_secret: str
    _login: str | None = None
    _password: str | None = None
    _lock: asyncio.Lock

    _token_url: str | None = None
    _signing_algorithms: list[str] | None = None
    _jwks_client: jwt.PyJWKClient | None = None

    _access_token: str | None = None
    _decoded_access_token: dict[str, Any] | None = None
    _access_expired_at: datetime | None = None

    _refresh_token: str | None = None
    _decoded_refresh_token: dict[str, Any] | None = None
    _refresh_expired_at: datetime | None = None

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        api_url: str | None = None,
        login: str | None = None,
        password: str | None = None,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        if (login is None) ^ (password is None):
            raise ValueError("Login and password must both be set")
        if api_url is not None:
            self._api_url = api_url
        self._login = login
        self._password = password
        self._lock = asyncio.Lock()

    async def _get_configuration(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self._api_url) as response:
                response.raise_for_status()
                ret = await response.json()
                self._token_url = ret["token_endpoint"]
                self._signing_algorithms = ret["id_token_signing_alg_values_supported"]
                self._jwks_client = jwt.PyJWKClient(ret["jwks_uri"])

    async def _ensure_configuration(self):
        if self._token_url is None:
            await self._get_configuration()

    async def _parse_response(self, response: dict[str, Any]):
        assert self._signing_algorithms is not None
        assert self._jwks_client is not None
        now = datetime.now()

        access_token = response["access_token"]
        signing_key = await asyncio.to_thread(self._jwks_client.get_signing_key_from_jwt, access_token)

        def decode_jwt(token: str):
            return jwt.decode(
                token,
                signing_key.key,
                algorithms=self._signing_algorithms,
                audience="account",
            )

        decoded_access_token = decode_jwt(access_token)
        if "refresh_token" in response:
            refresh_token = response["refresh_token"]
            decoded_refresh_token = decode_jwt(refresh_token)
        else:
            refresh_token = None
            decoded_refresh_token = None

        self._access_token = access_token
        self._decoded_access_token = decoded_access_token
        self._access_expired_at = now + _LAG_MULTIPLIER * timedelta(seconds=response["expires_in"])
        if refresh_token is not None:
            self._refresh_token = refresh_token
            self._decoded_refresh_token = decoded_refresh_token
            self._refresh_expired_at = now + _LAG_MULTIPLIER * timedelta(seconds=response["refresh_expires_in"])

    async def _get_tokens(self):
        await self._ensure_configuration()
        assert self._token_url is not None

        if self._login is None:
            # Authenticate with a service account.
            data = {
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            }
        else:
            # Authenticate with a direct grant.
            data = {
                "grant_type": "password",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "username": self._login,
                "password": self._password,
            }

        async with aiohttp.ClientSession() as session:
            async with session.post(self._token_url, data=data) as response:
                response.raise_for_status()
                ret = await response.json()
                await self._parse_response(ret)

    async def _refresh_tokens(self, refresh_token: str):
        await self._ensure_configuration()
        assert self._token_url is not None

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self._token_url,
                data={
                    "grant_type": "refresh_token",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "refresh_token": refresh_token,
                },
            ) as response:
                if response.status == 401:
                    # Refresh token is invalid, get a new one.
                    await self._get_tokens()
                    return
                response.raise_for_status()
                ret = await response.json()
                await self._parse_response(ret)

    def clear_tokens(self):
        self._access_token = None
        self._decoded_access_token = None
        self._access_expired_at = None

        self._refresh_token = None
        self._decoded_refresh_token = None
        self._refresh_expired_at = None

    async def ensure_token(self):
        async with self._lock:
            now = datetime.now()
            if self._access_expired_at is None or self._access_expired_at <= now:
                refresh_token = self._refresh_token
                refresh_expired_at = self._refresh_expired_at
                self.clear_tokens()
                if refresh_expired_at is not None and refresh_expired_at > now:
                    assert refresh_token is not None
                    await self._refresh_tokens(refresh_token)
                else:
                    await self._get_tokens()

    async def get_token(self):
        await self.ensure_token()
        return self._access_token

    async def _run_with_auth(
        self,
        func: Callable[..., Coroutine[Any, Any, aiohttp.ClientResponse]],
        *args,
        **kwargs,
    ):
        kwargs["headers"] = kwargs.get("headers", {})
        kwargs["headers"]["Authorization"] = f"Bearer {await self.get_token()}"
        response = await func(*args, **kwargs)
        if response.status == 401:
            response.release()
            await response.wait_for_close()
            self.clear_tokens()
            kwargs["headers"]["Authorization"] = f"Bearer {await self.get_token()}"
            response = await func(*args, **kwargs)
        return response

    def with_auth(
        self,
        func: Callable[..., Coroutine[Any, Any, aiohttp.ClientResponse]],
        *args,
        **kwargs,
    ):
        return _AuthResponseContextManager(self._run_with_auth(func, *args, **kwargs))

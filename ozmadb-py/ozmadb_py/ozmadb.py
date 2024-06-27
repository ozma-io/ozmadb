from urllib.parse import quote
from typing import Iterable, Literal
import aiohttp

from .types import (
    ActionName,
    ArgumentName,
    AttributeName,
    OzmaDBActionResponse,
    OzmaDBSavedSchemas,
    OzmaDBTransactionOp,
    OzmaDBTransactionResult,
    OzmaDBViewExprResult,
    OzmaQLValue,
    OzmaQLValueArg,
    RawOzmaQLValue,
    SchemaName,
    UserViewName,
    ozmaql_value_to_raw,
)
from .errors import check_ozma_response
from .auth import OzmaAuth


def ozmaql_name_to_str(v: str) -> str:
    escaped = v.replace('"', '\\"')
    return f'"{escaped}"'


def ozmaql_value_to_str(v: OzmaQLValue) -> str:
    match v:
        case None:
            return "NULL"
        case bool():
            return "TRUE" if v else "FALSE"
        case str():
            escaped = v.replace("'", "\\'")
            return f"'{escaped}'"
        case int() | float():
            return str(v)
        # TODO: Add support for datetime and timedelta.
        case _:
            raise TypeError(f"Invalid OzmaQL value type {type(v)}")


def dict_to_attributes(d: dict[AttributeName, OzmaQLValue]) -> str:
    escaped = ", ".join(f"{ozmaql_name_to_str(k)} = {ozmaql_value_to_str(v)}" for k, v in d.items())
    return "@{ " + escaped + " }"


# TODO: Convert manual dicts to Pydantic classes.
class OzmaDBAPI:
    BASE_URL = "https://{name}.api.ozma.org"
    _api_url: str
    _auth: OzmaAuth

    def __init__(self, auth: OzmaAuth, *, api_url: str | None = None, name: str | None = None):
        self._auth = auth
        if not ((api_url is not None) ^ (name is not None)):
            raise ValueError("Either api_url or name must be set")
        if api_url is not None:
            self._api_url = api_url
        else:
            self._api_url = self.BASE_URL.format(name=name)

    async def get_named_user_view(
        self, schema: SchemaName, name: UserViewName, args: dict[ArgumentName, OzmaQLValueArg] | None = None
    ) -> OzmaDBViewExprResult:
        async with aiohttp.ClientSession() as session:
            request = {}
            if args is not None:
                request["args"] = {name: ozmaql_value_to_raw(arg) for name, arg in args}
            async with self._auth.with_auth(
                session.post,
                f"{self._api_url}/views/by_name/{quote(schema)}/{quote(name)}/entries",
                headers={"Accept": "application/json"},
                json=request,
            ) as response:
                await check_ozma_response(response)
                ret = await response.json()
                return OzmaDBViewExprResult.model_validate(ret)

    async def run_transaction(
        self,
        operations: Iterable[OzmaDBTransactionOp],
        *,
        defer_constraints: bool = False,
        force_allow_broken: bool = False,
    ) -> OzmaDBTransactionResult:
        async with aiohttp.ClientSession() as session:
            params = {
                "operations": [op.model_dump(by_alias=True) for op in operations],
                "defer_constraints": defer_constraints,
                "force_allow_broken": force_allow_broken,
            }
            print(params)
            async with self._auth.with_auth(
                session.post,
                f"{self._api_url}/transaction",
                json=params,
            ) as response:
                await check_ozma_response(response)
                ret = await response.json()
                return OzmaDBTransactionResult.model_validate(ret)

    async def run_action(
        self, schema: SchemaName, name: ActionName, args: dict[str, OzmaQLValueArg] | None = None
    ) -> OzmaDBActionResponse:
        raw_args: dict[str, RawOzmaQLValue]
        if args is None:
            raw_args = {}
        else:
            raw_args = {name: ozmaql_value_to_raw(arg) for name, arg in args}
        async with aiohttp.ClientSession() as session:
            async with self._auth.with_auth(
                session.post,
                f"{self._api_url}/actions/{quote(schema)}/{quote(name)}/run",
                json=raw_args,
            ) as response:
                await check_ozma_response(response)
                ret = await response.json()
                return OzmaDBActionResponse.model_validate(ret)

    async def save_schemas(
        self, schemas: list[str] | Literal["all"], *, skip_preloaded: bool | None = None
    ) -> OzmaDBSavedSchemas:
        async with aiohttp.ClientSession() as session:
            params: dict[str, str | list[str]] = {}
            if schemas != "all":
                params["schema"] = schemas
            if skip_preloaded is not None:
                params["skip_preloaded"] = str(skip_preloaded)
            async with self._auth.with_auth(
                session.get,
                f"{self._api_url}/layouts",
                params=params,
                headers={"Accept": "application/json"},
            ) as response:
                await check_ozma_response(response)
                ret = await response.json()
                return OzmaDBSavedSchemas.model_validate(ret)

    async def restore_schemas(
        self,
        schemas: OzmaDBSavedSchemas,
        *,
        drop_others: bool | None = None,
        force_allow_broken: bool | None = None,
    ):
        async with aiohttp.ClientSession() as session:
            params: dict[str, str | list[str]] = {}
            if drop_others is not None:
                params["drop_others"] = str(drop_others)
            if force_allow_broken is not None:
                params["force_allow_broken"] = str(force_allow_broken)
            async with self._auth.with_auth(
                session.put,
                f"{self._api_url}/layouts",
                params=params,
                json=schemas.model_dump(by_alias=True)["schemas"],
            ) as response:
                await check_ozma_response(response)

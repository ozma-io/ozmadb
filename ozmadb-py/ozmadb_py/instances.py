from typing import Any
import aiohttp
from pydantic import BaseModel, field_serializer, field_validator
from datetime import datetime, timedelta
import urllib.parse

from .errors import check_ozma_response
from .auth import OzmaAuth


class InstancesModel(BaseModel, extra="ignore"):
    pass


class RateLimit(InstancesModel):
    limit: int
    period: timedelta

    @field_serializer("period", mode="plain")
    @staticmethod
    def serialize_period(value: timedelta) -> int:
        return int(value.total_seconds())

    @field_validator("period", mode="before")
    @staticmethod
    def validate_period(time) -> timedelta:
        match time:
            case timedelta():
                return time
            case int() | float():
                return timedelta(seconds=time)
            case _:
                raise ValueError("Invalid period")


class InstanceRateLimits(InstancesModel):
    max_size: int | None = None
    max_request_time: timedelta | None = None
    max_users: int | None = None
    read_rate_limits_per_user: list[RateLimit] | None = None
    write_rate_limits_per_user: list[RateLimit] | None = None

    @field_serializer("max_request_time", mode="plain")
    @staticmethod
    def serialize_max_request_time(value: timedelta | None) -> int | None:
        if value is None:
            return None
        return int(value.total_seconds() * 1000)

    @field_validator("max_request_time", mode="before")
    @staticmethod
    def validate_max_request_time(time) -> timedelta | None:
        match time:
            case timedelta():
                return time
            case int() | float():
                return timedelta(seconds=time)
            case _:
                raise ValueError("Invalid max_request_time")


class Instance(InstanceRateLimits):
    name: str
    owner: str
    region: str
    created_at: datetime
    accessed_at: datetime | None = None

    enabled: bool = True
    is_template: bool = False
    hidden: bool = False

    published: bool = True
    anyone_can_read: bool = False
    disable_security: bool = False


class NewInstance(InstanceRateLimits):
    name: str
    owner: str | None = None
    group: str | None = None
    randomize_name: bool = False

    hidden: bool = False
    is_template: bool = False

    published: bool = True
    disable_security: bool = False
    anyone_can_read: bool = False


class Instances(InstancesModel):
    instances: list[Instance]


class OzmaInstancesAPI:
    _api_url = "https://instances.services.ozma.io"
    _auth: OzmaAuth

    def __init__(self, auth: OzmaAuth, *, api_url: str | None = None):
        if api_url is not None:
            self._api_url = api_url
        self._auth = auth

    async def get_instances(
        self,
        *,
        owner: str | None = None,
        enabled_only: bool | None = None,
        or_template: bool | None = None,
    ) -> Instances:
        async with aiohttp.ClientSession() as session:
            params: dict[str, Any] = {}
            if owner is not None:
                params["owner"] = owner
            if enabled_only is not None:
                params["enabled_only"] = enabled_only
            if or_template is not None:
                params["or_template"] = or_template
            async with self._auth.with_auth(session.get, f"{self._api_url}/instances", params=params) as response:
                await check_ozma_response(response)
                ret = await response.json()
                return Instances.model_validate(ret)

    async def create_instance(self, instance: NewInstance) -> Instance:
        async with aiohttp.ClientSession() as session:
            async with self._auth.with_auth(
                session.post,
                f"{self._api_url}/instances",
                # FIXME: We have a mess with the default values vs `null` values in the instances service.
                # For now, just exclude the defaults.
                json=instance.model_dump(exclude_defaults=True, by_alias=True),
            ) as response:
                await check_ozma_response(response)
                ret = await response.json()
                return Instance.model_validate(ret)

    async def delete_instance(self, name: str) -> None:
        async with aiohttp.ClientSession() as session:
            async with self._auth.with_auth(
                session.delete,
                f"{self._api_url}/instances/{urllib.parse.quote(name, safe='')}",
            ) as response:
                await check_ozma_response(response)

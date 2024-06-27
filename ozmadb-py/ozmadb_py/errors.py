import aiohttp
from pydantic import BaseModel


class ErrorInfo(BaseModel, extra="allow"):
    error: str
    message: str


class OzmaError(Exception):
    _info: ErrorInfo

    def __init__(self, info: ErrorInfo):
        self._info = info
        super().__init__(info.message)

    @property
    def info(self):
        return self._info


async def check_ozma_response(response: aiohttp.ClientResponse):
    if response.status == 200:
        return
    raw_error_info = await response.json()
    raise OzmaError(ErrorInfo.model_validate(raw_error_info))

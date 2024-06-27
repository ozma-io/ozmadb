from dataclasses import dataclass
from enum import Enum
from typing import Literal


def ozmaql_name(name: str) -> str:
    return '"{}"'.format(name.replace('"', '""'))


def _ozmaql_string_char(char: str) -> str:
    if char == "\\":
        return "\\\\"
    elif char == "\b":
        return "\\b"
    elif char == "\f":
        return "\\f"
    elif char == "\n":
        return "\\n"
    elif char == "\r":
        return "\\r"
    elif char == "\t":
        return "\\t"
    elif char == "'":
        return "\\'"
    elif ord(char) < ord(" "):
        return "\\x{:02X}".format(ord(char))
    else:
        return char


def ozmaql_string(string: str) -> str:
    return "'{}'".format("".join((_ozmaql_string_char(char) for char in string)))


@dataclass
class OzmaQLRaw:
    raw: str


# This is a hack to allow `Literal[FUNQL_UNDEFINED]` to be used in `JsonInnerValue`.
class _OzmaQLUndefined(Enum):
    UNDEFINED = 0


FUNQL_UNDEFINED = _OzmaQLUndefined.UNDEFINED


JsonValue = None | str | int | float | bool | OzmaQLRaw | dict[str, "JsonInnerValue"] | list["JsonInnerValue"]
JsonInnerValue = JsonValue | Literal[FUNQL_UNDEFINED]


def ozmaql_json(json: JsonValue, *, pretty_offset: int | None = None, current_offset: int = 0) -> str:
    match json:
        case None:
            return "null"
        case str():
            return ozmaql_string(json)
        case int():
            return str(json)
        case float():
            return str(json)
        case bool():
            return "true" if json else "false"
        case OzmaQLRaw():
            return json.raw
        case dict():
            filtered_dict = {key: value for key, value in json.items() if value is not FUNQL_UNDEFINED}
            if len(filtered_dict) == 0:
                return "{}"
            elif pretty_offset is None:
                return (
                    "{"
                    + ", ".join(f"{ozmaql_name(key)}: {ozmaql_json(value)}" for key, value in filtered_dict.items())
                    + "}"
                )
            else:
                return (
                    "{\n"
                    + "".join(
                        f"{' ' * (current_offset + pretty_offset)}{ozmaql_name(key)}: {ozmaql_json(value, pretty_offset=pretty_offset, current_offset=current_offset + pretty_offset)},\n"
                        for key, value in filtered_dict.items()
                    )
                    + " " * current_offset
                    + "}"
                )
        case list():
            filtered_list = [value for value in json if value is not FUNQL_UNDEFINED]
            if len(filtered_list) == 0:
                return "[]"
            elif pretty_offset is None:
                return (
                    "[" + ", ".join(ozmaql_json(value, current_offset=current_offset) for value in filtered_list) + "]"
                )
            else:
                return (
                    "[\n"
                    + "".join(
                        f"{' ' * (current_offset + pretty_offset)}{ozmaql_json(value, pretty_offset=pretty_offset, current_offset=current_offset + pretty_offset)},\n"
                        for value in filtered_list
                    )
                    + " " * current_offset
                    + "]"
                )
        case _:
            raise TypeError(f"Invalid JSON value type {type(json)}")

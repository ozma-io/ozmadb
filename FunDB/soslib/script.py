from shlex import quote

from soslib import *


def __lldb_init_module(debugger, internal_dict):
    objs = get_heap_addresses(debugger, mt="00007ff48f223168")

    def get_connection_string(obj):
        info = dump_object(debugger, obj)
        if info.fields_by_name["_disposed"]["Value"] == "1":
            return None
        str_addr = info.fields_by_name["_connectionString"]["Value"]
        res = dump_object(debugger, str_addr).info["String"]
        print(res)
        return res

    print("Getting connections")
    addrs = [obj for obj in objs if (str := get_connection_string(obj)) is not None]

    for addr in addrs:
        print(f"\nConnection {addr}")
        debugger.HandleCommand(f"gcroot {quote(addr)}")

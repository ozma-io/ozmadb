from typing import Dict, List
import lldb
import shlex
from shlex import quote
import argparse
import re
from dataclasses import dataclass


def run_command(debugger, cmd):
    intr = debugger.GetCommandInterpreter()
    res = lldb.SBCommandReturnObject()
    intr.HandleCommand(cmd, res)
    if not res.Succeeded():
        err = res.GetError()
        raise RuntimeError(err)
    else:
        return res.GetOutput()


@dataclass
class DumpedObject:
    info: Dict[str, str]
    fields: List[Dict[str, str]]
    fields_by_field: Dict[str, Dict[str, str]]
    fields_by_name: Dict[str, Dict[str, str]]


DUMPOBJ_VALUES = re.compile(r"^([^ ]+): +(.+)$", re.MULTILINE)
DUMPOBJ_FIELDS = re.compile(r"^Fields:\n(.*)", re.MULTILINE | re.DOTALL)

def dump_object(debugger, address):
    raw_output = run_command(debugger, f"dumpobj {quote(address)}")
    options = {m.group(1): m.group(2) for m in DUMPOBJ_VALUES.finditer(raw_output)}
    raw_fields = [line for raw_line in DUMPOBJ_FIELDS.search(raw_output).group(1).splitlines() if (line := raw_line.strip()) != ""]
    entry_names = raw_fields[0].split()
    def make_entry(line):
        raw_cells = line.split(maxsplit=len(entry_names))
        return {name: value for name, value in zip(entry_names, raw_cells)}
    entry_lines = [make_entry(line) for line in raw_fields[1:]]
    fields_by_field = {line["Field"]: line for line in entry_lines}
    fields_by_name = {line["Name"]: line for line in entry_lines}
    return DumpedObject(
        info=options,
        fields=entry_lines,
        fields_by_field=fields_by_field,
        fields_by_name=fields_by_name,
    )


def get_heap_addresses(debugger, mt=None):
    args = ["dumpheap", "-short"]
    if mt is not None:
        args += ["-mt", mt]
    return run_command(debugger, " ".join(map(quote, args))).splitlines()


dumpfield_parser = argparse.ArgumentParser(prog="dumpfield")
dumpfield_parser.add_argument("address", metavar="ADDR")
dumpfield_parser.add_argument("fields", metavar="FIELD", nargs="*")

def dumpfield_command(debugger, command, result, internal_dict):
    raw_argv = shlex.split(command)
    args = dumpfield_parser.parse_args(raw_argv)
    addr = args.address
    for field in args.fields:
        addr = dump_object(debugger, addr).fields_by_name[field]["Value"]
    print(addr, file=result)


def __lldb_init_module(debugger, internal_dict):
    debugger.HandleCommand('command script add -f soslib.dumpfield_command dumpfield')

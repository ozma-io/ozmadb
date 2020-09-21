#!/bin/sh

dump="$1"
if [ -z "$dump" ]; then
  echo "Usage: $0 dump_file [" >&2
  exit 1
fi
shift

(zcat "$dump" && echo 'drop table public.state;') | psql "$@"

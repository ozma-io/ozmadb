#!/bin/sh

owner="$1"
if [ -z "$owner" ]; then
    echo "Usage: $0 owner [pg_args...]" >&2
    exit 1
fi
shift

psql "$@" <<EOF
    alter schema "public" set owner "$owner";
EOF

#!/bin/sh

owner="$(whoami)"
password="1"

psql -U postgres "$@" <<EOF
    create user "$owner" login password '$password';
    create database "$owner" owner "$owner";
    \c $owner
    alter schema "public" owner to "$owner";
EOF

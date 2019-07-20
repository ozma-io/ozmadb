#!/bin/sh

psql "$@" <<EOF
    drop schema "public" cascade;
    drop schema "funapp" cascade;
    drop schema "user" cascade;
    create schema "public";
EOF

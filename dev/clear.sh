#!/bin/sh

cat "$(dirname "$0")"/clear.sql | psql "$@"

#!/bin/sh

zcat dev.sql.gz | psql "$@"

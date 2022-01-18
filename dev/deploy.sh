#!/usr/bin/env bash

set -e

usage() {
  echo "Usage: $0 server_name deployment_name" >&2
  exit 1
}

server_name="$1"
shift

deployment_name="$1"
shift

if [ -z "$server_name" ] || [ -z "$deployment_name" ]; then
  usage
fi

set -x

rsync -ravlL --delete FunDB/publish/ "$server_name:$deployment_name/FunDB"
rsync -ravlL --delete FunApp/dist/ "$server_name:$deployment_name/FunApp"

ssh "$server_name" -- nixops deploy --network "$deployment_name/machines"

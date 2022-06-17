#!/usr/bin/env bash

set -e

usage() {
  echo "Usage: $0 server_name deployment_name" >&2
  exit 1
}

server_name="$1"
shift || usage

deployment_name="$1"
shift || usage

set -x

rsync -ravlL --delete bundle/ "$server_name:$deployment_name/ozma-api"

ssh "$server_name" -- nixops deploy --network "\$(readlink -f $deployment_name/machines)"

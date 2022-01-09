#!/usr/bin/env bash

set -e

usage() {
  echo "Usage: $0 server_name network" >&2
  exit 1
}

server_name="$1"
shift

network="$1"
shift

if [ -z "$server_name" ] || [ -z "$network" ]; then
  usage
fi

build_dir="builds/$BITBUCKET_REPO_SLUG/$BITBUCKET_BUILD_NUMBER"

set -x

rsync -ravlL --delete FunDB/publish/ "$server_name:$network/FunDB"
rsync -ravlL --delete FunApp/dist/ "$server_name:$network/FunApp"

ssh "$server_name" -- nixops deploy --network "$network"

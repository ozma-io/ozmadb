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

build_dir="builds/$BITBUCKET_REPO_SLUG/$BITBUCKET_BUILD_NUMBER"

set -x

rsync -ravlL --delete FunDB/publish/ "$server_name:$deployment_name/FunDB"
rsync -ravlL --delete FunApp/dist/ "$server_name:$deployment_name/FunApp"
rsync -ravlL --delete FunApp/storybook-static/ "$server_name:$deployment_name/FunApp-storybook"

ssh "$server_name" -- nixops deploy -d "$deployment_name"

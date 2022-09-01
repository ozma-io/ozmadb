#!/usr/bin/env bash
# Build and publish the application.

set -ex

test -f yarn.lock
rm -rf dist bundle *.tsbuildinfo
yarn
yarn build

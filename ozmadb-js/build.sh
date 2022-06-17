#!/bin/sh -e
# Build and publish the application.

set -x

test -f yarn.lock
rm -rf dist bundle *.tsbuildinfo
yarn
yarn build

#!/usr/bin/env bash
# Build and publish the application.

set -ex

pushd FunDB
dotnet --info
rm -rf bin
# Issue with lock files
# dotnet restore --locked-mode
dotnet restore --force-evaluate
dotnet publish -c Release
popd

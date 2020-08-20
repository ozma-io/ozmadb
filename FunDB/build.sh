#!/bin/sh -e
# Build and publish the application.

set -x

rm -rf bin publish
# Issue with lock files
# dotnet restore --locked-mode
dotnet restore
dotnet publish -c Release
cp -ar bin/*/*/publish publish

#!/bin/sh -e
# Build and publish the application.

set -x

rm -rf bin publish
dotnet publish -c Release
ln -s bin/*/*/publish publish

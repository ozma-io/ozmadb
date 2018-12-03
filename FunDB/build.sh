#!/bin/sh
# Build and publish the application.

set -x

rm -rf bin publish
dotnet publish -c Release
cp -r bin/*/*/publish publish

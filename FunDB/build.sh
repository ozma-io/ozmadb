#!/bin/sh
# Build and publish the application.

rm -rf bin publish
dotnet publish -c Release
cp -r bin/*/*/publish publish

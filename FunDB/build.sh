#!/bin/sh
# Build and publish the application.

rm -rf bin publish
dotnet -c Release publish
cp -r bin/*/*/publish publish

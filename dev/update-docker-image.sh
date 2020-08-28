#!/bin/sh -e
# Build and upload new version of development images.

docker build --no-cache -t myprocessx/funwithflags-dotnet:latest -f dev/Dockerfile.dotnet dev
docker push myprocessx/funwithflags-dotnet:latest

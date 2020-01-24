#!/bin/sh -e
# Build and upload new version of development images.

docker build --no-cache -t myprocessx/funwithflags-dotnet:latest -f docker/Dockerfile.dotnet docker
docker push myprocessx/funwithflags-dotnet:latest

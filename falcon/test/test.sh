#!/usr/bin/env bash

docker build -t falcon:test ../..

# Run unit tests within docker container
docker run --entrypoint pytest falcon:test

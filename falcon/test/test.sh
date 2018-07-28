#!/usr/bin/env bash

docker build -t falcon:test ../..

# Run unit tests within docker container
docker run --entrypoint python3 falcon:test -m pytest

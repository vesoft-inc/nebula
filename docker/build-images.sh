#!/usr/bin/env bash

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"/../

cd $PROJECT_DIR

docker build -t vesoft/nebula-graphd:latest -f docker/Dockerfile.graphd .
docker build -t vesoft/nebula-metad:latest -f docker/Dockerfile.metad .
docker build -t vesoft/nebula-storaged:latest -f docker/Dockerfile.storaged .
docker build -t vesoft/nebula-console:latest -f docker/Dockerfile.console .

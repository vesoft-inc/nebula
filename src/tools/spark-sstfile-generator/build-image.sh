#! /usr/bin/env bash

readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo $PROJECT_DIR

docker build -f $PROJECT_DIR/src/tools/spark-sstfile-generator/Dockerfile -t vesoft/spark-sstfile-generator $PROJECT_DIR

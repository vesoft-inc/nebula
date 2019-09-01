#! /usr/bin/env bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#docker build . -f build-ubuntu.Dockerfile -t nebula/build-ubuntu
docker build . -f build-centos.Dockerfile -t nebula/build-centos

#! /usr/bin/env bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker build $BASEDIR -f $BASEDIR/build-ubuntu.Dockerfile -t nebula/build-ubuntu

#docker build $BASEDIR -f $BASEDIR/build-centos.Dockerfile -t nebula/build-centos

# Need too many build tools
#docker build $BASEDIR -f $BASEDIR/nebula-centos.Dockerfile -t nebula/nebula-centos

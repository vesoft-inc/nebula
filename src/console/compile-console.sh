#!/bin/bash
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

git clone -b $1 https://github.com/vesoft-inc/nebula-console.git && \
pushd nebula-console && \
make && cp nebula-console $2/ && \
popd && \
rm -rf nebula-console

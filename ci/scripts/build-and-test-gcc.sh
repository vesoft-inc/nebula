#!/usr/bin/env bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -ex

export NEBULA_DEP_BIN=/opt/nebula/third-party/bin
export LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:${LIBRARY_PATH}

mkdir -p build && cd build

${NEBULA_DEP_BIN}/cmake -DCMAKE_C_COMPILER=${NEBULA_DEP_BIN}/gcc -DCMAKE_CXX_COMPILER=${NEBULA_DEP_BIN}/g++ -DCMAKE_BUILD_TYPE=Release ..
make -j $(nproc)
${NEBULA_DEP_BIN}/ctest -j $(nproc) --output-on-failure

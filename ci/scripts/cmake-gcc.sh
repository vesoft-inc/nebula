#!/usr/bin/env bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -ex

cd build

${NEBULA_DEP_BIN}/cmake -DCMAKE_C_COMPILER=${NEBULA_DEP_BIN}/gcc -DCMAKE_CXX_COMPILER=${NEBULA_DEP_BIN}/g++ -DCMAKE_BUILD_TYPE=Release ..

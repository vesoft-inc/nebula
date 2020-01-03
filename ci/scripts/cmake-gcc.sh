#!/usr/bin/env bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -ex

CURR_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR=$CURR_DIR/../..

cd $PROJECT_DIR/build

cmake -DCMAKE_BUILD_TYPE=Release $PROJECT_DIR

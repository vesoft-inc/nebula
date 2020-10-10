#! /usr/bin/env bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

set -e
cxx_cmd=${CXX:-g++}

libcxx=$($cxx_cmd --print-file-name=libstdc++.so)

strings $libcxx | egrep '^GLIBCXX_[.0-9]+$' | sed -r 's/.*_([.0-9]+)$/\1/' | sort -t'.' -ug  -k1,1 -k2,2 -k3,3 | tail -1

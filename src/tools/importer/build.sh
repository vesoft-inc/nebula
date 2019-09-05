#!/bin/bash

# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# $1: the path of pom.xml
mvn compile package -DNEBULA_BUILD_VERSION=$1 -DBUILD_TARGET=$2 -f $3/pom.xml

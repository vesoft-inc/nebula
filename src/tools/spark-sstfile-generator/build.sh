#!/bin/bash

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# $1: the path of pom.xml
mvn compile package -f $1/pom.xml

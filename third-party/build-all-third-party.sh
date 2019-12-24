#! /usr/bin/env bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# Please note that this utility must be invoked with root privilege.

[[ $# -ne 1 ]] && { echo "$0 <path to build-third-party.sh>" 1>&2; exit 1; }

for v in 7.1.0 7.5.0 8.3.0 9.1.0 9.2.0
do
    echo $v;
    source /opt/vesoft/toolset/gcc/$v/enable;
    rm -rf /opt/vesoft/third-party
    build_package=1 disable_cxx11_abi=0 $1 /opt/vesoft/third-party
    rm -rf /opt/vesoft/third-party
    build_package=1 disable_cxx11_abi=1 $1 /opt/vesoft/third-party
    source /opt/vesoft/toolset/gcc/$v/disable;
done

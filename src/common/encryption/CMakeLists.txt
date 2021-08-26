# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    encryption_obj OBJECT
    MD5Utils.cpp
    Base64.cpp
)

add_dependencies(module_common
    encryption_obj)

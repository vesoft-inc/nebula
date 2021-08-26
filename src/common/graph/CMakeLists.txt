# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    graph_obj OBJECT
    Response.cpp
)

add_dependencies(module_common
    graph_obj)

nebula_add_subdirectory(tests)

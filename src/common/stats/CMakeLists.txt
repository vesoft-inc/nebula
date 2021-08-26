# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    stats_obj
    OBJECT
    StatsManager.cpp
)

add_dependencies(module_common
    stats_obj)

nebula_add_subdirectory(test)

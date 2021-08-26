# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_module_library(
    function_manager_obj OBJECT module_common
    FunctionManager.cpp
)

nebula_add_module_library(
    agg_function_manager_obj OBJECT module_common
    AggFunctionManager.cpp
)

nebula_add_subdirectory(test)

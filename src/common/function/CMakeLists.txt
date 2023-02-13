# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    function_manager_obj OBJECT
    FunctionManager.cpp
    ../geo/GeoFunction.cpp
    FunctionUdfManager.cpp
    GraphFunction.h
)

nebula_add_library(
    agg_function_manager_obj OBJECT
    AggFunctionManager.cpp
)

nebula_add_subdirectory(test)

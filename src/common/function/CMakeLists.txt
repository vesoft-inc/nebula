# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    function_manager_obj SHARED
    FunctionManager.cpp
    ../datatypes/GeoFunction.cpp
)

nebula_add_library(
    agg_function_manager_obj SHARED
    AggFunctionManager.cpp
)
target_link_libraries(function_manager_obj PUBLIC "${Glog_LIBRARY}" time_utils_obj  )
target_link_libraries(agg_function_manager_obj PUBLIC "${Glog_LIBRARY}"  )

nebula_add_subdirectory(test)

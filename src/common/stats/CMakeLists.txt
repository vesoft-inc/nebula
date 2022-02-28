# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    stats_obj
    SHARED
    StatsManager.cpp
)

target_link_libraries(stats_obj PUBLIC "${Glog_LIBRARY}" time_obj )
nebula_add_subdirectory(test)

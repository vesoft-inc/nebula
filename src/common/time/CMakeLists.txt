# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    time_obj SHARED
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)

nebula_add_library(
    time_utils_obj SHARED
    TimeUtils.cpp
    TimezoneInfo.cpp
    TimeConversion.cpp
)

target_link_libraries(time_utils_obj PUBLIC "${Glog_LIBRARY}" datetime_parser_obj )


nebula_add_subdirectory(parser)

nebula_add_subdirectory(test)

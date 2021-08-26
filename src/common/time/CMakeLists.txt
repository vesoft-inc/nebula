# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_module_library(
    time_obj OBJECT module_common
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)

nebula_add_module_library(
    time_utils_obj OBJECT module_common
    TimeUtils.cpp
    TimezoneInfo.cpp
    TimeConversion.cpp
)

nebula_add_subdirectory(test)

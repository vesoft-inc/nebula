# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        duration_test
    SOURCES
        DurationTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        wallclock_test
    SOURCES
        WallClockTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        timezone_info_test
    SOURCES
        TimezoneInfoTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        time_utils_test
    SOURCES
        TimeUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
)

nebula_add_executable(
    NAME
        duration_bm
    SOURCES
        DurationBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        follybenchmark
        boost_regex
)

nebula_add_executable(
    NAME wallclock_bm
    SOURCES WallClockBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES follybenchmark boost_regex
)

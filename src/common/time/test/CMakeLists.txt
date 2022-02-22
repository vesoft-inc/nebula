# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        duration_test
    SOURCES
        DurationTest.cpp
        ScopedTimerTest.cpp
    LIBRARIES
        base_obj
        time_obj
        thread_obj
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        wallclock_test
    SOURCES
        WallClockTest.cpp
    LIBRARIES
        time_obj
        thread_obj
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        timezone_info_test
    SOURCES
        TimezoneInfoTest.cpp
    LIBRARIES
        base_obj
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        time_utils_test
    SOURCES
        TimeUtilsTest.cpp
    LIBRARIES
        time_utils_obj
        thread_obj
        datatypes_obj
        base_obj
        fs_obj
        wkt_wkb_io_obj
        datetime_parser_obj
    LIBRARIES
        gtest
)

nebula_add_executable(
    NAME
        duration_bm
    SOURCES
        DurationBenchmark.cpp
    LIBRARIES
        base_obj
        time_obj
        thread_obj
    LIBRARIES
        follybenchmark
        boost_regex
)

nebula_add_executable(
    NAME wallclock_bm
    SOURCES WallClockBenchmark.cpp
    LIBRARIES
        time_obj
    LIBRARIES follybenchmark boost_regex
)

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        stats_manager_test
    SOURCES
        StatsManagerTest.cpp
    LIBRARIES
        stats_obj
        datatypes_obj
        time_obj
        base_obj
        thread_obj
        wkt_wkb_io_obj
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        stats_manager_rate_test
    SOURCES
        StatsManagerRateTest.cpp
    LIBRARIES
        stats_obj
        datatypes_obj
        time_obj
        base_obj
        thread_obj
        wkt_wkb_io_obj
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        stats_manager_cross_level_test
    SOURCES
        StatsManagerCrossLevelTest.cpp
    LIBRARIES
        stats_obj
        datatypes_obj
        time_obj
        base_obj
        thread_obj
        wkt_wkb_io_obj
    LIBRARIES
        gtest
)


nebula_add_executable(
    NAME
        stats_manager_bm
    SOURCES
        StatsManagerBenchmark.cpp
    LIBRARIES
        stats_obj
        datatypes_obj
        time_obj
        base_obj
        wkt_wkb_io_obj
    LIBRARIES
        follybenchmark boost_regex
)

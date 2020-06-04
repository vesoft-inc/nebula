# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        stats_manager_test
    SOURCES
        StatsManagerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        stats_manager_rate_test
    SOURCES
        StatsManagerRateTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        stats_manager_cross_level_test
    SOURCES
        StatsManagerCrossLevelTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)


nebula_add_executable(
    NAME
        stats_manager_bm
    SOURCES
        StatsManagerBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:stats_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:base_obj>
    LIBRARIES
        follybenchmark boost_regex
)

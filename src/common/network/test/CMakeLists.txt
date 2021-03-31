# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        network_utils_test
    SOURCES
        NetworkUtilsTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        gtest
)


nebula_add_executable(
    NAME
        network_utils_bm
    SOURCES
        NetworkUtilsBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:network_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        wangle
        fizz
        sodium
        follybenchmark
)

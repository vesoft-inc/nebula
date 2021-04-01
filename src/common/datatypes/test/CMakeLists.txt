# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        host_addr_test
    SOURCES
        HostAddrTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)


nebula_add_test(
    NAME
        date_test
    SOURCES
        DateTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)


nebula_add_test(
    NAME
        value_test
    SOURCES
        ValueTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:expression_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:agg_function_manager_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)


nebula_add_test(
    NAME
        structral_value_test
    SOURCES
        PathTest.cpp
        EdgeTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
        $<TARGET_OBJECTS:function_manager_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:time_utils_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES
        gtest
        ${THRIFT_LIBRARIES}
)

nebula_add_test(
    NAME
        data_set_test
    SOURCES
        DataSetTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)

nebula_add_executable(
    NAME
        edge_bm
    SOURCES
        EdgeBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
)

nebula_add_executable(
    NAME
        value_bm
    SOURCES
        ValueBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
)

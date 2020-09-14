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
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)


nebula_add_test(
    NAME
        path_test
    SOURCES
        PathTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:datatypes_obj>
    LIBRARIES
        gtest
)

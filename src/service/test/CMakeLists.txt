# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        session_manager_test
    SOURCES
        SessionManagerTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:common_base_obj>
        $<TARGET_OBJECTS:common_thread_obj>
        $<TARGET_OBJECTS:common_datatypes_obj>
        $<TARGET_OBJECTS:common_time_obj>
        $<TARGET_OBJECTS:graph_flags_obj>
        $<TARGET_OBJECTS:session_obj>
        $<TARGET_OBJECTS:common_meta_thrift_obj>
        $<TARGET_OBJECTS:common_common_thrift_obj>
        $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
        ${THRIFT_LIBRARIES}
        wangle
        gtest
        gtest_main
)

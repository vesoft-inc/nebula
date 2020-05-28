# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME
        thread_test
    SOURCES
        ThreadTest.cpp
        GenericWorkerTest.cpp
        GenericThreadPoolTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:thread_obj>
        $<TARGET_OBJECTS:concurrent_obj>
        $<TARGET_OBJECTS:time_obj>
    LIBRARIES
        gtest
        gtest_main
)

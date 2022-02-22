# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        thread_test
    SOURCES
        ThreadTest.cpp
        GenericWorkerTest.cpp
        GenericThreadPoolTest.cpp
    LIBRARIES
        thread_obj
        time_obj
    LIBRARIES
        gtest
        gtest_main
)

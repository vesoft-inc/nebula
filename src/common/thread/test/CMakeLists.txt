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
)
target_link_libraries(
        thread_test
        PRIVATE
        thread_obj
        time_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
